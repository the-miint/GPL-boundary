mod arrow_ipc;
mod protocol;
mod shm;
#[cfg(test)]
mod test_util;
mod tools;

use std::io::{self, BufRead};
use std::process;

/// Read one line from the reader and deserialize as JSON type T.
/// Returns Ok(None) on EOF, Ok(Some(value)) on success, Err on parse/IO error.
fn read_json_line<R: BufRead, T: serde::de::DeserializeOwned>(
    reader: &mut R,
    label: &str,
) -> Result<Option<T>, String> {
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) => Ok(None),
        Ok(_) => serde_json::from_str(line.trim())
            .map(Some)
            .map_err(|e| format!("Invalid {label} JSON: {e}")),
        Err(e) => Err(format!("Failed to read stdin: {e}")),
    }
}

/// Write a JSON response as a single NDJSON line (JSON + newline) and flush.
/// Explicit flush is required because stdout may be a pipe (not a tty),
/// and LineWriter's newline-flush heuristic is not a documented guarantee.
fn write_response<W: io::Write>(
    writer: &mut W,
    response: &protocol::Response,
) -> Result<(), String> {
    let json = serde_json::to_string(response)
        .map_err(|e| format!("Failed to serialize response: {e}"))?;
    writeln!(writer, "{json}").map_err(|e| format!("Failed to write response: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("Failed to flush response: {e}"))
}

/// Deregister all output shm segments from the cleanup registry.
/// Must be called AFTER the response is written to stdout — if a signal
/// arrives between deregister and stdout write, the shm leaks.
fn deregister_outputs(response: &protocol::Response) {
    for shm_out in &response.shm_outputs {
        shm::deregister_cleanup(&shm_out.name);
    }
}

fn main() {
    // Install signal handlers for shm cleanup before any shm is created
    shm::install_signal_handlers();

    // Fail fast if two modules registered the same tool name
    {
        let mut names = tools::list_tools();
        let total = names.len();
        names.sort();
        names.dedup();
        if names.len() != total {
            eprintln!("fatal: duplicate tool names in registry");
            process::exit(1);
        }
    }

    let args: Vec<String> = std::env::args().collect();

    // Handle introspection flags before reading stdin
    if args.len() > 1 {
        match args[1].as_str() {
            "--version" => {
                let info = tools::version_info();
                println!("{}", serde_json::to_string_pretty(&info).unwrap());
                return;
            }
            "--list-tools" => {
                let names = tools::list_tools();
                println!("{}", serde_json::to_string_pretty(&names).unwrap());
                return;
            }
            "--describe" => {
                let tool_name = args.get(2).map(|s| s.as_str()).unwrap_or_else(|| {
                    eprintln!("Usage: gpl-boundary --describe <tool>");
                    process::exit(1);
                });
                match tools::describe_tool(tool_name) {
                    Some(desc) => {
                        println!("{}", serde_json::to_string_pretty(&desc).unwrap());
                    }
                    None => {
                        eprintln!("Unknown tool: {tool_name}");
                        eprintln!("Available: {:?}", tools::list_tools());
                        process::exit(1);
                    }
                }
                return;
            }
            other if other.starts_with('-') => {
                eprintln!("Unknown flag: {other}");
                eprintln!("Usage: gpl-boundary [--version | --list-tools | --describe <tool>]");
                eprintln!("       echo '{{...}}' | gpl-boundary");
                process::exit(1);
            }
            _ => {}
        }
    }

    // Normal execution: read JSON request from stdin (NDJSON line-by-line)
    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin.lock());
    let mut stdout = io::stdout().lock();

    let request: protocol::Request = match read_json_line(&mut reader, "request") {
        Ok(Some(r)) => r,
        Ok(None) => {
            eprintln!("Empty stdin: no request received");
            process::exit(1);
        }
        Err(e) => {
            let resp = protocol::Response::error(e);
            let _ = write_response(&mut stdout, &resp);
            process::exit(1);
        }
    };

    if request.stream {
        // Streaming mode: create context once, process multiple batches
        let (mut ctx, schema_version) = match tools::streaming_setup(&request.tool, &request.config)
        {
            Ok(pair) => pair,
            Err(resp) => {
                let _ = write_response(&mut stdout, &resp);
                process::exit(1);
            }
        };

        // First batch (from the initial request)
        let mut response = ctx.run_batch(&request.shm_input);
        if response.success {
            response.schema_version = Some(schema_version);
        }
        if write_response(&mut stdout, &response).is_err() {
            // Broken pipe to miint — no point continuing
            deregister_outputs(&response);
            process::exit(1);
        }
        deregister_outputs(&response);

        // Subsequent batches until EOF
        loop {
            match read_json_line::<_, protocol::BatchRequest>(&mut reader, "batch request") {
                Ok(None) => break, // EOF — session complete
                Ok(Some(batch)) => {
                    let mut resp = ctx.run_batch(&batch.shm_input);
                    if resp.success {
                        resp.schema_version = Some(schema_version);
                    }
                    if write_response(&mut stdout, &resp).is_err() {
                        deregister_outputs(&resp);
                        break; // Broken pipe — stop
                    }
                    deregister_outputs(&resp);
                }
                Err(e) => {
                    // IO or parse error reading from stdin. Write error
                    // response and terminate: if the pipe is broken, looping
                    // would produce infinite errors; if JSON is malformed,
                    // it's a bug in miint and the session is suspect.
                    let resp = protocol::Response::error(e);
                    let _ = write_response(&mut stdout, &resp);
                    break;
                }
            }
        }
        // ctx drops here — calls C library's destroy()
    } else {
        // Single-shot mode (existing behavior)
        let response = tools::dispatch(&request);
        if let Err(e) = write_response(&mut stdout, &response) {
            eprintln!("{e}");
            process::exit(1);
        }
        deregister_outputs(&response);

        if !response.success {
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_json_line_request() {
        let data = b"{\"tool\":\"fasttree\",\"shm_input\":\"/x\"}\n";
        let mut cursor = io::Cursor::new(data.as_slice());
        let result = read_json_line::<_, protocol::Request>(&mut cursor, "request");
        let req = result.unwrap().unwrap();
        assert_eq!(req.tool, "fasttree");
        assert_eq!(req.shm_input, "/x");
        assert!(!req.stream);
    }

    #[test]
    fn test_read_json_line_eof() {
        let mut cursor = io::Cursor::new(b"".as_slice());
        let result = read_json_line::<_, protocol::Request>(&mut cursor, "request");
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_read_json_line_no_trailing_newline() {
        // read_line returns partial line on EOF-without-newline
        let data = b"{\"tool\":\"fasttree\",\"shm_input\":\"/x\"}";
        let mut cursor = io::Cursor::new(data.as_slice());
        let result = read_json_line::<_, protocol::Request>(&mut cursor, "request");
        let req = result.unwrap().unwrap();
        assert_eq!(req.tool, "fasttree");
    }

    #[test]
    fn test_read_json_line_invalid_json() {
        let data = b"not json\n";
        let mut cursor = io::Cursor::new(data.as_slice());
        let result = read_json_line::<_, protocol::Request>(&mut cursor, "request");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid request JSON"));
    }

    #[test]
    fn test_read_json_line_batch_request() {
        let data = b"{\"shm_input\":\"/batch-1\"}\n";
        let mut cursor = io::Cursor::new(data.as_slice());
        let result = read_json_line::<_, protocol::BatchRequest>(&mut cursor, "batch request");
        let batch = result.unwrap().unwrap();
        assert_eq!(batch.shm_input, "/batch-1");
    }

    #[test]
    fn test_write_response_ndjson() {
        let resp = protocol::Response::ok(serde_json::json!({"count": 1}), vec![]);
        let mut buf = Vec::new();
        write_response(&mut buf, &resp).unwrap();
        let output = String::from_utf8(buf).unwrap();

        // Must end with exactly one newline
        assert!(output.ends_with('\n'));
        assert!(!output.ends_with("\n\n"));

        // Must be valid JSON (excluding the trailing newline)
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        assert!(parsed["success"].as_bool().unwrap());
    }

    #[test]
    fn test_write_response_error() {
        let resp = protocol::Response::error("test error");
        let mut buf = Vec::new();
        write_response(&mut buf, &resp).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        assert!(!parsed["success"].as_bool().unwrap());
        assert_eq!(parsed["error"], "test error");
    }
}
