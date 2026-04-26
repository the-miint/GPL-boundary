mod arrow_ipc;
mod protocol;
mod shm;
#[cfg(test)]
mod test_util;
mod tools;

use std::io::{self, BufRead};
use std::process;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use protocol::{BatchRequest, ControlMessage, Response};
use tools::StreamingContext;

/// Wire-format protocol version. Bumped on any breaking change to the
/// stdin/stdout envelope (init/batch/shutdown shape, response field set,
/// etc.). Reported in the init reply so miint can detect a drift before
/// sending any batch.
const PROTOCOL_VERSION: u32 = 1;

/// Default idle timeout (ms) when `init.idle_timeout_ms` is unset. Auto-exit
/// after this much stdin silence so a stuck miint doesn't leave gpl-boundary
/// processes pinned. miint can re-spawn cheaply, so a relatively short value
/// is fine.
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 60_000;

/// Events the stdin reader thread pushes onto the main loop's channel.
enum StdinEvent {
    Message(ControlMessage),
    Eof,
    /// Parse failure or stdin IO failure. `String` is human-readable.
    Error(String),
}

fn main() {
    // Install signal handlers as early as possible — before any shm is
    // created via `ShmWriter::new` further down the call chain.
    shm::install_signal_handlers();

    // Fail fast if two modules registered the same tool name. This catches
    // drift from `inventory::submit!` collisions at startup, not at first
    // dispatch.
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
    if args.len() > 1 {
        match args[1].as_str() {
            "--version" => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&tools::version_info()).unwrap()
                );
                return;
            }
            "--list-tools" => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&tools::list_tools()).unwrap()
                );
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
                eprintln!(
                    "Usage: gpl-boundary [--version | --list-tools | --describe <tool>]\n  \
                     daemon: pipe NDJSON {{init/batch/shutdown}} on stdin"
                );
                process::exit(1);
            }
            _ => {}
        }
    }

    // Daemon-only execution. Single-shot mode is gone — every invocation is
    // a session driven by the NDJSON envelope on stdin.
    let exit_code = run_session();
    process::exit(exit_code);
}

/// Run one daemon session. Returns the exit code: 0 for clean shutdown
/// (init succeeded then either `shutdown` sentinel, EOF, or idle timeout),
/// non-zero for protocol errors or IO failures during init.
fn run_session() -> i32 {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    let (tx, rx) = mpsc::channel();
    let stdin_thread = spawn_stdin_reader(tx);

    // --- Init handshake ---
    let init = match rx.recv() {
        Ok(StdinEvent::Message(ControlMessage::Init { init })) => init,
        Ok(StdinEvent::Message(_)) => {
            let _ = write_response(
                &mut stdout,
                &Response::error(
                    "First message must be 'init' (e.g. {\"init\": {}}); refusing to dispatch \
                     batches without a session handshake",
                ),
            );
            return 1;
        }
        Ok(StdinEvent::Eof) => {
            // Empty stdin — no session, exit cleanly. Helpful for `cargo run`
            // smoke tests with no input.
            let _ = stdin_thread.join();
            return 0;
        }
        Ok(StdinEvent::Error(e)) => {
            let _ = write_response(&mut stdout, &Response::error(format!("Init error: {e}")));
            return 1;
        }
        Err(_) => {
            // Sender dropped before sending anything (reader thread crashed).
            return 1;
        }
    };

    if write_response(&mut stdout, &Response::init_ok(PROTOCOL_VERSION)).is_err() {
        return 1;
    }

    let idle_timeout = init.idle_timeout_ms.unwrap_or(DEFAULT_IDLE_TIMEOUT_MS);
    // The remaining init fields (max_workers / workers_per_fingerprint /
    // max_idle_workers / worker_idle_ms) are accepted now for forward-compat
    // with Phase 4's worker pool but ignored in Phase 3.

    // --- Session loop ---

    // Phase 3: enforce single-fingerprint per session. The first batch
    // establishes the fingerprint (canonical config string + tool name) and
    // (if the tool supports it) creates a long-lived streaming context.
    // Subsequent mismatched batches return an error response without
    // disturbing the established context.
    let mut session_fingerprint: Option<Fingerprint> = None;
    let mut streaming_ctx: Option<Box<dyn StreamingContext>> = None;
    let mut session_schema_version: Option<u32> = None;

    loop {
        let event_opt = recv_with_timeout(&rx, idle_timeout);
        let event = match event_opt {
            Ok(Some(e)) => e,
            Ok(None) => {
                // Idle timeout: clean shutdown.
                break;
            }
            Err(()) => {
                // Channel disconnected — reader thread ended unexpectedly.
                break;
            }
        };

        match event {
            StdinEvent::Eof => break,
            StdinEvent::Error(e) => {
                let _ = write_response(&mut stdout, &Response::error(e));
                break;
            }
            StdinEvent::Message(ControlMessage::Shutdown { shutdown: true }) => break,
            StdinEvent::Message(ControlMessage::Shutdown { shutdown: false }) => {
                // `{"shutdown": false}` is meaningless but harmless. Ignore.
                continue;
            }
            StdinEvent::Message(ControlMessage::Init { .. }) => {
                // A second init mid-session means either a buggy client or
                // a re-exec into the same pipe. Either way, continuing under
                // the original session's assumptions (fingerprint, streaming
                // context) is unsafe. Surface the error and exit; the
                // client can restart with a fresh process.
                let _ = write_response(
                    &mut stdout,
                    &Response::error(
                        "Duplicate 'init' message — only valid as the first message in a \
                         session. Restart the process to begin a new session.",
                    ),
                );
                break;
            }
            StdinEvent::Message(ControlMessage::Batch(batch)) => {
                let response = run_batch(
                    &batch,
                    &mut session_fingerprint,
                    &mut streaming_ctx,
                    &mut session_schema_version,
                );

                if write_response(&mut stdout, &response).is_err() {
                    // Pipe to miint is broken — the response is NOT on
                    // stdout, so miint never learned the shm names.
                    // Leave them in the cleanup registry so the signal
                    // handler still unlinks them on process exit; calling
                    // deregister_outputs here would orphan them.
                    break;
                }
                deregister_outputs(&response);
            }
        }
    }

    // Drop the streaming context here so any C library `destroy` runs before
    // the process exits and registry cleanup runs.
    drop(streaming_ctx);

    // The stdin reader thread may still be blocked on `read_line`; let the
    // OS reap it when the process exits.
    let _ = stdin_thread;

    0
}

/// `(tool_name, canonical_config_string)`. The canonical config string is
/// `serde_json::to_string` of a recursively key-sorted Value, so two configs
/// that differ only in JSON key ordering produce identical fingerprints.
/// Phase 4 will hash this string with SHA-256 to key its worker registry;
/// Phase 3 only needs equality, which a string comparison gives us for free.
#[derive(Debug, PartialEq, Eq, Clone)]
struct Fingerprint {
    tool: String,
    canonical_config: String,
}

impl Fingerprint {
    fn from_batch(batch: &BatchRequest) -> Self {
        Self {
            tool: batch.tool.clone(),
            canonical_config: canonical_json(&batch.config),
        }
    }
}

/// Recursively serialize `value` with sorted object keys at every level.
/// Used to produce a canonical, order-independent representation for
/// fingerprint equality.
fn canonical_json(value: &serde_json::Value) -> String {
    fn canonicalize(value: &serde_json::Value) -> serde_json::Value {
        use serde_json::Value;
        match value {
            Value::Object(map) => {
                let mut entries: Vec<(&String, &Value)> = map.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                let canonical: serde_json::Map<String, Value> = entries
                    .into_iter()
                    .map(|(k, v)| (k.clone(), canonicalize(v)))
                    .collect();
                Value::Object(canonical)
            }
            Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string(&canonicalize(value)).unwrap_or_default()
}

/// Compute and dispatch one batch under the Phase 3 single-fingerprint rule.
/// Lazily creates the session's streaming context on the first batch.
fn run_batch(
    batch: &BatchRequest,
    session_fingerprint: &mut Option<Fingerprint>,
    streaming_ctx: &mut Option<Box<dyn StreamingContext>>,
    session_schema_version: &mut Option<u32>,
) -> Response {
    // Fingerprint check. Phase 3 rejects mismatches; Phase 4 will route by
    // fingerprint to a worker pool instead.
    let fp = Fingerprint::from_batch(batch);
    match session_fingerprint {
        Some(existing) if existing != &fp => {
            let mut r = Response::error(format!(
                "Phase 3 supports a single (tool, config) per session; \
                 first batch was tool '{}', got '{}'",
                existing.tool, batch.tool
            ));
            r.batch_id = batch.batch_id;
            return r;
        }
        Some(_) => { /* matches established fingerprint */ }
        None => {
            *session_fingerprint = Some(fp);
            // Try to create a long-lived streaming context.
            // - Err: real failure (unknown tool, or context init failed).
            //   Surface that as the batch response; do NOT fall through to
            //   `dispatch`, which would either re-fail or silently swallow
            //   a partially-initialized context error.
            // - Ok(None): the tool exists but doesn't support streaming.
            //   Fall through to per-batch dispatch — correct, just less
            //   efficient.
            // - Ok(Some): use the long-lived context for every batch.
            match tools::streaming_setup(&batch.tool, &batch.config) {
                Ok(Some((ctx, sv))) => {
                    *streaming_ctx = Some(ctx);
                    *session_schema_version = Some(sv);
                }
                Ok(None) => {
                    *session_schema_version = None;
                }
                Err(mut response) => {
                    response.batch_id = batch.batch_id;
                    return response;
                }
            }
        }
    }

    let mut response = match streaming_ctx.as_mut() {
        Some(ctx) => {
            let mut r = ctx.run_batch(&batch.shm_input);
            if r.success {
                r.schema_version = *session_schema_version;
            }
            r
        }
        None => tools::dispatch(batch),
    };
    response.batch_id = batch.batch_id;
    response
}

fn spawn_stdin_reader(tx: mpsc::Sender<StdinEvent>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let stdin = io::stdin();
        let mut reader = io::BufReader::new(stdin.lock());
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = tx.send(StdinEvent::Eof);
                    break;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        // Skip blank separator lines silently.
                        continue;
                    }
                    match serde_json::from_str::<ControlMessage>(trimmed) {
                        Ok(msg) => {
                            if tx.send(StdinEvent::Message(msg)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(StdinEvent::Error(format!(
                                "Invalid control message JSON: {e}"
                            )));
                            break;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(StdinEvent::Error(format!("stdin read failed: {e}")));
                    break;
                }
            }
        }
    })
}

/// `Ok(Some(event))` got an event before timeout.
/// `Ok(None)` timed out.
/// `Err(())` channel disconnected.
fn recv_with_timeout(
    rx: &mpsc::Receiver<StdinEvent>,
    idle_timeout_ms: u64,
) -> Result<Option<StdinEvent>, ()> {
    if idle_timeout_ms == 0 {
        // Disabled: block forever until something arrives or the channel
        // closes. Useful for callers that want strict request/response.
        match rx.recv() {
            Ok(e) => Ok(Some(e)),
            Err(_) => Err(()),
        }
    } else {
        match rx.recv_timeout(Duration::from_millis(idle_timeout_ms)) {
            Ok(e) => Ok(Some(e)),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(()),
        }
    }
}

fn write_response<W: io::Write>(writer: &mut W, response: &Response) -> Result<(), String> {
    let json = serde_json::to_string(response)
        .map_err(|e| format!("Failed to serialize response: {e}"))?;
    writeln!(writer, "{json}").map_err(|e| format!("Failed to write response: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("Failed to flush response: {e}"))
}

/// Deregister all output shm segments from the cleanup registry.
/// Must be called AFTER the response has been written to stdout — if a
/// signal arrives between deregister and write, the shm leaks (registry
/// has no reference, miint never sees the name).
fn deregister_outputs(response: &Response) {
    for shm_out in &response.shm_outputs {
        shm::deregister_cleanup(&shm_out.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_json_orders_keys() {
        let a = serde_json::json!({"a": 1, "b": 2});
        let b = serde_json::json!({"b": 2, "a": 1});
        assert_eq!(canonical_json(&a), canonical_json(&b));
        assert_eq!(canonical_json(&a), r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn test_canonical_json_recurses_into_nested_objects() {
        let a = serde_json::json!({"outer": {"a": 1, "b": 2}, "x": 3});
        let b = serde_json::json!({"x": 3, "outer": {"b": 2, "a": 1}});
        assert_eq!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn test_canonical_json_preserves_array_order() {
        let a = serde_json::json!({"v": [3, 1, 2]});
        let b = serde_json::json!({"v": [1, 2, 3]});
        assert_ne!(
            canonical_json(&a),
            canonical_json(&b),
            "Array order is meaningful and must NOT be sorted"
        );
    }

    #[test]
    fn test_fingerprint_equal_for_reordered_keys() {
        let b1 = BatchRequest {
            tool: "prodigal".to_string(),
            config: serde_json::json!({"meta_mode": true, "trans_table": 11}),
            shm_input: "/x".to_string(),
            batch_id: None,
        };
        let b2 = BatchRequest {
            tool: "prodigal".to_string(),
            config: serde_json::json!({"trans_table": 11, "meta_mode": true}),
            shm_input: "/x".to_string(),
            batch_id: None,
        };
        assert_eq!(Fingerprint::from_batch(&b1), Fingerprint::from_batch(&b2));
    }

    #[test]
    fn test_fingerprint_differs_for_different_values() {
        let b1 = BatchRequest {
            tool: "prodigal".to_string(),
            config: serde_json::json!({"meta_mode": true}),
            shm_input: "/x".to_string(),
            batch_id: None,
        };
        let b2 = BatchRequest {
            tool: "prodigal".to_string(),
            config: serde_json::json!({"meta_mode": false}),
            shm_input: "/x".to_string(),
            batch_id: None,
        };
        assert_ne!(Fingerprint::from_batch(&b1), Fingerprint::from_batch(&b2));
    }

    #[test]
    fn test_write_response_ndjson() {
        let resp = Response::ok(serde_json::json!({"count": 1}), vec![]);
        let mut buf = Vec::new();
        write_response(&mut buf, &resp).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.ends_with('\n'));
        assert!(!output.ends_with("\n\n"));
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        assert!(parsed["success"].as_bool().unwrap());
    }

    #[test]
    fn test_write_response_error() {
        let resp = Response::error("test error");
        let mut buf = Vec::new();
        write_response(&mut buf, &resp).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(output.trim()).unwrap();
        assert!(!parsed["success"].as_bool().unwrap());
        assert_eq!(parsed["error"], "test error");
    }

    #[test]
    fn test_recv_with_timeout_zero_blocks() {
        let (tx, rx) = mpsc::channel::<StdinEvent>();
        // Send something so the blocking recv returns.
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            tx.send(StdinEvent::Eof).unwrap();
        });
        let result = recv_with_timeout(&rx, 0);
        handle.join().unwrap();
        assert!(matches!(result, Ok(Some(StdinEvent::Eof))));
    }

    #[test]
    fn test_recv_with_timeout_times_out() {
        let (_tx, rx) = mpsc::channel::<StdinEvent>();
        let result = recv_with_timeout(&rx, 50);
        assert!(matches!(result, Ok(None)));
    }
}
