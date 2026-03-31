mod protocol;
mod shm;
mod tools;

use std::io::{self, Read};
use std::process;

fn main() {
    // Install signal handlers for shm cleanup before any shm is created
    shm::install_signal_handlers();

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

    // Normal execution: read JSON request from stdin
    let mut input = String::new();
    if let Err(e) = io::stdin().read_to_string(&mut input) {
        eprintln!("Failed to read stdin: {e}");
        process::exit(1);
    }

    let request: protocol::Request = match serde_json::from_str(&input) {
        Ok(r) => r,
        Err(e) => {
            let resp = protocol::Response::error(format!("Invalid request JSON: {e}"));
            println!("{}", serde_json::to_string(&resp).unwrap());
            process::exit(1);
        }
    };

    let response = tools::dispatch(&request);

    match serde_json::to_string(&response) {
        Ok(json) => println!("{json}"),
        Err(e) => {
            eprintln!("Failed to serialize response: {e}");
            process::exit(1);
        }
    }

    if !response.success {
        process::exit(1);
    }
}
