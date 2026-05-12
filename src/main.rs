mod arrow_ipc;
mod protocol;
mod registry;
mod shm;
#[cfg(test)]
mod test_util;
mod tools;
mod workers;

use std::io::{self, BufRead};
use std::process;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::Duration;

use protocol::{BatchRequest, ControlMessage, InitParams, Response};
use registry::{EvictionConfig, Registry};

/// Wire-format protocol version. Bumped on any breaking change to the
/// stdin/stdout envelope (init/batch/shutdown shape, response field set,
/// etc.). Reported in the init reply so miint can detect a drift before
/// sending any batch.
///
/// History:
/// - v1: initial daemon protocol (init / batch / shutdown envelopes;
///   batch carries `tool` + `config` + `shm_input` + optional `batch_id`).
/// - v2: batch request gains required `shm_input_size` field. The reader
///   no longer consults `fstat` to size the input mapping (Darwin POSIX
///   shm has unreliable `fstat` semantics); the protocol's explicit byte
///   count is now the cross-platform source of truth.
/// - v3: init reply gains a `tools: [{name, schema_version}, ...]`
///   registry advertisement. Lets miint do capability detection and
///   schema-version drift checks at handshake time without a trial
///   Submit. Strictly additive — older clients ignore the unknown field.
const PROTOCOL_VERSION: u32 = 3;

/// Default idle timeout (ms) when `init.idle_timeout_ms` is unset. Auto-exit
/// after this much stdin silence so a stuck miint doesn't leave gpl-boundary
/// processes pinned. miint can re-spawn cheaply, so a relatively short value
/// is fine.
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 60_000;

/// Events the stdin reader thread parses out of stdin.
enum StdinEvent {
    Message(ControlMessage),
    Eof,
    /// Parse failure or stdin IO failure. `String` is human-readable.
    Error(String),
}

/// Merged event stream the coordinator's main loop selects on. Stdin
/// events arrive from the stdin reader thread; responses arrive from
/// each worker (in-process worker threads or subprocess reader threads).
enum Event {
    Stdin(StdinEvent),
    Response(Response),
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
            "--worker" => {
                // Subprocess-worker mode. The parent gpl-boundary spawns us
                // with `--worker <tool>` and feeds us a config JSON line +
                // stream of BatchRequests. We loop until EOF, run each batch
                // against a long-lived streaming context, and emit
                // Response lines. See `run_worker` for the protocol.
                let tool_name = args.get(2).map(|s| s.as_str()).unwrap_or_else(|| {
                    eprintln!("Usage: gpl-boundary --worker <tool>");
                    process::exit(1);
                });
                let exit_code = run_worker(tool_name);
                process::exit(exit_code);
            }
            other if other.starts_with('-') => {
                eprintln!("Unknown flag: {other}");
                eprintln!(
                    "Usage: gpl-boundary [--version | --list-tools | --describe <tool> | --worker <tool>]\n  \
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

    // Single channel for the merged event stream. Stdin reader thread
    // pushes `Event::Stdin(...)`; worker forwarder thread pushes
    // `Event::Response(...)` (responses produced by per-fingerprint worker
    // threads or subprocess reader threads land here).
    let (event_tx, event_rx) = mpsc::channel::<Event>();
    let stdin_thread = spawn_stdin_reader(event_tx.clone());

    // Workers send `Response` on a separate channel that a tiny forwarder
    // thread bridges into `event_tx`. This lets `workers` stay decoupled
    // from `Event` (which lives in `main.rs`) at the cost of one extra
    // sleeping thread.
    let (response_tx, response_rx) = mpsc::channel::<Response>();
    let forwarder = spawn_response_forwarder(response_rx, event_tx.clone());

    // --- Init handshake ---
    let init = match event_rx.recv() {
        Ok(Event::Stdin(StdinEvent::Message(ControlMessage::Init { init }))) => init,
        Ok(Event::Stdin(StdinEvent::Message(_))) => {
            let _ = write_response(
                &mut stdout,
                &Response::error(
                    "First message must be 'init' (e.g. {\"init\": {}}); refusing to dispatch \
                     batches without a session handshake",
                ),
            );
            return 1;
        }
        Ok(Event::Stdin(StdinEvent::Eof)) => {
            // Empty stdin — no session, exit cleanly. Helpful for `cargo run`
            // smoke tests with no input.
            let _ = stdin_thread.join();
            return 0;
        }
        Ok(Event::Stdin(StdinEvent::Error(e))) => {
            let _ = write_response(&mut stdout, &Response::error(format!("Init error: {e}")));
            return 1;
        }
        Ok(Event::Response(_)) => {
            // No batches submitted yet; this can't happen in practice. Treat
            // as a protocol error and bail.
            let _ = write_response(
                &mut stdout,
                &Response::error("Unexpected response event before init"),
            );
            return 1;
        }
        Err(_) => {
            // Sender dropped before sending anything (reader thread crashed).
            return 1;
        }
    };

    if write_response(
        &mut stdout,
        &Response::init_ok(PROTOCOL_VERSION, tools::registered_tools_with_versions()),
    )
    .is_err()
    {
        return 1;
    }

    let idle_timeout = init.idle_timeout_ms.unwrap_or(DEFAULT_IDLE_TIMEOUT_MS);

    // --- Session loop ---

    let registry = Registry::new(response_tx.clone(), eviction_config_from(&init));

    // Drop our copy of response_tx so the forwarder can exit when the
    // registry's clones are all closed at session end. Without this,
    // the forwarder would keep its own clone alive and never observe
    // EOF on response_rx.
    drop(response_tx);

    // `pipe_broken` switches off stdout writes after a write_response
    // failure. We still drain `event_rx` in that case so the cleanup
    // registry stays consistent (see PID-sweep comment below).
    let mut pipe_broken = false;

    loop {
        let event = match recv_event(&event_rx, idle_timeout) {
            Ok(Some(e)) => e,
            Ok(None) => break, // Idle timeout: stop and drain.
            Err(()) => break,
        };

        match event {
            Event::Stdin(StdinEvent::Eof) => break,
            Event::Stdin(StdinEvent::Error(e)) => {
                let _ = write_response(&mut stdout, &Response::error(e));
                break;
            }
            Event::Stdin(StdinEvent::Message(ControlMessage::Shutdown { shutdown: true })) => {
                break;
            }
            Event::Stdin(StdinEvent::Message(ControlMessage::Shutdown { shutdown: false })) => {
                continue;
            }
            Event::Stdin(StdinEvent::Message(ControlMessage::Init { .. })) => {
                let _ = write_response(
                    &mut stdout,
                    &Response::error(
                        "Duplicate 'init' message — only valid as the first message in a \
                         session. Restart the process to begin a new session.",
                    ),
                );
                break;
            }
            Event::Stdin(StdinEvent::Message(ControlMessage::Batch(batch))) => {
                registry.submit(batch);
            }
            Event::Response(response) => {
                if write_response(&mut stdout, &response).is_err() {
                    // Pipe to miint is broken. Stop writing but keep
                    // draining so the cleanup registry deregister calls
                    // still run.
                    //
                    // ⚠️ Known orphan window: a normal exit does NOT run
                    // the signal handler, so any shm output produced for
                    // this failed-to-write batch will not be unlinked by
                    // *us*. miint's PID-staleness sweep is the only
                    // recovery — it watches for `/gb-{pid}-*` segments
                    // whose creator PID is no longer alive and unlinks
                    // them. Pre-existing behavior; called out so a
                    // future change doesn't accidentally rely on
                    // gpl-boundary alone for cleanup on this path.
                    pipe_broken = true;
                    break;
                }
                deregister_outputs(&response);
            }
        }
    }

    // Drain phase. Step 4 introduced the pending queue: when a session
    // ends with batches still in-flight or queued, we must let the
    // sweeper dispatch them and let workers emit their responses
    // *before* we close the registry. close_all() unconditionally fails
    // any pending batches, so without this drain a graceful shutdown
    // mid-pending would surface "not dispatched before shutdown" errors
    // for batches the user expected to succeed.
    //
    // Time-capped at 60s as a backstop against a stuck worker. Inside
    // the cap we keep processing response events as fast as they
    // arrive; the cap timeout per recv is small (100ms) so the loop
    // also notices when the registry has fully drained.
    let drain_deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < drain_deadline {
        if registry.in_flight_total() == 0 && registry.pending_count() == 0 {
            break;
        }
        match recv_event(&event_rx, 100) {
            Ok(Some(Event::Response(response))) => {
                if !pipe_broken && write_response(&mut stdout, &response).is_err() {
                    pipe_broken = true;
                }
                deregister_outputs(&response);
            }
            Ok(Some(_)) => continue, // ignore stdin events after shutdown
            Ok(None) => continue,    // 100ms tick — re-check counters
            Err(()) => break,        // event channel closed
        }
    }

    // Close all workers. Blocking — drains any remaining queued batches
    // (post-drain there shouldn't be any), runs C destroy hooks (or
    // reaps subprocesses), joins worker threads.
    registry.close_all();

    // Drop the registry to release its own response_tx clone; without
    // this the forwarder stays blocked on response_rx.recv() forever
    // (workers' clones are gone but the registry's clone keeps the
    // channel alive). Order matters: close workers first (so all
    // pending responses land in response_rx), then drop the registry.
    drop(registry);

    // Forwarder thread now sees response_rx EOF and exits. Joining it
    // here (before draining event_rx) guarantees every forwarded
    // response is in event_rx before we try_recv.
    let _ = forwarder.join();

    // Drain remaining responses for batches that completed during /
    // immediately after shutdown.
    while let Ok(event) = event_rx.try_recv() {
        if let Event::Response(response) = event {
            if !pipe_broken {
                let _ = write_response(&mut stdout, &response);
            }
            deregister_outputs(&response);
        }
    }

    // The stdin reader thread may still be blocked on `read_line`; let the
    // OS reap it when the process exits.
    let _ = stdin_thread;

    0
}

/// Run as a subprocess worker. The parent spawned us as
/// `gpl-boundary --worker <tool>` and is feeding us a config JSON line
/// followed by a stream of `BatchRequest` lines on stdin. We init the
/// streaming context once, then loop running each batch and emitting a
/// `Response` line on stdout. EOF on stdin = graceful shutdown; the
/// streaming context's `Drop` runs its C `destroy`.
///
/// Errors during context init exit non-zero so the parent can surface
/// them. Errors during batch processing produce error `Response`s and
/// keep the worker alive (matches the parent's per-batch error contract).
fn run_worker(tool_name: &str) -> i32 {
    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin.lock());
    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    // First line: config JSON.
    let mut config_line = String::new();
    if let Err(e) = reader.read_line(&mut config_line) {
        eprintln!("worker '{tool_name}': failed to read config line: {e}");
        return 1;
    }
    if config_line.is_empty() {
        // EOF before any config — nothing to do.
        return 0;
    }
    let config: serde_json::Value = match serde_json::from_str(config_line.trim()) {
        Ok(v) => v,
        Err(e) => {
            let resp = Response::error(format!("worker '{tool_name}': invalid config JSON: {e}"));
            let _ = write_response(&mut stdout, &resp);
            return 1;
        }
    };

    let mut ctx = match tools::streaming_setup(tool_name, &config) {
        Ok(Some((c, _sv))) => c,
        Ok(None) => {
            let resp = Response::error(format!(
                "worker '{tool_name}': tool does not support streaming; \
                 cannot run as a subprocess worker"
            ));
            let _ = write_response(&mut stdout, &resp);
            return 1;
        }
        Err(mut resp) => {
            let _ = write_response(&mut stdout, &resp);
            // Touch the response so clippy doesn't flag the mutable binding
            // we keep so callers can inspect/extend before write.
            resp.success = false;
            return 1;
        }
    };

    let schema_version = tools::tool_schema_version(tool_name).unwrap_or_default();

    // Batch loop.
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break, // EOF — graceful shutdown.
            Ok(_) => {}
            Err(e) => {
                eprintln!("worker '{tool_name}': stdin read failed: {e}");
                break;
            }
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let batch: BatchRequest = match serde_json::from_str(trimmed) {
            Ok(b) => b,
            Err(e) => {
                let resp =
                    Response::error(format!("worker '{tool_name}': malformed batch line: {e}"));
                if write_response(&mut stdout, &resp).is_err() {
                    break;
                }
                continue;
            }
        };

        let mut response = ctx.run_batch(&batch.shm_input, batch.shm_input_size);
        if response.success {
            response.schema_version = Some(schema_version);
        }
        response.batch_id = batch.batch_id;

        if write_response(&mut stdout, &response).is_err() {
            break;
        }
        // Note: the worker's *own* output shm is registered for cleanup in
        // ShmWriter::new and deregistered after the response is written.
        // The deregister happens in the parent's session loop for
        // in-process tools — for subprocess workers, we deregister here
        // because the parent never knows about the shm names directly
        // (they live in this process's cleanup registry).
        deregister_outputs(&response);
    }

    // Drop ctx → C destroy runs → exit.
    drop(ctx);
    0
}

fn spawn_stdin_reader(tx: Sender<Event>) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("gb-stdin".to_string())
        .spawn(move || {
            let stdin = io::stdin();
            let mut reader = io::BufReader::new(stdin.lock());
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        let _ = tx.send(Event::Stdin(StdinEvent::Eof));
                        break;
                    }
                    Ok(_) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<ControlMessage>(trimmed) {
                            Ok(msg) => {
                                if tx.send(Event::Stdin(StdinEvent::Message(msg))).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Event::Stdin(StdinEvent::Error(format!(
                                    "Invalid control message JSON: {e}"
                                ))));
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Event::Stdin(StdinEvent::Error(format!(
                            "stdin read failed: {e}"
                        ))));
                        break;
                    }
                }
            }
        })
        .expect("failed to spawn stdin reader thread")
}

/// Bridge worker `Response`s into the coordinator's `Event` stream. Exits
/// when the response channel closes (i.e. once the registry has dropped
/// its last sender at session end).
fn spawn_response_forwarder(
    response_rx: mpsc::Receiver<Response>,
    event_tx: Sender<Event>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("gb-fwd".to_string())
        .spawn(move || {
            while let Ok(response) = response_rx.recv() {
                if event_tx.send(Event::Response(response)).is_err() {
                    break;
                }
            }
        })
        .expect("failed to spawn response forwarder thread")
}

/// `Ok(Some(event))` got an event before timeout.
/// `Ok(None)` timed out.
/// `Err(())` channel disconnected.
fn recv_event(rx: &mpsc::Receiver<Event>, idle_timeout_ms: u64) -> Result<Option<Event>, ()> {
    if idle_timeout_ms == 0 {
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

/// Build an `EvictionConfig` from the init knobs the client sent. Missing
/// fields fall back to `EvictionConfig::default()`. `workers_per_fingerprint`
/// is accepted in the protocol but not yet used (step 3/4 keep it at 1
/// for bowtie2 because the C library has process-wide state behind a
/// mutex; >1 children for the same fingerprint just share an index for
/// nothing).
fn eviction_config_from(init: &InitParams) -> EvictionConfig {
    let mut cfg = EvictionConfig::default();
    if let Some(v) = init.max_workers {
        cfg.max_workers = v;
    }
    if let Some(v) = init.max_idle_workers {
        cfg.max_idle_workers = v;
    }
    if let Some(v) = init.worker_idle_ms {
        cfg.worker_idle_ms = v;
    }
    // workers_per_fingerprint is accepted for forward-compat; no-op today.
    let _ = init.workers_per_fingerprint;
    cfg
}

fn write_response<W: io::Write>(writer: &mut W, response: &Response) -> Result<(), String> {
    let json = serde_json::to_string(response)
        .map_err(|e| format!("Failed to serialize response: {e}"))?;
    writeln!(writer, "{json}").map_err(|e| format!("Failed to write response: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("Failed to flush response: {e}"))
}

/// Deregister this process's cleanup-registry entries for any output
/// shm referenced in `response`. Must be called AFTER the response has
/// been written to stdout — if a signal arrives between deregister and
/// write, the shm leaks (registry has no reference, miint never sees
/// the name).
///
/// For in-process tools, the parent created the shm via `ShmWriter`
/// which registered it; the deregister here transfers ownership to
/// miint. For subprocess workers, the *child* created the shm in its
/// own process and ran the equivalent deregister there — the parent
/// never knew the name in its own cleanup registry, so this call is a
/// harmless no-op for those names. We do it unconditionally to keep
/// the call site simple.
fn deregister_outputs(response: &Response) {
    for shm_out in &response.shm_outputs {
        shm::deregister_cleanup(&shm_out.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_recv_event_zero_blocks() {
        let (tx, rx) = mpsc::channel::<Event>();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            tx.send(Event::Stdin(StdinEvent::Eof)).unwrap();
        });
        let result = recv_event(&rx, 0);
        handle.join().unwrap();
        assert!(matches!(result, Ok(Some(Event::Stdin(StdinEvent::Eof)))));
    }

    #[test]
    fn test_recv_event_times_out() {
        let (_tx, rx) = mpsc::channel::<Event>();
        let result = recv_event(&rx, 50);
        assert!(matches!(result, Ok(None)));
    }
}
