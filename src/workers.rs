//! Worker abstraction for the per-fingerprint dispatcher.
//!
//! A `Worker` owns the long-lived state for one `(tool, config)` fingerprint
//! and serves batches submitted to that fingerprint. Phase 4 ships two
//! implementations:
//!
//! - `InProcessSlot` — wraps an optional `StreamingContext` (for tools that
//!   support it) or falls through to `tools::dispatch` (for tools that don't).
//!   Used by FastTree, Prodigal, SortMeRNA, and the bowtie2-build helper.
//!   Each slot owns a dedicated worker thread that drains a per-slot mpsc
//!   queue, so same-fingerprint batches are naturally serialized while
//!   distinct fingerprints run in parallel.
//!
//! - `SubprocessWorker` — drives a child gpl-boundary process via stdin/stdout
//!   pipes. Used by bowtie2-align so two distinct indexes can align in
//!   parallel without colliding on bowtie2's process-wide global mutex.
//!   Spawned with `gpl-boundary --worker bowtie2-align` plus the config
//!   JSON on the first stdin line.
//!
//! ## Async dispatch
//!
//! `submit` is fire-and-forget: it enqueues the batch on the worker's
//! internal channel and returns immediately. The worker produces a
//! `Response` later and sends it on the shared `Sender<Response>` passed
//! at construction. The coordinator merges those responses with stdin
//! events in its main loop, which is what unlocks cross-fingerprint
//! parallelism.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::protocol::{BatchRequest, Response};
use crate::tools::{self, StreamingContext};

/// Channel the coordinator listens on for completed batch responses. Every
/// `Worker` impl is constructed with a clone of this sender. The coordinator
/// bridges it into its merged `Event` stream via a small forwarder thread —
/// `workers` itself doesn't need to know about the coordinator's `Event`
/// type, which keeps the dispatch glue out of this module.
pub type ResponseTx = Sender<Response>;

/// Wall-clock instant captured as epoch-millis. The eviction sweeper (step 4)
/// reads `last_used` on every tick across many workers; an `AtomicU64` is
/// cheaper than a `Mutex<Instant>` and avoids contention with active
/// `submit` callers.
fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Distinguishes workers that count against the subprocess budget
/// (`max_workers`) from in-process workers that do not. The registry
/// uses this to decide whether to apply eviction pressure when a new
/// fingerprint arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerKind {
    InProcess,
    Subprocess,
}

/// One worker handles all batches for a single `(tool, config)` fingerprint.
/// Implementations must be `Send + Sync` because the registry hands out
/// `Arc<dyn Worker>` and submits run on the coordinator thread.
///
/// `submit` is fire-and-forget — the response arrives later on the
/// `ResponseTx` the worker was constructed with. Callers do not block.
pub trait Worker: Send + Sync {
    /// Enqueue `batch` for asynchronous execution. Implementations must
    /// always emit exactly one `Response` on the registry's `ResponseTx`,
    /// even on internal failures; the coordinator counts responses to
    /// know when everything is drained at session end.
    fn submit(&self, batch: BatchRequest);

    /// Wall-clock instant of the most recent submit. Used by the LRU +
    /// idle-deadline sweeper. Reads must be cheap enough to call from
    /// the eviction tick.
    fn last_used(&self) -> Instant;

    /// Graceful close. Idempotent, blocking. Drains any queued batches,
    /// runs the C `destroy` hook (or reaps the subprocess), joins the
    /// internal thread(s). After `close()` returns, no further responses
    /// will arrive on the `ResponseTx`.
    fn close(&self);

    /// Number of batches submitted but not yet responded to. The
    /// eviction sweeper consults this to enforce the "in-flight workers
    /// are protected from eviction" invariant. Implementations must
    /// increment *before* dispatching the batch and decrement *after*
    /// pushing the response onto `ResponseTx`, never the other way
    /// around — otherwise the registry could evict mid-response.
    fn in_flight(&self) -> u32;

    /// Whether this worker counts against the subprocess `max_workers`
    /// budget. In-process slots return `InProcess` (cheap, no budget
    /// pressure); subprocess workers return `Subprocess`.
    fn kind(&self) -> WorkerKind;
}

// ---------------------------------------------------------------------------
// InProcessSlot
// ---------------------------------------------------------------------------

/// In-process worker.
///
/// Three states matter for `submit`:
/// - **Streaming, open** — context exists, `closed == false`. Worker thread
///   runs `ctx.run_batch`.
/// - **Non-streaming** — context never existed (the tool's
///   `create_streaming_context` returned `Ok(None)`), `closed == false`.
///   Each batch goes through the stateless `tools::dispatch` path.
/// - **Closed** — `closed == true`. New submits respond with an error
///   without entering the worker thread; the worker thread itself has
///   already exited.
///
/// We track "closed" separately from "context is None" because both
/// non-streaming tools and closed streaming tools have `ctx: None`.
pub struct InProcessSlot {
    /// Sender into the worker thread's input queue. Wrapped in
    /// `Mutex<Option<...>>` so `close()` can drop it (signaling EOF to the
    /// worker thread) idempotently without consuming `self`.
    tx: Mutex<Option<Sender<BatchRequest>>>,
    /// Worker thread handle, taken by `close()` to join.
    thread: Mutex<Option<JoinHandle<()>>>,
    response_tx: ResponseTx,
    /// Static for the lifetime of the slot. Currently only used by the
    /// `non_streaming_routes_via_dispatch` test to verify routing
    /// without inferring it from a failure response.
    #[cfg_attr(not(test), allow(dead_code))]
    streaming_supported: bool,
    closed: AtomicBool,
    last_used_ms: AtomicU64,
    /// Submitted batches not yet responded to. Incremented before push
    /// to the worker thread's input queue; decremented by the worker
    /// thread *after* it pushes the response onto `response_tx`. Read
    /// by the registry's eviction sweeper.
    in_flight: Arc<AtomicU32>,
    tool_name: String,
}

impl InProcessSlot {
    /// Build a slot for `tool_name` with `config`.
    ///
    /// On success the slot has spawned its worker thread and is ready to
    /// accept `submit` calls. On failure (unknown tool, `create_streaming_context`
    /// errored), no thread or context is leaked — the caller surfaces the
    /// returned `Response` to the client and discards the slot.
    pub fn new(
        tool_name: &str,
        config: &serde_json::Value,
        response_tx: ResponseTx,
    ) -> Result<Self, Response> {
        let session = tools::streaming_setup(tool_name, config)?;
        let (ctx_opt, streaming_supported, schema_version) = match session {
            Some((c, sv)) => (Some(c), true, Some(sv)),
            None => (None, false, None),
        };

        let (tx, rx) = mpsc::channel::<BatchRequest>();
        let in_flight = Arc::new(AtomicU32::new(0));
        let worker_response_tx = response_tx.clone();
        let tool_name_for_thread = tool_name.to_string();
        let in_flight_for_thread = Arc::clone(&in_flight);
        let thread = thread::Builder::new()
            .name(format!("gb-inproc-{tool_name}"))
            .spawn(move || {
                worker_thread_main(
                    rx,
                    ctx_opt,
                    streaming_supported,
                    schema_version,
                    tool_name_for_thread,
                    worker_response_tx,
                    in_flight_for_thread,
                );
            })
            .expect("failed to spawn InProcessSlot worker thread");

        Ok(Self {
            tx: Mutex::new(Some(tx)),
            thread: Mutex::new(Some(thread)),
            response_tx,
            streaming_supported,
            closed: AtomicBool::new(false),
            last_used_ms: AtomicU64::new(now_epoch_ms()),
            in_flight,
            tool_name: tool_name.to_string(),
        })
    }
}

/// Worker-thread loop body. Owns the streaming context (if any) and exits
/// when the input channel is closed. Drops the context on exit so any C
/// `destroy` hook runs before the thread terminates.
///
/// The `in_flight` decrement happens *after* `response_tx.send` so the
/// registry's eviction sweeper, which checks `in_flight() == 0`, never
/// observes idle while a response is still in transit.
fn worker_thread_main(
    rx: mpsc::Receiver<BatchRequest>,
    mut ctx_opt: Option<Box<dyn StreamingContext>>,
    streaming_supported: bool,
    schema_version: Option<u32>,
    tool_name: String,
    response_tx: ResponseTx,
    in_flight: Arc<AtomicU32>,
) {
    while let Ok(batch) = rx.recv() {
        let mut response = if streaming_supported {
            match ctx_opt.as_mut() {
                Some(ctx) => {
                    let mut r = ctx.run_batch(&batch.shm_input);
                    if r.success {
                        r.schema_version = schema_version;
                    }
                    r
                }
                None => Response::error(format!(
                    "Worker for tool '{tool_name}' lost its streaming context unexpectedly"
                )),
            }
        } else {
            tools::dispatch(&batch)
        };
        response.batch_id = batch.batch_id;
        // If the coordinator is gone (channel closed) there's nothing useful
        // we can do with the response — drop it and exit. The cleanup
        // registry + miint's PID-staleness sweep handle any orphan shm.
        let send_result = response_tx.send(response);
        in_flight.fetch_sub(1, Ordering::AcqRel);
        if send_result.is_err() {
            break;
        }
    }
    // Drop the streaming context here so its C `destroy` runs on this
    // worker thread, not on whoever happens to drop the slot.
    drop(ctx_opt);
}

impl Worker for InProcessSlot {
    fn submit(&self, batch: BatchRequest) {
        if self.closed.load(Ordering::Acquire) {
            let mut r = Response::error(format!(
                "Worker for tool '{}' has been closed; submit a new batch \
                 to spawn a fresh worker",
                self.tool_name
            ));
            r.batch_id = batch.batch_id;
            let _ = self.response_tx.send(r);
            return;
        }

        // Snapshot the sender under the mutex; we don't hold the lock across
        // the send to avoid blocking close() on a slow consumer (the
        // worker thread). mpsc is unbounded so send never blocks anyway,
        // but the discipline matters for future bounded variants.
        let tx_opt = self
            .tx
            .lock()
            .expect("InProcessSlot tx mutex poisoned")
            .as_ref()
            .cloned();
        match tx_opt {
            Some(tx) => {
                self.last_used_ms.store(now_epoch_ms(), Ordering::Relaxed);
                // Increment in_flight *before* push so the eviction
                // sweeper never sees a window where the batch is queued
                // but in_flight is 0.
                self.in_flight.fetch_add(1, Ordering::AcqRel);
                let batch_id = batch.batch_id;
                if tx.send(batch).is_err() {
                    // Worker thread exited unexpectedly. Decrement to
                    // restore in_flight invariant, then surface error.
                    self.in_flight.fetch_sub(1, Ordering::AcqRel);
                    let mut r = Response::error(format!(
                        "Worker thread for tool '{}' is gone",
                        self.tool_name
                    ));
                    r.batch_id = batch_id;
                    let _ = self.response_tx.send(r);
                }
            }
            None => {
                // close() already ran. Honor the closed-slot contract.
                let mut r = Response::error(format!(
                    "Worker for tool '{}' has been closed",
                    self.tool_name
                ));
                r.batch_id = batch.batch_id;
                let _ = self.response_tx.send(r);
            }
        }
    }

    fn last_used(&self) -> Instant {
        // Anchor on `Instant::now()` minus elapsed wall-time. SystemTime
        // can step backward via NTP; saturating_sub clamps that case.
        let now_ms = now_epoch_ms();
        let last_ms = self.last_used_ms.load(Ordering::Relaxed);
        let elapsed_ms = now_ms.saturating_sub(last_ms);
        Instant::now() - std::time::Duration::from_millis(elapsed_ms)
    }

    fn close(&self) {
        // Mark closed first so any racing submit() bails out without queueing.
        self.closed.store(true, Ordering::Release);
        // Drop the input sender → worker thread sees recv() error after
        // draining queued batches → exits → drops streaming context (C destroy).
        if let Ok(mut guard) = self.tx.lock() {
            *guard = None;
        }
        if let Ok(mut handle) = self.thread.lock() {
            if let Some(h) = handle.take() {
                let _ = h.join();
            }
        }
    }

    fn in_flight(&self) -> u32 {
        self.in_flight.load(Ordering::Acquire)
    }

    fn kind(&self) -> WorkerKind {
        WorkerKind::InProcess
    }
}

// ---------------------------------------------------------------------------
// SubprocessWorker
// ---------------------------------------------------------------------------

/// Subprocess worker. Spawns `gpl-boundary --worker <tool>` and feeds it
/// batches over a pipe. The child runs the tool's streaming context against
/// its own copy of the shared-memory input, writes Arrow IPC output to a
/// fresh shm, and emits a `Response` line on stdout — the parent's reader
/// thread forwards each one onto the global `ResponseTx`.
///
/// Why subprocess: bowtie2 holds process-wide global state behind a mutex,
/// so two batches running in one process always serialize. Splitting one
/// process per fingerprint sidesteps that mutex while preserving the
/// loaded-index reuse benefit (each child loads its index exactly once).
///
/// Output shm names use the *child's* PID (`/gb-{child_pid}-...`), so
/// miint's PID-staleness sweep correctly attributes orphans on a child
/// crash.
pub struct SubprocessWorker {
    /// Pipe to the child's stdin. Wrapped so `close()` can drop it
    /// (sending EOF) idempotently. Locked across writes because Phase 4
    /// step 3 dispatches submits one at a time, but defensive against
    /// any future caller that submits concurrently from multiple threads.
    stdin: Mutex<Option<ChildStdin>>,
    /// Reader thread join handle.
    reader: Mutex<Option<JoinHandle<()>>>,
    /// Child handle.
    child: Mutex<Option<Child>>,
    /// PID of the child. Captured here for diagnostics (the post-crash
    /// orphan-shm sweep gets its own copy via the reader thread).
    #[allow(dead_code)]
    child_pid: u32,
    response_tx: ResponseTx,
    last_used_ms: AtomicU64,
    /// Set by `close()` to indicate parent-initiated graceful shutdown.
    /// Shared with the reader thread so it can distinguish "EOF because
    /// I closed stdin" from "EOF because the child crashed".
    closed: Arc<AtomicBool>,
    /// Set when the reader thread observes EOF on the child's stdout
    /// (child crashed or exited unexpectedly). Once true, future
    /// `submit` calls emit error responses without trying to write to
    /// the dead pipe. The plan deliberately does not auto-respawn —
    /// the user must restart gpl-boundary to recover the fingerprint.
    fingerprint_dead: Arc<AtomicBool>,
    /// In-submission-order list of batch_ids that have been written to
    /// the child's stdin but have not yet had a response read back.
    /// Pushed by `submit`, popped by the reader thread on each
    /// response. On a crash the reader drains the queue and emits error
    /// responses for every still-pending batch_id so the coordinator's
    /// drain loop can complete.
    in_flight_ids: Arc<Mutex<std::collections::VecDeque<Option<u64>>>>,
    /// Submitted batches not yet responded to. Incremented in `submit`
    /// before the stdin write; decremented by the reader thread *after*
    /// pushing the response onto `response_tx`. Read by the registry's
    /// eviction sweeper to enforce in-flight protection.
    in_flight: Arc<AtomicU32>,
    tool_name: String,
}

impl SubprocessWorker {
    /// Spawn a child gpl-boundary worker process for `tool_name` with
    /// `config`. The child reads `config` as the first line on its stdin,
    /// initializes its streaming context, then loops reading
    /// `BatchRequest` lines until EOF.
    ///
    /// Returns `Err(response)` if the spawn itself fails or the child
    /// fails to acknowledge init. Step 5 will add explicit init-handshake
    /// surfacing; step 3 lets dispatch errors travel through the normal
    /// response path.
    pub fn new(
        tool_name: &str,
        config: &serde_json::Value,
        response_tx: ResponseTx,
    ) -> Result<Self, Response> {
        let exe = std::env::current_exe().map_err(|e| {
            Response::error(format!("Failed to locate gpl-boundary executable: {e}"))
        })?;

        let mut child = Command::new(&exe)
            .arg("--worker")
            .arg(tool_name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| Response::error(format!("Failed to spawn worker '{tool_name}': {e}")))?;

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| Response::error("Failed to capture worker stdin pipe".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Response::error("Failed to capture worker stdout pipe".to_string()))?;

        // Send config on the first line so the child can build its context
        // before any batch arrives.
        let config_line = format!(
            "{}\n",
            serde_json::to_string(config).unwrap_or_else(|_| "{}".to_string())
        );
        stdin
            .write_all(config_line.as_bytes())
            .map_err(|e| Response::error(format!("Failed to send config to worker: {e}")))?;
        stdin
            .flush()
            .map_err(|e| Response::error(format!("Failed to flush config: {e}")))?;

        let child_pid = child.id();
        let tool_name_owned = tool_name.to_string();
        let reader_response_tx = response_tx.clone();
        let in_flight = Arc::new(AtomicU32::new(0));
        let in_flight_for_thread = Arc::clone(&in_flight);
        let in_flight_ids: Arc<Mutex<std::collections::VecDeque<Option<u64>>>> =
            Arc::new(Mutex::new(std::collections::VecDeque::new()));
        let in_flight_ids_for_thread = Arc::clone(&in_flight_ids);
        let fingerprint_dead = Arc::new(AtomicBool::new(false));
        let fingerprint_dead_for_thread = Arc::clone(&fingerprint_dead);
        let closed = Arc::new(AtomicBool::new(false));
        let closed_for_thread = Arc::clone(&closed);
        let reader = thread::Builder::new()
            .name(format!("gb-sub-rd-{tool_name}"))
            .spawn(move || {
                subprocess_reader_main(
                    stdout,
                    reader_response_tx,
                    tool_name_owned,
                    in_flight_for_thread,
                    in_flight_ids_for_thread,
                    fingerprint_dead_for_thread,
                    closed_for_thread,
                    child_pid,
                );
            })
            .expect("failed to spawn SubprocessWorker reader thread");

        Ok(Self {
            stdin: Mutex::new(Some(stdin)),
            reader: Mutex::new(Some(reader)),
            child: Mutex::new(Some(child)),
            child_pid,
            response_tx,
            last_used_ms: AtomicU64::new(now_epoch_ms()),
            closed,
            fingerprint_dead,
            in_flight_ids,
            in_flight,
            tool_name: tool_name.to_string(),
        })
    }
}

/// Reader thread for a `SubprocessWorker`: parses NDJSON `Response` lines
/// from the child's stdout and forwards them to the registry's
/// `ResponseTx`. Exits on EOF.
///
/// On normal shutdown (parent dropped the child's stdin) the loop sees
/// EOF after the child finished its last batch, so `in_flight_ids` is
/// empty and we just exit. On a crash the child dies mid-batch — the
/// loop sees EOF with `in_flight_ids` non-empty. We drain that queue,
/// emit an error response for every batch_id that will never get a
/// real response, and best-effort sweep the child's orphan shm so
/// miint's PID-staleness sweep has less to do.
///
/// `in_flight` is decremented *after* pushing the response, mirroring
/// `worker_thread_main` — the registry's eviction sweeper must never
/// observe `in_flight == 0` while a response is still in transit.
#[allow(clippy::too_many_arguments)]
fn subprocess_reader_main(
    stdout: std::process::ChildStdout,
    response_tx: ResponseTx,
    tool_name: String,
    in_flight: Arc<AtomicU32>,
    in_flight_ids: Arc<Mutex<std::collections::VecDeque<Option<u64>>>>,
    fingerprint_dead: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    child_pid: u32,
) {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break, // EOF — child exited
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let resp = serde_json::from_str::<Response>(trimmed).unwrap_or_else(|e| {
                    Response::error(format!(
                        "Worker '{tool_name}' produced malformed response: {e}"
                    ))
                });
                // Pop the matching batch_id from the front of the
                // in-flight tracker. The child processes batches
                // serially so responses arrive in submission order;
                // pop_front matches the response to its sender.
                let _ = in_flight_ids
                    .lock()
                    .expect("in_flight_ids mutex")
                    .pop_front();
                let send_result = response_tx.send(resp);
                in_flight.fetch_sub(1, Ordering::AcqRel);
                if send_result.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    // EOF or read error. Distinguish a parent-initiated graceful
    // shutdown (parent dropped stdin, set `closed`) from a real crash
    // (child died of its own accord with `closed == false`):
    //
    // - Graceful: nothing to do. Any pending batch_ids in flight will
    //   be drained by the coordinator's session-end drain loop; the
    //   parent's close() blocks on this thread's join.
    // - Crash: mark fingerprint_dead so future submits fail fast,
    //   emit error responses for every still-pending batch_id (the
    //   coordinator's drain loop counts on these landing), and sweep
    //   the child's orphan shm.
    let was_closed_by_parent = closed.load(Ordering::Acquire);
    let pending_ids: Vec<Option<u64>> = {
        let mut q = in_flight_ids.lock().expect("in_flight_ids mutex");
        q.drain(..).collect()
    };
    if !was_closed_by_parent {
        fingerprint_dead.store(true, Ordering::Release);
        for batch_id in pending_ids {
            let mut r = Response::error(format!(
                "Worker '{tool_name}' subprocess (pid {child_pid}) exited \
                 unexpectedly mid-batch; restart gpl-boundary to recover \
                 this fingerprint"
            ));
            r.batch_id = batch_id;
            let _ = response_tx.send(r);
            in_flight.fetch_sub(1, Ordering::AcqRel);
        }
        sweep_orphan_shm_for_pid(child_pid);
    }
}

/// Best-effort orphan-shm sweep for a dead worker subprocess. On Linux,
/// POSIX shm names map onto `/dev/shm/<name>` files; we readdir the
/// directory and unlink anything matching `gb-{child_pid}-*`. macOS
/// has no analogous filesystem view of POSIX shm, so we skip — miint's
/// PID-staleness sweep is the remaining safety net there.
fn sweep_orphan_shm_for_pid(child_pid: u32) {
    #[cfg(target_os = "linux")]
    {
        let prefix = format!("gb-{child_pid}-");
        let entries = match std::fs::read_dir("/dev/shm") {
            Ok(d) => d,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let name = match entry.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            if name.starts_with(&prefix) {
                if let Ok(c) = std::ffi::CString::new(format!("/{name}")) {
                    unsafe {
                        libc::shm_unlink(c.as_ptr());
                    }
                }
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = child_pid;
    }
}

impl Worker for SubprocessWorker {
    fn submit(&self, batch: BatchRequest) {
        if self.closed.load(Ordering::Acquire) {
            let mut r = Response::error(format!(
                "Worker for tool '{}' has been closed",
                self.tool_name
            ));
            r.batch_id = batch.batch_id;
            let _ = self.response_tx.send(r);
            return;
        }

        if self.fingerprint_dead.load(Ordering::Acquire) {
            // The reader thread observed the child crash. Per the plan,
            // we don't auto-respawn — the user must restart the daemon
            // to recover this fingerprint. Fail every subsequent submit
            // fast.
            let mut r = Response::error(format!(
                "Worker for tool '{}' is dead (subprocess crashed); \
                 restart gpl-boundary to recover this fingerprint",
                self.tool_name
            ));
            r.batch_id = batch.batch_id;
            let _ = self.response_tx.send(r);
            return;
        }

        let line = match serde_json::to_string(&batch) {
            Ok(s) => s,
            Err(e) => {
                let mut r = Response::error(format!("Failed to serialize batch: {e}"));
                r.batch_id = batch.batch_id;
                let _ = self.response_tx.send(r);
                return;
            }
        };

        self.last_used_ms.store(now_epoch_ms(), Ordering::Relaxed);
        // Order of bookkeeping: increment in_flight, push the batch_id
        // for the reader thread to match against, then write to stdin.
        // If the write fails, undo both. This ordering means the
        // reader thread always finds the batch_id in the queue when a
        // response (or a crash drain) processes it.
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        let batch_id = batch.batch_id;
        self.in_flight_ids
            .lock()
            .expect("in_flight_ids mutex")
            .push_back(batch_id);

        let send_result = {
            let mut guard = self
                .stdin
                .lock()
                .expect("SubprocessWorker stdin mutex poisoned");
            match guard.as_mut() {
                Some(stdin) => stdin
                    .write_all(line.as_bytes())
                    .and_then(|_| stdin.write_all(b"\n"))
                    .and_then(|_| stdin.flush()),
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "stdin already closed",
                )),
            }
        };

        if let Err(e) = send_result {
            // Roll back: pop the batch_id we just pushed (it's at the
            // back, not the front, so we use pop_back). The reader
            // thread will never see a response for this batch since
            // the write failed.
            self.in_flight_ids
                .lock()
                .expect("in_flight_ids mutex")
                .pop_back();
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            let mut r = Response::error(format!(
                "Failed to send batch to worker '{}': {e}",
                self.tool_name
            ));
            r.batch_id = batch_id;
            let _ = self.response_tx.send(r);
        }
    }

    fn last_used(&self) -> Instant {
        let now_ms = now_epoch_ms();
        let last_ms = self.last_used_ms.load(Ordering::Relaxed);
        let elapsed_ms = now_ms.saturating_sub(last_ms);
        Instant::now() - std::time::Duration::from_millis(elapsed_ms)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);

        // Drop stdin → child sees EOF → exits gracefully (running C destroy
        // via Drop). Idempotent.
        if let Ok(mut guard) = self.stdin.lock() {
            *guard = None;
        }

        // Reap the child first with a kill-on-timeout. A wedged child
        // (bowtie2 hang, deadlock in user code) would otherwise block
        // the reader thread's `read_line` forever — `close_all` calls
        // us, the parent's drain phase already gave the worker its
        // 60-second drain budget upstream, so anything still alive at
        // this point is misbehaving and gets the hammer. Polling
        // try_wait + sleep keeps the implementation portable
        // (no SIGCHLD handler) at the cost of the polling interval.
        const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);
        if let Ok(mut child_guard) = self.child.lock() {
            if let Some(mut child) = child_guard.take() {
                let deadline = std::time::Instant::now() + SHUTDOWN_GRACE;
                loop {
                    match child.try_wait() {
                        Ok(Some(_)) => break, // exited cleanly
                        Ok(None) => {
                            if std::time::Instant::now() >= deadline {
                                let _ = child.kill();
                                let _ = child.wait();
                                break;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                        Err(_) => {
                            let _ = child.kill();
                            let _ = child.wait();
                            break;
                        }
                    }
                }
            }
        }

        // Now the child is gone (gracefully or via kill), the reader
        // thread's `read_line` will see EOF and exit.
        if let Ok(mut handle) = self.reader.lock() {
            if let Some(h) = handle.take() {
                let _ = h.join();
            }
        }
    }

    fn in_flight(&self) -> u32 {
        self.in_flight.load(Ordering::Acquire)
    }

    fn kind(&self) -> WorkerKind {
        WorkerKind::Subprocess
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_response_channel() -> (ResponseTx, mpsc::Receiver<Response>) {
        mpsc::channel()
    }

    #[test]
    fn test_in_process_slot_unknown_tool_errors() {
        let (tx, _rx) = make_response_channel();
        let result = InProcessSlot::new("not-a-real-tool", &serde_json::json!({}), tx);
        match result {
            Ok(_) => panic!("Expected unknown-tool error"),
            Err(resp) => {
                assert!(!resp.success);
                assert!(resp.error.as_ref().unwrap().contains("Unknown tool"));
            }
        }
    }

    #[test]
    fn test_in_process_slot_non_streaming_routes_via_dispatch() {
        // FastTree's `create_streaming_context` returns `Ok(None)`, so the
        // slot's worker thread runs `tools::dispatch` per batch. We verify
        // by submitting a batch with a bogus shm and observing the dispatch
        // error response — plus checking `streaming_supported` directly.
        let (tx, rx) = make_response_channel();
        let slot = InProcessSlot::new(
            "fasttree",
            &serde_json::json!({"seq_type": "nucleotide"}),
            tx,
        )
        .unwrap();
        assert!(!slot.streaming_supported);

        slot.submit(BatchRequest {
            tool: "fasttree".to_string(),
            config: serde_json::json!({"seq_type": "nucleotide"}),
            shm_input: "/gb-does-not-exist".to_string(),
            batch_id: Some(99),
        });

        let response = rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("Worker should produce a response");
        assert!(!response.success);
        assert_eq!(response.batch_id, Some(99));

        slot.close();
    }

    #[test]
    fn test_in_process_slot_close_is_idempotent() {
        let (tx, _rx) = make_response_channel();
        let slot = InProcessSlot::new(
            "fasttree",
            &serde_json::json!({"seq_type": "nucleotide"}),
            tx,
        )
        .unwrap();
        slot.close();
        slot.close(); // must not panic or deadlock
    }

    #[test]
    fn test_in_process_slot_submit_after_close_errors() {
        let (tx, rx) = make_response_channel();
        let slot = InProcessSlot::new("prodigal", &serde_json::json!({"meta_mode": true}), tx)
            .expect("Prodigal meta_mode streaming-context init should succeed");
        slot.close();
        slot.submit(BatchRequest {
            tool: "prodigal".to_string(),
            config: serde_json::json!({"meta_mode": true}),
            shm_input: "/gb-does-not-exist".to_string(),
            batch_id: Some(7),
        });
        let response = rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("submit-after-close must still emit a response");
        assert!(!response.success);
        assert_eq!(response.batch_id, Some(7));
        assert!(
            response
                .error
                .as_ref()
                .map(|e| e.contains("closed"))
                .unwrap_or(false),
            "Expected 'closed' error, got: {:?}",
            response.error
        );
    }
}
