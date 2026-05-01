//! Per-fingerprint worker registry.
//!
//! The registry maps `(tool, canonical_config)` fingerprints to long-lived
//! `Worker`s. The coordinator's batch-handling loop calls
//! `Registry::submit(batch)` for every batch; the registry creates a worker
//! on first sight of a fingerprint and reuses it for every subsequent batch
//! with the same fingerprint.
//!
//! Routing per tool:
//! - `bowtie2-align` → `SubprocessWorker` (sidesteps bowtie2's process-wide
//!   alignment mutex; distinct fingerprints align in parallel because they
//!   live in distinct child processes).
//! - everything else → `InProcessSlot` (FastTree/Prodigal/SortMeRNA).
//!
//! ## Eviction (step 4)
//!
//! Two eviction triggers:
//! 1. **Idle deadline** — any worker whose `last_used + worker_idle_ms <
//!    now` is evicted on the next sweeper tick. Required for bowtie2 since
//!    a loaded index can be hundreds of MB; we cannot let an idle
//!    fingerprint pin that memory indefinitely.
//! 2. **Budget pressure** — when a *new* subprocess fingerprint arrives
//!    and the count of live subprocess workers is already at
//!    `max_workers`, the registry tries to evict the LRU idle subprocess
//!    worker. If none are idle (every existing worker has an in-flight
//!    batch), the new submit is queued into `pending` and dispatched
//!    later when a slot frees up.
//!
//! In-process workers are not subject to the budget cap (their resource
//! footprint is small), but they still respect the idle deadline.
//!
//! The sweeper thread ticks every 100ms. It also runs on demand when
//! the response forwarder calls `notify_response_received` after a
//! response lands — this keeps queued submits responsive to a freed slot
//! without paying the full tick latency.
//!
//! ## Fingerprint
//!
//! A fingerprint is the tool name plus the canonical (recursively
//! key-sorted) JSON serialization of the config. Two configs that differ
//! only in JSON key ordering produce identical fingerprints.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::protocol::{BatchRequest, Response};
use crate::workers::{InProcessSlot, ResponseTx, SubprocessWorker, Worker, WorkerKind};

/// Knobs that control eviction. Defaults match the plan.
///
/// **Note on `max_workers`:** this is a soft limit, not a hard one.
/// Concurrent submitters (the coordinator's batch handler and the
/// sweeper's `try_flush_pending`) each consult `subprocess_count()`
/// before building a new worker, but the build happens outside the
/// workers mutex (intentional — bowtie2's child spawn can take 10s of
/// ms and would otherwise block other dispatches). A second submitter
/// can pass the budget check between the first's check and the first's
/// insert. The transient overrun is bounded by the number of
/// concurrent submitters (today 2), so in practice you may briefly
/// see `max_workers + 1` resident subprocess workers. The next
/// dispatch trims back via the standard eviction path.
#[derive(Debug, Clone, Copy)]
pub struct EvictionConfig {
    /// Maximum number of *subprocess* workers concurrently resident.
    /// In-process workers don't count (cheap to keep around). Soft
    /// limit — see struct-level docs.
    pub max_workers: usize,
    /// Maximum number of *subprocess* workers idle at a time. The sweeper
    /// evicts down to this on every tick when more than this many idle
    /// subprocesses are sitting around. Tracked separately from
    /// `max_workers` so a long burst of work followed by idleness ends
    /// with the right amount of warm capacity.
    pub max_idle_workers: usize,
    /// Per-worker idle deadline. A worker with `last_used + this < now`
    /// AND `in_flight == 0` is evicted.
    pub worker_idle_ms: u64,
    /// Sweeper tick interval. The plan calls for 30s in production, but
    /// the responsiveness of pending dispatch on a freed slot depends
    /// on this — lower in tests via the env var
    /// `GPL_BOUNDARY_SWEEPER_TICK_MS`. The forwarder also nudges the
    /// sweeper on every response, so the tick is mostly a safety net.
    pub sweeper_tick_ms: u64,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        let sweeper_tick_ms = std::env::var("GPL_BOUNDARY_SWEEPER_TICK_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(100);
        Self {
            max_workers: 4,
            max_idle_workers: 4,
            worker_idle_ms: 300_000,
            sweeper_tick_ms,
        }
    }
}

/// `(tool_name, canonical_config_string)`. Two configs that differ only
/// in JSON key ordering produce identical fingerprints.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Fingerprint {
    pub tool: String,
    pub canonical_config: String,
}

impl Fingerprint {
    pub fn from_batch(batch: &BatchRequest) -> Self {
        Self {
            tool: batch.tool.clone(),
            canonical_config: canonical_json(&batch.config),
        }
    }
}

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

/// Coordinator-owned worker map. Wrapped in `Arc` so the sweeper thread
/// and the response forwarder can hold references.
pub struct Registry {
    workers: Mutex<HashMap<Fingerprint, Arc<dyn Worker>>>,
    pending: Mutex<VecDeque<BatchRequest>>,
    /// Notifies the sweeper to wake up early (e.g. on response arrival).
    sweeper_cv: Condvar,
    /// Sweeper-state mutex — the sweeper holds this while sleeping on
    /// `sweeper_cv`. We don't store data inside; the lock just gives
    /// `Condvar::wait_timeout` something to bind to.
    sweeper_lock: Mutex<()>,
    sweeper_shutdown: AtomicBool,
    sweeper_handle: Mutex<Option<JoinHandle<()>>>,
    response_tx: ResponseTx,
    config: EvictionConfig,
}

impl Registry {
    pub fn new(response_tx: ResponseTx, config: EvictionConfig) -> Arc<Self> {
        let r = Arc::new(Self {
            workers: Mutex::new(HashMap::new()),
            pending: Mutex::new(VecDeque::new()),
            sweeper_cv: Condvar::new(),
            sweeper_lock: Mutex::new(()),
            sweeper_shutdown: AtomicBool::new(false),
            sweeper_handle: Mutex::new(None),
            response_tx,
            config,
        });

        let r_for_thread = Arc::clone(&r);
        let handle = thread::Builder::new()
            .name("gb-sweeper".to_string())
            .spawn(move || sweeper_loop(r_for_thread))
            .expect("failed to spawn registry sweeper thread");
        *r.sweeper_handle.lock().unwrap() = Some(handle);
        r
    }

    /// Get-or-create the worker for `batch.fingerprint()` and submit. The
    /// over-budget path queues the batch for later dispatch when a slot
    /// frees up.
    pub fn submit(&self, batch: BatchRequest) {
        let fp = Fingerprint::from_batch(&batch);

        // Fast path: existing worker.
        if let Some(existing) = self.lookup(&fp) {
            existing.submit(batch);
            return;
        }

        // Proactive idle-eviction before checking budget — this gives us
        // the most chances to fit a new fingerprint without queuing.
        self.evict_expired_idle();

        let routes_to_subprocess = batch.tool == "bowtie2-align";
        if routes_to_subprocess && self.subprocess_count() >= self.config.max_workers {
            // Try to evict an idle subprocess worker (LRU among idle).
            if !self.evict_lru_idle_subprocess() {
                // No idle worker available; queue. The sweeper or the
                // forwarder's response notification will retry later.
                self.pending.lock().expect("pending mutex").push_back(batch);
                return;
            }
        }

        // Budget OK — build the worker. Construction happens *outside*
        // the workers mutex so a slow init (SortMeRNA reference indexing,
        // bowtie2 child spawn) doesn't block other dispatches.
        let new_worker = match self.build_worker(&batch.tool, &batch.config) {
            Ok(w) => w,
            Err(mut response) => {
                response.batch_id = batch.batch_id;
                let _ = self.response_tx.send(response);
                return;
            }
        };

        // Insert with a peer-check race window: if a concurrent submit
        // built a worker for the same fingerprint while we were
        // constructing, drop ours and use theirs. Today the coordinator
        // is single-threaded so this branch is academic, but it keeps
        // the contract honest for any future concurrent submitter.
        let worker_to_use: Arc<dyn Worker> = {
            let mut map = self.workers.lock().expect("registry workers mutex");
            if let Some(peer) = map.get(&fp).map(Arc::clone) {
                drop(map);
                new_worker.close();
                peer
            } else {
                map.insert(fp, Arc::clone(&new_worker));
                new_worker
            }
        };
        worker_to_use.submit(batch);
    }

    fn lookup(&self, fp: &Fingerprint) -> Option<Arc<dyn Worker>> {
        self.workers
            .lock()
            .expect("registry workers mutex")
            .get(fp)
            .map(Arc::clone)
    }

    fn build_worker(
        &self,
        tool_name: &str,
        config: &serde_json::Value,
    ) -> Result<Arc<dyn Worker>, Response> {
        if tool_name == "bowtie2-align" {
            let w = SubprocessWorker::new(tool_name, config, self.response_tx.clone())?;
            Ok(Arc::new(w))
        } else {
            let w = InProcessSlot::new(tool_name, config, self.response_tx.clone())?;
            Ok(Arc::new(w))
        }
    }

    /// Count workers of `WorkerKind::Subprocess`. Cheap iteration over a
    /// HashMap of typically <= max_workers entries.
    fn subprocess_count(&self) -> usize {
        self.workers
            .lock()
            .expect("registry workers mutex")
            .values()
            .filter(|w| w.kind() == WorkerKind::Subprocess)
            .count()
    }

    /// Evict every worker whose idle deadline has passed AND has no
    /// in-flight batch. Returns the number evicted.
    fn evict_expired_idle(&self) -> usize {
        let deadline = Duration::from_millis(self.config.worker_idle_ms);
        let now = Instant::now();
        let candidates: Vec<Fingerprint> = {
            let map = self.workers.lock().expect("registry workers mutex");
            map.iter()
                .filter(|(_, w)| {
                    w.in_flight() == 0 && now.duration_since(w.last_used()) >= deadline
                })
                .map(|(fp, _)| fp.clone())
                .collect()
        };
        let mut count = 0;
        for fp in candidates {
            if self.evict_one(&fp) {
                count += 1;
            }
        }
        count
    }

    /// Evict the LRU idle subprocess worker. Returns true on success.
    fn evict_lru_idle_subprocess(&self) -> bool {
        let candidate: Option<Fingerprint> = {
            let map = self.workers.lock().expect("registry workers mutex");
            map.iter()
                .filter(|(_, w)| w.kind() == WorkerKind::Subprocess && w.in_flight() == 0)
                .min_by_key(|(_, w)| w.last_used())
                .map(|(fp, _)| fp.clone())
        };
        match candidate {
            Some(fp) => self.evict_one(&fp),
            None => false,
        }
    }

    /// Trim idle subprocess workers down to `max_idle_workers`. Picks the
    /// LRU among idle subprocess workers each iteration.
    fn evict_excess_idle(&self) {
        loop {
            let (idle_count, victim): (usize, Option<Fingerprint>) = {
                let map = self.workers.lock().expect("registry workers mutex");
                let idle: Vec<(&Fingerprint, &Arc<dyn Worker>)> = map
                    .iter()
                    .filter(|(_, w)| w.kind() == WorkerKind::Subprocess && w.in_flight() == 0)
                    .collect();
                let count = idle.len();
                let lru = idle
                    .into_iter()
                    .min_by_key(|(_, w)| w.last_used())
                    .map(|(fp, _)| fp.clone());
                (count, lru)
            };
            if idle_count <= self.config.max_idle_workers {
                return;
            }
            match victim {
                Some(fp) => {
                    if !self.evict_one(&fp) {
                        return;
                    }
                }
                None => return,
            }
        }
    }

    fn evict_one(&self, fp: &Fingerprint) -> bool {
        let removed = {
            let mut map = self.workers.lock().expect("registry workers mutex");
            map.remove(fp)
        };
        if let Some(worker) = removed {
            // Recheck in_flight under the now-released map lock — we
            // dropped the lock between the candidate scan and here, so
            // a concurrent submit could have raced. Skip if a batch
            // arrived in the meantime; reinsert.
            if worker.in_flight() > 0 {
                let mut map = self.workers.lock().expect("registry workers mutex");
                map.insert(fp.clone(), worker);
                return false;
            }
            worker.close();
            true
        } else {
            false
        }
    }

    /// Try to dispatch as many pending batches as possible. Stops when
    /// the head of the queue can't be dispatched.
    ///
    /// Important: we never call back into `submit()` here, because
    /// `submit()` re-queues on budget pressure — combined with this
    /// loop that would spin forever if `evict_one`'s in-flight re-check
    /// undid an eviction we relied on (`evict_lru_idle_subprocess`
    /// reports "freed a slot" optimistically; a concurrent `submit`
    /// could race the slot back into use). Instead we build and insert
    /// the worker directly. If the build fails (invalid config, child
    /// spawn error, etc.) we surface an error response and move on so
    /// pending stays bounded.
    fn try_flush_pending(self: &Arc<Self>) {
        loop {
            let head = {
                let pending = self.pending.lock().expect("pending mutex");
                pending.front().cloned()
            };
            let batch = match head {
                Some(b) => b,
                None => return,
            };
            let fp = Fingerprint::from_batch(&batch);

            // Existing worker for this fingerprint? Dispatch immediately.
            if let Some(existing) = self.lookup(&fp) {
                self.pending.lock().expect("pending mutex").pop_front();
                existing.submit(batch);
                continue;
            }

            // Need to build. Check budget.
            let routes_to_subprocess = batch.tool == "bowtie2-align";
            if routes_to_subprocess
                && self.subprocess_count() >= self.config.max_workers
                && !self.evict_lru_idle_subprocess()
            {
                return; // can't dispatch right now
            }

            // Pop the batch and dispatch directly — bypassing submit()
            // so we never re-queue and risk an infinite loop with the
            // sweeper undoing eviction decisions mid-flush.
            self.pending.lock().expect("pending mutex").pop_front();

            let worker_arc: Arc<dyn Worker> = match self.build_worker(&batch.tool, &batch.config) {
                Ok(w) => w,
                Err(mut response) => {
                    response.batch_id = batch.batch_id;
                    let _ = self.response_tx.send(response);
                    continue;
                }
            };

            // Peer-check race: a concurrent `submit` may have inserted
            // the same fingerprint between our lookup and now. Honor
            // the peer and discard our duplicate.
            let dispatch_target: Arc<dyn Worker> = {
                let mut map = self.workers.lock().expect("registry workers mutex");
                if let Some(peer) = map.get(&fp).map(Arc::clone) {
                    drop(map);
                    worker_arc.close();
                    peer
                } else {
                    map.insert(fp, Arc::clone(&worker_arc));
                    worker_arc
                }
            };
            dispatch_target.submit(batch);
        }
    }

    /// Wakes the sweeper out of its tick wait. Called when something
    /// happens that the sweeper might want to react to immediately
    /// (responses landing, new submits queueing). Currently called only
    /// from `close_all`; step 5 will hook the response forwarder so
    /// pending dispatch latency drops below the tick interval.
    #[allow(dead_code)]
    pub fn notify_response_received(self: &Arc<Self>) {
        self.sweeper_cv.notify_all();
    }

    /// Close every worker and stop the sweeper. Blocking — drains queued
    /// batches, runs C `destroy` hooks (or reaps subprocesses), joins
    /// worker threads. After this returns, no further responses will be
    /// sent on the `ResponseTx`. Pending batches that never got
    /// dispatched receive an error response so the coordinator's
    /// response counters stay consistent.
    pub fn close_all(&self) {
        // Stop the sweeper first to avoid races where it's evicting
        // while we're draining.
        self.sweeper_shutdown.store(true, Ordering::Release);
        self.sweeper_cv.notify_all();
        if let Ok(mut handle_guard) = self.sweeper_handle.lock() {
            if let Some(h) = handle_guard.take() {
                let _ = h.join();
            }
        }

        // Fail any pending batches that never dispatched. The
        // coordinator counts each Response so leaving these silent
        // would make the drain loop wait forever for "missing"
        // responses.
        let pending: Vec<BatchRequest> = {
            let mut pending = self.pending.lock().expect("pending mutex");
            pending.drain(..).collect()
        };
        for batch in pending {
            let mut r = Response::error(format!(
                "Pending batch for tool '{}' was not dispatched before shutdown",
                batch.tool
            ));
            r.batch_id = batch.batch_id;
            let _ = self.response_tx.send(r);
        }

        let workers = std::mem::take(&mut *self.workers.lock().expect("registry workers mutex"));
        for (_, w) in workers {
            w.close();
        }
    }

    /// Sum of in_flight counters across all live workers. Used by the
    /// coordinator's session-end drain loop to know when every batch
    /// has emitted a response.
    pub fn in_flight_total(&self) -> u32 {
        self.workers
            .lock()
            .expect("registry workers mutex")
            .values()
            .map(|w| w.in_flight())
            .sum()
    }

    /// Number of batches queued but not yet dispatched. Used by the
    /// coordinator's session-end drain loop together with
    /// `in_flight_total` — both must reach zero before close_all runs,
    /// otherwise queued batches would be cancelled with errors.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().expect("pending mutex").len()
    }

    /// Test-only: how many fingerprints have a live worker.
    #[cfg(test)]
    pub fn worker_count(&self) -> usize {
        self.workers.lock().unwrap().len()
    }

    /// Test-only: how many subprocess workers are currently resident.
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn subprocess_count_for_test(&self) -> usize {
        self.subprocess_count()
    }
}

/// Background sweeper. Wakes every `sweeper_tick_ms` (or sooner via the
/// Condvar when a response arrives) to evict expired idle workers and
/// flush pending submits.
fn sweeper_loop(registry: Arc<Registry>) {
    let tick = Duration::from_millis(registry.config.sweeper_tick_ms);
    while !registry.sweeper_shutdown.load(Ordering::Acquire) {
        registry.evict_expired_idle();
        registry.evict_excess_idle();
        registry.try_flush_pending();

        let lock = registry.sweeper_lock.lock().expect("sweeper lock");
        let _ = registry.sweeper_cv.wait_timeout(lock, tick);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn make_response_channel() -> (ResponseTx, mpsc::Receiver<Response>) {
        mpsc::channel()
    }

    fn fast_eviction_config(idle_ms: u64) -> EvictionConfig {
        EvictionConfig {
            max_workers: 4,
            max_idle_workers: 4,
            worker_idle_ms: idle_ms,
            sweeper_tick_ms: 25,
        }
    }

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
        assert_ne!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn test_fingerprint_equal_for_reordered_keys() {
        let b1 = BatchRequest {
            tool: "prodigal".to_string(),
            config: serde_json::json!({"meta_mode": true, "trans_table": 11}),
            shm_input: "/x".to_string(),
            shm_input_size: 0,
            batch_id: None,
        };
        let b2 = BatchRequest {
            tool: "prodigal".to_string(),
            config: serde_json::json!({"trans_table": 11, "meta_mode": true}),
            shm_input: "/x".to_string(),
            shm_input_size: 0,
            batch_id: None,
        };
        assert_eq!(Fingerprint::from_batch(&b1), Fingerprint::from_batch(&b2));
    }

    #[test]
    fn test_registry_caches_worker_per_fingerprint() {
        let (tx, rx) = make_response_channel();
        let reg = Registry::new(tx, fast_eviction_config(60_000));
        let b = |id: u64, shm: &str| BatchRequest {
            tool: "fasttree".to_string(),
            config: serde_json::json!({"seq_type": "nucleotide"}),
            shm_input: shm.to_string(),
            shm_input_size: 0,
            batch_id: Some(id),
        };
        reg.submit(b(1, "/gb-does-not-exist-1"));
        reg.submit(b(2, "/gb-does-not-exist-2"));
        for _ in 0..2 {
            let _ = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        assert_eq!(reg.worker_count(), 1);
        reg.close_all();
    }

    #[test]
    fn test_registry_distinct_fingerprints_get_distinct_workers() {
        let (tx, rx) = make_response_channel();
        let reg = Registry::new(tx, fast_eviction_config(60_000));
        reg.submit(BatchRequest {
            tool: "fasttree".to_string(),
            config: serde_json::json!({"seq_type": "nucleotide"}),
            shm_input: "/gb-does-not-exist-1".to_string(),
            shm_input_size: 0,
            batch_id: Some(1),
        });
        reg.submit(BatchRequest {
            tool: "fasttree".to_string(),
            config: serde_json::json!({"seq_type": "protein"}),
            shm_input: "/gb-does-not-exist-2".to_string(),
            shm_input_size: 0,
            batch_id: Some(2),
        });
        for _ in 0..2 {
            let _ = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        assert_eq!(reg.worker_count(), 2);
        reg.close_all();
    }

    #[test]
    fn test_registry_unknown_tool_does_not_create_worker() {
        let (tx, rx) = make_response_channel();
        let reg = Registry::new(tx, fast_eviction_config(60_000));
        reg.submit(BatchRequest {
            tool: "not-a-real-tool".to_string(),
            config: serde_json::json!({}),
            shm_input: "/x".to_string(),
            shm_input_size: 0,
            batch_id: Some(1),
        });
        let response = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(!response.success);
        assert_eq!(response.batch_id, Some(1));
        assert_eq!(reg.worker_count(), 0);
        reg.close_all();
    }

    #[test]
    fn test_registry_idle_eviction_in_process() {
        // Idle deadline 50ms, sweeper tick 25ms. Run a batch, wait
        // 200ms, verify the worker was evicted.
        let (tx, rx) = make_response_channel();
        let reg = Registry::new(tx, fast_eviction_config(50));
        reg.submit(BatchRequest {
            tool: "fasttree".to_string(),
            config: serde_json::json!({"seq_type": "nucleotide"}),
            shm_input: "/gb-idle-evict-test".to_string(),
            shm_input_size: 0,
            batch_id: Some(1),
        });
        let _ = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(reg.worker_count(), 1);
        thread::sleep(Duration::from_millis(200));
        assert_eq!(
            reg.worker_count(),
            0,
            "Worker should have been evicted after the idle deadline"
        );
        reg.close_all();
    }
}
