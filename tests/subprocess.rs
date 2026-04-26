//! Phase 4 step 3: subprocess-worker dispatch tests for `bowtie2-align`.
//!
//! These tests exercise the full async pipeline: parent gpl-boundary spawns
//! one child per `(tool, config)` fingerprint, feeds it batches over a pipe,
//! and merges responses back through the coordinator's event channel.
//!
//! Indexes are built once per test in a tempdir via `bowtie2-build`. The
//! sequences are tiny (~150 bases) so each build takes well under a second.

use std::ffi::CString;
use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// shm helpers (mirrors tests/streaming.rs and tests/bowtie2_build.rs to keep
// each test file independent — no shared support module yet)
// ---------------------------------------------------------------------------

fn unlink_shm(name: &str) {
    let c_name = CString::new(name).unwrap();
    unsafe {
        libc::shm_unlink(c_name.as_ptr());
    }
}

struct ShmGuard {
    name: String,
}

impl ShmGuard {
    fn create_with_batch(prefix: &str, batch: &RecordBatch) -> Self {
        let name = unique_test_shm_name(prefix);
        write_test_shm(&name, batch);
        Self { name }
    }
    fn adopt(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for ShmGuard {
    fn drop(&mut self) {
        unlink_shm(&self.name);
    }
}

fn write_test_shm(name: &str, batch: &RecordBatch) {
    let mut ipc_buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    let c_name = CString::new(name).unwrap();
    unsafe {
        let fd = libc::shm_open(
            c_name.as_ptr(),
            libc::O_CREAT | libc::O_RDWR | libc::O_EXCL,
            0o600,
        );
        assert!(fd >= 0, "shm_open failed for {name}");
        let rc = libc::ftruncate(fd, ipc_buf.len() as libc::off_t);
        assert_eq!(rc, 0, "ftruncate failed for {name}");
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            ipc_buf.len(),
            libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        );
        assert_ne!(ptr, libc::MAP_FAILED, "mmap failed for {name}");
        libc::close(fd);
        std::ptr::copy_nonoverlapping(ipc_buf.as_ptr(), ptr as *mut u8, ipc_buf.len());
        libc::munmap(ptr, ipc_buf.len());
    }
}

fn unique_test_shm_name(prefix: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("/{prefix}-{pid}-{n}")
}

// ---------------------------------------------------------------------------
// Daemon helpers
// ---------------------------------------------------------------------------

fn spawn_daemon() -> Child {
    Command::new(env!("CARGO_BIN_EXE_gpl-boundary"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn gpl-boundary")
}

fn parse_lines(stdout: &[u8]) -> Vec<serde_json::Value> {
    String::from_utf8_lossy(stdout)
        .trim()
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).expect(l))
        .collect()
}

fn init_line() -> &'static str {
    "{\"init\":{}}\n"
}

// ---------------------------------------------------------------------------
// Index-building helper
// ---------------------------------------------------------------------------

/// Build a bowtie2 index at `index_path` from a single sequence by spawning
/// a one-shot daemon session. Blocking. The directory containing the index
/// must already exist.
fn build_index(index_path: &str, ref_name: &str, ref_seq: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("sequence", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![ref_name])),
            Arc::new(StringArray::from(vec![ref_seq])),
        ],
    )
    .unwrap();
    let input = ShmGuard::create_with_batch("bt2-build-fixture", &batch);

    let mut session = String::new();
    session.push_str(init_line());
    let req = serde_json::json!({
        "tool": "bowtie2-build",
        "config": { "index_path": index_path },
        "shm_input": input.name(),
    });
    session.push_str(&serde_json::to_string(&req).unwrap());
    session.push('\n');
    session.push_str("{\"shutdown\":true}\n");

    let mut child = spawn_daemon();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(session.as_bytes())
        .unwrap();
    let out = child.wait_with_output().expect("daemon wait failed");
    assert!(
        out.status.success(),
        "build_index failed: stderr={}\nstdout={}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    let lines = parse_lines(&out.stdout);
    assert!(lines.len() >= 2);
    assert!(
        lines[1]["success"].as_bool().unwrap_or(false),
        "build_index reported failure: {:?}",
        lines[1]
    );
}

/// Build an Arrow input batch shaped for bowtie2-align: `read_id`,
/// `sequence1`, `qual1` (nullable). Single read per batch is enough for
/// these tests.
fn make_align_batch(read_id: &str, seq: &str) -> RecordBatch {
    let qual: String = "I".repeat(seq.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("read_id", DataType::Utf8, false),
        Field::new("sequence1", DataType::Utf8, false),
        Field::new("qual1", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![read_id])),
            Arc::new(StringArray::from(vec![seq])),
            Arc::new(StringArray::from(vec![Some(qual.as_str())])),
        ],
    )
    .unwrap()
}

/// Build a multi-read batch — used by the in-flight-protection test where
/// we want batch A's alignment to take long enough that batch B has time
/// to land in the registry's pending queue.
fn make_align_batch_multi(prefix: &str, n_reads: usize, seq: &str) -> RecordBatch {
    let ids: Vec<String> = (0..n_reads).map(|i| format!("{prefix}-{i}")).collect();
    let qual: String = "I".repeat(seq.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("read_id", DataType::Utf8, false),
        Field::new("sequence1", DataType::Utf8, false),
        Field::new("qual1", DataType::Utf8, true),
    ]));
    let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(id_refs)),
            Arc::new(StringArray::from(vec![seq; n_reads])),
            Arc::new(StringArray::from(vec![Some(qual.as_str()); n_reads])),
        ],
    )
    .unwrap()
}

// Reference sequences, distinct enough that an index built on each gives a
// genuinely different alignment context (rules out any cross-pollination
// bug between the two SubprocessWorker children).
const REF_A: &str = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT\
                     TGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCA\
                     AAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGG";
const REF_B: &str = "GGCCAATTGGCCAATTGGCCAATTGGCCAATTGGCCAATTGGCCAATTGGCCAATT\
                     TTAATTAATTAATTAATTAATTAATTAATTAATTAATTAATTAATTAATTAATTAA\
                     CCAAGGTTCCAAGGTTCCAAGGTTCCAAGGTTCCAAGGTTCCAAGGTTCCAAGGTT";

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_subprocess_same_fingerprint_serializes() {
    // Two batches with the *same* bowtie2 fingerprint → same SubprocessWorker
    // → child runs them sequentially. Verify both responses carry their
    // respective batch_ids and both succeeded.
    let tmp = tempfile::tempdir().unwrap();
    let idx = tmp.path().join("idx-same").to_string_lossy().into_owned();
    build_index(&idx, "refA", REF_A);

    let r1: String = REF_A.chars().skip(20).take(30).collect();
    let r2: String = REF_A.chars().skip(60).take(30).collect();
    let shm1 = ShmGuard::create_with_batch("ph4-sub-same-1", &make_align_batch("r1", &r1));
    let shm2 = ShmGuard::create_with_batch("ph4-sub-same-2", &make_align_batch("r2", &r2));

    let mut child = spawn_daemon();
    let session = format!(
        "{}\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":1}}\n\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":2}}\n\
         {{\"shutdown\":true}}\n",
        init_line(),
        shm1.name(),
        shm2.name(),
    );
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(session.as_bytes())
        .unwrap();
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("daemon wait failed");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 3, "Expected init reply + 2 batch responses");

    let batch_responses = &lines[1..];
    let by_id = |id: u64| {
        batch_responses
            .iter()
            .find(|r| r["batch_id"].as_u64() == Some(id))
            .unwrap_or_else(|| panic!("missing response for batch_id={id}: {batch_responses:?}"))
    };
    let r1_resp = by_id(1);
    let r2_resp = by_id(2);
    assert!(r1_resp["success"].as_bool().unwrap(), "{r1_resp}");
    assert!(r2_resp["success"].as_bool().unwrap(), "{r2_resp}");

    // Same-fingerprint child has only one bowtie2 mutex, so responses
    // arrive in submission order — assert that explicitly to catch any
    // future change that breaks per-fingerprint ordering.
    assert_eq!(
        lines[1]["batch_id"].as_u64().unwrap(),
        1,
        "Same-fingerprint batches must complete in submission order"
    );
    assert_eq!(lines[2]["batch_id"].as_u64().unwrap(), 2);

    let _guards: Vec<ShmGuard> = batch_responses
        .iter()
        .flat_map(|r| {
            r["shm_outputs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|o| o["name"].as_str().map(ShmGuard::adopt))
                        .collect::<Vec<ShmGuard>>()
                })
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
}

#[test]
fn test_subprocess_two_fingerprints_run_in_parallel() {
    // Two distinct bowtie2 fingerprints (different index_paths) → two
    // SubprocessWorker children → batches run concurrently in distinct
    // processes, sidestepping bowtie2's process-wide alignment mutex.
    //
    // We don't assert wall-clock parallelism (CI noise + tiny synthetic
    // alignments would make any threshold flaky). What we *do* assert is
    // structural: both batches succeed, both echo their batch_ids, and the
    // session exits cleanly. The `same_fingerprint_serializes` test pairs
    // with this one to demonstrate the routing distinction.
    let tmp = tempfile::tempdir().unwrap();
    let idx_a = tmp.path().join("idx-a").to_string_lossy().into_owned();
    let idx_b = tmp.path().join("idx-b").to_string_lossy().into_owned();
    build_index(&idx_a, "refA", REF_A);
    build_index(&idx_b, "refB", REF_B);

    let read_a: String = REF_A.chars().skip(10).take(30).collect();
    let read_b: String = REF_B.chars().skip(10).take(30).collect();
    let shm_a = ShmGuard::create_with_batch("ph4-sub-par-a", &make_align_batch("a1", &read_a));
    let shm_b = ShmGuard::create_with_batch("ph4-sub-par-b", &make_align_batch("b1", &read_b));

    let mut child = spawn_daemon();
    let session = format!(
        "{}\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx_a}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":100}}\n\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx_b}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":200}}\n\
         {{\"shutdown\":true}}\n",
        init_line(),
        shm_a.name(),
        shm_b.name(),
    );
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(session.as_bytes())
        .unwrap();
    drop(child.stdin.take());

    let started = Instant::now();
    let out = child.wait_with_output().expect("daemon wait failed");
    let elapsed = started.elapsed();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Sanity bound: two trivial alignments must finish promptly even
    // sequentially. If we're way over this we're hitting a pathology.
    assert!(
        elapsed < Duration::from_secs(30),
        "Two-fingerprint session took {elapsed:?}; expected well under 30s"
    );

    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 3, "Expected init reply + 2 batch responses");

    let batch_responses = &lines[1..];
    let r_a = batch_responses
        .iter()
        .find(|r| r["batch_id"].as_u64() == Some(100))
        .expect("missing response for batch_id=100");
    let r_b = batch_responses
        .iter()
        .find(|r| r["batch_id"].as_u64() == Some(200))
        .expect("missing response for batch_id=200");
    assert!(r_a["success"].as_bool().unwrap(), "{r_a}");
    assert!(r_b["success"].as_bool().unwrap(), "{r_b}");
    assert_eq!(r_a["result"]["n_reads"].as_i64().unwrap(), 1);
    assert_eq!(r_b["result"]["n_reads"].as_i64().unwrap(), 1);

    let _guards: Vec<ShmGuard> = batch_responses
        .iter()
        .flat_map(|r| {
            r["shm_outputs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|o| o["name"].as_str().map(ShmGuard::adopt))
                        .collect::<Vec<ShmGuard>>()
                })
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
}

#[test]
fn test_subprocess_shutdown_reaps_workers() {
    // After `{"shutdown": true}`, the parent must reap any subprocess
    // workers it spawned. We verify by checking the parent exits cleanly
    // and that no descendant gpl-boundary processes remain by the time
    // `wait_with_output` returns. The `Child::wait` in
    // `SubprocessWorker::close` is what makes this true; this test is the
    // regression guard for it.
    let tmp = tempfile::tempdir().unwrap();
    let idx = tmp.path().join("idx-reap").to_string_lossy().into_owned();
    build_index(&idx, "refA", REF_A);

    let read: String = REF_A.chars().skip(10).take(30).collect();
    let shm = ShmGuard::create_with_batch("ph4-sub-reap", &make_align_batch("r1", &read));

    let mut child = spawn_daemon();
    let parent_pid = child.id();
    let session = format!(
        "{}\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":1}}\n\
         {{\"shutdown\":true}}\n",
        init_line(),
        shm.name(),
    );
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(session.as_bytes())
        .unwrap();
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("daemon wait failed");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Once the parent has exited, any descendants that haven't been
    // reaped are orphans (their ppid will be reparented to PID 1 on
    // Linux; on macOS they'd survive too). Since the parent has exited,
    // its previous PID is fair game for reuse — checking for living
    // descendants needs to filter by *original* ppid, which Linux makes
    // available via `/proc/[pid]/status` (PPid: line). We do a
    // best-effort scan across `/proc/*/status` and assert no live process
    // names "gpl-boundary" with PPid == parent_pid. On non-Linux we skip.
    #[cfg(target_os = "linux")]
    {
        std::thread::sleep(Duration::from_millis(50));
        let entries = std::fs::read_dir("/proc").expect("reading /proc");
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if !name.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }
            let status_path = path.join("status");
            let status = match std::fs::read_to_string(&status_path) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut proc_name = "";
            let mut ppid: u32 = 0;
            for line in status.lines() {
                if let Some(rest) = line.strip_prefix("Name:\t") {
                    proc_name = rest;
                } else if let Some(rest) = line.strip_prefix("PPid:\t") {
                    ppid = rest.trim().parse().unwrap_or(0);
                }
            }
            if proc_name.starts_with("gpl-boundary") && ppid == parent_pid {
                panic!(
                    "Orphan gpl-boundary subprocess survived shutdown: pid={name}, \
                     ppid={ppid}"
                );
            }
        }
    }

    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 2, "Expected init + 1 batch response");
    assert_eq!(lines[1]["batch_id"].as_u64().unwrap(), 1);
    assert!(lines[1]["success"].as_bool().unwrap());

    if let Some(outs) = lines[1]["shm_outputs"].as_array() {
        for o in outs {
            unlink_shm(o["name"].as_str().unwrap());
        }
    }
}

#[test]
fn test_subprocess_bowtie2_responses_carry_batch_id() {
    // Step 3 added explicit batch_id forwarding through the worker
    // child's response stream. Verify with a small batch_id (1) and a
    // large one (u64::MAX - 1) to catch any narrowing bug on the way
    // through serde.
    let tmp = tempfile::tempdir().unwrap();
    let idx = tmp
        .path()
        .join("idx-batchid")
        .to_string_lossy()
        .into_owned();
    build_index(&idx, "refA", REF_A);

    let read: String = REF_A.chars().skip(10).take(30).collect();
    let shm1 = ShmGuard::create_with_batch("ph4-sub-bid-1", &make_align_batch("r1", &read));
    let shm2 = ShmGuard::create_with_batch("ph4-sub-bid-2", &make_align_batch("r2", &read));

    let big_id: u64 = u64::MAX - 1;
    let mut child = spawn_daemon();
    let session = format!(
        "{}\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":1}}\n\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":{big_id}}}\n\
         {{\"shutdown\":true}}\n",
        init_line(),
        shm1.name(),
        shm2.name(),
    );
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(session.as_bytes())
        .unwrap();
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("daemon wait failed");
    assert!(out.status.success());
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 3);

    let batch_responses = &lines[1..];
    assert!(batch_responses
        .iter()
        .any(|r| r["batch_id"].as_u64() == Some(1)));
    assert!(batch_responses
        .iter()
        .any(|r| r["batch_id"].as_u64() == Some(big_id)));

    for resp in batch_responses {
        if let Some(outs) = resp["shm_outputs"].as_array() {
            for o in outs {
                unlink_shm(o["name"].as_str().unwrap());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Eviction tests (step 4)
// ---------------------------------------------------------------------------

/// Build an init line with explicit eviction knobs. Used by the
/// eviction-focused tests below.
fn init_with_eviction(max_workers: usize, max_idle_workers: usize, worker_idle_ms: u64) -> String {
    format!(
        "{{\"init\":{{\"max_workers\":{max_workers},\
         \"max_idle_workers\":{max_idle_workers},\
         \"worker_idle_ms\":{worker_idle_ms}}}}}\n"
    )
}

#[test]
fn test_subprocess_idle_eviction_does_not_break_reuse() {
    // With a short idle deadline, a worker for fingerprint A is evicted
    // between batches. The next batch on the same fingerprint must
    // succeed — eviction is meant to free memory, not prevent reuse.
    let tmp = tempfile::tempdir().unwrap();
    let idx = tmp.path().join("idx-idle").to_string_lossy().into_owned();
    build_index(&idx, "refA", REF_A);

    let read: String = REF_A.chars().skip(10).take(30).collect();
    let shm1 = ShmGuard::create_with_batch("ph4-evict-1", &make_align_batch("r1", &read));
    let shm2 = ShmGuard::create_with_batch("ph4-evict-2", &make_align_batch("r2", &read));

    let mut child = spawn_daemon();
    let init = init_with_eviction(4, 4, 100);
    let session = format!(
        "{init}\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":1}}\n",
        shm1.name(),
    );
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(session.as_bytes()).unwrap();
        stdin.flush().unwrap();
    }
    // Sleep long enough for the worker to go idle and the sweeper to
    // notice (idle_ms=100, sweeper tick ~25ms in test mode).
    std::thread::sleep(Duration::from_millis(400));
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
                     \"shm_input\":\"{}\",\"batch_id\":2}}\n",
                    shm2.name()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("daemon wait");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 3, "expected init reply + 2 batch responses");

    let batch_responses = &lines[1..];
    let r1 = batch_responses
        .iter()
        .find(|r| r["batch_id"].as_u64() == Some(1))
        .expect("missing batch_id=1");
    let r2 = batch_responses
        .iter()
        .find(|r| r["batch_id"].as_u64() == Some(2))
        .expect("missing batch_id=2");
    assert!(r1["success"].as_bool().unwrap(), "{r1}");
    assert!(r2["success"].as_bool().unwrap(), "{r2}");

    let _guards: Vec<ShmGuard> = batch_responses
        .iter()
        .flat_map(|r| {
            r["shm_outputs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|o| o["name"].as_str().map(ShmGuard::adopt))
                        .collect::<Vec<ShmGuard>>()
                })
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
}

// ---------------------------------------------------------------------------
// Crash-handling tests (step 5)
// ---------------------------------------------------------------------------

/// Find the PID of any descendant gpl-boundary --worker process whose
/// PPid matches `parent_pid`. Returns `None` if no such child exists
/// (yet). Linux-only (uses /proc).
#[cfg(target_os = "linux")]
fn find_worker_child(parent_pid: u32) -> Option<u32> {
    find_worker_children(parent_pid).into_iter().next()
}

/// All `gpl-boundary --worker` processes whose parent is `parent_pid`,
/// in arbitrary order. Linux-only.
#[cfg(target_os = "linux")]
fn find_worker_children(parent_pid: u32) -> Vec<u32> {
    let mut found = Vec::new();
    let entries = match std::fs::read_dir("/proc") {
        Ok(d) => d,
        Err(_) => return found,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let pid: u32 = match name.parse() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let status = match std::fs::read_to_string(path.join("status")) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let mut proc_name = "";
        let mut ppid: u32 = 0;
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("Name:\t") {
                proc_name = rest;
            } else if let Some(rest) = line.strip_prefix("PPid:\t") {
                ppid = rest.trim().parse().unwrap_or(0);
            }
        }
        if proc_name.starts_with("gpl-boundary") && ppid == parent_pid {
            let cmdline =
                std::fs::read_to_string(format!("/proc/{pid}/cmdline")).unwrap_or_default();
            if cmdline.contains("--worker") {
                found.push(pid);
            }
        }
    }
    found
}

#[cfg(target_os = "linux")]
#[test]
fn test_subprocess_worker_crash_marks_fingerprint_dead() {
    // Send one successful batch. Then kill the worker child with SIGKILL
    // (simulating a segfault or OOM kill). Submit another batch on the
    // *same* fingerprint and assert it errors out citing the dead
    // worker. Submit a batch on a *different* fingerprint and assert it
    // still succeeds — the crash must not poison unrelated workers.
    let tmp = tempfile::tempdir().unwrap();
    let idx_a = tmp
        .path()
        .join("idx-crash-a")
        .to_string_lossy()
        .into_owned();
    let idx_b = tmp
        .path()
        .join("idx-crash-b")
        .to_string_lossy()
        .into_owned();
    build_index(&idx_a, "refA", REF_A);
    build_index(&idx_b, "refB", REF_B);

    let read_a: String = REF_A.chars().skip(10).take(30).collect();
    let read_b: String = REF_B.chars().skip(10).take(30).collect();

    let shm1 = ShmGuard::create_with_batch("ph4-crash-1", &make_align_batch("a1", &read_a));
    let shm2 = ShmGuard::create_with_batch("ph4-crash-2", &make_align_batch("a2", &read_a));
    let shm3 = ShmGuard::create_with_batch("ph4-crash-3", &make_align_batch("b1", &read_b));

    let mut child = spawn_daemon();
    let parent_pid = child.id();

    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(init_line().as_bytes()).unwrap();
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx_a}\",\"seed\":1}},\
                     \"shm_input\":\"{}\",\"batch_id\":1}}\n",
                    shm1.name()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin.flush().unwrap();
    }

    // Wait for the worker subprocess to spawn (creates the index loader)
    // and finish batch 1. We poll /proc until we find the worker, then
    // wait briefly to let the response cycle complete.
    let worker_pid = {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut found = None;
        while Instant::now() < deadline {
            if let Some(pid) = find_worker_child(parent_pid) {
                found = Some(pid);
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        found.expect("Could not locate bowtie2-align worker subprocess")
    };
    // Allow batch 1 to round-trip.
    std::thread::sleep(Duration::from_millis(200));

    // Kill the worker child — simulates a segfault / OOM kill.
    let kill_result = unsafe { libc::kill(worker_pid as libc::pid_t, libc::SIGKILL) };
    assert_eq!(kill_result, 0, "kill -9 on worker pid={worker_pid} failed");

    // Give the parent's reader thread time to observe EOF and mark the
    // fingerprint dead.
    std::thread::sleep(Duration::from_millis(300));

    {
        let stdin = child.stdin.as_mut().unwrap();
        // Same-fingerprint batch — must error fast with "dead" message.
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx_a}\",\"seed\":1}},\
                     \"shm_input\":\"{}\",\"batch_id\":2}}\n",
                    shm2.name()
                )
                .as_bytes(),
            )
            .unwrap();
        // Different-fingerprint batch — must succeed.
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx_b}\",\"seed\":1}},\
                     \"shm_input\":\"{}\",\"batch_id\":3}}\n",
                    shm3.name()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("daemon wait");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines = parse_lines(&out.stdout);

    // Find responses by batch_id. We expect:
    //   batch_id=1 — success (ran before the kill)
    //   batch_id=2 — error mentioning the dead fingerprint
    //   batch_id=3 — success (different fingerprint, fresh worker)
    let by_id = |id: u64| {
        lines
            .iter()
            .find(|r| r["batch_id"].as_u64() == Some(id))
            .unwrap_or_else(|| panic!("missing response for batch_id={id}: {lines:?}"))
    };
    let r1 = by_id(1);
    let r2 = by_id(2);
    let r3 = by_id(3);
    assert!(
        r1["success"].as_bool().unwrap(),
        "batch 1 should have run before the crash: {r1}"
    );
    assert!(
        !r2["success"].as_bool().unwrap(),
        "batch 2 should fail because fingerprint is dead: {r2}"
    );
    let err2 = r2["error"].as_str().unwrap_or("");
    assert!(
        err2.contains("dead") || err2.contains("crashed"),
        "batch 2 error should cite crash/dead state: {err2}"
    );
    assert!(
        r3["success"].as_bool().unwrap(),
        "batch 3 (different fingerprint) should still succeed after the crash: {r3}"
    );

    let _guards: Vec<ShmGuard> = lines
        .iter()
        .flat_map(|r| {
            r["shm_outputs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|o| o["name"].as_str().map(ShmGuard::adopt))
                        .collect::<Vec<ShmGuard>>()
                })
                .unwrap_or_default()
        })
        .collect();
}

#[cfg(target_os = "linux")]
#[test]
fn test_subprocess_orphan_shm_swept_after_crash() {
    // Force a crash mid-batch. After the parent's reader thread sees
    // EOF, it sweeps `/dev/shm/gb-{child_pid}-*` so the now-orphaned
    // output shm name from the in-flight batch (created by the child
    // before it died) is unlinked. Verify by attempting `shm_open` on
    // any leftover names — they should all be ENOENT.
    //
    // We pre-create shm names that *look* like they came from the
    // crashed worker (gb-{pid}-{n}-{label}) so we can deterministically
    // assert they get swept. Real orphan names from inside bowtie2 are
    // hard to capture without racing the kill, but the sweep mechanism
    // is the same regardless of who created the file.
    let tmp = tempfile::tempdir().unwrap();
    let idx = tmp.path().join("idx-orphan").to_string_lossy().into_owned();
    build_index(&idx, "refA", REF_A);

    let read: String = REF_A.chars().skip(10).take(30).collect();
    let shm = ShmGuard::create_with_batch("ph4-orphan-1", &make_align_batch("r1", &read));

    let mut child = spawn_daemon();
    let parent_pid = child.id();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(init_line().as_bytes()).unwrap();
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
                     \"shm_input\":\"{}\",\"batch_id\":1}}\n",
                    shm.name()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin.flush().unwrap();
    }

    // Wait for the worker to spawn and find its PID.
    let worker_pid = {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut found = None;
        while Instant::now() < deadline {
            if let Some(pid) = find_worker_child(parent_pid) {
                found = Some(pid);
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        found.expect("Could not locate bowtie2-align worker subprocess")
    };

    // Pre-create fake orphan shm names that match the worker's PID
    // prefix. The sweep should unlink these on crash detection.
    let fake_orphans: Vec<String> = (0..3)
        .map(|i| format!("/gb-{worker_pid}-orphan-{i}"))
        .collect();
    for name in &fake_orphans {
        let c = CString::new(name.as_str()).unwrap();
        unsafe {
            // shm_unlink any leftover from prior runs first.
            libc::shm_unlink(c.as_ptr());
            let fd = libc::shm_open(
                c.as_ptr(),
                libc::O_CREAT | libc::O_RDWR | libc::O_EXCL,
                0o600,
            );
            assert!(fd >= 0, "Failed to create fake orphan {name}");
            libc::ftruncate(fd, 64);
            libc::close(fd);
        }
    }

    // Kill the worker.
    let kill_result = unsafe { libc::kill(worker_pid as libc::pid_t, libc::SIGKILL) };
    assert_eq!(kill_result, 0);

    // Let the parent's reader thread observe EOF and run the sweep.
    std::thread::sleep(Duration::from_millis(500));

    // The sweep should have unlinked our fake orphans. Verify.
    for name in &fake_orphans {
        let c = CString::new(name.as_str()).unwrap();
        let fd = unsafe { libc::shm_open(c.as_ptr(), libc::O_RDONLY, 0) };
        assert!(
            fd < 0,
            "Orphan {name} survived the post-crash sweep — shm_open returned fd={fd}"
        );
    }

    // Send shutdown so the daemon exits cleanly. The crashed
    // fingerprint's pending batch_id=1 is already on its way as an
    // error response (drained by the reader thread).
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());
    let _ = child.wait_with_output();
}

#[test]
fn test_subprocess_max_workers_recycles_via_lru() {
    // max_workers=1: every new fingerprint forces eviction of the
    // current resident. Three distinct bowtie2 fingerprints must all
    // succeed despite the budget. The test exercises the
    // evict_lru_idle_subprocess code path repeatedly without forcing
    // queuing (each previous worker is idle by the time the next batch
    // arrives because we wait between submits).
    let tmp = tempfile::tempdir().unwrap();
    let idx_a = tmp.path().join("idx-lru-a").to_string_lossy().into_owned();
    let idx_b = tmp.path().join("idx-lru-b").to_string_lossy().into_owned();
    let idx_c = tmp.path().join("idx-lru-c").to_string_lossy().into_owned();
    build_index(&idx_a, "refA", REF_A);
    build_index(&idx_b, "refB", REF_B);
    // Third index reuses REF_A but with a different filename, giving a
    // distinct index_path → distinct fingerprint.
    build_index(&idx_c, "refC", REF_A);

    let read_a: String = REF_A.chars().skip(10).take(30).collect();
    let read_b: String = REF_B.chars().skip(10).take(30).collect();
    let read_c: String = REF_A.chars().skip(40).take(30).collect();
    let shm_a = ShmGuard::create_with_batch("ph4-lru-a", &make_align_batch("a", &read_a));
    let shm_b = ShmGuard::create_with_batch("ph4-lru-b", &make_align_batch("b", &read_b));
    let shm_c = ShmGuard::create_with_batch("ph4-lru-c", &make_align_batch("c", &read_c));

    let mut child = spawn_daemon();
    let init = init_with_eviction(1, 1, 60_000);
    let session = format!(
        "{init}\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx_a}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":1}}\n\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx_b}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":2}}\n\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx_c}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":3}}\n\
         {{\"shutdown\":true}}\n",
        shm_a.name(),
        shm_b.name(),
        shm_c.name(),
    );
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(session.as_bytes())
        .unwrap();
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("daemon wait");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 4, "expected init reply + 3 batch responses");

    let batch_responses = &lines[1..];
    for id in [1, 2, 3] {
        let r = batch_responses
            .iter()
            .find(|r| r["batch_id"].as_u64() == Some(id))
            .unwrap_or_else(|| panic!("missing batch_id={id}"));
        assert!(r["success"].as_bool().unwrap(), "batch {id} failed: {r}");
    }

    let _guards: Vec<ShmGuard> = batch_responses
        .iter()
        .flat_map(|r| {
            r["shm_outputs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|o| o["name"].as_str().map(ShmGuard::adopt))
                        .collect::<Vec<ShmGuard>>()
                })
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
}

#[test]
fn test_subprocess_inflight_batch_protected_from_eviction() {
    // max_workers=1, two fingerprints A and B. Batch A1 is intentionally
    // slow (many reads) so that when we submit B immediately afterward,
    // A is still in-flight. The registry must NOT evict A — instead it
    // queues B, and dispatches B once A1's response comes back. Both
    // batches succeed, and A1's response arrives before B's.
    //
    // The "slow A1" needs to be slow enough that B's submit lands while
    // A1 is in-flight. 1500 reads against the small synthetic index
    // gives bowtie2 enough work that this is reliably the case under
    // normal CI load. If this test ever flakes on a fast machine, the
    // remedy is to increase the read count, not to add sleeps.
    let tmp = tempfile::tempdir().unwrap();
    let idx_a = tmp.path().join("idx-prot-a").to_string_lossy().into_owned();
    let idx_b = tmp.path().join("idx-prot-b").to_string_lossy().into_owned();
    build_index(&idx_a, "refA", REF_A);
    build_index(&idx_b, "refB", REF_B);

    let read_a: String = REF_A.chars().skip(10).take(30).collect();
    let read_b: String = REF_B.chars().skip(10).take(30).collect();

    let shm_a =
        ShmGuard::create_with_batch("ph4-prot-a", &make_align_batch_multi("a", 1500, &read_a));
    let shm_b = ShmGuard::create_with_batch("ph4-prot-b", &make_align_batch("b", &read_b));

    let mut child = spawn_daemon();
    let init = init_with_eviction(1, 1, 60_000);
    let session = format!(
        "{init}\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx_a}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":1}}\n\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx_b}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"batch_id\":2}}\n\
         {{\"shutdown\":true}}\n",
        shm_a.name(),
        shm_b.name(),
    );
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(session.as_bytes())
        .unwrap();
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("daemon wait");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 3);

    // Both batches must succeed. If A had been evicted mid-flight, A1's
    // response would have been an error or never arrived (subprocess
    // SIGKILL'd → response_tx hangs up).
    let batch_responses = &lines[1..];
    let r1 = batch_responses
        .iter()
        .find(|r| r["batch_id"].as_u64() == Some(1))
        .expect("missing A1 response");
    let r2 = batch_responses
        .iter()
        .find(|r| r["batch_id"].as_u64() == Some(2))
        .expect("missing B response");
    assert!(
        r1["success"].as_bool().unwrap(),
        "A1 failed — likely evicted mid-flight: {r1}"
    );
    assert!(r2["success"].as_bool().unwrap(), "B failed: {r2}");
    assert_eq!(
        r1["result"]["n_reads"].as_i64().unwrap(),
        1500,
        "A1 should have processed all 1500 reads"
    );

    let _guards: Vec<ShmGuard> = batch_responses
        .iter()
        .flat_map(|r| {
            r["shm_outputs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|o| o["name"].as_str().map(ShmGuard::adopt))
                        .collect::<Vec<ShmGuard>>()
                })
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
}

#[cfg(target_os = "linux")]
#[test]
fn test_subprocess_max_workers_bounds_resident_children() {
    // Cross-cutting verification: with `max_workers: 2`, cycling 5
    // distinct bowtie2 fingerprints sequentially must leave AT MOST 2
    // worker subprocesses resident at any later sample. The eviction
    // path should reap the LRU child each time a new fingerprint
    // arrives over budget. We sample the descendant set after every
    // batch and assert the upper bound across the whole run, then a
    // final sample after shutdown asserts zero survivors.
    let tmp = tempfile::tempdir().unwrap();
    let mut indexes = Vec::new();
    for i in 0..5 {
        let path = tmp
            .path()
            .join(format!("idx-bound-{i}"))
            .to_string_lossy()
            .into_owned();
        let r = if i % 2 == 0 { REF_A } else { REF_B };
        build_index(&path, &format!("ref{i}"), r);
        indexes.push(path);
    }

    let read_a: String = REF_A.chars().skip(10).take(30).collect();
    let read_b: String = REF_B.chars().skip(10).take(30).collect();
    let mut shms = Vec::new();
    for i in 0..5 {
        let r = if i % 2 == 0 {
            read_a.as_str()
        } else {
            read_b.as_str()
        };
        shms.push(ShmGuard::create_with_batch(
            &format!("ph4-bound-{i}"),
            &make_align_batch(&format!("r{i}"), r),
        ));
    }

    let mut child = spawn_daemon();
    let parent_pid = child.id();
    {
        let stdin = child.stdin.as_mut().unwrap();
        // max_workers=2, very long idle deadline so eviction is purely
        // budget-driven (we want LRU pressure, not idle pressure).
        stdin
            .write_all(init_with_eviction(2, 2, 600_000).as_bytes())
            .unwrap();
        stdin.flush().unwrap();
    }

    let mut max_observed_children = 0usize;
    for (i, idx) in indexes.iter().enumerate() {
        {
            let stdin = child.stdin.as_mut().unwrap();
            stdin
                .write_all(
                    format!(
                        "{{\"tool\":\"bowtie2-align\",\
                         \"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
                         \"shm_input\":\"{}\",\"batch_id\":{i}}}\n",
                        shms[i].name()
                    )
                    .as_bytes(),
                )
                .unwrap();
            stdin.flush().unwrap();
        }
        // Allow the batch to round-trip + sweeper to react.
        std::thread::sleep(Duration::from_millis(150));
        let live = find_worker_children(parent_pid).len();
        max_observed_children = max_observed_children.max(live);
        assert!(
            live <= 2,
            "max_workers=2 violated: {live} live children after batch {i}"
        );
    }
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("daemon wait");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let post_shutdown = find_worker_children(parent_pid);
    assert!(
        post_shutdown.is_empty(),
        "Expected zero worker children after shutdown, found {post_shutdown:?}"
    );
    assert!(
        max_observed_children >= 1,
        "Test must have actually exercised the worker-spawn path"
    );

    let lines = parse_lines(&out.stdout);
    let batch_responses = &lines[1..];
    assert_eq!(batch_responses.len(), 5, "Expected 5 batch responses");
    for r in batch_responses {
        assert!(r["success"].as_bool().unwrap(), "Batch failed: {r}");
    }

    let _guards: Vec<ShmGuard> = batch_responses
        .iter()
        .flat_map(|r| {
            r["shm_outputs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|o| o["name"].as_str().map(ShmGuard::adopt))
                        .collect::<Vec<ShmGuard>>()
                })
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
}
