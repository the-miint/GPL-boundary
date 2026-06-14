//! Phase 2: opt-in per-batch worker telemetry (`metrics`) for bowtie2-align.
//!
//! Drives the full daemon → subprocess-worker path: build a tiny index, then
//! submit bowtie2-align batches with `"metrics": true` and assert the response
//! carries a `metrics` object populated with the worker's per-batch getrusage
//! deltas. A non-opted batch must omit the object entirely.
//!
//! Helpers mirror tests/subprocess.rs (each test file is independent — no
//! shared support module yet). Daemon-clock fields (t_dispatch_ms / t_worker_ms)
//! are asserted in Phase 3, not here.

use std::ffi::CString;
use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

// --- shm helpers ----------------------------------------------------------

fn unlink_shm(name: &str) {
    let c_name = CString::new(name).unwrap();
    unsafe {
        libc::shm_unlink(c_name.as_ptr());
    }
}

struct ShmGuard {
    name: String,
    size: usize,
}

impl ShmGuard {
    fn create_with_batch(prefix: &str, batch: &RecordBatch) -> Self {
        let name = unique_test_shm_name(prefix);
        let size = write_test_shm(&name, batch);
        Self { name, size }
    }
    fn adopt(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            size: 0,
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn size(&self) -> usize {
        self.size
    }
}

impl Drop for ShmGuard {
    fn drop(&mut self) {
        unlink_shm(&self.name);
    }
}

fn write_test_shm(name: &str, batch: &RecordBatch) -> usize {
    let mut ipc_buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    let len = ipc_buf.len();
    let c_name = CString::new(name).unwrap();
    unsafe {
        let fd = libc::shm_open(
            c_name.as_ptr(),
            libc::O_CREAT | libc::O_RDWR | libc::O_EXCL,
            0o600,
        );
        assert!(fd >= 0, "shm_open failed for {name}");
        let rc = libc::ftruncate(fd, len as libc::off_t);
        assert_eq!(rc, 0, "ftruncate failed for {name}");
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        );
        assert_ne!(ptr, libc::MAP_FAILED, "mmap failed for {name}");
        libc::close(fd);
        std::ptr::copy_nonoverlapping(ipc_buf.as_ptr(), ptr as *mut u8, len);
        libc::munmap(ptr, len);
    }
    len
}

fn unique_test_shm_name(prefix: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("/{prefix}-{pid}-{n}")
}

// --- daemon helpers --------------------------------------------------------

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
    let input = ShmGuard::create_with_batch("bt2-build-metrics-fixture", &batch);

    let mut session = String::new();
    session.push_str(init_line());
    let req = serde_json::json!({
        "tool": "bowtie2-build",
        "config": { "index_path": index_path },
        "shm_input": input.name(), "shm_input_size": input.size(),
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
        "build_index failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines = parse_lines(&out.stdout);
    assert!(
        lines[1]["success"].as_bool().unwrap_or(false),
        "build_index reported failure: {:?}",
        lines[1]
    );
}

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

const REF_A: &str = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT\
                     TGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCA\
                     AAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGG";

/// Every worker-self-reported metric field that must be present on an opted-in
/// subprocess response. Daemon-clock fields are added in Phase 3.
const REQUIRED_WORKER_FIELDS: &[&str] = &[
    "worker_wall_ms",
    "worker_user_ms",
    "worker_sys_ms",
    "worker_majflt",
    "worker_minflt",
    "worker_inblock",
    "worker_oublock",
    "worker_nvcsw",
    "worker_nivcsw",
    "worker_rss_kb",
    "worker_reused",
];

#[test]
fn test_bowtie2_align_metrics_opt_in() {
    let tmp = tempfile::tempdir().unwrap();
    let idx = tmp
        .path()
        .join("idx-metrics")
        .to_string_lossy()
        .into_owned();
    build_index(&idx, "refA", REF_A);

    let read: String = REF_A.chars().skip(10).take(30).collect();
    // Three batches on the SAME fingerprint:
    //   1: metrics on  -> cold worker (worker_reused = false)
    //   2: metrics on  -> warm worker (worker_reused = true)
    //   3: metrics off -> no metrics object at all
    let shm1 = ShmGuard::create_with_batch("metrics-1", &make_align_batch("r1", &read));
    let shm2 = ShmGuard::create_with_batch("metrics-2", &make_align_batch("r2", &read));
    let shm3 = ShmGuard::create_with_batch("metrics-3", &make_align_batch("r3", &read));

    let mut child = spawn_daemon();
    let session = format!(
        "{}\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":1,\"metrics\":true}}\n\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":2,\"metrics\":true}}\n\
         {{\"tool\":\"bowtie2-align\",\"config\":{{\"index_path\":\"{idx}\",\"seed\":1}},\
         \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":3}}\n\
         {{\"shutdown\":true}}\n",
        init_line(),
        shm1.name(),
        shm1.size(),
        shm2.name(),
        shm2.size(),
        shm3.name(),
        shm3.size(),
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
    let by_id = |id: u64| {
        lines
            .iter()
            .find(|r| r["batch_id"].as_u64() == Some(id))
            .unwrap_or_else(|| panic!("missing response for batch_id={id}: {lines:?}"))
    };
    let r1 = by_id(1);
    let r2 = by_id(2);
    let r3 = by_id(3);
    assert!(r1["success"].as_bool().unwrap(), "{r1}");
    assert!(r2["success"].as_bool().unwrap(), "{r2}");
    assert!(r3["success"].as_bool().unwrap(), "{r3}");

    // Batch 1: metrics present, all worker fields present, cold worker.
    let m1 = &r1["metrics"];
    assert!(m1.is_object(), "batch 1 must carry a metrics object: {r1}");
    for key in REQUIRED_WORKER_FIELDS {
        assert!(m1.get(key).is_some(), "metrics missing `{key}`: {m1}");
    }
    assert!(
        m1["worker_wall_ms"].as_u64().is_some(),
        "worker_wall_ms must be an integer: {m1}"
    );
    assert!(
        m1["worker_majflt"].as_u64().is_some(),
        "worker_majflt must be an integer: {m1}"
    );
    assert!(
        !m1["worker_reused"].as_bool().unwrap(),
        "first batch after spawn must report a cold (non-reused) worker: {m1}"
    );
    // On Linux the /proc-derived fields must survive the round-trip from the
    // worker subprocess through the parent's reader thread.
    #[cfg(target_os = "linux")]
    for key in [
        "worker_rss_current_kb",
        "worker_read_bytes",
        "worker_write_bytes",
    ] {
        assert!(
            m1.get(key).is_some(),
            "Linux subprocess metrics must carry `{key}`: {m1}"
        );
    }

    // Daemon-clock fields, injected by the parent's reader thread.
    let t_dispatch = m1["t_dispatch_ms"].as_u64();
    let t_worker = m1["t_worker_ms"].as_u64();
    assert!(
        t_dispatch.is_some(),
        "metrics must carry t_dispatch_ms (daemon queue/dispatch time): {m1}"
    );
    assert!(
        t_worker.is_some(),
        "metrics must carry t_worker_ms (daemon-observed worker portion): {m1}"
    );
    // t_worker_ms brackets the worker's own wall time — it additionally covers
    // the batch read, Arrow (de)serialization, and pipe transit — so it must
    // be >= worker_wall_ms.
    assert!(
        t_worker.unwrap() >= m1["worker_wall_ms"].as_u64().unwrap(),
        "t_worker_ms ({t_worker:?}) must be >= worker_wall_ms ({:?}): {m1}",
        m1["worker_wall_ms"].as_u64()
    );

    // Batch 2: same fingerprint, warm worker reused.
    assert!(
        r2["metrics"]["worker_reused"].as_bool().unwrap(),
        "second same-fingerprint batch must report a reused worker: {}",
        r2["metrics"]
    );

    // Batch 3: did not opt in -> no metrics object at all.
    assert!(
        r3.get("metrics").is_none(),
        "non-opted batch must omit the metrics object: {r3}"
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
