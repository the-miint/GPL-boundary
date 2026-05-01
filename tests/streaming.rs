//! Integration tests for the daemon-mode NDJSON protocol (Phase 3).
//!
//! Each invocation of gpl-boundary is a session driven by:
//!   1. `{"init": {...}}` — required first line
//!   2. zero or more `{"tool": "...", "config": {...}, "shm_input": "..."}`
//!   3. `{"shutdown": true}` OR EOF OR idle timeout — graceful exit

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
// Shm helpers — RAII guard so a panicking assertion can't leak segments
// ---------------------------------------------------------------------------

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

fn unlink_shm(name: &str) {
    let c_name = CString::new(name).unwrap();
    unsafe {
        libc::shm_unlink(c_name.as_ptr());
    }
}

fn unique_test_shm_name(prefix: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("/{prefix}-{pid}-{n}")
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

// ---------------------------------------------------------------------------
// Test data
// ---------------------------------------------------------------------------

const TEST_SEQ_1: &str = "ACAGGTTAGAAACTACTCTGTTTTCTGGCTCCTTGTTTAATGCCCTGTCCTATTTTATTGCGAAAATTGTCTGTTTTTCACAGAAAACTGAGAGTAGTCAAGGGATTCCTTGTCCTTTGCTTTGGTCTGCACAGCTGTCTTGTTTTAAGGCCAAGTGGAATGAGACAGCTGACTCTTCAGGTGTGAAAACTTGGATGTAGGTGTAGATTGGCTCTCAGTTGACCTCCAGCTGGTGCAGATTCTTCAGTTTGTTTGATGGAGCTTTGAGAAGTCCTTTCAGACTGAAGGATACTCTGAATTTTAGCTATGGAGATAATGTGGATGTTGTGTGTTTAAGTCCTGTTTGCAGTTTTTTTCTGTTCAGTCAGTTATTTTACTGTGTGAGTCAGGACCCTTAGAAGCCCTCAGTGGCAACCACAGAGCGCACAGTTAATTTTCTGTGCAAGAAAATTAAGATCATACTCTGTGTCCAGGAAAGTCAAGAATATTCCTGGTTTTCTCTACTGTAAAATTTTATCTTGTAACTTGTGTTTGGGTCTGCATGATTATTCAAAAATCTTAGTAGATTTGGAAGGATGTTGCATATTATGGAAACAAAGTTGGAAAAAGTTTGTATCAGTTGCAGTATTTCTTCACATCATTTNTTAACNNCNTNNNNNNNNNGCTTCTGCCACTTGAAAAGACAAATTAAAAACNAATTTATAATGCTTATATGCTTTAGTTACATTNGGGTCTTTCAGTAACTTTAGTGCTTTTGATAGCCATACCTGTGAGNTTGACAGTGTCTAAAATTAGAAGTGTTCCTTTTCTTCTGCTCTTCCCATTCTCGTGTGTCTTCAATAGTTTCTGCAAATAATGATGTGCAGACTTAGCATTGATTCAACAGCAGAGGTAAGCATACCTGTGGCTTACTTGGCTTCAGCTTATCCAGCAGTGCCAACCACTCTCTGTTTGTCTTAC";
const TEST_SEQ_2: &str = "ACAGGTTAGAAACTACTCTGTTTTCTGGCTGCTTGTTTAATGCCCTCTCCTATTTTATTGTGACGATTGTCTGTTTTTCACAGAAAACTGAGAGTAGTCAAGGGATTCCTTGTCCTTTGCTTTGGTGTGCACAGCTGTCTTGTTTTAAGGCCCAGTGGAATGAGACAGCTGACTCTTCAGGTGTGAAAACTTGGATGTACGTGTAGATTGGCTCTCAGTTGACCTCCAGCTGGTGCAGATTCTTCAGTTTGTTTGATGGAGCTTTGAGAAGTCCTTTCAGACTGAAGGATACTCTGAATTTTAGCTATGGAAATAATGTGGATGTTGTGTGTTTAAGCACTGTTTGCAGTTTTTTTCTGTTCAGTCAGTTATTTTACTGTGTGAGTCAGGACTCTTAGAAGCCCTCTGTGGCAACCACAGAGCGCATAGTTAATTTTCTGTACAAGAAAATTAAGATCCTACTCAGTGTTCAGGAAAGTCAAGAATATTCCTGGTTTTCTCTACTGTAAAATTTTATCTTGTAACTTGTGTTTGGGTCTGCATGATTATTCAAAAATCTTAGTAGATTTGGAAGGATGTTGCATTTTATGGAAACAAAGTTGGGAAAAGTTTGTATCAGTTCCAGTATTTCTTCACATCATTTNTTAACNNCNTNNNNNNNNNGCTTCTACCACTTGAAAAGACAAATTAAAAACNAATTTATAATGCTTATATGCTTTAGTTACATTNGGGTCTTTCAGTAACTTTAGTGCTTTACTTAGCCATACCTGTGAGCNTGGCAGTGTCTAAAATTAGAAGTGTTCCTTTTCTTCTGCTCTTCCCATTCTCGTGTGTCTTCAATAGTTTCTGCAAATAATGATGTGCAGACTTAGCATTGGTTCAACAGCAGAGGTAAAAATACCTGTGGCTTACCCGGCTTCAGCTTATCCAGCAGTGCCAACCACTCTCTGTTTGTCTTAC";

fn make_prodigal_batch(names: &[&str], sequences: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("sequence", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(StringArray::from(sequences.to_vec())),
        ],
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Subprocess helpers
// ---------------------------------------------------------------------------

fn spawn_daemon() -> Child {
    Command::new(env!("CARGO_BIN_EXE_gpl-boundary"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn gpl-boundary")
}

/// Parse stdout as NDJSON lines.
fn parse_lines(stdout: &[u8]) -> Vec<serde_json::Value> {
    String::from_utf8_lossy(stdout)
        .trim()
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).expect(l))
        .collect()
}

/// Build an init JSON line ending in '\n'.
fn init_line(idle_timeout_ms: Option<u64>) -> String {
    match idle_timeout_ms {
        Some(ms) => format!("{{\"init\":{{\"idle_timeout_ms\":{ms}}}}}\n"),
        None => "{\"init\":{}}\n".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Phase 3: shutdown message + idle timeout
// ---------------------------------------------------------------------------

#[test]
fn test_shutdown_message_exits_cleanly() {
    let shm1 = ShmGuard::create_with_batch("ph3-sd", &make_prodigal_batch(&["c1"], &[TEST_SEQ_1]));
    let mut child = spawn_daemon();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(init_line(None).as_bytes()).unwrap();
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"prodigal\",\"config\":{{\"meta_mode\":true}},\
                     \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":1}}\n",
                    shm1.name(),
                    shm1.size()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("Failed to wait");
    assert!(
        out.status.success(),
        "Process exited non-zero: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 2, "Expected init reply + 1 batch response");

    // First line: init reply with protocol_version
    assert!(lines[0]["success"].as_bool().unwrap());
    assert_eq!(lines[0]["protocol_version"].as_u64().unwrap(), 2);

    // Second line: batch response with batch_id echoed
    assert!(lines[1]["success"].as_bool().unwrap());
    assert_eq!(lines[1]["batch_id"].as_u64().unwrap(), 1);

    // Clean up output shm
    if let Some(outputs) = lines[1]["shm_outputs"].as_array() {
        for o in outputs {
            unlink_shm(o["name"].as_str().unwrap());
        }
    }
}

#[test]
fn test_idle_timeout_self_exits() {
    let mut child = spawn_daemon();
    {
        let stdin = child.stdin.as_mut().unwrap();
        // 200 ms idle; send only the init, then keep stdin open and wait.
        stdin.write_all(init_line(Some(200)).as_bytes()).unwrap();
        stdin.flush().unwrap();
    }
    // Hold stdin open by NOT dropping it yet — daemon must self-exit on
    // its own timer rather than waiting for EOF.
    let started = Instant::now();
    // Poll for completion up to 5s.
    let mut exited = false;
    while started.elapsed() < Duration::from_secs(5) {
        if let Ok(Some(_)) = child.try_wait() {
            exited = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    if !exited {
        let _ = child.kill();
        panic!("Daemon did not self-exit within 5s of a 200 ms idle_timeout_ms");
    }

    // Drop stdin, collect output, sanity-check init reply was written.
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("Failed to wait");
    assert!(out.status.success(), "Idle-timeout exit was non-zero");
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 1, "Expected init reply only");
    assert!(lines[0]["success"].as_bool().unwrap());
}

#[test]
fn test_idle_timeout_disabled_stays_alive() {
    let shm1 =
        ShmGuard::create_with_batch("ph3-noidle-1", &make_prodigal_batch(&["c1"], &[TEST_SEQ_1]));
    let shm2 =
        ShmGuard::create_with_batch("ph3-noidle-2", &make_prodigal_batch(&["c2"], &[TEST_SEQ_2]));

    let mut child = spawn_daemon();
    {
        let stdin = child.stdin.as_mut().unwrap();
        // idle_timeout 0 → disabled; daemon must wait for batches indefinitely.
        stdin.write_all(init_line(Some(0)).as_bytes()).unwrap();
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"prodigal\",\"config\":{{\"meta_mode\":true}},\
                     \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":1}}\n",
                    shm1.name(),
                    shm1.size()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin.flush().unwrap();
    }

    // Wait through what would have been the default 60s timeout if 0 didn't
    // disable correctly. 500 ms is plenty — if the bug were that 0 means
    // "instant timeout", the daemon would have already exited.
    std::thread::sleep(Duration::from_millis(500));

    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"prodigal\",\"config\":{{\"meta_mode\":true}},\
                     \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":2}}\n",
                    shm2.name(),
                    shm2.size()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("Failed to wait");
    assert!(out.status.success());
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 3, "Expected init + 2 batches");
    assert!(lines[1]["success"].as_bool().unwrap());
    assert!(lines[2]["success"].as_bool().unwrap());
    assert_eq!(lines[1]["batch_id"].as_u64().unwrap(), 1);
    assert_eq!(lines[2]["batch_id"].as_u64().unwrap(), 2);

    for resp in &lines[1..] {
        if let Some(outputs) = resp["shm_outputs"].as_array() {
            for o in outputs {
                unlink_shm(o["name"].as_str().unwrap());
            }
        }
    }
}

#[test]
fn test_indefinite_batch_count() {
    // Spam 25 batches through a single session and verify each succeeds with
    // the expected batch_id echo. 25 keeps the test well under a second on
    // CI but is plenty to expose any per-batch leak or context corruption.
    let mut child = spawn_daemon();
    let mut guards: Vec<ShmGuard> = Vec::new();

    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(init_line(None).as_bytes()).unwrap();
        for i in 0..25 {
            let g = ShmGuard::create_with_batch(
                "ph3-many",
                &make_prodigal_batch(
                    &[&format!("c{i}")],
                    &[if i % 2 == 0 { TEST_SEQ_1 } else { TEST_SEQ_2 }],
                ),
            );
            stdin
                .write_all(
                    format!(
                        "{{\"tool\":\"prodigal\",\"config\":{{\"meta_mode\":true}},\
                         \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":{i}}}\n",
                        g.name(),
                        g.size()
                    )
                    .as_bytes(),
                )
                .unwrap();
            guards.push(g);
        }
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("Failed to wait");
    assert!(out.status.success());
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 1 + 25, "Expected init reply + 25 responses");

    for (i, line) in lines.iter().skip(1).enumerate() {
        assert!(
            line["success"].as_bool().unwrap(),
            "Batch {i} failed: {line}"
        );
        assert_eq!(line["batch_id"].as_u64().unwrap(), i as u64);
        if let Some(outputs) = line["shm_outputs"].as_array() {
            for o in outputs {
                unlink_shm(o["name"].as_str().unwrap());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Carried over from the old streaming.rs — verify the daemon protocol
// still satisfies the contracts the old `stream:true` mode covered
// ---------------------------------------------------------------------------

#[test]
fn test_streaming_prodigal_two_batches() {
    let shm1 =
        ShmGuard::create_with_batch("ph3-pd-1", &make_prodigal_batch(&["c1"], &[TEST_SEQ_1]));
    let shm2 =
        ShmGuard::create_with_batch("ph3-pd-2", &make_prodigal_batch(&["c2"], &[TEST_SEQ_2]));

    let mut child = spawn_daemon();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(init_line(None).as_bytes()).unwrap();
        for (i, (name, size)) in [(shm1.name(), shm1.size()), (shm2.name(), shm2.size())]
            .iter()
            .enumerate()
        {
            stdin
                .write_all(
                    format!(
                        "{{\"tool\":\"prodigal\",\"config\":{{\"meta_mode\":true}},\
                         \"shm_input\":\"{name}\",\"shm_input_size\":{size},\"batch_id\":{i}}}\n"
                    )
                    .as_bytes(),
                )
                .unwrap();
        }
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("Failed to wait");
    assert!(out.status.success());
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 3);

    for line in &lines[1..] {
        assert!(line["success"].as_bool().unwrap());
        assert!(line["schema_version"].as_u64().is_some());
        assert!(line["result"]["n_genes"].as_i64().unwrap() > 0);
        if let Some(outputs) = line["shm_outputs"].as_array() {
            for o in outputs {
                unlink_shm(o["name"].as_str().unwrap());
            }
        }
    }
}

#[test]
fn test_streaming_error_recovery() {
    // Batch 1: valid prodigal input. Batch 2: bad shm — must error but
    // session continues until shutdown.
    let shm1 = ShmGuard::create_with_batch("ph3-err", &make_prodigal_batch(&["c1"], &[TEST_SEQ_1]));

    let mut child = spawn_daemon();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(init_line(None).as_bytes()).unwrap();
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"prodigal\",\"config\":{{\"meta_mode\":true}},\
                     \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":1}}\n",
                    shm1.name(),
                    shm1.size()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin
            .write_all(
                b"{\"tool\":\"prodigal\",\"config\":{\"meta_mode\":true},\
                  \"shm_input\":\"/nonexistent-shm-for-error-test\",\"shm_input_size\":1024,\"batch_id\":2}\n",
            )
            .unwrap();
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("Failed to wait");
    assert!(out.status.success());
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 3);

    assert!(lines[1]["success"].as_bool().unwrap());
    assert_eq!(lines[1]["batch_id"].as_u64().unwrap(), 1);

    assert!(!lines[2]["success"].as_bool().unwrap());
    assert!(
        lines[2]["error"]
            .as_str()
            .unwrap()
            .contains("Failed to open shm"),
        "Expected shm error, got: {}",
        lines[2]["error"]
    );
    assert_eq!(lines[2]["batch_id"].as_u64().unwrap(), 2);

    if let Some(outputs) = lines[1]["shm_outputs"].as_array() {
        for o in outputs {
            unlink_shm(o["name"].as_str().unwrap());
        }
    }
}

// ---------------------------------------------------------------------------
// Negative-path tests
// ---------------------------------------------------------------------------

#[test]
fn test_first_message_must_be_init() {
    // Sending a batch as the first message should produce an error and
    // a non-zero exit code.
    let shm =
        ShmGuard::create_with_batch("ph3-noinit", &make_prodigal_batch(&["c1"], &[TEST_SEQ_1]));
    let mut child = spawn_daemon();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"prodigal\",\"config\":{{\"meta_mode\":true}},\
                     \"shm_input\":\"{}\",\"shm_input_size\":{}}}\n",
                    shm.name(),
                    shm.size()
                )
                .as_bytes(),
            )
            .unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("Failed to wait");
    assert!(!out.status.success());
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 1);
    assert!(!lines[0]["success"].as_bool().unwrap());
    assert!(
        lines[0]["error"]
            .as_str()
            .unwrap()
            .contains("First message must be 'init'"),
        "Unexpected error message: {}",
        lines[0]["error"]
    );
}

#[test]
fn test_duplicate_init_is_fatal() {
    // A second init mid-session aborts the session — the original config's
    // fingerprint and streaming context are no longer safe to assume the
    // client expects, so we surface the error and exit. The client can
    // restart with a fresh process.
    let mut child = spawn_daemon();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(init_line(None).as_bytes()).unwrap();
        stdin
            .write_all(b"{\"init\":{\"idle_timeout_ms\":1000}}\n")
            .unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("Failed to wait");
    let lines = parse_lines(&out.stdout);
    // init reply, then error response for the duplicate init.
    assert_eq!(lines.len(), 2);
    assert!(lines[0]["success"].as_bool().unwrap());
    assert!(!lines[1]["success"].as_bool().unwrap());
    assert!(
        lines[1]["error"]
            .as_str()
            .unwrap()
            .contains("Duplicate 'init' message"),
        "Unexpected error: {}",
        lines[1]["error"]
    );
}

#[test]
fn test_fingerprint_ignores_config_key_order() {
    // Two batches with the same prodigal config but different JSON key
    // ordering must NOT trigger the single-fingerprint mismatch error.
    // The fingerprint check canonicalizes keys before comparison.
    let shm1 =
        ShmGuard::create_with_batch("ph3-fp-key-1", &make_prodigal_batch(&["c1"], &[TEST_SEQ_1]));
    let shm2 =
        ShmGuard::create_with_batch("ph3-fp-key-2", &make_prodigal_batch(&["c2"], &[TEST_SEQ_2]));

    let mut child = spawn_daemon();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(init_line(None).as_bytes()).unwrap();
        // Batch 1: keys in order meta_mode, trans_table.
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"prodigal\",\
                     \"config\":{{\"meta_mode\":true,\"trans_table\":11}},\
                     \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":1}}\n",
                    shm1.name(),
                    shm1.size()
                )
                .as_bytes(),
            )
            .unwrap();
        // Batch 2: same keys, reversed order.
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"prodigal\",\
                     \"config\":{{\"trans_table\":11,\"meta_mode\":true}},\
                     \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":2}}\n",
                    shm2.name(),
                    shm2.size()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("Failed to wait");
    assert!(out.status.success());
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 3);
    assert!(
        lines[1]["success"].as_bool().unwrap(),
        "Batch 1 failed: {}",
        lines[1]
    );
    assert!(
        lines[2]["success"].as_bool().unwrap(),
        "Batch 2 was rejected as a fingerprint mismatch but the config is \
         semantically identical (just key-reordered): {}",
        lines[2]
    );

    for resp in &lines[1..] {
        if let Some(outputs) = resp["shm_outputs"].as_array() {
            for o in outputs {
                unlink_shm(o["name"].as_str().unwrap());
            }
        }
    }
}

#[test]
fn test_two_fingerprints_in_one_session() {
    // Phase 4 step 2 lifts the Phase 3 single-fingerprint constraint. Two
    // batches with distinct (tool, config) fingerprints must each succeed
    // — the registry creates one InProcessSlot per fingerprint and routes
    // batches accordingly. Both prodigal contexts use meta_mode=true (so
    // both can stream) but distinct trans_table values to force different
    // fingerprints.
    let shm1 =
        ShmGuard::create_with_batch("ph4-fp-a", &make_prodigal_batch(&["c1"], &[TEST_SEQ_1]));
    let shm2 =
        ShmGuard::create_with_batch("ph4-fp-b", &make_prodigal_batch(&["c2"], &[TEST_SEQ_2]));

    let mut child = spawn_daemon();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(init_line(None).as_bytes()).unwrap();
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"prodigal\",\
                     \"config\":{{\"meta_mode\":true,\"trans_table\":11}},\
                     \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":1}}\n",
                    shm1.name(),
                    shm1.size()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin
            .write_all(
                format!(
                    "{{\"tool\":\"prodigal\",\
                     \"config\":{{\"meta_mode\":true,\"trans_table\":4}},\
                     \"shm_input\":\"{}\",\"shm_input_size\":{},\"batch_id\":2}}\n",
                    shm2.name(),
                    shm2.size()
                )
                .as_bytes(),
            )
            .unwrap();
        stdin.write_all(b"{\"shutdown\":true}\n").unwrap();
    }
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("Failed to wait");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines = parse_lines(&out.stdout);
    assert_eq!(lines.len(), 3, "Expected init reply + 2 batch responses");

    // Step 3 made dispatch async, so cross-fingerprint responses can
    // arrive in either order. Find each response by batch_id rather than
    // assuming line index = submission order.
    let batch_responses = &lines[1..];
    let r1 = batch_responses
        .iter()
        .find(|r| r["batch_id"].as_u64() == Some(1))
        .expect("missing response with batch_id=1");
    let r2 = batch_responses
        .iter()
        .find(|r| r["batch_id"].as_u64() == Some(2))
        .expect("missing response with batch_id=2");
    assert!(r1["success"].as_bool().unwrap(), "{}", r1);
    assert!(r2["success"].as_bool().unwrap(), "{}", r2);

    for resp in batch_responses {
        if let Some(outputs) = resp["shm_outputs"].as_array() {
            for o in outputs {
                unlink_shm(o["name"].as_str().unwrap());
            }
        }
    }
}
