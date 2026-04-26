//! Integration test for the bowtie2-build tool.
//!
//! Verifies that gpl-boundary can build a bowtie2 index from Arrow input
//! (name + sequence columns) and that the resulting .bt2 files are usable
//! by the bowtie2-align tool. Round-trip ensures the index is real.

use std::ffi::CString;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Best-effort `shm_unlink`. Used by the `ShmGuard` Drop impl below; calling
/// on a name that doesn't exist (already cleaned, never created) is a no-op.
fn unlink_shm(name: &str) {
    let c_name = CString::new(name).unwrap();
    unsafe {
        libc::shm_unlink(c_name.as_ptr());
    }
}

/// RAII handle for a POSIX shm name. Drop unlinks the segment, including on
/// panic — without this, a failing `assert!` between create and the explicit
/// unlink call would leak the shm into `/dev/shm` and pollute subsequent
/// runs (especially under `cargo test` with re-used PIDs).
struct ShmGuard {
    name: String,
}

impl ShmGuard {
    fn create_with_batch(prefix: &str, batch: &RecordBatch) -> Self {
        let name = unique_test_shm_name(prefix);
        write_test_shm(&name, batch);
        Self { name }
    }

    /// Take a name returned by gpl-boundary as an output shm and wrap it in
    /// a guard so it gets unlinked even if the assertions below panic.
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

fn make_fasta_batch(names: &[&str], sequences: &[&str]) -> RecordBatch {
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

/// Drive a daemon session that runs exactly one batch and shuts down. Returns
/// the batch's response object (skipping the init reply line).
fn run_request(req: serde_json::Value) -> serde_json::Value {
    let mut session = String::new();
    session.push_str("{\"init\":{}}\n");
    session.push_str(&serde_json::to_string(&req).unwrap());
    session.push('\n');
    session.push_str("{\"shutdown\":true}\n");

    let output = Command::new(env!("CARGO_BIN_EXE_gpl-boundary"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(session.as_bytes())
                .unwrap();
            child.wait_with_output()
        })
        .expect("Failed to run gpl-boundary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert!(
        lines.len() >= 2,
        "Expected at least init reply + 1 batch response. \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    // lines[0] is the init reply (success + protocol_version). The batch
    // response is the next line.
    serde_json::from_str(lines[1]).unwrap_or_else(|e| {
        panic!(
            "Failed to parse batch response JSON: {e}\nline: {}",
            lines[1]
        )
    })
}

#[test]
fn test_bowtie2_build_creates_index() {
    let tmp = tempfile::tempdir().unwrap();
    let index_path = tmp.path().join("idx").to_string_lossy().into_owned();

    let names = ["seq1", "seq2", "seq3"];
    // ~150 bases each — small enough to build instantly, large enough that
    // the resulting index is non-trivial.
    let sequences = [
        "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT",
        "TGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCA",
        "AAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCC",
    ];
    let total_bases: i64 = sequences.iter().map(|s| s.len() as i64).sum();

    let batch = make_fasta_batch(&names, &sequences);
    let input = ShmGuard::create_with_batch("bt2-build-it", &batch);

    let resp = run_request(serde_json::json!({
        "tool": "bowtie2-build",
        "config": { "index_path": index_path },
        "shm_input": input.name(),
    }));

    assert!(
        resp["success"].as_bool().unwrap_or(false),
        "Build failed: {resp:?}"
    );
    assert_eq!(resp["schema_version"].as_u64().unwrap(), 1);

    for suffix in &[
        ".1.bt2",
        ".2.bt2",
        ".3.bt2",
        ".4.bt2",
        ".rev.1.bt2",
        ".rev.2.bt2",
    ] {
        let path = format!("{index_path}{suffix}");
        assert!(
            std::path::Path::new(&path).exists(),
            "Expected {path} to exist after bowtie2-build"
        );
    }

    let result = &resp["result"];
    assert_eq!(result["n_sequences"].as_i64().unwrap(), 3);
    assert_eq!(result["n_bases"].as_i64().unwrap(), total_bases);
    assert!(result["elapsed_ms"].as_i64().unwrap() >= 0);
    let files = result["index_files"].as_array().expect("index_files array");
    assert_eq!(files.len(), 6, "Expected 6 .bt2 files, got: {files:?}");
}

#[test]
fn test_bowtie2_build_missing_index_path() {
    let names = ["seq1"];
    let sequences = ["ACGTACGTACGTACGTACGTACGTACGT"];
    let batch = make_fasta_batch(&names, &sequences);
    let input = ShmGuard::create_with_batch("bt2-build-no-path", &batch);

    let resp = run_request(serde_json::json!({
        "tool": "bowtie2-build",
        "config": {},
        "shm_input": input.name(),
    }));

    assert!(!resp["success"].as_bool().unwrap_or(true));
    let err = resp["error"].as_str().unwrap_or("");
    assert!(
        err.contains("index_path"),
        "Expected error mentioning index_path, got: {err}"
    );
}

#[test]
fn test_bowtie2_build_round_trip_with_align() {
    // Build an index with bowtie2-build, then align a synthetic read against
    // it with bowtie2-align. Confirms the index is real and usable.
    let tmp = tempfile::tempdir().unwrap();
    let index_path = tmp.path().join("rt").to_string_lossy().into_owned();

    let reference = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT\
                     TGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCA\
                     AAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGG";
    let names = ["ref1"];
    let sequences = [reference];
    let batch = make_fasta_batch(&names, &sequences);
    let build_input = ShmGuard::create_with_batch("bt2-rt-build", &batch);

    let build_resp = run_request(serde_json::json!({
        "tool": "bowtie2-build",
        "config": { "index_path": index_path },
        "shm_input": build_input.name(),
    }));
    assert!(
        build_resp["success"].as_bool().unwrap_or(false),
        "Build phase failed: {build_resp:?}"
    );

    // Align a 30bp slice of the reference back against the index
    let read_seq: String = reference.chars().skip(20).take(30).collect();
    let read_qual = "I".repeat(30);
    let align_schema = Arc::new(Schema::new(vec![
        Field::new("read_id", DataType::Utf8, false),
        Field::new("sequence1", DataType::Utf8, false),
        Field::new("qual1", DataType::Utf8, true),
    ]));
    let align_batch = RecordBatch::try_new(
        align_schema,
        vec![
            Arc::new(StringArray::from(vec!["r1"])),
            Arc::new(StringArray::from(vec![read_seq.as_str()])),
            Arc::new(StringArray::from(vec![Some(read_qual.as_str())])),
        ],
    )
    .unwrap();
    let align_input = ShmGuard::create_with_batch("bt2-rt-align", &align_batch);

    let align_resp = run_request(serde_json::json!({
        "tool": "bowtie2-align",
        "config": { "index_path": index_path, "seed": 42 },
        "shm_input": align_input.name(),
    }));

    // Adopt the output shm into a guard *before* the assertions below, so a
    // failed alignment assertion still cleans up the output shm gpl-boundary
    // created. Vec-of-guards keeps them all alive until end of test.
    let _output_guards: Vec<ShmGuard> = align_resp["shm_outputs"]
        .as_array()
        .map(|outs| {
            outs.iter()
                .filter_map(|o| o["name"].as_str().map(ShmGuard::adopt))
                .collect()
        })
        .unwrap_or_default();

    assert!(
        align_resp["success"].as_bool().unwrap_or(false),
        "Align phase failed: {align_resp:?}"
    );
    assert_eq!(align_resp["result"]["n_reads"].as_i64().unwrap(), 1);
    assert_eq!(
        align_resp["result"]["n_aligned"].as_i64().unwrap(),
        1,
        "Synthetic read failed to align — index may be corrupt: {align_resp:?}"
    );
}
