//! Integration tests for batched streaming protocol.
//! Tests the full binary with NDJSON on stdin/stdout.

use std::io::Write;
use std::process::{Command, Stdio};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Write an Arrow RecordBatch to a POSIX shared memory segment.
/// Returns the segment name (caller must unlink).
fn write_test_shm(name: &str, batch: &RecordBatch) {
    use std::ffi::CString;

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
        libc::ftruncate(fd, ipc_buf.len() as libc::off_t);
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            ipc_buf.len(),
            libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        );
        libc::close(fd);
        std::ptr::copy_nonoverlapping(ipc_buf.as_ptr(), ptr as *mut u8, ipc_buf.len());
        libc::munmap(ptr, ipc_buf.len());
    }
}

/// Unlink a POSIX shared memory segment (best-effort).
fn unlink_shm(name: &str) {
    let c_name = std::ffi::CString::new(name).unwrap();
    unsafe {
        libc::shm_unlink(c_name.as_ptr());
    }
}

/// Generate a unique shm name for integration tests.
fn test_shm_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("/{prefix}-{pid}-{n}")
}

// Prodigal test sequences (from prodigal.rs)
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

#[test]
fn test_single_shot_ndjson() {
    // Verify that single-shot mode (no stream flag) works with the new
    // line-by-line reading in main.rs.
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
                .write_all(b"{\"tool\":\"fasttree\",\"shm_input\":\"/nonexistent\"}\n")
                .unwrap();
            child.wait_with_output()
        })
        .expect("Failed to run gpl-boundary");

    // Should fail (bad shm) but produce valid JSON response
    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let resp: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert!(!resp["success"].as_bool().unwrap());
}

#[test]
fn test_streaming_unsupported_tool() {
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
                .write_all(b"{\"tool\":\"fasttree\",\"shm_input\":\"/x\",\"stream\":true}\n")
                .unwrap();
            child.wait_with_output()
        })
        .expect("Failed to run gpl-boundary");

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let resp: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert!(!resp["success"].as_bool().unwrap());
    assert!(
        resp["error"]
            .as_str()
            .unwrap()
            .contains("does not support streaming"),
        "Expected 'does not support streaming', got: {}",
        resp["error"]
    );
}

#[test]
fn test_streaming_prodigal_two_batches() {
    let shm1 = test_shm_name("st-pd-b1");
    let shm2 = test_shm_name("st-pd-b2");

    let batch1 = make_prodigal_batch(&["contig_1"], &[TEST_SEQ_1]);
    let batch2 = make_prodigal_batch(&["contig_2"], &[TEST_SEQ_2]);
    write_test_shm(&shm1, &batch1);
    write_test_shm(&shm2, &batch2);

    let request_line = format!(
        "{{\"tool\":\"prodigal\",\"config\":{{\"meta_mode\":true}},\"shm_input\":\"{shm1}\",\"stream\":true}}\n"
    );
    let batch_line = format!("{{\"shm_input\":\"{shm2}\"}}\n");

    let mut child = Command::new(env!("CARGO_BIN_EXE_gpl-boundary"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn gpl-boundary");

    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(request_line.as_bytes()).unwrap();
        stdin.write_all(batch_line.as_bytes()).unwrap();
    }
    // Close stdin (EOF) by dropping the child's stdin handle
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("Failed to wait");
    assert!(
        output.status.success(),
        "Process failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Parse stdout as 2 NDJSON lines
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "Expected 2 response lines, got {}:\n{}",
        lines.len(),
        stdout
    );

    let resp1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    let resp2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert!(
        resp1["success"].as_bool().unwrap(),
        "Batch 1 failed: {resp1}"
    );
    assert!(
        resp2["success"].as_bool().unwrap(),
        "Batch 2 failed: {resp2}"
    );

    // Both should have schema_version
    assert!(resp1["schema_version"].as_u64().is_some());
    assert!(resp2["schema_version"].as_u64().is_some());

    // Both should find genes
    assert!(resp1["result"]["n_genes"].as_i64().unwrap() > 0);
    assert!(resp2["result"]["n_genes"].as_i64().unwrap() > 0);

    // Clean up output shm
    if let Some(outputs) = resp1["shm_outputs"].as_array() {
        for out in outputs {
            unlink_shm(out["name"].as_str().unwrap());
        }
    }
    if let Some(outputs) = resp2["shm_outputs"].as_array() {
        for out in outputs {
            unlink_shm(out["name"].as_str().unwrap());
        }
    }

    // Clean up input shm
    unlink_shm(&shm1);
    unlink_shm(&shm2);
}

#[test]
fn test_streaming_error_recovery() {
    // Batch 1: valid prodigal input
    // Batch 2: nonexistent shm (error, but session should continue until EOF)
    let shm1 = test_shm_name("st-pd-err");
    let batch1 = make_prodigal_batch(&["contig_1"], &[TEST_SEQ_1]);
    write_test_shm(&shm1, &batch1);

    let request_line = format!(
        "{{\"tool\":\"prodigal\",\"config\":{{\"meta_mode\":true}},\"shm_input\":\"{shm1}\",\"stream\":true}}\n"
    );
    let batch_line = "{\"shm_input\":\"/nonexistent-shm-for-error-test\"}\n";

    let mut child = Command::new(env!("CARGO_BIN_EXE_gpl-boundary"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn gpl-boundary");

    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(request_line.as_bytes()).unwrap();
        stdin.write_all(batch_line.as_bytes()).unwrap();
    }
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("Failed to wait");

    // Parse stdout — should have 2 lines
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "Expected 2 response lines, got {}:\n{}",
        lines.len(),
        stdout
    );

    let resp1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    let resp2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();

    // First batch succeeds
    assert!(
        resp1["success"].as_bool().unwrap(),
        "Batch 1 should succeed"
    );

    // Second batch fails (bad shm)
    assert!(!resp2["success"].as_bool().unwrap(), "Batch 2 should fail");
    assert!(
        resp2["error"]
            .as_str()
            .unwrap()
            .contains("Failed to open shm"),
        "Expected shm error, got: {}",
        resp2["error"]
    );

    // Clean up
    if let Some(outputs) = resp1["shm_outputs"].as_array() {
        for out in outputs {
            unlink_shm(out["name"].as_str().unwrap());
        }
    }
    unlink_shm(&shm1);
}
