//! Shared test utilities for Arrow IPC + shared memory roundtrips.
//!
//! Intentionally duplicates IPC serialization logic from `arrow_ipc.rs` so that
//! tests exercise production read/write paths against independently-written data.

use crate::shm::SharedMemory;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::path::PathBuf;
use std::sync::OnceLock;

/// Generate a unique shm name using prefix, PID, and an atomic counter.
/// Uses a counter instead of a timestamp to keep names under the macOS
/// POSIX shm 31-character limit. PID prevents collisions across concurrent
/// test processes; the counter prevents collisions within a process.
pub fn unique_shm_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("/{prefix}-{pid}-{n}")
}

/// Write an Arrow RecordBatch as IPC stream into a new shared memory region.
///
/// The returned `SharedMemory` holds the mapping open. The shm segment is
/// unlinked on drop, so keep the return value alive as long as readers need it.
pub fn write_arrow_to_shm(name: &str, batch: &RecordBatch) -> SharedMemory {
    let mut ipc_buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }

    let mut shm = SharedMemory::create(name, ipc_buf.len()).unwrap();
    shm.as_mut_slice()[..ipc_buf.len()].copy_from_slice(&ipc_buf);
    shm
}

/// Read all Arrow RecordBatches from an IPC stream in shared memory.
///
/// Delegates to `arrow_ipc::read_batches_from_shm` so tests exercise the
/// **same** decoder (`StreamDecoder::with_require_alignment(true)`) as
/// production. Using a separate `StreamReader`-based path here would
/// hide alignment / trailing-data bugs from the test suite — exactly
/// the class of mismatch that masked the original macOS `fstat` bug.
///
/// `size` is the exact data byte count. Callers reading their own input
/// segments can use `holder.len()` from the `SharedMemory` returned by
/// `write_arrow_to_shm`; callers reading gpl-boundary output segments
/// should pass `ShmOutput::size` from the response.
pub fn read_arrow_from_shm(name: &str, size: usize) -> Vec<RecordBatch> {
    crate::arrow_ipc::read_batches_from_shm(name, size).unwrap()
}

/// Extract `16S500/16S.1.p` from `ext/fasttree/16S_500.tar.gz` to
/// `target/fasttree-testdata/16S.1.p` (once per process), returning the
/// extracted path.
///
/// Shells out to the system `tar` to avoid pulling `tar`+`flate2` crates
/// just for one fixture.  Tests panic if `tar` is not on `$PATH`.
pub fn extracted_16s_phylip() -> PathBuf {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let tar_path = format!("{manifest_dir}/ext/fasttree/16S_500.tar.gz");
        let out_dir = PathBuf::from(format!("{manifest_dir}/target/fasttree-testdata"));
        std::fs::create_dir_all(&out_dir).expect("create target/fasttree-testdata");
        let out_path = out_dir.join("16S.1.p");
        if !out_path.exists() {
            let status = std::process::Command::new("tar")
                .args(["-xzf", &tar_path, "-C"])
                .arg(&out_dir)
                .args(["--strip-components=1", "16S500/16S.1.p"])
                .status()
                .expect("invoke tar");
            assert!(status.success(), "tar extraction of 16S.1.p failed");
        }
        out_path
    })
    .clone()
}

/// Parse an interleaved-PHYLIP alignment file into `(name, sequence)` pairs.
///
/// Header is " <n_seqs> <n_positions>". First block carries names in
/// columns 0–9 (right-padded); subsequent blocks are anonymous continuation
/// chunks. Spaces inside sequence chunks are stripped. Sequence characters
/// (gaps, IUPAC codes, etc.) are passed through verbatim and uppercased so
/// FastTree's nucleotide auto-detection sees a consistent alphabet.
///
/// Limitations (acceptable for our test fixture, not general-purpose):
/// - Names assumed ASCII: byte-indexed `&line[..10]` will panic on a
///   multi-byte UTF-8 boundary. Standard PHYLIP names are ASCII-only.
/// - Block boundaries are detected purely by blank lines; a stray
///   mid-block blank line (e.g., from a malformed file) silently
///   misaligns subsequent blocks.
pub fn parse_interleaved_phylip(path: &std::path::Path) -> Vec<(String, String)> {
    let raw = std::fs::read_to_string(path).expect("read PHYLIP file");
    let mut lines = raw.lines();
    let header = lines.next().expect("PHYLIP header line");
    let mut header_parts = header.split_whitespace();
    let n_seqs: usize = header_parts
        .next()
        .expect("n_seqs in header")
        .parse()
        .expect("n_seqs parse");
    let n_pos: usize = header_parts
        .next()
        .expect("n_pos in header")
        .parse()
        .expect("n_pos parse");

    let mut names: Vec<String> = Vec::with_capacity(n_seqs);
    let mut seqs: Vec<String> = vec![String::with_capacity(n_pos); n_seqs];

    let mut block_idx = 0usize;
    let mut row_in_block = 0usize;
    for line in lines {
        if line.trim().is_empty() {
            // Block separator.
            if row_in_block != 0 {
                assert_eq!(row_in_block, n_seqs, "incomplete PHYLIP block");
                block_idx += 1;
                row_in_block = 0;
            }
            continue;
        }
        let (name_part, seq_part) = if block_idx == 0 {
            // First block: cols 0–9 are the name (right-padded with spaces).
            assert!(line.len() > 10, "PHYLIP first-block line too short");
            (Some(line[..10].trim().to_string()), &line[10..])
        } else {
            (None, line)
        };
        if let Some(n) = name_part {
            names.push(n);
        }
        let chunk: String = seq_part
            .chars()
            .filter(|c| !c.is_whitespace())
            .map(|c| c.to_ascii_uppercase())
            .collect();
        seqs[row_in_block].push_str(&chunk);
        row_in_block += 1;
    }
    if row_in_block != 0 {
        assert_eq!(row_in_block, n_seqs, "trailing incomplete PHYLIP block");
    }

    assert_eq!(names.len(), n_seqs, "name count mismatch");
    for (i, s) in seqs.iter().enumerate() {
        assert_eq!(
            s.len(),
            n_pos,
            "seq {i} ({}) length {} != header n_pos {}",
            names[i],
            s.len(),
            n_pos
        );
    }
    names.into_iter().zip(seqs).collect()
}

/// Take the first `n` `(name, sequence)` pairs from an alignment.
pub fn subset_alignment(alignment: &[(String, String)], n: usize) -> Vec<(String, String)> {
    alignment.iter().take(n).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_arrow_shm_roundtrip() {
        let name = unique_shm_name("test-util-rt");

        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, false),
            Field::new("col_b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
                Arc::new(StringArray::from(vec!["1", "2", "3"])),
            ],
        )
        .unwrap();

        let holder = write_arrow_to_shm(&name, &batch);

        let batches = read_arrow_from_shm(&name, holder.len());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 2);

        let col_a = batches[0]
            .column_by_name("col_a")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col_a.value(0), "x");
        assert_eq!(col_a.value(2), "z");

        let _ = SharedMemory::unlink(&name);
    }

    #[test]
    fn test_phylip_extract_and_parse_16s_1() {
        let path = extracted_16s_phylip();
        assert!(path.exists(), "extraction did not produce file");
        let alignment = parse_interleaved_phylip(&path);
        // 16S.1.p header is "500 1287".
        assert_eq!(alignment.len(), 500);
        for (name, seq) in &alignment {
            assert!(!name.is_empty());
            assert_eq!(seq.len(), 1287, "seq for {name} has wrong length");
            // Alignment alphabet: {A,C,G,T,N,U,R,Y,S,W,K,M,B,D,H,V,-}.
            // We don't enforce the full IUPAC set; just sanity-check that
            // characters are uppercase and not whitespace.
            for c in seq.chars() {
                assert!(c.is_ascii_uppercase() || c == '-');
            }
        }

        let subset = subset_alignment(&alignment, 50);
        assert_eq!(subset.len(), 50);
        assert_eq!(subset[0], alignment[0]);
        assert_eq!(subset[49], alignment[49]);
    }
}
