//! Shared Arrow IPC helpers for reading/writing RecordBatches via shared memory.
//!
//! Every tool reads input from and writes output to POSIX shared memory using
//! Arrow IPC stream format. These helpers eliminate per-tool boilerplate.
//!
//! ## Performance note
//!
//! `write_batch_to_output_shm` serializes to a `Vec<u8>` then copies into shm.
//! Alternatives investigated and rejected:
//! - Over-allocate + ftruncate: unsafe with existing mmap (SIGBUS risk)
//! - Two-pass counting writer: viable but marginal benefit, adds complexity
//! - Temporary file: strictly worse than Vec approach
//!
//! Revisit if profiling shows this is a bottleneck for large outputs.

use crate::protocol::ShmOutput;
use crate::shm::SharedMemory;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Serialize a RecordBatch to Arrow IPC stream format and write it to a new
/// POSIX shared memory region with a PID-based name.
///
/// Registers the shm for signal-safe cleanup. The caller must call
/// `shm::deregister_cleanup(&shm_out.name)` after ownership is transferred
/// to the reader (i.e., after the JSON response is written to stdout).
pub fn write_batch_to_output_shm(batch: &RecordBatch, label: &str) -> Result<ShmOutput, String> {
    let mut ipc_buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buf, &batch.schema())
            .map_err(|e| format!("Failed to create Arrow IPC writer: {e}"))?;
        writer
            .write(batch)
            .map_err(|e| format!("Failed to write Arrow batch: {e}"))?;
        writer
            .finish()
            .map_err(|e| format!("Failed to finish Arrow IPC stream: {e}"))?;
    }

    let shm_name = crate::shm::output_shm_name(label);
    crate::shm::register_for_cleanup(&shm_name);

    let mut shm = SharedMemory::create(&shm_name, ipc_buf.len())
        .map_err(|e| format!("Failed to create output shm '{shm_name}': {e}"))?;

    shm.as_mut_slice()[..ipc_buf.len()].copy_from_slice(&ipc_buf);
    let size = ipc_buf.len();

    // Detach: munmap without unlinking. The reader (miint) will read and unlink.
    shm.detach();

    Ok(ShmOutput::new(shm_name, label, size))
}

/// Read all RecordBatches from an Arrow IPC stream in shared memory.
pub fn read_batches_from_shm(shm_name: &str) -> Result<Vec<RecordBatch>, String> {
    let shm = SharedMemory::open_readonly(shm_name)
        .map_err(|e| format!("Failed to open shm '{shm_name}': {e}"))?;
    let cursor = std::io::Cursor::new(shm.as_slice());
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("Failed to read Arrow IPC stream: {e}"))?;
    reader
        .into_iter()
        .map(|b| b.map_err(|e| format!("Failed to read Arrow batch: {e}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{read_arrow_from_shm, unique_shm_name, write_arrow_to_shm};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_write_batch_to_output_shm() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let shm_out = write_batch_to_output_shm(&batch, "test").unwrap();
        assert_eq!(shm_out.label, "test");
        assert!(shm_out.size > 0);
        assert!(shm_out.name.contains("gpl-boundary-"));
        assert!(shm_out.name.ends_with("-test"));

        // Verify the data is readable
        let batches = read_arrow_from_shm(&shm_out.name);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        let id_col = batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(2), 3);

        // Deregister and clean up
        crate::shm::deregister_cleanup(&shm_out.name);
        let _ = SharedMemory::unlink(&shm_out.name);
    }

    #[test]
    fn test_read_batches_from_shm() {
        let name = unique_shm_name("aipc-read");
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["hello", "world"]))],
        )
        .unwrap();

        let _shm = write_arrow_to_shm(&name, &batch);

        let result = read_batches_from_shm(&name).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);

        let col = result[0]
            .column_by_name("x")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "hello");
        assert_eq!(col.value(1), "world");

        let _ = SharedMemory::unlink(&name);
    }

    #[test]
    fn test_read_batches_nonexistent_shm() {
        let result = read_batches_from_shm("/nonexistent-shm-12345");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to open shm"));
    }
}
