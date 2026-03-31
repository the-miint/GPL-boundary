//! Shared test utilities for Arrow IPC + shared memory roundtrips.
//!
//! Intentionally duplicates IPC serialization logic from `arrow_ipc.rs` so that
//! tests exercise production read/write paths against independently-written data.

use crate::shm::SharedMemory;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Generate a unique shm name using prefix, PID, and nanosecond timestamp.
/// Includes PID to prevent collisions across concurrent test processes.
pub fn unique_shm_name(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    format!("/{prefix}-{pid}-{ts}")
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

/// Read all Arrow RecordBatches from IPC stream in shared memory.
pub fn read_arrow_from_shm(name: &str) -> Vec<RecordBatch> {
    let shm = SharedMemory::open_readonly(name).unwrap();
    let cursor = std::io::Cursor::new(shm.as_slice());
    let reader = StreamReader::try_new(cursor, None).unwrap();
    reader.into_iter().map(|b| b.unwrap()).collect()
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

        let _shm = write_arrow_to_shm(&name, &batch);

        let batches = read_arrow_from_shm(&name);
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
}
