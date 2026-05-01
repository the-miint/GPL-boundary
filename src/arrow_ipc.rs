//! Shared Arrow IPC helpers for reading and writing RecordBatches via POSIX
//! shared memory.
//!
//! ## Output: sparse-mmap zero-copy writer
//!
//! `ShmWriter` reserves a large virtual region (default 1 GiB, override via
//! `GPL_BOUNDARY_MAX_SHM_BYTES`) backed by a sparse `shm_open` segment. POSIX
//! shm objects on Linux and macOS are sparse — physical pages are only
//! allocated on first touch — so the reservation is virtually free. The
//! Arrow `StreamWriter` writes directly into the mapping; on `finish()` we
//! `ftruncate` the fd down to the exact written byte count, releasing any
//! unused pages back to the kernel. There is exactly one `mmap`, no remaps
//! during writing, and no `Vec<u8>` intermediate buffer.
//!
//! ## Input: zero-copy reader via `Buffer::from_custom_allocation`
//!
//! `read_batches_from_shm` opens the input segment as a `ReadOnlyShm` (a
//! Sync-by-construction wrapper around an `O_RDONLY` + `PROT_READ` mmap)
//! and wraps it as an `arrow::buffer::Buffer` whose Arc-counted owner is
//! the mmap itself. `StreamDecoder::with_require_alignment(true)` enforces
//! that buffer bodies are aligned in place — forcing the decoder to error
//! out rather than silently fall back to allocate + memcpy on misalignment.
//! The mmap stays alive until the last batch (and any column it carries)
//! is dropped, even after `shm_unlink`.

use std::ffi::CString;
use std::io::{self, Write};
use std::os::raw::{c_int, c_void};
use std::ptr::NonNull;
use std::sync::Arc;

use arrow::alloc::Allocation;
use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::protocol::ShmOutput;
use crate::shm::ReadOnlyShm;

/// Default upper bound on the virtual reservation per output shm. POSIX shm
/// segments are sparse — physical pages are allocated only on touch — so a
/// 1 GiB ceiling is essentially free for normal-sized outputs and accommodates
/// any realistic miint workload. Stress tests can lower this via the
/// `GPL_BOUNDARY_MAX_SHM_BYTES` env var.
const DEFAULT_MAX_SHM_BYTES: usize = 1 << 30;

/// Resolve the per-writer capacity ceiling from the environment, falling back
/// to `DEFAULT_MAX_SHM_BYTES`.
fn default_max_shm_bytes() -> usize {
    std::env::var("GPL_BOUNDARY_MAX_SHM_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_SHM_BYTES)
}

// ---------------------------------------------------------------------------
// ShmWriter — sparse-mmap output writer
// ---------------------------------------------------------------------------

/// A `std::io::Write` adapter that streams bytes directly into a POSIX shm
/// region without an intermediate `Vec<u8>`. Used by
/// `write_batch_to_output_shm` to drive Arrow's `StreamWriter` straight at
/// shared memory.
///
/// Lifecycle:
/// - `new(label)` — opens shm, reserves `max_capacity` virtual bytes, mmaps,
///   and registers the name for signal-handler cleanup.
/// - `Write::write` — copies bytes into the mapping at the current write
///   offset; returns `WriteZero` if the write would exceed `max_capacity`.
/// - `finish(self)` — munmaps, truncates the fd down to exactly `written`
///   bytes (releasing unused pages), closes the fd, and returns a
///   `ShmOutput` describing the segment. The shm remains linked for the
///   reader; the cleanup-registry deregister happens in `main.rs` after the
///   JSON response is on stdout.
/// - `Drop` (without `finish`) — munmaps, closes, deregisters cleanup, and
///   `shm_unlink`s. Used to clean up after a write error or an aborted
///   transaction.
pub struct ShmWriter {
    fd: c_int,
    ptr: *mut u8,
    max_capacity: usize,
    written: usize,
    name: String,
    label: String,
    finished: bool,
}

// SAFETY: ShmWriter owns its mapping and fd; no shared interior pointers
// escape. Send/Sync mirror SharedMemory's bounds.
unsafe impl Send for ShmWriter {}

impl ShmWriter {
    pub fn new(label: &str) -> io::Result<Self> {
        Self::new_with_capacity(label, default_max_shm_bytes())
    }

    pub fn new_with_capacity(label: &str, max_capacity: usize) -> io::Result<Self> {
        let name = crate::shm::output_shm_name(label)?;
        let c_name = CString::new(name.as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        // Register early so a signal between shm_open and the end of new()
        // still results in cleanup. The registry is signal-safe enough that
        // a duplicate unlink at end of new() (on error) is harmless.
        crate::shm::register_for_cleanup(&name);

        // SAFETY: libc FFI. On any failure path between shm_open and
        // returning Ok, we shm_unlink and deregister to leave no trace.
        unsafe {
            let fd = libc::shm_open(
                c_name.as_ptr(),
                libc::O_CREAT | libc::O_RDWR | libc::O_EXCL,
                0o600,
            );
            if fd < 0 {
                let err = io::Error::last_os_error();
                crate::shm::deregister_cleanup(&name);
                return Err(err);
            }

            if libc::ftruncate(fd, max_capacity as libc::off_t) < 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                libc::shm_unlink(c_name.as_ptr());
                crate::shm::deregister_cleanup(&name);
                return Err(err);
            }

            let ptr = libc::mmap(
                std::ptr::null_mut(),
                max_capacity,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            if ptr == libc::MAP_FAILED {
                let err = io::Error::last_os_error();
                libc::close(fd);
                libc::shm_unlink(c_name.as_ptr());
                crate::shm::deregister_cleanup(&name);
                return Err(err);
            }

            Ok(Self {
                fd,
                ptr: ptr as *mut u8,
                max_capacity,
                written: 0,
                name,
                label: label.to_string(),
                finished: false,
            })
        }
    }

    /// Consume the writer, truncate the shm down to the exact written size,
    /// munmap, close, and return a `ShmOutput` describing the segment. The
    /// shm remains linked; the caller (typically `write_batch_to_output_shm`)
    /// passes ownership of cleanup to miint via the response and to the
    /// signal-handler registry until `main.rs` deregisters after stdout
    /// flush.
    pub fn finish(mut self) -> io::Result<ShmOutput> {
        let written = self.written;
        let fd = self.fd;
        let ptr = self.ptr;
        let max_capacity = self.max_capacity;
        let name = std::mem::take(&mut self.name);
        let label = std::mem::take(&mut self.label);
        // Mark finished BEFORE doing any work so Drop won't double-cleanup
        // even if the calls below panic.
        self.finished = true;

        // SAFETY: ptr/fd are owned by self; no other references exist.
        unsafe {
            if libc::munmap(ptr as *mut c_void, max_capacity) < 0 {
                let err = io::Error::last_os_error();
                // Best-effort cleanup of the now-orphaned fd and name.
                libc::close(fd);
                let c_name = CString::new(name.as_str()).unwrap_or_default();
                libc::shm_unlink(c_name.as_ptr());
                crate::shm::deregister_cleanup(&name);
                return Err(err);
            }
            if libc::ftruncate(fd, written as libc::off_t) < 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                let c_name = CString::new(name.as_str()).unwrap_or_default();
                libc::shm_unlink(c_name.as_ptr());
                crate::shm::deregister_cleanup(&name);
                return Err(err);
            }
            libc::close(fd);
        }

        Ok(ShmOutput::new(name, label, written))
    }
}

impl Write for ShmWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let remaining = self.max_capacity.saturating_sub(self.written);
        if buf.len() > remaining {
            // We could partially write up to `remaining`, but Arrow's
            // StreamWriter expects all-or-nothing for record batch bodies —
            // a short write would corrupt the stream. Return WriteZero so
            // the caller sees a clear failure.
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "ShmWriter capacity exhausted: would write {} bytes but only \
                     {} of {} remain (raise GPL_BOUNDARY_MAX_SHM_BYTES if needed)",
                    buf.len(),
                    remaining,
                    self.max_capacity,
                ),
            ));
        }
        // SAFETY: ptr is valid for max_capacity bytes; written + buf.len() ≤
        // max_capacity by the check above.
        unsafe {
            std::ptr::copy_nonoverlapping(buf.as_ptr(), self.ptr.add(self.written), buf.len());
        }
        self.written += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Memory-mapped — no buffer to flush.
        Ok(())
    }
}

impl Drop for ShmWriter {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Error / panic / abandoned-transaction path: tear down everything
        // we own. The shm segment was never handed off, so unlink it and
        // remove from the cleanup registry.
        unsafe {
            if !self.ptr.is_null() {
                libc::munmap(self.ptr as *mut c_void, self.max_capacity);
            }
            if self.fd >= 0 {
                libc::close(self.fd);
            }
            if !self.name.is_empty() {
                if let Ok(c_name) = CString::new(self.name.as_str()) {
                    libc::shm_unlink(c_name.as_ptr());
                }
                crate::shm::deregister_cleanup(&self.name);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public write/read API
// ---------------------------------------------------------------------------

/// Serialize a RecordBatch to Arrow IPC stream format directly into a new
/// POSIX shm region — no intermediate `Vec<u8>`. The returned `ShmOutput`
/// names the segment, its label, and the exact byte count written.
///
/// The shm is registered for signal-handler cleanup. The caller must call
/// `shm::deregister_cleanup(&shm_out.name)` after ownership has been
/// transferred to the reader (i.e., after the JSON response is written to
/// stdout). See `main.rs` for the canonical pattern.
pub fn write_batch_to_output_shm(batch: &RecordBatch, label: &str) -> Result<ShmOutput, String> {
    let mut writer =
        ShmWriter::new(label).map_err(|e| format!("Failed to create output shm: {e}"))?;
    {
        let mut sw = StreamWriter::try_new(&mut writer, &batch.schema())
            .map_err(|e| format!("Failed to create Arrow IPC writer: {e}"))?;
        sw.write(batch)
            .map_err(|e| format!("Failed to write Arrow batch: {e}"))?;
        sw.finish()
            .map_err(|e| format!("Failed to finish Arrow IPC stream: {e}"))?;
        // sw drops here, releasing its borrow of `writer`.
    }
    writer
        .finish()
        .map_err(|e| format!("Failed to finalize output shm: {e}"))
}

/// Read all RecordBatches from an Arrow IPC stream in shared memory.
///
/// Zero-copy: the read-only mmap (`ReadOnlyShm`, which is `Send + Sync` by
/// construction — `O_RDONLY` + `PROT_READ` makes writes a SIGBUS) is
/// wrapped as an `arrow::buffer::Buffer` via `from_custom_allocation`,
/// with the mmap itself as the Arc-counted owner.
/// `StreamDecoder::with_require_alignment(true)` is the load-bearing
/// zero-copy gate: with the default (false), the decoder silently
/// allocates and copies into an aligned buffer when an array body's offset
/// doesn't meet the type's natural alignment. Forcing strict alignment
/// turns any misalignment into a decode error so a regression to the slow
/// path is loud, not silent. The mapping survives for as long as any
/// returned batch (or any column it carries) is alive — including across
/// `shm_unlink`, since unlink only removes the name.
pub fn read_batches_from_shm(shm_name: &str) -> Result<Vec<RecordBatch>, String> {
    let shm =
        ReadOnlyShm::open(shm_name).map_err(|e| format!("Failed to open shm '{shm_name}': {e}"))?;

    let len = shm.len();
    if len == 0 {
        return Ok(Vec::new());
    }

    let ptr = NonNull::new(shm.as_ptr() as *mut u8)
        .ok_or_else(|| format!("Mapped shm '{shm_name}' has null pointer"))?;

    // The Buffer's Arc-counted Allocation keeps `shm` alive for as long as
    // any batch buffer derived from this Buffer is alive.
    let allocation: Arc<dyn Allocation> = Arc::new(shm);
    // SAFETY: `ptr` / `len` cover a valid mmap region owned by `allocation`.
    // `from_custom_allocation` retains `allocation` via Arc; the mmap is not
    // unmapped until the Arc count drops to zero (i.e., all derived buffers
    // are dropped).
    let mut buf = unsafe { Buffer::from_custom_allocation(ptr, len, allocation) };

    let mut decoder = StreamDecoder::new().with_require_alignment(true);
    let mut out = Vec::new();
    while !buf.is_empty() {
        match decoder
            .decode(&mut buf)
            .map_err(|e| format!("Failed to decode Arrow IPC: {e}"))?
        {
            Some(batch) => out.push(batch),
            None => break,
        }
    }
    decoder
        .finish()
        .map_err(|e| format!("Arrow IPC stream ended without EOS marker: {e}"))?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shm::SharedMemory;
    use crate::test_util::{read_arrow_from_shm, unique_shm_name, write_arrow_to_shm};
    use arrow::array::{Array, Int32Array, StringArray};
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
        assert!(shm_out.name.contains("gb-"));
        assert!(shm_out.name.ends_with("-test"));

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

    // --- Phase 2 additions: zero-copy output + zero-copy input ---

    /// Build a RecordBatch large enough that the IPC body cannot be confused
    /// with the small framing bytes — easily detects truncation, page-fault,
    /// or off-by-one issues.
    fn make_large_batch(n_rows: usize, str_len: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let ids: Vec<i32> = (0..n_rows as i32).collect();
        let val = "x".repeat(str_len);
        let vals: Vec<&str> = (0..n_rows).map(|_| val.as_str()).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(vals)),
            ],
        )
        .unwrap()
    }

    /// Read the on-disk size of a POSIX shm name (post-finish, not the
    /// 1 GiB virtual reservation). Equivalent to `fstat` on a freshly opened
    /// fd for the segment.
    fn fstat_shm_size(name: &str) -> usize {
        let c_name = CString::new(name).unwrap();
        unsafe {
            let fd = libc::shm_open(c_name.as_ptr(), libc::O_RDONLY, 0);
            assert!(fd >= 0, "shm_open failed for {name}");
            let mut st: libc::stat = std::mem::zeroed();
            let rc = libc::fstat(fd, &mut st);
            libc::close(fd);
            assert_eq!(rc, 0, "fstat failed for {name}");
            st.st_size as usize
        }
    }

    #[test]
    fn test_shm_writer_truncates_to_written_size() {
        // After finish(), the shm fd's reported size must equal the byte
        // count we wrote, not the 1 GiB virtual reservation. Without the
        // ftruncate-down step, the segment would consume up to 1 GiB of
        // tmpfs quota even for a tiny output.
        let batch = make_large_batch(10, 4);
        let shm_out = write_batch_to_output_shm(&batch, "truncchk").unwrap();
        let on_disk = fstat_shm_size(&shm_out.name);
        assert_eq!(
            on_disk, shm_out.size,
            "ShmOutput.size and on-disk size disagree: {} vs {}",
            shm_out.size, on_disk
        );
        assert!(
            on_disk < 1 << 20,
            "Tiny batch should produce <1 MiB shm, got {on_disk} bytes \
             (suggests ftruncate-down didn't run)"
        );
        crate::shm::deregister_cleanup(&shm_out.name);
        let _ = SharedMemory::unlink(&shm_out.name);
    }

    #[test]
    fn test_shm_writer_large_batch_roundtrip() {
        // 16 MB IPC payload — well above any small-buffer fast path — to
        // catch off-by-one or partial-write bugs in the new write path.
        let batch = make_large_batch(2048, 8000); // ~16 MB of string body
        let shm_out = write_batch_to_output_shm(&batch, "rt-large").unwrap();
        // Expect roughly 16.4 MB (2048 * 8000 bytes of body + IPC framing).
        assert!(
            shm_out.size > 16_000_000,
            "expected >16 MB written, got {}",
            shm_out.size
        );

        let batches = read_batches_from_shm(&shm_out.name).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2048);

        let val_col = batches[0]
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(val_col.value(0).len(), 8000);
        assert_eq!(val_col.value(2047).len(), 8000);

        crate::shm::deregister_cleanup(&shm_out.name);
        let _ = SharedMemory::unlink(&shm_out.name);
    }

    #[test]
    fn test_shm_writer_overflow_returns_error() {
        // Bound the writer at 8 KiB and try to write ~16 MiB. The Write
        // impl must return an error rather than silently truncating.
        let batch = make_large_batch(2048, 8000);
        let mut writer = ShmWriter::new_with_capacity("ovfl", 8 * 1024).unwrap();
        let written_name = writer.name.clone();
        let mut sw = StreamWriter::try_new(&mut writer, &batch.schema()).unwrap();
        let res = sw.write(&batch).and_then(|_| sw.finish());
        assert!(res.is_err(), "expected write error on capacity overflow");
        // `writer` Drop runs after `sw` Drop releases the borrow; verify
        // the shm got cleaned up automatically (no manual unlink).
        drop(sw);
        drop(writer);
        let still_there = SharedMemory::open_readonly(&written_name).is_ok();
        assert!(
            !still_there,
            "ShmWriter Drop on error path must unlink the shm; {written_name} still present"
        );
    }

    #[test]
    fn test_shm_writer_drop_unlinks_partial() {
        // Construct a writer, write a few bytes, drop without finish().
        // Drop must munmap, close, and shm_unlink so we don't leak.
        let writer = ShmWriter::new("partial").unwrap();
        let name = writer.name.clone();
        // Write something so a partial physical page is allocated.
        let mut writer = writer;
        writer.write_all(b"hello").unwrap();
        drop(writer);
        let still_there = SharedMemory::open_readonly(&name).is_ok();
        assert!(!still_there, "partial-write shm not cleaned: {name}");
    }

    #[test]
    fn test_input_values_buffer_points_into_mmap() {
        // True zero-copy proof: the column's underlying buffer pointer
        // must lie inside the mmap range we used to read the batch. We
        // replicate the read path manually here so we own the mmap and
        // can compare addresses; the public `read_batches_from_shm` hides
        // the mmap base for encapsulation.
        let n_rows = 1024;
        let str_len = 1024;
        let batch = make_large_batch(n_rows, str_len);
        let shm_out = write_batch_to_output_shm(&batch, "zcin").unwrap();

        let shm = crate::shm::ReadOnlyShm::open(&shm_out.name).unwrap();
        let mmap_start = shm.as_ptr() as usize;
        let mmap_end = mmap_start + shm.len();
        let ptr = NonNull::new(shm.as_ptr() as *mut u8).unwrap();
        let allocation: Arc<dyn Allocation> = Arc::new(shm);
        let mut buf =
            unsafe { Buffer::from_custom_allocation(ptr, mmap_end - mmap_start, allocation) };
        let mut decoder = StreamDecoder::new().with_require_alignment(true);
        let mut batches = Vec::new();
        while !buf.is_empty() {
            if let Some(b) = decoder.decode(&mut buf).unwrap() {
                batches.push(b);
            } else {
                break;
            }
        }
        decoder.finish().unwrap();

        let val_col = batches[0]
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values_ptr = val_col.values().as_ptr() as usize;
        let values_end = values_ptr + val_col.values().len();
        assert!(
            values_ptr >= mmap_start && values_end <= mmap_end,
            "Values buffer [{values_ptr:#x}, {values_end:#x}) is outside \
             mmap range [{mmap_start:#x}, {mmap_end:#x}) — read path is \
             copying instead of zero-slicing into the mmap"
        );

        crate::shm::deregister_cleanup(&shm_out.name);
        drop(batches);
        let _ = SharedMemory::unlink(&shm_out.name);
    }

    #[test]
    fn test_input_mmap_outlives_unlink() {
        // Behavioral proof of zero-copy: shm_unlink only removes the name;
        // existing mmap regions stay valid until munmap. If the Arc owner
        // of the mmap is the returned Buffer, batches must remain readable
        // after the shm name is unlinked. A heap-copy implementation would
        // also pass this test, but combined with
        // `test_input_values_buffer_points_into_mmap` (which checks the
        // pointer is in the mmap range), passing both is true zero-copy
        // proof: addresses point at the mmap AND the mmap stays alive
        // through unlink.
        let n_rows = 256;
        let str_len = 512;
        let batch = make_large_batch(n_rows, str_len);
        let shm_out = write_batch_to_output_shm(&batch, "outlive").unwrap();

        let batches = read_batches_from_shm(&shm_out.name).unwrap();

        crate::shm::deregister_cleanup(&shm_out.name);
        SharedMemory::unlink(&shm_out.name).unwrap();
        assert!(SharedMemory::open_readonly(&shm_out.name).is_err());

        let val_col = batches[0]
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(val_col.len(), n_rows);
        assert_eq!(val_col.value(0).len(), str_len);
        assert_eq!(val_col.value(n_rows - 1).len(), str_len);
    }
}
