//! Platform-abstracted POSIX shared memory for Arrow IPC exchange.
//!
//! Uses shm_open() on both Linux and macOS. Names must start with '/'.
//!
//! ## Lifecycle
//!
//! gpl-boundary creates output shm regions with PID-based names
//! (e.g., `/gb-1234-out`). On normal exit or trappable signals
//! (SIGINT, SIGTERM), output shm is unlinked via the cleanup registry.
//! SIGKILL cannot be trapped; the caller (miint) should clean up stale
//! segments by checking if the PID in the name is still alive.

use std::ffi::CString;
use std::io;
use std::sync::Mutex;

// --- Cleanup registry for signal-safe shm unlink ---

/// Global list of shm names to unlink on exit/signal.
static CLEANUP_REGISTRY: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Register a shm name for cleanup on exit/signal.
pub fn register_for_cleanup(name: &str) {
    if let Ok(mut registry) = CLEANUP_REGISTRY.lock() {
        registry.push(name.to_string());
    }
}

/// Remove a shm name from the cleanup registry (e.g., after caller takes ownership).
pub fn deregister_cleanup(name: &str) {
    if let Ok(mut registry) = CLEANUP_REGISTRY.lock() {
        registry.retain(|n| n != name);
    }
}

/// Unlink all registered shm segments. Called on exit and from signal handlers.
#[allow(dead_code)]
pub fn cleanup_all() {
    if let Ok(mut registry) = CLEANUP_REGISTRY.lock() {
        for name in registry.drain(..) {
            if let Ok(c_name) = CString::new(name.as_str()) {
                unsafe {
                    libc::shm_unlink(c_name.as_ptr());
                }
            }
        }
    }
}

/// Install signal handlers that clean up shm before exiting.
/// Must be called once at startup.
pub fn install_signal_handlers() {
    unsafe {
        libc::signal(
            libc::SIGINT,
            signal_handler as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            signal_handler as *const () as libc::sighandler_t,
        );
    }
}

extern "C" fn signal_handler(sig: libc::c_int) {
    // FIXME(phase-4): Mutex::lock() is not async-signal-safe. If SIGTERM
    // arrives while the main thread holds the lock (e.g., inside
    // register_for_cleanup), this deadlocks. The PID-based naming convention
    // lets miint detect and clean up stale segments, so this is a fallback,
    // not a guarantee. A proper fix is needed for Phase 4 (subprocess workers
    // multiply the chance of an in-flight registry mutation when a signal
    // hits). Replace the `Mutex<Vec<String>>` with a lock-free structure
    // (intrusive linked list of stack-allocated entries, or just compute
    // names deterministically from PID + label so the handler can
    // reconstruct without the registry).
    if let Ok(registry) = CLEANUP_REGISTRY.lock() {
        for name in registry.iter() {
            if let Ok(c_name) = CString::new(name.as_str()) {
                unsafe {
                    libc::shm_unlink(c_name.as_ptr());
                }
            }
        }
    }
    unsafe {
        // Re-raise the signal with default handler to get correct exit status
        libc::signal(sig, libc::SIG_DFL);
        libc::raise(sig);
    }
}

/// Generate a unique output shm name using the process PID, an atomic counter,
/// and a label. The counter ensures uniqueness when a process creates multiple
/// output segments with the same label (e.g., across test runs).
/// Labels must be short ASCII identifiers (lowercase alphanumeric + hyphen).
pub fn output_shm_name(label: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);

    assert!(
        !label.is_empty()
            && label
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "shm label must be non-empty and contain only [a-z0-9-], got: {label:?}"
    );
    let pid = std::process::id();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    // Prefix "gb" keeps names within the macOS POSIX shm 31-character limit.
    // Budget: /gb-{pid}-{n}-{label} = 1+2+1+5+1+4+1+label ≤ 31
    // With 5-digit PID, 4-digit counter: 15 + label ≤ 31 → label ≤ 16 chars.
    let name = format!("/gb-{pid}-{n}-{label}");
    assert!(
        name.len() <= 31,
        "shm name exceeds macOS 31-char limit: {name} ({} chars)",
        name.len()
    );
    name
}

// --- SharedMemory type ---

/// A mapped shared memory region.
pub struct SharedMemory {
    ptr: *mut u8,
    len: usize,
    name: String,
    owned: bool, // if true, shm_unlink on drop
}

// SAFETY: SharedMemory owns its mapping; the pointer is valid for its lifetime.
// SharedMemory is NOT Sync because `as_mut_slice(&mut self)` exposes write
// access — concurrent shared access could race writers. For zero-copy reads
// where we need to share an immutable mapping across threads, see
// `ReadOnlyShm` below, which is Sync by construction.
unsafe impl Send for SharedMemory {}

impl SharedMemory {
    /// Open an existing shared memory region for reading.
    ///
    /// The production read path uses `ReadOnlyShm` instead — it is
    /// `Send + Sync` by construction and integrates with
    /// `arrow_ipc::read_batches_from_shm`'s zero-copy `Buffer`. This
    /// entry point remains for `test_util` and post-create open-for-read
    /// checks in tests.
    #[cfg(unix)]
    #[allow(dead_code)]
    pub fn open_readonly(name: &str) -> io::Result<Self> {
        let c_name = to_cstring(name)?;

        unsafe {
            let fd = libc::shm_open(c_name.as_ptr(), libc::O_RDONLY, 0);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }

            let len = fd_size(fd)?;

            let ptr = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            );
            libc::close(fd);

            if ptr == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }

            Ok(Self {
                ptr: ptr as *mut u8,
                len,
                name: name.to_string(),
                owned: false,
            })
        }
    }

    /// Create a new shared memory region for writing.
    ///
    /// Production write paths use `arrow_ipc::ShmWriter` instead of this —
    /// `ShmWriter` reserves a sparse 1 GiB region and `ftruncate`s down at
    /// finish, avoiding any Vec-then-memcpy. This entry point is kept for
    /// `test_util` and signal-handler-cleanup tests where a fixed-size
    /// allocation is convenient.
    #[cfg(unix)]
    #[allow(dead_code)]
    pub fn create(name: &str, size: usize) -> io::Result<Self> {
        let c_name = to_cstring(name)?;

        unsafe {
            let fd = libc::shm_open(
                c_name.as_ptr(),
                libc::O_CREAT | libc::O_RDWR | libc::O_EXCL,
                0o600,
            );
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }

            if libc::ftruncate(fd, size as libc::off_t) < 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                libc::shm_unlink(c_name.as_ptr());
                return Err(err);
            }

            let ptr = libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            libc::close(fd);

            if ptr == libc::MAP_FAILED {
                libc::shm_unlink(c_name.as_ptr());
                return Err(io::Error::last_os_error());
            }

            Ok(Self {
                ptr: ptr as *mut u8,
                len: size,
                name: name.to_string(),
                owned: true,
            })
        }
    }

    #[allow(dead_code)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    #[allow(dead_code)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    /// Raw pointer to the start of the mapping. The production zero-copy
    /// read path uses `ReadOnlyShm::as_ptr()` instead — this stays only
    /// for tests that exercise `SharedMemory` directly.
    #[allow(dead_code)]
    pub(crate) fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Detach from the shared memory mapping without unlinking.
    /// The shm object remains available for other processes to read.
    /// Consumes self, munmaps but does not shm_unlink.
    ///
    /// Used by `test_util` to hand a writable fixed-size shm off to a
    /// reader; production output paths use `ShmWriter::finish` which
    /// performs the equivalent munmap + ftruncate-down + close in one step.
    #[cfg(unix)]
    #[allow(dead_code)]
    pub fn detach(mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
        // Prevent Drop from running (which would unlink if owned)
        self.ptr = std::ptr::null_mut();
        self.len = 0;
        self.owned = false;
    }

    /// Unlink (remove) the shared memory object by name.
    #[cfg(unix)]
    #[allow(dead_code)]
    pub fn unlink(name: &str) -> io::Result<()> {
        let c_name = to_cstring(name)?;
        unsafe {
            if libc::shm_unlink(c_name.as_ptr()) < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
}

impl Drop for SharedMemory {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                libc::munmap(self.ptr as *mut libc::c_void, self.len);
            }
        }
        if self.owned {
            if let Ok(c_name) = CString::new(self.name.as_str()) {
                unsafe {
                    libc::shm_unlink(c_name.as_ptr());
                }
            }
        }
    }
}

fn to_cstring(name: &str) -> io::Result<CString> {
    CString::new(name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}

// --- ReadOnlyShm — Sync-by-construction read-only mapping ---

/// An immutable mmap of a POSIX shm segment, opened with `O_RDONLY` and
/// mapped `PROT_READ`. Write attempts would be SIGBUS at the OS level, so
/// the mapping is genuinely shareable across threads — `Send + Sync` is
/// sound by construction. Used by `arrow_ipc::read_batches_from_shm` to
/// wrap the input as a zero-copy `arrow_buffer::Buffer`.
///
/// The struct does NOT unlink on drop; it only munmaps. The shm name is
/// owned by the caller of `read_batches_from_shm` (typically miint).
pub struct ReadOnlyShm {
    ptr: *const u8,
    len: usize,
}

// SAFETY: `ReadOnlyShm` is constructed only by `open` below, which uses
// `O_RDONLY` + `PROT_READ`. The OS rejects writes via SIGBUS, so the
// mapping is immutable for its entire lifetime; concurrent reads from
// any number of threads are race-free.
unsafe impl Send for ReadOnlyShm {}
unsafe impl Sync for ReadOnlyShm {}

impl ReadOnlyShm {
    #[cfg(unix)]
    pub fn open(name: &str) -> io::Result<Self> {
        let c_name = to_cstring(name)?;
        unsafe {
            let fd = libc::shm_open(c_name.as_ptr(), libc::O_RDONLY, 0);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }

            let len = fd_size(fd)?;

            // Special-case the empty mapping: mmap with len=0 is undefined.
            // Return an empty handle that callers can treat as 0-byte input.
            if len == 0 {
                libc::close(fd);
                return Ok(Self {
                    ptr: std::ptr::null(),
                    len: 0,
                });
            }

            let ptr = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            );
            libc::close(fd);

            if ptr == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            Ok(Self {
                ptr: ptr as *const u8,
                len,
            })
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for ReadOnlyShm {
    fn drop(&mut self) {
        if !self.ptr.is_null() && self.len > 0 {
            unsafe {
                libc::munmap(self.ptr as *mut libc::c_void, self.len);
            }
        }
    }
}

#[cfg(unix)]
unsafe fn fd_size(fd: i32) -> io::Result<usize> {
    let mut stat: libc::stat = std::mem::zeroed();
    if libc::fstat(fd, &mut stat) < 0 {
        let err = io::Error::last_os_error();
        libc::close(fd);
        return Err(err);
    }
    Ok(stat.st_size as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_write_read_detach() {
        let name = "/gb-test-shm-detach";
        let data = b"hello shared memory";

        // Create, write, and detach (keeps shm alive for readers)
        {
            let mut shm = SharedMemory::create(name, 4096).unwrap();
            shm.as_mut_slice()[..data.len()].copy_from_slice(data);
            shm.detach(); // munmap but no unlink
        }

        // Should still be readable after detach
        {
            let reader = SharedMemory::open_readonly(name).unwrap();
            assert_eq!(&reader.as_slice()[..data.len()], data);
        }

        // Manual cleanup
        SharedMemory::unlink(name).unwrap();
    }

    #[test]
    fn test_owned_drop_unlinks() {
        let name = "/gb-test-shm-drop";

        {
            let _shm = SharedMemory::create(name, 4096).unwrap();
            // Drop unlinks
        }

        // Should fail to open since it was unlinked
        assert!(SharedMemory::open_readonly(name).is_err());
    }

    #[test]
    fn test_output_shm_name_with_label() {
        let name = output_shm_name("tree");
        assert!(name.starts_with("/gb-"));
        assert!(name.ends_with("-tree"));
    }

    #[test]
    fn test_output_shm_name_different_labels() {
        let name_a = output_shm_name("tree");
        let name_b = output_shm_name("dist");
        assert_ne!(name_a, name_b);
        assert!(name_a.ends_with("-tree"));
        assert!(name_b.ends_with("-dist"));
    }

    #[test]
    fn test_output_shm_name_unique_across_calls() {
        let name_a = output_shm_name("tree");
        let name_b = output_shm_name("tree");
        assert_ne!(name_a, name_b, "Same label should produce unique names");
    }

    #[test]
    fn test_cleanup_registry() {
        // Use a unique name so this test doesn't fight with parallel tests.
        let name = format!("/gb-test-reg-{}", std::process::id());
        let _shm = SharedMemory::create(&name, 64).unwrap();
        _shm.detach();

        // Register, then verify the name is in the registry.
        register_for_cleanup(&name);
        {
            let registry = CLEANUP_REGISTRY.lock().unwrap();
            assert!(
                registry.iter().any(|n| n == &name),
                "Expected {name} to be in cleanup registry"
            );
        }

        // Deregister and verify removal. We deliberately do NOT call
        // cleanup_all() here — it drains the entire global registry, which
        // would unlink shm segments registered by tests running in parallel
        // (e.g. arrow_ipc::tests::test_write_batch_to_output_shm). Production
        // only calls cleanup_all() from the signal handler at process exit.
        deregister_cleanup(&name);
        {
            let registry = CLEANUP_REGISTRY.lock().unwrap();
            assert!(
                !registry.iter().any(|n| n == &name),
                "Expected {name} to be removed from cleanup registry"
            );
        }

        // Manually unlink the shm we created so the test cleans up after itself.
        let _ = SharedMemory::unlink(&name);
        assert!(SharedMemory::open_readonly(&name).is_err());
    }
}
