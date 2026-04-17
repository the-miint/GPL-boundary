//! Platform-abstracted POSIX shared memory for Arrow IPC exchange.
//!
//! Uses shm_open() on both Linux and macOS. Names must start with '/'.
//!
//! ## Lifecycle
//!
//! gpl-boundary creates output shm regions with PID-based names
//! (e.g., `/gpl-boundary-1234-out`). On normal exit or trappable signals
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
    // FIXME: Mutex::lock() is not async-signal-safe. If SIGTERM arrives while
    // the main thread holds the lock (e.g., inside register_for_cleanup), this
    // deadlocks. The PID-based naming convention lets miint detect and clean up
    // stale segments, so this is a fallback, not a guarantee. A proper fix would
    // use a lock-free structure (atomic array or just compute the name from PID).
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
    format!("/gpl-boundary-{pid}-{n}-{label}")
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
unsafe impl Send for SharedMemory {}

impl SharedMemory {
    /// Open an existing shared memory region for reading.
    #[cfg(unix)]
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
    #[cfg(unix)]
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

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
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
    #[cfg(unix)]
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
        let name = "/gpl-boundary-test-shm-detach";
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
        let name = "/gpl-boundary-test-shm-drop";

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
        assert!(name.starts_with("/gpl-boundary-"));
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
        let name = "/gpl-boundary-test-registry";
        let _shm = SharedMemory::create(name, 64).unwrap();

        // Detach so drop doesn't unlink
        _shm.detach();

        // Register and cleanup
        register_for_cleanup(name);
        cleanup_all();

        // Should be gone
        assert!(SharedMemory::open_readonly(name).is_err());
    }
}
