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

use std::cell::UnsafeCell;
use std::ffi::CString;
use std::io;
use std::sync::atomic::{AtomicU8, Ordering};

// --- Cleanup registry for signal-safe shm unlink ---
//
// The registry must be safe to read from a signal handler. POSIX
// async-signal-safety forbids `pthread_mutex_lock` (and therefore
// `std::sync::Mutex::lock`) inside a handler — if the signal lands
// while the main thread already holds the lock, the handler
// deadlocks. With Phase 4 spinning up subprocess workers, every
// process inherits this exposure, so we replaced the prior
// `Mutex<Vec<String>>` with a fixed-size pool of atomic slots.
//
// Each slot stores its own NUL-terminated byte buffer (no heap
// allocation per registration after startup). Producers claim a
// slot via CAS on `state`; the signal handler iterates with
// relaxed loads and calls `shm_unlink` directly on the byte
// pointer. There is no shared lock and no allocation on the
// signal-handler path.
//
// Capacity: `MAX_CLEANUP_SLOTS` is the upper bound on
// concurrently-registered shm names. 64 comfortably covers the
// realistic in-flight set (a handful of in-process tools, each
// producing 1-2 outputs at a time). If exhausted, registration is
// skipped — the PID-based naming convention lets miint sweep
// orphans even when our handler misses one.

const STATE_FREE: u8 = 0;
const STATE_RESERVED: u8 = 1;
const STATE_OCCUPIED: u8 = 2;

const MAX_CLEANUP_SLOTS: usize = 64;
/// Holds a NUL-terminated shm name. POSIX shm names cap at 31 chars
/// on macOS (255 on Linux); 64 bytes covers either with margin and
/// keeps the slot small.
const NAME_BUF_LEN: usize = 64;

struct CleanupSlot {
    state: AtomicU8,
    name: UnsafeCell<[u8; NAME_BUF_LEN]>,
}

// SAFETY: `name` is mutated only after a producer successfully CAS's
// `state` from FREE to RESERVED, so at most one writer ever touches the
// buffer at a time. Readers (signal handler, deregister, cleanup_all)
// only inspect the buffer when `state == OCCUPIED`, which is established
// after the producer's writes via a Release store. Concurrent reads of
// stable bytes are race-free.
unsafe impl Sync for CleanupSlot {}

// `clippy::declare_interior_mutable_const` flags interior-mutable consts
// because reading them through `*CONST` copies and discards the mutability.
// Here the const is used *only* as an array initializer expression — each
// of the N elements is evaluated independently, producing N distinct
// atomics (`AtomicU8` is not `Copy`). This is the documented stable
// pattern for filling a static array with non-Copy interior-mutable
// elements, so the lint is a false positive in this position.
#[allow(clippy::declare_interior_mutable_const)]
const SLOT_INIT: CleanupSlot = CleanupSlot {
    state: AtomicU8::new(STATE_FREE),
    name: UnsafeCell::new([0u8; NAME_BUF_LEN]),
};

static CLEANUP_SLOTS: [CleanupSlot; MAX_CLEANUP_SLOTS] = [SLOT_INIT; MAX_CLEANUP_SLOTS];

/// Compare an OCCUPIED slot's NUL-terminated name buffer to `name`.
///
/// SAFETY: caller must guarantee the slot's state was last observed as
/// OCCUPIED (so the bytes are stable for the duration of the call).
unsafe fn slot_name_eq(slot: &CleanupSlot, name: &[u8]) -> bool {
    let buf = &*slot.name.get();
    // Find NUL terminator within the buffer.
    let nul_idx = buf.iter().position(|&b| b == 0).unwrap_or(NAME_BUF_LEN);
    nul_idx == name.len() && &buf[..nul_idx] == name
}

/// Register a shm name for cleanup on exit/signal.
///
/// Looks for a free slot, CAS's it to RESERVED, copies bytes + NUL,
/// then publishes OCCUPIED with Release ordering. If no slot is free
/// the registration is silently skipped — miint's stale-PID sweep is
/// the catch-all for missed cleanups.
pub fn register_for_cleanup(name: &str) {
    let bytes = name.as_bytes();
    if bytes.len() >= NAME_BUF_LEN || bytes.contains(&0) {
        // Names are constructed by `output_shm_name` which already
        // bounds length to 31; defensive guard against a future caller
        // bypassing that helper.
        return;
    }

    for slot in CLEANUP_SLOTS.iter() {
        if slot
            .state
            .compare_exchange(
                STATE_FREE,
                STATE_RESERVED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            // SAFETY: we hold exclusive access to this slot's bytes
            // because state is RESERVED (other writers see != FREE
            // and skip; readers only act on OCCUPIED).
            unsafe {
                let buf = &mut *slot.name.get();
                buf[..bytes.len()].copy_from_slice(bytes);
                buf[bytes.len()] = 0;
            }
            slot.state.store(STATE_OCCUPIED, Ordering::Release);
            return;
        }
    }
    // All slots in use. Best-effort registry — silently drop. The
    // PID-based naming convention lets miint sweep stale segments
    // when a process exits without unlinking.
}

/// Remove a shm name from the cleanup registry (e.g., after caller
/// takes ownership).
pub fn deregister_cleanup(name: &str) {
    let bytes = name.as_bytes();
    for slot in CLEANUP_SLOTS.iter() {
        if slot.state.load(Ordering::Acquire) != STATE_OCCUPIED {
            continue;
        }
        // SAFETY: we just observed OCCUPIED with Acquire; the bytes
        // are stable until we (or the signal handler) flip the state.
        if unsafe { slot_name_eq(slot, bytes) }
            && slot
                .state
                .compare_exchange(
                    STATE_OCCUPIED,
                    STATE_FREE,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            return;
        }
    }
}

/// Unlink all registered shm segments. Currently only used by tests; the
/// production cleanup paths are per-output `deregister_cleanup` (called
/// after a successful response write) and the inline signal-handler sweep
/// (which intentionally does *not* mutate slot state — see below).
///
/// Iterates without locks. Each OCCUPIED slot has a stable, NUL-terminated
/// name buffer that we hand directly to `shm_unlink`. The transition back
/// to FREE uses `compare_exchange(OCCUPIED → FREE)` rather than a plain
/// store: a non-CAS store would race a concurrent producer that observed
/// FREE, won its own `compare_exchange(FREE → RESERVED)`, and started
/// writing new bytes — the plain store would then clobber RESERVED,
/// breaking the producer's exclusive-write invariant on the buffer.
#[allow(dead_code)]
pub fn cleanup_all() {
    for slot in CLEANUP_SLOTS.iter() {
        if slot.state.load(Ordering::Acquire) != STATE_OCCUPIED {
            continue;
        }
        unsafe {
            let ptr = (*slot.name.get()).as_ptr() as *const libc::c_char;
            libc::shm_unlink(ptr);
        }
        // Best-effort: a CAS failure here means a concurrent deregister
        // already freed the slot (or — extremely unlikely outside a test —
        // a producer is mid-write). Either way the unlink ran on the
        // observed name, which is what we wanted.
        let _ = slot.state.compare_exchange(
            STATE_OCCUPIED,
            STATE_FREE,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }
}

/// Test-only: report whether `name` is currently in the registry.
/// Walks the slot pool without locking.
#[cfg(test)]
fn is_registered(name: &str) -> bool {
    let bytes = name.as_bytes();
    for slot in CLEANUP_SLOTS.iter() {
        if slot.state.load(Ordering::Acquire) != STATE_OCCUPIED {
            continue;
        }
        // SAFETY: state == OCCUPIED → buffer bytes stable.
        if unsafe { slot_name_eq(slot, bytes) } {
            return true;
        }
    }
    false
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
    // Async-signal-safe: only atomic loads and `shm_unlink` (POSIX lists
    // shm_unlink as signal-safe). No locks, no allocation, no string
    // formatting. The state load is `Acquire` so it pairs with the
    // producer's `Release` store of OCCUPIED — that's what makes the
    // name buffer bytes visible to this thread. A `Relaxed` load would
    // be wrong on weakly-ordered architectures (macOS ARM64 in
    // particular) where the buffer write and state store can become
    // visible out of order. We don't write to slot state here: leaving
    // OCCUPIED slots OCCUPIED is fine because the handler re-raises with
    // SIG_DFL and the process exits immediately afterward, so no other
    // thread can observe the registry again.
    for slot in CLEANUP_SLOTS.iter() {
        if slot.state.load(Ordering::Acquire) == STATE_OCCUPIED {
            unsafe {
                let ptr = (*slot.name.get()).as_ptr() as *const libc::c_char;
                libc::shm_unlink(ptr);
            }
        }
    }
    unsafe {
        // Re-raise the signal with default handler to get correct exit status.
        libc::signal(sig, libc::SIG_DFL);
        libc::raise(sig);
    }
}

/// Generate a unique output shm name using the process PID, an atomic counter,
/// and a label. The counter ensures uniqueness when a process creates multiple
/// output segments with the same label (e.g., across test runs).
/// Labels must be short ASCII identifiers (lowercase alphanumeric + hyphen).
///
/// Returns `Err` if the resulting name would exceed the macOS POSIX shm
/// 31-character limit. This can happen on long-lived sessions where the
/// counter grows past ~8 digits combined with a 7-digit Linux PID and a
/// long label like "alignments". Replacing the previous runtime assert
/// with a fallible return so the caller can surface the failure as a
/// normal error response rather than panicking out of an FFI path.
pub fn output_shm_name(label: &str) -> io::Result<String> {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);

    if label.is_empty()
        || !label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("shm label must be non-empty [a-z0-9-], got: {label:?}"),
        ));
    }
    let pid = std::process::id();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    // Prefix "gb" keeps names within the macOS POSIX shm 31-character limit.
    // Worst-case overhead: 1+2+1+10+1+10+1 = 26 chars (u32 max counter and
    // 7-digit Linux PID), leaving 5 chars for the label. Most labels
    // (e.g. "tree", "genes") fit; "alignments" (10 chars) overflows once
    // the counter passes ~10 million.
    let name = format!("/gb-{pid}-{n}-{label}");
    if name.len() > 31 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "shm name {name} exceeds macOS 31-char limit ({} chars); \
                 use a shorter label or restart the session to reset the counter",
                name.len()
            ),
        ));
    }
    Ok(name)
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
    /// Open an existing shared memory region for reading. **Test-only.**
    ///
    /// The production read path uses `ReadOnlyShm::open(name, size)`
    /// — it takes the data length out-of-band (the protocol is the
    /// authoritative size source) and integrates with
    /// `arrow_ipc::read_batches_from_shm`'s zero-copy `Buffer`. This
    /// entry point uses `fstat` to size its mapping, which is
    /// unreliable on Darwin POSIX shm; it remains only for the
    /// existence-probe pattern in tests (`open_readonly(name).is_ok()`
    /// after `unlink`), where the success/failure of the call matters
    /// and the mapped bytes are not consumed.
    #[cfg(all(unix, test))]
    pub fn open_readonly(name: &str) -> io::Result<Self> {
        let c_name = to_cstring(name)?;

        unsafe {
            let fd = libc::shm_open(c_name.as_ptr(), libc::O_RDONLY, 0);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }

            let len = match fd_size(fd) {
                Ok(n) => n,
                Err(e) => {
                    libc::close(fd);
                    return Err(e);
                }
            };

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
    /// `ShmWriter` reserves a large sparse region (default 1 GiB) and
    /// hands its size out-of-band via `ShmOutput::size`, avoiding any
    /// Vec-then-memcpy. This entry point is kept for `test_util` and
    /// signal-handler-cleanup tests where a fixed-size allocation is
    /// convenient. Note that POSIX shm on macOS only allows `ftruncate`
    /// to set the size **once** — `create` does that here and never
    /// resizes again.
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
/// sound by construction. Used by `arrow_ipc::read_batches_from_shm` and
/// `read_batches_from_shm` to wrap a segment as a zero-copy
/// `arrow_buffer::Buffer`.
///
/// The struct does NOT unlink on drop; it only munmaps. The shm name is
/// owned by the caller (typically miint).
pub struct ReadOnlyShm {
    ptr: *const u8,
    len: usize,
}

// SAFETY: `ReadOnlyShm` is constructed only by `open`
// below, both of which use `O_RDONLY` + `PROT_READ`. The OS rejects writes
// via SIGBUS, so the mapping is immutable for its entire lifetime;
// concurrent reads from any number of threads are race-free.
unsafe impl Send for ReadOnlyShm {}
unsafe impl Sync for ReadOnlyShm {}

impl ReadOnlyShm {
    /// Open a segment with an explicit `size`, mapping exactly `size`
    /// bytes. `fstat` is not consulted for sizing: Darwin POSIX shm has
    /// unreliable `fstat` semantics for our pattern, so the data length
    /// must travel out-of-band via the protocol (`BatchRequest::shm_input_size`
    /// for inputs, `ShmOutput::size` for outputs).
    ///
    /// `size == 0` is allowed only when the segment is genuinely empty —
    /// a freshly-created shm whose `ftruncate` has not yet enlarged it,
    /// or a tool whose input batch is intentionally zero-length. Passing
    /// `size == 0` for a non-empty segment is treated as a caller bug
    /// (forgot to plumb `shm_input_size`) and surfaced as an error
    /// rather than silently returning an empty handle. The check uses
    /// `fstat` solely as a sanity gate — `st_size > 0` is a reliable
    /// signal of "non-empty" on both Linux and macOS even when its exact
    /// value isn't trustworthy on Darwin.
    #[cfg(unix)]
    pub fn open(name: &str, size: usize) -> io::Result<Self> {
        let c_name = to_cstring(name)?;
        unsafe {
            let fd = libc::shm_open(c_name.as_ptr(), libc::O_RDONLY, 0);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            // Special-case the empty mapping: mmap with len=0 is undefined.
            // Reject `size == 0` against a non-empty segment as a caller
            // bug (likely forgot to plumb shm_input_size).
            if size == 0 {
                let mut stat: libc::stat = std::mem::zeroed();
                let rc = libc::fstat(fd, &mut stat);
                libc::close(fd);
                if rc < 0 {
                    return Err(io::Error::last_os_error());
                }
                if stat.st_size > 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "ReadOnlyShm::open called with size=0 against non-empty \
                             segment '{name}' (fstat reports {} bytes); caller likely \
                             forgot to plumb shm_input_size",
                            stat.st_size
                        ),
                    ));
                }
                return Ok(Self {
                    ptr: std::ptr::null(),
                    len: 0,
                });
            }
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                size,
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
                len: size,
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

/// Size an fd via `fstat`. Test-only: the production read path takes
/// the data length out-of-band via the protocol (`shm_input_size` /
/// `ShmOutput::size`). Caller owns the fd in all cases — this function
/// does not close it on either success or error.
#[cfg(all(unix, test))]
unsafe fn fd_size(fd: i32) -> io::Result<usize> {
    let mut stat: libc::stat = std::mem::zeroed();
    if libc::fstat(fd, &mut stat) < 0 {
        return Err(io::Error::last_os_error());
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
        let name = output_shm_name("tree").unwrap();
        assert!(name.starts_with("/gb-"));
        assert!(name.ends_with("-tree"));
    }

    #[test]
    fn test_output_shm_name_different_labels() {
        let name_a = output_shm_name("tree").unwrap();
        let name_b = output_shm_name("dist").unwrap();
        assert_ne!(name_a, name_b);
        assert!(name_a.ends_with("-tree"));
        assert!(name_b.ends_with("-dist"));
    }

    #[test]
    fn test_output_shm_name_unique_across_calls() {
        let name_a = output_shm_name("tree").unwrap();
        let name_b = output_shm_name("tree").unwrap();
        assert_ne!(name_a, name_b, "Same label should produce unique names");
    }

    #[test]
    fn test_output_shm_name_rejects_overlong() {
        // 25-char label with a 5-digit PID and a 1-digit counter already
        // overflows: 1+2+1+5+1+1+1+25 = 37 > 31. Confirms the fallible
        // path returns Err rather than panicking.
        let long_label = "x".repeat(25);
        let result = output_shm_name(&long_label);
        assert!(result.is_err(), "Expected Err for over-long label");
    }

    #[test]
    fn test_output_shm_name_rejects_invalid_chars() {
        // Uppercase, spaces, etc. — guard against the assertion path
        // that previously panicked.
        assert!(output_shm_name("Tree").is_err());
        assert!(output_shm_name("tree alignments").is_err());
        assert!(output_shm_name("").is_err());
    }

    /// Serializes tests that compete for slots in the global pool: any test
    /// that asserts on its own slot landing must run while no other slot-
    /// pressure test is filling the pool. The basic and overflow tests
    /// share this lock; the long-name test doesn't consume slots and skips
    /// it.
    static REGISTRY_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_cleanup_registry() {
        let _guard = REGISTRY_TEST_LOCK.lock().unwrap();

        // Use a unique name so this test doesn't fight with parallel tests
        // that aren't slot-pressure tests (those still register/deregister
        // their own names without holding REGISTRY_TEST_LOCK).
        let name = format!("/gb-test-reg-{}", std::process::id());
        let _shm = SharedMemory::create(&name, 64).unwrap();
        _shm.detach();

        // Register, then verify the name is in the registry. We use the
        // private `is_registered` helper instead of poking the static
        // directly — the slot pool has no lockable handle, only atomics.
        register_for_cleanup(&name);
        assert!(
            is_registered(&name),
            "Expected {name} to be in cleanup registry"
        );

        // Deregister and verify removal. We deliberately do NOT call
        // cleanup_all() here — it walks every slot and would unlink shm
        // segments registered by tests running in parallel (e.g.
        // arrow_ipc::tests::test_write_batch_to_output_shm). Production
        // only calls cleanup_all() from the signal handler at process exit.
        deregister_cleanup(&name);
        assert!(
            !is_registered(&name),
            "Expected {name} to be removed from cleanup registry"
        );

        // Manually unlink the shm we created so the test cleans up after itself.
        let _ = SharedMemory::unlink(&name);
        assert!(SharedMemory::open_readonly(&name).is_err());
    }

    #[test]
    fn test_cleanup_registry_overflow_is_silent() {
        let _guard = REGISTRY_TEST_LOCK.lock().unwrap();

        // Filling the slot pool past its capacity must not panic and must
        // not corrupt other entries. We register MAX+5 names; at most MAX
        // can land. After deregister all are gone.
        //
        // Holding REGISTRY_TEST_LOCK keeps `test_cleanup_registry` out of
        // our way; other tests using the registry (arrow_ipc writes, etc.)
        // still register/deregister independently. We accept that some
        // small number of slots may already be occupied by those tests
        // when we start — the assertion bounds `present` from above only.
        let prefix = format!("/gb-ovf-{}", std::process::id());
        let total = MAX_CLEANUP_SLOTS + 5;
        let names: Vec<String> = (0..total).map(|i| format!("{prefix}-{i}")).collect();

        for n in &names {
            register_for_cleanup(n);
        }

        let present: usize = names.iter().filter(|n| is_registered(n)).count();
        assert!(
            present <= MAX_CLEANUP_SLOTS,
            "Registered {present} names; max is {MAX_CLEANUP_SLOTS}"
        );

        for n in &names {
            deregister_cleanup(n);
        }
        for n in &names {
            assert!(
                !is_registered(n),
                "Name {n} still in registry after deregister"
            );
        }
    }

    #[test]
    fn test_cleanup_registry_handles_long_name() {
        // Names exceeding the slot buffer must be rejected silently — the
        // alternative is data corruption in the signal handler when it
        // calls strlen on a non-NUL-terminated buffer.
        let long = format!("/gb-{}", "x".repeat(NAME_BUF_LEN));
        register_for_cleanup(&long);
        assert!(!is_registered(&long), "Over-long name must not register");
    }
}
