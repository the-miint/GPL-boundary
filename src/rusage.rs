//! Per-batch worker telemetry: `getrusage(RUSAGE_SELF)` deltas plus Linux
//! `/proc/self` reads, packaged as a [`BatchMetrics`].
//!
//! This is only meaningful inside a *single-purpose* subprocess worker
//! (bowtie2-align), where `RUSAGE_SELF` maps to exactly that worker's work and
//! batches run serially. In the shared daemon process `RUSAGE_SELF` would
//! aggregate concurrent batches across threads, so it is never used there.
//!
//! Counters (`ru_majflt`, CPU time, block I/O, context switches) are cumulative
//! per process, so a batch's contribution is `after - before`. `ru_maxrss` is a
//! high-water mark, so we report its absolute value, not a delta. The Linux
//! `/proc/self/io` byte counters and `/proc/self/statm` current-RSS are
//! `None` on platforms without `/proc`.

use std::time::Instant;

use crate::protocol::BatchMetrics;

/// A snapshot of the cumulative per-process counters we delta across a batch,
/// taken via one `getrusage` call plus (on Linux) one `/proc/self/io` read.
#[derive(Clone, Copy)]
pub struct ResourceSnapshot {
    utime_us: u64,
    stime_us: u64,
    majflt: u64,
    minflt: u64,
    inblock: u64,
    oublock: u64,
    nvcsw: u64,
    nivcsw: u64,
    /// `ru_maxrss` normalized to KB (Linux reports KB, macOS reports bytes).
    maxrss_kb: u64,
    /// `/proc/self/io` `read_bytes` (Linux only).
    read_bytes: Option<u64>,
    /// `/proc/self/io` `write_bytes` (Linux only).
    write_bytes: Option<u64>,
}

impl ResourceSnapshot {
    /// Capture the current resource counters for this process.
    pub fn capture() -> Self {
        // Safety: `getrusage` only writes into the provided struct; a
        // zero-initialized `rusage` is a valid out-param.
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        // getrusage(RUSAGE_SELF, &valid) cannot fail at runtime — EINVAL is for
        // a bad `who`, EFAULT for a bad pointer, neither reachable here — so a
        // nonzero return is a programmer error, caught in debug/test builds.
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
        debug_assert_eq!(
            rc,
            0,
            "getrusage(RUSAGE_SELF) failed: {}",
            std::io::Error::last_os_error()
        );
        // Clamp on each field's own type (tv_usec is i32 on macOS, i64 on
        // Linux) so a stray negative can't wrap to a huge positive.
        let to_us =
            |tv: libc::timeval| (tv.tv_sec.max(0) as u64) * 1_000_000 + (tv.tv_usec.max(0) as u64);
        let nonneg = |v: libc::c_long| v.max(0) as u64;
        let (read_bytes, write_bytes) = proc_io_bytes();
        Self {
            utime_us: to_us(ru.ru_utime),
            stime_us: to_us(ru.ru_stime),
            majflt: nonneg(ru.ru_majflt),
            minflt: nonneg(ru.ru_minflt),
            inblock: nonneg(ru.ru_inblock),
            oublock: nonneg(ru.ru_oublock),
            nvcsw: nonneg(ru.ru_nvcsw),
            nivcsw: nonneg(ru.ru_nivcsw),
            maxrss_kb: maxrss_to_kb(ru.ru_maxrss),
            read_bytes,
            write_bytes,
        }
    }
}

/// Build the per-batch [`BatchMetrics`] from a before/after snapshot pair.
/// The daemon-clock fields (`t_dispatch_ms`, `t_worker_ms`) are left `None`
/// for the parent's reader thread to fill in.
fn metrics_from(
    before: &ResourceSnapshot,
    after: &ResourceSnapshot,
    wall_ms: u64,
    reused: bool,
) -> BatchMetrics {
    // Saturating: cumulative counters only grow, but a saturating sub keeps a
    // bogus 0 instead of an underflow panic if the kernel ever surprises us.
    let d = |a: u64, b: u64| a.saturating_sub(b);
    let delta_opt = |a: Option<u64>, b: Option<u64>| match (a, b) {
        (Some(a), Some(b)) => Some(a.saturating_sub(b)),
        _ => None,
    };
    BatchMetrics {
        worker_wall_ms: wall_ms,
        worker_user_ms: d(after.utime_us, before.utime_us) / 1000,
        worker_sys_ms: d(after.stime_us, before.stime_us) / 1000,
        worker_majflt: d(after.majflt, before.majflt),
        worker_minflt: d(after.minflt, before.minflt),
        worker_inblock: d(after.inblock, before.inblock),
        worker_oublock: d(after.oublock, before.oublock),
        worker_nvcsw: d(after.nvcsw, before.nvcsw),
        worker_nivcsw: d(after.nivcsw, before.nivcsw),
        worker_rss_kb: after.maxrss_kb,
        worker_reused: reused,
        worker_rss_current_kb: current_rss_kb(),
        worker_read_bytes: delta_opt(after.read_bytes, before.read_bytes),
        worker_write_bytes: delta_opt(after.write_bytes, before.write_bytes),
        t_dispatch_ms: None,
        t_worker_ms: None,
    }
}

/// Run `f`, bracketing it with resource snapshots and a wall clock, and return
/// its result alongside the populated [`BatchMetrics`]. `reused` records
/// whether the worker had already processed an earlier batch (warm index).
pub fn measure<R>(reused: bool, f: impl FnOnce() -> R) -> (R, BatchMetrics) {
    let before = ResourceSnapshot::capture();
    let start = Instant::now();
    let out = f();
    let wall_ms = start.elapsed().as_millis() as u64;
    let after = ResourceSnapshot::capture();
    (out, metrics_from(&before, &after, wall_ms, reused))
}

// --- platform-specific /proc readers (Linux only) -------------------------

#[cfg(target_os = "linux")]
fn maxrss_to_kb(v: libc::c_long) -> u64 {
    // Linux: ru_maxrss is already in KB.
    v.max(0) as u64
}

#[cfg(not(target_os = "linux"))]
fn maxrss_to_kb(v: libc::c_long) -> u64 {
    // macOS/BSD: ru_maxrss is in bytes.
    (v.max(0) as u64) / 1024
}

#[cfg(target_os = "linux")]
fn proc_io_bytes() -> (Option<u64>, Option<u64>) {
    let contents = match std::fs::read_to_string("/proc/self/io") {
        Ok(c) => c,
        Err(_) => return (None, None),
    };
    let mut read_bytes = None;
    let mut write_bytes = None;
    for line in contents.lines() {
        if let Some(v) = line.strip_prefix("read_bytes:") {
            read_bytes = v.trim().parse().ok();
        } else if let Some(v) = line.strip_prefix("write_bytes:") {
            write_bytes = v.trim().parse().ok();
        }
    }
    (read_bytes, write_bytes)
}

#[cfg(not(target_os = "linux"))]
fn proc_io_bytes() -> (Option<u64>, Option<u64>) {
    (None, None)
}

#[cfg(target_os = "linux")]
fn current_rss_kb() -> Option<u64> {
    // /proc/self/statm fields are in pages: size resident shared text ...
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        // sysconf error (or a nonsensical page size) — report "unknown", not 0.
        return None;
    }
    // Multiply before dividing so a sub-KB page size can't truncate to 0.
    Some(resident_pages * page_size as u64 / 1024)
}

#[cfg(not(target_os = "linux"))]
fn current_rss_kb() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_measure_populates_metrics() {
        // Do a little work that touches memory so the deltas are plausible.
        let (sum, metrics) = measure(false, || {
            let mut acc = 0u64;
            let v: Vec<u64> = (0..100_000).collect();
            for x in &v {
                acc = acc.wrapping_add(*x);
            }
            acc
        });
        assert_eq!(sum, (0..100_000u64).sum::<u64>());
        // worker_reused threads through verbatim.
        assert!(!metrics.worker_reused);
        // RSS is an absolute peak, so it must be non-zero on any live process.
        assert!(metrics.worker_rss_kb > 0, "peak RSS should be > 0");
        // Daemon-clock fields are not set by the worker side.
        assert!(metrics.t_dispatch_ms.is_none());
        assert!(metrics.t_worker_ms.is_none());
    }

    #[test]
    fn test_measure_reused_flag_threads_through() {
        let (_, metrics) = measure(true, || {});
        assert!(metrics.worker_reused);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_linux_proc_fields_present() {
        // On Linux the /proc-derived fields must resolve to Some.
        let (_, metrics) = measure(false, || {});
        assert!(
            metrics.worker_rss_current_kb.is_some(),
            "current RSS should be readable from /proc/self/statm on Linux"
        );
        assert!(
            metrics.worker_read_bytes.is_some(),
            "read_bytes delta should be Some on Linux"
        );
    }
}
