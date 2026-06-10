//! FFI bindings shared between bowtie2_align and bowtie2_build.
//!
//! Both tools link the same libbowtie2 static archive and call into the
//! same C symbol set. Declaring `extern "C"` blocks separately in each
//! module would create two parallel signatures of the same symbol —
//! Rust does not type-check `extern "C"` declarations across crates or
//! modules, so a future signature change to `bt2_strerror` could silently
//! diverge between modules and corrupt the stack on call. Centralizing the
//! shared declarations here makes that impossible.

use std::os::raw::{c_char, c_int};

/// bowtie2 **software** version reported by `GplTool::version()` for both
/// bowtie2-align and bowtie2-build. Tracks the `ext/bowtie2` submodule pin
/// (`v2.5.5-miint`) and must stay in sync with the `BOWTIE2_VERSION` string
/// compiled into the C library by `build.rs` — bump both together when the
/// submodule is upgraded.
///
/// This is deliberately NOT the C **API** version (`BT2_API_VERSION_*` in
/// `bt2_api.h`, currently 0.3.0) nor a per-tool `schema_version`. `version()`
/// reports the upstream tool version, matching `FASTTREE_VERSION` /
/// `PRODIGAL_VERSION`. The bowtie2 C API exposes no runtime version accessor
/// (unlike sortmerna's `smr_version()`), so this is a manual constant; if the
/// submodule ever adds a `bt2_version()` symbol, prefer querying it at runtime.
pub const BOWTIE2_VERSION: &str = "2.5.5";

/// Success return code shared by every bt2_*_run function.
pub const BT2_OK: c_int = 0;

extern "C" {
    /// Return a static string describing the BT2_ERR_* category for `code`.
    /// Never returns NULL. Safe to call from any thread.
    pub fn bt2_strerror(code: c_int) -> *const c_char;
}
