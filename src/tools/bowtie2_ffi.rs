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

/// API version reported by the linked libbowtie2. Mirrors the
/// `BT2_API_VERSION_*` macros in `ext/bowtie2/bt2_api.h`. Update when the
/// submodule bumps its version. Used by both bowtie2_align and bowtie2_build
/// as the value returned from `GplTool::version()`.
pub const BT2_VERSION: &str = "0.2.0";

/// Success return code shared by every bt2_*_run function.
pub const BT2_OK: c_int = 0;

extern "C" {
    /// Return a static string describing the BT2_ERR_* category for `code`.
    /// Never returns NULL. Safe to call from any thread.
    pub fn bt2_strerror(code: c_int) -> *const c_char;
}
