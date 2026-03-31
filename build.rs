use std::env;
use std::path::PathBuf;

fn main() {
    build_fasttree();
    link_math();
}

/// Compile FastTree C sources into a static library.
/// Each submodule gets its own build function and separate .a output
/// to prevent symbol collisions between C libraries.
fn build_fasttree() {
    let dir = PathBuf::from("ext/fasttree");

    cc::Build::new()
        .file(dir.join("fasttree_core.c"))
        .file(dir.join("fasttree_api.c"))
        .include(&dir)
        .define("FASTTREE_NO_MAIN", None)
        .define("USE_DOUBLE", None)
        .opt_level(3)
        .flag_if_supported("-finline-functions")
        .flag_if_supported("-funroll-loops")
        .warnings(false)
        .compile("fasttree_c");

    println!("cargo:rerun-if-changed=ext/fasttree/fasttree_core.c");
    println!("cargo:rerun-if-changed=ext/fasttree/fasttree_api.c");
    println!("cargo:rerun-if-changed=ext/fasttree/fasttree.h");
    println!("cargo:rerun-if-changed=ext/fasttree/fasttree_internal.h");
}

/// Link libm on Linux (required for math functions in C submodules).
/// macOS includes libm symbols in libSystem.dylib, which is always linked.
fn link_math() {
    if env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() == "linux" {
        println!("cargo:rustc-link-lib=m");
    }
}
