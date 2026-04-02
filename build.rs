use std::env;
use std::path::PathBuf;

fn main() {
    build_fasttree();
    build_prodigal();
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

/// Compile Prodigal C sources into a static library.
fn build_prodigal() {
    let dir = PathBuf::from("ext/prodigal");

    cc::Build::new()
        .file(dir.join("bitmap.c"))
        .file(dir.join("dprog.c"))
        .file(dir.join("gene.c"))
        .file(dir.join("metagenomic.c"))
        .file(dir.join("node.c"))
        .file(dir.join("sequence.c"))
        .file(dir.join("training.c"))
        .file(dir.join("prodigal_api.c"))
        .include(&dir)
        .define("PRODIGAL_NO_MAIN", None)
        .opt_level(3)
        .flag_if_supported("-finline-functions")
        .flag_if_supported("-funroll-loops")
        .warnings(false)
        .compile("prodigal_c");

    println!("cargo:rerun-if-changed=ext/prodigal/bitmap.c");
    println!("cargo:rerun-if-changed=ext/prodigal/dprog.c");
    println!("cargo:rerun-if-changed=ext/prodigal/gene.c");
    println!("cargo:rerun-if-changed=ext/prodigal/metagenomic.c");
    println!("cargo:rerun-if-changed=ext/prodigal/node.c");
    println!("cargo:rerun-if-changed=ext/prodigal/sequence.c");
    println!("cargo:rerun-if-changed=ext/prodigal/training.c");
    println!("cargo:rerun-if-changed=ext/prodigal/prodigal_api.c");
    println!("cargo:rerun-if-changed=ext/prodigal/prodigal.h");
    println!("cargo:rerun-if-changed=ext/prodigal/prodigal_internal.h");
    println!("cargo:rerun-if-changed=ext/prodigal/bitmap.h");
    println!("cargo:rerun-if-changed=ext/prodigal/dprog.h");
    println!("cargo:rerun-if-changed=ext/prodigal/fptr.h");
    println!("cargo:rerun-if-changed=ext/prodigal/gene.h");
    println!("cargo:rerun-if-changed=ext/prodigal/metagenomic.h");
    println!("cargo:rerun-if-changed=ext/prodigal/node.h");
    println!("cargo:rerun-if-changed=ext/prodigal/sequence.h");
    println!("cargo:rerun-if-changed=ext/prodigal/training.h");
}

/// Link libm on Linux (required for math functions in C submodules).
/// macOS includes libm symbols in libSystem.dylib, which is always linked.
fn link_math() {
    if env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() == "linux" {
        println!("cargo:rustc-link-lib=m");
    }
}
