use std::env;
use std::path::PathBuf;

fn main() {
    let fasttree_dir = PathBuf::from("ext/fasttree");

    cc::Build::new()
        .file(fasttree_dir.join("fasttree_core.c"))
        .file(fasttree_dir.join("fasttree_api.c"))
        .include(&fasttree_dir)
        .define("FASTTREE_NO_MAIN", None)
        .define("USE_DOUBLE", None)
        .opt_level(3)
        .flag_if_supported("-finline-functions")
        .flag_if_supported("-funroll-loops")
        .warnings(false)
        .compile("fasttree");

    println!("cargo:rerun-if-changed=ext/fasttree/fasttree_core.c");
    println!("cargo:rerun-if-changed=ext/fasttree/fasttree_api.c");
    println!("cargo:rerun-if-changed=ext/fasttree/fasttree.h");
    println!("cargo:rerun-if-changed=ext/fasttree/fasttree_internal.h");

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "linux" || target_os == "macos" {
        println!("cargo:rustc-link-lib=m");
    }
}
