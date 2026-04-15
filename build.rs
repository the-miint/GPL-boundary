use std::env;
use std::path::PathBuf;

fn main() {
    build_fasttree();
    build_prodigal();
    build_sortmerna();
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

/// Compile SortMeRNA C and C++ sources into static libraries.
///
/// SortMeRNA is C++17 and requires RocksDB + zlib as system dependencies.
/// Split into two cc::Build instances: one for C sources (cmph + ssw),
/// one for C++17 sources (alp + sortmerna core + smr_api).
fn build_sortmerna() {
    let dir = PathBuf::from("ext/sortmerna");
    let smr_src = dir.join("src/sortmerna");
    let api_src = dir.join("src/smr_api");
    let cmph_dir = dir.join("3rdparty/cmph");
    let alp_dir = dir.join("3rdparty/alp");
    let cq_dir = dir.join("3rdparty/concurrentqueue");

    // -- C sources: cmph (23 files) + ssw.c --
    let cmph_files: Vec<PathBuf> = [
        "bdz.c",
        "bdz_ph.c",
        "bmz.c",
        "bmz8.c",
        "brz.c",
        "buffer_entry.c",
        "buffer_manager.c",
        "chd.c",
        "chd_ph.c",
        "chm.c",
        "cmph.c",
        "cmph_structs.c",
        "compressed_rank.c",
        "compressed_seq.c",
        "fch.c",
        "fch_buckets.c",
        "graph.c",
        "hash.c",
        "jenkins_hash.c",
        "miller_rabin.c",
        "select.c",
        "vqueue.c",
        "vstack.c",
    ]
    .iter()
    .map(|f| cmph_dir.join(f))
    .collect();

    let mut c_build = cc::Build::new();
    for f in &cmph_files {
        c_build.file(f);
    }
    c_build
        .file(smr_src.join("ssw.c"))
        .include(&cmph_dir)
        .include(dir.join("include"))
        .opt_level(3)
        .warnings(false)
        .compile("smr_c");

    // -- C++17 sources: alp (15 files) + sortmerna core (31 files) + smr_api --
    let alp_files: Vec<PathBuf> = [
        "njn_dynprogprob.cpp",
        "njn_dynprogproblim.cpp",
        "njn_dynprogprobproto.cpp",
        "njn_ioutil.cpp",
        "njn_localmaxstat.cpp",
        "njn_localmaxstatmatrix.cpp",
        "njn_localmaxstatutil.cpp",
        "njn_random.cpp",
        "sls_alignment_evaluer.cpp",
        "sls_alp.cpp",
        "sls_alp_data.cpp",
        "sls_alp_regression.cpp",
        "sls_alp_sim.cpp",
        "sls_basic.cpp",
        "sls_pvalues.cpp",
    ]
    .iter()
    .map(|f| alp_dir.join(f))
    .collect();

    // Core sortmerna .cpp files (excludes main.cpp, read_control.cpp, writer.cpp,
    // minoccur.cpp [dead code: find_minoccur() is never called])
    let core_files: Vec<PathBuf> = [
        "alignment.cpp",
        "bitvector.cpp",
        "cmd.cpp",
        "index.cpp",
        "indexdb.cpp",
        "izlib.cpp",
        "kseq_load.cpp",
        "kvdb.cpp",
        "options.cpp",
        "otumap.cpp",
        "output.cpp",
        "paralleltraversal.cpp",
        "processor.cpp",
        "read.cpp",
        "readfeed.cpp",
        "readstats.cpp",
        "references.cpp",
        "refstats.cpp",
        "report.cpp",
        "report_biom.cpp",
        "report_blast.cpp",
        "report_denovo.cpp",
        "report_fastx.cpp",
        "report_fx_base.cpp",
        "report_fx_other.cpp",
        "report_sam.cpp",
        "smr_log.cpp",
        "summary.cpp",
        "traverse_bursttrie.cpp",
        "util.cpp",
    ]
    .iter()
    .map(|f| smr_src.join(f))
    .collect();

    let mut cpp_build = cc::Build::new();
    cpp_build.cpp(true);
    cpp_build.std("c++17");
    for f in &alp_files {
        cpp_build.file(f);
    }
    for f in &core_files {
        cpp_build.file(f);
    }
    cpp_build
        .file(api_src.join("smr_api.cpp"))
        .include(dir.join("include"))
        .include(&cmph_dir)
        .include(&alp_dir)
        .include(&cq_dir)
        .define("SMR_NO_MAIN", None)
        .opt_level(3)
        .warnings(false)
        // alp uses deprecated `register` keyword; Clang 16+ treats this as
        // a hard error in C++17 mode even with -w. Suppress it explicitly.
        .flag_if_supported("-Wno-register")
        .compile("smr_cpp");

    // Provide build_version symbols expected by options.cpp.
    // CMake generates these from git info; we provide stubs since
    // opt_version() is only reachable from the standalone binary's main().
    let build_ver_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("smr_build_version.cpp");
    std::fs::write(
        &build_ver_path,
        r#"
extern "C++" {
    const char* sortmerna_build_compile_date = "embedded";
    const char* sortmerna_build_git_sha = "embedded";
    const char* sortmerna_build_git_date = "embedded";
}
"#,
    )
    .expect("Failed to write smr_build_version.cpp");

    cc::Build::new()
        .cpp(true)
        .file(&build_ver_path)
        .warnings(false)
        .compile("smr_build_version");

    link_sortmerna_deps();

    // Rebuild triggers
    println!("cargo:rerun-if-changed=ext/sortmerna/include/smr_api.h");
    println!("cargo:rerun-if-changed=ext/sortmerna/src/smr_api/smr_api.cpp");
}

/// Link system libraries required by SortMeRNA.
fn link_sortmerna_deps() {
    // RocksDB via pkg-config (finds headers + link flags automatically)
    pkg_config::Config::new()
        .probe("rocksdb")
        .expect("RocksDB not found. Install librocksdb-dev (Debian/Ubuntu) or rocksdb (brew).");

    // zlib (used directly by sortmerna for gzip support)
    println!("cargo:rustc-link-lib=z");

    // C++ standard library
    println!("cargo:rustc-link-lib=stdc++");

    // Linux-specific: libdl for dynamic loading
    if env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() == "linux" {
        println!("cargo:rustc-link-lib=dl");
    }
}

/// Link libm on Linux (required for math functions in C submodules).
/// macOS includes libm symbols in libSystem.dylib, which is always linked.
fn link_math() {
    if env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() == "linux" {
        println!("cargo:rustc-link-lib=m");
    }
}
