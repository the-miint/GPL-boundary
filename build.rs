use std::env;
use std::path::PathBuf;

fn main() {
    build_fasttree();
    build_prodigal();
    build_sortmerna();
    // If BT2_CMAKE_LIB is set, link a pre-built CMake static library instead
    // of compiling via cc. Used to diagnose macOS aarch64 SIGSEGV — isolates
    // whether the crash is cc-crate-specific or inherent to bowtie2.
    if let Ok(lib_dir) = env::var("BT2_CMAKE_LIB") {
        println!("cargo:rustc-link-search=native={lib_dir}");
        println!("cargo:rustc-link-lib=static=bowtie2-combined-s-lib");
        // CMake build links C++ stdlib and pthreads
        let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        if target_os == "macos" {
            println!("cargo:rustc-link-lib=c++");
        } else {
            println!("cargo:rustc-link-lib=stdc++");
            println!("cargo:rustc-link-lib=pthread");
        }
        println!("cargo:rerun-if-env-changed=BT2_CMAKE_LIB");
    } else {
        build_bowtie2();
    }
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
    let cq_dir = PathBuf::from("vendor/concurrentqueue"); // vendored header-only lib

    // Probe RocksDB early so we can pass its include paths to the C++ build.
    let rocksdb = pkg_config::Config::new()
        .probe("rocksdb")
        .expect("RocksDB not found. Install librocksdb-dev (Debian/Ubuntu) or rocksdb (brew).");

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
        .include(&cq_dir);
    // RocksDB include paths from pkg-config (needed on macOS where brew
    // installs to /opt/homebrew, not on the default include path)
    for path in &rocksdb.include_paths {
        cpp_build.include(path);
    }
    cpp_build
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
/// RocksDB is already probed via pkg-config in build_sortmerna() (which
/// emits the link flags). This handles the remaining libraries.
fn link_sortmerna_deps() {
    // zlib (used directly by sortmerna for gzip support)
    println!("cargo:rustc-link-lib=z");

    // C++ standard library (cc crate handles this for compiled objects,
    // but we also need it for the final link of the Rust binary)
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" {
        println!("cargo:rustc-link-lib=c++");
    } else {
        println!("cargo:rustc-link-lib=stdc++");
    }

    // Linux-specific: libdl for dynamic loading
    if target_os == "linux" {
        println!("cargo:rustc-link-lib=dl");
    }
}

/// Compile bowtie2 C++11 sources into a static library.
///
/// Bowtie2 is C++11 with extern "C" API wrappers (bt2_api.h). We compile the
/// combined library (aligner + builder) so that tests can use bt2_build_* to
/// create index fixtures without shipping binary index files.
fn build_bowtie2() {
    let dir = PathBuf::from("ext/bowtie2");

    // SEARCH_CPPS from CMakeLists.txt (32 files)
    let search_files: Vec<PathBuf> = [
        "qual.cpp",
        "pat.cpp",
        "sam.cpp",
        "read_qseq.cpp",
        "aligner_seed_policy.cpp",
        "aligner_seed.cpp",
        "aligner_seed2.cpp",
        "aligner_sw.cpp",
        "aligner_sw_driver.cpp",
        "aligner_cache.cpp",
        "aligner_result.cpp",
        "ref_coord.cpp",
        "mask.cpp",
        "pe.cpp",
        "aln_sink.cpp",
        "dp_framer.cpp",
        "scoring.cpp",
        "presets.cpp",
        "unique.cpp",
        "simple_func.cpp",
        "random_util.cpp",
        "aligner_bt.cpp",
        "sse_util.cpp",
        "aligner_swsse.cpp",
        "outq.cpp",
        "aligner_swsse_loc_i16.cpp",
        "aligner_swsse_ee_i16.cpp",
        "aligner_swsse_loc_u8.cpp",
        "aligner_swsse_ee_u8.cpp",
        "aligner_driver.cpp",
        "bowtie_main.cpp",
        "bt2_search.cpp",
    ]
    .iter()
    .map(|f| dir.join(f))
    .collect();

    // API + build files for combined library (8 files)
    let combined_files: Vec<PathBuf> = [
        "bt2_api.cpp",
        "bt2_api_common.cpp",
        "bt2_sam_parse.cpp",
        "aln_sink_columnar.cpp",
        "bt2_build.cpp",
        "diff_sample.cpp",
        "bowtie_build_main.cpp",
        "bt2_build_api.cpp",
    ]
    .iter()
    .map(|f| dir.join(f))
    .collect();

    // SHARED_CPPS from CMakeLists.txt (14 files)
    let shared_files: Vec<PathBuf> = [
        "ccnt_lut.cpp",
        "ref_read.cpp",
        "alphabet.cpp",
        "shmem.cpp",
        "edit.cpp",
        "bt2_idx.cpp",
        "bt2_io.cpp",
        "bt2_locks.cpp",
        "bt2_util.cpp",
        "reference.cpp",
        "ds.cpp",
        "multikey_qsort.cpp",
        "limit.cpp",
        "random_source.cpp",
    ]
    .iter()
    .map(|f| dir.join(f))
    .collect();

    let mut build = cc::Build::new();
    build.cpp(true);
    build.std("c++11");

    for f in &search_files {
        build.file(f);
    }
    for f in &combined_files {
        build.file(f);
    }
    for f in &shared_files {
        build.file(f);
    }

    build
        .include(&dir)
        .include(dir.join("third_party"))
        .define("BT2_NO_MAIN", None)
        .define("BT2_BUILD_NO_MAIN", None)
        .define("BOWTIE2", None)
        // BOWTIE_MM intentionally NOT defined. It enables memory-mapped index
        // loading (mmap of .bt2 files) which is unnecessary for GPL-boundary's
        // single-process model and causes SIGSEGV on macOS aarch64.
        .define("_LARGEFILE_SOURCE", None)
        .define("_FILE_OFFSET_BITS", Some("64"))
        .define("_GNU_SOURCE", None)
        .define("NDEBUG", None)
        .define("NO_SPINLOCK", None)
        .define("WITH_QUEUELOCK", Some("1"))
        // Stub version/build strings (only used in guarded showVersion paths)
        .define("BOWTIE2_VERSION", Some("\"2.5.5\""))
        .define("BUILD_HOST", Some("\"embedded\""))
        .define("BUILD_TIME", Some("\"embedded\""))
        .define("COMPILER_VERSION", Some("\"embedded\""))
        .define("COMPILER_OPTIONS", Some("\"embedded\""));

    // Architecture-specific flags
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    if target_arch == "x86_64" {
        build.flag_if_supported("-msse2");
        build.define("POPCNT_CAPABILITY", None);
    } else if matches!(
        target_arch.as_str(),
        "aarch64" | "arm" | "s390x" | "powerpc64"
    ) {
        build.flag_if_supported("-fopenmp-simd");
    }

    build
        .opt_level(3)
        .flag_if_supported("-funroll-loops")
        .warnings(false)
        .compile("bowtie2_cpp");

    // Bowtie2 needs pthreads on Linux (sortmerna already links stdc++/c++ and zlib)
    if env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() == "linux" {
        println!("cargo:rustc-link-lib=pthread");
    }

    // Rebuild if any source in the submodule changes. With 54 compiled files,
    // directory-level tracking is more practical than listing each one.
    println!("cargo:rerun-if-changed=ext/bowtie2");
}

/// Link libm on Linux (required for math functions in C submodules).
/// macOS includes libm symbols in libSystem.dylib, which is always linked.
fn link_math() {
    if env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() == "linux" {
        println!("cargo:rustc-link-lib=m");
    }
}
