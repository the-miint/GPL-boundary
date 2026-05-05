use std::env;
use std::path::PathBuf;

fn main() {
    let rocksdb_include = build_rocksdb();
    build_fasttree();
    build_prodigal();
    build_sortmerna(&rocksdb_include);
    build_bowtie2();
    link_math();
}

/// Build RocksDB as a static library from the vendored submodule.
///
/// Pinned to v8.11.5. We deviate from SortMeRNA's own pin (v7.10.2)
/// because that release omits `<cstdint>` in headers that use
/// `uint16_t`/`uint8_t` and fails to compile on GCC 13+. SortMeRNA only
/// includes the most stable RocksDB headers (`db.h`, `options.h`,
/// `slice.h`, `version.h`) — all API-stable across 7.x→8.x — so the bump
/// is API-safe.
///
/// CMake flags mirror SortMeRNA's `cmake/presets/CMakePresets_rocksdb.json`:
/// tests/tools/gflags off, only zlib compression on. `PORTABLE=1` is added
/// to disable `-march=native` so the resulting binary runs on any CPU of
/// the target architecture (required for distributed release builds).
///
/// Returns the include path (`<install>/include`) so `build_sortmerna` can
/// add it to its C++ include search list. The `librocksdb.a` link-lib is
/// emitted from `link_sortmerna_deps` to control link order: SortMeRNA's
/// objects must appear before `-lrocksdb` so the linker resolves their
/// references.
fn build_rocksdb() -> PathBuf {
    let dst = cmake::Config::new("ext/rocksdb")
        .profile("Release")
        .define("WITH_TESTS", "OFF")
        .define("WITH_TOOLS", "OFF")
        .define("WITH_BENCHMARK_TOOLS", "OFF")
        .define("WITH_CORE_TOOLS", "OFF")
        .define("WITH_GFLAGS", "OFF")
        .define("ROCKSDB_BUILD_SHARED", "OFF")
        .define("WITH_ZLIB", "ON")
        .define("USE_RTTI", "1")
        .define("PORTABLE", "1")
        .define("FAIL_ON_WARNINGS", "OFF")
        .build();

    println!("cargo:rustc-link-search=native={}/lib", dst.display());

    println!("cargo:rerun-if-changed=ext/rocksdb/CMakeLists.txt");

    dst.join("include")
}

/// Compile FastTree C sources into a static library.
/// Each submodule gets its own build function and separate .a output
/// to prevent symbol collisions between C libraries.
///
/// OpenMP: `-DOPENMP` activates the parallel pragmas + omp_lock_t fields
/// in `fasttree_internal.h`. Without it, `#pragma omp parallel for` is
/// dropped and `n_threads` from `FastTreeConfig` has no effect. The
/// submodule's `MLQuartetNNI` (since `2a6c14b`) gates its single-thread
/// override on `omp_get_max_threads() == 1` at runtime, so OpenMP and
/// non-OpenMP builds produce bit-equal trees at `n_threads=1`; parity
/// baselines are regenerated against the non-OpenMP `FastTree` binary.
/// See `GUIDANCE_PARITY_TESTS.md`.
fn build_fasttree() {
    let dir = PathBuf::from("ext/fasttree");
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();

    let mut build = cc::Build::new();
    build
        .file(dir.join("fasttree_core.c"))
        .file(dir.join("fasttree_api.c"))
        .include(&dir)
        .define("FASTTREE_NO_MAIN", None)
        .define("USE_DOUBLE", None)
        .define("OPENMP", None)
        .flag("-fvisibility=hidden")
        .opt_level(3)
        .flag_if_supported("-finline-functions")
        .flag_if_supported("-funroll-loops")
        .warnings(false);

    if target_os == "macos" {
        // Apple's clang ships without libomp. Probe Homebrew's libomp and
        // fail-fast if missing — `-fopenmp` would otherwise be silently
        // accepted by the preprocessor while the runtime symbols never
        // resolve, leaving `n_threads` a no-op without any error.
        //
        // Link libomp **statically** so the released binary has no runtime
        // dependency on a Homebrew-installed `libomp.dylib`. A dynamic link
        // bakes `/opt/homebrew/opt/libomp/lib/libomp.dylib` into
        // `LC_LOAD_DYLIB`, which makes the binary unusable on any macOS
        // box without Homebrew. Homebrew's libomp formula ships
        // `libomp.a` alongside the dylib, and libomp's only transitive
        // deps on macOS (pthread, libm, libdl) live in libSystem.dylib —
        // so the static archive folds in cleanly with no further link
        // additions.
        let libomp = locate_macos_libomp();
        build
            .flag("-Xpreprocessor")
            .flag("-fopenmp")
            .include(libomp.join("include"));
        println!(
            "cargo:rustc-link-search=native={}",
            libomp.join("lib").display()
        );
        println!("cargo:rustc-link-lib=static=omp");
    } else {
        // Linux + other Unix: GCC and Clang both accept `-fopenmp` for
        // the *compiler* (it enables the pragmas + sets the omp_* macros),
        // but they ship different runtime libraries — GCC needs `gomp`,
        // Clang needs `omp`. Refusing `-fopenmp` outright is rare on
        // Linux but possible on minimal toolchains; treat it as
        // configuration error rather than letting the link fail with a
        // cryptic `__kmpc_*` undefined-symbol message.
        if !build.is_flag_supported("-fopenmp").unwrap_or(false) {
            panic!(
                "Compiler does not accept `-fopenmp`. FastTree's `threads` knob \
                 requires an OpenMP-capable C compiler. Install GCC, or a Clang \
                 with libomp headers, and re-run."
            );
        }
        build.flag("-fopenmp");
        let compiler = build.get_compiler();
        let runtime = if compiler.is_like_clang() {
            "omp"
        } else {
            "gomp"
        };
        println!("cargo:rustc-link-lib={runtime}");
    }

    build.compile("fasttree_c");

    println!("cargo:rerun-if-changed=ext/fasttree/fasttree_core.c");
    println!("cargo:rerun-if-changed=ext/fasttree/fasttree_api.c");
    println!("cargo:rerun-if-changed=ext/fasttree/fasttree.h");
    println!("cargo:rerun-if-changed=ext/fasttree/fasttree_internal.h");
}

/// Find a usable Homebrew libomp installation on macOS, or panic with a
/// clear remediation message. Apple's clang does not ship libomp; without
/// this guard the build would either fail at link time with cryptic
/// `__kmpc_*` symbol errors, or — worse — succeed but leave OpenMP
/// pragmas as no-ops because `<omp.h>` isn't on the include path.
fn locate_macos_libomp() -> PathBuf {
    // `brew --prefix libomp` is the source of truth; fall back to the
    // standard Apple-Silicon and Intel-mac install prefixes if `brew`
    // isn't on PATH (e.g., a CI image with libomp installed manually).
    let candidates = [
        std::process::Command::new("brew")
            .args(["--prefix", "libomp"])
            .output()
            .ok()
            .and_then(|o| {
                if o.status.success() {
                    String::from_utf8(o.stdout)
                        .ok()
                        .map(|s| PathBuf::from(s.trim()))
                } else {
                    None
                }
            }),
        Some(PathBuf::from("/opt/homebrew/opt/libomp")),
        Some(PathBuf::from("/usr/local/opt/libomp")),
    ];
    // Require both the header AND the static archive. We link libomp
    // statically (see `build_fasttree`) so a prefix that only has
    // `libomp.dylib` is not usable. Homebrew's libomp formula ships
    // both, so a normal `brew install libomp` satisfies this; an
    // unusual half-install would otherwise pass an `is_dir()` check
    // and fail later with cryptic `__kmpc_*` errors at link time.
    for p in candidates.into_iter().flatten() {
        if p.join("include/omp.h").is_file() && p.join("lib/libomp.a").is_file() {
            return p;
        }
    }
    panic!(
        "OpenMP static archive (libomp.a) not found on macOS. Install with:\n\
         \n    brew install libomp\n\n\
         FastTree needs OpenMP to honor the `threads` config knob, and we \
         link it statically so the resulting binary has no runtime dependency \
         on Homebrew. Checked: `brew --prefix libomp`, /opt/homebrew/opt/libomp, \
         /usr/local/opt/libomp."
    );
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
        .flag("-fvisibility=hidden")
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
/// SortMeRNA is C++17. RocksDB comes from the vendored static build
/// (`build_rocksdb`); zlib is still expected from the system. Split into
/// two cc::Build instances: one for C sources (cmph + ssw), one for C++17
/// sources (alp + sortmerna core + smr_api).
fn build_sortmerna(rocksdb_include: &PathBuf) {
    let dir = PathBuf::from("ext/sortmerna");
    let smr_src = dir.join("src/sortmerna");
    let api_src = dir.join("src/smr_api");
    let cmph_dir = dir.join("3rdparty/cmph");
    let alp_dir = dir.join("3rdparty/alp");
    let cq_dir = PathBuf::from("vendor/concurrentqueue"); // vendored header-only lib

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
        .flag("-fvisibility=hidden")
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
        .include(rocksdb_include);
    cpp_build
        .define("SMR_NO_MAIN", None)
        .flag("-fvisibility=hidden")
        .flag("-fvisibility-inlines-hidden")
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
namespace sortmerna {
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
        .flag("-fvisibility=hidden")
        .warnings(false)
        .compile("smr_build_version");

    link_sortmerna_deps();

    // Rebuild triggers
    println!("cargo:rerun-if-changed=ext/sortmerna/include/smr_api.h");
    println!("cargo:rerun-if-changed=ext/sortmerna/src/smr_api/smr_api.cpp");
}

/// Link libraries required by SortMeRNA.
///
/// Order matters for static linking: the linker resolves symbols
/// left-to-right, so each lib must come *after* anything that depends on it.
/// SortMeRNA's `smr_cpp` archive is emitted by cc::Build above; this
/// function appends rocksdb (referenced by smr_cpp), then zlib (referenced
/// by both smr_cpp and rocksdb), then the C++ runtime.
fn link_sortmerna_deps() {
    // RocksDB static lib from build_rocksdb(). Must come AFTER smr_cpp
    // (emitted by cc::Build above) and BEFORE zlib (which rocksdb uses).
    println!("cargo:rustc-link-lib=static=rocksdb");

    // zlib (used directly by sortmerna for gzip support and by rocksdb
    // for SST compression). Stays dynamic — every supported platform ships
    // a usable libz.
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
        .define("BOWTIE_MM", None)
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

    // Hide all C++ symbols — only the extern "C" bt2_* API is needed by Rust.
    // Without this, C++ template instantiations (SStringExpandable, EList, etc.)
    // leak into the Rust binary's symbol table and collide with system/runtime
    // symbols on macOS aarch64, causing SIGSEGV in worker threads.
    build
        .flag("-fvisibility=hidden")
        .flag("-fvisibility-inlines-hidden")
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
