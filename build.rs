use std::env;
use std::path::PathBuf;

fn main() {
    build_fasttree();
    build_prodigal();
    build_bowtie2();
    link_math();
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
        // dependency on a Homebrew-installed `libomp.dylib`. A dynamic
        // link bakes `/opt/homebrew/opt/libomp/lib/libomp.dylib` into
        // `LC_LOAD_DYLIB`, which makes the binary unusable on any macOS
        // box without Homebrew.
        //
        // We pass `libomp.a` as a positional linker arg rather than using
        // `cargo:rustc-link-lib=static=omp`. On Linux the `static=` kind
        // wraps the lib in `-Wl,-Bstatic ... -Wl,-Bdynamic` so GNU ld is
        // forced to pick the archive; Apple's ld has no `-Bstatic`/
        // `-Bdynamic` switch, so when both `libomp.a` and `libomp.dylib`
        // sit in the same Homebrew lib dir, ld silently picks the dylib
        // and the `static=` annotation is a no-op. A positional `.a`
        // path is unambiguous: ld treats it as "include this archive."
        // Homebrew's libomp formula ships `libomp.a` alongside the
        // dylib, and libomp's only transitive deps on macOS (pthread,
        // libm, libdl) live in libSystem.dylib — so the static archive
        // folds in cleanly with no further link additions.
        let libomp = locate_macos_libomp();
        build
            .flag("-Xpreprocessor")
            .flag("-fopenmp")
            .include(libomp.join("include"));
        println!(
            "cargo:rustc-link-arg={}",
            libomp.join("lib/libomp.a").display()
        );
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
        // As the fork's Makefile: without it aarch64 fuses multiply-adds and
        // prints some scores differently from x86-64 in the last digit.
        .flag("-ffp-contract=off")
        // -O3 alone, as the fork's Makefile: -funroll-loops (with
        // -finline-functions, FastTree's flags, copied here once) made
        // metagenomic mode 46% slower (E. coli 12.4 s → 18.1 s, the same
        // output; -finline-functions alone changes nothing at -O3).
        .opt_level(3)
        .warnings(false)
        .compile("prodigal_c");

    // The whole submodule, so a source added or removed upstream is seen
    println!("cargo:rerun-if-changed=ext/prodigal");
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

    // After bowtie2_cpp, which references them: zlib (dynamic; every supported
    // platform ships one), the C++ runtime for the final link of the Rust
    // binary, and pthreads on Linux.
    println!("cargo:rustc-link-lib=z");
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "macos" {
        println!("cargo:rustc-link-lib=c++");
    } else {
        println!("cargo:rustc-link-lib=stdc++");
    }
    if target_os == "linux" {
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
