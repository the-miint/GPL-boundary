# Bowtie2 SIGSEGV on macOS aarch64 — all bt2_align_run calls crash

## Summary

`bt2_align_run` crashes with SIGSEGV on macOS aarch64 (Apple Silicon)
for **any** input — not limited to all-N reads or `no_unal`. The crash
occurs in `MemoryPatternSource::nextBatch` during string copy. The same
inputs work on Linux x86_64. The bowtie2 maintainers' standalone C
reproducer did not crash (see their investigation below), suggesting the
issue involves the Rust test harness environment or `cc` crate build
flags.

## Environment

- **Platform**: macOS aarch64 (GitHub Actions `macos-latest`, Apple M-series)
- **Toolchain**: Rust stable 1.95.0, Apple Clang (Xcode default)
- **Bowtie2 submodule**: `the-miint/bowtie2` branch `v2.5.5-miint`,
  commit `65c91b4`
- **Linux (works)**: Ubuntu x86_64 (GitHub Actions `ubuntu-latest`),
  same bowtie2 commit, same test input — passes without issue

## Reproducer

The following test reliably segfaults on macOS aarch64 CI:

```rust
#[test]
fn test_bowtie2_align_no_unal_zero_records() {
    let (_dir, index_prefix) = build_test_index();

    // A read that won't align to the reference
    let garbage_read = "NNNNNNNNNNNNNNNNNNNNNNNNNNN";  // 27 N's
    let garbage_qual = &"!".repeat(27);                // lowest Phred

    // ... write read to shared memory as Arrow IPC ...

    let config = serde_json::json!({
        "index_path": index_prefix,
        "no_unal": true,
        "seed": 42,
    });
    let response = tool.execute(&config, &input_name);
    // SIGSEGV here on macOS aarch64
}
```

The test index is a 200 bp synthetic reference built via `bt2_build_run`.
The reference sequence is:

```
>ref1
ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT
AAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGG
TTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCG
GCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAG
```

## What the test exercises

The test verifies the zero-records output path: when `no_unal=true` and
no reads align, `bt2_align_run` should return `n_records=0` with NULL
array pointers in `bt2_align_output_t`. The Rust adapter's
`soa_to_record_batch` function handles this case by producing an empty
Arrow RecordBatch without dereferencing the NULL pointers.

On Linux x86_64, this path works correctly — `bt2_align_run` returns
successfully with `n_records=0`. On macOS aarch64, the crash occurs
inside `bt2_align_run` before it returns to the caller.

## C API call sequence

```c
bt2_align_config_t config;
bt2_align_config_init(&config);
config.index_path = "/path/to/idx";
config.no_unal = 1;
config.seed = 42;

bt2_align_ctx_t *ctx = bt2_align_create(&config, &err);
// ctx is non-NULL (index loads successfully)

bt2_input_t input;
bt2_input_init(&input);
input.names = &name_ptr;    // "garbage"
input.seqs  = &seq_ptr;     // "NNNNNNNNNNNNNNNNNNNNNNNNNNN"
input.quals = &qual_ptr;    // "!!!!!!!!!!!!!!!!!!!!!!!!!!!"
input.n_reads = 1;

bt2_align_output_t *output = NULL;
bt2_align_stats_t stats;
int rc = bt2_align_run(ctx, &input, &output, &stats);
// ^^^ SIGSEGV on macOS aarch64
```

## lldb backtrace (from CI run 24594946717)

Obtained by running the Rust test binary under `lldb -b` on macOS
aarch64 in GitHub Actions:

```
thread #4, stop reason = EXC_BAD_ACCESS (code=1, address=0x10)
  frame #0: MemoryPatternSource::nextBatch(PerThreadReadBuf&, bool, bool)
    [inlined] SStringExpandable<char, 1024, 2, 1024>::operator=
             (this=<unavailable>, o=<unavailable>)
    at sstring.h:1740:3 [opt]

   1739  SStringExpandable<T,S,M,I>& operator=(const SStringExpandable<T,S,M,I>& o) {
-> 1740      install(o.cs_, o.len_);
              ^
   1741      return *this;
```

The crash is a NULL or near-NULL dereference (`address=0x10`) during
string copy in `SStringExpandable::operator=`. The source object `o`
has an invalid `cs_` pointer. This happens inside
`MemoryPatternSource::nextBatch`, which reads from the `bt2_input_t`
read arrays passed to `bt2_align_run`.

The crash is on thread #4 (not the main thread), inside bowtie2's
internal threading for `multiseedSearch`.

## Bowtie2 maintainers' investigation

The maintainers tested a standalone C reproducer
(`test_repro_no_unal_all_n.c`) with the exact same inputs on macOS
aarch64 — it passed, including under ASan. Their conclusion: the crash
involves the Rust test harness environment or build configuration, not
a simple `inputs → bt2_align_run` issue. See
`ISSUE-bowtie2-sigsegv-macos-aarch64-investigation.md` for their full
analysis.

Their suggested areas to investigate:
1. **Process-level state contamination** — prior tests in the same
   process may leave global/static state that affects later calls
2. **Build-flag mismatch** — the `cc` crate's compilation flags may
   differ from CMake's defaults (optimization, threading, NDEBUG)
3. **Thread stack size** — Rust's default 2 MB thread stack vs. the
   larger default on macOS

## Analysis of the backtrace

The fault address `0x10` is a struct member offset from a NULL base
pointer. In `SStringExpandable::operator=`, `o.cs_` is being read —
if `o` itself is at address `0x0`, then `o.cs_` (at some offset like
`+0x10`) produces the faulting address. This means
`MemoryPatternSource::nextBatch` received or constructed an
`SStringExpandable` with a NULL or dangling backing pointer.

Since the standalone C reproducer works, the issue is likely in how
the `cc` crate compiles `MemoryPatternSource` or how the Rust binary's
link-time environment differs from a CMake build. Differences to
investigate:
- `-O3` vs `-O2` (the `cc` crate uses `-O3` by default in `build.rs`)
- Missing defines (`-DNDEBUG`, threading macros)
- Link order or missing TLS initialization

## Current workaround

All bowtie2 tests that call `bt2_align_run` are skipped on macOS:

```rust
#[cfg_attr(target_os = "macos", ignore = "SIGSEGV in bowtie2 C library on macOS aarch64")]
```

## Next steps

1. Compare `build.rs` cc crate flags with bowtie2's CMakeLists.txt for
   missing defines or flag mismatches on aarch64
2. Try building with `-O0` or `-O2` instead of `-O3` to rule out
   optimization-induced UB
3. Run the Rust test binary under ASan on macOS (requires
   `RUSTFLAGS="-Zsanitizer=address"` with nightly)
