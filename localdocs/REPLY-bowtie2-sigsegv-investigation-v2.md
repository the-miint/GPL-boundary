# Reply to Bowtie2 SIGSEGV Investigation v2

Responses to the four data points requested.

## 1. config.nthreads value

**Always 1.** The roundtrip tests explicitly set `"nthreads": 1`. The
no_unal test does not set nthreads, so it gets the default from
`bt2_align_config_init` (which sets `nthreads = 1`). The streaming
tests also use the default.

The crash backtrace shows thread #4, which is surprising for
`nthreads=1`. Bowtie2 may still spawn internal worker threads even
with `nthreads=1`.

## 2. cc crate build flags

From `build.rs` lines 374-409, the effective defines are:

```
-DBOWTIE2
-DBOWTIE_MM              ← changes worker-thread setup path
-D_LARGEFILE_SOURCE
-D_FILE_OFFSET_BITS=64
-D_GNU_SOURCE
-DNDEBUG                 ← silences assert_lt and other invariant checks
-DNO_SPINLOCK
-DWITH_QUEUELOCK=1
-DBOWTIE2_VERSION="2.5.5"
-DBUILD_HOST="embedded"
-DBUILD_TIME="embedded"
-DCOMPILER_VERSION="embedded"
-DCOMPILER_OPTIONS="embedded"
-DBT2_NO_MAIN
-DBT2_BUILD_NO_MAIN
```

On aarch64: `-fopenmp-simd` (if supported).
On x86_64: `-msse2 -DPOPCNT_CAPABILITY`.

Optimization: `-O3` (cc crate `opt_level(3)`).

Additional flags: `-funroll-loops`, warnings disabled.

**Notably: `-DNDEBUG` is defined and `-DBOWTIE_MM` is defined.** Your
hypothesis #2 (NDEBUG mismatch) is confirmed — we define NDEBUG, which
silences `assert_lt(cur_, bufs_.size())` in `nextBatchImpl`. If the
CMake reproducer does NOT define NDEBUG (e.g., `RelWithDebInfo` keeps
asserts on), the assert would trap before the NULL deref, explaining
why the standalone reproducer passes.

`BOWTIE_MM` is also potentially relevant — it enables memory-mapped
shared-memory paths that change how worker-thread buffers are
initialized. The standalone reproducer may not define this.

## 3. Crash-site variable dump

Updated the CI lldb step to collect registers and memory at the crash
site. Will be available in the next CI run. The lldb commands added:

```
frame select 0
frame variable --show-globals --show-types
register read x0 x1 x19 x20
memory read -s 8 -fx -c 8 $x19
```

## 4. Does the crash reproduce in isolation?

From CI run 24592719766 (--test-threads=1), the crash occurred on the
`test_bowtie2_align_single_end_fastq_roundtrip` test running alone via
`--test-threads=1 --exact`. The four tests that ran before it
(`bad_input_shm`, `empty_input`, `invalid_index_path`,
`missing_index_path`) do NOT call `bt2_align_run` — they fail during
config validation or shm open before reaching the C library.

**The crash reproduces in isolation.** State contamination from prior
tests is ruled out.

## 5. Adapter source

The adapter is at `src/tools/bowtie2_align.rs` in
`the-miint/GPL-boundary` (branch `add-bowtie2-align`).

The input marshaling path (execute, lines 1393-1510):
1. Read Arrow IPC from shm → `Bt2InputData` (read_ids, seqs1, quals1,
   optional seqs2/quals2)
2. Convert each string to `CString`, build `Vec<*const c_char>` pointer
   arrays
3. Populate `Bt2Input` struct: `bt2_input_init(&mut bt2_input)`, then
   set `names`, `seqs`, `quals`, `n_reads` from the pointer arrays
4. Call `bt2_align_create(&bt2_config, &mut error_code)` — succeeds
5. Call `bt2_align_run(ctx, &bt2_input, &mut output, &mut stats)` — SIGSEGV

The CString vecs and pointer vecs are all live on the stack for the
duration of the `bt2_align_run` call. No dangling pointers.

The streaming path (`run_batch`, lines 578-745) follows the same
marshaling pattern but uses a pre-created context.

## Suggested next step

Try removing `-DNDEBUG` from `build.rs` and rebuild on macOS aarch64.
If an assert fires before the SIGSEGV, it will identify the exact
invariant violation. This is the fastest path to root cause.

Also try removing `-DBOWTIE_MM` — if the crash goes away, the
memory-mapped shared-memory worker-thread setup is the culprit.
