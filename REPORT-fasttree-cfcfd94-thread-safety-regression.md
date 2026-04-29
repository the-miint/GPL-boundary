# Follow-up to FastTree submodule team: cfcfd94 fixes the divergence but breaks per-context thread safety

**From**: gpl-boundary
**Date**: 2026-04-28
**Scope**: `ext/fasttree` at `cfcfd94` ("Fix library API divergence:
defer SetMLRates to first ML NNI iteration")
**Companion to**: `REPORT-fasttree-library-cli-divergence.md`

## Summary

`cfcfd94` resolves the library/CLI divergence reported in
`REPORT-fasttree-library-cli-divergence.md` — the library API now
produces the bit-equal Newick that the CLI does on our 50-sequence
16S subset.

However, the fix introduces a **thread-safety regression**: running
multiple `fasttree_build_soa` calls concurrently (each with its own
`fasttree_ctx_t`) reliably SIGSEGVs at `cfcfd94`, while the parent
commit `139e801` runs the same parallel workload without crashing.

This contradicts the explicit thread-safety contract documented in
`fasttree.h:13-15`:

> Thread safety: Each fasttree_ctx_t is independent. It is safe to
> use different contexts concurrently from different threads. A single
> context must not be used from multiple threads simultaneously.

gpl-boundary relies on this contract: its per-fingerprint streaming
worker pool spawns concurrent in-process workers that each construct
and drive their own `fasttree_ctx_t`. With the regression, we cannot
upgrade the submodule pin to `cfcfd94` without either serializing all
FastTree work behind a process-wide lock (defeating the per-fingerprint
concurrency) or staying on the known-divergent `139e801`.

## Evidence

All measurements taken on Linux x86_64, gcc 14, against the gpl-boundary
test suite at `/home/dtmcdonald/dev/GPL-boundary` HEAD `b29eeb8`. No
gpl-boundary code change between the two submodule SHAs — only the
submodule pointer was bumped.

### `cfcfd94` (post-fix), parallel cargo test

```
$ cargo test --bin gpl-boundary tools::fasttree::tests::
running 7 tests
test tools::fasttree::tests::test_config_struct_abi_size ... ok
test tools::fasttree::tests::test_fasttree_bad_input_shm ... ok
test tools::fasttree::tests::test_soa_to_record_batch_sentinel_translation ... ok
test tools::fasttree::tests::test_fasttree_nucleotide_roundtrip ... ok
test tools::fasttree::tests::test_fasttree_protein_roundtrip ... ok
error: test failed
Caused by:
  process didn't exit successfully: ... (signal: 11, SIGSEGV: invalid memory reference)
```

### `cfcfd94`, serial cargo test (`--test-threads=1`)

```
running 7 tests
test tools::fasttree::tests::test_config_struct_abi_size ... ok
test tools::fasttree::tests::test_fasttree_16s_50seq_smoke ... ok
test tools::fasttree::tests::test_fasttree_bad_input_shm ... ok
test tools::fasttree::tests::test_fasttree_nucleotide_roundtrip ... ok
test tools::fasttree::tests::test_fasttree_protein_roundtrip ... ok
test tools::fasttree::tests::test_soa_to_newick_via_c_smoke ... ok
test tools::fasttree::tests::test_soa_to_record_batch_sentinel_translation ... ok
test result: ok. 7 passed; 0 failed
```

### `139e801` (parent commit, pre-fix), parallel cargo test

```
running 7 tests
test tools::fasttree::tests::test_config_struct_abi_size ... ok
test tools::fasttree::tests::test_fasttree_bad_input_shm ... ok
test tools::fasttree::tests::test_soa_to_record_batch_sentinel_translation ... ok
test tools::fasttree::tests::test_fasttree_nucleotide_roundtrip ... ok
test tools::fasttree::tests::test_fasttree_protein_roundtrip ... ok
test tools::fasttree::tests::test_soa_to_newick_via_c_smoke ... ok
test tools::fasttree::tests::test_fasttree_16s_50seq_smoke ... ok
test result: ok. 7 passed; 0 failed
```

Same test set, same gpl-boundary code, same compile flags. Only the
submodule SHA changed.

### Process-level concurrency works

Five concurrent invocations of a pure-C driver (each in its own
process) against the `cfcfd94` `libfasttree.a` all complete cleanly
and produce bit-equal Newick:

```
$ for i in 1 2 3 4 5; do ./lib_driver /tmp/16S.1.50seq.fasta > /tmp/par.$i.nwk & done; wait
run 1: out=1807b err=0b
run 2: out=1807b err=0b
run 3: out=1807b err=0b
run 4: out=1807b err=0b
run 5: out=1807b err=0b
```

So the regression is specifically **shared-address-space (thread)**
concurrency, not process-level.

## Hypothesis

The fix removed the pre-loop `SetMLGtr` / `SetMLRates` calls in
`fasttree_api.c::_run_algorithm`. The in-loop call sites in
`fasttree_core.c` (the existing CLI path) are now reached for the
first time from the library API.

If `SetMLRates` or `SetMLGtr` (or anything they call) touches state
outside the `fasttree_ctx_t` — global variables, file-scope static
buffers, lazily-initialized singletons — concurrent contexts will
race. The CLI never exercised concurrent invocation (one process =
one tree), so any non-ctx-scoped state in those paths was latent.

## Suggested investigation

1. **Add a thread-safety regression test** to the `make test` target.
   A minimal pthreads driver that spawns N threads, each running
   `fasttree_build_soa` against its own context on the same input,
   and asserts each thread's Newick matches the existing
   `testdata/ground_truth/16S.1.nwk`. The same test against
   `139e801` should pass; against `cfcfd94` it should reproduce the
   SIGSEGV.

2. **Audit `SetMLRates` and `SetMLGtr` in `fasttree_core.c`** for any
   non-`ctx`-scoped state — globals, static buffers, lazily
   initialized resources. Anything touched there that isn't behind a
   per-`ctx` field is a candidate.

3. **Audit any helpers called from the in-loop path** that the
   pre-loop call site bypassed. The fix made the in-loop code path
   reachable from the library API for the first time; whatever was
   safe in the CLI's single-process-single-context world but unsafe
   under concurrent contexts will now surface.

## gpl-boundary impact

We are blocked on upgrading the submodule pin to `cfcfd94`:

- Per-fingerprint streaming workers in our coordinator
  (`src/registry.rs`, `src/workers.rs`) run concurrently in-process.
  Each worker builds its own `fasttree_ctx_t`; they execute
  simultaneously when distinct `(tool, config)` fingerprints arrive.
  This is exactly the workload that crashes at `cfcfd94`.
- We are staying on `139e801` (with the known library/CLI divergence)
  until thread safety is restored.
- Per-knob byte-equal parity tests against native FastTree (Phase 1
  of our config-surface expansion) remain blocked, since the only
  versions that match upstream byte-for-byte (`cfcfd94`+) are
  unsafe for our concurrency model.

## What we will do in the meantime

- Stay on `139e801` so the worker pool keeps working.
- Continue gpl-boundary phases that don't require parity (Phase 2:
  threads / OpenMP enablement; Phase 4: `describe_version`; Phase 5:
  documentation), which assert structural invariants rather than
  byte-equal Newick.
- Re-pin to a future submodule SHA that preserves both the parity
  fix from `cfcfd94` and the thread-safety property from `139e801`.

## Compile-flag context

Same as the original divergence report:

- `gcc -O3 -finline-functions -funroll-loops -DUSE_DOUBLE -fvisibility=hidden`
- No `-fopenmp`, no `-march=native`, no `-fopen-simd`
- `gpl-boundary/build.rs` defines `FASTTREE_NO_MAIN` for
  `fasttree_core.c` only
