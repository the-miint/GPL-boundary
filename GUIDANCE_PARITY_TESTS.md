# Native-FastTree parity testing

This file documents how the FastTree adapter's parity tests are kept honest
against native FastTree, and how to regenerate the hardcoded expected
outputs when the FastTree submodule is updated or new config knobs are
wired.

## Why this exists

The FastTree adapter exposes ~21 config knobs that map to fields in
`fasttree_config_t`. Each knob's parity test asserts that the library
adapter (`gpl-boundary` linked against `ext/fasttree`) produces the **same
Newick string** as native FastTree given the same input alignment, seed,
and parameter values.

Native FastTree is **not** invoked from inside the test suite. Instead:

1. We run native FastTree once, manually, in the `fasttree` conda
   environment.
2. We capture its Newick output.
3. We hardcode that output as a `const &str` in the corresponding parity
   test.
4. The test invokes the library adapter, calls the test-only FFI binding
   `fasttree_tree_soa_to_newick` to canonically serialize the SOA result,
   and asserts byte-equal against the hardcoded constant.

This keeps the test suite self-contained (no conda dependency in CI) while
giving a hard parity guarantee per knob.

## Determinism prerequisites

Single-threaded FastTree is deterministic given a fixed seed. All parity
tests in this repo run with `threads=1`. Multi-thread tests assert
statistical/topology invariants only.

**Use the non-OpenMP `FastTree` binary.** Phase 2 enabled OpenMP in
`build.rs` (`-DOPENMP -fopenmp`); the FastTree submodule at SHA
`2a6c14b` gates the `MLQuartetNNI` star-topology override on
`omp_get_max_threads() == 1` at runtime, so OpenMP and non-OpenMP
builds produce bit-equal trees at one thread. Bioconda's
`fasttree=2.2.0` `FastTree` (no OpenMP) is the canonical reference.
Bioconda's `FastTreeMP` is **not** suitable as ground truth: it
predates the `2a6c14b` fix and still produces divergent trees at
`OMP_NUM_THREADS=1`.

## Regenerating expected Newicks

### One-time setup

```bash
conda activate fasttree
which FastTree   # confirm bioconda's FastTree is on $PATH
FastTree 2>&1 | head -1  # should print "Double precision" (no "OpenMP")
```

### Extract the canonical fixture

The 50-sequence prefix of `ext/fasttree/16S_500.tar.gz`'s `16S.1.p` is the
default fixture. Tests extract this lazily under `target/`:

```bash
mkdir -p target/fasttree-testdata
tar -xzf ext/fasttree/16S_500.tar.gz -C target/fasttree-testdata \
    --strip-components=1 16S500/16S.1.p
```

To produce a 50-sequence subset for native FastTree (which expects PHYLIP
input), use the helper script (TODO Phase 1: add `scripts/subset-phylip.py`)
or extract programmatically with the parser in `src/test_util.rs`.

### Per-knob regeneration

For each parity test, the workflow is:

1. Compute the `seed`, knob values, and fixture used by the test.
2. Run `FastTree` with the equivalent CLI flags on the same input.
   Per-test regen commands live in the doc comment above each `_parity`
   test (the canonical, executable form).
3. Replace the test's `EXPECTED` constant with the fresh output.

Common shape:

```bash
conda run -n fasttree FastTree -nt -seed 12345 \
    [knob-flags] target/scratch/16S.1.50seq.fasta > target/scratch/expected.nwk
```

When in doubt, check the test's surrounding doc comment — it names the
exact invocation used to generate its expected output.

## When to regenerate

- Any time `ext/fasttree` is bumped to a new submodule SHA.
- Any time a knob's default value changes (intentionally).
- Never to mask a test failure caused by an adapter bug — investigate
  first; only regenerate if native FastTree's behavior also changed.
