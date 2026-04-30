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

The 50-sequence prefix of `ext/fasttree/16S_500.tar.gz`'s `16S.1.p`
is the default fixture. The Rust test harness extracts the PHYLIP
file lazily under `target/fasttree-testdata/` once per process via
`OnceLock` (see `src/test_util.rs::extracted_16s_phylip`). For
manual regen, you also need a FASTA copy under `target/scratch/`:

```bash
mkdir -p target/fasttree-testdata target/scratch
tar -xzf ext/fasttree/16S_500.tar.gz -C target/fasttree-testdata \
    --strip-components=1 16S500/16S.1.p
python3 scripts/subset-phylip-to-fasta.py 50 \
    < target/fasttree-testdata/16S.1.p \
    > target/scratch/16S.1.50seq.fasta
python3 scripts/subset-phylip-to-fasta.py 100 \
    < target/fasttree-testdata/16S.1.p \
    > target/scratch/16S.1.100seq.fasta
```

`target/scratch/` is local-only scratch space and never committed.
50-seq is the default for parity tests (~1.8 KB Newicks, sub-second
runs); use 100-seq when the knob's effect doesn't show on 50 (`spr`
and `notop` are the precedents).

### Per-knob regeneration

For each parity test, the workflow is:

1. Compute the `seed`, knob values, and fixture used by the test.
2. Run `FastTree` with the equivalent CLI flags on the same input.
   The doc comment above each `_parity` test is the canonical,
   executable form — copy and run.
3. Replace the test's `EXPECTED` constant with the fresh output.
4. Run the test; commit if it passes.

Common shape:

```bash
conda run -n fasttree FastTree -nt -seed 12345 \
    [knob-flags] target/scratch/16S.1.50seq.fasta > target/scratch/expected.nwk
```

### All wired parity tests (as of Phase 3)

Each line below is the canonical regen invocation for one
`_parity`-suffixed test. The fixture column shows which fasta the
test feeds. Tests not listed here (e.g. `_sets_field`, `_rejected`)
are struct-level or conflict-rejection tests — they do not embed a
hardcoded Newick and therefore have no regen command.

| Test | Fixture | Regen flags |
|---|---|---|
| `test_fasttree_50seq_parity_baseline` | 50 | (none) |
| `test_fasttree_100seq_parity_baseline` | 100 | (none) |
| `test_bootstrap_100_parity` | 50 | `-boot 100` |
| `test_nosupport_parity` | 50 | `-nosupport` |
| `test_pseudo_default_weight_parity` | 50 | `-pseudo` |
| `test_pseudo_weight_2_5_parity` | 50 | `-pseudo 2.5` |
| `test_nni_0_parity` | 50 | `-nni 0 -spr 2` (literal-not-CLI) |
| `test_spr_0_parity_100seq` | 100 | `-spr 0` |
| `test_mlnni_0_parity` | 50 | `-mlnni 0` |
| `test_mlacc_3_parity` | 50 | `-mlacc 3` |
| `test_cat_5_parity` | 50 | `-cat 5` |
| `test_threads_1_parity` | 50 | (none — threads=1 = baseline) |
| `test_model_gtr_parity` | 50 | `-gtr` |
| `test_bionj_parity` | 50 | `-bionj` |
| `test_notop_parity_100seq` | 100 | `-notop` |
| `test_gamma_parity` | 50 | `-gamma` |

The CLI invocation is always `conda run -n fasttree FastTree -nt -seed 12345 [flags] target/scratch/16S.1.<size>seq.fasta`.

## When to regenerate

- Any time `ext/fasttree` is bumped to a new submodule SHA.
- Any time a knob's default value changes (intentionally).
- Never to mask a test failure caused by an adapter bug — investigate
  first; only regenerate if native FastTree's behavior also changed.
