# Report to FastTree submodule (the-miint/fasttree, branch v2.3.0-miint): library API path produces different trees than CLI path

**From**: gpl-boundary
**Date**: 2026-04-28
**Scope**: `ext/fasttree`'s library API (`fasttree_api.c` + `fasttree_core.c`)
vs the original CLI (`FastTree.c`)
**Submodule SHA**: as committed in
`/home/dtmcdonald/dev/GPL-boundary` at the time of writing
(`fasttree.h` defines `FASTTREE_VERSION_STRING "2.3.0"`)

## Summary

Given the same input alignment, the same compile flags, and the same
algorithmic defaults, the FastTree library API path and the FastTree CLI
path inside this submodule produce **different trees** — same leaf set,
slightly different topology (one leaf rearrangement), and different
branch lengths and support values throughout.

The submodule's own regression test (`make test`) compares only the CLI
path (`FastTree.orig` built from `FastTree.c`) against
`testdata/ground_truth/*.nwk`. The library path is not exercised by any
ground-truth comparison, so this divergence has gone undetected.

The CLI path produces output that is bit-equal to bioconda's
`FastTree 2.2.0`. The library path does not.

## Why this matters

Downstream consumers of the library (e.g., gpl-boundary, which links
`libfasttree.a` into a Rust binary used by the
`miint` DuckDB extension) expect the library API to be a drop-in
replacement for the CLI. They reasonably assume that

```c
fasttree_config_init(&cfg);
cfg.seq_type = FASTTREE_SEQ_NUCLEOTIDE;
cfg.seed = 12345;
fasttree_build_soa(ctx, names, seqs, n, n_pos, &tree, &stats);
```

is equivalent to

```sh
FastTree -nt -seed 12345 < input.fasta
```

It is not equivalent today. This blocks gpl-boundary's per-knob parity
tests against native FastTree, which we want as the trusted external
reference for our own adapter-level regression suite.

## Reproduction

Tested on Linux x86_64, gcc 14, in a clean checkout of
`https://github.com/morgannprice/fasttree` for the upstream side, and
the current submodule state for the library side.

### Step 1: Prepare a 50-sequence FASTA fixture

```bash
# In gpl-boundary checkout
mkdir -p target/fasttree-testdata
tar -xzf ext/fasttree/16S_500.tar.gz -C target/fasttree-testdata \
    --strip-components=1 16S500/16S.1.p
python3 scripts/subset-phylip-to-fasta.py 50 \
    < target/fasttree-testdata/16S.1.p > /tmp/16S.1.50seq.fasta
```

`scripts/subset-phylip-to-fasta.py` is in the gpl-boundary repo. It
strips inter-block whitespace, uppercases, and writes standard
multi-line FASTA. Each output sequence is exactly 1287 characters
(matching the PHYLIP header `500 1287`).

### Step 2: Build upstream `FastTree` with our flags

```bash
mkdir -p /tmp/upstream-fasttree-scratch
cd /tmp/upstream-fasttree-scratch
git clone --depth 1 https://github.com/morgannprice/fasttree.git
cd fasttree
gcc -O3 -finline-functions -funroll-loops -DUSE_DOUBLE \
    -fvisibility=hidden -o FastTree-ours FastTree.c -lm
./FastTree-ours -nt -seed 12345 < /tmp/16S.1.50seq.fasta > /tmp/upstream50.nwk
```

`FT_VERSION` in upstream `FastTree.c` is `"2.2.0"`. The flags above
match `gpl-boundary/build.rs`'s `build_fasttree()` exactly (no OpenMP).

### Step 3: Build the submodule library and a tiny C driver

```bash
cd ext/fasttree
make libfasttree.a
```

Save the following as `/tmp/upstream-fasttree-scratch/lib_driver.c`:

```c
#include "fasttree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    FILE *fp = fopen(argv[1], "r");
    char **names = NULL, **seqs = NULL;
    int n = 0, cap = 0;
    char line[16384];
    char *current = NULL;
    size_t curlen = 0, curcap = 0;
    while (fgets(line, sizeof(line), fp)) {
        size_t L = strlen(line);
        while (L && (line[L-1] == '\n' || line[L-1] == '\r')) line[--L] = 0;
        if (line[0] == '>') {
            if (n == cap) {
                cap = cap ? cap*2 : 16;
                names = realloc(names, cap*sizeof(char*));
                seqs = realloc(seqs, cap*sizeof(char*));
            }
            names[n] = strdup(line + 1);
            current = NULL; curlen = 0; curcap = 0;
            seqs[n] = NULL;
            n++;
        } else {
            if (curlen + L + 1 > curcap) {
                curcap = (curlen + L + 1) * 2;
                current = realloc(current, curcap);
                seqs[n-1] = current;
            }
            memcpy(current + curlen, line, L);
            curlen += L;
            current[curlen] = 0;
        }
    }
    fclose(fp);

    int n_pos = strlen(seqs[0]);
    fasttree_config_t cfg;
    fasttree_config_init(&cfg);
    cfg.seq_type = FASTTREE_SEQ_NUCLEOTIDE;
    cfg.seed = 12345;
    fasttree_ctx_t *ctx = fasttree_create(&cfg);
    fasttree_tree_soa_t *tree = NULL;
    fasttree_stats_t stats;
    int rc = fasttree_build_soa(ctx, (const char**)names, (const char**)seqs,
                                n, n_pos, &tree, &stats);
    if (rc != 0) {
        fprintf(stderr, "build failed: %s\n", fasttree_last_error(ctx));
        return 4;
    }
    char *newick = fasttree_tree_soa_to_newick(tree, 1);
    fputs(newick, stdout);
    free(newick);
    fasttree_tree_soa_free(tree);
    fasttree_destroy(ctx);
    return 0;
}
```

Compile and run:

```bash
gcc -O3 -finline-functions -funroll-loops -DUSE_DOUBLE \
    -fvisibility=hidden \
    -I.../GPL-boundary/ext/fasttree \
    -o lib_driver lib_driver.c \
    .../GPL-boundary/ext/fasttree/libfasttree.a -lm
./lib_driver /tmp/16S.1.50seq.fasta > /tmp/lib_via_c.nwk
```

### Step 4: Compare

```bash
diff /tmp/upstream50.nwk /tmp/lib_via_c.nwk
```

The diff is non-empty. Outputs are 1807 bytes each.

We additionally verified:

- `bioconda fasttree=2.2.0`'s `FastTree -nt -seed 12345` on the same
  FASTA produces output **bit-equal** to `/tmp/upstream50.nwk` (so the
  upstream-with-our-flags build matches conda).
- gpl-boundary's Rust adapter calling `fasttree_build_soa` produces
  output **bit-equal** to `/tmp/lib_via_c.nwk` (so the divergence is
  not in our Rust glue).

In other words:

| Build | Output |
|---|---|
| Conda `FastTree 2.2.0` | A |
| Upstream `FastTree.c` compiled with our flags | A (bit-equal to conda) |
| Submodule library (`libfasttree.a`) via C driver | B (≠ A) |
| Submodule library via gpl-boundary Rust adapter | B (bit-equal to C driver) |

## What differs

Both outputs have:

- The same 50 leaves
- Approximately the same general topology
- Same n_leaves, same n_internal_nodes

They differ in:

1. **Every branch length**, in the last few significant digits. E.g.,
   conda emits `(10607:0.108728183,1828:0.122617756)` where the
   library emits `(10607:0.108692627,1828:0.122857439)`.
2. **Every support value**, in the first decimal. E.g., conda emits
   `0.968` for the first internal split where the library emits
   `0.977`.
3. **One topology rearrangement** — leaf `114317`:
   - **CLI / conda**: sister to `((100492,104661),...)` *inside* the
     right subclade; `(103543,(78515,114176))` is sister to that
     whole structure.
   - **Library**: sister to the entire
     `((103543,(78515,114176)),((100492,104661),...))` clade.

The single topology rearrangement is consistent with a tied-score NNI
or SPR move resolved differently. Once one decision diverges, all
downstream branch-length and support computations diverge.

## Hypothesis

The divergence is intra-submodule (`fasttree_api.c + fasttree_core.c`
vs `FastTree.c`), not compile-flag-dependent (we verified by building
upstream with the same flags). The most likely sources are init-order
or default-state differences between
`main()` in `FastTree.c` and the API path
`fasttree_create()` → `fasttree_build_soa()` → `_run_algorithm()`:

1. **RNG init timing**. `_run_algorithm()` calls `ran_start(ft_ctx,
   (long)ft_ctx->seed)` after `_build_setup()`. The CLI may call
   `ran_start` at a different point relative to other initialization
   (e.g., before vs after alignment auto-detection or
   profile-buffer allocation), so the random sequence consumed at the
   first NNI/SPR scoring loop is different.
2. **`fasttree_ctx_init()` vs `main()` defaults**. We checked the
   obvious config fields (`nni_rounds=-1`, `spr_rounds=2`,
   `ml_nni_rounds=-1`, `n_rate_cats=20`, `n_bootstrap=1000`,
   `tophitsMult=1.0`, `tophitsClose=-1.0`, `tophitsRefresh=0.8`,
   `pseudoWeight=0.0`, `mlAccuracy=1`, `fastest=0`, `slow=0`,
   `bionj=0`) — they match between `fasttree_ctx_init()` and the
   `main()` initial values in `FastTree.c`. But there are several
   `ctx` fields not exposed in `fasttree_config_t` and not defaulted
   visibly in `_run_algorithm`:
   - `c->verbose = 1`
   - `c->showProgress = 1`
   - `c->useTopHits2nd = false`
   - `c->topvisibleMult = 1.5`
   - `c->staleOutLimit = 0.01`
   - `c->fResetOutProfile = 0.02`
   - `c->nResetOutProfile = 200`
   - `c->MEMinDelta = 1.0e-4`
   - `c->fastNNI = true`
   - `c->bGammaLogLk = false`
   - `c->closeLogLkLimit = 5.0`
   - `c->treeLogLkDelta = 0.1`
   - `c->exactML = true`
   - `c->approxMLminf = 0.95`
   - `c->approxMLminratio = 2/3.0`
   - `c->approxMLnearT = 0.2`

   If `main()` in `FastTree.c` mutates any of these globals based on
   command-line parsing or its own internal logic before calling into
   the algorithm, and the API path inherits the
   `fasttree_ctx_init()` default unchanged, the algorithm sees a
   different state.
3. **Alignment input handling**. The CLI calls `ReadAlignment(stdin,
   ...)` which parses FASTA/PHYLIP. The API receives `const char**`
   arrays directly. We confirmed identical sequence bytes go in
   (uppercased, gap-preserved, exactly 1287 chars per sequence, same
   order). But internal post-read processing
   (e.g., dedup, profile construction, hash-based ordering of
   identical sequences) might use a different code path between
   `ReadAlignment()` and the API entry, even if the resulting
   alignment data is the same.
4. **`useMatrix` for nucleotide**. `fasttree_create()` sets
   `ctx->useMatrix = false` for `FASTTREE_SEQ_NUCLEOTIDE`. CLI's
   `main()` may default `useMatrix` differently when `-nt` is parsed
   — worth double-checking whether `useMatrix=false` on the API path
   matches the CLI's behavior with `-nt -nomatrix` (likely the
   intended equivalent) vs `-nt` alone.

## Suggested investigation

1. **Add a regression test** to `Makefile`'s `test` target that
   compiles `test_api` (or a similar driver) against
   `libfasttree.a`, runs `fasttree_build_soa` with default config +
   seed 12345 on the existing `testdata/16S500/16S.1.p` fixture, and
   diffs against `testdata/ground_truth/16S.1.nwk`. Today this
   regression passes silently because the CLI path is the only one
   tested. Once it catches the divergence on every commit, fixing it
   becomes tractable.
2. **Bisect**. The Makefile already has the `FastTree.orig` target
   for upstream `FastTree.c`, and `test_api` for the library. A
   test that compares their outputs on the bundled
   `testdata/16S500/*.p` files at default settings would lock down
   parity. Once that test is failing, the bisect-and-fix loop is
   internal to the submodule.
3. **Audit `fasttree_create()` against `main()` in `FastTree.c`** for
   the `ctx` fields listed above. Any field that `main()` mutates
   from its `ctx_init` default (based on default behavior or implicit
   `-nt` handling) must be either copied through `fasttree_config_t`
   or hard-coded to the same value in the API path.
4. **Audit RNG init order**. Make sure
   `ran_start(ctx, seed)` is called at the same point in the
   pipeline (relative to alignment loading, profile building, and the
   first random-consuming function) on both paths.

## What gpl-boundary will do in the meantime

Until library-vs-CLI parity is restored:

- We will use the **library's own output** as the ground truth for
  our adapter regression tests (capture once, hardcode the expected
  Newick, detect drift). This catches regressions in our adapter
  but does **not** validate the library against upstream FastTree
  behavior.
- We will record this divergence in `CLAUDE.md` and our memory
  system so future maintainers know not to expect bit-equality with
  conda/upstream.
- Once the submodule confirms parity is restored, we will switch our
  parity-test ground truth to native FastTree (regenerated in the
  `fasttree` conda env).

## Compile-flag specifics for reproducibility

Both `FastTree-ours` and `libfasttree.a` were built with:

- `gcc -O3 -finline-functions -funroll-loops -DUSE_DOUBLE -fvisibility=hidden`
- No `-fopenmp`, no `-march=native`, no `-fopen-simd`
- `gpl-boundary/build.rs` adds `.define("FASTTREE_NO_MAIN", None)` for
  `fasttree_core.c` only (not for `fasttree_api.c`); the same `-DUSE_DOUBLE`
  applies to both. The `FastTree-ours` build uses upstream `FastTree.c`
  unmodified (no `FASTTREE_NO_MAIN`).

If the divergence requires a specific compiler version to reproduce,
the Linux x86_64 environment used was:

- gcc 14.x
- glibc 2.x
- Linux 6.12 kernel
