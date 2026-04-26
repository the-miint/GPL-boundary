# GPL-boundary

## Project overview

GPL-boundary provides a legally appropriate process-isolation boundary between
GPL-licensed bioinformatics tools and the BSD-licensed miint DuckDB extension.
GPL code runs in a separate Rust binary; miint (C++, at `../duckdb-miint`)
spawns it as a long-lived daemon (one per DuckDB instance).

## Architecture

- **Control plane**: JSON over stdin/stdout
- **Data exchange**: Apache Arrow IPC over POSIX shared memory (zero-copy)
- **GPL-boundary owns data transformation**: receives Arrow columnar data from
  miint, reshapes into tool-specific format, runs the tool, reshapes results
  back into Arrow columnar format
- **GPL-boundary creates output shm**: the caller only provides `shm_input`;
  gpl-boundary creates its own output shm and returns the name + size in the
  JSON response
- **Platform-abstracted shared memory**: uses `shm_open()` on both Linux and
  macOS (not `/dev/shm` file paths, which are Linux-only)
- **WASM is a future goal**, not currently targeted

**Batched streaming**: Tools with expensive context creation (bowtie2: index
loading, sortmerna: reference indexing, prodigal: metagenomic bin
initialization) support batched streaming. The `StreamingContext` trait
provides `run_batch()` for per-batch work against a pre-loaded context.
Single-genome prodigal does NOT support streaming (requires all sequences for
training).

**Per-fingerprint dispatch**: every batch carries its own `(tool, config)`
pair. The coordinator computes a canonical-config fingerprint and routes to
a per-fingerprint worker. Distinct fingerprints execute concurrently;
same-fingerprint batches serialize through one worker (preserving
per-fingerprint order).

- `bowtie2-align` → **subprocess workers** (`gpl-boundary --worker
  bowtie2-align`). Each fingerprint is its own child process, sidestepping
  bowtie2's process-wide alignment mutex.
- everything else → **in-process workers** with a dedicated worker thread
  per fingerprint.

**Eviction (sweeper thread, default 100ms tick)**:
- Idle deadline: workers past `worker_idle_ms` AND with `in_flight == 0` are
  evicted. Required for bowtie2 since loaded indexes are hundreds of MB.
- Budget pressure: a new subprocess fingerprint over `max_workers` evicts
  the LRU idle subprocess; if all workers are in-flight, the new submit is
  queued and dispatched when a slot frees up.
- In-flight batches are protected from eviction.

**Crash handling**: if a subprocess child exits unexpectedly (segfault,
OOM kill), the parent's reader thread surfaces an error response for the
in-flight batch, marks the fingerprint dead so subsequent same-fingerprint
batches fail fast, and best-effort sweeps any orphan
`/dev/shm/gb-{child_pid}-*` segments. Distinct fingerprints are unaffected;
no auto-respawn — restart gpl-boundary to recover the dead fingerprint.

## Shared memory lifecycle (critical)

This is the most important part of the architecture to understand.

### Normal flow

1. **miint** creates input shm, writes Arrow IPC data
2. **miint** spawns `gpl-boundary`, writes JSON request to stdin (includes
   `shm_input` name)
3. **gpl-boundary** reads input from shm, runs tool, creates output shm(s) with
   PID-based names (`/gb-{pid}-{n}-{label}`), writes Arrow IPC results
4. **gpl-boundary** responds via stdout with `shm_outputs` array (name, label,
   size for each output)
5. **miint** reads response, opens each output shm, reads `size` bytes of
   Arrow IPC
6. **miint** unlinks both input and all output shm (it owns cleanup of all)

### Signal handling

gpl-boundary installs signal handlers for SIGINT and SIGTERM that unlink any
output shm it has created. This prevents leaks on graceful termination.

**SIGKILL cannot be trapped.** The PID-based naming convention
(`/gb-{pid}-{n}-{label}`) lets miint detect and clean up stale segments
by checking if the PID is still alive.

### Cleanup registry

- `shm::register_for_cleanup(name)` -- called when output shm is created
  (inside `arrow_ipc::write_batch_to_output_shm`)
- `shm::deregister_cleanup(name)` -- called in `main.rs` **after** the JSON
  response has been written to stdout (not inside `execute()`)
- Signal handler and `cleanup_all()` iterate the registry and unlink everything

**Important**: deregister must happen after stdout write, not before. If a
signal arrives between deregister and stdout write, the shm leaks.

### SharedMemory::detach()

When output shm is ready for the caller to read, we call `detach()` which
munmaps without unlinking. This is the proper API -- never use `mem::forget`.

## Submodules

GPL-licensed tools live as git submodules under `ext/`. Currently:

- `ext/fasttree` -- FastTree phylogenetic tree inference (GPL-2.0+, C99,
  branch `v2.3.0-miint`)
- `ext/prodigal` -- Prodigal prokaryotic gene prediction (GPL-3.0, C99,
  branch `v2.6.4-miint`)
- `ext/sortmerna` -- SortMeRNA rRNA filtering and sequence alignment
  (LGPL-3.0, C++17, branch `v4.4.0-miint`). Requires system-installed
  RocksDB and zlib. Build uses two `cc::Build` instances (C and C++17)
  plus pkg-config for RocksDB discovery.
- `ext/bowtie2` -- Bowtie2 short read aligner (GPL-3.0, C++11, branch
  `v2.5.5-miint`). Requires zlib. Uses global mutable state behind a mutex
  (only one alignment per process at a time; acceptable for single-invocation
  model). Build compiles ~54 .cpp files via `cc::Build` with `cpp(true)`.
  The combined library includes both aligner and builder APIs; the builder
  is used in tests to create index fixtures programmatically. Requires
  pre-built `.bt2` index files on disk (path passed via `index_path` config).

**We control the submodule APIs** (the-miint org maintains forks). If an API
does not fit our needs, we can modify it. However, changes require
documentation for the separate submodule development teams.

### Adding a new submodule

Each tool gets:
1. A git submodule under `ext/<toolname>`
2. A `build_<toolname>()` function in `build.rs` (separate `cc::Build` per tool
   to prevent symbol collisions)
3. An adapter in `src/tools/<toolname>.rs` implementing `GplTool`
4. Auto-registration via `inventory::submit!` in the tool module (no manual
   dispatch code needed)
5. `pub mod <toolname>;` in `src/tools/mod.rs`
6. Tool-specific API documentation (input/output schemas, config params) via
   the `describe()` method on `GplTool`
7. An ABI size-check test for each `#[repr(C)]` config struct
8. Smoke tests using real shared memory (use `test_util` helpers)
9. **(Optional) Streaming support** via `create_streaming_context()` on
   `GplTool`. Required if the tool has expensive context creation and processes
   independent records per batch. See `GUIDANCE_INTEGRATE.md` Step 6.

Use `arrow_ipc::write_batch_to_output_shm()` and
`arrow_ipc::read_batches_from_shm()` for Arrow IPC marshaling -- do not
duplicate this logic in tool modules.

We do not exhaustively test submodules; that is their own CI's job.

## Build

```bash
cargo build          # builds fasttree C lib via cc crate + links it
cargo test           # unit + integration tests (uses real POSIX shared memory)
make check           # fmt + clippy + test
```

The `build.rs` compiles C/C++ sources from submodules using the `cc` crate.
SortMeRNA requires system-installed RocksDB and zlib (`librocksdb-dev` and
`libz-dev` on Debian/Ubuntu, or `brew install rocksdb zlib` on macOS).
RocksDB is discovered via `pkg-config`.

## Code organization

```
src/
  main.rs            # Entry point: CLI flags, stdin JSON dispatch, shm deregister
  arrow_ipc.rs       # Shared Arrow IPC helpers (write_batch_to_output_shm, read_batches_from_shm)
  protocol.rs        # Request/Response/ShmOutput JSON types (serde)
  shm.rs             # POSIX shared memory, cleanup registry, signal handlers
  test_util.rs       # Shared test helpers (cfg(test) only)
  tools/
    mod.rs           # GplTool trait, ToolRegistration, StreamingContext trait, inventory-based dispatch
    fasttree.rs      # FastTree FFI bindings + GplTool impl + tests
    prodigal.rs      # Prodigal FFI bindings + GplTool impl + tests
    sortmerna.rs     # SortMeRNA FFI bindings + GplTool impl + tests
    bowtie2_align.rs # Bowtie2 aligner FFI bindings + GplTool impl + tests
    bowtie2_build.rs # Bowtie2 index builder FFI bindings + GplTool impl + tests
ext/
  fasttree/          # git submodule (GPL-2.0+, C99)
  prodigal/          # git submodule (GPL-3.0, C99)
  sortmerna/         # git submodule (LGPL-3.0, C++17)
  bowtie2/           # git submodule (GPL-3.0, C++11)
tests/
  build_sanity.rs    # Integration tests: binary links and runs correctly
  bowtie2_build.rs   # Integration tests: bowtie2 index builder + round-trip with align
  streaming.rs       # Integration tests: NDJSON streaming protocol
build.rs             # Per-tool C compilation functions via cc crate
```

## CLI introspection

The binary supports introspection flags (no stdin required):

- `gpl-boundary --version` -- JSON with gpl-boundary version + all tool versions
- `gpl-boundary --list-tools` -- JSON array of available tool names
- `gpl-boundary --describe fasttree` -- JSON with config params, input/output
  Arrow schemas, response metadata fields

These let miint programmatically discover capabilities without hardcoding.

## Key conventions

- **Rust edition 2021**, standard cargo project
- **Tests use real shared memory**: smoke tests create POSIX shm segments,
  write Arrow IPC, run tools, read Arrow IPC output. No mocking.
- **FFI**: manual `#[repr(C)]` bindings in each tool module (not bindgen).
  Structs must match C headers exactly. Each config struct must have an ABI
  size-check test. Size checks catch added/removed fields but not reordering;
  manual review against the C header is still required when fields change.
- **Error handling**: tools return `Response::error(msg)`. Never panic across
  FFI.
- **Tool registration**: tools self-register via `inventory::submit!`. No
  manual dispatch match arms. Duplicate names are caught at startup.
- **CI**: GitHub Actions on Linux + macOS. Runs `cargo test`, `cargo clippy`,
  `cargo fmt --check`.
- **No Newick output**: fasttree returns SOA tree structure as Arrow columnar
  data, not Newick strings.
- **Arrow IPC stream format** (not file format) for shared memory exchange.
- **No bulk data in JSON**: sequences, trees, etc. always go through Arrow IPC
  in shared memory. JSON carries only tool name, parameters, shm paths, and
  lightweight result metadata.
- **Streaming tests**: streaming-capable tools must have multi-batch smoke tests
  verifying the context survives across `run_batch()` calls
- **StreamingContext Drop**: must call the C library's `destroy()` function

## Protocol

Daemon-only NDJSON over stdin/stdout. Every invocation is a session.
There is no single-shot mode.

```
1. {"init": {...}}                         ← required first line
   ←  {"success": true, "protocol_version": 1}
2. {"tool":"...","config":{...},"shm_input":"...","batch_id":N}   (≥0 times)
   ←  {"success": true, "schema_version": K, "batch_id": N,
        "shm_outputs": [...], "result": {...}}
3. {"shutdown": true}  OR  EOF  OR  idle_timeout_ms expires       ← exit 0
```

**Init message** (first line, required):
```json
{"init": {
  "idle_timeout_ms": 60000,
  "max_workers": 4,
  "workers_per_fingerprint": 1,
  "max_idle_workers": 4,
  "worker_idle_ms": 300000
}}
```
- `idle_timeout_ms` — auto-exit after this much stdin silence. Default
  60_000. Set to `0` to disable.
- `max_workers` — soft cap on resident subprocess workers (only
  bowtie2-align uses subprocesses today). Default 4.
- `workers_per_fingerprint` — per-fingerprint subprocess cap. Default
  1; bowtie2's process-wide global mutex makes >1 useless.
- `max_idle_workers` — idle subprocess workers retained after a burst.
  Default 4.
- `worker_idle_ms` — per-worker idle deadline. The sweeper evicts a
  worker when its `last_used + worker_idle_ms < now` AND it has no
  in-flight batch. Default 300_000.

**Init reply** carries the wire-protocol version:
```json
{"success": true, "protocol_version": 1}
```
- `protocol_version` is incremented on any breaking change to the wire
  envelope (init/batch/shutdown shape, response field set). Separate from
  per-tool `schema_version`.

**Batch request** (one per line, any number):
```json
{"tool": "fasttree",
 "config": {"seq_type": "nucleotide", "seed": 12345},
 "shm_input": "/miint-input-uuid",
 "batch_id": 42}
```
- `batch_id` is optional but recommended. Echoed verbatim on the matching
  response so out-of-order completions across distinct fingerprints can be
  correlated.
- **Multi-fingerprint dispatch:** every batch carries its own
  `(tool, config)` and is routed independently. Distinct fingerprints
  may complete out of order; correlate responses via `batch_id`.

**Batch response**:
```json
{"success": true,
 "schema_version": 2,
 "batch_id": 42,
 "shm_outputs": [{"name": "/gb-1234-0-tree", "label": "tree", "size": 8192}],
 "result": {"n_nodes": 7, "n_leaves": 4, "root": 6}}
```
- `schema_version` — per-tool (FastTree=2, Prodigal=1, SortMeRNA=2,
  Bowtie2=1, Bowtie2-build=1 as of writing); bumped on breaking
  output-schema changes.
- `shm_outputs` — omitted when empty. Labels match `[a-z0-9-]+`.
- `result` — lightweight metadata; bulk data always travels in
  `shm_outputs`.

**Shutdown** (graceful):
```json
{"shutdown": true}
```
Or close stdin (EOF), or wait `idle_timeout_ms`. All three result in a
clean exit code 0; in-flight batches complete before exit.

## Arrow schemas

**FastTree input** (written by miint to shm_input):
- `name: Utf8` -- sequence identifier
- `sequence: Utf8` -- aligned sequence (all must be equal length)

**FastTree output** (written by gpl-boundary to shm_outputs, label "tree"; schema_version=2):
- `node_index: Int64` -- node index [0, n_nodes)
- `parent_index: Int64` (nullable) -- parent node's `node_index`; NULL for the root
- `edge_id: Int64` (nullable) -- inbound-edge join key (= `node_index` for non-root); NULL for the root. miint joins downstream tree-edge tables on this column.
- `branch_length: Float64` (nullable) -- NULL when the C library emits NaN
- `support: Float64` (nullable) -- SH-like local support [0, 1]; NULL when not computed (replaces the prior -1 sentinel)
- `n_children: Int32` -- 0 for tips
- `is_tip: Boolean` -- whether node is a tip (renamed from `is_leaf`)
- `name: Utf8` (nullable) -- tip name, NULL for internal nodes

**Prodigal input** (written by miint to shm_input):
- `name: Utf8` -- contig identifier
- `sequence: Utf8` -- nucleotide sequence (different lengths allowed)

**Prodigal output** (written by gpl-boundary to shm_outputs, label "genes"):
- `seq_name: Utf8` -- source contig identifier
- `begin: Int32` -- gene start position (1-based)
- `end: Int32` -- gene end position (1-based)
- `strand: Int32` -- +1 forward, -1 reverse
- `partial_left: Boolean` -- gene is partial at left edge
- `partial_right: Boolean` -- gene is partial at right edge
- `start_type: Int32` -- 0=ATG, 1=GTG, 2=TTG, 3=Edge
- `cscore: Float64` -- coding score
- `sscore: Float64` -- start score
- `rscore: Float64` -- RBS score
- `uscore: Float64` -- upstream score
- `tscore: Float64` -- type score
- `confidence: Float64` -- gene confidence [50, 100]
- `gc_cont: Float64` -- per-gene GC content
- `rbs_motif: Utf8` -- ribosome binding site motif
- `rbs_spacer: Utf8` -- RBS spacer region

**SortMeRNA input** (written by miint to shm_input):
- `read_id: Utf8` -- sequence identifier
- `sequence: Utf8` -- forward read nucleotide sequence
- `sequence2: Utf8` (nullable) -- reverse read for paired-end; absence = single-end

**SortMeRNA output** (written by gpl-boundary to shm_outputs, label "alignments"; schema_version=2):
- `read_id: Utf8` -- read identifier (from input)
- `aligned: Int32` -- 1 if aligned, 0 otherwise
- `strand: Int32` (nullable) -- 1=forward, 0=reverse-complement; NULL if unaligned
- `ref_name: Utf8` (nullable) -- reference sequence ID; NULL if unaligned
- `ref_start: Int32` (nullable) -- 1-based start on reference; NULL if unaligned
- `ref_end: Int32` (nullable) -- 1-based end on reference; NULL if unaligned
- `cigar: Utf8` (nullable) -- CIGAR string; NULL if unaligned
- `score: Int32` (nullable) -- Smith-Waterman alignment score; NULL if unaligned
- `e_value: Float64` -- E-value of best alignment (0.0 when unaligned)
- `identity: Float64` -- percent identity (0-100; 0.0 when unaligned)
- `coverage: Float64` -- query coverage (0-100; 0.0 when unaligned)
- `edit_distance: Int32` (nullable) -- edit distance (mismatches + gaps); NULL if unaligned

Schema v2 (Phase 5) replaced the prior `-1` / `0` sentinels in the
alignment-only columns with NULL keyed off `aligned == 0`.

SortMeRNA requires `ref_paths` (reference FASTA file paths) in the config
JSON. Reference indexing is handled internally. Paired-end mode is inferred
from the presence of the `sequence2` column — sequences are interleaved as
`[fwd0, rev0, fwd1, rev1, ...]` for the C API.

**Bowtie2-align input** (written by miint to shm_input):
- `read_id: Utf8` -- read identifier
- `sequence1: Utf8` -- mate 1 (or unpaired) DNA sequence
- `sequence2: Utf8` (nullable) -- mate 2 sequence; absence or all-null = single-end
- `qual1: Utf8` (nullable) -- Phred+33 quality string; null = FASTA (default quality)
- `qual2: Utf8` (nullable) -- mate 2 quality string; null = FASTA

**Bowtie2-align output** (written by gpl-boundary to shm_outputs, label "alignments"):
- `read_id: Utf8` -- read name (QNAME)
- `flags: UInt16` -- SAM flags
- `reference: Utf8` -- reference name (RNAME); * if unmapped
- `position: Int64` -- 1-based leftmost position; 0 if unmapped
- `mapq: UInt8` -- mapping quality
- `cigar: Utf8` -- CIGAR string; * if unmapped
- `mate_reference: Utf8` -- mate reference name; * if unavailable
- `mate_position: Int64` -- mate position; 0 if unavailable
- `template_length: Int64` -- template length; 0 if unavailable
- `tag_as: Int32` (nullable) -- AS:i alignment score
- `tag_xs: Int32` (nullable) -- XS:i second-best score
- `tag_nm: Int32` (nullable) -- NM:i edit distance
- `tag_yt: Utf8` (nullable) -- YT:Z pairing type (UU/CP/DP/UP)
- `tag_md: Utf8` (nullable) -- MD:Z mismatch string

Bowtie2-align requires `index_path` (path to pre-built `.bt2` index files) in
the config JSON. The output does not include `seq` or `qual` columns (caller
already has the reads). Paired-end mode is inferred from the `sequence2`
column. The tool has ~30 config parameters covering scoring, seeding,
paired-end behavior, effort, and SAM output options — see `--describe
bowtie2-align` for the full list.

**Bowtie2-build input** (written by miint to shm_input):
- `name: Utf8` -- sequence identifier
- `sequence: Utf8` -- DNA sequence (different lengths allowed)

**Bowtie2-build output**: no Arrow output. The `result` JSON carries:
- `elapsed_ms: integer` -- wall-clock build time
- `n_sequences: integer` -- sequences in input
- `n_bases: integer` -- total bases in input
- `index_files: array<string>` -- absolute paths of `.bt2` (or `.bt2l`) files written

Bowtie2-build requires `index_path` (output basename for `.bt2` files) in the
config JSON. The tool materializes the input Arrow batches to a tempfile FASTA
and then invokes bowtie2's C builder API on disk paths — the in-memory build
path is not yet exposed by the bowtie2 C API. Caller must ensure the parent
directory of `index_path` exists. Config knobs: `nthreads`, `seed`, `offrate`,
`packed`, `verbose` — see `--describe bowtie2-build`.

## Arrow IPC write strategy

**Output (`write_batch_to_output_shm`)**: zero-copy via `arrow_ipc::ShmWriter`,
which `shm_open`s a fresh segment, sparsely reserves `GPL_BOUNDARY_MAX_SHM_BYTES`
virtual bytes (default 1 GiB), and `mmap`s once. The Arrow `StreamWriter` writes
directly into the mapping. On `finish()` the writer `munmap`s, `ftruncate`s the fd
down to the exact written byte count (releasing unused pages back to the kernel),
and closes. There is no intermediate `Vec<u8>` and no remap during writing. POSIX
shm objects are sparse on Linux and macOS, so the 1 GiB reservation is virtually
free for normal-sized outputs. If a tool produces an output larger than the cap,
`Write::write` returns `WriteZero` and the writer's `Drop` unlinks the partial
segment.

**Input (`read_batches_from_shm`)**: zero-copy via Arrow's
`Buffer::from_custom_allocation`. The `SharedMemory` mmap is wrapped as a
`Buffer` whose Arc-counted `Allocation` owner is the mapping itself. Arrow's
`StreamDecoder` slices that buffer for each batch body without copying. The
returned `RecordBatch`es reference the mmap directly; the mapping survives
until the last batch (and any column derived from it) is dropped, even after
`shm_unlink`. Verified by `arrow_ipc::tests::test_input_mmap_outlives_unlink`.

## C API contract for submodules

C libraries integrated as submodules must expose:

```c
void tool_config_init(tool_config_t *config);    // struct_size as first field
tool_ctx_t *tool_create(const tool_config_t *config);
void tool_destroy(tool_ctx_t *ctx);
int tool_run(tool_ctx_t *ctx, ...);              // returns error code
void tool_output_free(tool_output_t *output);
const char *tool_strerror(int code);
const char *tool_last_error(tool_ctx_t *ctx);
```

Requirements: no global state, no `main()` (use `TOOL_NO_MAIN` define guard),
no stdout/stderr (use log callback), no `exit()`/`abort()`, deterministic with
seed, SOA outputs preferred (maps cleanly to Arrow columns), ABI versioning
via `struct_size` as first field in config.

Tools intended for batched streaming should follow the create/run*/destroy
pattern where the context survives errors from `run()`. See
GUIDANCE_API_LIBRARY.md for details.

## Related projects

- **miint**: `../duckdb-miint` -- C++ DuckDB extension that spawns this binary
- **fasttree**: `ext/fasttree` -- GPL phylogenetic tree library
- **prodigal**: `ext/prodigal` -- GPL prokaryotic gene prediction library
- **sortmerna**: `ext/sortmerna` -- LGPL rRNA filtering and alignment library
