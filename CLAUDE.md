# GPL-boundary

## Project overview

GPL-boundary provides a legally appropriate process-isolation boundary between
GPL-licensed bioinformatics tools and the BSD-licensed miint DuckDB extension.
GPL code runs in a separate Rust binary; miint (C++, at `../duckdb-miint`)
spawns it as a short-lived child process.

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
provides `run_batch()` for per-batch work against a pre-loaded context. The
protocol is lock-step NDJSON — at most 1 input + 1 output shm segment exists
at any time, naturally respecting DuckDB backpressure. Single-genome prodigal
does NOT support streaming (requires all sequences for training).

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
ext/
  fasttree/          # git submodule (GPL-2.0+, C99)
  prodigal/          # git submodule (GPL-3.0, C99)
  sortmerna/         # git submodule (LGPL-3.0, C++17)
  bowtie2/           # git submodule (GPL-3.0, C++11)
tests/
  build_sanity.rs    # Integration tests: binary links and runs correctly
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

Request (stdin JSON):
```json
{
  "tool": "fasttree",
  "config": { "seq_type": "nucleotide", "seed": 12345 },
  "shm_input": "/miint-input-uuid"
}
```

Response (stdout JSON):
```json
{
  "success": true,
  "schema_version": 1,
  "shm_outputs": [
    { "name": "/gb-1234-0-tree", "label": "tree", "size": 8192 }
  ],
  "result": { "n_nodes": 7, "n_leaves": 4, "root": 6, "stats": { ... } }
}
```

`schema_version` is an integer that is bumped on any breaking output schema
change (new/removed/renamed columns, type changes). It lets consumers fail
fast when boundary and extension versions drift. It is set by `dispatch()`
from the tool's `schema_version()` method and is only present on success.

Tools may produce multiple outputs (each a separate shm segment with a
distinct label). `shm_outputs` is omitted from JSON when empty (error
responses, metadata-only tools). Labels must be `[a-z0-9-]+`.

Streaming request (first line of NDJSON):
```json
{"tool": "prodigal", "config": {"meta_mode": true}, "shm_input": "/input-0", "stream": true}
```

Subsequent batch requests (one per line):
```json
{"shm_input": "/input-1"}
```

EOF on stdin terminates the session. Each batch gets its own response line on
stdout. Non-fatal batch errors return an error response; the session continues
until EOF or IO error.

## Arrow schemas

**FastTree input** (written by miint to shm_input):
- `name: Utf8` -- sequence identifier
- `sequence: Utf8` -- aligned sequence (all must be equal length)

**FastTree output** (written by gpl-boundary to shm_outputs, label "tree"):
- `id: Int32` -- node index [0, n_nodes)
- `parent: Int32` -- parent node index (-1 for root)
- `branch_length: Float64`
- `support: Float64` -- SH-like local support (-1 if not computed)
- `n_children: Int32` -- 0 for leaves
- `is_leaf: Boolean`
- `name: Utf8` (nullable) -- leaf name, null for internal nodes

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

**SortMeRNA output** (written by gpl-boundary to shm_outputs, label "alignments"):
- `read_id: Utf8` -- read identifier (from input)
- `aligned: Int32` -- 1 if aligned, 0 otherwise
- `strand: Int32` -- 1=forward, 0=reverse-complement, -1=unaligned
- `ref_name: Utf8` (nullable) -- reference sequence ID, null if unaligned
- `ref_start: Int32` -- 1-based start on reference, 0 if unaligned
- `ref_end: Int32` -- 1-based end on reference, 0 if unaligned
- `cigar: Utf8` (nullable) -- CIGAR string, null if unaligned
- `score: Int32` -- Smith-Waterman alignment score, -1 if unaligned
- `e_value: Float64` -- E-value of best alignment
- `identity: Float64` -- percent identity (0-100)
- `coverage: Float64` -- query coverage (0-100)
- `edit_distance: Int32` -- edit distance (mismatches + gaps), -1 if unaligned

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

## Arrow IPC write strategy

`write_batch_to_output_shm` serializes to `Vec<u8>` then copies into shm.
Alternatives were investigated and rejected:
- **Over-allocate + ftruncate**: unsafe with existing mmap (SIGBUS risk if
  accessing pages beyond truncated size)
- **Two-pass counting writer**: viable but marginal benefit, adds complexity
- **Temporary file**: strictly worse than Vec approach

Revisit if profiling shows this is a bottleneck for large outputs.

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
