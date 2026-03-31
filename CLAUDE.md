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

## Shared memory lifecycle (critical)

This is the most important part of the architecture to understand.

### Normal flow

1. **miint** creates input shm, writes Arrow IPC data
2. **miint** spawns `gpl-boundary`, writes JSON request to stdin (includes
   `shm_input` name)
3. **gpl-boundary** reads input from shm, runs tool, creates output shm with
   PID-based name (`/gpl-boundary-{pid}-out`), writes Arrow IPC results
4. **gpl-boundary** responds via stdout with `shm_output` name and
   `shm_output_size`
5. **miint** reads response, opens output shm, reads `shm_output_size` bytes
   of Arrow IPC
6. **miint** unlinks both input and output shm (it owns cleanup of both)

### Signal handling

gpl-boundary installs signal handlers for SIGINT and SIGTERM that unlink any
output shm it has created. This prevents leaks on graceful termination.

**SIGKILL cannot be trapped.** The PID-based naming convention
(`/gpl-boundary-{pid}-out`) lets miint detect and clean up stale segments by
checking if the PID is still alive.

### Cleanup registry

- `shm::register_for_cleanup(name)` -- called when output shm is created
- `shm::deregister_cleanup(name)` -- called after successful response (caller
  now owns it)
- Signal handler and `cleanup_all()` iterate the registry and unlink everything

### SharedMemory::detach()

When output shm is ready for the caller to read, we call `detach()` which
munmaps without unlinking. This is the proper API -- never use `mem::forget`.

## Submodules

GPL-licensed tools live as git submodules under `ext/`. Currently:

- `ext/fasttree` -- FastTree phylogenetic tree inference (GPL-2.0+, C99,
  branch `v2.3.0-miint`)

**We control the submodule APIs** (the-miint org maintains forks). If an API
does not fit our needs, we can modify it. However, changes require
documentation for the separate submodule development teams.

### Adding a new submodule

Each tool gets:
1. A git submodule under `ext/<toolname>`
2. Build integration in `build.rs`
3. An adapter in `src/tools/<toolname>.rs` implementing `GplTool`
4. Registration in `src/tools/mod.rs` dispatch
5. Tool-specific API documentation (input/output schemas, config params) via
   the `describe()` method on `GplTool`
6. Smoke tests using real shared memory

We do not exhaustively test submodules; that is their own CI's job.

## Build

```bash
cargo build          # builds fasttree C lib via cc crate + links it
cargo test           # unit + integration tests (uses real POSIX shared memory)
make check           # fmt + clippy + test
```

The `build.rs` compiles C sources from submodules using the `cc` crate. No
system-level install of submodule libraries is needed.

## Code organization

```
src/
  main.rs            # Entry point: CLI flags or stdin JSON dispatch
  protocol.rs        # Request/Response JSON types (serde)
  shm.rs             # POSIX shared memory, cleanup registry, signal handlers
  tools/
    mod.rs           # GplTool trait, dispatch, introspection (describe/version)
    fasttree.rs      # FastTree FFI bindings + Arrow IPC marshaling + tests
ext/
  fasttree/          # git submodule (GPL, C99)
build.rs             # Compiles C submodule sources via cc crate
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
- **FFI**: raw bindings in each tool module. `#[repr(C)]` structs must match
  C headers exactly.
- **Error handling**: tools return `Response::error(msg)`. Never panic across
  FFI.
- **CI**: GitHub Actions on Linux + macOS. Runs `cargo test`, `cargo clippy`,
  `cargo fmt --check`.
- **No Newick output**: fasttree returns SOA tree structure as Arrow columnar
  data, not Newick strings.
- **Arrow IPC stream format** (not file format) for shared memory exchange.
- **No bulk data in JSON**: sequences, trees, etc. always go through Arrow IPC
  in shared memory. JSON carries only tool name, parameters, shm paths, and
  lightweight result metadata.

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
  "shm_output": "/gpl-boundary-1234-out",
  "shm_output_size": 8192,
  "result": { "n_nodes": 7, "n_leaves": 4, "root": 6, "stats": { ... } }
}
```

## Arrow schemas

**FastTree input** (written by miint to shm_input):
- `name: Utf8` -- sequence identifier
- `sequence: Utf8` -- aligned sequence (all must be equal length)

**FastTree output** (written by gpl-boundary to shm_output):
- `id: Int32` -- node index [0, n_nodes)
- `parent: Int32` -- parent node index (-1 for root)
- `branch_length: Float64`
- `support: Float64` -- SH-like local support (-1 if not computed)
- `n_children: Int32` -- 0 for leaves
- `is_leaf: Boolean`
- `name: Utf8` (nullable) -- leaf name, null for internal nodes

## Related projects

- **miint**: `../duckdb-miint` -- C++ DuckDB extension that spawns this binary
- **fasttree**: `ext/fasttree` -- GPL phylogenetic tree library
