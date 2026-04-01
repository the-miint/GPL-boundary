# GPL-boundary

Process-isolation boundary between GPL-licensed bioinformatics tools and the BSD-licensed [miint](https://github.com/the-miint/duckdb-miint) DuckDB extension.

## Purpose

miint is a BSD-licensed DuckDB extension. Some bioinformatics tools it needs to use are GPL-licensed. To maintain legal separation, this project runs GPL code in a **separate process** and communicates with miint via:

- **Control plane**: JSON over stdin/stdout
- **Data exchange**: Apache Arrow IPC over POSIX shared memory (zero-copy)

## Supported tools

| Tool | License | Description |
|------|---------|-------------|
| [FastTree](https://github.com/the-miint/fasttree) | GPL-2.0+ | Approximately-maximum-likelihood phylogenetic trees |

## Building

Requires Rust toolchain and a C compiler.

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/the-miint/GPL-boundary.git
cd GPL-boundary

# Build
cargo build

# Test
cargo test

# All checks (fmt + clippy + test)
make check
```

## Introspection

The binary supports introspection flags for programmatic discovery:

```bash
# Version info (gpl-boundary + all tool versions)
gpl-boundary --version

# List available tools
gpl-boundary --list-tools

# Full tool description (config params, Arrow schemas, response metadata)
gpl-boundary --describe fasttree
```

## Usage

miint spawns gpl-boundary as a child process. The JSON request provides the
tool name, config parameters, and the POSIX shared memory name where input
Arrow IPC data has been written. gpl-boundary creates its own output shared
memory, and returns the name and size in the JSON response.

### Request (stdin)

```json
{
  "tool": "fasttree",
  "config": { "seq_type": "nucleotide", "seed": 12345 },
  "shm_input": "/miint-input-uuid"
}
```

### Response (stdout)

```json
{
  "success": true,
  "schema_version": 1,
  "shm_outputs": [
    { "name": "/gpl-boundary-1234-tree", "label": "tree", "size": 8192 }
  ],
  "result": { "n_nodes": 7, "n_leaves": 4, "root": 6, "stats": { ... } }
}
```

## Architecture

```
miint (C++ DuckDB ext)                    gpl-boundary (Rust)
======================                    ===================
1. Create input shm, write Arrow IPC
2. Spawn gpl-boundary
   Write JSON request to stdin
   Close stdin
3.                                        Read JSON from stdin
                                          mmap input shm
                                          Run tool (e.g., fasttree)
                                          Create output shm
                                          Write Arrow IPC results
                                          Write JSON response to stdout
                                          Exit
4. Read JSON from stdout
   Open each output shm, read `size` bytes of Arrow IPC
   Unlink input and all output shm
```

### Shared memory lifecycle

- **Input shm**: created by miint, read by gpl-boundary, unlinked by miint
- **Output shm**: created by gpl-boundary (PID-based name), read by miint, unlinked by miint
- **Signal safety**: gpl-boundary installs SIGINT/SIGTERM handlers that unlink output shm on abnormal exit
- **SIGKILL**: cannot be trapped; miint can detect stale segments via the PID in the name

## Protocol reference

This section specifies the protocol precisely enough for a clean-room client
implementation (e.g., the BSD-licensed miint consumer).

### JSON request (stdin)

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `tool` | string | yes | Must match a tool from `--list-tools` |
| `config` | object | yes | May be `{}`; defaults are applied for missing keys. Unknown keys are silently ignored. |
| `shm_input` | string | yes | POSIX shm name (must start with `/`) |

Config parameter types (`seed` is a JSON number, `seq_type` is a JSON string,
etc.) are documented per-tool in `--describe` output.

### JSON response (stdout)

| Field | Type | Present | Notes |
|-------|------|---------|-------|
| `success` | boolean | always | |
| `error` | string | on failure only | Human-readable error message |
| `schema_version` | integer | on success only | Bumped on breaking output schema changes |
| `shm_outputs` | array | on success, if non-empty | Each element: `{ "name": string, "label": string, "size": integer }` |
| `result` | object | on success | Tool-specific lightweight metadata (never bulk data) |

### Arrow IPC layout in shared memory

- **Format**: Arrow IPC **stream** format (not file format).
- **Offset**: raw IPC bytes start at byte 0 of the shared memory region.
- **Length**: `size` in `shm_outputs` is the exact byte count of IPC data.
  Read exactly `size` bytes; the shm region may be larger due to page alignment.
- **Batches**: a single RecordBatch per stream (current convention; consumers
  should handle multiple batches for forward compatibility).

### Shared memory naming constraints

- Names must start with `/` (POSIX requirement).
- **macOS**: maximum 31 characters including the leading `/`.
- **Linux**: maximum 255 characters including the leading `/`.
- Allowed characters: alphanumeric, `-`, `_`, `.` (portable set).
- gpl-boundary does not validate input names beyond passing them to
  `shm_open()`; invalid names produce OS-level errors.
- Output names follow the pattern `/gpl-boundary-{pid}-{label}` where label
  is `[a-z0-9-]+`.

## Submodule API control

We (the-miint org) maintain forks of the GPL submodules and control their library APIs. If the upstream API does not fit our integration needs, we can modify it. Changes to submodule APIs require documentation and coordination with the respective submodule development teams.

## License

GPL-3.0-or-later (required because this project links GPL-licensed libraries).
