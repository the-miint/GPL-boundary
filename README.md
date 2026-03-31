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
  "shm_output": "/gpl-boundary-1234-out",
  "shm_output_size": 8192,
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
   Open output shm, read shm_output_size bytes
   Unlink input and output shm
```

### Shared memory lifecycle

- **Input shm**: created by miint, read by gpl-boundary, unlinked by miint
- **Output shm**: created by gpl-boundary (PID-based name), read by miint, unlinked by miint
- **Signal safety**: gpl-boundary installs SIGINT/SIGTERM handlers that unlink output shm on abnormal exit
- **SIGKILL**: cannot be trapped; miint can detect stale segments via the PID in the name

## Submodule API control

We (the-miint org) maintain forks of the GPL submodules and control their library APIs. If the upstream API does not fit our integration needs, we can modify it. Changes to submodule APIs require documentation and coordination with the respective submodule development teams.

## License

GPL-3.0-or-later (required because this project links GPL-licensed libraries).
