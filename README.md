# GPL-boundary

Process-isolation boundary between GPL-licensed bioinformatics tools and the BSD-licensed [miint](https://github.com/the-miint/duckdb-miint) DuckDB extension.

## Purpose

miint is a BSD-licensed DuckDB extension. Some bioinformatics tools it needs to use are GPL-licensed. To maintain legal separation, this project runs GPL code in a **separate process** and communicates with miint via:

- **Control plane**: JSON over stdin/stdout
- **Data exchange**: Apache Arrow IPC over POSIX shared memory (zero-copy)

## Supported tools

| Tool | License | Description | Streaming |
|------|---------|-------------|-----------|
| [FastTree](https://github.com/the-miint/fasttree) | GPL-2.0+ | Approximately-maximum-likelihood phylogenetic trees | no |
| [Prodigal](https://github.com/the-miint/prodigal) | GPL-3.0 | Prokaryotic gene prediction | metagenomic mode only |
| [SortMeRNA](https://github.com/the-miint/sortmerna) | LGPL-3.0 | rRNA filtering and sequence alignment | yes |
| [Bowtie2 (align)](https://github.com/the-miint/bowtie2) | GPL-3.0 | Short read aligner; requires `.bt2` index (built in process or via `bowtie2-build`) | yes |
| [Bowtie2 (build)](https://github.com/the-miint/bowtie2) | GPL-3.0 | Build a `.bt2` index from in-memory FASTA-style sequences | no |

## Building

Requires a Rust toolchain, a C/C++ compiler, and the following system
libraries (needed by SortMeRNA):

- RocksDB (`librocksdb-dev` on Debian/Ubuntu, `brew install rocksdb` on macOS)
- zlib (`libz-dev` on Debian/Ubuntu, `brew install zlib` on macOS)

RocksDB is discovered via `pkg-config`.

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
gpl-boundary --describe prodigal
gpl-boundary --describe sortmerna
gpl-boundary --describe bowtie2-align
```

## Usage

miint spawns gpl-boundary as a long-lived child process and drives a session
via NDJSON on stdin/stdout. Every line is one JSON object: an `init`
handshake, a batch request, or a `shutdown` sentinel.

### Session

```jsonc
// 1. Init handshake (required first line)
{"init": {"idle_timeout_ms": 60000}}
// gpl-boundary replies:
{"success": true, "protocol_version": 1}

// 2. One or more batch requests — each carries its own (tool, config)
{"tool": "fasttree",
 "config": {"seq_type": "nucleotide", "seed": 12345},
 "shm_input": "/miint-input-uuid",
 "batch_id": 42}
// gpl-boundary replies (responses may arrive out of submission order
// across distinct fingerprints — correlate by `batch_id`):
{"success": true,
 "schema_version": 2,
 "batch_id": 42,
 "shm_outputs": [{"name": "/gb-1234-0-tree", "label": "tree", "size": 8192}],
 "result": {"n_nodes": 7, "n_leaves": 4, "root": 6}}

// 3. Graceful shutdown — or close stdin, or wait idle_timeout_ms
{"shutdown": true}
```

`protocol_version` (init reply) is bumped on any wire-envelope change;
`schema_version` (per response, per-tool) is bumped on per-tool output
schema changes. Both let consumers detect drift without parsing the data.

The dispatcher routes batches by `(tool, canonical_config)` fingerprint:
distinct fingerprints get distinct workers and run in parallel. Tools
with expensive setup (bowtie2 index loading, sortmerna reference
indexing, prodigal metagenomic init) reuse a long-lived context across
batches in the session.

### Worker routing and eviction

- `bowtie2-align` → **subprocess workers**. Each fingerprint gets its
  own `gpl-boundary --worker bowtie2-align` child process so distinct
  indexes can align in parallel without colliding on bowtie2's
  process-wide alignment mutex.
- everything else → **in-process workers**. Each fingerprint gets a
  dedicated worker thread; same-fingerprint batches serialize through
  its mpsc input queue, distinct fingerprints run concurrently.

A sweeper thread evicts workers that pass `worker_idle_ms` (default 5
min) AND have no in-flight batch — required for bowtie2 since loaded
indexes can be hundreds of MB and cannot stay resident indefinitely.
When a new subprocess fingerprint arrives over the `max_workers`
budget (default 4), the LRU idle subprocess is evicted; if every
existing worker is in-flight, the new submit is queued and dispatched
when a slot frees up.

Init knobs (all optional, JSON values on the `init` line):

| field | default | effect |
|---|---|---|
| `idle_timeout_ms` | 60_000 | Auto-shutdown after this much stdin silence; `0` disables. |
| `max_workers` | 4 | Soft cap on resident subprocess workers. |
| `workers_per_fingerprint` | 1 | Per-fingerprint subprocess cap (today: 1; bowtie2's global mutex makes >1 useless). |
| `max_idle_workers` | 4 | Idle subprocess workers retained after a burst. |
| `worker_idle_ms` | 300_000 | Per-worker idle deadline in ms. |

If a subprocess child crashes mid-batch, the parent surfaces an error
response for the in-flight batch, marks the fingerprint dead (so
subsequent same-fingerprint batches fail fast until session restart),
and best-effort sweeps any orphan `/dev/shm/gb-{child_pid}-*` segments.
Distinct fingerprints are unaffected.

## Architecture

```
miint (C++ DuckDB ext)                    gpl-boundary (Rust)
======================                    ===================
1. Spawn gpl-boundary                   ► Read {"init":{...}}, reply
                                          {"success":true,"protocol_version":1}
                                          Spawn sweeper thread + forwarder
2. For each batch:
   Create input shm, write Arrow IPC
   Send {"tool":...,"shm_input":...}    ► Read line, compute fingerprint
                                          Get-or-create worker
                                            (bowtie2-align: subprocess child)
                                            (others: in-process worker thread)
                                          Worker mmaps input shm (zero-copy),
                                            runs tool, creates output shm via
                                            sparse-mmap ShmWriter (no Vec memcpy),
                                            sends Response to coordinator
                                          Coordinator forwards Response to stdout
3. Read response line (out-of-order across fingerprints — match batch_id)
   Open each output shm, read `size` bytes of Arrow IPC
   Unlink input + output shm
... continue submitting more batches ...
   Sweeper evicts idle workers past `worker_idle_ms`; LRU evicts on
   max_workers pressure; in-flight batches are protected from eviction.
4. Send {"shutdown":true}               ► Drain in-flight + pending batches
                                          Close all workers (C destroy /
                                            child reap), exit 0
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
- Output names follow the pattern `/gb-{pid}-{n}-{label}` where label
  is `[a-z0-9-]+`.

## Submodule API control

We (the-miint org) maintain forks of the GPL submodules and control their library APIs. If the upstream API does not fit our integration needs, we can modify it. Changes to submodule APIs require documentation and coordination with the respective submodule development teams.

## License

GPL-3.0-or-later (required because this project links GPL-licensed libraries).
