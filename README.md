# GPL-boundary

Process-isolation boundary between GPL-licensed bioinformatics tools and the BSD-licensed [miint](https://github.com/the-miint/duckdb-miint) DuckDB extension.

## Purpose

miint is a BSD-licensed DuckDB extension. Some bioinformatics tools it needs to use are GPL-licensed. To maintain legal separation, this project runs GPL code in a **separate process** and communicates with miint via:

- **Control plane**: JSON over stdin/stdout
- **Data exchange**: Apache Arrow IPC over POSIX shared memory (zero-copy)

## Install

### Prebuilt binary (recommended)

Prebuilt binaries are published on every `v*` git tag for:

| Platform | Target triple | Notes |
|----------|---------------|-------|
| Linux x86_64 | `x86_64-unknown-linux-gnu` | glibc 2.35+ (Ubuntu 22.04+, Debian 12+, RHEL 9+) |
| macOS Apple Silicon | `aarch64-apple-darwin` | macOS 14+ |

Install the latest release with:

```bash
curl -fsSL https://github.com/the-miint/GPL-boundary/releases/latest/download/install.sh | sh
```

The script detects your platform, downloads the matching tarball,
verifies its SHA256 against the release's `SHA256SUMS`, and installs to
`~/.local/bin/gpl-boundary`. Override with environment variables:

```bash
GPL_BOUNDARY_VERSION=v0.2.0 INSTALL_DIR=/usr/local/bin \
  curl -fsSL https://github.com/the-miint/GPL-boundary/releases/latest/download/install.sh | sh
```

Runtime requirements: zlib (every Linux distro ships it; macOS provides
it in the SDK). The macOS binary statically links libomp, so no
Homebrew install is needed at runtime.

### Building from source

For platforms outside the prebuilt matrix — **Intel Macs**, **Linux
arm64**, older glibc, and any other configuration — build from source.
The build is self-contained: no system RocksDB, no `pkg-config` magic.

```bash
# 1. Install build prerequisites
#    Debian/Ubuntu:
sudo apt-get install -y build-essential cmake libz-dev curl
#    macOS (any arch):
brew install cmake libomp     # Xcode command-line tools provide the rest

# 2. Install the Rust toolchain (skip if you already have it)
curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"

# 3. Clone with submodules and build a release binary
git clone --recurse-submodules https://github.com/the-miint/GPL-boundary.git
cd GPL-boundary
cargo build --release

# 4. Install the binary somewhere on PATH
install -m 755 target/release/gpl-boundary ~/.local/bin/
```

The first build takes ~5 minutes (RocksDB compile is the bottleneck;
subsequent builds are cached by the `cmake` crate). The submodule clone
is heavy — RocksDB alone is several hundred MB. If you want to verify
the build, run `cargo test` or `make check`.

## Supported tools

| Tool | License | Description | Streaming |
|------|---------|-------------|-----------|
| [FastTree](https://github.com/the-miint/fasttree) | GPL-2.0+ | Approximately-maximum-likelihood phylogenetic trees | no |
| [Prodigal](https://github.com/the-miint/prodigal) | GPL-3.0 | Prokaryotic gene prediction | metagenomic mode only |
| [SortMeRNA](https://github.com/the-miint/sortmerna) | LGPL-3.0 | rRNA filtering and sequence alignment | yes |
| [Bowtie2 (align)](https://github.com/the-miint/bowtie2) | GPL-3.0 | Short read aligner; requires `.bt2` index (built in process or via `bowtie2-build`) | yes |
| [Bowtie2 (build)](https://github.com/the-miint/bowtie2) | GPL-3.0 | Build a `.bt2` index from in-memory FASTA-style sequences | no |

## Building

See [Building from source](#building-from-source) above. Quick reference
for contributors already set up:

```bash
cargo build          # debug build
cargo test           # unit + integration tests (uses real POSIX shared memory)
make check           # fmt + clippy + test
```

RocksDB is built from a vendored submodule (`ext/rocksdb`, pinned to
v8.11.5) and linked statically into the binary. The first build adds
~3–5 minutes for the RocksDB compile; subsequent builds are cached by
the `cmake` crate.

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
gpl-boundary --describe bowtie2-build
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
 "shm_input_size": 8192,
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

The wire format is line-delimited JSON (NDJSON) on stdin/stdout. Every line
on stdin is one of three envelopes; each line on stdout is one response.

### Stdin envelopes

| Envelope | Required first? | Shape |
|----------|-----------------|-------|
| `init` | yes (line 1) | `{"init": { ...session knobs... }}` |
| batch | no | `{"tool": "...", "config": {...}, "shm_input": "...", "batch_id": ...}` |
| `shutdown` | no | `{"shutdown": true}` |

Closing stdin or letting `idle_timeout_ms` elapse is equivalent to sending
`shutdown`.

### Batch request fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `tool` | string | yes | Must match a tool from `--list-tools` |
| `config` | object | yes | May be `{}`; defaults are applied for missing keys. Unknown keys are silently ignored. |
| `shm_input` | string | yes | POSIX shm name (must start with `/`) |
| `shm_input_size` | integer | yes | Exact byte count of the Arrow IPC stream in `shm_input`. The reader uses this to size its mapping; `fstat` is not reliable on Darwin POSIX shm. miint, which created the input segment, already knows this value. |
| `batch_id` | integer | no | Echoed on the response so out-of-order completions across fingerprints can be correlated |

Config parameter types (`seed` is a JSON number, `seq_type` is a JSON string,
etc.) are documented per-tool in `--describe` output.

### Response fields (stdout)

| Field | Type | Present | Notes |
|-------|------|---------|-------|
| `success` | boolean | always | |
| `error` | string | on failure only | Human-readable error message |
| `protocol_version` | integer | init reply only | Wire-envelope version; bumped on any envelope shape change |
| `schema_version` | integer | batch success only | Per-tool output Arrow schema version |
| `batch_id` | integer | batch reply, if request set it | Echoed from the matching request |
| `shm_outputs` | array | batch success, if non-empty | Each element: `{ "name": string, "label": string, "size": integer }` |
| `result` | object | batch success | Tool-specific lightweight metadata (never bulk data) |

### Versioning

Three independent integer versions cover three independent surfaces:

- **`protocol_version`** — the wire envelope (init/batch/shutdown shape).
  Returned once on the init reply. Bumped on any envelope-shape change.
- **`schema_version`** — per-tool output Arrow schema. Returned on every
  successful batch response. Bumped when a tool's output columns,
  types, or nullability change.
- **`describe_version`** — per-tool `--describe` introspection surface
  (config-knob set, defaults, doc strings). Surfaced inside the
  `--describe` JSON. Bumped when a tool's config surface changes.

Schema changes always co-bump `describe_version` (the schema is part of
the describe surface), but pure introspection edits (e.g. a new doc
string on an existing knob) bump only `describe_version`.

### Arrow IPC layout in shared memory

- **Format**: Arrow IPC **stream** format (not file format).
- **Offset**: raw IPC bytes start at byte 0 of the shared memory region.
- **Length**: the protocol's explicit byte count is the **only** authoritative
  size source. For batch *inputs*, that's `shm_input_size` in the request;
  for batch *outputs*, it's `size` in each `shm_outputs` entry of the response.
  Readers must mmap exactly that many bytes from offset 0; **do not call
  `fstat` to size the mapping**. This is required for cross-platform
  correctness: Darwin POSIX shm has unreliable `fstat` semantics, and
  gpl-boundary's own outputs are over-allocated to a sparse-mmap reservation
  (default 1 GiB), so `fstat` returns the reservation rather than the data
  length on either platform.
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
