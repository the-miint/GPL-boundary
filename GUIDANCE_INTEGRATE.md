# Integration Guide: Adding a New Tool to GPL-boundary

This document is for GPL-boundary maintainers adding a new submodule tool.
It covers the exact files to create or modify, the patterns to follow, and
the verification steps. For C API requirements that the submodule itself must
satisfy, see [GUIDANCE_API_LIBRARY.md](GUIDANCE_API_LIBRARY.md).

The reference integration is FastTree (`ext/fasttree/`, `build.rs`,
`src/tools/fasttree.rs`).

## Files to touch

| File | Change |
|------|--------|
| `.gitmodules` | New submodule entry (automatic via `git submodule add`) |
| `ext/<tool>/` | Submodule checkout |
| `build.rs` | Add `build_<tool>()` function, call it from `main()` |
| `src/tools/<tool>.rs` | **New file**: FFI bindings, Arrow schemas, `GplTool` impl, tests |
| `src/tools/mod.rs` | Add `pub mod <tool>;` |

You do **not** need to modify `main.rs`, `protocol.rs`, `frontend.rs`,
`arrow_ipc.rs`, `shm.rs`, or `Cargo.toml` (unless the tool requires new Rust
dependencies). The `inventory` crate handles dispatch, introspection, and
registry automatically.

A tool never touches shared memory. It takes its input as Arrow record
batches and returns its outputs as record batches; the native front end
(`src/frontend.rs`) reads the request's input segment, runs the tool, and
writes the outputs to new segments. A tool module that names the transport
(`shm`, shared memory, `arrow_ipc`) fails
`frontend::tests::test_tool_modules_are_transport_free`.

## Step 1: Add the git submodule

```bash
git submodule add -b <branch> https://github.com/the-miint/<tool>.git ext/<tool>
```

The submodule must expose a C API conforming to
[GUIDANCE_API_LIBRARY.md](GUIDANCE_API_LIBRARY.md): `config_init`, `create`,
`destroy`, `run`, `output_free`, `strerror`, `last_error`, a `NO_MAIN`
guard, `struct_size` as first config field, SOA output, no global mutable
state, no stdout/stderr writes.

## Step 2: Add `build_<tool>()` to `build.rs`

Each tool gets its own static archive, a unique `.compile()` name, to prevent
symbol collisions between C libraries. A source that needs other flags is
compiled on its own (`compile_intermediates()`) and its object added to the
tool's archive (`.objects(...)`), as `build_prodigal()` does for Prodigal's
`training.c`: a second archive would make the link order matter.

```rust
fn main() {
    build_fasttree();
    build_newtool();   // add this call
    link_math();
}

fn build_newtool() {
    let dir = PathBuf::from("ext/newtool");

    let mut build = cc::Build::new();
    build
        .file(dir.join("newtool_core.c"))
        .file(dir.join("newtool_api.c"))
        .include(&dir)
        .define("NEWTOOL_NO_MAIN", None)
        .flag("-fvisibility=hidden")
        // The submodule's own release-build flags (its Makefile's, or
        // CMake Release's), nothing else, unless a compiler cannot build a
        // file with them: extra optimisation flags can cost
        // (-funroll-loops made Prodigal 46% slower) and FP flags can change
        // results
        .opt_level(3)
        .warnings(false);

    // C++ builds also need -fvisibility-inlines-hidden
    if is_cpp {
        build.flag("-fvisibility-inlines-hidden");
    }

    build.compile("newtool_c");

    println!("cargo:rerun-if-changed=ext/newtool/newtool_core.c");
    println!("cargo:rerun-if-changed=ext/newtool/newtool_api.c");
    println!("cargo:rerun-if-changed=ext/newtool/newtool.h");
}
```

Key points:
- **Separate `cc::Build` per tool** -- never add files to another tool's build
- **`define("NEWTOOL_NO_MAIN", None)`** -- suppresses the submodule's `main()`
- **`flag("-fvisibility=hidden")`** -- **required**. Hides all symbols except
  the `extern "C"` API. Without this, internal C/C++ symbols (template
  instantiations, static helpers) leak into the Rust binary's symbol table
  and can collide with system/runtime symbols, causing crashes on macOS
  aarch64. For C++ builds, also add `flag("-fvisibility-inlines-hidden")`.
- **`compile("newtool_c")`** -- unique static library name
- **`cargo:rerun-if-changed`** for every source and header file the build uses
- Add tool-specific defines as needed (e.g., `USE_DOUBLE` for precision)

## Step 3: Create `src/tools/<tool>.rs`

This file contains the complete tool adapter. It has five sections.

### 3a. FFI bindings

Manual `#[repr(C)]` structs that mirror the C header exactly. Do not use
bindgen.

```rust
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

#[repr(C)]
pub struct NewToolConfig {
    pub struct_size: usize,     // always first field
    pub param_a: c_int,
    pub seed: i64,
    // ... match C header field-for-field, order matters
}

#[repr(C)]
pub struct NewToolStats {
    // value-only fields, no pointers
    pub iterations: c_int,
    pub score: f64,
}

#[repr(C)]
pub struct NewToolOutput {
    // SOA layout: parallel arrays
    pub n_items: c_int,
    pub id: *const c_int,
    pub score: *const f64,
    pub name: *const *const c_char,
    // backing store (not accessed from Rust, but must be present for ABI)
    pub _name_buf: *const c_char,
    pub _base: *mut c_void,
}

#[allow(non_camel_case_types)]
type newtool_ctx_t = c_void;

extern "C" {
    fn newtool_config_init(config: *mut NewToolConfig);
    fn newtool_create(config: *const NewToolConfig) -> *mut newtool_ctx_t;
    fn newtool_destroy(ctx: *mut newtool_ctx_t);
    fn newtool_run(
        ctx: *mut newtool_ctx_t,
        /* input arrays + counts */
        output: *mut *mut NewToolOutput,
        stats: *mut NewToolStats,
    ) -> c_int;
    fn newtool_output_free(output: *mut NewToolOutput);
    fn newtool_strerror(code: c_int) -> *const c_char;
    fn newtool_last_error(ctx: *mut newtool_ctx_t) -> *const c_char;
}

const NEWTOOL_OK: c_int = 0;
const NEWTOOL_VERSION: &str = "1.0.0";
```

### 3b. Arrow schema definitions

Define the input schema (what the tool expects from miint) and each
output's schema (what the tool produces as Arrow IPC) once, as
`FieldDescription`s: `--describe` reports them, and
`crate::tools::arrow_schema` builds the Arrow `Schema` from the same list,
so the two can't drift. `src/tools/prodigal.rs` does this; the older
adapters still write their Arrow schemas by hand.

```rust
use crate::tools::{arrow_schema, FieldDescription, OutputDescription};

const INPUT_FIELDS: &[FieldDescription] = &[
    FieldDescription::new("name", "Utf8", false, "Item identifier"),
    FieldDescription::new("value", "Float64", false, "Item value"),
];

const RESULTS: OutputDescription = OutputDescription {
    label: "results",
    schema: &[
        // Index columns: prefer Int64 for any column that could plausibly
        // exceed 2³¹ — node indices, alignment positions, etc. Int32 is
        // fine only for small bounded counts (children counts, flags).
        FieldDescription::new("id", "Int64", false, "Item index"),
        // Optional / "absent" values: use nullable columns rather than
        // sentinel values like -1 or 0. Translate the C library's
        // sentinel (documented in its header) to Arrow NULL on emission.
        // See FastTree's `branch_length` and `support`, and bowtie2-align's
        // XN/XM/XO/XG tags (gated on the unmapped flag), for examples.
        FieldDescription::new("score", "Float64", true, "Score; NULL if none"),
        FieldDescription::new("name", "Utf8", true, "Name; NULL if none"),
    ],
};
```

`arrow_schema(RESULTS.schema)?` is the output's Arrow `Schema`; a type
that isn't Arrow's `DataType` notation (`"Int32"`, `"Utf8"`, ...) is an
error. A test can build the input's the same way.

### 3c. Tool struct and registration

```rust
pub struct NewTool;

inventory::submit! {
    crate::tools::ToolRegistration {
        create: || Box::new(NewTool),
    }
}
```

This single `inventory::submit!` is all the registration needed. The tool
automatically appears in `--version`, `--list-tools`, `--describe`, and
JSON request dispatch. No manual match arms or dispatch tables.

### 3d. `GplTool` trait implementation

Implement five methods:

```rust
impl GplTool for NewTool {
    fn name(&self) -> &str { "newtool" }

    fn version(&self) -> String { NEWTOOL_VERSION.to_string() }

    fn schema_version(&self) -> u32 { 1 }

    fn describe(&self) -> ToolDescription {
        ToolDescription {
            name: "newtool",
            version: self.version(),
            schema_version: self.schema_version(),
            // Bump on any change to this description's surface, and update
            // the landmark test in src/tools/mod.rs
            describe_version: 1,
            description: "One-line description of what this tool does",
            config_params: vec![
                ConfigParam {
                    name: "seed",
                    param_type: "integer",
                    default: serde_json::json!(42),
                    description: "Random seed for reproducibility",
                    allowed_values: vec![],
                },
                // ... all config params
            ],
            input_schema: INPUT_FIELDS.to_vec(),
            // One per output, in the order the response lists them
            outputs: vec![RESULTS],
            response_metadata: vec![/* FieldDescription for JSON result fields */],
        }
    }

    fn execute(
        &self,
        config: &serde_json::Value,
        input: Vec<RecordBatch>,
    ) -> Result<ToolOutput, String> {
        // See below
    }
}
```

**`execute()` follows this pattern:**

1. Read the input batches: every batch of the request's stream, in order;
   there may be none. Natively they are read-only views of the caller's
   input segment: don't keep them, or anything sliced from them, past the
   call.
2. Extract columns, validate (a null where the tool needs a value is an
   error naming the input row), convert to C types (`Vec<CString>` etc.;
   a NUL byte is an error, not an `unwrap`). `utf8_column`, `non_null` and
   `c_strings` in `crate::tools` do these and word the errors. Once the
   input is copied out, `drop(input)` before the C library runs.
3. Initialize C config via FFI: `zeroed()` + `config_init()`, then apply
   the JSON strictly: `config_knobs_known(config, &params)` refuses a knob
   `--describe` doesn't list, and `config_bool`/`config_str`/`config_f64`/
   `config_cint` (all in `crate::tools`) refuse a wrong JSON type or an
   integer outside C int, naming the knob, and take null as the default
4. Create context: `newtool_create(&config)`, check for null
5. Call the computation: `newtool_run(ctx, ...)`
6. On error: `strerror(rc)` + `last_error(ctx)` → `Err(format!(...))`,
   then `destroy(ctx)`
7. On success: convert SOA output → Arrow `RecordBatch` using
   `arrow_schema(RESULTS.schema)`. The batch must own its memory (copy the
   C arrays, as below): the front end writes it after the call returns.
8. Free C output + destroy context, whether the conversion succeeded or not
9. Return `Ok(ToolOutput { outputs: vec![(RESULTS.label, batch)], result })`,
   the outputs under their `OutputDescription` labels, in `describe()`'s
   order. The front end writes them all or none and builds the response;
   an `Err` becomes the error response.

**Important**: always free C resources (output, context) on every code path,
including errors. The tool must never panic across FFI.

### 3e. SOA → RecordBatch conversion

Write an `unsafe` helper that wraps each C array into an Arrow column:

```rust
unsafe fn soa_to_record_batch(output: &NewToolOutput) -> Result<RecordBatch, String> {
    let schema = Arc::new(arrow_schema(RESULTS.schema)?);
    // A negative count is an error, not a huge slice
    let n = usize::try_from(output.n_items)
        .map_err(|_| format!("the library returned {} items", output.n_items))?;
    // With no items the arrays may be NULL, which from_raw_parts must not get
    if n == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let ids = std::slice::from_raw_parts(output.id, n); // int64_t *
    let scores = std::slice::from_raw_parts(output.score, n);
    let name_ptrs = std::slice::from_raw_parts(output.name, n);

    let names: Vec<Option<String>> = name_ptrs.iter().map(|&p| {
        if p.is_null() { None }
        else { Some(CStr::from_ptr(p).to_string_lossy().into_owned()) }
    }).collect();

    RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids.to_vec())),
        Arc::new(Float64Array::from(scores.to_vec())),
        Arc::new(StringArray::from(
            names.iter().map(|n| n.as_deref()).collect::<Vec<Option<&str>>>()
        )),
    ]).map_err(|e| format!("Failed to create Arrow RecordBatch: {e}"))
}
```

### 3f. Tests

Three required tests in a `#[cfg(test)] mod tests` block:

Tool tests call the tool directly with record batches; no shared memory
(the front end has its own tests on real segments, and `tests/` drives the
daemon).

**1. Happy-path roundtrip:**

```rust
#[test]
fn test_newtool_roundtrip() {
    let batch = make_input_batch(/* test data */);
    let config = serde_json::json!({ "seed": 42 });
    let output = NewTool.execute(&config, vec![batch]).expect("newtool failed");

    // The labels, order and columns `describe()` lists
    assert_outputs_described(&NewTool.describe(), &output);
    // ... assert expected metadata fields in output.result ...

    let (label, results) = &output.outputs[0];
    assert_eq!(*label, RESULTS.label);
    // ... assert row count, column values ...
}
```

**2. A refused input** (an empty input is refused; a null names its row,
a missing or non-Utf8 column names the column):

```rust
#[test]
fn test_newtool_refuses_bad_input() {
    let err = NewTool.execute(&serde_json::json!({}), vec![]).unwrap_err();
    assert!(err.contains("At least 1"), "{err}");
}
```

**3. ABI size check:**

```rust
#[test]
fn test_config_struct_abi_size() {
    let mut config: NewToolConfig = unsafe { std::mem::zeroed() };
    unsafe { newtool_config_init(&mut config) };
    assert_eq!(
        config.struct_size,
        std::mem::size_of::<NewToolConfig>(),
        "ABI mismatch: Rust NewToolConfig ({} bytes) vs C ({} bytes)",
        std::mem::size_of::<NewToolConfig>(),
        config.struct_size,
    );
}
```

4. **(If streaming)** Multi-batch streaming roundtrip — see Step 6 for the
   test template.

If the tool accepts meaningfully different input types (e.g., nucleotide vs.
protein), add a roundtrip test for each.

## Step 4: Register the module in `src/tools/mod.rs`

Add one line:

```rust
pub mod fasttree;
pub mod newtool;   // add this
```

## Step 5: Verify

```bash
cargo build                       # compiles C sources + links
cargo test                        # all tests including new smoke tests
make check                        # fmt + clippy + test + test-scripts (full CI check)
```

Verify introspection:

```bash
cargo run -- --version            # should list newtool with version
cargo run -- --list-tools         # should include "newtool"
cargo run -- --describe newtool   # should show full schema/config docs
```

## Step 6 (optional): Add streaming support

Add streaming support when a tool has expensive context creation (index
loading, reference indexing, model initialization) AND processes independent
records per batch. Tools where all input must be present at once (e.g.,
FastTree) do not need streaming.

### What to implement

1. A `<Tool>StreamingContext` type holding the session's state: usually
   the C context pointer, whose owner's `Drop` calls the C `destroy()`
   function.
2. Override `create_streaming_context()` on the `GplTool` impl. Parse config,
   call C `create()`, return the context. A config whose batches can't
   share one context still gets a context that does the per-batch work
   (e.g., prodigal's `train_on_batch` trains a fresh one on each batch), so
   execute() and the session run the same code.
3. Implement `StreamingContext::run_batch()`: the same input, output and
   errors as `execute`, against the loaded context: read the input
   batches, call C `run()`, convert the output, return the `ToolOutput`.
   An error fails only that batch; the context must stay usable. Share
   the per-batch code between `execute` and `run_batch` (bowtie2-align's
   `Bowtie2StreamingContext::open` and `align`), rather than two copies.

### Streaming test template

```rust
#[test]
fn test_newtool_streaming_two_batches() {
    let tool = NewTool;
    let config = serde_json::json!({/* ... */});
    let mut ctx = tool.create_streaming_context(&config)
        .expect("context creation failed")
        .expect("tool should support streaming");

    // Batch 1
    let first = ctx.run_batch(vec![make_input_batch(/* data */)]);
    let first = first.expect("Batch 1 failed");

    // Batch 2, on the same context
    let second = ctx.run_batch(vec![make_input_batch(/* data */)]);
    let second = second.expect("Batch 2 failed");
    // ... assert the context carried over (e.g. a model trained once) ...
}
```

## Routing in the daemon

Once a tool's `inventory::submit!` registration is in place, the daemon
dispatcher routes batches to it automatically based on a `(tool_name,
canonical_config)` fingerprint. Two routing decisions matter for new
tools:

- **In-process vs. subprocess.** Default is in-process: the registry
  spawns a dedicated worker thread per fingerprint, the tool's runner
  (`tools::open`: its streaming context, or the tool itself per batch)
  runs there through the front end, responses flow back via
  the worker's `Sender<Response>`. Same-fingerprint batches serialize
  through the worker thread's mpsc queue; distinct fingerprints run
  concurrently. The exception is **subprocess routing**, used today
  only for `bowtie2-align` because bowtie2 has process-wide global
  state behind a mutex — multiple contexts cannot coexist in one
  address space, so each fingerprint gets its own
  `gpl-boundary --worker bowtie2-align` child process. Add subprocess
  routing for a new tool only if the C library has the same
  process-wide-state limitation; the routing decision lives in
  `src/registry.rs::build_worker`.

- **Eviction.** The sweeper thread evicts workers past
  `worker_idle_ms` AND with `in_flight == 0`, plus LRU eviction of
  idle subprocess workers under `max_workers` budget pressure. This
  applies uniformly — no per-tool opt-in required. If your tool's
  context holds expensive resources (loaded indexes, reference DBs),
  the eviction path frees them when the fingerprint goes idle long
  enough; you don't need to do anything special, just make sure the
  streaming context's `Drop` impl correctly tears down the C
  resources.

## Schema versioning

Bump `schema_version()` whenever you make a breaking change to the
output schema (renames, type changes, removed columns). The version
is per-tool and surfaces in every response's `schema_version` field
plus the `--describe` output. Conventions to follow on new tools:

- Prefer `Int64` for index columns; reserve `Int32` for bounded
  counts.
- Prefer Arrow NULL over `-1`/`0`/`NaN` sentinels; gate on a clear
  signal (a sibling `aligned == 0` flag, or a documented C sentinel).
- Pick column names that match consumer vocabulary (e.g. `is_tip`
  rather than `is_leaf` for tree nodes).

## Per-tool integration contracts

Cross-cutting conventions live above; this section documents
tool-specific input shapes and lifetime invariants that callers
(currently `duckdb-miint`) need to understand to drive the daemon
correctly.

### bowtie2-align: paired-end auto-detection

Detection is by the presence of any non-null `sequence2` value in the
input Arrow stream:

- **Single-end**: `sequence2` column either missing entirely OR present
  with all rows null.
- **Paired-end**: `sequence2` column present with at least one non-null
  value. All rows in the submission must then have non-null
  `sequence2`.

The rules:

1. **Within a single submission (one request's input batches), paired/single mode is
   uniform.** A submission containing one paired batch and one
   all-null-`sequence2` batch under the same schema is rejected with
   "inconsistent paired-end mode across batches". Mode-mixing here
   would leave `seqs2.len() < read_ids.len()` and feed the C library a
   short pointer array; the rejection surfaces that as a clean error
   rather than silent OOB.
2. **Across separate `run_batch` calls in one streaming session, mode
   switching is permitted.** Each batch is parsed independently, so
   one session can serve a mix of single-end and paired-end
   submissions back-to-back through the same loaded index.
3. **Within a single submission, `sequence2` must be uniformly null
   or uniformly non-null.** Mixed null/non-null `sequence2` rows are
   rejected.

### bowtie2-align: qual1 / qual2 nullability

`qual1` and `qual2` are Phred+33 quality strings; null on a row means
"FASTA-equivalent default quality" for that mate. The daemon enforces:

- `qual1` must be uniformly null (FASTA mode) or uniformly non-null
  (FASTQ mode) across all rows in a submission. Mixed → reject.
- In paired-end mode, `qual2` follows the same rule.
- In paired-end mode, the `qual1` and `qual2` null-states must agree:
  FASTQ on both mates or FASTA on both mates. FASTQ on one mate and
  FASTA on the other is rejected. A missing `qual2` column counts as
  "all null" (paired FASTA on mate 2).

The rationale is to surface a precise SQL-level diagnostic at the
input boundary rather than a cryptic bowtie2 internal error
mid-alignment.

### bowtie2-align: streaming semantics

The daemon's per-fingerprint worker pool gives sharded callers
per-shard parallelism for free. The four invariants miint relies on:

1. **One streaming context = one loaded index = one
   `(tool, config_fingerprint)` worker.** The
   `src/registry.rs::Registry` keys on canonical-config JSON, so two
   configs differing only in key order share a worker.
2. **Each `run_batch()` call answers its own batch:** its outputs go
   to fresh output segments in that batch's response. No batching
   across `run_batch` calls.
3. **Same `index_path` (and same other config) → same fingerprint →
   serialized through one subprocess worker.** bowtie2's
   process-wide alignment mutex makes within-fingerprint parallelism
   pointless; the `workers_per_fingerprint` init knob is accepted but
   has no effect for bowtie2 (`src/main.rs::eviction_config_from`
   discards it).
4. **Distinct `index_path` values → distinct fingerprints → distinct
   subprocess workers running in parallel.** This is the sharded
   alignment path: each shard is a different `index_path`; the daemon
   schedules them concurrently up to the `max_workers` budget. No
   shard-aware tool variant needed on the gpl-boundary side.

**Index loading — gpl-boundary always memory-maps the index (`--mm`).**
Invariant 1's "one loaded index" describes the daemon's worker model, not
the C library. As of the v2.5.5-miint pin (bt2 C API v0.4), the bowtie2 C
API *reconstructs* the FM-index on every `bt2_align_run` rather than holding
it resident — the persistent-`Ebwt` refactor was declined upstream as too
complex. To keep that per-batch reload cheap, gpl-boundary sets bowtie2's
`--mm` unconditionally on every alignment (`memory_mapped = 1`): the index is
mapped from the page cache (shared `MAP_SHARED` pages, cheap minor faults)
instead of read-copied into a private buffer each batch, which removes the
bulk of the prior ~30–40% per-batch reload tax. This is **implicit** and is
**not** a config knob — there is no miint scenario that benefits from turning
it off, so it does not appear in the `--describe` surface and does not affect
`describe_version`. Two implications for miint: (a) the on-disk `.bt2` index
files must stay present and unmodified for the lifetime of a worker (already
true — miint owns them and builds them once); (b) per-worker RSS shifts toward
shared page-cache pages rather than private anonymous memory, so resident-set
accounting for a bowtie2-align worker will read lower and be shared with other
processes mapping the same index.

Eviction (sweeper thread, default 100 ms tick) frees a subprocess
when its `last_used + worker_idle_ms < now` AND it has no in-flight
batches, or under LRU pressure when a new fingerprint would exceed
`max_workers`. In-flight batches are protected from eviction.

### bowtie2-build: temp files + index ownership

`bowtie2-build` materializes its input Arrow batches to a temporary
FASTA file before invoking the C builder API (the builder takes disk
paths only). Two contract points:

- **`TMPDIR` is honored.** The temp FASTA is created via
  `tempfile::Builder::new().tempfile()`, which calls
  `std::env::temp_dir()`, which honors `TMPDIR` on Unix. HPC users
  who set `TMPDIR` to a scratch filesystem will see the materialized
  FASTA land there.
- **`index_files` paths in the response are caller-owned.** The
  caller specifies `index_path` (basename) in config; bowtie2-build
  writes `.bt2` / `.bt2l` files at that basename; the response's
  `result.index_files` array lists the absolute paths actually
  written. The daemon does NOT manage their lifetime — no LRU, no
  TTL, no auto-cleanup. The caller is responsible for `unlink()`ing
  them when done (miint does this in its `Data` destructor).

There is no daemon-managed built-index cache today. A future LRU is
plausible but explicitly out of scope.

## Common mistakes

### Forgetting `cargo:rerun-if-changed` in build.rs

Without these directives, cargo won't recompile the C sources when they
change. List every `.c` and `.h` file used by the build.

### Returning output that views C memory

An output record batch must own its memory. One built over the C library's
arrays (`Buffer::from_custom_allocation`, or a slice of a C pointer) dangles
once the tool frees its C output, which happens before the front end writes
the batch. Copy the arrays into Arrow builders or `Vec`s.

### Keeping the input past the call

The input batches are views of the caller's segment. A tool that keeps one,
or a column sliced from it, in a streaming context keeps the whole input
mapped across batches; a C library that keeps a pointer into it reads freed
memory once the batch drops. Copy out what must outlive the call.

### Struct field order mismatch

The `#[repr(C)]` struct must match the C header field-for-field in order.
The ABI size-check test catches added/removed fields but not reordering.
Manual review against the C header is required when fields change.

### Reusing another tool's cc::Build

Each tool must have its own archive, built from its own
`cc::Build::new()` chains. Adding files to another tool's archive causes
symbol collisions.

### Storing raw pointers to per-batch data in the streaming context

Only store pointers to session-lived state (config strings, reference paths),
never to per-batch input. Per-batch input is owned by the Rust adapter and
freed after `run_batch()` returns.

### Forgetting Drop on the streaming context

The streaming context struct must implement `Drop` to call the C library's
`destroy()` function. Without it, the C context leaks on every streaming
session.

### Writing bulk data to JSON

Sequences, trees, matrices, and other bulk data always go through Arrow IPC
in shared memory. JSON carries only tool name, parameters, shm paths, and
lightweight result metadata.

### Sentinel values in non-nullable columns

If your C library uses `-1` (or `0`, or `INT_MIN`) to mean "absent",
emit the Arrow column as nullable and translate the sentinel to NULL
in the SOA→RecordBatch conversion. Non-nullable columns with
in-band sentinels force every consumer to remember the per-column
sentinel value, and produce silently-wrong joins when the sentinel
collides with a valid value. See `src/tools/fasttree.rs` and
`src/tools/bowtie2_align.rs` for the pattern: gate either on a clean
sibling column (e.g. bowtie2-align's XN/XM/XO/XG tags, NULL when the
`flags` column has the unmapped bit 0x4) or on a documented
out-of-range value (e.g. FastTree's `parent == -1` for root).

## Updating CLAUDE.md

After integration, update `CLAUDE.md` to document:
- The new submodule under "Submodules"
- Input and output Arrow schemas
- Any new protocol fields or conventions
