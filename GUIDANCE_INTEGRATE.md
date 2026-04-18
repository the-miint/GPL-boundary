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

You do **not** need to modify `main.rs`, `protocol.rs`, `arrow_ipc.rs`,
`shm.rs`, or `Cargo.toml` (unless the tool requires new Rust dependencies).
The `inventory` crate handles dispatch, introspection, and registry
automatically.

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

Each tool gets its own `cc::Build` with a unique `.compile()` name to prevent
symbol collisions between C libraries.

```rust
fn main() {
    build_fasttree();
    build_newtool();   // add this call
    link_math();
}

fn build_newtool() {
    let dir = PathBuf::from("ext/newtool");

    cc::Build::new()
        .file(dir.join("newtool_core.c"))
        .file(dir.join("newtool_api.c"))
        .include(&dir)
        .define("NEWTOOL_NO_MAIN", None)
        .opt_level(3)
        .flag_if_supported("-finline-functions")
        .flag_if_supported("-funroll-loops")
        .warnings(false)
        .compile("newtool_c");

    println!("cargo:rerun-if-changed=ext/newtool/newtool_core.c");
    println!("cargo:rerun-if-changed=ext/newtool/newtool_api.c");
    println!("cargo:rerun-if-changed=ext/newtool/newtool.h");
}
```

Key points:
- **Separate `cc::Build` per tool** -- never add files to another tool's build
- **`define("NEWTOOL_NO_MAIN", None)`** -- suppresses the submodule's `main()`
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

Define the input schema (what the tool expects from miint) and the output
schema (what the tool produces as Arrow IPC).

```rust
pub fn input_schema() -> Schema {
    Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ])
}

fn output_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Float64, false),
        Field::new("name", DataType::Utf8, true),   // nullable if applicable
    ])
}
```

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
            input_schema: vec![/* FieldDescription for each input column */],
            output_schema: vec![/* FieldDescription for each output column */],
            response_metadata: vec![/* FieldDescription for JSON result fields */],
        }
    }

    fn execute(&self, config: &serde_json::Value, shm_input: &str) -> Response {
        // See below
    }
}
```

**`execute()` follows this pattern:**

1. Read Arrow input: `crate::arrow_ipc::read_batches_from_shm(shm_input)?`
2. Extract columns, validate, convert to C types (`Vec<CString>` etc.)
3. Initialize C config via FFI: `zeroed()` + `config_init()` + apply JSON
   config values
4. Create context: `newtool_create(&config)`, check for null
5. Call the computation: `newtool_run(ctx, ...)`
6. On error: `strerror(rc)` + `last_error(ctx)` → `Response::error(...)`,
   then `destroy(ctx)`
7. On success: convert SOA output → Arrow `RecordBatch` using `output_schema()`
8. Write output: `crate::arrow_ipc::write_batch_to_output_shm(&batch, "label")`
9. Free C output + destroy context
10. Return `Response::ok(result_json, vec![shm_output])`

**Important**: always free C resources (output, context) on every code path,
including errors. The tool must never panic across FFI.

### 3e. SOA → RecordBatch conversion

Write an `unsafe` helper that wraps each C array into an Arrow column:

```rust
unsafe fn soa_to_record_batch(output: &NewToolOutput) -> Result<RecordBatch, String> {
    let n = output.n_items as usize;

    let ids = std::slice::from_raw_parts(output.id, n);
    let scores = std::slice::from_raw_parts(output.score, n);
    let name_ptrs = std::slice::from_raw_parts(output.name, n);

    let names: Vec<Option<String>> = name_ptrs.iter().map(|&p| {
        if p.is_null() { None }
        else { Some(CStr::from_ptr(p).to_string_lossy().into_owned()) }
    }).collect();

    let schema = Arc::new(output_schema());
    RecordBatch::try_new(schema, vec![
        Arc::new(Int32Array::from(ids.to_vec())),
        Arc::new(Float64Array::from(scores.to_vec())),
        Arc::new(StringArray::from(
            names.iter().map(|n| n.as_deref()).collect::<Vec<Option<&str>>>()
        )),
    ]).map_err(|e| format!("Failed to create Arrow RecordBatch: {e}"))
}
```

### 3f. Tests

Three required tests in a `#[cfg(test)] mod tests` block:

**1. Happy-path roundtrip:**

```rust
#[test]
fn test_newtool_roundtrip() {
    let input_name = unique_shm_name("nt-in");
    let batch = make_input_batch(/* test data */);
    let _input_shm = write_arrow_to_shm(&input_name, &batch);

    let tool = NewTool;
    let config = serde_json::json!({ "seed": 42 });
    let response = tool.execute(&config, &input_name);

    assert!(response.success, "Failed: {:?}", response.error);
    assert_eq!(response.shm_outputs.len(), 1);
    assert!(response.shm_outputs[0].name.starts_with("/gb-"));
    assert_eq!(response.shm_outputs[0].label, "your-label");
    assert!(response.shm_outputs[0].size > 0);

    let result = response.result.unwrap();
    // ... assert expected metadata fields ...

    let out_batches = read_arrow_from_shm(&response.shm_outputs[0].name);
    assert_eq!(out_batches.len(), 1);
    // ... assert schema, row count, column values ...

    let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
}
```

Key: keep `_input_shm` alive until after `execute()` returns.

**2. Bad shm name:**

```rust
#[test]
fn test_newtool_bad_input_shm() {
    let tool = NewTool;
    let config = serde_json::json!({});
    let response = tool.execute(&config, "/nonexistent-shm-name");
    assert!(!response.success);
    assert!(response.error.unwrap().contains("Failed to open shm"));
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
make check                        # fmt + clippy + test (full CI check)
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

1. A `<Tool>StreamingContext` struct holding the C context pointer and any
   session-lived state. Implement `Drop` to call the C `destroy()` function.
2. Override `create_streaming_context()` on the `GplTool` impl. Parse config,
   call C `create()`, return the context. Validate streaming is supported
   (e.g., prodigal only supports streaming in meta mode).
3. Implement `StreamingContext::run_batch()`. Read input from shm, call C
   `run()`, convert output, write to output shm, return Response.

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
    let input1 = unique_shm_name("nt-str-1");
    let _shm1 = write_arrow_to_shm(&input1, &make_input_batch(/* data */));
    let resp1 = ctx.run_batch(&input1);
    assert!(resp1.success, "Batch 1 failed: {:?}", resp1.error);
    let _ = SharedMemory::unlink(&resp1.shm_outputs[0].name);

    // Batch 2
    let input2 = unique_shm_name("nt-str-2");
    let _shm2 = write_arrow_to_shm(&input2, &make_input_batch(/* data */));
    let resp2 = ctx.run_batch(&input2);
    assert!(resp2.success, "Batch 2 failed: {:?}", resp2.error);
    let _ = SharedMemory::unlink(&resp2.shm_outputs[0].name);
}
```

## Common mistakes

### Forgetting `cargo:rerun-if-changed` in build.rs

Without these directives, cargo won't recompile the C sources when they
change. List every `.c` and `.h` file used by the build.

### Dropping the input shm handle before execute()

In tests, the `SharedMemory` returned by `write_arrow_to_shm` unlinks the
segment on drop. If you don't bind it to a variable (or bind it to `_`
instead of `_input_shm`), the segment is unlinked immediately and the tool
gets a "Failed to open shm" error.

### Not cleaning up output shm in tests

The tool creates output shm but does not unlink it (that is the caller's
job). Each test must call `SharedMemory::unlink(&output_name)` at the end.

### Struct field order mismatch

The `#[repr(C)]` struct must match the C header field-for-field in order.
The ABI size-check test catches added/removed fields but not reordering.
Manual review against the C header is required when fields change.

### Reusing another tool's cc::Build

Each tool must have its own `cc::Build::new()` chain. Adding files to
another tool's build causes symbol collisions.

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

## Updating CLAUDE.md

After integration, update `CLAUDE.md` to document:
- The new submodule under "Submodules"
- Input and output Arrow schemas
- Any new protocol fields or conventions
