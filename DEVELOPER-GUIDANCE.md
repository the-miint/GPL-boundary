# Developer Guidance: Adding a Program to GPL-boundary

This document is for developers of C/C++ bioinformatics tools who want to
integrate their program with GPL-boundary. It covers C API design decisions
that minimize friction during integration, common pitfalls, and the data
exchange contract.

GPL-boundary wraps your tool as a short-lived child process. Data arrives as
Apache Arrow IPC in POSIX shared memory. Your tool runs, and results go back
the same way. Your C code never touches shared memory or Arrow directly --
the Rust adapter handles that. But the shape of your C API determines how
much Rust glue code is needed and how error-prone the integration is.

The reference integration is FastTree (`ext/fasttree/fasttree.h` and
`src/tools/fasttree.rs`).

## Required C API surface

Your library must expose a public API with this structure:

```c
/* 1. Config initialization */
void tool_config_init(tool_config_t *config);

/* 2. Context lifecycle */
tool_ctx_t *tool_create(const tool_config_t *config);
void tool_destroy(tool_ctx_t *ctx);

/* 3. Core computation */
int tool_run(tool_ctx_t *ctx,
             const char **inputs, int n_inputs,
             tool_output_t **output_out,
             tool_stats_t *stats_out);

/* 4. Output cleanup */
void tool_output_free(tool_output_t *output);

/* 5. Error reporting */
const char *tool_strerror(int error_code);
const char *tool_last_error(const tool_ctx_t *ctx);
```

The names and exact signatures will vary. What matters is the pattern:
initialize config, create context, run, free output, destroy context, and
structured error reporting at every step.

## Config struct design

The config struct is the most important part of your API to get right. The
Rust side creates a `#[repr(C)]` mirror of it and passes it across FFI.

### struct_size must be the first field

```c
typedef struct {
    size_t struct_size;   /* Set by tool_config_init(). DO NOT set manually. */
    int    param_a;
    double param_b;
    /* ... */
} tool_config_t;
```

`tool_config_init()` sets `struct_size = sizeof(tool_config_t)` along with
all default values. The Rust side checks this against its own struct size at
test time. If you add fields in a future version, the Rust adapter detects
the mismatch immediately.

**Why this matters**: without it, a field added to the C struct silently
shifts all subsequent fields. The Rust struct reads garbage from the wrong
offsets. The tool produces wrong results with no error. The size check turns
this into a loud test failure.

New fields must be appended at the end of the struct. Never reorder or
remove existing fields.

### Use int for booleans, not bool

```c
int  fast_mode;       /* nonzero = enable fast mode */
int  verbose;         /* nonzero = enable verbose output */
```

The C `_Bool` and C++ `bool` types have implementation-defined sizes (1 byte
on most platforms, but not guaranteed). `int` is always 4 bytes with
predictable alignment. The Rust side maps this to `c_int`.

### Use explicit-width types for non-trivial fields

```c
int64_t seed;         /* random seed for reproducibility */
```

Plain `long` varies between platforms (4 bytes on 32-bit, 8 bytes on 64-bit
Linux, 4 bytes on 64-bit Windows). Use `<stdint.h>` types (`int32_t`,
`int64_t`, `uint64_t`) for any field where the size matters. The Rust side
maps these to `i32`, `i64`, `u64`.

### Callbacks use void* user_data

```c
int  (*progress_callback)(const char *stage, double frac_done, void *user_data);
void  *progress_user_data;

void (*log_callback)(const char *msg, void *user_data);
void  *log_user_data;
```

This pattern lets the Rust adapter pass a closure context through C without
C knowing anything about it. Always pair a callback function pointer with a
`void *user_data` pointer. The Rust side typically sets both to null (GPL-
boundary discards progress and log output by default).

### Default values must produce correct results

`tool_config_init()` must set defaults such that calling `tool_create()` and
`tool_run()` immediately after initialization produces a valid result. No
field should require the caller to set it for basic operation.

## Context lifecycle

### Opaque context pointer

```c
typedef struct tool_ctx tool_ctx_t;   /* Forward declaration only */

tool_ctx_t *tool_create(const tool_config_t *config);
void        tool_destroy(tool_ctx_t *ctx);
```

The context struct definition lives in your `.c` file, not the public header.
The caller sees only a pointer. This lets you change internal fields without
breaking ABI.

### No global state

Your library must not use file-scope or `static` variables for computation
state. All state belongs in the context. This is the most common source of
integration problems.

Global state causes:
- Non-deterministic results when the caller creates multiple contexts
- Thread-safety violations (GPL-boundary is single-threaded today but this
  is not guaranteed)
- Stale state from a previous run leaking into the next run

Global constants (lookup tables, string literals) are fine. Mutable global
state is not.

### Context reuse after errors

If `tool_run()` returns an error code, the context should reset internally
so the caller can call `tool_run()` again with different input. Document
this behavior.

### Return NULL on allocation failure

`tool_create()` should return `NULL` if memory allocation fails. The Rust
side checks for null and returns a structured error. Do not call `exit()` or
`abort()` on OOM.

## Output struct design

This is where API design has the largest impact on integration effort.

### Prefer Structure of Arrays (SOA) over Array of Structures (AoS)

GPL-boundary exchanges data as Apache Arrow columnar format. Each column is
a contiguous array of a single type. If your output is SOA, the Rust adapter
can wrap each array directly into an Arrow column with minimal copying.

**SOA (ideal):**
```c
typedef struct {
    int     n_items;
    int    *id;            /* contiguous array, length n_items */
    double *score;         /* contiguous array, length n_items */
    int    *parent;        /* contiguous array, length n_items */
    const char **name;     /* array of string pointers, length n_items */
    /* Backing store */
    char   *_name_buf;     /* single buffer holding all names */
    void   *_base;         /* single allocation backing everything */
} tool_output_soa_t;
```

The Rust adapter wraps each array into an Arrow column:
```rust
let score = std::slice::from_raw_parts(output.score, n);
Arc::new(Float64Array::from(score.to_vec()))
```

**AoS (avoid):**
```c
typedef struct {
    int id;
    double score;
    int parent;
    const char *name;
} tool_item_t;

typedef struct {
    int n_items;
    tool_item_t *items;   /* array of structs */
} tool_output_t;
```

The Rust adapter must transpose the entire array:
```rust
let scores: Vec<f64> = (0..n).map(|i| (*items.add(i)).score).collect();
let parents: Vec<i32> = (0..n).map(|i| (*items.add(i)).parent).collect();
// ... repeat for every field
```

This is more code, more error-prone, and slower (poor cache locality during
the transpose). SOA eliminates all of this.

### Single backing allocation

Allocate all output arrays from a single `malloc` block. This has two
benefits:
- One `free` call releases everything (no risk of partial leaks)
- Sub-arrays are 16-byte aligned within the block (safe for SIMD and
  double/pointer access on all platforms)

The FastTree SOA struct uses a `_base` pointer for this:
```c
void *_base;   /* single allocation backing all arrays */
```

And a single free function:
```c
void tool_output_free(tool_output_soa_t *output);
```

### Nullable string arrays

If some entries in a string column can be absent (e.g., internal tree nodes
have no name), use `NULL` pointers:

```c
const char **name;   /* NULL for entries with no name */
```

The Rust adapter maps `NULL` to Arrow's null representation:
```rust
if p.is_null() { None } else { Some(CStr::from_ptr(p).to_string_lossy().into_owned()) }
```

Document which pointer fields can be `NULL` and under what conditions.

### Statistics as a separate, value-only struct

Lightweight computation statistics (log-likelihood, iteration counts, timing)
should be returned in a separate struct with no pointers:

```c
typedef struct {
    int    n_iterations;
    double log_likelihood;
    double elapsed_seconds;
} tool_stats_t;
```

This struct contains only scalar values and needs no free function. The Rust
adapter copies it directly and includes it in the JSON response metadata
(not in Arrow -- it is too small to justify columnar encoding).

## Error reporting

### Error codes

Define negative integer error codes. Zero means success.

```c
#define TOOL_OK                  0
#define TOOL_ERR_NOMEM          -1
#define TOOL_ERR_INVALID_INPUT  -2
#define TOOL_ERR_INTERNAL       -3
```

### Two-level error messages

```c
/* Category string for an error code (static, never freed). */
const char *tool_strerror(int error_code);

/* Detailed message from the last failed operation on this context.
   Valid until the next API call on the same context, or until
   tool_destroy(). */
const char *tool_last_error(const tool_ctx_t *ctx);
```

`tool_strerror()` returns a static string like "Invalid input" -- safe to
call from any thread, never freed. `tool_last_error()` returns a detailed
message owned by the context, like "Sequence 'SeqA' has length 20 but
expected 30 (alignment length mismatch)".

The Rust adapter combines both:
```rust
let code_msg = CStr::from_ptr(tool_strerror(rc)).to_string_lossy();
let detail = CStr::from_ptr(tool_last_error(ctx)).to_string_lossy();
Response::error(format!("{code_msg}: {detail}"))
```

### Never call exit() or abort()

Your library runs inside a Rust process that manages shared memory cleanup.
If you call `exit()`, the cleanup code does not run and shared memory
segments leak. Return an error code and let the caller decide how to exit.

This includes assertion failures. Replace `assert()` with error returns in
library code.

### Never write to stdout or stderr

The JSON protocol uses stdout. Any output your library writes to stdout
corrupts the protocol and causes parse failures on the caller side. Use the
log callback instead:

```c
if (config->log_callback) {
    config->log_callback("Starting NJ phase", config->log_user_data);
}
```

The Rust adapter currently discards log output. Future versions may route it
to stderr.

## Build integration

Your source files are compiled into a static library by `build.rs` using the
`cc` crate. No system-level installation is required.

### Provide a NO_MAIN define guard

If your codebase contains a `main()` function (e.g., a CLI entry point),
guard it:

```c
#ifndef TOOL_NO_MAIN
int main(int argc, char **argv) {
    /* ... */
}
#endif
```

`build.rs` defines `TOOL_NO_MAIN` during compilation. Without this, you get
a linker error for duplicate `main` symbols.

### Separate API from core

Structure your code so that the public API and the algorithmic core are in
separate source files:

```
ext/toolname/
  toolname.h          # Public API header
  toolname_api.c      # API implementation (config_init, create, run, etc.)
  toolname_core.c     # Algorithm implementation
```

This lets `build.rs` compile only the files it needs, and makes it clear
which symbols are public.

### Minimize external dependencies

The build system compiles your C code with the `cc` crate. It supports
standard C libraries (`-lm`, `-lpthread`) but not arbitrary system
libraries. If your code depends on zlib, BLAS, or other external libraries,
discuss this with the GPL-boundary maintainers before starting integration.

Math functions (`sqrt`, `log`, `exp`, etc.) are available -- `build.rs`
links `-lm` on Linux.

## Determinism

Accept a random seed in the config struct. Use it consistently for all
pseudo-random decisions. Two runs with the same seed and same input must
produce bit-identical output.

```c
int64_t seed;   /* default: set by config_init */
```

Determinism is essential for:
- Reproducible test results across CI environments
- Users who need to verify that results are stable

If your algorithm is inherently non-deterministic (e.g., depends on thread
scheduling), document this and provide a single-threaded deterministic mode.

## Threading

Do not create threads unless the caller opts in via a config parameter:

```c
int n_threads;   /* 0 = single-threaded; >0 = use this many threads */
```

GPL-boundary spawns your tool as a child process. Thread creation inside
the library is allowed but must be controllable. The default must be
single-threaded.

If you use OpenMP, guard it:
```c
#ifdef _OPENMP
    if (config->n_threads > 0)
        omp_set_num_threads(config->n_threads);
#endif
```

## Memory allocation

For most tools, using standard `malloc`/`free` internally is fine. If your
tool benefits from custom allocation (arena allocators, memory pools), expose
it through the config struct:

```c
void *(*alloc_fn)(size_t size, void *user_data);   /* must return 16-byte aligned */
void  (*free_fn)(void *ptr, void *user_data);
void  *alloc_user_data;
```

When `alloc_fn` is `NULL`, use `malloc`. This is optional -- most tools do
not need it.

## Data types and Arrow mapping

Your output struct fields map to Arrow column types. The Rust adapter handles
the conversion, but your choice of C types determines which Arrow types are
used and how much glue code is needed.

| C type | Arrow type | Notes |
|--------|-----------|-------|
| `int` / `int32_t` | `Int32` | Node indices, counts, flags |
| `int64_t` | `Int64` | Large counts, timestamps |
| `double` | `Float64` | Scores, distances, likelihoods |
| `float` | `Float32` | Use only if precision is genuinely sufficient |
| `int` (0/nonzero) | `Boolean` | `is_leaf`, `is_valid`, etc. |
| `const char *` | `Utf8` | Names, labels; NULL maps to Arrow null |
| `const char **` | `Utf8` (nullable) | Array of strings, some may be NULL |

Avoid:
- **Bitfields**: no portable way to access from Rust
- **Unions**: Rust has no equivalent for C unions in `#[repr(C)]`
- **Nested struct pointers**: each level of indirection requires unsafe
  dereferencing in Rust; flatten into SOA arrays instead
- **Variable-length inline arrays** (`char name[256]`): fixed-size arrays
  waste memory and truncate long values; use `const char *` instead

## Input format

Your `tool_run()` function receives input as arrays of C strings. The Rust
adapter reads Arrow IPC from shared memory, extracts the relevant columns,
converts them to `Vec<CString>`, and passes raw pointer arrays to your
function:

```c
int tool_run(tool_ctx_t *ctx,
             const char **names,    /* array of null-terminated strings */
             const char **seqs,     /* array of null-terminated strings */
             int n_items,
             int item_length,       /* if applicable (e.g., alignment width) */
             tool_output_t **out,
             tool_stats_t *stats);
```

The string pointers are valid for the duration of the `tool_run()` call. Do
not store them beyond the call (copy if needed). The Rust side owns the
`CString` backing memory and frees it after the call returns.

If your tool needs non-string input (numeric arrays, matrices), discuss the
input Arrow schema with the GPL-boundary maintainers. The Rust adapter can
extract any Arrow column type.

## Checklist

Before starting integration, verify your C API against this list:

- [ ] `struct_size` is the first field in the config struct
- [ ] `config_init()` sets `struct_size` and all defaults
- [ ] No global mutable state
- [ ] No `main()` (or guarded with `#ifndef TOOL_NO_MAIN`)
- [ ] No `exit()`, `abort()`, or `assert()` in library code
- [ ] No writes to stdout or stderr (use log callback)
- [ ] Error codes returned from all fallible functions
- [ ] `strerror()` for error code categories
- [ ] `last_error()` for detailed context-specific messages
- [ ] Output in SOA layout (preferred) or documented AoS
- [ ] Single backing allocation for output (one `free` call)
- [ ] Nullable pointers documented (which fields can be `NULL` and when)
- [ ] Stats in a separate pointer-free struct
- [ ] Random seed parameter for determinism
- [ ] Single-threaded by default (threading opt-in via config)
- [ ] No external library dependencies beyond libc/libm (or discussed with
      maintainers)
- [ ] `create()` returns `NULL` on allocation failure (no `exit`)
- [ ] Context reusable after error
- [ ] Separate API and core source files
- [ ] Public header with `extern "C"` guards for C++ compatibility
