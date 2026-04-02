# GPL-boundary Guidance Documents

Two documents cover the full process of adding a new tool to GPL-boundary.
They address different audiences and different stages of the work.

## [GUIDANCE_API_LIBRARY.md](GUIDANCE_API_LIBRARY.md) -- C/C++ Library API

**Audience**: Developers of the C/C++ bioinformatics tool being integrated.

**Intent**: Defines the C API contract that a submodule library must satisfy
before integration can begin. Covers config struct design (`struct_size`,
field types, defaults), context lifecycle (create/destroy, no global state),
output layout (SOA preferred, single backing allocation), error reporting
(error codes, `strerror`, `last_error`, no `exit`/`abort`), build constraints
(`NO_MAIN` guard, no external dependencies), determinism (seed parameter),
and the data type mapping between C types and Arrow column types.

Use this document when designing or reviewing a C library's public API for
compatibility with GPL-boundary.

## [GUIDANCE_INTEGRATE.md](GUIDANCE_INTEGRATE.md) -- Rust Integration Steps

**Audience**: GPL-boundary maintainers adding a new tool to the repository.

**Intent**: Step-by-step technical guide for wiring a conforming C library
into the Rust project. Covers the five files to touch (`.gitmodules`,
`build.rs`, `src/tools/<tool>.rs`, `src/tools/mod.rs`, submodule checkout),
the `cc::Build` pattern, FFI binding conventions (`#[repr(C)]`, manual not
bindgen), Arrow schema definitions, `GplTool` trait implementation,
`inventory::submit!` auto-registration, required test patterns (roundtrip,
bad shm, ABI size check), and verification commands.

Use this document when you have a C library that already meets the API
contract and you are ready to add it to the repository.
