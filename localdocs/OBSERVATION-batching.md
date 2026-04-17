# Observation: Batched Streaming for Bowtie2 (and GPL-boundary Generally)

## The problem

Bowtie2 alignment workloads will involve read sets larger than main memory.
The current GPL-boundary execution model is single-shot: one JSON request, one
`execute()` call, one response, process exits. All input data must fit in a
single shared memory segment. This does not scale to datasets that exceed
available RAM.

## What the bowtie2 C API supports

The `bt2_align_run()` function can be called **multiple times on the same
context**. The context (`bt2_align_ctx_t`) holds the loaded index and is
expensive to create (index loading). Once created, it accepts repeated
`bt2_input_t` batches and produces independent `bt2_align_output_t` results:

```c
bt2_align_ctx_t *ctx = bt2_align_create(&config, &err);
for (int i = 0; i < n_batches; i++) {
    bt2_align_run(ctx, &inputs[i], &outputs[i], NULL);
    process(outputs[i]);
    bt2_align_output_free(outputs[i]);
}
bt2_align_destroy(ctx);
```

The context survives errors -- a failed `bt2_align_run` does not invalidate
the context. This makes batch-oriented streaming a natural fit for the API.

## What GPL-boundary would need to change

The current architecture assumes a single request/response cycle:

1. miint writes one Arrow IPC batch to `shm_input`
2. miint spawns gpl-boundary, writes one JSON request to stdin
3. gpl-boundary reads input, runs tool, writes output shm, responds on stdout
4. Process exits

To support batched streaming, at least three options exist:

### Option A: Multiple request/response cycles on stdin/stdout

The process stays alive after the first response. miint sends a stream of
JSON requests (one per batch), gpl-boundary responds to each, and miint
sends an explicit "done" message to trigger shutdown.

**Pros**: Amortizes context creation (index loading) across all batches.
Natural fit for the `create/run*/destroy` lifecycle.

**Cons**: Requires protocol changes (framing, "done" signal, error recovery
mid-stream). Changes to `main.rs` dispatch loop. Every tool would need to
support the streaming protocol, even if most tools are single-shot.

### Option B: Multiple shm inputs in one request

The JSON request includes an array of `shm_input` names. gpl-boundary
processes them sequentially against the same context.

**Pros**: Single request/response, simpler protocol. miint pre-stages all
batches in parallel shm segments before spawning the process.

**Cons**: All shm segments must exist simultaneously, which partially defeats
the purpose if the data exceeds RAM. miint must know batch boundaries at
request time.

### Option C: Streaming protocol with chunked I/O

A more fundamental redesign where gpl-boundary reads input incrementally
(e.g., from a pipe or sequential shm segments) and writes output
incrementally. Closer to a Unix pipeline model.

**Pros**: True streaming, constant memory regardless of dataset size.

**Cons**: Largest architectural change. Breaks the clean "one shm in, one shm
out" model. Adds complexity to both miint and gpl-boundary.

## Current status

We implemented bowtie2-align with single-batch semantics (April 2026). The
adapter is already structured around the `create/run/destroy` lifecycle --
`execute()` creates a context, calls `bt2_align_run` once, destroys the
context. Adapting to multiple `run` calls is straightforward once the
protocol supports it.

The decision was made to defer streaming to a separate effort because:

- It is an architectural change to the protocol and `main.rs` that affects
  every tool, not just bowtie2
- The bowtie2 adapter's internal structure (context lifecycle, input
  preparation, output conversion) transfers directly to a batched model
- Getting bowtie2 working end-to-end first validates the FFI integration,
  Arrow schemas, and config mapping independently of the streaming question

## Recommendation

Option A (multiple request/response cycles) is the most natural fit. It
amortizes expensive context creation, works within POSIX shm size limits,
and keeps miint in control of batch pacing. The protocol change is additive
-- existing single-shot tools continue to work with a single request/response
cycle, while streaming-capable tools handle multiple cycles.

Key design points for Option A:

- The JSON request gains an optional `"stream": true` field. When set,
  gpl-boundary keeps the context alive after responding and reads the next
  request from stdin.
- Each batch response includes the output shm for that batch. miint reads
  and unlinks it before sending the next request.
- A `{"done": true}` message signals end-of-stream. gpl-boundary destroys
  the context and exits.
- Error handling: a failed batch returns an error response but the context
  remains valid. miint decides whether to continue or abort.
- Tools that do not support streaming simply ignore the `stream` flag and
  exit after the first response (backward compatible).

This is not bowtie2-specific. SortMeRNA's `smr_run_seqs` could also benefit
from batched execution against a pre-loaded reference index. The streaming
protocol should be designed generically.
