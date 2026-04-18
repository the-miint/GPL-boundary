# Design: Batched Streaming for GPL-boundary

## 1. Motivation

Bowtie2 and SortMeRNA alignment workloads routinely exceed main memory. A
typical Illumina lane produces 200-400M read pairs; at ~300 bytes per paired
read, that is 60-120 GB of sequence data before quality strings. The current
GPL-boundary execution model loads all input into a single shared memory
segment, processes it in one shot, and exits. This does not scale.

Both tools already support a **create / run\* / destroy** lifecycle — the
expensive operation is loading the reference index (seconds to minutes for a
human genome), while individual `run()` calls against a loaded context are
cheap. The single-shot model forces miint to either:

- Reload the index per batch (spawning a new process each time), or
- Fit the entire dataset in one shm segment

Neither is acceptable. We need a protocol that keeps the process alive across
batches so the index load is amortized.

## 2. Background

### 2.1 Current architecture

```
DuckDB  ──┐
           │  C++ (BSD)
         miint ──stdin/stdout──▶ gpl-boundary ──FFI──▶ GPL tool
           │  ◀──POSIX shm────▶              (Rust)   (C/C++)
           │
```

One request in, one response out, process exits. The JSON request names an
input shm segment; the response names zero or more output shm segments. Bulk
data is Arrow IPC in shared memory; JSON carries only metadata.

Key code paths:
- `main.rs`: reads all of stdin, parses one `Request`, calls `dispatch()`,
  writes one `Response`, deregisters output shm, exits.
- `GplTool::execute()`: takes `(config, shm_input)`, returns `Response`.
  No state survives across calls.
- `arrow_ipc::write_batch_to_output_shm()`: serializes a `RecordBatch` to
  `Vec<u8>`, creates an output shm segment of exact size, copies data in.

### 2.2 DuckDB execution model and backpressure

DuckDB table functions produce results via repeated `GetNext()` calls. DuckDB
pulls data when it is ready — there is no push mechanism. This means:

- **DuckDB controls pacing.** If a downstream operator (JOIN, aggregation,
  write to Parquet) is slow, DuckDB simply stops calling `GetNext()`.
- **Table functions cannot produce ahead.** A table function that produces
  output faster than DuckDB consumes it has nowhere to put it — there is no
  output buffer between the function and DuckDB's pipeline.
- **Cancellation is implicit.** When DuckDB is done (LIMIT satisfied, query
  cancelled), it stops calling `GetNext()` and destroys the function state.
  No explicit "stop" signal.

DuckDB can create considerable backpressure. A query like:

```sql
SELECT a.read_id, a.reference, a.mapq, g.gene_name
FROM bowtie2_align('reads.fq', 'hg38') a
JOIN gene_annotations g ON a.reference = g.chrom
  AND a.position BETWEEN g.start AND g.end
WHERE a.mapq >= 30
```

requires DuckDB to probe the join for every alignment batch before requesting
more. The join itself may spill to disk on large annotation tables. During
that time, GPL-boundary's loaded index sits idle.

The implication: **GPL-boundary must tolerate arbitrary pauses between batches
without leaking resources.** And it should not produce output that nobody has
asked for, since buffered output shm consumes physical memory (tmpfs-backed on
Linux).

### 2.3 Tool context lifecycle

Both streaming-capable tools follow the same pattern:

**Bowtie2:**
```c
bt2_align_ctx_t *ctx = bt2_align_create(&config, &err);  // loads .bt2 index
for (int i = 0; i < n_batches; i++) {
    bt2_align_run(ctx, &inputs[i], &outputs[i], NULL);    // cheap per-batch
    bt2_align_output_free(outputs[i]);
}
bt2_align_destroy(ctx);
```

**SortMeRNA:**
```c
smr_ctx_t *ctx = smr_ctx_create(&cfg);      // indexes reference FASTAs
for (int i = 0; i < n_batches; i++) {
    smr_run_seqs(ctx, refs, seqs, &out, &stats);  // cheap per-batch
    smr_output_free(out);
}
smr_ctx_destroy(ctx);
```

**Expensive state** (seconds to minutes): index/reference loading during
`create()`. This state is immutable after creation and reused across all
batches.

**Cheap state** (milliseconds): per-batch input marshaling, alignment, output
marshaling. Independent between batches.

**Error recovery**: a failed `run()` does not invalidate the context. The
caller can retry with different input or continue with the next batch.

**Concurrency**: bowtie2 serializes all API calls behind an internal mutex.
No benefit from concurrent `run()` calls within a single process.

## 3. Requirements

**Must have:**
1. Process stays alive across batches to amortize context creation
2. Bounded memory: at most O(1) shm segments live at any time (not O(batches))
3. DuckDB backpressure respected: no output produced without a corresponding
   request
4. Clean shutdown: EOF on stdin terminates the session; signals clean up shm
5. Error in one batch does not terminate the session
6. Backward compatible: single-shot tools and callers unaware of streaming
   continue to work without changes

**Should have:**
7. Pipeline overlap: GPL-boundary processes batch N while DuckDB consumes
   batch N-1
8. Configurable batch size to tune memory/throughput tradeoff
9. Per-batch statistics in responses (for progress reporting)
10. Cumulative session statistics at shutdown

**Won't have (this design):**
11. Parallel batch processing within a single process (blocked by tool mutexes)
12. Multi-process fan-out for parallelism
13. Adaptive batch sizing (auto-tuning based on memory pressure)

## 4. Design space

### 4.1 Process lifecycle

**Option P1: Respawn per batch (status quo)**

miint spawns a new `gpl-boundary` process for each batch. No protocol changes.

- Pro: Zero changes to gpl-boundary.
- Con: Index reloaded every batch. For a 4 GB bowtie2 index and 1000 batches,
  that is ~1000 index loads (minutes of overhead). Non-starter.

**Option P2: Long-lived process, stdin/stdout request loop**

Process stays alive. miint sends one JSON request per batch on stdin,
gpl-boundary responds per batch on stdout. EOF terminates.

- Pro: Amortizes context creation. Natural fit for create/run\*/destroy.
  Additive protocol change — single-shot tools exit after one cycle.
- Con: Requires framing protocol for multiple JSON messages. Changes to
  main.rs dispatch loop.

**Option P3: Socket-based server**

gpl-boundary listens on a Unix domain socket. miint connects per batch (or
maintains a persistent connection).

- Pro: Decouples process lifetime from miint's. Could serve multiple clients.
- Con: Massive architectural change. Server lifecycle management. Overkill
  for a 1:1 caller/tool relationship.

**Recommendation: P2.** It is the minimum change that achieves the goal. P3
adds operational complexity (who starts the server? who stops it? what about
port conflicts?) for no benefit in the 1:1 model.

### 4.2 Buffering strategies

This is the critical design axis. The question is: **how many batches can be
in flight between miint and GPL-boundary at any time?**

#### B1: Lock-step (zero buffering)

```
miint                          gpl-boundary
  │                                │
  ├─ send request 0 ──────────▶   │
  │                                ├─ process batch 0
  │          ◀──────────── response 0 ─┤
  ├─ return to DuckDB              │
  │  [DuckDB processes...]         │  ← idle
  ├─ send request 1 ──────────▶   │
  │                                ├─ process batch 1
  ...
```

- In flight: 1 request at a time
- Live shm: 1 input + 1 output (peak)
- Backpressure: inherent — DuckDB rate-limits everything
- Throughput: pipeline bubble while DuckDB processes each batch

**Analysis:** Simple and correct. The pipeline bubble (GPL-boundary idle while
DuckDB processes) is the main cost. For CPU-bound alignment, the bubble is the
ratio of DuckDB processing time to alignment time. If alignment takes 10s and
DuckDB processing takes 0.5s, throughput loss is ~5%. If DuckDB is doing heavy
joins and takes 5s, throughput loss is ~33%.

#### B2: Send-ahead (one-deep input buffer)

```
miint                          gpl-boundary
  │                                │
  ├─ send request 0 ──────────▶   │
  │                                ├─ process batch 0
  │          ◀──────────── response 0 ─┤
  ├─ send request 1 ──────────▶   │  ← sent before returning to DuckDB
  ├─ return to DuckDB             ├─ process batch 1 (overlaps DuckDB)
  │  [DuckDB processes...]         │
  │          ◀──────────── response 1 ─┤
  ├─ send request 2 ──────────▶   │
  ├─ return to DuckDB             ├─ process batch 2
  ...
```

- In flight: 1 outstanding response + 1 request being processed
- Live shm: 2 input + 1 output (peak) — the "current" input and the
  "next" input exist simultaneously, plus the output being returned
- Backpressure: still bounded — miint only sends the next request when it
  reads the current response
- Throughput: overlaps GPL-boundary processing with DuckDB processing

**Analysis:** This is a miint-side optimization, not a protocol change.
GPL-boundary sees the same lock-step sequence of requests; it's just that the
next request arrives sooner because miint pre-staged it. The memory overhead
is one additional input shm segment.

However, this requires miint to **read ahead from its input source** (FASTQ
file or DuckDB pipeline) before DuckDB has asked for the next chunk. This is
straightforward when reading from files but requires careful design when the
input comes from another DuckDB operator.

#### B3: Bounded output queue

```
miint                          gpl-boundary
  │                                │
  ├─ send request 0 ──────────▶   │
  ├─ send request 1 ──────────▶   ├─ process batch 0
  ├─ send request 2 ──────────▶   │  (3 requests queued)
  │                                ├─ response 0
  │                                ├─ process batch 1
  │          ◀──────────── response 0 ─┤  (miint reads when DuckDB pulls)
  │                                ├─ response 1
  │                                ├─ process batch 2
  │          ◀──────────── response 1 ─┤
  ...
```

- In flight: up to K requests, up to K output shm segments
- Live shm: K input + K output segments
- Backpressure: miint limits outstanding requests to K; stops sending when K
  unread responses are buffered
- Throughput: GPL-boundary never idles (up to K batches ahead)

**Analysis:** Maximum throughput — GPL-boundary stays busy as long as input is
available. But:

1. **Memory cost is O(K).** Each output shm segment is backed by physical
   memory. With K=4 and 200 MB per output batch, that's 800 MB of output
   buffered in tmpfs, plus 800 MB of input. This directly competes with
   DuckDB's own buffer pool.
2. **Defeats DuckDB's backpressure.** The whole point of DuckDB's pull model
   is to avoid producing data nobody has asked for. Buffering K outputs ahead
   means GPL-boundary has done work that DuckDB may not need (e.g., LIMIT
   clause satisfied after 2 batches, but 4 were already processed).
3. **Wasted work on cancellation.** If the query is cancelled, up to K-1
   batches of alignment work are thrown away.
4. **Complexity.** miint must track outstanding requests, manage multiple
   live shm segments, and implement explicit flow control.

#### B4: Async pipeline with backpressure signaling

A variant of B3 where GPL-boundary monitors a backpressure signal (e.g., a
semaphore or a control message from miint) to pause processing when miint's
output buffer is full.

**Analysis:** All the complexity of B3 plus IPC-based flow control. This is
the right design for high-throughput streaming systems (Kafka, gRPC streams)
but oversized for a 1:1 process-boundary protocol where the bottleneck is
CPU-bound alignment, not I/O.

#### Recommendation: B1 (lock-step) as protocol, B2 (send-ahead) as miint optimization

The lock-step protocol provides the correctness guarantees we need:
- Bounded memory (1 input + 1 output at any moment)
- DuckDB backpressure propagates naturally
- No wasted work on cancellation
- Simple error recovery (one batch at a time)

The send-ahead optimization (B2) is a **miint implementation detail** that does
not change the GPL-boundary protocol. miint simply sends the next request
immediately after reading the current response, before returning data to
DuckDB. This overlaps alignment with DuckDB processing and eliminates most
pipeline bubbles.

If profiling shows that the pipeline bubble is a significant bottleneck (i.e.,
DuckDB processing time is comparable to alignment time), we can revisit B3
with a small bounded queue (K=2). But this should be driven by measurement,
not speculation.

### 4.3 Stream termination

**Option T1: Explicit "done" message**

miint sends `{"done": true}` to signal end-of-stream. gpl-boundary reads it,
destroys context, exits.

- Con: If miint crashes before sending "done", gpl-boundary hangs forever
  waiting for input. Requires a read timeout as fallback.
- Con: Another message type to parse and handle.
- Con: Redundant — the same information is conveyed by closing stdin.

**Option T2: EOF on stdin**

miint closes the write end of its stdin pipe. gpl-boundary's next read returns
EOF. It destroys the context and exits.

- Pro: Standard Unix convention. Works even if miint is killed (the OS closes
  the fd).
- Pro: No special message type. No timeout needed.
- Pro: SIGKILL of miint closes the pipe — gpl-boundary gets EOF or SIGPIPE
  on next I/O.
- Con: None significant.

**Recommendation: T2 (EOF).** It is simpler, more robust, and handles the
miint-crash case correctly without explicit timeouts.

### 4.4 Session statistics

When EOF is received and the session ends, gpl-boundary can optionally write
a final JSON line to stdout with cumulative statistics before exiting:

```json
{"session_complete": true, "batches_processed": 42, "total_reads": 84000000, ...}
```

miint can choose to read or ignore this. It is not a response to a request —
it is a session summary emitted on clean shutdown.

This is deferred to implementation; the protocol supports it (miint reads
until EOF on stdout) but does not require it.

## 5. Recommended design

### 5.1 Overview

```
 miint                          gpl-boundary
   │                                │
   │  ┌─ Request ──────────────▶    │
   │  │  {tool, config,             │
   │  │   shm_input, stream:true}   │
   │  │                             ├─ create context (load index)
   │  │                             ├─ process batch 0
   │  │  ◀──────── Response ────┐   │
   │  │   {success, shm_outputs}│   │
   │  │                         │   │
   │  │  (read output shm,     │   │
   │  │   unlink input+output)  │   │
   │  │                         │   │
   │  ├─ BatchRequest ─────────▶│   │
   │  │  {shm_input}            │   │
   │  │                         │   ├─ process batch 1
   │  │  ◀──────── Response ────┘   │
   │  │                             │
   │  │  ... repeat ...             │
   │  │                             │
   │  ├─ [close stdin / EOF] ──▶    │
   │  │                             ├─ destroy context
   │  │                             ├─ exit(0)
   │  └────────────────────────     │
```

### 5.2 Protocol specification

#### Message framing

Newline-delimited JSON (NDJSON). One JSON object per line on both stdin and
stdout. This matches the existing behavior (serde_json::to_string produces
single-line output) and requires only that the reader switches from "read all
stdin" to "read one line."

#### First message: Request

```json
{
  "tool": "bowtie2-align",
  "config": {"index_path": "/ref/hg38", "threads": 8, ...},
  "shm_input": "/miint-input-0",
  "stream": true
}
```

Same as existing `Request` with an additional optional `stream` field.
When `stream` is absent or false, the request is single-shot (existing
behavior).

#### Subsequent messages: BatchRequest

```json
{"shm_input": "/miint-input-1"}
```

Only the `shm_input` field. Tool name and config are fixed for the session
(they are properties of the context, which was created from the first
request). Sending tool/config again is unnecessary and introduces a validation
burden.

#### Response (per batch)

```json
{
  "success": true,
  "schema_version": 1,
  "shm_outputs": [
    {"name": "/gb-1234-0-alignments", "label": "alignments", "size": 52428800}
  ],
  "result": {"n_reads": 100000, "n_aligned": 87432, "batch_index": 0}
}
```

Identical to existing `Response`. The `batch_index` in `result` is optional
metadata indicating which batch this response corresponds to. Batch indices
are 0-based and increment monotonically.

#### Error response

```json
{
  "success": false,
  "error": "Invalid FASTQ: read at index 4217 has empty sequence",
  "result": {"batch_index": 3}
}
```

An error response for one batch does not end the session. The context remains
valid. miint decides whether to continue (send next batch) or abort (close
stdin).

If the error is fatal (e.g., out of memory, context corrupted), the response
should indicate this:

```json
{
  "success": false,
  "error": "Context invalidated: out of memory during alignment",
  "fatal": true
}
```

When `fatal` is true, gpl-boundary destroys the context and exits regardless
of further input. miint should close stdin and read the exit status.

#### Termination

miint closes its stdin pipe. gpl-boundary reads EOF, destroys the context,
and exits with code 0 (if all batches succeeded) or 1 (if any batch failed).

#### Single-shot fallback

If a tool does not support streaming, the `stream` flag is ignored.
gpl-boundary processes the one batch and exits, exactly as today. miint
detects this by seeing EOF on gpl-boundary's stdout after the first response.

If a tool supports streaming but the request does not set `stream: true`,
gpl-boundary processes one batch and exits. No context is kept alive.

### 5.3 Shared memory lifecycle in streaming mode

The shm lifecycle per batch is identical to single-shot mode:

1. miint creates input shm, writes Arrow IPC
2. miint sends request (or batch request) naming the input shm
3. gpl-boundary opens input shm (read-only), processes it
4. gpl-boundary creates output shm, writes Arrow IPC, detaches
5. gpl-boundary sends response naming the output shm
6. miint reads response, opens output shm, reads data
7. miint unlinks both input and output shm

**Memory bound:** At steady state, exactly 2 shm segments exist: the current
batch's input and the current batch's output. After miint reads the output and
unlinks both, the count drops to 0 until the next batch.

With send-ahead (B2), the peak is 3 segments: current output (being read by
miint), next input (pre-staged), and possibly the current input (not yet
unlinked). This is still O(1).

**Cleanup registry:** The registry tracks output shm segments created during
the current batch. After each response is written to stdout, main.rs
deregisters those segments (miint now owns them). The registry is empty
between batches.

If a signal arrives mid-batch, the handler unlinks any registered output
segments. The input segment is owned by miint and not in the registry.

### 5.4 GplTool trait changes

```rust
/// A reusable tool context for batched streaming.
/// Created once per session, used for multiple batches.
pub trait StreamingContext {
    /// Process one batch of input. Same contract as GplTool::execute()
    /// but against a pre-loaded context.
    fn run_batch(&mut self, shm_input: &str) -> Response;
}

pub trait GplTool {
    fn name(&self) -> &str;
    fn version(&self) -> String;
    fn schema_version(&self) -> u32;
    fn describe(&self) -> ToolDescription;

    /// Single-shot execution (existing behavior, unchanged).
    fn execute(&self, config: &serde_json::Value, shm_input: &str) -> Response;

    /// Create a streaming context for batched execution.
    /// Returns None if the tool does not support streaming.
    /// The context holds expensive state (loaded index, reference DB)
    /// and is reused across multiple run_batch() calls.
    fn create_streaming_context(
        &self,
        config: &serde_json::Value,
    ) -> Result<Option<Box<dyn StreamingContext>>, String> {
        Ok(None)  // Default: streaming not supported
    }
}
```

Single-shot tools (fasttree, prodigal) do not implement
`create_streaming_context` and inherit the default `Ok(None)`.

Streaming tools (bowtie2, sortmerna) implement it by:
1. Parsing config (same as current `execute()` preamble)
2. Calling the C `create()` function
3. Returning a context object that holds the C context pointer

The context's `Drop` impl calls the C `destroy()` function.

### 5.5 main.rs dispatch loop

Pseudocode for the new dispatch flow:

```rust
fn main() {
    install_signal_handlers();
    validate_tool_registry();

    // Handle introspection flags (--version, --list-tools, --describe)
    if handle_introspection_flags() { return; }

    let stdin = io::stdin().lock();
    let mut lines = stdin.lines();

    // Read first request
    let first_line = lines.next().unwrap_or(/* error: empty stdin */);
    let request: Request = serde_json::from_str(&first_line)?;
    let tool = find_tool(&request.tool)?;

    if request.stream.unwrap_or(false) {
        // Streaming mode
        match tool.create_streaming_context(&request.config) {
            Ok(Some(mut ctx)) => {
                // First batch
                let response = ctx.run_batch(&request.shm_input);
                write_response(&response);
                deregister_outputs(&response);

                // Subsequent batches
                for line in lines {
                    let batch_req: BatchRequest = serde_json::from_str(&line)?;
                    let response = ctx.run_batch(&batch_req.shm_input);
                    write_response(&response);
                    deregister_outputs(&response);
                }

                // EOF reached — context drops here (calls C destroy())
            }
            Ok(None) => {
                // Tool doesn't support streaming — single-shot fallback
                let response = tool.execute(&request.config, &request.shm_input);
                write_response(&response);
                deregister_outputs(&response);
            }
            Err(e) => {
                write_response(&Response::error(e));
            }
        }
    } else {
        // Single-shot mode (existing behavior)
        let response = tool.execute(&request.config, &request.shm_input);
        write_response(&response);
        deregister_outputs(&response);
    }
}
```

Key points:
- The `for line in lines` loop blocks on stdin. When miint pauses (DuckDB
  backpressure), gpl-boundary blocks on the read. No busy-waiting, no
  timeout, no polling.
- EOF breaks the loop. Context drops. Process exits.
- Fatal errors from `run_batch` should set a flag and break the loop, rather
  than continuing to read more requests.

### 5.6 Signal handling in streaming mode

The existing signal handler is sufficient. It unlinks all registered output
shm segments and re-raises the signal. In streaming mode:

- Between batches, the registry is empty (outputs have been deregistered).
  A signal between batches is a clean shutdown — context drops, process exits.
- During a batch, the registry holds the current batch's output segments.
  A signal mid-batch unlinks them. The input segment is miint's responsibility.

One addition: the context's `Drop` implementation must be safe to call from
the main thread during signal-induced unwinding. Since `Drop` calls the C
`destroy()` function, which is a normal cleanup call (not async-signal-safe),
this is fine for SIGTERM (which triggers normal Rust unwinding via the
re-raise path) but not for signals that abort. This matches the existing
behavior — SIGKILL cannot be handled, and PID-based naming lets miint detect
stale segments.

### 5.7 Batch sizing

Batch sizing is a miint-side concern. GPL-boundary processes whatever it
receives. However, the design should inform miint's choices:

#### Factors

| Factor | Smaller batches | Larger batches |
|--------|----------------|----------------|
| Memory per shm segment | Lower | Higher |
| Per-batch overhead (marshaling, IPC) | Higher fraction | Lower fraction |
| Granularity of backpressure | Finer | Coarser |
| Wasted work on cancellation | Less | More |
| Progress reporting frequency | More frequent | Less frequent |
| DuckDB chunk alignment | Natural fit | Requires aggregation |

#### Guidelines for miint

**Row-count-based batching** is simpler than byte-count-based. Recommended
defaults:

| Dataset scale | Batch size | Rationale |
|--------------|------------|-----------|
| < 100K reads | 1 batch (no streaming) | Overhead of streaming exceeds benefit |
| 100K - 10M reads | 50K - 100K reads/batch | Good balance of throughput and memory |
| > 10M reads | 100K - 500K reads/batch | Larger batches amortize IPC overhead |

For paired-end reads, the batch size refers to read pairs (each pair is one
row in the Arrow input).

**Memory estimate per batch** (paired-end, 150bp reads, Phred+33 quality):
- Input: ~(150 + 150 + 40 + 40 + 20) bytes/read × N reads + Arrow overhead
  ≈ 400 bytes/read × 100K reads ≈ 40 MB
- Output: ~200 bytes/alignment × 100K reads (assuming ~1 alignment/read)
  ≈ 20 MB
- Total shm per batch: ~60 MB
- Peak with send-ahead (B2): ~120 MB (2 batches of shm)

This is well within typical `/dev/shm` capacity (half of RAM) and leaves
plenty of room for DuckDB's buffer pool.

miint should expose a batch size parameter to the user (e.g., `batch_size`
in the table function arguments) with a sensible default. Power users tuning
for their hardware can adjust.

## 6. Error handling

### Per-batch errors

A non-fatal error in one batch (bad input, alignment failure) returns an
error response with `"success": false`. The session continues. miint can:

- **Skip and continue:** Send the next batch, ignoring the failed one.
- **Retry:** Re-send the same input shm (if still available). The context
  is stateless between batches, so retries are safe.
- **Abort:** Close stdin. gpl-boundary exits cleanly.

### Fatal errors

A fatal error (out of memory, context corruption) returns an error response
with `"fatal": true`. gpl-boundary exits after sending the response. miint
should not send further requests.

### Deserialization errors

If gpl-boundary cannot parse a batch request (malformed JSON, missing
`shm_input`), it sends an error response and continues reading. This is a
non-fatal protocol error — the session is not terminated because the context
is not affected.

### Broken pipe

If miint crashes or is killed, gpl-boundary will get an error on the next
stdout write (SIGPIPE or EPIPE). The signal handler cleans up output shm.
The context's `Drop` cleans up the C library state.

If gpl-boundary crashes, miint will get EOF on its stdout pipe and an error
from `waitpid()`. miint detects stale shm segments by PID and cleans them
up.

## 7. Backward compatibility

The design is fully backward compatible:

| Scenario | Behavior |
|----------|----------|
| Old miint, new gpl-boundary | miint sends request without `stream` field. gpl-boundary uses single-shot path. No change. |
| New miint, old gpl-boundary | miint sends `stream: true`. Old gpl-boundary ignores unknown fields (serde default), processes one batch, exits. miint sees EOF on stdout after first response. |
| New miint, new gpl-boundary, non-streaming tool | `create_streaming_context()` returns `None`. Falls back to single-shot. |
| New miint, new gpl-boundary, streaming tool | Full streaming protocol. |

The `Request` struct gains one optional field (`stream: Option<bool>`).
The `BatchRequest` is a new struct used only in streaming mode. No existing
fields are changed or removed.

## 8. miint integration (high-level)

This section is advisory — miint's implementation is its own concern.

### Table function lifecycle

```
Init():
  spawn gpl-boundary process
  send first Request with stream: true
  wait for first Response
  store response in state

GetNext():
  if state has buffered output rows:
    fill DataChunk from buffer
    if buffer exhausted and more input available:
      prepare next input shm
      send BatchRequest
    return
  if no more input and no buffered output:
    close stdin (triggers EOF)
    wait for process exit
    set cardinality to 0 (done)
    return
  read next Response from gpl-boundary stdout
  fill DataChunk from response
  if more input available:
    prepare next input shm
    send BatchRequest  ← send-ahead: overlaps alignment with DuckDB processing
  return

Cleanup():
  if process still alive:
    close stdin
    waitpid
  unlink any remaining shm
```

### Send-ahead implementation

The send-ahead optimization (B2) is implemented by sending the next
`BatchRequest` *before* returning from `GetNext()`. When `GetNext()` is called
again, the response is likely already waiting (or nearly done). This requires:

1. Pre-reading the next batch of input from the FASTQ file (or DuckDB source)
2. Writing it to a new input shm segment
3. Sending the `BatchRequest` on stdin

The pre-read happens after filling the current `DataChunk` but before
returning control to DuckDB. This adds latency to the current `GetNext()` call
(the time to read and write the next input batch) but eliminates the idle gap
where GPL-boundary would otherwise wait for the next request.

If input comes from another DuckDB operator (not a file), send-ahead may
not be possible without buffering input. In that case, miint falls back to
lock-step.

## 9. Implementation plan

### Phase 1: Protocol and dispatch loop

1. Add `stream: Option<bool>` to `Request` in `protocol.rs`
2. Add `BatchRequest` struct to `protocol.rs`
3. Add `StreamingContext` trait and `create_streaming_context()` default to
   `tools/mod.rs`
4. Refactor `main.rs` to read NDJSON lines and implement the streaming
   dispatch loop
5. Add integration tests: streaming request with multiple batches, EOF
   termination, error recovery mid-stream

### Phase 2: Bowtie2 streaming adapter

6. Refactor `bowtie2_align.rs` to separate context creation from batch
   execution (the code is already structured this way — `execute()` calls
   `create()`, `run()`, `destroy()` in sequence)
7. Implement `StreamingContext` for bowtie2: `run_batch()` calls
   `bt2_align_run()` on the held context, marshals output
8. Implement `create_streaming_context()` for bowtie2: parses config,
   calls `bt2_align_create()`, returns context
9. Test: multi-batch streaming against a bowtie2 index

### Phase 3: SortMeRNA streaming adapter

10. Same refactoring as bowtie2, for sortmerna

### Phase 4: miint integration

11. Implement streaming table function in miint (separate repo/effort)
12. End-to-end testing with real datasets

## 10. Future work

**Parallel fan-out.** For tools without global mutexes (or future tools that
support concurrent contexts), miint could spawn N gpl-boundary processes, each
with the same config, and partition input across them. The streaming protocol
works unchanged — each process is an independent stream.

**Adaptive batch sizing.** miint could monitor alignment throughput and output
size per batch, and adjust the batch size dynamically. Smaller batches when
DuckDB is backpressured (less wasted work on cancellation); larger batches
when throughput is the bottleneck.

**Shared index.** For parallel fan-out, loading the same index N times is
wasteful. A shared-memory index that multiple processes can mmap would
eliminate this. This requires changes to the C library APIs and is a
substantial effort.

**Session statistics.** A final JSON line on stdout after the last response,
summarizing cumulative statistics across all batches. Useful for logging and
progress reporting.

**Checkpointing.** For very long-running alignments (billions of reads), the
ability to resume a session from a known batch index after a crash. This
requires miint to track which batches have been successfully processed and
re-send from the last successful batch. The stateless-context property makes
this straightforward — just restart the session and skip processed batches.
