use serde::{Deserialize, Serialize};

/// Top-level NDJSON message on stdin, in its final-shape envelope so the
/// protocol is cut once and Phase 4's per-(tool, config) routing slots in
/// without another break.
///
/// Wire form (one JSON object per line):
/// - `{"init": {...}}` — required first message; sets session-wide knobs
/// - `{"tool": "...", "config": {...}, "shm_input": "...", "shm_input_size": ..., "batch_id": ...}`
///   — a single batch request
/// - `{"shutdown": true}` — graceful shutdown sentinel
///
/// Handled with serde's `untagged` representation: the three variants have
/// disjoint required fields (`init` / `tool` / `shutdown`), so each line
/// matches exactly one variant.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ControlMessage {
    Init { init: InitParams },
    Shutdown { shutdown: bool },
    Batch(BatchRequest),
}

/// Session-wide knobs set by the init message. All fields optional; any
/// missing field uses its default. The pool-related fields apply to
/// subprocess workers (currently only `bowtie2-align`); they have no
/// effect on in-process tools.
#[derive(Debug, Default, Deserialize)]
pub struct InitParams {
    /// Auto-shutdown after this many ms of stdin silence. `0` disables.
    /// Default 60_000.
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    /// Phase 4: total subprocess worker budget across all fingerprints.
    #[serde(default)]
    pub max_workers: Option<usize>,
    /// Phase 4: per-fingerprint worker cap.
    #[serde(default)]
    pub workers_per_fingerprint: Option<usize>,
    /// Phase 4: idle workers to keep warm.
    #[serde(default)]
    pub max_idle_workers: Option<usize>,
    /// Phase 4: per-worker idle deadline (ms).
    #[serde(default)]
    pub worker_idle_ms: Option<u64>,
}

/// One batch request. Every batch carries its own `tool` + `config` so the
/// dispatcher can route by `(tool, config_fingerprint)` without a further
/// protocol change.
///
/// `Serialize` is needed by `SubprocessWorker` to forward batches over a
/// pipe to the child gpl-boundary worker process — the child uses the same
/// type to deserialize.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BatchRequest {
    pub tool: String,
    #[serde(default)]
    pub config: serde_json::Value,
    pub shm_input: String,
    /// Exact byte count of the Arrow IPC stream in `shm_input`. The reader
    /// must mmap exactly this many bytes — `fstat` is not a reliable size
    /// source on Darwin POSIX shm. miint, which creates the input segment,
    /// already knows this value.
    pub shm_input_size: usize,
    /// Echoed back on the matching response so out-of-order completions
    /// can be correlated.
    #[serde(default)]
    pub batch_id: Option<u64>,
    /// Opt-in per-batch worker telemetry. When `true`, a subprocess worker
    /// (bowtie2-align today) self-reports `getrusage` deltas and friends in
    /// the response's `metrics` field. Defaults `false` — non-opted batches
    /// pay nothing (no extra syscalls). Ignored by in-process tools, whose
    /// shared-process `getrusage` would mix concurrent batches.
    #[serde(default)]
    pub metrics: bool,
}

/// Per-batch worker telemetry, populated only when the request opts in with
/// `"metrics": true` AND the batch ran in a subprocess worker (bowtie2-align).
/// Diagnostic only; never load-bearing.
///
/// Counter fields are per-batch deltas (after − before, bracketing the tool
/// call). `worker_rss_kb` is an absolute peak (a high-water mark has no
/// meaningful delta). Linux-only `/proc`-derived fields are `None` elsewhere
/// and omitted from JSON. The daemon-clock fields (`t_dispatch_ms`,
/// `t_worker_ms`) are filled in by the parent's reader thread, not the worker.
///
/// Field names of the worker/daemon set match the duckdb-miint
/// `REQUEST-gpl-boundary-batch-metrics` contract; fields beyond that request
/// (`worker_oublock`, `worker_nvcsw`, `worker_nivcsw`, `worker_rss_current_kb`,
/// `worker_read_bytes`, `worker_write_bytes`) are additive — miint ignores
/// unknown keys.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchMetrics {
    /// Wall-clock time the worker spent on this batch (ms).
    pub worker_wall_ms: u64,
    /// `getrusage` `ru_utime` delta (ms) — user CPU.
    pub worker_user_ms: u64,
    /// `getrusage` `ru_stime` delta (ms) — system CPU.
    pub worker_sys_ms: u64,
    /// `ru_majflt` delta — major (disk-backed) page faults. The headline
    /// compute-vs-cold-I/O signal.
    pub worker_majflt: u64,
    /// `ru_minflt` delta — minor (no-I/O) page faults.
    pub worker_minflt: u64,
    /// `ru_inblock` delta — 512B block reads charged to the process. Always
    /// 0 on macOS (Darwin does not track it).
    pub worker_inblock: u64,
    /// `ru_oublock` delta — 512B block writes. Always 0 on macOS.
    pub worker_oublock: u64,
    /// `ru_nvcsw` delta — voluntary context switches (a blocking-I/O signal).
    pub worker_nvcsw: u64,
    /// `ru_nivcsw` delta — involuntary context switches (CPU-contention signal).
    pub worker_nivcsw: u64,
    /// `ru_maxrss` — peak resident set size in KB (absolute, not a delta).
    pub worker_rss_kb: u64,
    /// `false` on the first batch after a worker spawns (cold index); `true`
    /// once the worker has processed an earlier batch (warm/reused index).
    pub worker_reused: bool,

    /// Current RSS in KB (`/proc/self/statm`). Linux only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_rss_current_kb: Option<u64>,
    /// `/proc/self/io` `read_bytes` delta — bytes fetched from the storage
    /// layer (post-page-cache). Linux only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_read_bytes: Option<u64>,
    /// `/proc/self/io` `write_bytes` delta. Linux only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_write_bytes: Option<u64>,

    /// Daemon-observed dispatch queue time (batch received → written to the
    /// worker's stdin), ms. Set by the parent's reader thread; `None` until
    /// then (and on the worker's own copy). Populated in Phase 3.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub t_dispatch_ms: Option<u64>,
    /// Daemon-observed worker portion (stdin write → response read), ms.
    /// Cross-checks `worker_wall_ms`. Set by the parent's reader thread;
    /// `None` until then. Populated in Phase 3.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub t_worker_ms: Option<u64>,
}

/// One entry of the init reply's tool-registry advertisement. Lets miint
/// query "is bowtie2-align registered?" and "what schema does it emit?"
/// at session-init time without a trial Submit. Added in protocol v2.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ToolEntry {
    pub name: String,
    pub schema_version: u32,
}

/// A single shared memory output segment.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShmOutput {
    pub name: String,
    pub label: String,
    pub size: usize,
}

impl ShmOutput {
    pub fn new(name: impl Into<String>, label: impl Into<String>, size: usize) -> Self {
        Self {
            name: name.into(),
            label: label.into(),
            size,
        }
    }
}

/// Response sent on stdout. Bulk results travel in `shm_outputs`; JSON
/// carries only metadata.
///
/// `Deserialize` is needed by `SubprocessWorker`'s reader thread, which
/// parses NDJSON lines from a child gpl-boundary worker process and
/// forwards them onto the coordinator's response channel.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Response {
    pub success: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Per-tool output schema version; bumped on any breaking schema change.
    /// Set by `dispatch` / `run_batch` after the tool succeeds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u32>,

    /// Top-level protocol version. Set on the init reply only — separate
    /// from per-tool `schema_version`. Lets miint detect a wire-format drift
    /// before sending any batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<u32>,

    /// Echoed from the matching `BatchRequest.batch_id`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<u64>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shm_outputs: Vec<ShmOutput>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,

    /// Tool registry advertisement on the init reply. Empty (and omitted)
    /// on every other response. Lets miint do capability detection +
    /// schema-version drift checks at session-init time without a trial
    /// Submit.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolEntry>,

    /// Opt-in per-batch worker telemetry. Present only when the request set
    /// `metrics: true` and the batch ran in a subprocess worker; omitted
    /// otherwise. Purely additive — clients that don't ask never see it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<BatchMetrics>,
}

impl Response {
    pub fn ok(result: serde_json::Value, shm_outputs: Vec<ShmOutput>) -> Self {
        Self {
            success: true,
            shm_outputs,
            result: Some(result),
            ..Self::default()
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            error: Some(msg.into()),
            ..Self::default()
        }
    }

    /// Reply to a successful init message — carries the protocol version
    /// plus the tool registry (added in protocol v2 so miint can do
    /// capability detection at handshake time). `PROTOCOL_VERSION` lives
    /// in `main.rs`; bump there on any wire-envelope change.
    pub fn init_ok(protocol_version: u32, tools: Vec<ToolEntry>) -> Self {
        Self {
            success: true,
            protocol_version: Some(protocol_version),
            tools,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_single_output_json() {
        let resp = Response::ok(
            serde_json::json!({"count": 42}),
            vec![ShmOutput::new("/out1", "tree", 100)],
        );
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert!(json["success"].as_bool().unwrap());
        assert!(json.get("schema_version").is_none());
        assert!(json.get("protocol_version").is_none());
        assert!(json.get("batch_id").is_none());
        let outputs = json["shm_outputs"].as_array().unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0]["name"], "/out1");
        assert_eq!(json["result"]["count"], 42);
    }

    #[test]
    fn test_response_empty_outputs_json() {
        let resp = Response::ok(serde_json::json!({"msg": "metadata only"}), vec![]);
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert!(json["success"].as_bool().unwrap());
        assert!(json.get("shm_outputs").is_none());
    }

    #[test]
    fn test_response_error_omits_outputs() {
        let resp = Response::error("something broke");
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert!(!json["success"].as_bool().unwrap());
        assert_eq!(json["error"], "something broke");
        assert!(json.get("shm_outputs").is_none());
        assert!(json.get("schema_version").is_none());
        assert!(json.get("result").is_none());
    }

    #[test]
    fn test_response_init_ok_no_tools() {
        // Empty registry advertisement: `tools` should be omitted from JSON.
        let resp = Response::init_ok(3, vec![]);
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert!(json["success"].as_bool().unwrap());
        assert_eq!(json["protocol_version"].as_u64().unwrap(), 3);
        assert!(json.get("batch_id").is_none());
        assert!(json.get("schema_version").is_none());
        assert!(json.get("shm_outputs").is_none());
        assert!(json.get("tools").is_none());
    }

    #[test]
    fn test_response_init_ok_with_tools() {
        let resp = Response::init_ok(
            3,
            vec![
                ToolEntry {
                    name: "bowtie2-align".into(),
                    schema_version: 2,
                },
                ToolEntry {
                    name: "fasttree".into(),
                    schema_version: 2,
                },
            ],
        );
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert!(json["success"].as_bool().unwrap());
        assert_eq!(json["protocol_version"].as_u64().unwrap(), 3);
        let tools = json["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0]["name"], "bowtie2-align");
        assert_eq!(tools[0]["schema_version"].as_u64().unwrap(), 2);
        assert_eq!(tools[1]["name"], "fasttree");
    }

    #[test]
    fn test_response_init_ok_round_trip() {
        // miint will deserialize the init reply on its side; round-trip
        // through serde to verify ToolEntry's serde derives are symmetric.
        let resp = Response::init_ok(
            3,
            vec![ToolEntry {
                name: "prodigal".into(),
                schema_version: 1,
            }],
        );
        let s = serde_json::to_string(&resp).unwrap();
        let back: Response = serde_json::from_str(&s).unwrap();
        assert_eq!(back.protocol_version, Some(3));
        assert_eq!(back.tools.len(), 1);
        assert_eq!(back.tools[0].name, "prodigal");
        assert_eq!(back.tools[0].schema_version, 1);
    }

    #[test]
    fn test_response_with_batch_id_round_trip() {
        let mut resp = Response::ok(serde_json::json!({}), vec![]);
        resp.batch_id = Some(42);
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["batch_id"].as_u64().unwrap(), 42);
    }

    // --- metrics opt-in flag + BatchMetrics ---

    #[test]
    fn test_batch_request_metrics_defaults_false() {
        // A batch line without the flag must deserialize to metrics = false
        // (so existing callers pay nothing and stay wire-compatible).
        let s = r#"{"tool":"bowtie2-align","shm_input":"/x","shm_input_size":256}"#;
        let b: BatchRequest = serde_json::from_str(s).unwrap();
        assert!(!b.metrics);
    }

    #[test]
    fn test_batch_request_metrics_opt_in() {
        let s = r#"{"tool":"bowtie2-align","shm_input":"/x","shm_input_size":256,"metrics":true}"#;
        let b: BatchRequest = serde_json::from_str(s).unwrap();
        assert!(b.metrics);
    }

    #[test]
    fn test_response_omits_metrics_when_none() {
        // Default ok/error responses carry no metrics → key absent from JSON.
        let resp = Response::ok(serde_json::json!({}), vec![]);
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert!(json.get("metrics").is_none());
    }

    #[test]
    fn test_response_with_metrics_serializes() {
        let mut resp = Response::ok(serde_json::json!({}), vec![]);
        resp.metrics = Some(BatchMetrics {
            worker_wall_ms: 55_230,
            worker_majflt: 51_234,
            worker_reused: true,
            ..Default::default()
        });
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["metrics"]["worker_wall_ms"].as_u64().unwrap(), 55_230);
        assert_eq!(json["metrics"]["worker_majflt"].as_u64().unwrap(), 51_234);
        assert!(json["metrics"]["worker_reused"].as_bool().unwrap());
    }

    #[test]
    fn test_batch_metrics_optional_fields_skipped_when_none() {
        // Linux-only and daemon-clock fields are Option; when None they must
        // be omitted (a non-Linux daemon, or the worker's pre-injection copy).
        let m = BatchMetrics::default();
        let json: serde_json::Value = serde_json::to_value(&m).unwrap();
        // Always-present scalar fields:
        assert!(json.get("worker_majflt").is_some());
        assert!(json.get("worker_reused").is_some());
        // Optional fields omitted when None:
        for absent in [
            "worker_rss_current_kb",
            "worker_read_bytes",
            "worker_write_bytes",
            "t_dispatch_ms",
            "t_worker_ms",
        ] {
            assert!(
                json.get(absent).is_none(),
                "{absent} must be omitted when None"
            );
        }
    }

    #[test]
    fn test_batch_metrics_round_trip() {
        let m = BatchMetrics {
            worker_wall_ms: 10,
            worker_majflt: 7,
            worker_read_bytes: Some(4096),
            t_worker_ms: Some(11),
            ..Default::default()
        };
        let s = serde_json::to_string(&m).unwrap();
        let back: BatchMetrics = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    // --- ControlMessage parsing ---

    #[test]
    fn test_parse_init_message() {
        let s = r#"{"init":{"idle_timeout_ms":1000}}"#;
        let m: ControlMessage = serde_json::from_str(s).unwrap();
        match m {
            ControlMessage::Init { init } => {
                assert_eq!(init.idle_timeout_ms, Some(1000));
            }
            other => panic!("Expected Init, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_init_message_empty_object() {
        let m: ControlMessage = serde_json::from_str(r#"{"init":{}}"#).unwrap();
        match m {
            ControlMessage::Init { init } => {
                assert_eq!(init.idle_timeout_ms, None);
                assert_eq!(init.max_workers, None);
            }
            other => panic!("Expected Init, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_batch_message() {
        let s = r#"{"tool":"fasttree","config":{"seed":1},"shm_input":"/x","shm_input_size":1024,"batch_id":7}"#;
        let m: ControlMessage = serde_json::from_str(s).unwrap();
        match m {
            ControlMessage::Batch(b) => {
                assert_eq!(b.tool, "fasttree");
                assert_eq!(b.shm_input, "/x");
                assert_eq!(b.shm_input_size, 1024);
                assert_eq!(b.batch_id, Some(7));
                assert_eq!(b.config["seed"], 1);
            }
            other => panic!("Expected Batch, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_batch_without_batch_id() {
        let s = r#"{"tool":"fasttree","shm_input":"/x","shm_input_size":256}"#;
        let m: ControlMessage = serde_json::from_str(s).unwrap();
        match m {
            ControlMessage::Batch(b) => {
                assert_eq!(b.tool, "fasttree");
                assert_eq!(b.shm_input_size, 256);
                assert_eq!(b.batch_id, None);
                assert!(b.config.is_null());
            }
            other => panic!("Expected Batch, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_shutdown_message() {
        let m: ControlMessage = serde_json::from_str(r#"{"shutdown":true}"#).unwrap();
        match m {
            ControlMessage::Shutdown { shutdown } => assert!(shutdown),
            other => panic!("Expected Shutdown, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_rejects_message_with_unknown_shape() {
        // No discriminating field — should fail.
        let result = serde_json::from_str::<ControlMessage>(r#"{"hello":"world"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_response_multiple_outputs_json() {
        let resp = Response::ok(
            serde_json::json!({}),
            vec![
                ShmOutput::new("/out-tree", "tree", 200),
                ShmOutput::new("/out-dist", "distances", 500),
            ],
        );
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        let outputs = json["shm_outputs"].as_array().unwrap();
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0]["label"], "tree");
        assert_eq!(outputs[1]["label"], "distances");
    }
}
