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
