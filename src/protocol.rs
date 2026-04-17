use serde::{Deserialize, Serialize};

/// Request sent from miint to GPL-boundary via stdin.
/// Bulk data is in Arrow IPC format in the shared memory region -- never in JSON.
#[derive(Debug, Deserialize)]
pub struct Request {
    /// Which tool to run (e.g., "fasttree")
    pub tool: String,

    /// Tool-specific parameters (model, seed, etc. -- never bulk data)
    #[serde(default)]
    pub config: serde_json::Value,

    /// POSIX shared memory name for the input Arrow IPC data.
    /// Created and owned by the caller (miint).
    pub shm_input: String,

    /// When true, the process stays alive after the first response and reads
    /// subsequent BatchRequest messages from stdin until EOF. The tool's
    /// context (loaded index, models) is reused across batches.
    /// Absent or false = single-shot (existing behavior).
    #[serde(default)]
    pub stream: bool,
}

/// A subsequent batch request in streaming mode. Contains only the shm name
/// for the next batch of input. Tool name and config are fixed for the session
/// (established by the initial Request).
#[derive(Debug, Deserialize)]
pub struct BatchRequest {
    /// POSIX shared memory name for this batch's input Arrow IPC data.
    pub shm_input: String,
}

/// A single shared memory output segment.
#[derive(Debug, Serialize, Clone)]
pub struct ShmOutput {
    /// POSIX shared memory name (e.g., "/gpl-boundary-1234-tree").
    pub name: String,
    /// Tool-defined label identifying this output (e.g., "tree", "alignment").
    pub label: String,
    /// Number of valid bytes to read as Arrow IPC stream.
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

/// Response sent from GPL-boundary to miint via stdout.
/// Bulk results are in Arrow IPC in shm_outputs -- JSON carries only metadata.
#[derive(Debug, Serialize)]
pub struct Response {
    pub success: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Schema version for the tool's output Arrow schema. Bumped on any
    /// breaking change (new/removed/renamed columns, type changes). Lets the
    /// caller fail fast when boundary and extension versions drift.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u32>,

    /// Output shared memory segments, each containing Arrow IPC data.
    /// Created by gpl-boundary. Caller is responsible for reading and unlinking.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub shm_outputs: Vec<ShmOutput>,

    /// Lightweight result metadata (e.g., stats, row counts).
    /// Never bulk data -- that goes to shm_outputs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

impl Response {
    pub fn ok(result: serde_json::Value, shm_outputs: Vec<ShmOutput>) -> Self {
        Self {
            success: true,
            error: None,
            schema_version: None,
            shm_outputs,
            result: Some(result),
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            error: Some(msg.into()),
            schema_version: None,
            shm_outputs: vec![],
            result: None,
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
        // schema_version is set by dispatch, not Response::ok
        assert!(json.get("schema_version").is_none());
        let outputs = json["shm_outputs"].as_array().unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0]["name"], "/out1");
        assert_eq!(outputs[0]["label"], "tree");
        assert_eq!(outputs[0]["size"], 100);
        assert_eq!(json["result"]["count"], 42);
        assert!(json.get("error").is_none());
    }

    #[test]
    fn test_response_empty_outputs_json() {
        let resp = Response::ok(serde_json::json!({"msg": "metadata only"}), vec![]);
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        assert!(json["success"].as_bool().unwrap());
        // shm_outputs should be omitted entirely when empty
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

    // -- Request.stream field tests --

    #[test]
    fn test_request_stream_true() {
        let req: Request =
            serde_json::from_str(r#"{"tool":"foo","shm_input":"/x","stream":true}"#).unwrap();
        assert!(req.stream);
    }

    #[test]
    fn test_request_stream_absent() {
        let req: Request = serde_json::from_str(r#"{"tool":"foo","shm_input":"/x"}"#).unwrap();
        assert!(!req.stream);
    }

    #[test]
    fn test_request_stream_false() {
        let req: Request =
            serde_json::from_str(r#"{"tool":"foo","shm_input":"/x","stream":false}"#).unwrap();
        assert!(!req.stream);
    }

    // -- BatchRequest tests --

    #[test]
    fn test_batch_request_deserialize() {
        let batch: BatchRequest = serde_json::from_str(r#"{"shm_input":"/batch-1"}"#).unwrap();
        assert_eq!(batch.shm_input, "/batch-1");
    }

    #[test]
    fn test_batch_request_missing_shm_input() {
        let result = serde_json::from_str::<BatchRequest>("{}");
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_request_ignores_extra_fields() {
        let batch: BatchRequest =
            serde_json::from_str(r#"{"shm_input":"/x","extra":"whatever"}"#).unwrap();
        assert_eq!(batch.shm_input, "/x");
    }

    #[test]
    fn test_response_multiple_outputs_json() {
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
