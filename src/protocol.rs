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
}

/// Response sent from GPL-boundary to miint via stdout.
/// Bulk results are in Arrow IPC in shm_output -- JSON carries only metadata.
#[derive(Debug, Serialize)]
pub struct Response {
    pub success: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// POSIX shared memory name where output Arrow IPC was written.
    /// Created by gpl-boundary. Caller is responsible for reading and unlinking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shm_output: Option<String>,

    /// Number of valid bytes in the output shared memory region.
    /// Caller should read exactly this many bytes as Arrow IPC stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shm_output_size: Option<usize>,

    /// Lightweight result metadata (e.g., stats, row counts).
    /// Never bulk data -- that goes to shm_output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

impl Response {
    pub fn ok(result: serde_json::Value, shm_output: String, shm_output_size: usize) -> Self {
        Self {
            success: true,
            error: None,
            shm_output: Some(shm_output),
            shm_output_size: Some(shm_output_size),
            result: Some(result),
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            error: Some(msg.into()),
            shm_output: None,
            shm_output_size: None,
            result: None,
        }
    }
}
