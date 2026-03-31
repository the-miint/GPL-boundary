pub mod fasttree;

use crate::protocol::{Request, Response};

/// Description of an Arrow schema field for introspection.
#[derive(serde::Serialize)]
pub struct FieldDescription {
    pub name: &'static str,
    pub arrow_type: &'static str,
    pub nullable: bool,
    pub description: &'static str,
}

/// Description of a config parameter for introspection.
#[derive(serde::Serialize)]
pub struct ConfigParam {
    pub name: &'static str,
    pub param_type: &'static str,
    pub default: serde_json::Value,
    pub description: &'static str,
    /// If non-empty, the allowed values for this parameter.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub allowed_values: Vec<&'static str>,
}

/// Full tool description for programmatic introspection.
#[derive(serde::Serialize)]
pub struct ToolDescription {
    pub name: &'static str,
    pub version: String,
    pub description: &'static str,
    pub config_params: Vec<ConfigParam>,
    pub input_schema: Vec<FieldDescription>,
    pub output_schema: Vec<FieldDescription>,
    pub response_metadata: Vec<FieldDescription>,
}

/// Trait for GPL-licensed tools that can be dispatched.
pub trait GplTool {
    fn name(&self) -> &str;
    fn version(&self) -> String;
    fn describe(&self) -> ToolDescription;
    /// Execute the tool. The tool reads input from shm_input, creates its own
    /// output shm, and returns a Response containing the output shm name and size.
    fn execute(&self, config: &serde_json::Value, shm_input: &str) -> Response;
}

/// Return all registered tools.
fn all_tools() -> Vec<Box<dyn GplTool>> {
    vec![Box::new(fasttree::FastTreeTool)]
}

/// Dispatch a request to the appropriate tool.
pub fn dispatch(request: &Request) -> Response {
    match request.tool.as_str() {
        "fasttree" => {
            let tool = fasttree::FastTreeTool;
            tool.execute(&request.config, &request.shm_input)
        }
        other => Response::error(format!("Unknown tool: {other}")),
    }
}

/// List available tool names.
pub fn list_tools() -> Vec<String> {
    all_tools().iter().map(|t| t.name().to_string()).collect()
}

/// Describe a specific tool, or None if not found.
pub fn describe_tool(name: &str) -> Option<ToolDescription> {
    all_tools()
        .into_iter()
        .find(|t| t.name() == name)
        .map(|t| t.describe())
}

/// Version info for gpl-boundary and all tools.
pub fn version_info() -> serde_json::Value {
    let tools: serde_json::Value = all_tools()
        .iter()
        .map(|t| {
            serde_json::json!({
                "name": t.name(),
                "version": t.version(),
            })
        })
        .collect();

    serde_json::json!({
        "gpl_boundary": env!("CARGO_PKG_VERSION"),
        "tools": tools,
    })
}
