pub mod fasttree;
pub mod prodigal;

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
    pub schema_version: u32,
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
    /// Output Arrow schema version. Bump on any breaking schema change
    /// (new/removed/renamed columns, type changes) so consumers can fail fast.
    fn schema_version(&self) -> u32;
    fn describe(&self) -> ToolDescription;
    /// Execute the tool. The tool reads input from shm_input, creates its own
    /// output shm, and returns a Response containing the output shm name and size.
    fn execute(&self, config: &serde_json::Value, shm_input: &str) -> Response;
}

/// Registration entry for the tool registry. Each tool module calls
/// `inventory::submit!` with one of these to register itself.
pub struct ToolRegistration {
    pub(crate) create: fn() -> Box<dyn GplTool>,
}

inventory::collect!(ToolRegistration);

/// Return all registered tools.
fn all_tools() -> Vec<Box<dyn GplTool>> {
    inventory::iter::<ToolRegistration>
        .into_iter()
        .map(|reg| (reg.create)())
        .collect()
}

/// Dispatch a request to the appropriate tool.
pub fn dispatch(request: &Request) -> Response {
    let tool = all_tools().into_iter().find(|t| t.name() == request.tool);
    match tool {
        Some(t) => {
            let mut response = t.execute(&request.config, &request.shm_input);
            if response.success {
                response.schema_version = Some(t.schema_version());
            }
            response
        }
        None => Response::error(format!("Unknown tool: {}", request.tool)),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fasttree_in_registry() {
        let names = list_tools();
        assert!(
            names.contains(&"fasttree".to_string()),
            "fasttree not found in registry: {names:?}"
        );
    }

    #[test]
    fn test_prodigal_in_registry() {
        let names = list_tools();
        assert!(
            names.contains(&"prodigal".to_string()),
            "prodigal not found in registry: {names:?}"
        );
    }

    #[test]
    fn test_dispatch_unknown_tool() {
        let request = Request {
            tool: "nonexistent".to_string(),
            config: serde_json::json!({}),
            shm_input: "/dummy".to_string(),
        };
        let response = dispatch(&request);
        assert!(!response.success);
        assert!(response
            .error
            .as_ref()
            .unwrap()
            .contains("Unknown tool: nonexistent"));
    }

    #[test]
    fn test_describe_from_registry() {
        let desc = describe_tool("fasttree");
        assert!(desc.is_some());
        let desc = desc.unwrap();
        assert_eq!(desc.name, "fasttree");
        assert!(!desc.config_params.is_empty());
        assert!(!desc.input_schema.is_empty());
        assert!(!desc.output_schema.is_empty());
    }

    #[test]
    fn test_all_registered_tools_have_unique_names() {
        let tools = all_tools();
        let mut names: Vec<String> = tools.iter().map(|t| t.name().to_string()).collect();
        let original_len = names.len();
        names.sort();
        names.dedup();
        assert_eq!(
            names.len(),
            original_len,
            "Duplicate tool names in registry"
        );
    }
}
