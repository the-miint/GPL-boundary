pub mod bowtie2_align;
pub mod bowtie2_build;
pub mod bowtie2_ffi;
pub mod fasttree;
pub mod prodigal;
pub mod sortmerna;

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

/// A reusable tool context for batched streaming. Created once per session
/// via `GplTool::create_streaming_context`, then used for multiple batches.
/// Holds expensive state (loaded index, reference DB) that is reused across
/// `run_batch` calls. The `Drop` impl must call the C library's `destroy()`.
pub trait StreamingContext {
    /// Process one batch of input. Same contract as `GplTool::execute()` but
    /// against a pre-loaded context: reads input from shm, creates output shm,
    /// returns Response.
    ///
    /// **Caller responsibility**: after writing the Response to stdout, the
    /// caller must call `shm::deregister_cleanup` for each `shm_outputs` entry.
    /// Output shm is registered for signal-handler cleanup inside `run_batch`;
    /// deregistering transfers ownership to miint. This must happen *after*
    /// the response is on stdout to prevent leaks if a signal arrives mid-write.
    fn run_batch(&mut self, shm_input: &str) -> Response;
}

/// Trait for GPL-licensed tools that can be dispatched.
pub trait GplTool {
    fn name(&self) -> &str;
    fn version(&self) -> String;
    /// Output Arrow schema version. Bump on any breaking schema change
    /// (new/removed/renamed columns, type changes) so consumers can fail fast.
    fn schema_version(&self) -> u32;
    fn describe(&self) -> ToolDescription;
    /// Execute the tool (single-shot). The tool reads input from shm_input,
    /// creates its own output shm, and returns a Response.
    fn execute(&self, config: &serde_json::Value, shm_input: &str) -> Response;

    /// Create a streaming context for batched execution. Returns `Ok(Some(ctx))`
    /// if the tool supports streaming, `Ok(None)` if it does not (default).
    /// The context holds expensive state (loaded index, models) and is reused
    /// across multiple `run_batch()` calls.
    fn create_streaming_context(
        &self,
        config: &serde_json::Value,
    ) -> Result<Option<Box<dyn StreamingContext>>, String> {
        let _ = config;
        Ok(None)
    }
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

/// Set up a streaming session for the named tool. Returns the streaming
/// context and the tool's schema version, or an error Response.
pub fn streaming_setup(
    tool_name: &str,
    config: &serde_json::Value,
) -> Result<(Box<dyn StreamingContext>, u32), Response> {
    let tool = all_tools().into_iter().find(|t| t.name() == tool_name);
    match tool {
        None => Err(Response::error(format!("Unknown tool: {tool_name}"))),
        Some(t) => {
            let schema_version = t.schema_version();
            match t.create_streaming_context(config) {
                Ok(Some(ctx)) => Ok((ctx, schema_version)),
                Ok(None) => Err(Response::error(format!(
                    "Tool '{}' does not support streaming",
                    tool_name
                ))),
                Err(e) => Err(Response::error(e)),
            }
        }
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
    fn test_bowtie2_align_in_registry() {
        let names = list_tools();
        assert!(
            names.contains(&"bowtie2-align".to_string()),
            "bowtie2-align not found in registry: {names:?}"
        );
    }

    #[test]
    fn test_bowtie2_build_in_registry() {
        let names = list_tools();
        assert!(
            names.contains(&"bowtie2-build".to_string()),
            "bowtie2-build not found in registry: {names:?}"
        );
    }

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
    fn test_sortmerna_in_registry() {
        let names = list_tools();
        assert!(
            names.contains(&"sortmerna".to_string()),
            "sortmerna not found in registry: {names:?}"
        );
    }

    #[test]
    fn test_dispatch_unknown_tool() {
        let request = Request {
            tool: "nonexistent".to_string(),
            config: serde_json::json!({}),
            shm_input: "/dummy".to_string(),
            stream: false,
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

    // -- StreamingContext and streaming_setup tests --

    #[test]
    fn test_streaming_context_trait_is_object_safe() {
        struct DummyCtx;
        impl StreamingContext for DummyCtx {
            fn run_batch(&mut self, _shm_input: &str) -> Response {
                Response::error("stub")
            }
        }
        let mut ctx: Box<dyn StreamingContext> = Box::new(DummyCtx);
        let resp = ctx.run_batch("/dummy");
        assert!(!resp.success);
    }

    #[test]
    fn test_default_create_streaming_context_returns_none() {
        let tools = all_tools();
        let fasttree = tools.iter().find(|t| t.name() == "fasttree").unwrap();
        let result = fasttree.create_streaming_context(&serde_json::json!({}));
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_streaming_setup_unknown_tool() {
        match streaming_setup("nonexistent", &serde_json::json!({})) {
            Err(resp) => assert!(resp.error.as_ref().unwrap().contains("Unknown tool")),
            Ok(_) => panic!("Expected error for unknown tool"),
        }
    }

    #[test]
    fn test_streaming_setup_non_streaming_tool() {
        match streaming_setup("fasttree", &serde_json::json!({})) {
            Err(resp) => assert!(
                resp.error
                    .as_ref()
                    .unwrap()
                    .contains("does not support streaming"),
                "Expected 'does not support streaming', got: {:?}",
                resp.error
            ),
            Ok(_) => panic!("Expected error for non-streaming tool"),
        }
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
