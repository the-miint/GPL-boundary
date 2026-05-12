pub mod bowtie2_align;
pub mod bowtie2_build;
pub mod bowtie2_ffi;
pub mod fasttree;
pub mod prodigal;
pub mod sortmerna;

use crate::protocol::{BatchRequest, Response};

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
///
/// Three independent versions live on this surface; they govern
/// different things and must be bumped independently:
///
/// - **`schema_version`** — output Arrow schema only. Bump when
///   columns change name/type/nullability or when null-translation
///   semantics change. Lets a caller's columnar reader fail fast
///   on mismatched expectations. Adding/removing/renaming entries
///   in `input_schema` or `output_schema` always implies an Arrow
///   schema change, so those changes bump `schema_version` (and
///   `describe_version` — they're not mutually exclusive).
/// - **`describe_version`** — `--describe` introspection surface.
///   Bump on any change to `config_params` (add/rename/remove,
///   type/default/allowed-values edit) OR to `response_metadata`
///   (which is documentation-only — it does not affect the Arrow
///   output schema). `input_schema` / `output_schema` changes also
///   bump this, but they always co-bump `schema_version`.
/// - **`protocol_version`** — wire envelope (init/batch/shutdown
///   shape). Lives on the init reply, not here. Bump on any
///   breaking change to the JSON envelope.
///
/// A caller polling `--describe` for capability detection should
/// use `describe_version` to gate "does this tool advertise
/// knob X?". A caller decoding a batch response should check
/// `schema_version`. A caller speaking the wire protocol should
/// check `protocol_version`. None of the three is a substitute
/// for another.
#[derive(serde::Serialize)]
pub struct ToolDescription {
    pub name: &'static str,
    pub version: String,
    pub schema_version: u32,
    /// Bumped on any `--describe` surface change: `config_params`
    /// edits, `response_metadata` edits, or `input_schema` /
    /// `output_schema` edits (the last two also co-bump
    /// `schema_version`, since they imply Arrow schema changes).
    /// See the struct doc-comment for the full bump policy.
    pub describe_version: u32,
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
///
/// `Send` is required because the Phase 4 worker registry stores contexts
/// inside an `Arc<dyn Worker>` whose `Mutex<Option<Box<dyn StreamingContext>>>`
/// must be `Send + Sync`. The contexts themselves are not accessed concurrently
/// — the Mutex guarantees single-threaded use of the underlying C state — but
/// they may be created on one thread and dropped on another (e.g. during
/// step-4 idle-deadline eviction from a sweeper thread). Any C library held
/// inside a context must therefore be safe to free from a thread other than
/// the one that constructed it. All current submodules satisfy this.
pub trait StreamingContext: Send {
    /// Process one batch of input. Same contract as `GplTool::execute()` but
    /// against a pre-loaded context: reads input from shm, creates output shm,
    /// returns Response.
    ///
    /// **Caller responsibility**: after writing the Response to stdout, the
    /// caller must call `shm::deregister_cleanup` for each `shm_outputs` entry.
    /// Output shm is registered for signal-handler cleanup inside `run_batch`;
    /// deregistering transfers ownership to miint. This must happen *after*
    /// the response is on stdout to prevent leaks if a signal arrives mid-write.
    fn run_batch(&mut self, shm_input: &str, shm_input_size: usize) -> Response;
}

/// Trait for GPL-licensed tools that can be dispatched.
pub trait GplTool {
    fn name(&self) -> &str;
    fn version(&self) -> String;
    /// Output Arrow schema version. Bump on any breaking schema change
    /// (new/removed/renamed columns, type changes) so consumers can fail fast.
    fn schema_version(&self) -> u32;
    fn describe(&self) -> ToolDescription;
    /// Execute the tool (single-shot). The tool reads `shm_input_size`
    /// bytes from `shm_input` (the protocol's authoritative size — `fstat`
    /// is not reliable on Darwin), creates its own output shm, and
    /// returns a Response.
    fn execute(
        &self,
        config: &serde_json::Value,
        shm_input: &str,
        shm_input_size: usize,
    ) -> Response;

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

/// Dispatch a single batch to the appropriate tool. Stateless: each call
/// runs the tool's `execute` from scratch. Phase 4's worker pool will keep
/// long-lived contexts; Phase 3 still falls through to this path for tools
/// that don't expose `create_streaming_context`.
pub fn dispatch(request: &BatchRequest) -> Response {
    let tool = all_tools().into_iter().find(|t| t.name() == request.tool);
    match tool {
        Some(t) => {
            let mut response =
                t.execute(&request.config, &request.shm_input, request.shm_input_size);
            if response.success {
                response.schema_version = Some(t.schema_version());
            }
            response
        }
        None => Response::error(format!("Unknown tool: {}", request.tool)),
    }
}

/// `(streaming context, tool schema_version)` returned by a successful
/// `streaming_setup` call.
pub type StreamingSession = (Box<dyn StreamingContext>, u32);

/// Try to set up a streaming context for the named tool.
///
/// Three outcomes:
/// - `Err(response)` — real failure: unknown tool name, OR the tool supports
///   streaming but `create_streaming_context` returned an error (e.g.
///   bowtie2 index not found). The caller should surface this response
///   directly to the client; do NOT fall back to per-batch dispatch.
/// - `Ok(None)` — the tool exists but does not implement streaming (i.e.
///   `create_streaming_context` returned `Ok(None)`). Caller should fall
///   through to `dispatch` for each batch; there is no long-lived context
///   to set up.
/// - `Ok(Some((ctx, schema_version)))` — context successfully created;
///   reuse `ctx` across all batches in the session.
pub fn streaming_setup(
    tool_name: &str,
    config: &serde_json::Value,
) -> Result<Option<StreamingSession>, Response> {
    let tool = all_tools().into_iter().find(|t| t.name() == tool_name);
    match tool {
        None => Err(Response::error(format!("Unknown tool: {tool_name}"))),
        Some(t) => {
            let schema_version = t.schema_version();
            match t.create_streaming_context(config) {
                Ok(Some(ctx)) => Ok(Some((ctx, schema_version))),
                Ok(None) => Ok(None),
                Err(e) => Err(Response::error(format!(
                    "Failed to create streaming context for '{tool_name}': {e}"
                ))),
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

/// Look up the schema version for `name` without paying the cost of a full
/// `describe()`. Used by the subprocess worker to populate response
/// `schema_version` after `streaming_setup` succeeds.
pub fn tool_schema_version(name: &str) -> Option<u32> {
    all_tools()
        .into_iter()
        .find(|t| t.name() == name)
        .map(|t| t.schema_version())
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
        let request = BatchRequest {
            tool: "nonexistent".to_string(),
            config: serde_json::json!({}),
            shm_input: "/dummy".to_string(),
            shm_input_size: 0,
            batch_id: None,
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

    /// Every registered tool must advertise a non-zero `describe_version`
    /// and a non-zero `schema_version`. The two are independent (each
    /// has its own bump policy — see the `ToolDescription` doc-comment).
    #[test]
    fn test_every_tool_describe_version_set() {
        let names = list_tools();
        assert!(!names.is_empty(), "tool registry must not be empty");
        for name in &names {
            let desc = describe_tool(name).expect("registered tool must describe");
            assert!(
                desc.describe_version >= 1,
                "{name} describe_version must be >= 1, got {}",
                desc.describe_version
            );
            assert!(
                desc.schema_version >= 1,
                "{name} schema_version must be >= 1, got {}",
                desc.schema_version
            );
        }
    }

    /// Describe-version landmarks. `fasttree` is at 3 (Phase 1 + 2 + 3
    /// bumps); the other four tools start at 1 and will be bumped only
    /// when their `--describe` surfaces actually change. Asserting the
    /// exact values guards against accidental unbumped-on-change drift
    /// and accidental bumped-on-no-change drift in equal measure.
    #[test]
    fn test_describe_version_landmarks() {
        let cases: &[(&str, u32)] = &[
            ("fasttree", 3),
            ("prodigal", 1),
            ("sortmerna", 1),
            ("bowtie2-align", 2),
            ("bowtie2-build", 1),
        ];
        for (name, expected) in cases {
            let desc = describe_tool(name).expect("registered tool must describe");
            assert_eq!(
                desc.describe_version, *expected,
                "{name} describe_version drifted from documented landmark {expected}: \
                 if you intentionally added/renamed/removed a config_param or schema field, \
                 bump describe_version in {name}.rs's describe() and update this assertion"
            );
        }
    }

    /// The serialized JSON form actually contains `describe_version`
    /// (this is what miint sees over the wire). Catches any future
    /// `#[serde(skip)]` accident.
    #[test]
    fn test_describe_version_serializes() {
        let desc = describe_tool("fasttree").unwrap();
        let json = serde_json::to_value(&desc).unwrap();
        assert_eq!(
            json["describe_version"].as_u64(),
            Some(desc.describe_version as u64)
        );
    }

    // -- StreamingContext and streaming_setup tests --

    #[test]
    fn test_streaming_context_trait_is_object_safe() {
        struct DummyCtx;
        impl StreamingContext for DummyCtx {
            fn run_batch(&mut self, _shm_input: &str, _shm_input_size: usize) -> Response {
                Response::error("stub")
            }
        }
        let mut ctx: Box<dyn StreamingContext> = Box::new(DummyCtx);
        let resp = ctx.run_batch("/dummy", 0);
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
            Ok(_) => panic!("Expected Err for unknown tool"),
        }
    }

    #[test]
    fn test_streaming_setup_non_streaming_tool_returns_ok_none() {
        // FastTree exists but does not support streaming. Returning
        // `Ok(None)` lets the caller fall through to `dispatch` per batch
        // without conflating "no streaming support" with "init failed".
        match streaming_setup("fasttree", &serde_json::json!({})) {
            Ok(None) => {}
            Ok(Some(_)) => panic!("FastTree should not return a streaming context"),
            Err(resp) => panic!(
                "Expected Ok(None) for non-streaming tool, got Err: {:?}",
                resp.error
            ),
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
