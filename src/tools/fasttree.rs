//! FastTree adapter: reads Arrow IPC from shared memory, runs fasttree,
//! writes SOA tree as Arrow IPC back to shared memory.

use crate::protocol::{Response, ShmOutput};
use crate::tools::{ConfigParam, FieldDescription, GplTool, ToolDescription};
use arrow::array::{
    BooleanArray, Float64Builder, Int32Array, Int64Array, Int64Builder, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::Arc;

// --- FFI bindings to libfasttree ---

#[repr(C)]
pub struct FastTreeConfig {
    pub struct_size: usize,
    pub seq_type: c_int,
    pub model: c_int,
    pub gtr_rates: [f64; 6],
    pub gtr_freq: [f64; 4],
    pub gtr_from_alignment: c_int,
    pub nni_rounds: c_int,
    pub spr_rounds: c_int,
    pub ml_nni_rounds: c_int,
    pub n_rate_cats: c_int,
    pub slow: c_int,
    pub fastest: c_int,
    pub n_bootstrap: c_int,
    pub gamma_log_lk: c_int,
    pub seed: i64,
    pub use_top_hits: c_int,
    pub top_hits_mult: f64,
    pub use_bionj: c_int,
    pub pseudo_weight: f64,
    pub constraint_weight: f64,
    pub ml_accuracy: c_int,
    pub start_newick: *const c_char,
    pub quote_names: c_int,
    pub n_threads: c_int,
    pub progress_callback: Option<unsafe extern "C" fn(*const c_char, f64, *mut c_void) -> c_int>,
    pub progress_user_data: *mut c_void,
    pub log_callback: Option<unsafe extern "C" fn(*const c_char, *mut c_void)>,
    pub log_user_data: *mut c_void,
    pub alloc_fn: Option<unsafe extern "C" fn(usize, *mut c_void) -> *mut c_void>,
    pub free_fn: Option<unsafe extern "C" fn(*mut c_void, *mut c_void)>,
    pub alloc_user_data: *mut c_void,
}

#[repr(C)]
pub struct FastTreeStats {
    pub n_unique_seqs: c_int,
    pub log_likelihood: f64,
    pub gamma_log_lk: f64,
    pub n_nni: c_int,
    pub n_spr: c_int,
    pub n_ml_nni: c_int,
}

#[repr(C)]
pub struct FastTreeSoa {
    pub n_nodes: c_int,
    pub n_leaves: c_int,
    pub root: c_int,
    pub parent: *const c_int,
    pub branch_length: *const f64,
    pub support: *const f64,
    pub n_children: *const c_int,
    pub is_leaf: *const c_int,
    pub children_offset: *const c_int,
    pub name: *const *const c_char,
    pub _children_buf: *const c_int,
    pub _name_buf: *const c_char,
    pub _base: *mut c_void,
}

#[allow(non_camel_case_types)]
type fasttree_ctx_t = c_void;

extern "C" {
    fn fasttree_config_init(config: *mut FastTreeConfig);
    fn fasttree_create(config: *const FastTreeConfig) -> *mut fasttree_ctx_t;
    fn fasttree_destroy(ctx: *mut fasttree_ctx_t);
    fn fasttree_build_soa(
        ctx: *mut fasttree_ctx_t,
        names: *const *const c_char,
        seqs: *const *const c_char,
        n_seq: c_int,
        n_pos: c_int,
        tree_out: *mut *mut FastTreeSoa,
        stats_out: *mut FastTreeStats,
    ) -> c_int;
    fn fasttree_tree_soa_free(tree: *mut FastTreeSoa);
    fn fasttree_strerror(code: c_int) -> *const c_char;
    fn fasttree_last_error(ctx: *mut fasttree_ctx_t) -> *const c_char;
}

const FASTTREE_OK: c_int = 0;
const FASTTREE_VERSION: &str = "2.3.0";

/// C-callable log callback that writes messages to stderr.
/// Passed as `log_callback` to fasttree when verbose mode is enabled.
unsafe extern "C" fn stderr_log_callback(msg: *const c_char, _user_data: *mut c_void) {
    if !msg.is_null() {
        let s = CStr::from_ptr(msg).to_string_lossy();
        eprint!("{s}");
    }
}

// --- Arrow schema definitions ---

/// Input schema: columnar alignment data from miint.
#[allow(dead_code)]
pub fn input_schema() -> Schema {
    Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("sequence", DataType::Utf8, false),
    ])
}

/// Output schema: SOA tree columnar data for miint.
///
/// Schema version 2 (Phase 5) renames and widens for miint's tree-edge
/// join semantics:
/// - `node_index` (Int64): node index `[0, n_nodes)`. Renamed from `id`.
/// - `parent_index` (Int64, nullable): parent's `node_index`; NULL for the
///   root (no -1 sentinel).
/// - `edge_id` (Int64, nullable): join key for the inbound edge of each
///   non-root node. Convention: `edge_id == node_index`. NULL for the
///   root. miint's downstream tree-edge tables join on this column.
/// - `branch_length` (Float64, nullable): NaN from the C library is
///   emitted as NULL.
/// - `support` (Float64, nullable): the C library's `-1` "not computed"
///   sentinel is emitted as NULL.
/// - `n_children` (Int32): kept narrow — children counts never exceed
///   the leaf count of a tiny binary tree.
/// - `is_tip` (Boolean): renamed from `is_leaf` to match miint's
///   tree vocabulary.
/// - `name` (Utf8, nullable): leaf name; NULL for internal nodes.
fn output_schema() -> Schema {
    Schema::new(vec![
        Field::new("node_index", DataType::Int64, false),
        Field::new("parent_index", DataType::Int64, true),
        Field::new("edge_id", DataType::Int64, true),
        Field::new("branch_length", DataType::Float64, true),
        Field::new("support", DataType::Float64, true),
        Field::new("n_children", DataType::Int32, false),
        Field::new("is_tip", DataType::Boolean, false),
        Field::new("name", DataType::Utf8, true),
    ])
}

// --- JSON config decoder ---

/// Apply gpl-boundary's JSON config knobs onto an already-`fasttree_config_init`'d
/// `FastTreeConfig`. Validates mutually-exclusive combinations at the
/// JSON→C-config boundary so the same error message reaches every consumer
/// regardless of how the boundary is invoked.
///
/// Caller MUST have called `fasttree_config_init(ft_config)` immediately
/// before this function so unsupplied knobs sit at their C-library defaults
/// (e.g., `n_bootstrap = 1000`, `nni_rounds = -1` (auto), etc.).
///
/// Wired-up knobs (Phase 1 onward):
/// - `seq_type`: "auto" (default) | "nucleotide" | "protein"
/// - `seed`: i64, default 314159
/// - `verbose`: bool, default false (routes stderr log if true)
/// - `bootstrap`: i64 ≥ 0 → `n_bootstrap`. Absent = C default (1000).
/// - `nosupport`: bool. true → `n_bootstrap = 0`. Mutually exclusive with
///   `bootstrap > 0`.
fn apply_json_to_config(
    config: &serde_json::Value,
    ft_config: &mut FastTreeConfig,
) -> Result<(), String> {
    // seq_type
    ft_config.seq_type = match config.get("seq_type").and_then(|v| v.as_str()) {
        Some("nucleotide") => 2,
        Some("protein") => 1,
        Some("auto") | None => 0,
        Some(other) => return Err(format!("invalid seq_type: {other:?}")),
    };

    if let Some(seed) = config.get("seed").and_then(|v| v.as_i64()) {
        ft_config.seed = seed;
    }

    if config
        .get("verbose")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        ft_config.log_callback = Some(stderr_log_callback);
    }

    // bootstrap + nosupport
    let bootstrap = config.get("bootstrap").and_then(|v| v.as_i64());
    let nosupport = config
        .get("nosupport")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if let Some(b) = bootstrap {
        if nosupport && b > 0 {
            return Err(format!(
                "config conflict: bootstrap={b} cannot be combined with nosupport=true \
                 (use bootstrap=0 or omit nosupport)"
            ));
        }
        if b < 0 {
            return Err(format!("bootstrap must be >= 0 (got {b})"));
        }
        ft_config.n_bootstrap = b as c_int;
    }
    if nosupport {
        ft_config.n_bootstrap = 0;
    }

    Ok(())
}

// --- Tool implementation ---

pub struct FastTreeTool;

inventory::submit! {
    crate::tools::ToolRegistration {
        create: || Box::new(FastTreeTool),
    }
}

impl FastTreeTool {
    /// Read Arrow IPC stream from shared memory, extract name and sequence columns.
    fn read_input(shm_input: &str) -> Result<(Vec<String>, Vec<String>), String> {
        let batches = crate::arrow_ipc::read_batches_from_shm(shm_input)?;

        let mut all_names = Vec::new();
        let mut all_seqs = Vec::new();

        for batch in &batches {
            let name_col = batch
                .column_by_name("name")
                .ok_or("Input missing 'name' column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("'name' column is not Utf8")?;

            let seq_col = batch
                .column_by_name("sequence")
                .ok_or("Input missing 'sequence' column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("'sequence' column is not Utf8")?;

            for i in 0..batch.num_rows() {
                all_names.push(name_col.value(i).to_string());
                all_seqs.push(seq_col.value(i).to_string());
            }
        }

        Ok((all_names, all_seqs))
    }

    /// Convert SOA tree to Arrow RecordBatch and write as IPC to a new shared
    /// memory region. Returns ShmOutput with name, label, and byte count.
    fn write_output(tree: &FastTreeSoa) -> Result<ShmOutput, String> {
        let batch = unsafe { soa_to_record_batch(tree)? };
        crate::arrow_ipc::write_batch_to_output_shm(&batch, "tree")
    }
}

impl GplTool for FastTreeTool {
    fn name(&self) -> &str {
        "fasttree"
    }

    fn version(&self) -> String {
        FASTTREE_VERSION.to_string()
    }

    fn schema_version(&self) -> u32 {
        2
    }

    fn describe(&self) -> ToolDescription {
        ToolDescription {
            name: "fasttree",
            version: self.version(),
            schema_version: self.schema_version(),
            description:
                "Approximately-maximum-likelihood phylogenetic trees from sequence alignments",
            config_params: vec![
                ConfigParam {
                    name: "seq_type",
                    param_type: "string",
                    default: serde_json::json!("auto"),
                    description: "Sequence type: auto-detect, nucleotide, or protein",
                    allowed_values: vec!["auto", "nucleotide", "protein"],
                },
                ConfigParam {
                    name: "seed",
                    param_type: "integer",
                    default: serde_json::json!(314159),
                    description: "Random seed for reproducibility",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "model",
                    param_type: "string",
                    default: serde_json::json!("auto"),
                    description: "Substitution model (auto=JTT for protein, JC for nucleotide)",
                    allowed_values: vec!["auto", "jtt", "lg", "wag", "jc", "gtr"],
                },
                ConfigParam {
                    name: "fastest",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Use fastest heuristics (less accurate)",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "gamma",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Compute gamma log-likelihood",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "verbose",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Write tool log output to stderr for diagnostics",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "bootstrap",
                    param_type: "integer",
                    default: serde_json::json!(1000),
                    description: "SH-like local-support resamples (>= 0). 0 disables support computation; equivalent to nosupport=true.",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "nosupport",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Disable SH-like local-support computation (synthesizes bootstrap=0). Mutually exclusive with bootstrap > 0.",
                    allowed_values: vec![],
                },
            ],
            input_schema: vec![
                FieldDescription {
                    name: "name",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Sequence identifier",
                },
                FieldDescription {
                    name: "sequence",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Aligned sequence (all must be equal length)",
                },
            ],
            output_schema: vec![
                FieldDescription {
                    name: "node_index",
                    arrow_type: "Int64",
                    nullable: false,
                    description: "Node index [0, n_nodes)",
                },
                FieldDescription {
                    name: "parent_index",
                    arrow_type: "Int64",
                    nullable: true,
                    description: "Parent node's node_index; NULL for the root",
                },
                FieldDescription {
                    name: "edge_id",
                    arrow_type: "Int64",
                    nullable: true,
                    description:
                        "Inbound-edge join key (= node_index for non-root); NULL for the root",
                },
                FieldDescription {
                    name: "branch_length",
                    arrow_type: "Float64",
                    nullable: true,
                    description: "Branch length to parent; NULL when the C library emits NaN",
                },
                FieldDescription {
                    name: "support",
                    arrow_type: "Float64",
                    nullable: true,
                    description: "SH-like local support [0, 1]; NULL when not computed",
                },
                FieldDescription {
                    name: "n_children",
                    arrow_type: "Int32",
                    nullable: false,
                    description: "Number of children (0 for tips)",
                },
                FieldDescription {
                    name: "is_tip",
                    arrow_type: "Boolean",
                    nullable: false,
                    description: "Whether node is a tip (formerly is_leaf)",
                },
                FieldDescription {
                    name: "name",
                    arrow_type: "Utf8",
                    nullable: true,
                    description: "Tip name (NULL for internal nodes)",
                },
            ],
            response_metadata: vec![
                FieldDescription {
                    name: "n_nodes",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Total node count in tree",
                },
                FieldDescription {
                    name: "n_leaves",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Leaf count in tree",
                },
                FieldDescription {
                    name: "root",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Index of root node",
                },
                FieldDescription {
                    name: "stats.n_unique_seqs",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Number of unique sequences after deduplication",
                },
                FieldDescription {
                    name: "stats.log_likelihood",
                    arrow_type: "float64",
                    nullable: false,
                    description: "Final tree log-likelihood (-1 if ML disabled)",
                },
                FieldDescription {
                    name: "stats.gamma_log_lk",
                    arrow_type: "float64",
                    nullable: false,
                    description: "Gamma log-likelihood (-1 if not computed)",
                },
                FieldDescription {
                    name: "stats.n_nni",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Total ME-NNI topology changes",
                },
                FieldDescription {
                    name: "stats.n_spr",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Total SPR topology changes",
                },
                FieldDescription {
                    name: "stats.n_ml_nni",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Total ML-NNI topology changes",
                },
            ],
        }
    }

    fn execute(&self, config: &serde_json::Value, shm_input: &str) -> Response {
        // Read input alignment from Arrow IPC in shared memory
        let (names, sequences) = match Self::read_input(shm_input) {
            Ok(data) => data,
            Err(e) => return Response::error(e),
        };

        if names.len() < 3 {
            return Response::error("At least 3 sequences required");
        }

        let n_seq = names.len() as c_int;
        let n_pos = sequences[0].len() as c_int;

        if sequences.iter().any(|s| s.len() as c_int != n_pos) {
            return Response::error("All sequences must have the same length (aligned)");
        }

        // Convert to C strings for fasttree
        let c_names: Vec<CString> = names
            .iter()
            .map(|n| CString::new(n.as_str()).unwrap())
            .collect();
        let c_seqs: Vec<CString> = sequences
            .iter()
            .map(|s| CString::new(s.as_str()).unwrap())
            .collect();
        let c_name_ptrs: Vec<*const c_char> = c_names.iter().map(|n| n.as_ptr()).collect();
        let c_seq_ptrs: Vec<*const c_char> = c_seqs.iter().map(|s| s.as_ptr()).collect();

        unsafe {
            let mut ft_config: FastTreeConfig = std::mem::zeroed();
            fasttree_config_init(&mut ft_config);
            if let Err(e) = apply_json_to_config(config, &mut ft_config) {
                return Response::error(e);
            }

            let ctx = fasttree_create(&ft_config);
            if ctx.is_null() {
                return Response::error("Failed to create fasttree context (out of memory)");
            }

            let mut tree: *mut FastTreeSoa = ptr::null_mut();
            let mut stats: FastTreeStats = std::mem::zeroed();

            let rc = fasttree_build_soa(
                ctx,
                c_name_ptrs.as_ptr(),
                c_seq_ptrs.as_ptr(),
                n_seq,
                n_pos,
                &mut tree,
                &mut stats,
            );

            if rc != FASTTREE_OK {
                let err_msg = CStr::from_ptr(fasttree_last_error(ctx))
                    .to_string_lossy()
                    .into_owned();
                let code_msg = CStr::from_ptr(fasttree_strerror(rc))
                    .to_string_lossy()
                    .into_owned();
                fasttree_destroy(ctx);
                return Response::error(format!("{code_msg}: {err_msg}"));
            }

            // Write tree as Arrow IPC to a new output shared memory region
            let shm_out = match Self::write_output(&*tree) {
                Ok(v) => v,
                Err(e) => {
                    fasttree_tree_soa_free(tree);
                    fasttree_destroy(ctx);
                    return Response::error(e);
                }
            };

            // Build lightweight stats for JSON response
            let result = serde_json::json!({
                "n_nodes": (*tree).n_nodes,
                "n_leaves": (*tree).n_leaves,
                "root": (*tree).root,
                "stats": {
                    "n_unique_seqs": stats.n_unique_seqs,
                    "log_likelihood": stats.log_likelihood,
                    "gamma_log_lk": stats.gamma_log_lk,
                    "n_nni": stats.n_nni,
                    "n_spr": stats.n_spr,
                    "n_ml_nni": stats.n_ml_nni,
                }
            });

            fasttree_tree_soa_free(tree);
            fasttree_destroy(ctx);

            Response::ok(result, vec![shm_out])
        }
    }
}

/// Convert fasttree SOA tree to an Arrow RecordBatch.
///
/// Sentinel translation (schema v2):
/// - `parent[i] == -1` (root) → NULL `parent_index`, NULL `edge_id`.
/// - `parent[i] != -1` → `parent_index = parent[i] as i64`,
///   `edge_id = i as i64` (one inbound edge per non-root node;
///   miint joins downstream tables on `edge_id`).
/// - `branch_length[i].is_nan()` → NULL.
/// - `support[i] < 0.0` → NULL (FastTree emits -1 when support was not
///   computed; we treat any negative as "absent" since SH-like support
///   is bounded `[0, 1]`).
unsafe fn soa_to_record_batch(tree: &FastTreeSoa) -> Result<RecordBatch, String> {
    let n = tree.n_nodes as usize;

    let parent = std::slice::from_raw_parts(tree.parent, n);
    let branch_length = std::slice::from_raw_parts(tree.branch_length, n);
    let support = std::slice::from_raw_parts(tree.support, n);
    let n_children = std::slice::from_raw_parts(tree.n_children, n);
    let is_leaf_raw = std::slice::from_raw_parts(tree.is_leaf, n);
    let name_ptrs = std::slice::from_raw_parts(tree.name, n);

    let node_index: Vec<i64> = (0..n as i64).collect();

    let mut parent_index = Int64Builder::with_capacity(n);
    let mut edge_id = Int64Builder::with_capacity(n);
    for (i, &p) in parent.iter().enumerate() {
        if p < 0 {
            parent_index.append_null();
            edge_id.append_null();
        } else {
            parent_index.append_value(p as i64);
            edge_id.append_value(i as i64);
        }
    }

    // Asymmetric NULL handling between branch_length and support is
    // deliberate. The C header (ext/fasttree/fasttree.h) documents an
    // explicit `-1` "not computed" sentinel for support, and SH-like
    // support is bounded `[0, 1]` by construction — so negative values
    // (any of them, not just -1) plus NaN map to NULL. branch_length
    // has no documented sentinel; FastTree can produce negative branch
    // lengths in pathological cases (NJ degenerate inputs) and those
    // are real data, not "absent" — only NaN maps to NULL.
    let mut bl = Float64Builder::with_capacity(n);
    for &v in branch_length {
        if v.is_nan() {
            bl.append_null();
        } else {
            bl.append_value(v);
        }
    }

    let mut sup = Float64Builder::with_capacity(n);
    for &v in support {
        if v < 0.0 || v.is_nan() {
            sup.append_null();
        } else {
            sup.append_value(v);
        }
    }

    let names: Vec<Option<String>> = name_ptrs
        .iter()
        .map(|&p| {
            if p.is_null() {
                None
            } else {
                Some(CStr::from_ptr(p).to_string_lossy().into_owned())
            }
        })
        .collect();

    let schema = Arc::new(output_schema());

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(node_index)),
            Arc::new(parent_index.finish()),
            Arc::new(edge_id.finish()),
            Arc::new(bl.finish()),
            Arc::new(sup.finish()),
            Arc::new(Int32Array::from(n_children.to_vec())),
            Arc::new(BooleanArray::from(
                is_leaf_raw.iter().map(|&v| v != 0).collect::<Vec<bool>>(),
            )),
            Arc::new(StringArray::from(
                names
                    .iter()
                    .map(|n| n.as_deref())
                    .collect::<Vec<Option<&str>>>(),
            )),
        ],
    )
    .map_err(|e| format!("Failed to create Arrow RecordBatch: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shm::SharedMemory;
    use crate::test_util::{
        extracted_16s_phylip, parse_interleaved_phylip, read_arrow_from_shm, subset_alignment,
        unique_shm_name, write_arrow_to_shm,
    };
    use arrow::array::{Array, Float64Array};

    // Test-only FFI binding for `fasttree_tree_soa_to_newick`.
    // Used by parity tests to serialize a library-built SOA tree into the
    // canonical FastTree Newick string for byte-equal comparison against a
    // hardcoded native-FastTree expected output.
    extern "C" {
        fn fasttree_tree_soa_to_newick(
            tree: *const FastTreeSoa,
            show_support: c_int,
        ) -> *mut c_char;
    }

    /// Serialize a SOA tree to the FastTree-canonical Newick string.
    ///
    /// Safety: `libc::free` here is sound only because our adapter never
    /// wires `FastTreeConfig.alloc_fn` (see `execute()` — the config
    /// struct is zeroed + `fasttree_config_init`'d, never modified to
    /// install a custom allocator). The non-SOA `fasttree_tree_to_newick`
    /// at `ext/fasttree/fasttree.h:288` documents "Returns malloc'd string
    /// (caller frees with free())"; the SOA variant at line 305 omits the
    /// guarantee but observably matches. If a future change ever wires
    /// `alloc_fn`, this free becomes UB.
    unsafe fn soa_to_newick_via_c(tree: &FastTreeSoa, show_support: bool) -> String {
        let flag: c_int = if show_support { 1 } else { 0 };
        let p = fasttree_tree_soa_to_newick(tree as *const _, flag);
        assert!(!p.is_null(), "fasttree_tree_soa_to_newick returned NULL");
        let s = CStr::from_ptr(p).to_string_lossy().into_owned();
        libc::free(p as *mut c_void);
        s
    }

    fn make_input_batch(names: &[&str], sequences: &[&str]) -> RecordBatch {
        let schema = Arc::new(input_schema());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(sequences.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_input_batch_owned(alignment: &[(String, String)]) -> RecordBatch {
        let schema = Arc::new(input_schema());
        let names: Vec<&str> = alignment.iter().map(|(n, _)| n.as_str()).collect();
        let seqs: Vec<&str> = alignment.iter().map(|(_, s)| s.as_str()).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(seqs)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_fasttree_nucleotide_roundtrip() {
        let input_name = unique_shm_name("ft-nt-in");

        let batch = make_input_batch(
            &["SeqA", "SeqB", "SeqC", "SeqD"],
            &[
                "ACGTACGTACGTACGTACGT",
                "ACGTACGTACGTACGTACGA",
                "TGCATGCATGCATGCATGCA",
                "TGCATGCATGCATGCATGCG",
            ],
        );

        // Write input to shm (simulating what miint does)
        let _input_shm = write_arrow_to_shm(&input_name, &batch);

        let tool = FastTreeTool;
        let config = serde_json::json!({
            "seq_type": "nucleotide",
            "seed": 12345
        });

        let response = tool.execute(&config, &input_name);
        assert!(response.success, "FastTree failed: {:?}", response.error);

        // Response should contain output shm info
        assert_eq!(response.shm_outputs.len(), 1);
        let output_name = &response.shm_outputs[0].name;
        let output_size = response.shm_outputs[0].size;
        assert!(output_size > 0);
        assert!(output_name.starts_with("/gb-"));
        assert_eq!(response.shm_outputs[0].label, "tree");

        // Verify JSON metadata
        let result = response.result.unwrap();
        assert_eq!(result["n_leaves"], 4);
        assert!(result["n_nodes"].as_i64().unwrap() >= 4);
        assert!(result["stats"]["log_likelihood"].as_f64().unwrap() < 0.0);

        // Verify Arrow IPC output in shared memory
        let out_batches = read_arrow_from_shm(output_name);
        assert_eq!(out_batches.len(), 1);
        let out = &out_batches[0];

        let n_nodes = result["n_nodes"].as_i64().unwrap() as usize;
        assert_eq!(out.num_rows(), n_nodes);
        assert_eq!(
            out.num_columns(),
            8,
            "schema v2 has 8 columns including edge_id"
        );

        // Check column names + types match schema v2.
        let schema = out.schema();
        assert_eq!(schema.field(0).name(), "node_index");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "parent_index");
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert!(schema.field(1).is_nullable());
        assert_eq!(schema.field(2).name(), "edge_id");
        assert!(schema.field(2).is_nullable());
        assert_eq!(schema.field(3).name(), "branch_length");
        assert!(schema.field(3).is_nullable());
        assert_eq!(schema.field(4).name(), "support");
        assert!(schema.field(4).is_nullable());
        assert_eq!(schema.field(6).name(), "is_tip");
        assert_eq!(schema.field(7).name(), "name");

        // Count tips in output
        let is_tip = out
            .column_by_name("is_tip")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let tip_count = (0..is_tip.len()).filter(|&i| is_tip.value(i)).count();
        assert_eq!(tip_count, 4);

        // Verify column 5 (`n_children`) is at the right offset and has
        // tree-shape consistency: every tip has 0 children; the sum of
        // child counts equals `n_nodes - 1` (every non-root node is
        // someone's child exactly once).
        assert_eq!(out.schema().field(5).name(), "n_children");
        assert_eq!(out.schema().field(5).data_type(), &DataType::Int32);
        let n_children = out
            .column_by_name("n_children")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut total_children: i64 = 0;
        for i in 0..n_nodes {
            let c = n_children.value(i);
            if is_tip.value(i) {
                assert_eq!(c, 0, "tip {i} should have 0 children, got {c}");
            }
            total_children += c as i64;
        }
        assert_eq!(
            total_children,
            (n_nodes - 1) as i64,
            "sum of n_children should equal n_nodes - 1 in any rooted tree"
        );

        // Root must have NULL parent_index and NULL edge_id; every
        // other node must have non-null values in both columns.
        let root = result["root"].as_i64().unwrap() as usize;
        let parent_index = out
            .column_by_name("parent_index")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let edge_id = out
            .column_by_name("edge_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            parent_index.is_null(root),
            "Root's parent_index must be NULL (no -1 sentinel)"
        );
        assert!(edge_id.is_null(root), "Root's edge_id must be NULL");
        for i in 0..n_nodes {
            if i == root {
                continue;
            }
            assert!(
                !parent_index.is_null(i),
                "Non-root node {i} parent_index must not be NULL"
            );
            assert!(
                !edge_id.is_null(i),
                "Non-root node {i} edge_id must not be NULL"
            );
            assert_eq!(
                edge_id.value(i),
                i as i64,
                "edge_id convention: == node_index for non-root nodes"
            );
        }

        // Caller cleans up output shm (simulating what miint does)
        let _ = SharedMemory::unlink(output_name);
    }

    #[test]
    fn test_fasttree_protein_roundtrip() {
        let input_name = unique_shm_name("ft-aa-in");

        let batch = make_input_batch(
            &["ProtA", "ProtB", "ProtC", "ProtD"],
            &["ARNDCQEGHI", "ARNDCQEGHL", "LKMFPSTWYV", "LKMFPSTWYA"],
        );

        let _input_shm = write_arrow_to_shm(&input_name, &batch);

        let tool = FastTreeTool;
        let config = serde_json::json!({
            "seq_type": "protein",
            "seed": 12345
        });

        let response = tool.execute(&config, &input_name);
        assert!(
            response.success,
            "FastTree protein failed: {:?}",
            response.error
        );

        assert_eq!(response.shm_outputs.len(), 1);
        let output_name = &response.shm_outputs[0].name;
        let result = response.result.unwrap();
        assert_eq!(result["n_leaves"], 4);

        let out_batches = read_arrow_from_shm(output_name);
        assert_eq!(out_batches.len(), 1);
        assert_eq!(
            out_batches[0].num_rows(),
            result["n_nodes"].as_i64().unwrap() as usize
        );

        let _ = SharedMemory::unlink(output_name);
    }

    #[test]
    fn test_fasttree_bad_input_shm() {
        let tool = FastTreeTool;
        let config = serde_json::json!({"seq_type": "nucleotide"});

        let response = tool.execute(&config, "/nonexistent-shm-name");
        assert!(!response.success);
        assert!(response.error.unwrap().contains("Failed to open shm"));
    }

    /// Unit test for the schema-v2 sentinel→NULL emission path. Builds a
    /// minimal FastTreeSoa fixture with NaN/negative sentinels and feeds
    /// it through `soa_to_record_batch`. This is the only way to verify
    /// the conversion without invoking real fasttree (which never emits
    /// NaN in our current test sequences). No real C state is touched.
    #[test]
    fn test_soa_to_record_batch_sentinel_translation() {
        // 4-node fixture exercising every NULL-emission path:
        //   Leaf 0 (parent=root): NaN branch_length → NULL; support 0.95 (real).
        //   Leaf 1 (parent=root): 0.5 branch_length; support -1 → NULL (negative sentinel).
        //   Leaf 2 (parent=root): 0.7 branch_length; support NaN → NULL.
        //   Root  (idx 3, parent=-1): branch_length 0.0; support -1 → NULL.
        let parent: [c_int; 4] = [3, 3, 3, -1];
        let branch_length: [f64; 4] = [f64::NAN, 0.5, 0.7, 0.0];
        let support: [f64; 4] = [0.95, -1.0, f64::NAN, -1.0];
        let n_children: [c_int; 4] = [0, 0, 0, 3];
        let is_leaf: [c_int; 4] = [1, 1, 1, 0];
        let name_a = CString::new("leaf_a").unwrap();
        let name_b = CString::new("leaf_b").unwrap();
        let name_c = CString::new("leaf_c").unwrap();
        let name_ptrs: [*const c_char; 4] = [
            name_a.as_ptr(),
            name_b.as_ptr(),
            name_c.as_ptr(),
            ptr::null(),
        ];

        let tree = FastTreeSoa {
            n_nodes: 4,
            n_leaves: 3,
            root: 3,
            parent: parent.as_ptr(),
            branch_length: branch_length.as_ptr(),
            support: support.as_ptr(),
            n_children: n_children.as_ptr(),
            is_leaf: is_leaf.as_ptr(),
            children_offset: ptr::null(),
            name: name_ptrs.as_ptr(),
            _children_buf: ptr::null(),
            _name_buf: ptr::null(),
            _base: ptr::null_mut(),
        };

        let batch = unsafe { soa_to_record_batch(&tree) }.unwrap();
        assert_eq!(batch.num_rows(), 4);

        let parent_index = batch
            .column_by_name("parent_index")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let edge_id = batch
            .column_by_name("edge_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bl = batch
            .column_by_name("branch_length")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sup = batch
            .column_by_name("support")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Root (idx 3): parent_index NULL, edge_id NULL, support NULL.
        assert!(parent_index.is_null(3));
        assert!(edge_id.is_null(3));
        assert!(sup.is_null(3));
        assert!(!bl.is_null(3));
        assert_eq!(bl.value(3), 0.0);

        // Leaf 0: NaN branch_length → NULL; support 0.95 → 0.95.
        assert_eq!(parent_index.value(0), 3);
        assert_eq!(edge_id.value(0), 0);
        assert!(bl.is_null(0), "NaN branch_length must become NULL");
        assert!(!sup.is_null(0));
        assert_eq!(sup.value(0), 0.95);

        // Leaf 1: branch_length 0.5; support -1 → NULL.
        assert_eq!(parent_index.value(1), 3);
        assert_eq!(edge_id.value(1), 1);
        assert!(!bl.is_null(1));
        assert_eq!(bl.value(1), 0.5);
        assert!(sup.is_null(1), "negative support must become NULL");

        // Leaf 2: branch_length 0.7; support NaN → NULL.
        assert_eq!(parent_index.value(2), 3);
        assert_eq!(edge_id.value(2), 2);
        assert!(!bl.is_null(2));
        assert_eq!(bl.value(2), 0.7);
        assert!(sup.is_null(2), "NaN support must become NULL");
    }

    /// Verify that the Rust #[repr(C)] FastTreeConfig has the same size as the
    /// C fasttree_config_t. The C side sets struct_size in config_init(), so a
    /// mismatch here means the Rust struct definition has drifted from the C header.
    ///
    /// Limitation: this catches added/removed fields but NOT field reordering or
    /// type changes that preserve size (e.g., swapping two c_int fields). Manual
    /// review against the C header is still required when fields change.
    ///
    /// Every new tool should add an equivalent test for its config struct.
    #[test]
    fn test_config_struct_abi_size() {
        let mut config: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut config) };
        assert_eq!(
            config.struct_size,
            std::mem::size_of::<FastTreeConfig>(),
            "ABI mismatch: Rust FastTreeConfig ({} bytes) vs C fasttree_config_t ({} bytes). \
             Check field types and padding against ext/fasttree/fasttree.h.",
            std::mem::size_of::<FastTreeConfig>(),
            config.struct_size,
        );
    }

    /// Phase 0 smoke test: build a tree from the first 50 sequences of
    /// `ext/fasttree/16S_500.tar.gz`'s `16S.1.p` PHYLIP file under the
    /// current 6-param adapter surface. No native-FastTree parity yet —
    /// that arrives in Phase 1 alongside each new wired knob.
    ///
    /// Validates: extraction works, the PHYLIP parser feeds clean input
    /// to the adapter, the tree-shape invariants hold, and the test-only
    /// `fasttree_tree_soa_to_newick` FFI returns a non-empty Newick.
    #[test]
    fn test_fasttree_16s_50seq_smoke() {
        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        assert_eq!(alignment.len(), 50);

        let input_name = unique_shm_name("ft-16s-50");
        let batch = make_input_batch_owned(&alignment);
        let _input_shm = write_arrow_to_shm(&input_name, &batch);

        let tool = FastTreeTool;
        let config = serde_json::json!({
            "seq_type": "nucleotide",
            "seed": 12345,
        });

        let response = tool.execute(&config, &input_name);
        assert!(
            response.success,
            "16S smoke: FastTree failed: {:?}",
            response.error
        );

        let result = response.result.expect("result metadata");
        let n_leaves = result["n_leaves"].as_i64().expect("n_leaves") as usize;
        let n_nodes = result["n_nodes"].as_i64().expect("n_nodes") as usize;
        // FastTree dedups identical sequences; in practice the 16S.1.p
        // subset is unique, but allow the equality slack just in case.
        assert!(
            n_leaves > 0 && n_leaves <= 50,
            "n_leaves out of range: {n_leaves}"
        );
        assert!(n_nodes >= n_leaves);

        // Tree-shape invariant: sum of n_children == n_nodes - 1
        // (every non-root node is someone's child exactly once).
        let output_name = &response.shm_outputs[0].name;
        let out_batches = read_arrow_from_shm(output_name);
        let out = &out_batches[0];
        let n_children = out
            .column_by_name("n_children")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let total: i64 = (0..n_nodes).map(|i| n_children.value(i) as i64).sum();
        assert_eq!(total, (n_nodes - 1) as i64);

        let _ = SharedMemory::unlink(output_name);
    }

    /// Phase 0 sanity: `fasttree_tree_soa_to_newick` round-trips the
    /// in-process tree to a Newick string. Establishes that the
    /// test-only FFI binding works end-to-end before Phase 1 starts
    /// using it for parity comparisons.
    #[test]
    fn test_soa_to_newick_via_c_smoke() {
        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 20);
        let names: Vec<CString> = alignment
            .iter()
            .map(|(n, _)| CString::new(n.as_str()).unwrap())
            .collect();
        let seqs: Vec<CString> = alignment
            .iter()
            .map(|(_, s)| CString::new(s.as_str()).unwrap())
            .collect();
        let name_ptrs: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
        let seq_ptrs: Vec<*const c_char> = seqs.iter().map(|s| s.as_ptr()).collect();

        unsafe {
            let mut cfg: FastTreeConfig = std::mem::zeroed();
            fasttree_config_init(&mut cfg);
            cfg.seq_type = 2; // nucleotide
            cfg.seed = 12345;
            let ctx = fasttree_create(&cfg);
            assert!(!ctx.is_null());

            let mut tree: *mut FastTreeSoa = ptr::null_mut();
            let mut stats: FastTreeStats = std::mem::zeroed();
            let rc = fasttree_build_soa(
                ctx,
                name_ptrs.as_ptr(),
                seq_ptrs.as_ptr(),
                alignment.len() as c_int,
                alignment[0].1.len() as c_int,
                &mut tree,
                &mut stats,
            );
            assert_eq!(rc, FASTTREE_OK);

            let newick = soa_to_newick_via_c(&*tree, false);
            assert!(newick.starts_with('('), "Newick should start with '('");
            assert!(newick.ends_with(";\n") || newick.ends_with(';'));
            // Every input name should appear (FastTree dedupes but the
            // 20-seq subset is unique in practice).
            for (n, _) in &alignment {
                assert!(newick.contains(n), "Newick missing leaf {n}");
            }

            fasttree_tree_soa_free(tree);
            fasttree_destroy(ctx);
        }
    }

    /// Build a tree from `alignment` by driving the C library directly
    /// (skipping the JSON decoder + Arrow IPC layers) and serialize the
    /// SOA to a Newick string via `fasttree_tree_soa_to_newick`. The
    /// `configure` closure runs after `fasttree_config_init`, allowing
    /// per-test knob overrides on top of the C library's defaults. The
    /// closure may return `Err` to abort before `fasttree_create`.
    ///
    /// Used by parity tests to compare bit-for-bit against native
    /// FastTree (conda 2.2.0) output captured offline. See
    /// `GUIDANCE_PARITY_TESTS.md` for the regen workflow.
    unsafe fn build_newick_via_c(
        alignment: &[(String, String)],
        configure: impl FnOnce(&mut FastTreeConfig) -> Result<(), String>,
        show_support: bool,
    ) -> Result<String, String> {
        let names: Vec<CString> = alignment
            .iter()
            .map(|(n, _)| CString::new(n.as_str()).unwrap())
            .collect();
        let seqs: Vec<CString> = alignment
            .iter()
            .map(|(_, s)| CString::new(s.as_str()).unwrap())
            .collect();
        let name_ptrs: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
        let seq_ptrs: Vec<*const c_char> = seqs.iter().map(|s| s.as_ptr()).collect();

        let mut cfg: FastTreeConfig = std::mem::zeroed();
        fasttree_config_init(&mut cfg);
        configure(&mut cfg)?;

        let ctx = fasttree_create(&cfg);
        if ctx.is_null() {
            return Err("fasttree_create returned NULL".into());
        }

        let mut tree: *mut FastTreeSoa = ptr::null_mut();
        let mut stats: FastTreeStats = std::mem::zeroed();
        let rc = fasttree_build_soa(
            ctx,
            name_ptrs.as_ptr(),
            seq_ptrs.as_ptr(),
            alignment.len() as c_int,
            alignment[0].1.len() as c_int,
            &mut tree,
            &mut stats,
        );
        if rc != FASTTREE_OK {
            let err = CStr::from_ptr(fasttree_last_error(ctx))
                .to_string_lossy()
                .into_owned();
            fasttree_destroy(ctx);
            return Err(format!("fasttree_build_soa failed: {err}"));
        }

        let newick = soa_to_newick_via_c(&*tree, show_support);

        fasttree_tree_soa_free(tree);
        fasttree_destroy(ctx);
        Ok(newick)
    }

    /// Drive the production JSON decoder + the C library + the test-only
    /// Newick serializer. Per-knob parity tests use this to validate that
    /// `apply_json_to_config` correctly maps JSON knobs onto the
    /// `FastTreeConfig` fields the C library acts on.
    unsafe fn build_newick_via_json(
        alignment: &[(String, String)],
        config_json: &serde_json::Value,
        show_support: bool,
    ) -> Result<String, String> {
        build_newick_via_c(
            alignment,
            |cfg| apply_json_to_config(config_json, cfg),
            show_support,
        )
    }

    /// Native-FastTree parity baseline: 50-sequence subset of
    /// `ext/fasttree/16S_500.tar.gz`'s `16S.1.p`, default config + fixed
    /// seed, support values included.
    ///
    /// Regenerated with conda's `fasttree=2.2.0`:
    ///   conda run -n fasttree FastTree -nt -seed 12345 \
    ///       /tmp/16S.1.50seq.fasta
    /// where `/tmp/16S.1.50seq.fasta` was produced by
    ///   python3 scripts/subset-phylip-to-fasta.py 50 \
    ///       < target/fasttree-testdata/16S.1.p
    ///
    /// If this test ever drifts, our linked `ext/fasttree` library has
    /// diverged from upstream behavior — investigate before regenerating.
    /// (See `REPORT-fasttree-library-cli-divergence.md` and
    /// `REPORT-fasttree-cfcfd94-thread-safety-regression.md` for the
    /// 2026-04-28 round-trip with the FastTree submodule team that
    /// established this parity in the first place.)
    #[test]
    fn test_fasttree_50seq_parity_baseline() {
        const EXPECTED: &str = "((10607:0.108728183,1828:0.122617756)0.968:0.029784172,((((112138:0.033883874,11326:0.060069834)1.000:0.139823276,(72728:0.469274735,(2181:0.170590261,(77434:0.082689947,(99565:0.092547907,101540:0.104777748)0.801:0.027296752)0.978:0.045947821)0.997:0.073100991)0.890:0.044420886)0.922:0.017371404,((105325:0.067027677,11179:0.058740204)1.000:0.135281789,(((109612:0.038444192,(52575:0.041602501,19103:0.029245209)0.825:0.010775536)1.000:0.071852375,((69848:0.052322287,(8071:0.038960071,((102957:0.036993227,(37204:0.032886181,(9944:0.010841675,9669:0.016835590)0.944:0.009614309)1.000:0.031400631)0.969:0.017801804,(75690:0.052930806,114738:0.048955244)0.188:0.004986093)1.000:0.051618818)0.926:0.026253833)0.890:0.016831614,72638:0.036015850)0.501:0.012705519)0.999:0.048395482,(111880:0.082925522,(29930:0.067479705,((100639:0.061363527,(54630:0.028632074,65474:0.058988375)0.981:0.023574978)0.063:0.007656523,(89315:0.034813563,(5903:0.034324338,101793:0.046061462)1.000:0.116234675)0.461:0.010309408)0.995:0.029255734)0.611:0.023606761)1.000:0.038934525)0.293:0.011670275)0.322:0.015313215)0.916:0.013617425,38854:0.119745452)0.925:0.012283122,(((109556:0.094712097,(83619:0.093304494,(40531:0.116098375,104854:0.036481237)0.988:0.026154190)0.861:0.013688556)0.964:0.022623573,(16077:0.122036067,(92528:0.125166108,(102607:0.120186435,(84810:0.012379015,3353:0.009442461)1.000:0.064244782)1.000:0.061086276)0.614:0.021412930)0.841:0.024118426)0.688:0.017162443,((103543:0.090887321,(78515:0.081205030,114176:0.179495854)0.994:0.040873948)0.997:0.048795167,(114317:0.128771221,((100492:0.037183916,104661:0.039529991)0.251:0.007801462,((25310:0.014386300,100957:0.035791254)0.877:0.005488228,22197:0.059582638)0.602:0.005619043)1.000:0.080180535)0.091:0.017948287)0.500:0.017997704)0.921:0.015062351);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    // === Phase 1, knob 1: bootstrap + nosupport ===

    /// `bootstrap=100` overrides the C default of 1000. Smaller resample
    /// count → different SH-like support values; topology + branch lengths
    /// are unchanged because bootstrap only affects support computation.
    ///
    /// Regenerated with:
    ///   conda run -n fasttree FastTree -nt -seed 12345 -boot 100 \
    ///       /tmp/16S.1.50seq.fasta
    #[test]
    fn test_bootstrap_100_parity() {
        const EXPECTED: &str = "((10607:0.108728183,1828:0.122617756)0.940:0.029784172,((((112138:0.033883874,11326:0.060069834)1.000:0.139823276,(72728:0.469274735,(2181:0.170590261,(77434:0.082689947,(99565:0.092547907,101540:0.104777748)0.880:0.027296752)0.980:0.045947821)1.000:0.073100991)0.910:0.044420886)0.930:0.017371404,((105325:0.067027677,11179:0.058740204)1.000:0.135281789,(((109612:0.038444192,(52575:0.041602501,19103:0.029245209)0.830:0.010775536)1.000:0.071852375,((69848:0.052322287,(8071:0.038960071,((102957:0.036993227,(37204:0.032886181,(9944:0.010841675,9669:0.016835590)0.940:0.009614309)1.000:0.031400631)0.980:0.017801804,(75690:0.052930806,114738:0.048955244)0.160:0.004986093)1.000:0.051618818)0.910:0.026253833)0.940:0.016831614,72638:0.036015850)0.520:0.012705519)1.000:0.048395482,(111880:0.082925522,(29930:0.067479705,((100639:0.061363527,(54630:0.028632074,65474:0.058988375)0.990:0.023574978)0.100:0.007656523,(89315:0.034813563,(5903:0.034324338,101793:0.046061462)1.000:0.116234675)0.440:0.010309408)0.990:0.029255734)0.560:0.023606761)1.000:0.038934525)0.290:0.011670275)0.350:0.015313215)0.920:0.013617425,38854:0.119745452)0.930:0.012283122,(((109556:0.094712097,(83619:0.093304494,(40531:0.116098375,104854:0.036481237)0.990:0.026154190)0.830:0.013688556)0.950:0.022623573,(16077:0.122036067,(92528:0.125166108,(102607:0.120186435,(84810:0.012379015,3353:0.009442461)1.000:0.064244782)1.000:0.061086276)0.620:0.021412930)0.800:0.024118426)0.680:0.017162443,((103543:0.090887321,(78515:0.081205030,114176:0.179495854)0.990:0.040873948)0.990:0.048795167,(114317:0.128771221,((100492:0.037183916,104661:0.039529991)0.330:0.007801462,((25310:0.014386300,100957:0.035791254)0.910:0.005488228,22197:0.059582638)0.590:0.005619043)1.000:0.080180535)0.110:0.017948287)0.480:0.017997704)0.940:0.015062351);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "bootstrap": 100}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    /// `nosupport=true` synthesizes `n_bootstrap = 0` → output Newick has
    /// no support annotations.
    ///
    /// Regenerated with:
    ///   conda run -n fasttree FastTree -nt -seed 12345 -nosupport \
    ///       /tmp/16S.1.50seq.fasta
    #[test]
    fn test_nosupport_parity() {
        const EXPECTED: &str = "((10607:0.108728183,1828:0.122617756):0.029784172,((((112138:0.033883874,11326:0.060069834):0.139823276,(72728:0.469274735,(2181:0.170590261,(77434:0.082689947,(99565:0.092547907,101540:0.104777748):0.027296752):0.045947821):0.073100991):0.044420886):0.017371404,((105325:0.067027677,11179:0.058740204):0.135281789,(((109612:0.038444192,(52575:0.041602501,19103:0.029245209):0.010775536):0.071852375,((69848:0.052322287,(8071:0.038960071,((102957:0.036993227,(37204:0.032886181,(9944:0.010841675,9669:0.016835590):0.009614309):0.031400631):0.017801804,(75690:0.052930806,114738:0.048955244):0.004986093):0.051618818):0.026253833):0.016831614,72638:0.036015850):0.012705519):0.048395482,(111880:0.082925522,(29930:0.067479705,((100639:0.061363527,(54630:0.028632074,65474:0.058988375):0.023574978):0.007656523,(89315:0.034813563,(5903:0.034324338,101793:0.046061462):0.116234675):0.010309408):0.029255734):0.023606761):0.038934525):0.011670275):0.015313215):0.013617425,38854:0.119745452):0.012283122,(((109556:0.094712097,(83619:0.093304494,(40531:0.116098375,104854:0.036481237):0.026154190):0.013688556):0.022623573,(16077:0.122036067,(92528:0.125166108,(102607:0.120186435,(84810:0.012379015,3353:0.009442461):0.064244782):0.061086276):0.021412930):0.024118426):0.017162443,((103543:0.090887321,(78515:0.081205030,114176:0.179495854):0.040873948):0.048795167,(114317:0.128771221,((100492:0.037183916,104661:0.039529991):0.007801462,((25310:0.014386300,100957:0.035791254):0.005488228,22197:0.059582638):0.005619043):0.080180535):0.017948287):0.017997704):0.015062351);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "nosupport": true}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    /// `bootstrap=0` is exactly equivalent to `nosupport=true` — same C
    /// field set to 0, same Newick output.
    #[test]
    fn test_bootstrap_zero_eq_nosupport() {
        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let nosupport = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "nosupport": true}),
                true,
            )
        }
        .unwrap();
        let bootstrap_zero = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "bootstrap": 0}),
                true,
            )
        }
        .unwrap();
        assert_eq!(nosupport, bootstrap_zero);
    }

    /// `bootstrap > 0 && nosupport == true` is a config conflict; the
    /// adapter rejects with a clear error before invoking FastTree.
    #[test]
    fn test_bootstrap_nosupport_conflict_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(
            &serde_json::json!({"bootstrap": 100, "nosupport": true}),
            &mut cfg,
        )
        .unwrap_err();
        assert!(
            err.contains("conflict") && err.contains("bootstrap") && err.contains("nosupport"),
            "expected a clear conflict error mentioning bootstrap and nosupport, got: {err}"
        );
    }

    /// Negative `bootstrap` is a hard error.
    #[test]
    fn test_bootstrap_negative_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err =
            apply_json_to_config(&serde_json::json!({"bootstrap": -1}), &mut cfg).unwrap_err();
        assert!(
            err.contains("bootstrap"),
            "expected 'bootstrap' in error, got: {err}"
        );
    }
}
