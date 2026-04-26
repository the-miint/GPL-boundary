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

        let seq_type = match config.get("seq_type").and_then(|v| v.as_str()) {
            Some("nucleotide") => 2,
            Some("protein") => 1,
            _ => 0,
        };
        let seed = config
            .get("seed")
            .and_then(|v| v.as_i64())
            .unwrap_or(314159);
        let verbose = config
            .get("verbose")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        unsafe {
            let mut ft_config: FastTreeConfig = std::mem::zeroed();
            fasttree_config_init(&mut ft_config);
            ft_config.seq_type = seq_type;
            ft_config.seed = seed;

            if verbose {
                ft_config.log_callback = Some(stderr_log_callback);
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
    use crate::test_util::{read_arrow_from_shm, unique_shm_name, write_arrow_to_shm};
    use arrow::array::{Array, Float64Array};

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
}
