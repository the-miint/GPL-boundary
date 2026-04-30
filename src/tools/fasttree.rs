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

/// Convert a JSON-decoded `i64` into the `c_int` width the C library
/// expects, with a positive overflow guard. Negative-value range checks
/// remain per-knob (some knobs accept 0 as "auto sentinel", others
/// require strictly positive).
fn checked_cint(name: &str, v: i64) -> Result<c_int, String> {
    if v > c_int::MAX as i64 {
        return Err(format!(
            "{name}={v} exceeds C int range (max {})",
            c_int::MAX
        ));
    }
    if v < c_int::MIN as i64 {
        return Err(format!("{name}={v} below C int range (min {})", c_int::MIN));
    }
    Ok(v as c_int)
}

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
/// - `pseudo`: bool. Gates the `pseudo_weight` knob. true → enable
///   pseudocounts (default weight 1.0 if `pseudo_weight` is absent).
///   false → disable (`pseudo_weight = 0.0`, the C default).
/// - `pseudo_weight`: f64 ≥ 0. Only meaningful with `pseudo = true`;
///   setting it without explicitly enabling pseudo is an error.
/// - `nni`: i64 ≥ 0 → `nni_rounds`. Absent/null = auto (C default -1,
///   which the C library expands to `4*log2(N)`). Negative explicit
///   values are rejected — use absent for "auto" instead of `-1`.
/// - `spr`: i64 ≥ 0 → `spr_rounds`. Absent = C default (2). Negative
///   values rejected.
/// - `mlnni`: i64 ≥ 0 → `ml_nni_rounds`. Absent/null = auto (C default
///   -1, which the C library expands to `2*log2(N)`). 0 = disable ML
///   NNI. Negative explicit values are rejected.
/// - `mlacc`: i64 ≥ 1 → `ml_accuracy`. Absent = C default (1).
///   Higher values request more rounds of ML branch-length
///   optimization.
/// - `cat`: i64 ≥ 1 → `n_rate_cats`. Absent = C default (20).
/// - `noml`: bool. true → `ml_nni_rounds = 0` (synthesizes
///   "disable ML NNI"). Mutually exclusive with explicit `mlnni > 0`.
/// - `threads`: i64 ≥ 1 → `n_threads`. Absent = 1 (the adapter
///   overrides the C library's default of 0, which would otherwise
///   consult `OMP_NUM_THREADS`). Reproducibility-by-default;
///   parallelism is opt-in. Values >1 trade topology determinism
///   for speed (FastTree's NNI/SPR phases produce floating-point
///   non-determinism across thread counts).
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
    // Conflict check runs first so users hitting both errors at once see
    // the more useful message. (`{nosupport: true, bootstrap: -1}` would
    // otherwise mask the conflict behind a range error.)
    let bootstrap = config.get("bootstrap").and_then(|v| v.as_i64());
    let nosupport = config
        .get("nosupport")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if let Some(b) = bootstrap {
        if nosupport && b != 0 {
            return Err(format!(
                "config conflict: bootstrap={b} cannot be combined with nosupport=true \
                 (use bootstrap=0 or omit nosupport)"
            ));
        }
        if b < 0 {
            return Err(format!("bootstrap must be >= 0 (got {b})"));
        }
        ft_config.n_bootstrap = checked_cint("bootstrap", b)?;
    }
    if nosupport {
        ft_config.n_bootstrap = 0;
    }

    // pseudo + pseudo_weight
    let pseudo = config.get("pseudo").and_then(|v| v.as_bool());
    let pseudo_weight = config.get("pseudo_weight").and_then(|v| v.as_f64());
    if let Some(w) = pseudo_weight {
        if pseudo != Some(true) {
            return Err(format!(
                "config conflict: pseudo_weight={w} requires pseudo=true \
                 (set pseudo=true to enable, or omit pseudo_weight)"
            ));
        }
        if w < 0.0 {
            return Err(format!("pseudo_weight must be >= 0 (got {w})"));
        }
    }
    match pseudo {
        Some(true) => {
            ft_config.pseudo_weight = pseudo_weight.unwrap_or(1.0);
        }
        Some(false) => {
            ft_config.pseudo_weight = 0.0;
        }
        None => {} // leave at C default (0.0)
    }

    // nni
    // TODO(Phase 3 — fastest): when `fastest` lands, the upstream CLI's
    // `-fastest` clamps `nni_rounds = min(nni_rounds, 2)`. Decide whether
    // to replicate that or reject `{fastest: true, nni > 2}` explicitly
    // so users don't get silently downgraded.
    if let Some(v) = config.get("nni").and_then(|v| v.as_i64()) {
        if v < 0 {
            return Err(format!(
                "nni must be >= 0 (got {v}); omit the field to use the C library's auto default"
            ));
        }
        ft_config.nni_rounds = checked_cint("nni", v)?;
    }

    // spr
    if let Some(v) = config.get("spr").and_then(|v| v.as_i64()) {
        if v < 0 {
            return Err(format!("spr must be >= 0 (got {v})"));
        }
        ft_config.spr_rounds = checked_cint("spr", v)?;
    }

    // mlnni + noml
    let mlnni = config.get("mlnni").and_then(|v| v.as_i64());
    let noml = config
        .get("noml")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if let Some(v) = mlnni {
        if v < 0 {
            return Err(format!(
                "mlnni must be >= 0 (got {v}); omit the field to use the C library's auto default"
            ));
        }
        if noml && v > 0 {
            return Err(format!(
                "config conflict: mlnni={v} cannot be combined with noml=true \
                 (use mlnni=0 or omit noml)"
            ));
        }
        ft_config.ml_nni_rounds = checked_cint("mlnni", v)?;
    }
    if noml {
        ft_config.ml_nni_rounds = 0;
    }

    // mlacc
    if let Some(v) = config.get("mlacc").and_then(|v| v.as_i64()) {
        if v < 1 {
            return Err(format!("mlacc must be >= 1 (got {v})"));
        }
        ft_config.ml_accuracy = checked_cint("mlacc", v)?;
    }

    // cat
    if let Some(v) = config.get("cat").and_then(|v| v.as_i64()) {
        if v < 1 {
            return Err(format!("cat must be >= 1 (got {v})"));
        }
        ft_config.n_rate_cats = checked_cint("cat", v)?;
    }

    // threads
    // The C library's default `n_threads = 0` consults `OMP_NUM_THREADS`,
    // which would make adapter behavior depend on the daemon's
    // environment. We override to 1 unless the caller explicitly opts
    // in — single-thread is deterministic, multi-thread is not.
    let threads = config.get("threads").and_then(|v| v.as_i64()).unwrap_or(1);
    if threads < 1 {
        return Err(format!(
            "threads must be >= 1 (got {threads}); set explicitly or omit for default 1"
        ));
    }
    ft_config.n_threads = checked_cint("threads", threads)?;

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
                // NOTE: `model`, `fastest`, `gamma` were advertised here in the
                // initial 6-param describe() surface but their JSON paths were
                // never wired into the decoder. They are intentionally absent
                // until Phase 3 wires them with proper validation (model needs
                // gtrrates/gtrfreq companions; fastest interacts with nni —
                // see TODO in apply_json_to_config; gamma is observable only in
                // the response stats, not the tree).
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
                ConfigParam {
                    name: "pseudo",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Enable pseudocounts to dampen long-branch artifacts on highly gapped alignments. Gates pseudo_weight.",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "pseudo_weight",
                    param_type: "number",
                    default: serde_json::json!(1.0),
                    description: "Pseudocount weight (>= 0). Only meaningful when pseudo=true; default is 1.0 in that case.",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "nni",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "ME-NNI rounds (>= 0). Omit (or set to JSON null) to use the C library's auto default of 4*log2(N).",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "spr",
                    param_type: "integer",
                    default: serde_json::json!(2),
                    description: "SPR rounds (>= 0). The CLI's nni=0 side effect (which forces spr=0) is NOT replicated by the JSON adapter; set both knobs explicitly if both should be 0.",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "mlnni",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "ML-NNI rounds (>= 0). Omit (or null) to use the C library's auto default of 2*log2(N). 0 disables ML NNI entirely; equivalent to noml=true.",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "mlacc",
                    param_type: "integer",
                    default: serde_json::json!(1),
                    description: "ML branch-length optimization rounds (>= 1). Higher = more accurate.",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "cat",
                    param_type: "integer",
                    default: serde_json::json!(20),
                    description: "Number of CAT rate categories (>= 1).",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "noml",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Disable ML phase entirely (synthesizes mlnni=0). Mutually exclusive with explicit mlnni > 0.",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "threads",
                    param_type: "integer",
                    default: serde_json::json!(1),
                    description: "OpenMP thread count (>= 1). The adapter defaults to 1 for reproducibility — the C library's default of 0 (which consults OMP_NUM_THREADS) is intentionally not exposed. Values >1 parallelize NJ/NNI/SPR phases at the cost of topology determinism: FastTree's parallel sections produce floating-point non-determinism across thread counts, so identical seeds + identical inputs may yield different trees at threads > 1.",
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
        // alignment[0].1.len() below would panic on an empty slice;
        // surface that as an error instead.
        if alignment.is_empty() {
            return Err("build_newick_via_c: alignment is empty".into());
        }
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
    ///       target/scratch/16S.1.50seq.fasta
    /// where `target/scratch/16S.1.50seq.fasta` was produced by
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
    ///       target/scratch/16S.1.50seq.fasta
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
    ///       target/scratch/16S.1.50seq.fasta
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

    // === Phase 1, knob 2: pseudo + pseudo_weight ===

    /// `pseudo=true` with no explicit weight uses the default of 1.0.
    /// Verifies bit-equality against:
    ///   conda run -n fasttree FastTree -nt -seed 12345 -pseudo \
    ///       target/scratch/16S.1.50seq.fasta
    #[test]
    fn test_pseudo_default_weight_parity() {
        const EXPECTED: &str = "((10607:0.108728183,1828:0.122617756)0.968:0.029784172,((((112138:0.033883874,11326:0.060069834)1.000:0.139823276,(72728:0.469274735,(2181:0.170590261,(77434:0.082689947,(99565:0.092547907,101540:0.104777748)0.801:0.027296752)0.978:0.045947821)0.997:0.073100990)0.890:0.044420886)0.922:0.017371404,((105325:0.067027677,11179:0.058740204)1.000:0.135281789,(((109612:0.038444192,(52575:0.041602501,19103:0.029245209)0.825:0.010775536)1.000:0.071852374,((69848:0.052322286,(8071:0.038960072,((102957:0.036993231,(37204:0.032886181,(9944:0.010841675,9669:0.016835590)0.944:0.009614309)1.000:0.031400629)0.969:0.017801805,(75690:0.052930804,114738:0.048955243)0.188:0.004986094)1.000:0.051618817)0.926:0.026253832)0.890:0.016831614,72638:0.036015850)0.501:0.012705519)0.999:0.048395482,(111880:0.082925522,(29930:0.067479705,((100639:0.061363528,(54630:0.028632074,65474:0.058988375)0.981:0.023574978)0.063:0.007656523,(89315:0.034813563,(5903:0.034324338,101793:0.046061462)1.000:0.116234675)0.461:0.010309408)0.995:0.029255734)0.611:0.023606761)1.000:0.038934525)0.293:0.011670275)0.322:0.015313215)0.916:0.013617426,38854:0.119745450)0.925:0.012283122,(((109556:0.094712097,(83619:0.093304494,(40531:0.116098375,104854:0.036481237)0.988:0.026154190)0.861:0.013688556)0.964:0.022623573,(16077:0.122036067,(92528:0.125166108,(102607:0.120186435,(84810:0.012379015,3353:0.009442461)1.000:0.064244782)1.000:0.061086276)0.614:0.021412929)0.841:0.024118427)0.688:0.017162443,((103543:0.090887321,(78515:0.081205030,114176:0.179495854)0.994:0.040873947)0.997:0.048795167,(114317:0.128771221,((100492:0.037183915,104661:0.039529991)0.251:0.007801462,((25310:0.014386300,100957:0.035791255)0.877:0.005488228,22197:0.059582639)0.602:0.005619043)1.000:0.080180535)0.091:0.017948287)0.500:0.017997704)0.921:0.015062351);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "pseudo": true}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    /// `pseudo=true, pseudo_weight=2.5` overrides the default weight.
    /// Verifies bit-equality against:
    ///   conda run -n fasttree FastTree -nt -seed 12345 -pseudo 2.5 \
    ///       target/scratch/16S.1.50seq.fasta
    #[test]
    fn test_pseudo_weight_2_5_parity() {
        const EXPECTED: &str = "((10607:0.108728183,1828:0.122617756)0.968:0.029784172,((((112138:0.033883874,11326:0.060069834)1.000:0.139823275,(72728:0.469274735,(2181:0.170590261,(77434:0.082689947,(99565:0.092547907,101540:0.104777748)0.801:0.027296752)0.978:0.045947822)0.997:0.073100989)0.890:0.044420887)0.922:0.017371404,((105325:0.067027676,11179:0.058740204)1.000:0.135281789,(((109612:0.038444192,(52575:0.041602501,19103:0.029245209)0.825:0.010775535)1.000:0.071852374,((69848:0.052322284,(8071:0.038960075,((102957:0.036993238,(37204:0.032886181,(9944:0.010841674,9669:0.016835590)0.944:0.009614310)1.000:0.031400627)0.969:0.017801806,(75690:0.052930802,114738:0.048955243)0.188:0.004986095)1.000:0.051618816)0.926:0.026253832)0.890:0.016831614,72638:0.036015851)0.501:0.012705519)0.999:0.048395482,(111880:0.082925522,(29930:0.067479705,((100639:0.061363531,(54630:0.028632074,65474:0.058988375)0.981:0.023574978)0.063:0.007656523,(89315:0.034813563,(5903:0.034324338,101793:0.046061463)1.000:0.116234674)0.461:0.010309408)0.995:0.029255734)0.611:0.023606761)1.000:0.038934525)0.293:0.011670274)0.322:0.015313215)0.916:0.013617426,38854:0.119745448)0.925:0.012283122,(((109556:0.094712096,(83619:0.093304495,(40531:0.116098375,104854:0.036481237)0.988:0.026154190)0.861:0.013688556)0.964:0.022623572,(16077:0.122036068,(92528:0.125166108,(102607:0.120186434,(84810:0.012379015,3353:0.009442461)1.000:0.064244781)1.000:0.061086277)0.614:0.021412928)0.841:0.024118428)0.688:0.017162443,((103543:0.090887321,(78515:0.081205031,114176:0.179495854)0.994:0.040873947)0.997:0.048795167,(114317:0.128771221,((100492:0.037183915,104661:0.039529991)0.251:0.007801462,((25310:0.014386300,100957:0.035791255)0.877:0.005488228,22197:0.059582640)0.602:0.005619044)1.000:0.080180535)0.091:0.017948287)0.500:0.017997704)0.921:0.015062351);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "pseudo": true, "pseudo_weight": 2.5}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    /// `pseudo_weight` without `pseudo=true` is rejected.
    #[test]
    fn test_pseudo_weight_without_enable_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err =
            apply_json_to_config(&serde_json::json!({"pseudo_weight": 2.5}), &mut cfg).unwrap_err();
        assert!(
            err.contains("pseudo_weight") && err.contains("pseudo=true"),
            "expected error mentioning pseudo_weight + pseudo=true, got: {err}"
        );
    }

    /// `pseudo: false, pseudo_weight: ...` is also rejected (same conflict).
    #[test]
    fn test_pseudo_false_with_weight_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(
            &serde_json::json!({"pseudo": false, "pseudo_weight": 1.0}),
            &mut cfg,
        )
        .unwrap_err();
        assert!(
            err.contains("pseudo_weight") && err.contains("pseudo=true"),
            "expected error mentioning pseudo_weight + pseudo=true, got: {err}"
        );
    }

    /// Negative `pseudo_weight` is a hard error.
    #[test]
    fn test_pseudo_weight_negative_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(
            &serde_json::json!({"pseudo": true, "pseudo_weight": -0.5}),
            &mut cfg,
        )
        .unwrap_err();
        assert!(
            err.contains("pseudo_weight"),
            "expected 'pseudo_weight' in error, got: {err}"
        );
    }

    // === Phase 1, knob 3: nni ===

    /// `nni=0` disables ME-NNI rounds. The JSON `nni` knob is **literal**:
    /// it sets only `nni_rounds`. The upstream `FastTree` CLI's `-nni 0`
    /// has a side effect (`if (nni == 0) spr = 0;` at FastTree.c:1793-1794)
    /// that our JSON does NOT replicate — users who want both 0 must set
    /// both knobs explicitly.
    ///
    /// Regenerated with the matching explicit-spr CLI command:
    ///   conda run -n fasttree FastTree -nt -seed 12345 -nni 0 -spr 2 \
    ///       target/scratch/16S.1.50seq.fasta
    /// (`-spr 2` is the C default, exposed explicitly to bypass the CLI's
    /// nni=0-implies-spr=0 ergonomic shortcut.)
    #[test]
    fn test_nni_0_parity() {
        const EXPECTED: &str = "((10607:0.108727492,1828:0.122617398)0.968:0.029784799,((((112138:0.033885106,11326:0.060068663)1.000:0.139823967,(72728:0.469273144,(2181:0.170588596,(77434:0.082690265,(99565:0.092547454,101540:0.104778159)0.801:0.027297039)0.978:0.045948800)0.997:0.073101317)0.890:0.044420449)0.922:0.017370695,((105325:0.067029726,11179:0.058741248)1.000:0.135246333,(((109612:0.038445420,(52575:0.041635714,19103:0.029247013)0.825:0.010774281)1.000:0.071841341,(72638:0.036019104,(69848:0.052301378,(8071:0.038996215,((102957:0.037011804,(37204:0.032885573,(9944:0.010843985,9669:0.016841290)0.944:0.009616043)1.000:0.031377331)0.969:0.017795156,(75690:0.052939225,114738:0.048955598)0.188:0.004987029)1.000:0.051601455)0.926:0.026254638)0.889:0.016843267)0.499:0.012701688)0.999:0.048408226,(111880:0.082925256,(29930:0.067479603,((100639:0.061375677,(54630:0.028631254,65474:0.058989976)0.981:0.023574419)0.063:0.007656000,(89315:0.034812762,(5903:0.034323537,101793:0.046064524)1.000:0.116236342)0.461:0.010309296)0.995:0.029258136)0.610:0.023606872)1.000:0.038932815)0.293:0.011675856)0.322:0.015311587)0.916:0.013617207,38854:0.119745068)0.925:0.012283886,(((109556:0.094711935,(83619:0.093304769,(40531:0.116098457,104854:0.036481199)0.988:0.026154301)0.861:0.013688555)0.964:0.022624288,(16077:0.122036648,(92528:0.125166401,(102607:0.120186758,(84810:0.012379073,3353:0.009442396)1.000:0.064244762)1.000:0.061086316)0.614:0.021412278)0.841:0.024117056)0.688:0.017161374,((103543:0.090887103,(78515:0.081205045,114176:0.179495837)0.994:0.040873850)0.997:0.048794839,(114317:0.128770684,((100492:0.037183535,104661:0.039529320)0.251:0.007801598,((25310:0.014386283,100957:0.035791539)0.877:0.005488249,22197:0.059582329)0.601:0.005618889)1.000:0.080181655)0.091:0.017948449)0.500:0.017998326)0.921:0.015063043);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "nni": 0}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    /// `nni` absent ≡ baseline (the C library auto-default).
    #[test]
    fn test_nni_absent_is_auto() {
        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let baseline = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345}),
                true,
            )
        }
        .unwrap();
        let nni_null = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "nni": null}),
                true,
            )
        }
        .unwrap();
        assert_eq!(baseline, nni_null);
    }

    /// Negative explicit `nni` is rejected (use absent for auto).
    #[test]
    fn test_nni_negative_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(&serde_json::json!({"nni": -2}), &mut cfg).unwrap_err();
        assert!(err.contains("nni"), "expected 'nni' in error, got: {err}");
    }

    // === Phase 1, knob 4: spr ===

    /// SPR rounds don't visibly affect the 50-seq tree (all values 0..=20
    /// converge to the same topology). 100 sequences give SPR room to find
    /// rearrangements, so we step up the fixture for parity tests on this
    /// knob. The 100-seq baseline below is included as a sibling sanity
    /// check — if it ever diverges, the library has lost parity at a size
    /// the 50-seq baseline doesn't catch.
    ///
    /// Regenerated with:
    ///   conda run -n fasttree FastTree -nt -seed 12345 \
    ///       target/scratch/16S.1.100seq.fasta
    /// where target/scratch/16S.1.100seq.fasta is the first 100 PHYLIP records as
    /// FASTA via scripts/subset-phylip-to-fasta.py.
    #[test]
    fn test_fasttree_100seq_parity_baseline() {
        const EXPECTED: &str = "((((105325:0.062322599,11179:0.066015811)1.000:0.124972218,(114155:0.089832682,10472:0.093596122)0.913:0.028525467)0.414:0.013874358,(121156:0.135228694,10607:0.087730467)0.967:0.025278807)0.941:0.016417690,((((84810:0.015493340,3353:0.006420605)1.000:0.072298137,(102607:0.094273800,3499:0.055916562)0.998:0.036643559)1.000:0.065965184,(((19106:0.076411853,99257:0.069263639)0.978:0.033477093,((77434:0.101907741,(99565:0.092916476,101540:0.107819834)0.387:0.019180862)0.971:0.040640841,(2181:0.160972300,109504:0.115597506)0.952:0.031413517)0.942:0.018225473)1.000:0.106035512,(102151:0.179238048,(72728:0.168699462,97471:0.074700235)1.000:0.379755618)0.794:0.037185704)0.127:0.033242688)0.809:0.019962589,((65384:0.079769965,((72638:0.034205039,(((((114738:0.047045523,(75690:0.055567446,(102957:0.038451522,((9944:0.009349343,9669:0.018314858)0.855:0.004924795,(24844:0.008968900,37204:0.035605856)0.597:0.005880272)1.000:0.033956065)0.977:0.017404578)0.486:0.007108208)1.000:0.047943548,98992:0.099519857)0.257:0.011639723,8071:0.036376547)0.992:0.019642077,7960:0.064403910)0.645:0.011919485,(40847:0.058735163,(16371:0.006808569,(69848:0.014769648,71836:0.035848224)0.306:0.001762963)1.000:0.043965952)0.951:0.016736674)0.537:0.011714296)0.952:0.016423695,((6864:0.053754584,(109612:0.023400609,93999:0.038019683)0.976:0.016789422)0.555:0.007481019,((19103:0.020186166,114568:0.011058102)0.932:0.014235818,(52575:0.017345648,108779:0.041666847)1.000:0.037233056)0.711:0.009384175)1.000:0.072537950)0.259:0.022891496)0.990:0.035663431,(111880:0.079918619,((29930:0.071186968,((100639:0.031636629,(87284:0.010153511,5163:0.035194768)0.974:0.013282311)0.997:0.023946173,(106389:0.053139278,(106525:0.040606623,(89315:0.043303884,(54630:0.025686531,65474:0.064146883)0.992:0.023446681)0.875:0.009233258)0.914:0.012199509)0.567:0.013573871)0.950:0.018694020)0.168:0.011871908,(108692:0.065565822,(5903:0.033482918,101793:0.047655412)1.000:0.106742730)0.949:0.020293580)0.948:0.029125722)1.000:0.043380047)0.868:0.019973747)0.877:0.018038663,(((104007:0.126948071,3769:0.155805443)0.999:0.056841888,((((107881:0.198931174,((103543:0.052676174,105026:0.098165818)0.996:0.042622237,(78515:0.078719438,114176:0.191876571)0.989:0.041890071)0.940:0.024965962)0.980:0.033603370,((92528:0.089615596,72749:0.070821717)0.999:0.051938800,(66261:0.192812109,(112138:0.040916808,11326:0.054612022)1.000:0.135407501)0.759:0.041704008)0.801:0.011226190)0.709:0.017993552,((83619:0.092841864,(104854:0.028070936,((14982:0.049656260,(((15167:0.052484316,(40531:0.063779491,32963:0.124696972)0.721:0.020869944)0.989:0.027615828,15053:0.043270703)0.965:0.021169642,106115:0.057843729)0.470:0.016925674)0.929:0.012878317,(102292:0.056908942,44055:0.048987094)0.855:0.007642908)0.530:0.007454427)0.999:0.040265209)0.965:0.017658672,(109556:0.098265605,(103195:0.108863783,((22352:0.081509184,(16077:0.072880573,16043:0.031040310)0.388:0.014114733)0.999:0.052606562,((44923:0.020225501,126385:0.043627334)0.830:0.012897751,(110678:0.038754410,43305:0.066303024)0.899:0.013415915)1.000:0.054914700)0.944:0.019321004)0.544:0.016958614)0.618:0.014046425)0.988:0.023694962)0.231:0.011401598,((114317:0.037947723,81111:0.068600418)1.000:0.108828584,((((100492:0.034290294,22197:0.057493817)0.682:0.013099256,(114033:0.039977365,(25310:0.018662968,100957:0.031899641)0.689:0.005623164)0.382:0.007337839)0.959:0.010450693,104661:0.037546877)0.562:0.010791752,11758:0.022772793)1.000:0.072034307)0.962:0.020628652)0.801:0.008531958)0.496:0.006741895,(38854:0.119975765,(113517:0.101796284,(1828:0.065283857,84610:0.046164562)0.993:0.042043087)0.991:0.036850580)0.863:0.017342209)0.959:0.017315829);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 100);
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

    /// `spr=0` disables SPR rearrangements; on 100 seqs the resulting
    /// tree differs from the C default (spr=2).
    ///
    /// Regenerated with:
    ///   conda run -n fasttree FastTree -nt -seed 12345 -spr 0 \
    ///       target/scratch/16S.1.100seq.fasta
    #[test]
    fn test_spr_0_parity_100seq() {
        const EXPECTED: &str = "(((((107881:0.199174992,((103543:0.052773990,105026:0.098213491)0.995:0.042563591,(78515:0.077549847,114176:0.192743930)0.989:0.042394351)0.939:0.024960715)0.980:0.033642753,((92528:0.089800501,72749:0.070794302)0.999:0.051720148,(66261:0.192405024,(112138:0.041933711,11326:0.053632919)1.000:0.135880265)0.774:0.041642082)0.801:0.010797889)0.639:0.017631529,((83619:0.092834494,(104854:0.028207140,((14982:0.049620126,(((15167:0.052503677,(40531:0.063876547,32963:0.124782536)0.712:0.020784925)0.989:0.027562059,15053:0.043287872)0.965:0.021193662,106115:0.057763839)0.386:0.016931529)0.930:0.012934059,(102292:0.056913781,44055:0.048930617)0.852:0.007616023)0.445:0.007362208)0.999:0.040304320)0.972:0.019222777,(109556:0.098767761,(103195:0.109029154,((22352:0.081772847,(16077:0.073042601,16043:0.030916970)0.360:0.013853203)0.999:0.053177732,((44923:0.020045072,126385:0.043839598)0.829:0.012823642,(110678:0.038962739,43305:0.066123337)0.901:0.013448486)1.000:0.054865379)0.940:0.019201337)0.479:0.016241251)0.389:0.013665387)0.989:0.023215355)0.326:0.011471779,((114317:0.038212656,81111:0.068256192)1.000:0.110450125,((((100492:0.034105951,22197:0.057703635)0.686:0.013140660,(114033:0.040129707,(25310:0.018666985,100957:0.031873945)0.678:0.005471187)0.409:0.007312987)0.962:0.010519604,104661:0.037653938)0.566:0.010703883,11758:0.022604000)1.000:0.071758287)0.965:0.021463587)0.833:0.008987194,(104007:0.126772825,3769:0.154877973)0.999:0.054589018,((38854:0.118844935,(113517:0.100975672,(1828:0.065590801,84610:0.045798913)0.994:0.042941861)0.987:0.036638296)0.868:0.017875944,(((105325:0.063295376,11179:0.064332343)1.000:0.123329252,(((84810:0.015447077,3353:0.006467253)1.000:0.070210795,(102607:0.095123753,3499:0.055051080)0.997:0.038597284)1.000:0.065852459,(((19106:0.075851341,99257:0.069676607)0.979:0.034082577,((77434:0.101694985,(99565:0.093420713,101540:0.107226793)0.368:0.019443286)0.970:0.040512125,(2181:0.160729970,109504:0.115566210)0.955:0.031535168)0.943:0.018285640)1.000:0.105265034,(102151:0.177133404,(72728:0.166578640,97471:0.077006048)1.000:0.382303144)0.763:0.036482973)0.192:0.032669780)0.908:0.020659211)0.540:0.013495244,((10607:0.101116655,(114155:0.086871228,10472:0.099418220)0.969:0.029605978)0.082:0.008142363,(121156:0.139899154,(((72638:0.036600480,(((((114738:0.047140398,(75690:0.055523261,(102957:0.038458112,((9944:0.009344959,9669:0.018317206)0.856:0.004954916,(24844:0.008991009,37204:0.035618235)0.549:0.005827869)1.000:0.033968524)0.977:0.017547499)0.348:0.007012154)1.000:0.047830183,98992:0.099192025)0.231:0.011601082,8071:0.036980944)0.990:0.019062124,7960:0.063946270)0.873:0.013758478,(40847:0.060739452,(16371:0.006839608,(69848:0.014778931,71836:0.035782231)0.264:0.001739184)1.000:0.042443537)0.920:0.014691405)0.703:0.010359343)0.927:0.012858731,(65384:0.080975600,((6864:0.053435138,(109612:0.023355629,93999:0.038063258)0.977:0.017027749)0.695:0.008006545,((19103:0.020020117,114568:0.011155730)0.932:0.014567049,(52575:0.018076811,108779:0.040951350)1.000:0.037110286)0.123:0.008503762)1.000:0.064898783)0.517:0.019282682)1.000:0.051712937,(111880:0.077903858,((29930:0.072466200,((100639:0.032065696,(87284:0.010102597,5163:0.035232749)0.967:0.012972778)0.997:0.023745810,(106389:0.052852081,(106525:0.040396489,(89315:0.043494298,(54630:0.025690704,65474:0.064094592)0.992:0.023245392)0.866:0.009367412)0.919:0.012395313)0.621:0.014045374)0.919:0.016778412)0.530:0.014265366,(108692:0.065179897,(5903:0.032320458,101793:0.048887546)1.000:0.107987312)0.940:0.018398899)0.975:0.031181290)0.999:0.038355310)0.950:0.017751666)0.051:0.018435287)0.942:0.023241508)0.967:0.015847090)0.206:0.007398704);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 100);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "spr": 0}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    /// Negative `spr` is rejected.
    #[test]
    fn test_spr_negative_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(&serde_json::json!({"spr": -1}), &mut cfg).unwrap_err();
        assert!(err.contains("spr"), "expected 'spr' in error, got: {err}");
    }

    // === Phase 1, knob 5: mlnni ===

    /// `mlnni=0` disables ML NNI rounds. Output differs from the auto
    /// default (2*log2(N)).
    ///
    /// Regenerated with:
    ///   conda run -n fasttree FastTree -nt -seed 12345 -mlnni 0 \
    ///       target/scratch/16S.1.50seq.fasta
    #[test]
    fn test_mlnni_0_parity() {
        const EXPECTED: &str = "(38854:0.091866101,((((112138:0.039407849,11326:0.048308824)1.000:0.083629760,(72728:0.308479269,(2181:0.132299068,(101540:0.087109640,(99565:0.085719743,77434:0.079725592)0.544:0.005832008)1.000:0.031173659)0.986:0.033300897)0.961:0.025580749)0.885:0.008768864,((105325:0.060329245,11179:0.053191714)1.000:0.074592945,(((109612:0.038565717,(52575:0.034810581,19103:0.032449435)0.622:0.003244465)1.000:0.046565175,((69848:0.047339032,72638:0.039197524)0.531:0.002377636,(8071:0.046177733,((102957:0.036364918,(37204:0.028163797,(9944:0.011007269,9669:0.016608194)1.000:0.010342798)1.000:0.021892005)0.908:0.008299244,(75690:0.047682512,114738:0.045980231)0.735:0.003915193)1.000:0.024586091)0.938:0.010568359)0.941:0.006653102)1.000:0.025689231,(111880:0.074384748,((5903:0.032723016,101793:0.043634915)1.000:0.076402129,(29930:0.062971243,(100639:0.047631761,(89315:0.035520219,(54630:0.028073226,65474:0.051564698)0.994:0.014667269)0.856:0.005287703)0.740:0.004539755)0.865:0.010094708)0.851:0.006541180)0.996:0.015766025)0.829:0.005455146)0.891:0.006603149)0.607:0.001954413,((92528:0.100743143,(102607:0.096355640,(84810:0.011142681,3353:0.010650620)1.000:0.062782819)1.000:0.035728211)0.532:0.002626386,(16077:0.095105069,(109556:0.074376173,(83619:0.083248369,(40531:0.090805257,104854:0.044133747)0.965:0.012933693)0.878:0.009537401)0.991:0.012807140)0.510:0.005586587)0.960:0.006021667)0.829:0.005485252,((10607:0.087422868,1828:0.103943661)0.701:0.006258181,((103543:0.093938450,(78515:0.087981307,114176:0.136000693)0.544:0.007342228)0.999:0.019657600,(114317:0.095678851,(22197:0.044583919,(100492:0.034651795,((25310:0.015693410,100957:0.034186463)0.995:0.009075633,104661:0.035944631)0.675:0.002732751)0.774:0.007328904)1.000:0.054293970)0.792:0.007128480)0.629:0.002779007)0.468:0.001319398);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "mlnni": 0}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    /// `mlnni` absent ≡ baseline (the C library auto-default).
    #[test]
    fn test_mlnni_absent_is_auto() {
        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let baseline = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345}),
                true,
            )
        }
        .unwrap();
        let mlnni_null = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "mlnni": null}),
                true,
            )
        }
        .unwrap();
        assert_eq!(baseline, mlnni_null);
    }

    /// Negative explicit `mlnni` is rejected (use absent for auto).
    #[test]
    fn test_mlnni_negative_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(&serde_json::json!({"mlnni": -2}), &mut cfg).unwrap_err();
        assert!(
            err.contains("mlnni"),
            "expected 'mlnni' in error, got: {err}"
        );
    }

    // === Phase 1, knob 6: mlacc ===

    /// `mlacc=3` (vs C default 1) changes branch-length numerics.
    ///
    /// Regenerated with:
    ///   conda run -n fasttree FastTree -nt -seed 12345 -mlacc 3 \
    ///       target/scratch/16S.1.50seq.fasta
    #[test]
    fn test_mlacc_3_parity() {
        const EXPECTED: &str = "((10607:0.108751017,1828:0.122720017)0.967:0.029764589,((((112138:0.033649393,11326:0.060187704)1.000:0.139853640,(72728:0.469222666,(2181:0.170647720,(77434:0.082702186,(99565:0.092547008,101540:0.104777573)0.805:0.027238602)0.977:0.045914230)0.997:0.073318264)0.886:0.044298153)0.919:0.017379019,((105325:0.067029334,11179:0.058705424)1.000:0.135294335,(((109612:0.038435606,(52575:0.041628587,19103:0.029238186)0.819:0.010772757)1.000:0.071847160,((69848:0.052298014,(8071:0.038990325,((102957:0.037013229,(37204:0.032886057,(9944:0.010847631,9669:0.016836826)0.944:0.009621600)1.000:0.031395935)0.969:0.017793009,(75690:0.052945106,114738:0.048951109)0.187:0.004988583)1.000:0.051602433)0.928:0.026236397)0.892:0.016844911,72638:0.036007120)0.501:0.012706856)0.999:0.048415676,(111880:0.082869901,(29930:0.067458914,((100639:0.061387972,(54630:0.028641290,65474:0.058987824)0.981:0.023559193)0.063:0.007651424,(89315:0.034815054,(5903:0.034325821,101793:0.046091181)1.000:0.116235840)0.461:0.010312788)0.994:0.029263562)0.611:0.023652163)1.000:0.038955511)0.292:0.011661220)0.323:0.015309706)0.912:0.013618830,38854:0.119762045)0.923:0.012282447,(((109556:0.094698151,(83619:0.093362875,(40531:0.116017265,104854:0.036462405)0.987:0.026151852)0.870:0.013682755)0.960:0.022637467,(16077:0.121982506,(92528:0.125099277,(102607:0.120222180,(84810:0.012364323,3353:0.009445078)1.000:0.064291790)1.000:0.061011629)0.614:0.021443779)0.835:0.024124954)0.694:0.017166037,((103543:0.090894609,(78515:0.081138708,114176:0.179387037)0.993:0.040913748)0.996:0.048772654,(114317:0.128769624,((100492:0.037179414,104661:0.039513494)0.252:0.007798778,((25310:0.014381626,100957:0.035803074)0.877:0.005488709,22197:0.059606012)0.602:0.005615630)1.000:0.080241493)0.089:0.017941138)0.500:0.017991027)0.920:0.015064205);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "mlacc": 3}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    /// `mlacc < 1` is rejected.
    #[test]
    fn test_mlacc_zero_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(&serde_json::json!({"mlacc": 0}), &mut cfg).unwrap_err();
        assert!(
            err.contains("mlacc"),
            "expected 'mlacc' in error, got: {err}"
        );
    }

    // === Phase 1, knob 7: cat ===

    /// `cat=5` (vs C default 20) changes ML rate-categorization.
    ///
    /// Regenerated with:
    ///   conda run -n fasttree FastTree -nt -seed 12345 -cat 5 \
    ///       target/scratch/16S.1.50seq.fasta
    #[test]
    fn test_cat_5_parity() {
        const EXPECTED: &str = "((10607:0.110822829,1828:0.124880820)0.972:0.029700310,((((112138:0.035014039,11326:0.059534929)1.000:0.142807918,(72728:0.488346397,(2181:0.174518373,(77434:0.083090752,(99565:0.094410303,101540:0.106033033)0.716:0.027011159)0.983:0.047334957)0.996:0.071259023)0.938:0.048979680)0.902:0.015631453,((105325:0.068229294,11179:0.058898454)1.000:0.138868794,(((109612:0.038747606,(52575:0.041915241,19103:0.029378762)0.732:0.010481334)1.000:0.072667665,((69848:0.052728065,(8071:0.038815686,((102957:0.037137564,(37204:0.033007542,(9944:0.010910693,9669:0.016777213)0.928:0.009577950)1.000:0.031666005)0.969:0.017381178,(75690:0.053210402,114738:0.048920567)0.435:0.005583687)1.000:0.051872661)0.949:0.026401072)0.919:0.016946861,72638:0.035928670)0.662:0.012902654)0.998:0.047777724,(111880:0.083665738,(29930:0.067982258,((89315:0.042458713,(54630:0.027526951,65474:0.060946624)0.986:0.025330958)0.639:0.010724829,(100639:0.050716131,(5903:0.034465767,101793:0.046453324)1.000:0.116261040)0.421:0.012246706)0.974:0.026907188)0.713:0.024173990)1.000:0.039623965)0.086:0.011346465)0.637:0.016888732)0.916:0.012445916,38854:0.121715265)0.904:0.011560471,(((109556:0.095088001,(83619:0.094093818,(40531:0.117018280,104854:0.036454146)0.984:0.026338430)0.842:0.014138373)0.970:0.022170477,(16077:0.123788831,(92528:0.127847725,(102607:0.120078923,(84810:0.012084286,3353:0.009791907)1.000:0.066145614)1.000:0.060949527)0.799:0.022206872)0.572:0.023522439)0.850:0.019912603,(114317:0.138869365,((103543:0.093971338,(78515:0.080975889,114176:0.182642039)0.991:0.041758154)0.996:0.047266735,((100492:0.037402409,104661:0.040115599)0.000:0.006983809,((25310:0.014442218,100957:0.035723850)0.885:0.005731463,22197:0.059777548)0.875:0.006214584)1.000:0.086547030)0.151:0.010858164)0.389:0.016827831)0.878:0.013183583);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "cat": 5}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    /// `cat < 1` is rejected.
    #[test]
    fn test_cat_zero_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(&serde_json::json!({"cat": 0}), &mut cfg).unwrap_err();
        assert!(err.contains("cat"), "expected 'cat' in error, got: {err}");
    }

    // === Phase 1, knob 8: noml ===

    /// `noml=true` is exactly equivalent to `mlnni=0` (same C field set
    /// to 0, same Newick output). The dedicated parity reference is the
    /// same file as `mlnni 0`:
    ///   conda run -n fasttree FastTree -nt -seed 12345 -noml \
    ///       target/scratch/16S.1.50seq.fasta
    #[test]
    fn test_noml_eq_mlnni_zero() {
        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let mlnni_zero = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "mlnni": 0}),
                true,
            )
        }
        .unwrap();
        let noml_true = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "noml": true}),
                true,
            )
        }
        .unwrap();
        assert_eq!(noml_true, mlnni_zero);
    }

    /// `noml=true && mlnni > 0` is a config conflict.
    #[test]
    fn test_noml_with_mlnni_positive_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(&serde_json::json!({"noml": true, "mlnni": 5}), &mut cfg)
            .unwrap_err();
        assert!(
            err.contains("conflict") && err.contains("mlnni") && err.contains("noml"),
            "expected conflict error mentioning mlnni and noml, got: {err}"
        );
    }

    /// `noml=true && mlnni=0` is fine (both express the same intent).
    #[test]
    fn test_noml_with_mlnni_zero_ok() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        apply_json_to_config(&serde_json::json!({"noml": true, "mlnni": 0}), &mut cfg)
            .expect("noml + mlnni=0 must be accepted");
        assert_eq!(cfg.ml_nni_rounds, 0);
    }

    // === Phase 2: threads ===

    /// `threads=1` reproduces the existing 50-seq parity baseline, proving
    /// the JSON knob plumbs through to `n_threads` and that single-thread
    /// execution under OpenMP is deterministic.
    ///
    /// Unlike the higher-thread sanity test below, this one *can* assert
    /// byte-equality on the Newick because OpenMP at one thread runs the
    /// `#pragma omp parallel for` loops sequentially, in the same order
    /// the non-parallel build would.
    #[test]
    fn test_threads_1_parity() {
        // Same expected Newick as `test_fasttree_50seq_parity_baseline`.
        // Inlined rather than referenced to keep each parity test
        // self-contained (the const naming convention assumes one EXPECTED
        // per fn).
        const EXPECTED: &str = "((10607:0.108728183,1828:0.122617756)0.968:0.029784172,((((112138:0.033883874,11326:0.060069834)1.000:0.139823276,(72728:0.469274735,(2181:0.170590261,(77434:0.082689947,(99565:0.092547907,101540:0.104777748)0.801:0.027296752)0.978:0.045947821)0.997:0.073100991)0.890:0.044420886)0.922:0.017371404,((105325:0.067027677,11179:0.058740204)1.000:0.135281789,(((109612:0.038444192,(52575:0.041602501,19103:0.029245209)0.825:0.010775536)1.000:0.071852375,((69848:0.052322287,(8071:0.038960071,((102957:0.036993227,(37204:0.032886181,(9944:0.010841675,9669:0.016835590)0.944:0.009614309)1.000:0.031400631)0.969:0.017801804,(75690:0.052930806,114738:0.048955244)0.188:0.004986093)1.000:0.051618818)0.926:0.026253833)0.890:0.016831614,72638:0.036015850)0.501:0.012705519)0.999:0.048395482,(111880:0.082925522,(29930:0.067479705,((100639:0.061363527,(54630:0.028632074,65474:0.058988375)0.981:0.023574978)0.063:0.007656523,(89315:0.034813563,(5903:0.034324338,101793:0.046061462)1.000:0.116234675)0.461:0.010309408)0.995:0.029255734)0.611:0.023606761)1.000:0.038934525)0.293:0.011670275)0.322:0.015313215)0.916:0.013617425,38854:0.119745452)0.925:0.012283122,(((109556:0.094712097,(83619:0.093304494,(40531:0.116098375,104854:0.036481237)0.988:0.026154190)0.861:0.013688556)0.964:0.022623573,(16077:0.122036067,(92528:0.125166108,(102607:0.120186435,(84810:0.012379015,3353:0.009442461)1.000:0.064244782)1.000:0.061086276)0.614:0.021412930)0.841:0.024118426)0.688:0.017162443,((103543:0.090887321,(78515:0.081205030,114176:0.179495854)0.994:0.040873948)0.997:0.048795167,(114317:0.128771221,((100492:0.037183916,104661:0.039529991)0.251:0.007801462,((25310:0.014386300,100957:0.035791254)0.877:0.005488228,22197:0.059582638)0.602:0.005619043)1.000:0.080180535)0.091:0.017948287)0.500:0.017997704)0.921:0.015062351);\n";

        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);
        let actual = unsafe {
            build_newick_via_json(
                &alignment,
                &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "threads": 1}),
                true,
            )
        }
        .unwrap();
        assert_eq!(actual, EXPECTED);
    }

    /// `threads=4` produces a structurally valid tree, but FastTree's
    /// parallel NJ/NNI/SPR phases are non-deterministic across thread
    /// counts (top-hit selection order, floating-point reductions —
    /// see the OpenMP comment block in `ext/fasttree/fasttree_core.c`),
    /// so we deliberately do NOT assert byte-equal Newick. Instead:
    ///
    /// - The leaf set must equal the input set (no sequences lost).
    /// - The rooted-tree invariant `sum(n_children) == n_nodes - 1`
    ///   must hold.
    /// - Final log-likelihood must be finite and negative — what this
    ///   test can actually guarantee. A tighter numeric bound would be
    ///   theatre: for a 50-seq 16S alignment the log-likelihood lives
    ///   near -75k nats, so even a 1% relative tolerance ≈ 750 nats of
    ///   slack — wide enough to mask a serious topology regression.
    ///   Topology errors are detected upstream by the leaf-set and
    ///   tree-shape checks; the log-likelihood assertion only catches
    ///   the *kind* of regression numerics can produce (NaN/Inf).
    #[test]
    fn test_threads_4_sanity() {
        let alignment = subset_alignment(&parse_interleaved_phylip(&extracted_16s_phylip()), 50);

        let input_name = unique_shm_name("ft-threads-4");
        let batch = make_input_batch_owned(&alignment);
        let _shm = write_arrow_to_shm(&input_name, &batch);
        let resp = FastTreeTool.execute(
            &serde_json::json!({"seq_type": "nucleotide", "seed": 12345, "threads": 4}),
            &input_name,
        );
        assert!(resp.success, "threads=4 failed: {:?}", resp.error);

        let result = resp.result.as_ref().unwrap();
        let n_nodes = result["n_nodes"].as_i64().unwrap() as usize;
        let n_leaves = result["n_leaves"].as_i64().unwrap() as usize;
        assert_eq!(n_leaves, alignment.len(), "no leaves lost at threads=4");

        let out_batches = read_arrow_from_shm(&resp.shm_outputs[0].name);
        let out = &out_batches[0];

        // Every input name appears as a tip exactly once.
        let names_col = out
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let is_tip = out
            .column_by_name("is_tip")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let mut leaf_names: Vec<&str> = (0..out.num_rows())
            .filter(|&i| is_tip.value(i))
            .map(|i| names_col.value(i))
            .collect();
        leaf_names.sort();
        let mut expected_names: Vec<&str> = alignment.iter().map(|(n, _)| n.as_str()).collect();
        expected_names.sort();
        assert_eq!(leaf_names, expected_names, "leaf set must match input");

        // Tree-shape invariant.
        let n_children = out
            .column_by_name("n_children")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let total: i64 = (0..n_nodes).map(|i| n_children.value(i) as i64).sum();
        assert_eq!(total, (n_nodes - 1) as i64);

        // Log-likelihood is finite and negative (the only numeric
        // assertion we can make without re-deriving FastTree's expected
        // output for this specific 4-thread interleaving).
        let lk = result["stats"]["log_likelihood"].as_f64().unwrap();
        assert!(
            lk.is_finite() && lk < 0.0,
            "log_likelihood not finite-negative: {lk}"
        );

        let _ = SharedMemory::unlink(&resp.shm_outputs[0].name);
    }

    /// `threads=0` is rejected — the C library treats 0 as "consult
    /// OMP_NUM_THREADS", which we deliberately do not expose.
    #[test]
    fn test_threads_zero_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(&serde_json::json!({"threads": 0}), &mut cfg).unwrap_err();
        assert!(
            err.contains("threads"),
            "expected 'threads' in error, got: {err}"
        );
    }

    /// Negative `threads` is rejected.
    #[test]
    fn test_threads_negative_rejected() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let err = apply_json_to_config(&serde_json::json!({"threads": -3}), &mut cfg).unwrap_err();
        assert!(
            err.contains("threads"),
            "expected 'threads' in error, got: {err}"
        );
    }

    /// Absent `threads` defaults to 1, NOT to the C library's 0
    /// (which would consult OMP_NUM_THREADS).
    #[test]
    fn test_threads_absent_defaults_to_one() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        apply_json_to_config(&serde_json::json!({}), &mut cfg).unwrap();
        assert_eq!(
            cfg.n_threads, 1,
            "default must override the C library's 0 (which consults OMP_NUM_THREADS)"
        );
    }

    // === Range-check + UX hygiene (Linus review followups) ===

    /// Numeric knobs reject values that would silently truncate when
    /// cast to `c_int`. Spot-check `bootstrap` (any of the 5 numeric
    /// knobs would do — they all funnel through `checked_cint`).
    #[test]
    fn test_numeric_knob_rejects_overflow() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        let big = c_int::MAX as i64 + 1;
        let err =
            apply_json_to_config(&serde_json::json!({"bootstrap": big}), &mut cfg).unwrap_err();
        assert!(
            err.contains("bootstrap") && err.contains("range"),
            "expected overflow error mentioning bootstrap and range, got: {err}"
        );
    }

    /// `bootstrap=0 + nosupport=true` is allowed (both express the same
    /// intent). The conflict guard fires only on `bootstrap != 0`.
    #[test]
    fn test_bootstrap_zero_with_nosupport_ok() {
        let mut cfg: FastTreeConfig = unsafe { std::mem::zeroed() };
        unsafe { fasttree_config_init(&mut cfg) };
        apply_json_to_config(
            &serde_json::json!({"bootstrap": 0, "nosupport": true}),
            &mut cfg,
        )
        .expect("bootstrap=0 + nosupport=true must be accepted");
        assert_eq!(cfg.n_bootstrap, 0);
    }

    /// Empty-alignment guard in `build_newick_via_c` returns an error
    /// instead of panicking inside unsafe code on `alignment[0]`.
    #[test]
    fn test_build_newick_via_c_rejects_empty_alignment() {
        let err = unsafe { build_newick_via_c(&[], |_| Ok(()), false) }.unwrap_err();
        assert!(
            err.contains("empty"),
            "expected empty-alignment error, got: {err}"
        );
    }
}
