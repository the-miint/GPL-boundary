use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int32Array, Int32Builder, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::protocol::Response;
use crate::tools::{
    ConfigParam, FieldDescription, GplTool, StreamingContext, ToolDescription, ToolRegistration,
};

// ---------------------------------------------------------------------------
// FFI bindings — mirror ext/sortmerna/include/smr_api.h
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct SmrConfig {
    pub struct_size: u32, // uint32_t — cross-platform consistency
    pub num_threads: i32,
    pub match_score: i32, // "match" in C, renamed to avoid Rust keyword
    pub mismatch: i32,
    pub gap_open: i32,
    pub gap_ext: i32,
    pub score_n: i32,
    pub evalue: f64,
    pub seed_win_len: u32,
    pub num_alignments: u32,
    pub best: i32,
    pub paired: i32,
    pub forward_only: i32,
    pub reverse_only: i32,
    pub full_search: i32,
    pub fastx: i32,
    pub sam: i32,
    pub blast: i32,
    pub otu_map: i32,
    pub denovo: i32,
    pub workdir: *const c_char,
    pub log_callback: Option<unsafe extern "C" fn(c_int, *const c_char, *mut c_void)>,
    pub log_user_data: *mut c_void,
}

#[repr(C)]
pub struct SmrOutput {
    pub num_reads: u64,
    pub num_aligned: u64,
    pub read_ids: *const *const c_char,
    pub aligned: *const i32,
    pub ref_index: *const i32,
    pub e_value: *const f64,
    pub identity: *const f64,
    pub coverage: *const f64,
    pub ref_start: *const i32,
    pub ref_end: *const i32,
    pub cigar: *const *const c_char,
    pub ref_name: *const *const c_char,
    pub strand: *const i32,
    pub score: *const i32,
    pub edit_distance: *const i32,
}

#[repr(C)]
pub struct SmrStats {
    pub total_reads: u64,
    pub total_aligned: u64,
    pub total_id_cov_pass: u64,
    pub total_denovo: u64,
    pub min_read_len: u32,
    pub max_read_len: u32,
    pub wall_time_sec: f64,
}

#[repr(C)]
pub struct SmrSeq {
    pub id: *const c_char,
    pub sequence: *const c_char,
    pub quality: *const c_char,
}

#[allow(non_camel_case_types)]
type smr_context_t = c_void;

extern "C" {
    fn smr_config_init(cfg: *mut SmrConfig);
    fn smr_ctx_create(cfg: *const SmrConfig) -> *mut smr_context_t;
    fn smr_ctx_destroy(ctx: *mut smr_context_t);
    fn smr_run_seqs(
        ctx: *mut smr_context_t,
        ref_paths: *const *const c_char,
        num_refs: i32,
        seqs: *const SmrSeq,
        num_seqs: i32,
        out: *mut *mut SmrOutput,
        stats: *mut SmrStats,
    ) -> c_int;
    fn smr_output_free(out: *mut SmrOutput);
    fn smr_strerror(code: c_int) -> *const c_char;
    fn smr_last_error(ctx: *const smr_context_t) -> *const c_char;
    fn smr_version() -> *const c_char;
}

const SMR_OK: c_int = 0;

// ---------------------------------------------------------------------------
// Log callback
// ---------------------------------------------------------------------------

unsafe extern "C" fn stderr_log_callback(
    _level: c_int,
    msg: *const c_char,
    _user_data: *mut c_void,
) {
    if !msg.is_null() {
        let s = CStr::from_ptr(msg).to_string_lossy();
        eprint!("{s}");
    }
}

// ---------------------------------------------------------------------------
// Arrow schemas
// ---------------------------------------------------------------------------

/// Output schema. Schema version 2 (Phase 5) replaces the prior `-1` /
/// `0` sentinels in alignment-only columns with NULL when `aligned == 0`.
/// `aligned` itself stays non-nullable (always 0 or 1). Quality metrics
/// `e_value`, `identity`, `coverage` are kept as Float64 — the C library
/// emits zeros for unaligned reads and downstream consumers already
/// guard on `aligned`.
fn output_schema() -> Schema {
    Schema::new(vec![
        Field::new("read_id", DataType::Utf8, false),
        Field::new("aligned", DataType::Int32, false),
        Field::new("strand", DataType::Int32, true),
        Field::new("ref_name", DataType::Utf8, true),
        Field::new("ref_start", DataType::Int32, true),
        Field::new("ref_end", DataType::Int32, true),
        Field::new("cigar", DataType::Utf8, true),
        Field::new("score", DataType::Int32, true),
        Field::new("e_value", DataType::Float64, false),
        Field::new("identity", DataType::Float64, false),
        Field::new("coverage", DataType::Float64, false),
        Field::new("edit_distance", DataType::Int32, true),
    ])
}

// ---------------------------------------------------------------------------
// Tool struct and registration
// ---------------------------------------------------------------------------

pub struct SortMeRnaTool;

inventory::submit! {
    ToolRegistration {
        create: || Box::new(SortMeRnaTool),
    }
}

fn smr_version_string() -> String {
    unsafe {
        let p = smr_version();
        if p.is_null() {
            "unknown".to_string()
        } else {
            CStr::from_ptr(p).to_string_lossy().into_owned()
        }
    }
}

// ---------------------------------------------------------------------------
// Streaming context
// ---------------------------------------------------------------------------

/// Holds a SortMeRNA context for batched streaming. Reference indexing happens
/// once during create and is reused across all run_batch calls.
struct SortMeRnaStreamingContext {
    ctx: *mut smr_context_t,
    /// Reference path CStrings — must live for the session because ref_ptrs
    /// holds raw pointers into them and smr_run_seqs uses them on every call.
    #[allow(dead_code)]
    ref_cstrings: Vec<CString>,
    /// Raw pointers into ref_cstrings for the C API.
    ref_ptrs: Vec<*const c_char>,
}

// Safety: The underlying C context is not thread-safe, but no shared
// references to self.ctx are handed out. Only &mut self methods exist
// (run_batch), so exclusive access is enforced at the type level.
unsafe impl Send for SortMeRnaStreamingContext {}

impl Drop for SortMeRnaStreamingContext {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            unsafe { smr_ctx_destroy(self.ctx) };
        }
    }
}

impl StreamingContext for SortMeRnaStreamingContext {
    fn run_batch(&mut self, shm_input: &str) -> Response {
        let (read_ids, sequences, sequences2) = match SortMeRnaTool::read_input(shm_input) {
            Ok(data) => data,
            Err(e) => return Response::error(e),
        };

        if read_ids.is_empty() {
            return Response::error("At least 1 read required");
        }

        let is_paired = sequences2.is_some();

        // Build smr_seq_t array (per-batch, ephemeral)
        let (seq_ids_c, seq_seqs_c, smr_seqs) = if is_paired {
            let seqs2 = sequences2.as_ref().unwrap();
            let mut ids = Vec::with_capacity(read_ids.len() * 2);
            let mut seqs = Vec::with_capacity(sequences.len() * 2);
            for i in 0..read_ids.len() {
                let id = match CString::new(read_ids[i].as_str()) {
                    Ok(v) => v,
                    Err(_) => {
                        return Response::error(format!(
                            "read_id '{}' contains interior null byte",
                            read_ids[i]
                        ))
                    }
                };
                let s1 = match CString::new(sequences[i].as_str()) {
                    Ok(v) => v,
                    Err(_) => {
                        return Response::error(format!(
                            "sequence for '{}' contains interior null byte",
                            read_ids[i]
                        ))
                    }
                };
                let s2 = match CString::new(seqs2[i].as_str()) {
                    Ok(v) => v,
                    Err(_) => {
                        return Response::error(format!(
                            "sequence2 for '{}' contains interior null byte",
                            read_ids[i]
                        ))
                    }
                };
                ids.push(id.clone());
                seqs.push(s1);
                ids.push(id);
                seqs.push(s2);
            }
            let smr: Vec<SmrSeq> = ids
                .iter()
                .zip(seqs.iter())
                .map(|(id, seq)| SmrSeq {
                    id: id.as_ptr(),
                    sequence: seq.as_ptr(),
                    quality: ptr::null(),
                })
                .collect();
            (ids, seqs, smr)
        } else {
            let ids: Vec<CString> = match read_ids
                .iter()
                .map(|s| CString::new(s.as_str()))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(v) => v,
                Err(_) => return Response::error("read_id contains interior null byte"),
            };
            let seqs: Vec<CString> = match sequences
                .iter()
                .map(|s| CString::new(s.as_str()))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(v) => v,
                Err(_) => return Response::error("sequence contains interior null byte"),
            };
            let smr: Vec<SmrSeq> = ids
                .iter()
                .zip(seqs.iter())
                .map(|(id, seq)| SmrSeq {
                    id: id.as_ptr(),
                    sequence: seq.as_ptr(),
                    quality: ptr::null(),
                })
                .collect();
            (ids, seqs, smr)
        };

        let _keep_ids = &seq_ids_c;
        let _keep_seqs = &seq_seqs_c;

        unsafe {
            let mut output: *mut SmrOutput = ptr::null_mut();
            let mut stats: SmrStats = std::mem::zeroed();

            let rc = smr_run_seqs(
                self.ctx,
                self.ref_ptrs.as_ptr(),
                self.ref_ptrs.len() as i32,
                smr_seqs.as_ptr(),
                smr_seqs.len() as i32,
                &mut output,
                &mut stats,
            );

            if rc != SMR_OK {
                let err_msg = CStr::from_ptr(smr_last_error(self.ctx))
                    .to_string_lossy()
                    .into_owned();
                let code_msg = CStr::from_ptr(smr_strerror(rc))
                    .to_string_lossy()
                    .into_owned();
                return Response::error(format!("{code_msg}: {err_msg}"));
            }

            let batch = match soa_to_record_batch(&*output) {
                Ok(b) => b,
                Err(e) => {
                    smr_output_free(output);
                    return Response::error(e);
                }
            };

            let shm_out = match crate::arrow_ipc::write_batch_to_output_shm(&batch, "alignments") {
                Ok(v) => v,
                Err(e) => {
                    smr_output_free(output);
                    return Response::error(e);
                }
            };

            let result = serde_json::json!({
                "total_reads": stats.total_reads,
                "total_aligned": stats.total_aligned,
                "total_id_cov_pass": stats.total_id_cov_pass,
                "wall_time_sec": stats.wall_time_sec,
            });

            smr_output_free(output);

            Response::ok(result, vec![shm_out])
        }
    }
}

// ---------------------------------------------------------------------------
// GplTool implementation
// ---------------------------------------------------------------------------

/// (read_ids, sequences, optional_sequences2)
type ReadInput = (Vec<String>, Vec<String>, Option<Vec<String>>);

impl SortMeRnaTool {
    /// Read Arrow IPC from shared memory, extract read_id, sequence, and
    /// optional sequence2 columns.
    fn read_input(shm_input: &str) -> Result<ReadInput, String> {
        let batches = crate::arrow_ipc::read_batches_from_shm(shm_input)?;

        let mut read_ids = Vec::new();
        let mut sequences = Vec::new();
        let mut sequences2: Option<Vec<String>> = None;

        for batch in &batches {
            let id_col = batch
                .column_by_name("read_id")
                .ok_or("Input missing 'read_id' column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("'read_id' column is not Utf8")?;

            let seq_col = batch
                .column_by_name("sequence")
                .ok_or("Input missing 'sequence' column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("'sequence' column is not Utf8")?;

            let seq2_col = batch
                .column_by_name("sequence2")
                .map(|c| {
                    c.as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or("'sequence2' column is not Utf8")
                })
                .transpose()?;

            for i in 0..batch.num_rows() {
                read_ids.push(id_col.value(i).to_string());
                sequences.push(seq_col.value(i).to_string());
            }

            if let Some(s2) = seq2_col {
                let buf = sequences2.get_or_insert_with(Vec::new);
                for i in 0..batch.num_rows() {
                    if s2.is_null(i) {
                        return Err(
                            "sequence2 column has null values; all must be non-null for paired-end"
                                .to_string(),
                        );
                    }
                    buf.push(s2.value(i).to_string());
                }
            }
        }

        Ok((read_ids, sequences, sequences2))
    }
}

/// Convert smr_output_t SOA arrays into an Arrow RecordBatch.
///
/// Schema v2 sentinel translation: when `aligned[i] == 0`, the
/// alignment-only columns (`strand`, `ref_start`, `ref_end`, `score`,
/// `edit_distance`) emit NULL instead of the C library's prior
/// `-1`/`0` sentinels. `ref_name` and `cigar` were already nullable in
/// v1; their NULL semantics carry over unchanged. `e_value`, `identity`,
/// `coverage` stay as zero-valued floats for unaligned reads — callers
/// gate on `aligned` for those metrics.
unsafe fn soa_to_record_batch(output: &SmrOutput) -> Result<RecordBatch, String> {
    let n = output.num_reads as usize;

    let id_ptrs = std::slice::from_raw_parts(output.read_ids, n);
    let aligned = std::slice::from_raw_parts(output.aligned, n);
    let strand = std::slice::from_raw_parts(output.strand, n);
    let ref_start = std::slice::from_raw_parts(output.ref_start, n);
    let ref_end = std::slice::from_raw_parts(output.ref_end, n);
    let score = std::slice::from_raw_parts(output.score, n);
    let e_value = std::slice::from_raw_parts(output.e_value, n);
    let identity = std::slice::from_raw_parts(output.identity, n);
    let coverage = std::slice::from_raw_parts(output.coverage, n);
    let edit_distance = std::slice::from_raw_parts(output.edit_distance, n);
    let cigar_ptrs = std::slice::from_raw_parts(output.cigar, n);
    let ref_name_ptrs = std::slice::from_raw_parts(output.ref_name, n);

    let read_ids: Vec<String> = id_ptrs
        .iter()
        .map(|&p| {
            if p.is_null() {
                String::new()
            } else {
                CStr::from_ptr(p).to_string_lossy().into_owned()
            }
        })
        .collect();

    let ref_names: Vec<Option<String>> = ref_name_ptrs
        .iter()
        .map(|&p| {
            if p.is_null() {
                None
            } else {
                Some(CStr::from_ptr(p).to_string_lossy().into_owned())
            }
        })
        .collect();

    let cigars: Vec<Option<String>> = cigar_ptrs
        .iter()
        .map(|&p| {
            if p.is_null() {
                None
            } else {
                Some(CStr::from_ptr(p).to_string_lossy().into_owned())
            }
        })
        .collect();

    // Build the alignment-only Int32 columns with NULL on unaligned
    // rows. We use one builder per column rather than a closure so the
    // hot loop stays a tight switch on `aligned[i]`.
    let mut strand_b = Int32Builder::with_capacity(n);
    let mut ref_start_b = Int32Builder::with_capacity(n);
    let mut ref_end_b = Int32Builder::with_capacity(n);
    let mut score_b = Int32Builder::with_capacity(n);
    let mut edit_distance_b = Int32Builder::with_capacity(n);
    for i in 0..n {
        if aligned[i] == 0 {
            strand_b.append_null();
            ref_start_b.append_null();
            ref_end_b.append_null();
            score_b.append_null();
            edit_distance_b.append_null();
        } else {
            strand_b.append_value(strand[i]);
            ref_start_b.append_value(ref_start[i]);
            ref_end_b.append_value(ref_end[i]);
            score_b.append_value(score[i]);
            edit_distance_b.append_value(edit_distance[i]);
        }
    }

    let schema = Arc::new(output_schema());
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                read_ids.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
            )),
            Arc::new(Int32Array::from(aligned.to_vec())),
            Arc::new(strand_b.finish()),
            Arc::new(StringArray::from(
                ref_names
                    .iter()
                    .map(|s| s.as_deref())
                    .collect::<Vec<Option<&str>>>(),
            )),
            Arc::new(ref_start_b.finish()),
            Arc::new(ref_end_b.finish()),
            Arc::new(StringArray::from(
                cigars
                    .iter()
                    .map(|s| s.as_deref())
                    .collect::<Vec<Option<&str>>>(),
            )),
            Arc::new(score_b.finish()),
            Arc::new(Float64Array::from(e_value.to_vec())),
            Arc::new(Float64Array::from(identity.to_vec())),
            Arc::new(Float64Array::from(coverage.to_vec())),
            Arc::new(edit_distance_b.finish()),
        ],
    )
    .map_err(|e| format!("Failed to create Arrow RecordBatch: {e}"))
}

impl GplTool for SortMeRnaTool {
    fn name(&self) -> &str {
        "sortmerna"
    }

    fn version(&self) -> String {
        smr_version_string()
    }

    fn schema_version(&self) -> u32 {
        2
    }

    fn describe(&self) -> ToolDescription {
        ToolDescription {
            name: "sortmerna",
            version: self.version(),
            schema_version: self.schema_version(),
            describe_version: 1,
            description: "rRNA filtering and sequence alignment against reference databases",
            config_params: vec![
                ConfigParam {
                    name: "ref_paths",
                    param_type: "string_array",
                    default: serde_json::json!([]),
                    description: "Reference FASTA file paths (required, at least one)",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "workdir",
                    param_type: "string",
                    default: serde_json::json!(null),
                    description: "Working directory for index persistence; null = auto temp dir",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "num_threads",
                    param_type: "integer",
                    default: serde_json::json!(2),
                    description: "Number of threads for alignment",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "match",
                    param_type: "integer",
                    default: serde_json::json!(2),
                    description: "Match reward score",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "mismatch",
                    param_type: "integer",
                    default: serde_json::json!(-3),
                    description: "Mismatch penalty",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "gap_open",
                    param_type: "integer",
                    default: serde_json::json!(5),
                    description: "Gap open penalty",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "gap_ext",
                    param_type: "integer",
                    default: serde_json::json!(2),
                    description: "Gap extension penalty",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "score_N",
                    param_type: "integer",
                    default: serde_json::json!(-3),
                    description: "Penalty for ambiguous bases (N)",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "evalue",
                    param_type: "float",
                    default: serde_json::json!(-1.0),
                    description: "E-value threshold; -1 disables filtering",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "seed_win_len",
                    param_type: "integer",
                    default: serde_json::json!(18),
                    description: "Seed window length",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "num_alignments",
                    param_type: "integer",
                    default: serde_json::json!(1),
                    description: "Number of alignments to report per read",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "best",
                    param_type: "boolean",
                    default: serde_json::json!(true),
                    description: "Report best alignment only",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "forward_only",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Search forward strand only",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "reverse_only",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Search reverse strand only",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "full_search",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Full search (all seed hits, slower but more sensitive)",
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
                    name: "read_id",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Sequence identifier",
                },
                FieldDescription {
                    name: "sequence",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Forward read nucleotide sequence",
                },
                FieldDescription {
                    name: "sequence2",
                    arrow_type: "Utf8",
                    nullable: true,
                    description: "Reverse read for paired-end; absence or all-null = single-end",
                },
            ],
            output_schema: vec![
                FieldDescription {
                    name: "read_id",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Read identifier (from input)",
                },
                FieldDescription {
                    name: "aligned",
                    arrow_type: "Int32",
                    nullable: false,
                    description: "1 if aligned, 0 otherwise",
                },
                FieldDescription {
                    name: "strand",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "1=forward, 0=reverse-complement; NULL if unaligned",
                },
                FieldDescription {
                    name: "ref_name",
                    arrow_type: "Utf8",
                    nullable: true,
                    description: "Reference sequence ID; NULL if unaligned",
                },
                FieldDescription {
                    name: "ref_start",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "1-based start on reference; NULL if unaligned",
                },
                FieldDescription {
                    name: "ref_end",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "1-based end on reference; NULL if unaligned",
                },
                FieldDescription {
                    name: "cigar",
                    arrow_type: "Utf8",
                    nullable: true,
                    description: "CIGAR string; NULL if unaligned",
                },
                FieldDescription {
                    name: "score",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "Smith-Waterman alignment score; NULL if unaligned",
                },
                FieldDescription {
                    name: "e_value",
                    arrow_type: "Float64",
                    nullable: false,
                    description: "E-value of best alignment (0.0 when unaligned)",
                },
                FieldDescription {
                    name: "identity",
                    arrow_type: "Float64",
                    nullable: false,
                    description: "Percent identity (0-100; 0.0 when unaligned)",
                },
                FieldDescription {
                    name: "coverage",
                    arrow_type: "Float64",
                    nullable: false,
                    description: "Query coverage (0-100; 0.0 when unaligned)",
                },
                FieldDescription {
                    name: "edit_distance",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "Edit distance (mismatches + gaps); NULL if unaligned",
                },
            ],
            response_metadata: vec![
                FieldDescription {
                    name: "total_reads",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Total reads in input",
                },
                FieldDescription {
                    name: "total_aligned",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Reads passing alignment threshold",
                },
                FieldDescription {
                    name: "total_id_cov_pass",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Reads passing both identity and coverage thresholds",
                },
                FieldDescription {
                    name: "wall_time_sec",
                    arrow_type: "float",
                    nullable: false,
                    description: "Wall clock time in seconds",
                },
            ],
        }
    }

    fn execute(&self, config: &serde_json::Value, shm_input: &str) -> Response {
        // -- Parse ref_paths (required) --
        let ref_path_strs: Vec<String> = match config.get("ref_paths").and_then(|v| v.as_array()) {
            Some(arr) => arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            None => {
                return Response::error("ref_paths is required (array of reference FASTA paths)")
            }
        };
        if ref_path_strs.is_empty() {
            return Response::error("ref_paths must contain at least one reference FASTA path");
        }

        // -- Read input from shared memory --
        let (read_ids, sequences, sequences2) = match Self::read_input(shm_input) {
            Ok(data) => data,
            Err(e) => return Response::error(e),
        };

        if read_ids.is_empty() {
            return Response::error("At least 1 sequence required");
        }

        let is_paired = sequences2.is_some();

        // -- Parse config parameters --
        let num_threads = config
            .get("num_threads")
            .and_then(|v| v.as_i64())
            .unwrap_or(2) as i32;
        let match_score = config.get("match").and_then(|v| v.as_i64()).unwrap_or(2) as i32;
        let mismatch = config
            .get("mismatch")
            .and_then(|v| v.as_i64())
            .unwrap_or(-3) as i32;
        let gap_open = config.get("gap_open").and_then(|v| v.as_i64()).unwrap_or(5) as i32;
        let gap_ext = config.get("gap_ext").and_then(|v| v.as_i64()).unwrap_or(2) as i32;
        let score_n = config.get("score_N").and_then(|v| v.as_i64()).unwrap_or(-3) as i32;
        let evalue = config
            .get("evalue")
            .and_then(|v| v.as_f64())
            .unwrap_or(-1.0);
        let seed_win_len = config
            .get("seed_win_len")
            .and_then(|v| v.as_i64())
            .unwrap_or(18) as u32;
        let num_alignments = config
            .get("num_alignments")
            .and_then(|v| v.as_i64())
            .unwrap_or(1) as u32;
        let best = config.get("best").and_then(|v| v.as_bool()).unwrap_or(true);
        let forward_only = config
            .get("forward_only")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let reverse_only = config
            .get("reverse_only")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let full_search = config
            .get("full_search")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let verbose = config
            .get("verbose")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // -- Workdir (optional) --
        let workdir_cstring = config
            .get("workdir")
            .and_then(|v| v.as_str())
            .and_then(|s| CString::new(s).ok());

        // -- Build C ref_paths --
        let ref_cstrings: Vec<CString> = match ref_path_strs
            .iter()
            .map(|s| CString::new(s.as_str()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(_) => return Response::error("ref_paths contains interior null byte"),
        };
        let ref_ptrs: Vec<*const c_char> = ref_cstrings.iter().map(|s| s.as_ptr()).collect();

        // -- Build smr_seq_t array --
        // For paired mode, interleave: [fwd0, rev0, fwd1, rev1, ...]
        let (seq_ids_c, seq_seqs_c, smr_seqs) = if is_paired {
            let seqs2 = sequences2.as_ref().unwrap();
            let mut ids = Vec::with_capacity(read_ids.len() * 2);
            let mut seqs = Vec::with_capacity(sequences.len() * 2);
            for i in 0..read_ids.len() {
                let id = match CString::new(read_ids[i].as_str()) {
                    Ok(v) => v,
                    Err(_) => {
                        return Response::error(format!(
                            "read_id '{}' contains interior null byte",
                            read_ids[i]
                        ))
                    }
                };
                let s1 = match CString::new(sequences[i].as_str()) {
                    Ok(v) => v,
                    Err(_) => {
                        return Response::error(format!(
                            "sequence for '{}' contains interior null byte",
                            read_ids[i]
                        ))
                    }
                };
                let s2 = match CString::new(seqs2[i].as_str()) {
                    Ok(v) => v,
                    Err(_) => {
                        return Response::error(format!(
                            "sequence2 for '{}' contains interior null byte",
                            read_ids[i]
                        ))
                    }
                };
                // Forward read
                ids.push(id.clone());
                seqs.push(s1);
                // Reverse read (same id)
                ids.push(id);
                seqs.push(s2);
            }
            let smr: Vec<SmrSeq> = ids
                .iter()
                .zip(seqs.iter())
                .map(|(id, seq)| SmrSeq {
                    id: id.as_ptr(),
                    sequence: seq.as_ptr(),
                    quality: ptr::null(),
                })
                .collect();
            (ids, seqs, smr)
        } else {
            let ids: Vec<CString> = match read_ids
                .iter()
                .map(|s| CString::new(s.as_str()))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(v) => v,
                Err(_) => return Response::error("read_id contains interior null byte"),
            };
            let seqs: Vec<CString> = match sequences
                .iter()
                .map(|s| CString::new(s.as_str()))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(v) => v,
                Err(_) => return Response::error("sequence contains interior null byte"),
            };
            let smr: Vec<SmrSeq> = ids
                .iter()
                .zip(seqs.iter())
                .map(|(id, seq)| SmrSeq {
                    id: id.as_ptr(),
                    sequence: seq.as_ptr(),
                    quality: ptr::null(),
                })
                .collect();
            (ids, seqs, smr)
        };

        // Keep backing CStrings alive for the duration of the FFI call
        let _keep_ids = &seq_ids_c;
        let _keep_seqs = &seq_seqs_c;

        unsafe {
            // -- Initialize config --
            let mut cfg: SmrConfig = std::mem::zeroed();
            smr_config_init(&mut cfg);
            cfg.num_threads = num_threads;
            cfg.match_score = match_score;
            cfg.mismatch = mismatch;
            cfg.gap_open = gap_open;
            cfg.gap_ext = gap_ext;
            cfg.score_n = score_n;
            cfg.evalue = evalue;
            cfg.seed_win_len = seed_win_len;
            cfg.num_alignments = num_alignments;
            cfg.best = if best { 1 } else { 0 };
            cfg.paired = if is_paired { 1 } else { 0 };
            cfg.forward_only = if forward_only { 1 } else { 0 };
            cfg.reverse_only = if reverse_only { 1 } else { 0 };
            cfg.full_search = if full_search { 1 } else { 0 };

            if let Some(ref wd) = workdir_cstring {
                cfg.workdir = wd.as_ptr();
            }

            if verbose {
                cfg.log_callback = Some(stderr_log_callback);
            }

            // -- Create context --
            let ctx = smr_ctx_create(&cfg);
            if ctx.is_null() {
                return Response::error("Failed to create SortMeRNA context");
            }

            // -- Run alignment --
            let mut output: *mut SmrOutput = ptr::null_mut();
            let mut stats: SmrStats = std::mem::zeroed();

            let rc = smr_run_seqs(
                ctx,
                ref_ptrs.as_ptr(),
                ref_ptrs.len() as i32,
                smr_seqs.as_ptr(),
                smr_seqs.len() as i32,
                &mut output,
                &mut stats,
            );

            if rc != SMR_OK {
                let err_msg = CStr::from_ptr(smr_last_error(ctx))
                    .to_string_lossy()
                    .into_owned();
                let code_msg = CStr::from_ptr(smr_strerror(rc))
                    .to_string_lossy()
                    .into_owned();
                smr_ctx_destroy(ctx);
                return Response::error(format!("{code_msg}: {err_msg}"));
            }

            // -- Convert output to Arrow --
            let batch = match soa_to_record_batch(&*output) {
                Ok(b) => b,
                Err(e) => {
                    smr_output_free(output);
                    smr_ctx_destroy(ctx);
                    return Response::error(e);
                }
            };

            let shm_out = match crate::arrow_ipc::write_batch_to_output_shm(&batch, "alignments") {
                Ok(v) => v,
                Err(e) => {
                    smr_output_free(output);
                    smr_ctx_destroy(ctx);
                    return Response::error(e);
                }
            };

            // -- Build response metadata --
            let result = serde_json::json!({
                "total_reads": stats.total_reads,
                "total_aligned": stats.total_aligned,
                "total_id_cov_pass": stats.total_id_cov_pass,
                "wall_time_sec": stats.wall_time_sec,
            });

            // -- Cleanup --
            smr_output_free(output);
            smr_ctx_destroy(ctx);

            Response::ok(result, vec![shm_out])
        }
    }

    fn create_streaming_context(
        &self,
        config: &serde_json::Value,
    ) -> Result<Option<Box<dyn StreamingContext>>, String> {
        // Extract and validate ref_paths
        let ref_path_strs: Vec<String> = match config.get("ref_paths") {
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    v.as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| format!("ref_paths[{i}] is not a string"))
                })
                .collect::<Result<Vec<_>, _>>()?,
            Some(_) => return Err("ref_paths must be an array of strings".to_string()),
            None => return Err("Config missing required field: ref_paths".to_string()),
        };
        if ref_path_strs.is_empty() {
            return Err("ref_paths must not be empty".to_string());
        }

        let ref_cstrings: Vec<CString> = ref_path_strs
            .iter()
            .map(|s| CString::new(s.as_str()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "ref_paths contains interior null byte".to_string())?;
        let ref_ptrs: Vec<*const c_char> = ref_cstrings.iter().map(|s| s.as_ptr()).collect();

        // Parse scoring/alignment config
        let num_threads = config
            .get("num_threads")
            .and_then(|v| v.as_i64())
            .unwrap_or(2) as i32;
        let match_score = config.get("match").and_then(|v| v.as_i64()).unwrap_or(2) as i32;
        let mismatch = config
            .get("mismatch")
            .and_then(|v| v.as_i64())
            .unwrap_or(-3) as i32;
        let gap_open = config.get("gap_open").and_then(|v| v.as_i64()).unwrap_or(5) as i32;
        let gap_ext = config.get("gap_ext").and_then(|v| v.as_i64()).unwrap_or(2) as i32;
        let score_n = config.get("score_N").and_then(|v| v.as_i64()).unwrap_or(-3) as i32;
        let evalue = config
            .get("evalue")
            .and_then(|v| v.as_f64())
            .unwrap_or(-1.0);
        let seed_win_len = config
            .get("seed_win_len")
            .and_then(|v| v.as_i64())
            .unwrap_or(18) as u32;
        let num_alignments = config
            .get("num_alignments")
            .and_then(|v| v.as_i64())
            .unwrap_or(1) as u32;
        let best = config.get("best").and_then(|v| v.as_bool()).unwrap_or(true);
        let forward_only = config
            .get("forward_only")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let reverse_only = config
            .get("reverse_only")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let full_search = config
            .get("full_search")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let verbose = config
            .get("verbose")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let workdir_cstring = config
            .get("workdir")
            .and_then(|v| v.as_str())
            .and_then(|s| CString::new(s).ok());

        unsafe {
            let mut cfg: SmrConfig = std::mem::zeroed();
            smr_config_init(&mut cfg);
            cfg.num_threads = num_threads;
            cfg.match_score = match_score;
            cfg.mismatch = mismatch;
            cfg.gap_open = gap_open;
            cfg.gap_ext = gap_ext;
            cfg.score_n = score_n;
            cfg.evalue = evalue;
            cfg.seed_win_len = seed_win_len;
            cfg.num_alignments = num_alignments;
            cfg.best = if best { 1 } else { 0 };
            cfg.forward_only = if forward_only { 1 } else { 0 };
            cfg.reverse_only = if reverse_only { 1 } else { 0 };
            cfg.full_search = if full_search { 1 } else { 0 };

            if let Some(ref wd) = workdir_cstring {
                cfg.workdir = wd.as_ptr();
            }

            if verbose {
                cfg.log_callback = Some(stderr_log_callback);
            }

            let ctx = smr_ctx_create(&cfg);
            if ctx.is_null() {
                return Err("Failed to create SortMeRNA context".to_string());
            }

            Ok(Some(Box::new(SortMeRnaStreamingContext {
                ctx,
                ref_cstrings,
                ref_ptrs,
            })))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shm::SharedMemory;
    use crate::test_util::{read_arrow_from_shm, unique_shm_name, write_arrow_to_shm};

    fn input_schema() -> Schema {
        Schema::new(vec![
            Field::new("read_id", DataType::Utf8, false),
            Field::new("sequence", DataType::Utf8, false),
        ])
    }

    fn make_input_batch(read_ids: &[&str], sequences: &[&str]) -> RecordBatch {
        let schema = Arc::new(input_schema());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(read_ids.to_vec())),
                Arc::new(StringArray::from(sequences.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_paired_input_batch(
        read_ids: &[&str],
        sequences: &[&str],
        sequences2: &[&str],
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("read_id", DataType::Utf8, false),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("sequence2", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(read_ids.to_vec())),
                Arc::new(StringArray::from(sequences.to_vec())),
                Arc::new(StringArray::from(sequences2.to_vec())),
            ],
        )
        .unwrap()
    }

    // AB271211 sequence from ext/sortmerna/data/test_read.fasta (1487 nt)
    const TEST_SEQ_AB271211: &str = "\
TCCAACGCGTTGGGAGCTCTCCCATATGGTCGACCTGCAGGCGGCCGCACTAGTGATTAG\
AGTTTGATCCTGGCTCAGGATGAACGCTGGCGGCGTGCCTAACACATGCAAGTCGAACGG\
GAATCTTCGGATTCTAGTGGCGGACGGGTGAGTAACGCGTAAGAATCTAACTTCAGGACG\
GGGACAACAGTGGGAAACGACTGCTAATACCCGATGTGCCGCGAGGTGAAACCTAATTGG\
CCTGAAGAGGAGCTTGCGTCTGATTAGCTAGTTGGTGGGGTAAGAGCCTACCAAGGCGAC\
GATCAGTAGCTGGTCTGAGAGGATGAGCAGCCACACTGGGACTGAGACACGGCCCAGACTC\
CTACGGGAGGCAGCAGTGGGGAATTTTCCGCAATGGGCGAAAGCCTGACGGAGCAACGCC\
GCGTGAGGGAGGAAGGTCTTTGGATTGTAAACCTCTTTTCTCAAGGAAGAAGTTCTGACGG\
TACTTGAGGAATCAGCCTCGGCTAACTCCGTGCCAGCAGCCGCGGTAATACGGGGGAGGC\
AAGCGTTATCCGGAATTATTGGGCGTAAAGCGTCCGCAGGTGGTCAGCCAAGTCTGCCGT\
CAAATCAGGTTGCTTAACGACCTAAAGGCGGTGGAAACTGGCAGACTAGAGAGCAGTAGGG\
GTAGCAGGAATTCCCAGTGTAGCGGTGAAATGCGTAGAGATTGGGAAGAACATCGGTGGC\
GAAAGCGTGCTACTGGGCTGTATCTGACACTCAGGGACGAAAGCTAGGGGAGCGAAAGGG\
ATTAGATACCCCTGTAGTCCTAGCCGTAAACGATGGATACTAGGCGTGGCTTGTATCGACC\
CGAGCCGTGCCGAAGCTAACGCGTTAAGTATCCCGCCTGGGGAGTACGCACGCAAGTGTG\
AAACTCAAAGGAATTGACGGGGGCCCGCACAAGCGGTGGAGTATGTGGTTTAATTCGATG\
CAACGCGAAGAACCTTACCAAGACTTGACATGTCGCGAACCCTGGTGAAAGCTGGGGGTG\
CCTTCGGGAGCGCGAACACAGGTGGTGCATGGCTGTCGTCAGCTCGTGTCGTGAGATGTT\
GGGTTAAGTCCCGCAACGAGCGCAACCCTCGTTCTTAGTTGCCAGCATTAAGTTGGGGAC\
TCTAAGGAGACTGCCGGTGACAAACCGGAGGAAGGTGGGGATGACGTCAAGTCAGCATGC\
CCCTTACGTCTTGGGCGACACACGTACTACAATGGTCGGGACAAAGGGCAGCGAACTTGCG\
AGAGCCAGCGAATCCCAGCAAACCCGGCCTCAGTTCAGATTGCAGGCTGCAACTCGCCTGC\
ATGAAGGAGGAATCGCTAGTAATCGCCGGTCAGCATACGGCGGTGAATTCGTTCCCGGGC\
CTTGTACACACCGCCCGTCACACCATGGAAGCTGGTCACGCCCGAAGTCATTACCTCAACC\
GCAAGGAGGGGGATGCCTAAGGCAGGGCTAGTGACTGGGG";

    // Reverse-complement of TEST_SEQ_AB271211
    const TEST_SEQ_AB271211_RC: &str = "\
CCCCAGTCACTAGCCCTGCCTTAGGCATCCCCCTCCTTGCGGTTGAGGTAATGACTTCGGG\
CGTGACCAGCTTCCATGGTGTGACGGGCGGTGTGTACAAGGCCCGGGAACGAATTCACCG\
CCGTATGCTGACCGGCGATTACTAGCGATTCCTCCTTCATGCAGGCGAGTTGCAGCCTGC\
AATCTGAACTGAGGCCGGGTTTGCTGGGATTCGCTGGCTCTCGCAAGTTCGCTGCCCTTT\
GTCCCGACCATTGTAGTACGTGTGTCGCCCAAGACGTAAGGGGCATGCTGACTTGACGTC\
ATCCCCACCTTCCTCCGGTTTGTCACCGGCAGTCTCCTTAGAGTCCCCAACTTAATGCTGG\
CAACTAAGAACGAGGGTTGCGCTCGTTGCGGGACTTAACCCAACATCTCACGACACGAGC\
TGACGACAGCCATGCACCACCTGTGTTCGCGCTCCCGAAGGCACCCCCAGCTTTCACCAGG\
GTTCGCGACATGTCAAGTCTTGGTAAGGTTCTTCGCGTTGCATCGAATTAAACCACATAC\
TCCACCGCTTGTGCGGGCCCCCGTCAATTCCTTTGAGTTTCACACTTGCGTGCGTACTCC\
CCAGGCGGGATACTTAACGCGTTAGCTTCGGCACGGCTCGGGTCGATACAAGCCACGCCTA\
GTATCCATCGTTTACGGCTAGGACTACAGGGGTATCTAATCCCTTTCGCTCCCCTAGCTTT\
CGTCCCTGAGTGTCAGATACAGCCCAGTAGCACGCTTTCGCCACCGATGTTCTTCCCAATC\
TCTACGCATTTCACCGCTACACTGGGAATTCCTGCTACCCCTACTGCTCTCTAGTCTGCCA\
GTTTCCACCGCCTTTAGGTCGTTAAGCAACCTGATTTGACGGCAGACTTGGCTGACCACCT\
GCGGACGCTTTACGCCCAATAATTCCGGATAACGCTTGCCTCCCCCGTATTACCGCGGCTG\
CTGGCACGGAGTTAGCCGAGGCTGATTCCTCAAGTACCGTCAGAACTTCTTCCTTGAGAAA\
AGAGGTTTACAATCCAAAGACCTTCCTCCCTCACGCGGCGTTGCTCCGTCAGGCTTTCGCC\
CATTGCGGAAAATTCCCCACTGCTGCCTCCCGTAGGAGTCTGGGCCGTGTCTCAGTCCCAG\
TGTGGCTGCTCATCCTCTCAGACCAGCTACTGATCGTCGCCTTGGTAGGCTCTTACCCCAC\
CAACTAGCTAATCAGACGCAAGCTCCTCTTCAGGCCAATTAGGTTTCACCTCGCGGCACAT\
CGGGTATTAGCAGTCGTTTCCCACTGTTGTCCCCGTCCTGAAGTTAGATTCTTACGCGTTA\
CTCACCCGTCCGCCACTAGAATCCGAAGATTCCCGTTCGACTTGCATGTGTTAGGCACGCC\
GCCAGCGTTCATCCTGAGCCAGGATCAAACTCTAATCACTAGTGCGGCCGCCTGCAGGTCG\
ACCATATGGGAGAGCTCCCAACGCGTTGGA";

    // ---------------------------------------------------------------
    // ABI size check
    // ---------------------------------------------------------------

    #[test]
    fn test_config_struct_abi_size() {
        let mut config: SmrConfig = unsafe { std::mem::zeroed() };
        unsafe { smr_config_init(&mut config) };
        assert_eq!(
            config.struct_size as usize,
            std::mem::size_of::<SmrConfig>(),
            "ABI mismatch: Rust SmrConfig ({} bytes) vs C smr_config_t ({} bytes). \
             Check field types and padding against ext/sortmerna/include/smr_api.h.",
            std::mem::size_of::<SmrConfig>(),
            config.struct_size,
        );
    }

    // ---------------------------------------------------------------
    // Schema v2: NULL emission for unaligned rows. Built from a fixture
    // SmrOutput so we can deterministically exercise the unaligned path
    // without depending on a synthesized-no-hit reference dataset.
    // ---------------------------------------------------------------

    #[test]
    fn test_soa_to_record_batch_unaligned_emits_null() {
        // 2 rows: row 0 aligned, row 1 unaligned.
        let read_a = CString::new("read_a").unwrap();
        let read_b = CString::new("read_b").unwrap();
        let read_ids: [*const c_char; 2] = [read_a.as_ptr(), read_b.as_ptr()];
        let aligned: [i32; 2] = [1, 0];
        let strand: [i32; 2] = [1, -1];
        let ref_index: [i32; 2] = [0, -1];
        let e_value: [f64; 2] = [1e-30, 0.0];
        let identity: [f64; 2] = [99.0, 0.0];
        let coverage: [f64; 2] = [100.0, 0.0];
        let ref_start: [i32; 2] = [10, 0];
        let ref_end: [i32; 2] = [40, 0];
        let score: [i32; 2] = [42, -1];
        let edit_distance: [i32; 2] = [0, -1];
        let cigar_a = CString::new("30M").unwrap();
        let cigars: [*const c_char; 2] = [cigar_a.as_ptr(), ptr::null()];
        let ref_name_a = CString::new("ref1").unwrap();
        let ref_names: [*const c_char; 2] = [ref_name_a.as_ptr(), ptr::null()];

        let output = SmrOutput {
            num_reads: 2,
            num_aligned: 1,
            read_ids: read_ids.as_ptr(),
            aligned: aligned.as_ptr(),
            ref_index: ref_index.as_ptr(),
            e_value: e_value.as_ptr(),
            identity: identity.as_ptr(),
            coverage: coverage.as_ptr(),
            ref_start: ref_start.as_ptr(),
            ref_end: ref_end.as_ptr(),
            cigar: cigars.as_ptr(),
            ref_name: ref_names.as_ptr(),
            strand: strand.as_ptr(),
            score: score.as_ptr(),
            edit_distance: edit_distance.as_ptr(),
        };

        let batch = unsafe { soa_to_record_batch(&output) }.unwrap();
        assert_eq!(batch.num_rows(), 2);

        let strand_col = batch
            .column_by_name("strand")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ref_start_col = batch
            .column_by_name("ref_start")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ref_end_col = batch
            .column_by_name("ref_end")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let score_col = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let edit_distance_col = batch
            .column_by_name("edit_distance")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Aligned row: real values.
        assert_eq!(strand_col.value(0), 1);
        assert_eq!(ref_start_col.value(0), 10);
        assert_eq!(ref_end_col.value(0), 40);
        assert_eq!(score_col.value(0), 42);
        assert_eq!(edit_distance_col.value(0), 0);

        // Unaligned row: every alignment-only column NULL.
        assert!(strand_col.is_null(1), "strand must be NULL for unaligned");
        assert!(
            ref_start_col.is_null(1),
            "ref_start must be NULL for unaligned"
        );
        assert!(ref_end_col.is_null(1), "ref_end must be NULL for unaligned");
        assert!(score_col.is_null(1), "score must be NULL for unaligned");
        assert!(
            edit_distance_col.is_null(1),
            "edit_distance must be NULL for unaligned"
        );

        // ref_name and cigar were already nullable in v1; verify they
        // still emit NULL on the unaligned row.
        let ref_name_col = batch
            .column_by_name("ref_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let cigar_col = batch
            .column_by_name("cigar")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(ref_name_col.is_null(1));
        assert!(cigar_col.is_null(1));
    }

    // ---------------------------------------------------------------
    // Error paths
    // ---------------------------------------------------------------

    #[test]
    fn test_sortmerna_bad_input_shm() {
        let tool = SortMeRnaTool;
        let config = serde_json::json!({"ref_paths": ["ext/sortmerna/data/test_ref.fasta"]});
        let response = tool.execute(&config, "/nonexistent-shm-name");
        assert!(!response.success);
        assert!(response
            .error
            .as_ref()
            .unwrap()
            .contains("Failed to open shm"));
    }

    #[test]
    fn test_sortmerna_missing_ref_paths() {
        let input_name = unique_shm_name("smr-err");
        let batch = make_input_batch(&["r1"], &["ACGT"]);
        let _shm = write_arrow_to_shm(&input_name, &batch);
        let tool = SortMeRnaTool;
        let response = tool.execute(&serde_json::json!({}), &input_name);
        assert!(!response.success);
        assert!(
            response.error.as_ref().unwrap().contains("ref_paths"),
            "Expected error about ref_paths, got: {:?}",
            response.error
        );
    }

    // ---------------------------------------------------------------
    // Single-end roundtrip
    // ---------------------------------------------------------------

    #[test]
    fn test_sortmerna_single_end_roundtrip() {
        let input_name = unique_shm_name("smr-se");
        let batch = make_input_batch(&["AB271211"], &[TEST_SEQ_AB271211]);
        let _shm = write_arrow_to_shm(&input_name, &batch);

        let tool = SortMeRnaTool;
        let config = serde_json::json!({
            "ref_paths": ["ext/sortmerna/data/test_ref.fasta"],
            "num_threads": 1,
        });
        let response = tool.execute(&config, &input_name);

        assert!(response.success, "Failed: {:?}", response.error);
        assert_eq!(response.shm_outputs.len(), 1);
        assert_eq!(response.shm_outputs[0].label, "alignments");
        assert!(response.shm_outputs[0].name.starts_with("/gb-"));
        assert!(response.shm_outputs[0].size > 0);

        let result = response.result.unwrap();
        assert_eq!(result["total_reads"], 1);
        assert_eq!(result["total_aligned"], 1);

        let batches = read_arrow_from_shm(&response.shm_outputs[0].name);
        assert_eq!(batches.len(), 1);
        let out = &batches[0];
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.num_columns(), 12);

        // Golden values from sortmerna's own test_run_tiny_strand_score_edit
        let aligned_col = out
            .column_by_name("aligned")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(aligned_col.value(0), 1);

        let strand_col = out
            .column_by_name("strand")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(strand_col.value(0), 1); // forward

        let score_col = out
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(score_col.value(0) > 0, "Expected positive alignment score");

        let ed_col = out
            .column_by_name("edit_distance")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(ed_col.value(0) >= 0, "Expected non-negative edit distance");

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    // ---------------------------------------------------------------
    // Reverse strand
    // ---------------------------------------------------------------

    #[test]
    fn test_sortmerna_reverse_strand() {
        let input_name = unique_shm_name("smr-rc");
        let batch = make_input_batch(&["AB271211_rc"], &[TEST_SEQ_AB271211_RC]);
        let _shm = write_arrow_to_shm(&input_name, &batch);

        let tool = SortMeRnaTool;
        let config = serde_json::json!({
            "ref_paths": ["ext/sortmerna/data/test_ref.fasta"],
            "num_threads": 1,
        });
        let response = tool.execute(&config, &input_name);

        assert!(response.success, "Failed: {:?}", response.error);

        let batches = read_arrow_from_shm(&response.shm_outputs[0].name);
        let out = &batches[0];

        let strand_col = out
            .column_by_name("strand")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(strand_col.value(0), 0); // reverse-complement

        let score_col = out
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(
            score_col.value(0) > 0,
            "Expected positive alignment score for RC read"
        );

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    // ---------------------------------------------------------------
    // Paired-end roundtrip
    // ---------------------------------------------------------------

    #[test]
    fn test_sortmerna_paired_end_roundtrip() {
        let input_name = unique_shm_name("smr-pe");
        // Use forward and RC of same read as a "pair"
        let batch =
            make_paired_input_batch(&["AB271211"], &[TEST_SEQ_AB271211], &[TEST_SEQ_AB271211_RC]);
        let _shm = write_arrow_to_shm(&input_name, &batch);

        let tool = SortMeRnaTool;
        let config = serde_json::json!({
            "ref_paths": ["ext/sortmerna/data/test_ref.fasta"],
            "num_threads": 1,
        });
        let response = tool.execute(&config, &input_name);
        assert!(response.success, "Failed: {:?}", response.error);

        let result = response.result.unwrap();
        // Paired mode: 1 input row → 2 reads (interleaved fwd/rev)
        assert_eq!(result["total_reads"], 2);

        let batches = read_arrow_from_shm(&response.shm_outputs[0].name);
        assert_eq!(batches[0].num_rows(), 2);

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    // -- Streaming context tests --

    #[test]
    fn test_sortmerna_create_streaming_context() {
        let tool = SortMeRnaTool;
        let config = serde_json::json!({
            "ref_paths": ["ext/sortmerna/data/test_ref.fasta"],
        });
        let result = tool.create_streaming_context(&config);
        assert!(
            result.is_ok(),
            "create_streaming_context failed: {:?}",
            result.err()
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_sortmerna_streaming_missing_ref_paths() {
        let tool = SortMeRnaTool;
        let config = serde_json::json!({});
        match tool.create_streaming_context(&config) {
            Err(e) => assert!(
                e.contains("ref_paths"),
                "Expected ref_paths error, got: {e}"
            ),
            Ok(_) => panic!("Expected error for missing ref_paths"),
        }
    }

    #[test]
    fn test_sortmerna_streaming_run_batch() {
        let tool = SortMeRnaTool;
        let config = serde_json::json!({
            "ref_paths": ["ext/sortmerna/data/test_ref.fasta"],
            "num_threads": 1,
        });
        let mut ctx = tool
            .create_streaming_context(&config)
            .expect("context creation failed")
            .expect("tool should support streaming");

        let input_name = unique_shm_name("smr-str-in");
        let batch = make_input_batch(&["AB271211"], &[TEST_SEQ_AB271211]);
        let _shm = write_arrow_to_shm(&input_name, &batch);

        let response = ctx.run_batch(&input_name);
        assert!(response.success, "run_batch failed: {:?}", response.error);
        assert_eq!(response.shm_outputs.len(), 1);
        assert_eq!(response.shm_outputs[0].label, "alignments");

        let result = response.result.unwrap();
        assert_eq!(result["total_aligned"], 1);

        let out_batches = read_arrow_from_shm(&response.shm_outputs[0].name);
        assert_eq!(out_batches.len(), 1);
        let out = &out_batches[0];
        let score_col = out
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(score_col.value(0) > 0, "Expected positive alignment score");

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    #[test]
    fn test_sortmerna_streaming_two_batches() {
        let tool = SortMeRnaTool;
        let config = serde_json::json!({
            "ref_paths": ["ext/sortmerna/data/test_ref.fasta"],
            "num_threads": 1,
        });
        let mut ctx = tool
            .create_streaming_context(&config)
            .expect("context creation failed")
            .expect("tool should support streaming");

        // Batch 1: forward read
        let input1 = unique_shm_name("smr-str-b1");
        let batch1 = make_input_batch(&["read_fwd"], &[TEST_SEQ_AB271211]);
        let _shm1 = write_arrow_to_shm(&input1, &batch1);
        let resp1 = ctx.run_batch(&input1);
        assert!(resp1.success, "Batch 1 failed: {:?}", resp1.error);

        let out1 = read_arrow_from_shm(&resp1.shm_outputs[0].name);
        let strand1 = out1[0]
            .column_by_name("strand")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(strand1, 1, "Forward read should have strand=1");
        let _ = SharedMemory::unlink(&resp1.shm_outputs[0].name);

        // Batch 2: reverse complement
        let input2 = unique_shm_name("smr-str-b2");
        let batch2 = make_input_batch(&["read_rc"], &[TEST_SEQ_AB271211_RC]);
        let _shm2 = write_arrow_to_shm(&input2, &batch2);
        let resp2 = ctx.run_batch(&input2);
        assert!(resp2.success, "Batch 2 failed: {:?}", resp2.error);

        let out2 = read_arrow_from_shm(&resp2.shm_outputs[0].name);
        let strand2 = out2[0]
            .column_by_name("strand")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(strand2, 0, "RC read should have strand=0");
        let _ = SharedMemory::unlink(&resp2.shm_outputs[0].name);
    }
}
