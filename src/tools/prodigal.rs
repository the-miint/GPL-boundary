//! Prodigal adapter: reads Arrow IPC from shared memory, runs prodigal
//! gene prediction, writes SOA genes as Arrow IPC back to shared memory.

use crate::protocol::{Response, ShmOutput};
use crate::tools::{ConfigParam, FieldDescription, GplTool, StreamingContext, ToolDescription};
use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_double, c_int, c_void};
use std::ptr;
use std::sync::Arc;

// --- FFI bindings to libprodigal ---

#[repr(C)]
pub struct ProdigalConfig {
    pub struct_size: usize,
    pub trans_table: c_int,
    pub closed_ends: c_int,
    pub mask_regions: c_int,
    pub force_nonsd: c_int,
    pub meta_mode: c_int,
    pub start_weight: c_double,
    pub alloc_fn: Option<unsafe extern "C" fn(usize, *mut c_void) -> *mut c_void>,
    pub free_fn: Option<unsafe extern "C" fn(*mut c_void, *mut c_void)>,
    pub allocator_user_data: *mut c_void,
    pub log_callback: Option<unsafe extern "C" fn(*const c_char, *mut c_void)>,
    pub log_user_data: *mut c_void,
    pub progress_callback:
        Option<unsafe extern "C" fn(*const c_char, c_double, *mut c_void) -> c_int>,
    pub progress_user_data: *mut c_void,
}

#[repr(C)]
pub struct ProdigalGenesSoa {
    pub n_genes: i32,
    pub begin: *const i32,
    pub end: *const i32,
    pub strand: *const i32,
    pub partial_left: *const i32,
    pub partial_right: *const i32,
    pub start_type: *const i32,
    pub cscore: *const c_double,
    pub sscore: *const c_double,
    pub rscore: *const c_double,
    pub uscore: *const c_double,
    pub tscore: *const c_double,
    pub confidence: *const c_double,
    pub gc_cont: *const c_double,
    pub rbs_motif: *const *const c_char,
    pub rbs_spacer: *const *const c_char,
    pub _base: *mut c_void,
}

#[repr(C)]
pub struct ProdigalStats {
    pub n_genes: i32,
    pub n_nodes: i32,
    pub gc_content: c_double,
    pub translation_table: i32,
    pub uses_sd: i32,
    pub best_meta_bin: i32,
    pub best_meta_desc: [c_char; 512],
}

#[allow(non_camel_case_types)]
type prodigal_ctx_t = c_void;

extern "C" {
    fn prodigal_config_init(config: *mut ProdigalConfig);
    fn prodigal_create(config: *const ProdigalConfig) -> *mut prodigal_ctx_t;
    fn prodigal_destroy(ctx: *mut prodigal_ctx_t);
    fn prodigal_set_sequence(
        ctx: *mut prodigal_ctx_t,
        seq: *const c_char,
        len: i32,
        header: *const c_char,
    ) -> c_int;
    fn prodigal_set_training_sequences(
        ctx: *mut prodigal_ctx_t,
        seqs: *const *const c_char,
        headers: *const *const c_char,
        lens: *const i32,
        n_seqs: i32,
    ) -> c_int;
    fn prodigal_train(ctx: *mut prodigal_ctx_t) -> c_int;
    fn prodigal_find_genes(
        ctx: *mut prodigal_ctx_t,
        genes_out: *mut *mut ProdigalGenesSoa,
        stats_out: *mut ProdigalStats,
    ) -> c_int;
    fn prodigal_genes_free(genes: *mut ProdigalGenesSoa);
    fn prodigal_strerror(error_code: c_int) -> *const c_char;
    fn prodigal_last_error(ctx: *const prodigal_ctx_t) -> *const c_char;
}

const PRODIGAL_OK: c_int = 0;
const PRODIGAL_VERSION: &str = "2.6.4";

/// C-callable log callback that writes messages to stderr.
unsafe extern "C" fn stderr_log_callback(msg: *const c_char, _user_data: *mut c_void) {
    if !msg.is_null() {
        let s = CStr::from_ptr(msg).to_string_lossy();
        eprint!("{s}");
    }
}

// --- Arrow schema definitions ---

/// Input schema: contig sequences from miint.
#[allow(dead_code)]
pub fn input_schema() -> Schema {
    Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("sequence", DataType::Utf8, false),
    ])
}

/// Output schema: predicted genes as columnar data for miint.
fn output_schema() -> Schema {
    Schema::new(vec![
        Field::new("seq_name", DataType::Utf8, false),
        Field::new("begin", DataType::Int32, false),
        Field::new("end", DataType::Int32, false),
        Field::new("strand", DataType::Int32, false),
        Field::new("partial_left", DataType::Boolean, false),
        Field::new("partial_right", DataType::Boolean, false),
        Field::new("start_type", DataType::Int32, false),
        Field::new("cscore", DataType::Float64, false),
        Field::new("sscore", DataType::Float64, false),
        Field::new("rscore", DataType::Float64, false),
        Field::new("uscore", DataType::Float64, false),
        Field::new("tscore", DataType::Float64, false),
        Field::new("confidence", DataType::Float64, false),
        Field::new("gc_cont", DataType::Float64, false),
        Field::new("rbs_motif", DataType::Utf8, false),
        Field::new("rbs_spacer", DataType::Utf8, false),
    ])
}

// --- Gene accumulator ---

/// Accumulates gene results from multiple per-contig find_genes calls
/// into parallel Vecs that map directly to Arrow columns.
struct GeneAccumulator {
    seq_names: Vec<String>,
    begin: Vec<i32>,
    end: Vec<i32>,
    strand: Vec<i32>,
    partial_left: Vec<bool>,
    partial_right: Vec<bool>,
    start_type: Vec<i32>,
    cscore: Vec<f64>,
    sscore: Vec<f64>,
    rscore: Vec<f64>,
    uscore: Vec<f64>,
    tscore: Vec<f64>,
    confidence: Vec<f64>,
    gc_cont: Vec<f64>,
    rbs_motif: Vec<String>,
    rbs_spacer: Vec<String>,
}

impl GeneAccumulator {
    fn new() -> Self {
        Self {
            seq_names: Vec::new(),
            begin: Vec::new(),
            end: Vec::new(),
            strand: Vec::new(),
            partial_left: Vec::new(),
            partial_right: Vec::new(),
            start_type: Vec::new(),
            cscore: Vec::new(),
            sscore: Vec::new(),
            rscore: Vec::new(),
            uscore: Vec::new(),
            tscore: Vec::new(),
            confidence: Vec::new(),
            gc_cont: Vec::new(),
            rbs_motif: Vec::new(),
            rbs_spacer: Vec::new(),
        }
    }

    /// Read C SOA arrays into Rust Vecs. Caller must ensure soa is valid
    /// and n_genes > 0.
    unsafe fn append_from_soa(&mut self, seq_name: &str, soa: &ProdigalGenesSoa) {
        let n = soa.n_genes as usize;

        let begin = std::slice::from_raw_parts(soa.begin, n);
        let end = std::slice::from_raw_parts(soa.end, n);
        let strand = std::slice::from_raw_parts(soa.strand, n);
        let partial_left = std::slice::from_raw_parts(soa.partial_left, n);
        let partial_right = std::slice::from_raw_parts(soa.partial_right, n);
        let start_type = std::slice::from_raw_parts(soa.start_type, n);
        let cscore = std::slice::from_raw_parts(soa.cscore, n);
        let sscore = std::slice::from_raw_parts(soa.sscore, n);
        let rscore = std::slice::from_raw_parts(soa.rscore, n);
        let uscore = std::slice::from_raw_parts(soa.uscore, n);
        let tscore = std::slice::from_raw_parts(soa.tscore, n);
        let confidence = std::slice::from_raw_parts(soa.confidence, n);
        let gc_cont = std::slice::from_raw_parts(soa.gc_cont, n);
        let motif_ptrs = std::slice::from_raw_parts(soa.rbs_motif, n);
        let spacer_ptrs = std::slice::from_raw_parts(soa.rbs_spacer, n);

        for _ in 0..n {
            self.seq_names.push(seq_name.to_string());
        }
        self.begin.extend_from_slice(begin);
        self.end.extend_from_slice(end);
        self.strand.extend_from_slice(strand);
        self.partial_left
            .extend(partial_left.iter().map(|&v| v != 0));
        self.partial_right
            .extend(partial_right.iter().map(|&v| v != 0));
        self.start_type.extend_from_slice(start_type);
        self.cscore.extend_from_slice(cscore);
        self.sscore.extend_from_slice(sscore);
        self.rscore.extend_from_slice(rscore);
        self.uscore.extend_from_slice(uscore);
        self.tscore.extend_from_slice(tscore);
        self.confidence.extend_from_slice(confidence);
        self.gc_cont.extend_from_slice(gc_cont);

        for &p in motif_ptrs {
            self.rbs_motif.push(if p.is_null() {
                String::new()
            } else {
                CStr::from_ptr(p).to_string_lossy().into_owned()
            });
        }
        for &p in spacer_ptrs {
            self.rbs_spacer.push(if p.is_null() {
                String::new()
            } else {
                CStr::from_ptr(p).to_string_lossy().into_owned()
            });
        }
    }

    fn n_genes(&self) -> usize {
        self.begin.len()
    }

    fn into_record_batch(self) -> Result<RecordBatch, String> {
        let schema = Arc::new(output_schema());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    self.seq_names
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<&str>>(),
                )),
                Arc::new(Int32Array::from(self.begin)),
                Arc::new(Int32Array::from(self.end)),
                Arc::new(Int32Array::from(self.strand)),
                Arc::new(BooleanArray::from(self.partial_left)),
                Arc::new(BooleanArray::from(self.partial_right)),
                Arc::new(Int32Array::from(self.start_type)),
                Arc::new(Float64Array::from(self.cscore)),
                Arc::new(Float64Array::from(self.sscore)),
                Arc::new(Float64Array::from(self.rscore)),
                Arc::new(Float64Array::from(self.uscore)),
                Arc::new(Float64Array::from(self.tscore)),
                Arc::new(Float64Array::from(self.confidence)),
                Arc::new(Float64Array::from(self.gc_cont)),
                Arc::new(StringArray::from(
                    self.rbs_motif
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<&str>>(),
                )),
                Arc::new(StringArray::from(
                    self.rbs_spacer
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<&str>>(),
                )),
            ],
        )
        .map_err(|e| format!("Failed to create Arrow RecordBatch: {e}"))
    }
}

// --- Tool implementation ---

pub struct ProdigalTool;

inventory::submit! {
    crate::tools::ToolRegistration {
        create: || Box::new(ProdigalTool),
    }
}

/// Parsed prodigal config values shared by execute() and create_streaming_context().
struct ProdigalParsedConfig {
    meta_mode: bool,
    trans_table: c_int,
    closed_ends: bool,
    mask_regions: bool,
    force_nonsd: bool,
    start_weight: f64,
    verbose: bool,
}

impl ProdigalParsedConfig {
    fn from_json(config: &serde_json::Value) -> Self {
        Self {
            meta_mode: config
                .get("meta_mode")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            trans_table: config
                .get("trans_table")
                .and_then(|v| v.as_i64())
                .unwrap_or(11) as c_int,
            closed_ends: config
                .get("closed_ends")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            mask_regions: config
                .get("mask_regions")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            force_nonsd: config
                .get("force_nonsd")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            start_weight: config
                .get("start_weight")
                .and_then(|v| v.as_f64())
                .unwrap_or(4.35),
            verbose: config
                .get("verbose")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        }
    }

    /// Build a C ProdigalConfig from parsed values.
    unsafe fn to_c_config(&self) -> ProdigalConfig {
        let mut pd: ProdigalConfig = std::mem::zeroed();
        prodigal_config_init(&mut pd);
        pd.trans_table = self.trans_table;
        pd.closed_ends = if self.closed_ends { 1 } else { 0 };
        pd.mask_regions = if self.mask_regions { 1 } else { 0 };
        pd.force_nonsd = if self.force_nonsd { 1 } else { 0 };
        pd.meta_mode = if self.meta_mode { 1 } else { 0 };
        pd.start_weight = self.start_weight;
        if self.verbose {
            pd.log_callback = Some(stderr_log_callback);
        }
        pd
    }
}

/// Convert the fixed-size `best_meta_desc` char array from ProdigalStats to String.
fn best_meta_desc_to_string(stats: &ProdigalStats) -> String {
    let bytes = &stats.best_meta_desc;
    let nul_pos = bytes.iter().position(|&b| b == 0).unwrap_or(512);
    let slice = &bytes[..nul_pos];
    let u8_slice: &[u8] =
        unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len()) };
    String::from_utf8_lossy(u8_slice).into_owned()
}

impl ProdigalTool {
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

    /// Build Arrow RecordBatch from accumulated genes and write to output shm.
    fn write_output(accumulator: GeneAccumulator) -> Result<ShmOutput, String> {
        let batch = accumulator.into_record_batch()?;
        crate::arrow_ipc::write_batch_to_output_shm(&batch, "genes")
    }
}

// --- Streaming context ---

/// Holds a prodigal context for batched streaming. The metagenomic bin
/// initialization (~28MB) happens once on the first `find_genes` call
/// and is reused across all subsequent batches.
struct ProdigalStreamingContext {
    ctx: *mut prodigal_ctx_t,
}

// Safety: The underlying C context is not thread-safe, but no shared
// references to self.ctx are handed out. Only &mut self methods exist
// (run_batch), so exclusive access is enforced at the type level.
unsafe impl Send for ProdigalStreamingContext {}

impl Drop for ProdigalStreamingContext {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            unsafe { prodigal_destroy(self.ctx) };
        }
    }
}

impl StreamingContext for ProdigalStreamingContext {
    fn run_batch(&mut self, shm_input: &str) -> Response {
        let (names, sequences) = match ProdigalTool::read_input(shm_input) {
            Ok(data) => data,
            Err(e) => return Response::error(e),
        };

        if names.is_empty() {
            return Response::error("At least 1 sequence required");
        }

        unsafe {
            let mut accumulator = GeneAccumulator::new();
            let mut last_stats: ProdigalStats = std::mem::zeroed();

            for (name, sequence) in names.iter().zip(sequences.iter()) {
                if sequence.len() > i32::MAX as usize {
                    return Response::error(format!(
                        "Sequence '{}' is too long: {} bytes (max {})",
                        name,
                        sequence.len(),
                        i32::MAX
                    ));
                }
                let c_seq = match CString::new(sequence.as_str()) {
                    Ok(v) => v,
                    Err(_) => {
                        return Response::error(format!(
                            "Sequence '{name}' contains interior null byte"
                        ));
                    }
                };
                let c_header = match CString::new(name.as_str()) {
                    Ok(v) => v,
                    Err(_) => {
                        return Response::error(format!(
                            "Sequence name '{name}' contains interior null byte"
                        ));
                    }
                };
                let seq_len = sequence.len() as i32;

                // prodigal_set_sequence resets internal state from any previous
                // sequence. The context is safe to reuse after errors — the
                // next set_sequence call will reset.
                let rc =
                    prodigal_set_sequence(self.ctx, c_seq.as_ptr(), seq_len, c_header.as_ptr());
                if rc != PRODIGAL_OK {
                    let err_msg = CStr::from_ptr(prodigal_last_error(self.ctx))
                        .to_string_lossy()
                        .into_owned();
                    let code_msg = CStr::from_ptr(prodigal_strerror(rc))
                        .to_string_lossy()
                        .into_owned();
                    return Response::error(format!("{code_msg}: {err_msg}"));
                }

                let mut genes: *mut ProdigalGenesSoa = ptr::null_mut();
                let mut stats: ProdigalStats = std::mem::zeroed();

                let rc = prodigal_find_genes(self.ctx, &mut genes, &mut stats);
                if rc != PRODIGAL_OK {
                    let err_msg = CStr::from_ptr(prodigal_last_error(self.ctx))
                        .to_string_lossy()
                        .into_owned();
                    let code_msg = CStr::from_ptr(prodigal_strerror(rc))
                        .to_string_lossy()
                        .into_owned();
                    return Response::error(format!("{code_msg}: {err_msg}"));
                }

                if !genes.is_null() {
                    if (*genes).n_genes > 0 {
                        accumulator.append_from_soa(name, &*genes);
                    }
                    prodigal_genes_free(genes);
                }
                last_stats = stats;
            }

            let total_genes = accumulator.n_genes();

            let shm_out = match ProdigalTool::write_output(accumulator) {
                Ok(v) => v,
                Err(e) => return Response::error(e),
            };

            let result = serde_json::json!({
                "n_genes": total_genes,
                "n_sequences": names.len(),
                "stats": {
                    "gc_content": last_stats.gc_content,
                    "translation_table": last_stats.translation_table,
                    "uses_sd": last_stats.uses_sd,
                    "best_meta_bin": last_stats.best_meta_bin,
                    "best_meta_desc": best_meta_desc_to_string(&last_stats),
                }
            });

            Response::ok(result, vec![shm_out])
        }
    }
}

impl GplTool for ProdigalTool {
    fn name(&self) -> &str {
        "prodigal"
    }

    fn version(&self) -> String {
        PRODIGAL_VERSION.to_string()
    }

    fn schema_version(&self) -> u32 {
        1
    }

    fn describe(&self) -> ToolDescription {
        ToolDescription {
            name: "prodigal",
            version: self.version(),
            schema_version: self.schema_version(),
            describe_version: 1,
            description: "Prokaryotic gene prediction for metagenomic and single-genome sequences",
            config_params: vec![
                ConfigParam {
                    name: "meta_mode",
                    param_type: "boolean",
                    default: serde_json::json!(true),
                    description:
                        "Use pre-trained metagenomic models (true) or train on input (false)",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "trans_table",
                    param_type: "integer",
                    default: serde_json::json!(11),
                    description:
                        "NCBI translation table number (e.g., 11 for standard prokaryotic)",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "closed_ends",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Do not allow genes to run off sequence edges",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "mask_regions",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Mask runs of N's as unknown regions",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "force_nonsd",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Force non-Shine-Dalgarno RBS finder",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "start_weight",
                    param_type: "float",
                    default: serde_json::json!(4.35),
                    description: "Weight for start codon score",
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
                    description: "Sequence (contig) identifier",
                },
                FieldDescription {
                    name: "sequence",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Nucleotide sequence (sequences may have different lengths)",
                },
            ],
            output_schema: vec![
                FieldDescription {
                    name: "seq_name",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Source contig identifier",
                },
                FieldDescription {
                    name: "begin",
                    arrow_type: "Int32",
                    nullable: false,
                    description: "Gene start position (1-based)",
                },
                FieldDescription {
                    name: "end",
                    arrow_type: "Int32",
                    nullable: false,
                    description: "Gene end position (1-based)",
                },
                FieldDescription {
                    name: "strand",
                    arrow_type: "Int32",
                    nullable: false,
                    description: "Strand (+1 or -1)",
                },
                FieldDescription {
                    name: "partial_left",
                    arrow_type: "Boolean",
                    nullable: false,
                    description: "Gene is partial at left edge",
                },
                FieldDescription {
                    name: "partial_right",
                    arrow_type: "Boolean",
                    nullable: false,
                    description: "Gene is partial at right edge",
                },
                FieldDescription {
                    name: "start_type",
                    arrow_type: "Int32",
                    nullable: false,
                    description: "Start codon type (0=ATG, 1=GTG, 2=TTG, 3=Edge)",
                },
                FieldDescription {
                    name: "cscore",
                    arrow_type: "Float64",
                    nullable: false,
                    description: "Coding score",
                },
                FieldDescription {
                    name: "sscore",
                    arrow_type: "Float64",
                    nullable: false,
                    description: "Start score",
                },
                FieldDescription {
                    name: "rscore",
                    arrow_type: "Float64",
                    nullable: false,
                    description: "RBS score",
                },
                FieldDescription {
                    name: "uscore",
                    arrow_type: "Float64",
                    nullable: false,
                    description: "Upstream score",
                },
                FieldDescription {
                    name: "tscore",
                    arrow_type: "Float64",
                    nullable: false,
                    description: "Type score",
                },
                FieldDescription {
                    name: "confidence",
                    arrow_type: "Float64",
                    nullable: false,
                    description: "Gene confidence [50, 100]",
                },
                FieldDescription {
                    name: "gc_cont",
                    arrow_type: "Float64",
                    nullable: false,
                    description: "GC content of gene region",
                },
                FieldDescription {
                    name: "rbs_motif",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Ribosome binding site motif (empty string if none assigned)",
                },
                FieldDescription {
                    name: "rbs_spacer",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "RBS spacer region (empty string if none assigned)",
                },
            ],
            response_metadata: vec![
                FieldDescription {
                    name: "n_genes",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Total gene count across all sequences",
                },
                FieldDescription {
                    name: "n_sequences",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Number of input sequences processed",
                },
                FieldDescription {
                    name: "stats.gc_content",
                    arrow_type: "float64",
                    nullable: false,
                    description: "GC content from last processed sequence",
                },
                FieldDescription {
                    name: "stats.translation_table",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Translation table used",
                },
                FieldDescription {
                    name: "stats.uses_sd",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Whether Shine-Dalgarno motifs were used",
                },
                FieldDescription {
                    name: "stats.best_meta_bin",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Best metagenomic bin index (-1 if not meta mode)",
                },
                FieldDescription {
                    name: "stats.best_meta_desc",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Description of best metagenomic bin",
                },
            ],
        }
    }

    fn execute(&self, config: &serde_json::Value, shm_input: &str) -> Response {
        // Read input sequences from Arrow IPC in shared memory
        let (names, sequences) = match Self::read_input(shm_input) {
            Ok(data) => data,
            Err(e) => return Response::error(e),
        };

        if names.is_empty() {
            return Response::error("At least 1 sequence required");
        }

        // Parse config
        let parsed = ProdigalParsedConfig::from_json(config);

        unsafe {
            let pd_config = parsed.to_c_config();

            let ctx = prodigal_create(&pd_config);
            if ctx.is_null() {
                return Response::error("Failed to create prodigal context");
            }

            // Single-genome mode: train on all input sequences first
            if !parsed.meta_mode {
                let c_seqs: Vec<CString> = match sequences
                    .iter()
                    .map(|s| CString::new(s.as_str()))
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(v) => v,
                    Err(_) => {
                        prodigal_destroy(ctx);
                        return Response::error("Sequence contains interior null byte");
                    }
                };
                let c_headers: Vec<CString> = match names
                    .iter()
                    .map(|n| CString::new(n.as_str()))
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(v) => v,
                    Err(_) => {
                        prodigal_destroy(ctx);
                        return Response::error("Sequence name contains interior null byte");
                    }
                };
                let seq_lens: Vec<i32> = sequences.iter().map(|s| s.len() as i32).collect();
                let seq_ptrs: Vec<*const c_char> = c_seqs.iter().map(|s| s.as_ptr()).collect();
                let hdr_ptrs: Vec<*const c_char> = c_headers.iter().map(|h| h.as_ptr()).collect();

                let rc = prodigal_set_training_sequences(
                    ctx,
                    seq_ptrs.as_ptr(),
                    hdr_ptrs.as_ptr(),
                    seq_lens.as_ptr(),
                    sequences.len() as i32,
                );
                if rc != PRODIGAL_OK {
                    let err_msg = CStr::from_ptr(prodigal_last_error(ctx))
                        .to_string_lossy()
                        .into_owned();
                    let code_msg = CStr::from_ptr(prodigal_strerror(rc))
                        .to_string_lossy()
                        .into_owned();
                    prodigal_destroy(ctx);
                    return Response::error(format!("{code_msg}: {err_msg}"));
                }

                let rc = prodigal_train(ctx);
                if rc != PRODIGAL_OK {
                    let err_msg = CStr::from_ptr(prodigal_last_error(ctx))
                        .to_string_lossy()
                        .into_owned();
                    let code_msg = CStr::from_ptr(prodigal_strerror(rc))
                        .to_string_lossy()
                        .into_owned();
                    prodigal_destroy(ctx);
                    return Response::error(format!("{code_msg}: {err_msg}"));
                }
            }

            // Find genes per contig
            let mut accumulator = GeneAccumulator::new();
            let mut last_stats: ProdigalStats = std::mem::zeroed();

            for (name, sequence) in names.iter().zip(sequences.iter()) {
                let c_seq = match CString::new(sequence.as_str()) {
                    Ok(v) => v,
                    Err(_) => {
                        prodigal_destroy(ctx);
                        return Response::error(format!(
                            "Sequence '{name}' contains interior null byte"
                        ));
                    }
                };
                let c_header = match CString::new(name.as_str()) {
                    Ok(v) => v,
                    Err(_) => {
                        prodigal_destroy(ctx);
                        return Response::error(format!(
                            "Sequence name '{name}' contains interior null byte"
                        ));
                    }
                };
                let seq_len = sequence.len() as i32;

                let rc = prodigal_set_sequence(ctx, c_seq.as_ptr(), seq_len, c_header.as_ptr());
                if rc != PRODIGAL_OK {
                    let err_msg = CStr::from_ptr(prodigal_last_error(ctx))
                        .to_string_lossy()
                        .into_owned();
                    let code_msg = CStr::from_ptr(prodigal_strerror(rc))
                        .to_string_lossy()
                        .into_owned();
                    prodigal_destroy(ctx);
                    return Response::error(format!("{code_msg}: {err_msg}"));
                }

                let mut genes: *mut ProdigalGenesSoa = ptr::null_mut();
                let mut stats: ProdigalStats = std::mem::zeroed();

                let rc = prodigal_find_genes(ctx, &mut genes, &mut stats);
                if rc != PRODIGAL_OK {
                    let err_msg = CStr::from_ptr(prodigal_last_error(ctx))
                        .to_string_lossy()
                        .into_owned();
                    let code_msg = CStr::from_ptr(prodigal_strerror(rc))
                        .to_string_lossy()
                        .into_owned();
                    prodigal_destroy(ctx);
                    return Response::error(format!("{code_msg}: {err_msg}"));
                }

                if !genes.is_null() {
                    if (*genes).n_genes > 0 {
                        accumulator.append_from_soa(name, &*genes);
                    }
                    prodigal_genes_free(genes);
                }
                last_stats = stats;
            }

            // Capture total gene count before moving accumulator
            let total_genes = accumulator.n_genes();

            // Write accumulated genes to output shm
            let shm_out = match Self::write_output(accumulator) {
                Ok(v) => v,
                Err(e) => {
                    prodigal_destroy(ctx);
                    return Response::error(e);
                }
            };

            let result = serde_json::json!({
                "n_genes": total_genes,
                "n_sequences": names.len(),
                "stats": {
                    "gc_content": last_stats.gc_content,
                    "translation_table": last_stats.translation_table,
                    "uses_sd": last_stats.uses_sd,
                    "best_meta_bin": last_stats.best_meta_bin,
                    "best_meta_desc": best_meta_desc_to_string(&last_stats),
                }
            });

            prodigal_destroy(ctx);

            Response::ok(result, vec![shm_out])
        }
    }

    fn create_streaming_context(
        &self,
        config: &serde_json::Value,
    ) -> Result<Option<Box<dyn StreamingContext>>, String> {
        let mut parsed = ProdigalParsedConfig::from_json(config);

        if !parsed.meta_mode {
            return Err(
                "Prodigal single-genome mode requires all sequences for training; \
                 use meta_mode for streaming"
                    .to_string(),
            );
        }
        // Streaming always uses meta mode (enforce regardless of parsed value)
        parsed.meta_mode = true;

        unsafe {
            let pd_config = parsed.to_c_config();

            let ctx = prodigal_create(&pd_config);
            if ctx.is_null() {
                return Err("Failed to create prodigal context".to_string());
            }

            Ok(Some(Box::new(ProdigalStreamingContext { ctx })))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shm::SharedMemory;
    use crate::test_util::{read_arrow_from_shm, unique_shm_name, write_arrow_to_shm};
    use arrow::array::Array;

    // Test sequences from Prodigal's anthus_aco.fas (960 bp each)
    const TEST_SEQ_1: &str = "ACAGGTTAGAAACTACTCTGTTTTCTGGCTCCTTGTTTAATGCCCTGTCCTATTTTATTGCGAAAATTGTCTGTTTTTCACAGAAAACTGAGAGTAGTCAAGGGATTCCTTGTCCTTTGCTTTGGTCTGCACAGCTGTCTTGTTTTAAGGCCAAGTGGAATGAGACAGCTGACTCTTCAGGTGTGAAAACTTGGATGTAGGTGTAGATTGGCTCTCAGTTGACCTCCAGCTGGTGCAGATTCTTCAGTTTGTTTGATGGAGCTTTGAGAAGTCCTTTCAGACTGAAGGATACTCTGAATTTTAGCTATGGAGATAATGTGGATGTTGTGTGTTTAAGTCCTGTTTGCAGTTTTTTTCTGTTCAGTCAGTTATTTTACTGTGTGAGTCAGGACCCTTAGAAGCCCTCAGTGGCAACCACAGAGCGCACAGTTAATTTTCTGTGCAAGAAAATTAAGATCATACTCTGTGTCCAGGAAAGTCAAGAATATTCCTGGTTTTCTCTACTGTAAAATTTTATCTTGTAACTTGTGTTTGGGTCTGCATGATTATTCAAAAATCTTAGTAGATTTGGAAGGATGTTGCATATTATGGAAACAAAGTTGGAAAAAGTTTGTATCAGTTGCAGTATTTCTTCACATCATTTNTTAACNNCNTNNNNNNNNNGCTTCTGCCACTTGAAAAGACAAATTAAAAACNAATTTATAATGCTTATATGCTTTAGTTACATTNGGGTCTTTCAGTAACTTTAGTGCTTTTGATAGCCATACCTGTGAGNTTGACAGTGTCTAAAATTAGAAGTGTTCCTTTTCTTCTGCTCTTCCCATTCTCGTGTGTCTTCAATAGTTTCTGCAAATAATGATGTGCAGACTTAGCATTGATTCAACAGCAGAGGTAAGCATACCTGTGGCTTACTTGGCTTCAGCTTATCCAGCAGTGCCAACCACTCTCTGTTTGTCTTAC";
    const TEST_SEQ_2: &str = "ACAGGTTAGAAACTACTCTGTTTTCTGGCTGCTTGTTTAATGCCCTCTCCTATTTTATTGTGACGATTGTCTGTTTTTCACAGAAAACTGAGAGTAGTCAAGGGATTCCTTGTCCTTTGCTTTGGTGTGCACAGCTGTCTTGTTTTAAGGCCCAGTGGAATGAGACAGCTGACTCTTCAGGTGTGAAAACTTGGATGTACGTGTAGATTGGCTCTCAGTTGACCTCCAGCTGGTGCAGATTCTTCAGTTTGTTTGATGGAGCTTTGAGAAGTCCTTTCAGACTGAAGGATACTCTGAATTTTAGCTATGGAAATAATGTGGATGTTGTGTGTTTAAGCACTGTTTGCAGTTTTTTTCTGTTCAGTCAGTTATTTTACTGTGTGAGTCAGGACTCTTAGAAGCCCTCTGTGGCAACCACAGAGCGCATAGTTAATTTTCTGTACAAGAAAATTAAGATCCTACTCAGTGTTCAGGAAAGTCAAGAATATTCCTGGTTTTCTCTACTGTAAAATTTTATCTTGTAACTTGTGTTTGGGTCTGCATGATTATTCAAAAATCTTAGTAGATTTGGAAGGATGTTGCATTTTATGGAAACAAAGTTGGGAAAAGTTTGTATCAGTTCCAGTATTTCTTCACATCATTTNTTAACNNCNTNNNNNNNNNGCTTCTACCACTTGAAAAGACAAATTAAAAACNAATTTATAATGCTTATATGCTTTAGTTACATTNGGGTCTTTCAGTAACTTTAGTGCTTTACTTAGCCATACCTGTGAGCNTGGCAGTGTCTAAAATTAGAAGTGTTCCTTTTCTTCTGCTCTTCCCATTCTCGTGTGTCTTCAATAGTTTCTGCAAATAATGATGTGCAGACTTAGCATTGGTTCAACAGCAGAGGTAAAAATACCTGTGGCTTACCCGGCTTCAGCTTATCCAGCAGTGCCAACCACTCTCTGTTTGTCTTAC";

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
    fn test_prodigal_meta_roundtrip() {
        let input_name = unique_shm_name("pd-meta-in");

        let batch = make_input_batch(&["contig_1", "contig_2"], &[TEST_SEQ_1, TEST_SEQ_2]);

        let _input_shm = write_arrow_to_shm(&input_name, &batch);

        let tool = ProdigalTool;
        let config = serde_json::json!({
            "meta_mode": true,
            "trans_table": 11,
        });

        let response = tool.execute(&config, &input_name);
        assert!(
            response.success,
            "Prodigal meta failed: {:?}",
            response.error
        );

        // Verify output shm metadata
        assert_eq!(response.shm_outputs.len(), 1);
        let output_name = &response.shm_outputs[0].name;
        assert!(output_name.starts_with("/gb-"));
        assert_eq!(response.shm_outputs[0].label, "genes");
        assert!(response.shm_outputs[0].size > 0);

        // Verify JSON metadata
        let result = response.result.unwrap();
        assert!(
            result["n_genes"].as_i64().unwrap() > 0,
            "Expected prodigal to find genes in test sequences"
        );
        assert_eq!(result["n_sequences"], 2);
        assert!(result["stats"]["gc_content"].as_f64().is_some());
        assert!(result["stats"]["best_meta_bin"].as_i64().unwrap() >= 0);

        // Verify Arrow IPC output
        let out_batches = read_arrow_from_shm(output_name);
        assert_eq!(out_batches.len(), 1);
        let out = &out_batches[0];
        assert_eq!(out.num_columns(), 16);

        // Check column names
        assert_eq!(out.schema().field(0).name(), "seq_name");
        assert_eq!(out.schema().field(1).name(), "begin");
        assert_eq!(out.schema().field(2).name(), "end");
        assert_eq!(out.schema().field(3).name(), "strand");
        assert_eq!(out.schema().field(4).name(), "partial_left");
        assert_eq!(out.schema().field(5).name(), "partial_right");
        assert_eq!(out.schema().field(6).name(), "start_type");
        assert_eq!(out.schema().field(14).name(), "rbs_motif");
        assert_eq!(out.schema().field(15).name(), "rbs_spacer");

        // Verify seq_name values come from input contig names
        if out.num_rows() > 0 {
            let seq_name_col = out
                .column_by_name("seq_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..seq_name_col.len() {
                let val = seq_name_col.value(i);
                assert!(
                    val == "contig_1" || val == "contig_2",
                    "Unexpected seq_name: {val}"
                );
            }
        }

        // Caller cleans up output shm
        let _ = SharedMemory::unlink(output_name);
    }

    #[test]
    fn test_prodigal_single_genome_too_short() {
        let input_name = unique_shm_name("pd-sg-in");

        // 960 bp sequences are far below the 20000 bp minimum for training
        let batch = make_input_batch(&["contig_1"], &[TEST_SEQ_1]);
        let _input_shm = write_arrow_to_shm(&input_name, &batch);

        let tool = ProdigalTool;
        let config = serde_json::json!({
            "meta_mode": false,
            "trans_table": 11,
        });

        let response = tool.execute(&config, &input_name);
        assert!(
            !response.success,
            "Expected failure for too-short training input"
        );
        assert!(
            response.error.as_ref().unwrap().contains("too short")
                || response.error.as_ref().unwrap().contains("Too short")
                || response
                    .error
                    .as_ref()
                    .unwrap()
                    .contains("Sequence too short"),
            "Expected sequence-too-short error, got: {:?}",
            response.error
        );
    }

    #[test]
    fn test_prodigal_bad_input_shm() {
        let tool = ProdigalTool;
        let config = serde_json::json!({"meta_mode": true});

        let response = tool.execute(&config, "/nonexistent-shm-name");
        assert!(!response.success);
        assert!(response.error.unwrap().contains("Failed to open shm"));
    }

    // -- Streaming context tests --

    #[test]
    fn test_prodigal_create_streaming_context() {
        let tool = ProdigalTool;
        let config = serde_json::json!({"meta_mode": true});
        let result = tool.create_streaming_context(&config);
        assert!(
            result.is_ok(),
            "create_streaming_context failed: {:?}",
            result.err()
        );
        assert!(
            result.unwrap().is_some(),
            "Expected Some(ctx) for meta_mode streaming"
        );
    }

    #[test]
    fn test_prodigal_streaming_rejects_single_genome() {
        let tool = ProdigalTool;
        let config = serde_json::json!({"meta_mode": false});
        match tool.create_streaming_context(&config) {
            Err(err) => assert!(
                err.contains("single-genome") || err.contains("training"),
                "Expected single-genome rejection, got: {err}"
            ),
            Ok(_) => panic!("Expected error for single-genome streaming"),
        }
    }

    #[test]
    fn test_prodigal_streaming_run_batch() {
        let tool = ProdigalTool;
        let config = serde_json::json!({"meta_mode": true, "trans_table": 11});
        let mut ctx = tool
            .create_streaming_context(&config)
            .expect("context creation failed")
            .expect("tool should support streaming");

        let input_name = unique_shm_name("pd-str-in");
        let batch = make_input_batch(&["contig_1", "contig_2"], &[TEST_SEQ_1, TEST_SEQ_2]);
        let _input_shm = write_arrow_to_shm(&input_name, &batch);

        let response = ctx.run_batch(&input_name);
        assert!(response.success, "run_batch failed: {:?}", response.error);
        assert_eq!(response.shm_outputs.len(), 1);
        assert_eq!(response.shm_outputs[0].label, "genes");

        let result = response.result.unwrap();
        assert!(result["n_genes"].as_i64().unwrap() > 0);
        assert_eq!(result["n_sequences"], 2);

        let out_batches = read_arrow_from_shm(&response.shm_outputs[0].name);
        assert_eq!(out_batches.len(), 1);
        assert!(out_batches[0].num_rows() > 0);

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    #[test]
    fn test_prodigal_streaming_two_batches() {
        let tool = ProdigalTool;
        let config = serde_json::json!({"meta_mode": true});
        let mut ctx = tool
            .create_streaming_context(&config)
            .expect("context creation failed")
            .expect("tool should support streaming");

        // Batch 1: one sequence
        let input1 = unique_shm_name("pd-str-b1");
        let batch1 = make_input_batch(&["contig_1"], &[TEST_SEQ_1]);
        let _shm1 = write_arrow_to_shm(&input1, &batch1);
        let resp1 = ctx.run_batch(&input1);
        assert!(resp1.success, "Batch 1 failed: {:?}", resp1.error);
        let genes1 = resp1.result.as_ref().unwrap()["n_genes"].as_i64().unwrap();
        let _ = SharedMemory::unlink(&resp1.shm_outputs[0].name);

        // Batch 2: different sequence
        let input2 = unique_shm_name("pd-str-b2");
        let batch2 = make_input_batch(&["contig_2"], &[TEST_SEQ_2]);
        let _shm2 = write_arrow_to_shm(&input2, &batch2);
        let resp2 = ctx.run_batch(&input2);
        assert!(resp2.success, "Batch 2 failed: {:?}", resp2.error);
        let genes2 = resp2.result.as_ref().unwrap()["n_genes"].as_i64().unwrap();
        let _ = SharedMemory::unlink(&resp2.shm_outputs[0].name);

        // Both batches should find genes independently
        assert!(genes1 > 0, "Batch 1 found no genes");
        assert!(genes2 > 0, "Batch 2 found no genes");
    }

    #[test]
    fn test_config_struct_abi_size() {
        let mut config: ProdigalConfig = unsafe { std::mem::zeroed() };
        unsafe { prodigal_config_init(&mut config) };
        assert_eq!(
            config.struct_size,
            std::mem::size_of::<ProdigalConfig>(),
            "ABI mismatch: Rust ProdigalConfig ({} bytes) vs C prodigal_config_t ({} bytes). \
             Check field types and padding against ext/prodigal/prodigal.h.",
            std::mem::size_of::<ProdigalConfig>(),
            config.struct_size,
        );
    }
}
