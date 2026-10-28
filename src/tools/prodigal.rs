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

/// The ABI version these mirrors were written for (prodigal.h's
/// PRODIGAL_ABI_VERSION); prodigal_config_init_v refuses any other.
const PRODIGAL_ABI_VERSION: c_int = 1;

/// Declares a `#[repr(C)]` mirror of a prodigal.h struct and, for tests,
/// its rows as `prodigal_abi_layout()` lists them, so
/// `test_abi_layout_matches_c` checks every field against the library
/// without a second field list.
/// Outer attributes apply to the struct only: a mirror gated by a cfg other
/// than `test` would need it on the generated impl too.
macro_rules! c_mirror {
    ($(#[$meta:meta])* $c_name:literal => pub struct $name:ident {
        $(pub $field:ident: $ty:ty,)*
    }) => {
        $(#[$meta])*
        #[repr(C)]
        pub struct $name {
            $(pub $field: $ty,)*
        }

        #[cfg(test)]
        impl $name {
            /// (type, field, offset, size): the struct's own row (no field,
            /// offset 0, its size), then one per field, in order.
            fn c_layout() -> Vec<(&'static str, Option<&'static str>, usize, usize)> {
                vec![
                    ($c_name, None, 0, std::mem::size_of::<$name>()),
                    $((
                        $c_name,
                        Some(stringify!($field).trim_start_matches("r#")),
                        std::mem::offset_of!($name, $field),
                        std::mem::size_of::<$ty>(),
                    ),)*
                ]
            }
        }
    };
}

c_mirror! {
    "prodigal_config_t" => pub struct ProdigalConfig {
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
        pub want_proteins: c_int,
    }
}

c_mirror! {
    "prodigal_genes_soa_t" => pub struct ProdigalGenesSoa {
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
        pub protein_offsets: *const i32,
        pub protein_data: *const c_char,
        pub _base: *mut c_void,
    }
}

c_mirror! {
    "prodigal_stats_t" => pub struct ProdigalStats {
        pub n_genes: i32,
        pub n_nodes: i32,
        pub gc_content: c_double,
        pub translation_table: i32,
        pub uses_sd: i32,
        pub best_meta_bin: i32,
        pub best_meta_desc: [c_char; 512],
    }
}

c_mirror! {
    /// A row of prodigal_abi_layout(): the ABI test reads the table with it.
    #[cfg(test)]
    "prodigal_abi_field_t" => pub struct ProdigalAbiField {
        pub r#type: *const c_char,
        pub field: *const c_char,
        pub offset: usize,
        pub size: usize,
    }
}

#[allow(non_camel_case_types)]
type prodigal_ctx_t = c_void;

extern "C" {
    fn prodigal_config_init_v(
        config: *mut ProdigalConfig,
        abi_version: c_int,
        struct_size: usize,
    ) -> c_int;
    fn prodigal_config_validate(
        config: *const ProdigalConfig,
        msg: *mut c_char,
        msg_len: usize,
    ) -> c_int;
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
    fn prodigal_version_string() -> *const c_char;
}

#[cfg(test)]
extern "C" {
    fn prodigal_abi_version() -> c_int;
    fn prodigal_abi_layout(n: *mut usize) -> *const ProdigalAbiField;
}

const PRODIGAL_OK: c_int = 0;

impl ProdigalConfig {
    /// The library's default config (prodigal_config_init_v), or why the
    /// library refuses these mirrors.
    unsafe fn library_defaults() -> Result<Self, String> {
        let mut pd: Self = std::mem::zeroed();
        let size = std::mem::size_of::<Self>();
        if prodigal_config_init_v(&mut pd, PRODIGAL_ABI_VERSION, size) != PRODIGAL_OK {
            return Err(format!(
                "libprodigal refuses ABI version {PRODIGAL_ABI_VERSION} with a \
                 {size}-byte config: rebuild against ext/prodigal/prodigal.h"
            ));
        }
        Ok(pd)
    }
}

/// A NUL-terminated C char buffer as a String (to the buffer's end if it
/// has no NUL).
fn c_buf_to_string(buf: &[c_char]) -> String {
    let bytes: Vec<u8> = buf
        .iter()
        .take_while(|&&b| b != 0)
        .map(|&b| b as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

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
    /// None: the library's default (prodigal_config_init_v)
    trans_table: Option<c_int>,
    closed_ends: bool,
    mask_regions: bool,
    force_nonsd: bool,
    /// None: the library's default, the only weight metagenomic mode takes
    start_weight: Option<f64>,
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
                .map(|t| t as c_int),
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
            start_weight: config.get("start_weight").and_then(|v| v.as_f64()),
            verbose: config
                .get("verbose")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        }
    }

    /// Build a C ProdigalConfig from parsed values, on the library's
    /// defaults.
    unsafe fn c_config(&self) -> Result<ProdigalConfig, String> {
        let mut pd = ProdigalConfig::library_defaults()?;
        if let Some(table) = self.trans_table {
            pd.trans_table = table;
        }
        pd.closed_ends = if self.closed_ends { 1 } else { 0 };
        pd.mask_regions = if self.mask_regions { 1 } else { 0 };
        pd.force_nonsd = if self.force_nonsd { 1 } else { 0 };
        pd.meta_mode = if self.meta_mode { 1 } else { 0 };
        if let Some(weight) = self.start_weight {
            pd.start_weight = weight;
        }
        if self.verbose {
            pd.log_callback = Some(stderr_log_callback);
        }
        Ok(pd)
    }

    /// Create a context, or say why not: the library's reason for a config
    /// it refuses (prodigal_config_validate), else out of memory.
    unsafe fn create_context(&self) -> Result<*mut prodigal_ctx_t, String> {
        let pd = self.c_config()?;
        let ctx = prodigal_create(&pd);
        if !ctx.is_null() {
            return Ok(ctx);
        }
        let mut msg: [c_char; 256] = [0; 256];
        if prodigal_config_validate(&pd, msg.as_mut_ptr(), msg.len()) != PRODIGAL_OK {
            let why = c_buf_to_string(&msg);
            return Err(format!("Invalid prodigal config: {why}"));
        }
        Err("Failed to create prodigal context (out of memory)".to_string())
    }
}

impl ProdigalTool {
    /// Read Arrow IPC stream from shared memory, extract name and sequence columns.
    fn read_input(
        shm_input: &str,
        shm_input_size: usize,
    ) -> Result<(Vec<String>, Vec<String>), String> {
        let batches = crate::arrow_ipc::read_batches_from_shm(shm_input, shm_input_size)?;

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
    fn run_batch(&mut self, shm_input: &str, shm_input_size: usize) -> Response {
        let (names, sequences) = match ProdigalTool::read_input(shm_input, shm_input_size) {
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
                    "best_meta_desc": c_buf_to_string(&last_stats.best_meta_desc),
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
        // The linked library's own version (prodigal.h's runtime query)
        unsafe { CStr::from_ptr(prodigal_version_string()) }
            .to_string_lossy()
            .into_owned()
    }

    fn schema_version(&self) -> u32 {
        1
    }

    fn describe(&self) -> ToolDescription {
        ToolDescription {
            name: "prodigal",
            version: self.version(),
            schema_version: self.schema_version(),
            // 2: trans_table lists the supported tables, and the metagenomic
            // limits of trans_table / force_nonsd / start_weight are stated
            // (Prodigal 2.6.5 refuses other values); rbs_* say "None".
            describe_version: 2,
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
                    description: "NCBI translation table number (11: bacteria, archaea, \
                                  plant plastids). Metagenomic mode takes 11 only (its bins \
                                  choose 11 or 4 per contig); the others need single-genome mode",
                    allowed_values: vec!["4", "5", "9", "11", "13", "15", "21", "25", "26", "32"],
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
                    description: "Force non-Shine-Dalgarno RBS finder (single-genome mode \
                                  only: metagenomic mode refuses it)",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "start_weight",
                    param_type: "float",
                    default: serde_json::json!(4.35),
                    description: "Weight for start codon score, finite and > 0 \
                                  (metagenomic mode takes only the default)",
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
                    description: "Ribosome binding site motif as Prodigal prints it (e.g. \
                                  \"AGGAG\", or an upstream motif such as \"TAA\"); \
                                  \"None\" if none",
                },
                FieldDescription {
                    name: "rbs_spacer",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "RBS spacer (e.g. \"5-10bp\"); \"None\" if none",
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

    fn execute(
        &self,
        config: &serde_json::Value,
        shm_input: &str,
        shm_input_size: usize,
    ) -> Response {
        // Read input sequences from Arrow IPC in shared memory
        let (names, sequences) = match Self::read_input(shm_input, shm_input_size) {
            Ok(data) => data,
            Err(e) => return Response::error(e),
        };

        if names.is_empty() {
            return Response::error("At least 1 sequence required");
        }

        // Parse config
        let parsed = ProdigalParsedConfig::from_json(config);

        unsafe {
            let ctx = match parsed.create_context() {
                Ok(ctx) => ctx,
                Err(e) => return Response::error(e),
            };

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
                    "best_meta_desc": c_buf_to_string(&last_stats.best_meta_desc),
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
        let parsed = ProdigalParsedConfig::from_json(config);

        if !parsed.meta_mode {
            return Err(
                "Prodigal single-genome mode requires all sequences for training; \
                 use meta_mode for streaming"
                    .to_string(),
            );
        }

        unsafe {
            let ctx = parsed.create_context()?;
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

        let input_shm = write_arrow_to_shm(&input_name, &batch);
        let input_shm_size = input_shm.len();

        let tool = ProdigalTool;
        let config = serde_json::json!({
            "meta_mode": true,
            "trans_table": 11,
        });

        let response = tool.execute(&config, &input_name, input_shm_size);
        assert!(
            response.success,
            "Prodigal meta failed: {:?}",
            response.error
        );

        // Verify output shm metadata
        assert_eq!(response.shm_outputs.len(), 1);
        let output_name = &response.shm_outputs[0].name;
        let output_size = response.shm_outputs[0].size;
        assert!(output_name.starts_with("/gb-"));
        assert_eq!(response.shm_outputs[0].label, "genes");
        assert!(output_size > 0);

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
        let out_batches = read_arrow_from_shm(output_name, output_size);
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
        let input_shm = write_arrow_to_shm(&input_name, &batch);
        let input_shm_size = input_shm.len();

        let tool = ProdigalTool;
        let config = serde_json::json!({
            "meta_mode": false,
            "trans_table": 11,
        });

        let response = tool.execute(&config, &input_name, input_shm_size);
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

        let response = tool.execute(&config, "/nonexistent-shm-name", 0);
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
        let input_shm = write_arrow_to_shm(&input_name, &batch);
        let input_shm_size = input_shm.len();

        let response = ctx.run_batch(&input_name, input_shm_size);
        assert!(response.success, "run_batch failed: {:?}", response.error);
        assert_eq!(response.shm_outputs.len(), 1);
        assert_eq!(response.shm_outputs[0].label, "genes");

        let result = response.result.unwrap();
        assert!(result["n_genes"].as_i64().unwrap() > 0);
        assert_eq!(result["n_sequences"], 2);

        let out_batches =
            read_arrow_from_shm(&response.shm_outputs[0].name, response.shm_outputs[0].size);
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
        let shm_holder1 = write_arrow_to_shm(&input1, &batch1);
        let shm_holder1_size = shm_holder1.len();
        let resp1 = ctx.run_batch(&input1, shm_holder1_size);
        assert!(resp1.success, "Batch 1 failed: {:?}", resp1.error);
        let genes1 = resp1.result.as_ref().unwrap()["n_genes"].as_i64().unwrap();
        let _ = SharedMemory::unlink(&resp1.shm_outputs[0].name);

        // Batch 2: different sequence
        let input2 = unique_shm_name("pd-str-b2");
        let batch2 = make_input_batch(&["contig_2"], &[TEST_SEQ_2]);
        let shm_holder2 = write_arrow_to_shm(&input2, &batch2);
        let shm_holder2_size = shm_holder2.len();
        let resp2 = ctx.run_batch(&input2, shm_holder2_size);
        assert!(resp2.success, "Batch 2 failed: {:?}", resp2.error);
        let genes2 = resp2.result.as_ref().unwrap()["n_genes"].as_i64().unwrap();
        let _ = SharedMemory::unlink(&resp2.shm_outputs[0].name);

        // Both batches should find genes independently
        assert!(genes1 > 0, "Batch 1 found no genes");
        assert!(genes2 > 0, "Batch 2 found no genes");
    }

    /// Every mirror matches the library's own layout field by field
    /// (prodigal_abi_layout()), which also catches reordered fields and
    /// additions that fit in padding, and the library's ABI version is the
    /// one the mirrors were written for.
    #[test]
    fn test_abi_layout_matches_c() {
        // The table and its strings are static
        unsafe fn text(p: *const c_char) -> &'static str {
            CStr::from_ptr(p).to_str().expect("ASCII")
        }
        assert_eq!(unsafe { prodigal_abi_version() }, PRODIGAL_ABI_VERSION);
        let mut n = 0usize;
        let table = unsafe { std::slice::from_raw_parts(prodigal_abi_layout(&mut n), n) };
        let c_rows: Vec<_> = table
            .iter()
            .map(|r| unsafe {
                let field = (!r.field.is_null()).then(|| text(r.field));
                (text(r.r#type), field, r.offset, r.size)
            })
            .collect();
        for rust in [
            ProdigalConfig::c_layout(),
            ProdigalGenesSoa::c_layout(),
            ProdigalStats::c_layout(),
            ProdigalAbiField::c_layout(),
        ] {
            let c: Vec<_> = c_rows
                .iter()
                .filter(|r| r.0 == rust[0].0)
                .copied()
                .collect();
            assert_eq!(c, rust, "(type, field, offset, size): C left, Rust right");
        }
    }

    /// What the JSON leaves out keeps the library's defaults
    /// (prodigal_config_init_v; 2.6.5 refuses a metagenomic start weight
    /// that differs from its default by any amount), and --describe
    /// advertises them.
    #[test]
    fn test_omitted_fields_keep_library_defaults() {
        let defaults = unsafe { ProdigalConfig::library_defaults() }.unwrap();
        let pd =
            unsafe { ProdigalParsedConfig::from_json(&serde_json::json!({})).c_config() }.unwrap();
        assert_eq!(pd.trans_table, defaults.trans_table);
        assert_eq!(pd.start_weight.to_bits(), defaults.start_weight.to_bits());

        let desc = ProdigalTool.describe();
        let default_of = |name: &str| {
            let param = desc.config_params.iter().find(|p| p.name == name);
            param.expect(name).default.clone()
        };
        assert_eq!(
            default_of("trans_table"),
            serde_json::json!(defaults.trans_table)
        );
        assert_eq!(
            default_of("start_weight"),
            serde_json::json!(defaults.start_weight)
        );
        for (name, value) in [
            ("closed_ends", defaults.closed_ends),
            ("mask_regions", defaults.mask_regions),
            ("force_nonsd", defaults.force_nonsd),
        ] {
            assert_eq!(default_of(name), serde_json::json!(value != 0), "{name}");
        }
    }

    /// --describe lists exactly the translation tables the library accepts
    /// (single-genome mode; metagenomic mode takes 11 only).
    #[test]
    fn test_describe_trans_tables_are_the_librarys() {
        let desc = ProdigalTool.describe();
        let param = desc.config_params.iter().find(|p| p.name == "trans_table");
        let listed: Vec<c_int> = param
            .expect("trans_table param")
            .allowed_values
            .iter()
            .map(|v| v.parse().expect("integer"))
            .collect();
        let accepted: Vec<c_int> = (1..=99)
            .filter(|&t| unsafe {
                let mut pd = ProdigalConfig::library_defaults().unwrap();
                pd.trans_table = t;
                prodigal_config_validate(&pd, ptr::null_mut(), 0) == PRODIGAL_OK
            })
            .collect();
        assert_eq!(listed, accepted);
    }

    #[test]
    fn test_version_is_the_librarys() {
        assert_eq!(ProdigalTool.version(), "2.6.5");
    }

    /// A config the library refuses is an error carrying the library's
    /// reason (prodigal_config_validate), in execute and in streaming.
    #[test]
    fn test_refused_config_says_why() {
        let reason = |config: &serde_json::Value| unsafe {
            let pd = ProdigalParsedConfig::from_json(config).c_config().unwrap();
            let mut msg: [c_char; 256] = [0; 256];
            let rc = prodigal_config_validate(&pd, msg.as_mut_ptr(), msg.len());
            assert_ne!(rc, PRODIGAL_OK, "{config}: the library accepts it");
            c_buf_to_string(&msg)
        };
        let tool = ProdigalTool;
        let input_name = unique_shm_name("pd-refused");
        let input_shm = write_arrow_to_shm(&input_name, &make_input_batch(&["c1"], &[TEST_SEQ_1]));
        for config in [
            serde_json::json!({"meta_mode": true, "trans_table": 4}),
            serde_json::json!({"meta_mode": true, "force_nonsd": true}),
            serde_json::json!({"meta_mode": true, "start_weight": 5.0}),
            serde_json::json!({"meta_mode": false, "trans_table": 1}),
        ] {
            let response = tool.execute(&config, &input_name, input_shm.len());
            assert!(!response.success, "{config}: expected a refusal");
            let err = response.error.unwrap();
            assert!(err.contains(&reason(&config)), "{config}: {err}");
        }

        // The daemon's route, streaming, reports it the same way
        let config = serde_json::json!({"meta_mode": true, "trans_table": 4});
        match tool.create_streaming_context(&config) {
            Err(err) => assert!(err.contains(&reason(&config)), "{err}"),
            Ok(_) => panic!("expected a refusal"),
        }
    }
}
