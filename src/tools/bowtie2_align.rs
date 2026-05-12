use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::Arc;

use arrow::array::{
    Array, Int32Array, Int32Builder, Int64Array, StringArray, StringBuilder, UInt16Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::protocol::Response;
use crate::tools::{
    ConfigParam, FieldDescription, GplTool, StreamingContext, ToolDescription, ToolRegistration,
};

// ---------------------------------------------------------------------------
// FFI bindings — mirror ext/bowtie2/bt2_api.h
//
// Bowtie2 uses global mutable state behind a mutex. Only one alignment can
// run at a time per process. This is acceptable for GPL-boundary's
// single-invocation model.
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct Bt2AlignConfig {
    pub struct_size: usize,
    pub index_path: *const c_char,
    pub seed: i64,
    pub nthreads: c_int,
    pub preset: c_int,
    pub local_align: c_int,
    pub quiet: c_int,
    pub log_fn: Option<unsafe extern "C" fn(*mut c_void, c_int, *const c_char)>,
    pub log_user_data: *mut c_void,
    // v0.2 fields
    pub k: c_int,
    pub report_all: c_int,
    pub trim5: c_int,
    pub trim3: c_int,
    pub match_bonus: c_int,
    pub mismatch_penalty: c_int,
    pub n_penalty: c_int,
    pub read_gap_open: c_int,
    pub read_gap_extend: c_int,
    pub ref_gap_open: c_int,
    pub ref_gap_extend: c_int,
    pub score_min: *const c_char,
    pub min_insert: c_int,
    pub max_insert: c_int,
    pub mate_orientation: c_int,
    pub no_mixed: c_int,
    pub no_discordant: c_int,
    pub dovetail: c_int,
    pub no_contain: c_int,
    pub no_overlap: c_int,
    pub nofw: c_int,
    pub norc: c_int,
    pub seed_mismatches: c_int,
    pub seed_length: c_int,
    pub max_dp_failures: c_int,
    pub max_seed_rounds: c_int,
    pub no_unal: c_int,
    pub xeq: c_int,
    pub rg_id: *const c_char,
    pub ignore_quals: c_int,
    pub reorder: c_int,
}

#[repr(C)]
pub struct Bt2AlignOutput {
    pub struct_size: usize,
    pub n_records: usize,
    pub qname: *const *const c_char,
    pub flag: *const i32,
    pub rname: *const *const c_char,
    pub pos: *const i64,
    pub mapq: *const u8,
    pub cigar: *const *const c_char,
    pub rnext: *const *const c_char,
    pub pnext: *const i64,
    pub tlen: *const i64,
    pub seq: *const *const c_char,
    pub qual: *const *const c_char,
    pub tag_as: *const i32,
    pub tag_xs: *const i32,
    pub tag_nm: *const i32,
    pub tag_md: *const *const c_char,
    pub tag_yt: *const *const c_char,
    // v0.3 fields (bt2_api.h BT2_API_VERSION_MINOR=3, commit 7e3f900):
    pub tag_ys: *const i32,
    pub tag_xn: *const i32,
    pub tag_xm: *const i32,
    pub tag_xo: *const i32,
    pub tag_xg: *const i32,
    pub _backing: *mut c_void,
}

#[repr(C)]
pub struct Bt2AlignStats {
    pub n_reads: i64,
    pub n_aligned: i64,
    pub n_unaligned: i64,
    pub n_aligned_concordant: i64,
    pub elapsed_ms: i64,
}

#[repr(C)]
pub struct Bt2Input {
    pub struct_size: usize,
    pub names: *const *const c_char,
    pub seqs: *const *const c_char,
    pub quals: *const *const c_char,
    pub n_reads: usize,
    pub names2: *const *const c_char,
    pub seqs2: *const *const c_char,
    pub quals2: *const *const c_char,
    pub n_reads2: usize,
}

#[allow(non_camel_case_types)]
type bt2_align_ctx_t = c_void;

extern "C" {
    fn bt2_align_config_init(config: *mut Bt2AlignConfig);
    fn bt2_align_create(
        config: *const Bt2AlignConfig,
        error_out: *mut c_int,
    ) -> *mut bt2_align_ctx_t;
    fn bt2_align_destroy(ctx: *mut bt2_align_ctx_t);
    fn bt2_input_init(input: *mut Bt2Input);
    fn bt2_align_run(
        ctx: *mut bt2_align_ctx_t,
        input: *const Bt2Input,
        output_out: *mut *mut Bt2AlignOutput,
        stats_out: *mut Bt2AlignStats,
    ) -> c_int;
    fn bt2_align_output_free(output: *mut Bt2AlignOutput);
    fn bt2_align_last_error(ctx: *const bt2_align_ctx_t) -> *const c_char;
}

use crate::tools::bowtie2_ffi::{bt2_strerror, BT2_OK, BT2_VERSION};

// Preset constants
const BT2_PRESET_VERY_FAST: c_int = 0;
const BT2_PRESET_FAST: c_int = 1;
const BT2_PRESET_SENSITIVE: c_int = 2;
const BT2_PRESET_VERY_SENSITIVE: c_int = 3;

// Mate orientation constants
const BT2_MATE_FR: c_int = 0;
const BT2_MATE_RF: c_int = 1;
const BT2_MATE_FF: c_int = 2;

// ---------------------------------------------------------------------------
// Log callback
// ---------------------------------------------------------------------------

unsafe extern "C" fn stderr_log_callback(
    _user_data: *mut c_void,
    _level: c_int,
    msg: *const c_char,
) {
    if !msg.is_null() {
        let s = CStr::from_ptr(msg).to_string_lossy();
        eprint!("{s}");
    }
}

// ---------------------------------------------------------------------------
// Arrow schemas
// ---------------------------------------------------------------------------

fn output_schema() -> Schema {
    Schema::new(vec![
        Field::new("read_id", DataType::Utf8, false),
        Field::new("flags", DataType::UInt16, false),
        Field::new("reference", DataType::Utf8, false),
        Field::new("position", DataType::Int64, false),
        Field::new("stop_position", DataType::Int64, false),
        Field::new("mapq", DataType::UInt8, false),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mate_reference", DataType::Utf8, false),
        Field::new("mate_position", DataType::Int64, false),
        Field::new("template_length", DataType::Int64, false),
        Field::new("tag_as", DataType::Int32, true),
        Field::new("tag_xs", DataType::Int32, true),
        Field::new("tag_ys", DataType::Int32, true),
        Field::new("tag_xn", DataType::Int32, true),
        Field::new("tag_xm", DataType::Int32, true),
        Field::new("tag_xo", DataType::Int32, true),
        Field::new("tag_xg", DataType::Int32, true),
        Field::new("tag_nm", DataType::Int32, true),
        Field::new("tag_yt", DataType::Utf8, true),
        Field::new("tag_md", DataType::Utf8, true),
        Field::new("tag_sa", DataType::Utf8, true),
    ])
}

// ---------------------------------------------------------------------------
// Tool struct and registration
// ---------------------------------------------------------------------------

pub struct Bowtie2AlignTool;

inventory::submit! {
    ToolRegistration {
        create: || Box::new(Bowtie2AlignTool),
    }
}

// ---------------------------------------------------------------------------
// Input data
// ---------------------------------------------------------------------------

struct Bt2InputData {
    read_ids: Vec<String>,
    seqs1: Vec<String>,
    quals1: Vec<Option<String>>,
    seqs2: Option<Vec<String>>,
    quals2: Option<Vec<Option<String>>>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

impl Bowtie2AlignTool {
    fn read_input(shm_input: &str, shm_input_size: usize) -> Result<Bt2InputData, String> {
        let batches = crate::arrow_ipc::read_batches_from_shm(shm_input, shm_input_size)?;

        let mut read_ids = Vec::new();
        let mut seqs1 = Vec::new();
        let mut quals1: Vec<Option<String>> = Vec::new();
        let mut seqs2: Option<Vec<String>> = None;
        let mut quals2: Option<Vec<Option<String>>> = None;

        for batch in &batches {
            let id_col = batch
                .column_by_name("read_id")
                .ok_or("Input missing 'read_id' column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("'read_id' column is not Utf8")?;

            let seq1_col = batch
                .column_by_name("sequence1")
                .ok_or("Input missing 'sequence1' column")?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("'sequence1' column is not Utf8")?;

            // qual1 is optional (nullable for FASTA)
            let q1_col = batch
                .column_by_name("qual1")
                .map(|c| {
                    c.as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or("'qual1' column is not Utf8")
                })
                .transpose()?;

            // sequence2 is optional (nullable, paired indicator)
            let seq2_col = batch
                .column_by_name("sequence2")
                .map(|c| {
                    c.as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or("'sequence2' column is not Utf8")
                })
                .transpose()?;

            // qual2 is optional
            let q2_col = batch
                .column_by_name("qual2")
                .map(|c| {
                    c.as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or("'qual2' column is not Utf8")
                })
                .transpose()?;

            for i in 0..batch.num_rows() {
                read_ids.push(id_col.value(i).to_string());
                seqs1.push(seq1_col.value(i).to_string());

                quals1.push(match q1_col {
                    Some(col) if !col.is_null(i) => Some(col.value(i).to_string()),
                    _ => None,
                });
            }

            // Detect paired-end from sequence2 column having non-null values
            if let Some(s2) = seq2_col {
                let has_non_null = (0..batch.num_rows()).any(|i| !s2.is_null(i));
                if has_non_null {
                    let buf = seqs2.get_or_insert_with(Vec::new);
                    for i in 0..batch.num_rows() {
                        if s2.is_null(i) {
                            return Err(
                                "sequence2 has mix of null and non-null values; all must be non-null for paired-end"
                                    .to_string(),
                            );
                        }
                        buf.push(s2.value(i).to_string());
                    }

                    if let Some(q2) = q2_col {
                        let qbuf = quals2.get_or_insert_with(Vec::new);
                        for i in 0..batch.num_rows() {
                            if q2.is_null(i) {
                                qbuf.push(None);
                            } else {
                                qbuf.push(Some(q2.value(i).to_string()));
                            }
                        }
                    }
                }
            }
        }

        Ok(Bt2InputData {
            read_ids,
            seqs1,
            quals1,
            seqs2,
            quals2,
        })
    }
}

/// Parse preset string to (preset_int, local_align_int).
/// Accepts: "very-fast", "fast", "sensitive", "very-sensitive",
/// and the -local variants: "very-fast-local", etc.
fn parse_preset(s: &str) -> Result<(c_int, c_int), String> {
    match s {
        "very-fast" => Ok((BT2_PRESET_VERY_FAST, 0)),
        "fast" => Ok((BT2_PRESET_FAST, 0)),
        "sensitive" => Ok((BT2_PRESET_SENSITIVE, 0)),
        "very-sensitive" => Ok((BT2_PRESET_VERY_SENSITIVE, 0)),
        "very-fast-local" => Ok((BT2_PRESET_VERY_FAST, 1)),
        "fast-local" => Ok((BT2_PRESET_FAST, 1)),
        "sensitive-local" => Ok((BT2_PRESET_SENSITIVE, 1)),
        "very-sensitive-local" => Ok((BT2_PRESET_VERY_SENSITIVE, 1)),
        other => Err(format!("Unknown preset: {other}")),
    }
}

/// Parse mate orientation string to BT2_MATE_* constant.
fn parse_mate_orientation(s: &str) -> Result<c_int, String> {
    match s {
        "fr" => Ok(BT2_MATE_FR),
        "rf" => Ok(BT2_MATE_RF),
        "ff" => Ok(BT2_MATE_FF),
        other => Err(format!("Unknown mate_orientation: {other}")),
    }
}

/// Build a Bt2AlignConfig from JSON config.
/// Returns the config and a Vec<CString> that must be kept alive for the
/// duration of the config's use (the config holds raw pointers into them).
fn build_config(
    config: &serde_json::Value,
    verbose: bool,
) -> Result<(Bt2AlignConfig, Vec<CString>), String> {
    let mut bt2: Bt2AlignConfig = unsafe { std::mem::zeroed() };
    unsafe { bt2_align_config_init(&mut bt2) };

    let mut cstrings: Vec<CString> = Vec::new();

    // index_path (required)
    let index_path_str = config
        .get("index_path")
        .and_then(|v| v.as_str())
        .ok_or("index_path is required (string path to bowtie2 index basename)")?;
    let index_path_c =
        CString::new(index_path_str).map_err(|_| "index_path contains interior null byte")?;
    bt2.index_path = index_path_c.as_ptr();
    cstrings.push(index_path_c);

    // Core
    if let Some(v) = config.get("seed").and_then(|v| v.as_i64()) {
        bt2.seed = v;
    }
    if let Some(v) = config.get("nthreads").and_then(|v| v.as_i64()) {
        bt2.nthreads = v as c_int;
    }
    if let Some(s) = config.get("preset").and_then(|v| v.as_str()) {
        let (preset, local) = parse_preset(s)?;
        bt2.preset = preset;
        bt2.local_align = local;
    }
    if let Some(v) = config.get("local_align").and_then(|v| v.as_bool()) {
        bt2.local_align = if v { 1 } else { 0 };
    }

    // Reporting
    if let Some(v) = config.get("k").and_then(|v| v.as_i64()) {
        bt2.k = v as c_int;
    }
    if let Some(v) = config.get("report_all").and_then(|v| v.as_bool()) {
        bt2.report_all = if v { 1 } else { 0 };
    }

    // Trimming
    if let Some(v) = config.get("trim5").and_then(|v| v.as_i64()) {
        bt2.trim5 = v as c_int;
    }
    if let Some(v) = config.get("trim3").and_then(|v| v.as_i64()) {
        bt2.trim3 = v as c_int;
    }

    // Scoring (JSON null / absent -> keep sentinel -1 from config_init)
    if let Some(v) = config.get("match_bonus").and_then(|v| v.as_i64()) {
        bt2.match_bonus = v as c_int;
    }
    if let Some(v) = config.get("mismatch_penalty").and_then(|v| v.as_i64()) {
        bt2.mismatch_penalty = v as c_int;
    }
    if let Some(v) = config.get("n_penalty").and_then(|v| v.as_i64()) {
        bt2.n_penalty = v as c_int;
    }
    if let Some(v) = config.get("read_gap_open").and_then(|v| v.as_i64()) {
        bt2.read_gap_open = v as c_int;
    }
    if let Some(v) = config.get("read_gap_extend").and_then(|v| v.as_i64()) {
        bt2.read_gap_extend = v as c_int;
    }
    if let Some(v) = config.get("ref_gap_open").and_then(|v| v.as_i64()) {
        bt2.ref_gap_open = v as c_int;
    }
    if let Some(v) = config.get("ref_gap_extend").and_then(|v| v.as_i64()) {
        bt2.ref_gap_extend = v as c_int;
    }
    if let Some(s) = config.get("score_min").and_then(|v| v.as_str()) {
        let c = CString::new(s).map_err(|_| "score_min contains interior null byte")?;
        bt2.score_min = c.as_ptr();
        cstrings.push(c);
    }

    // Paired-end
    if let Some(v) = config.get("min_insert").and_then(|v| v.as_i64()) {
        bt2.min_insert = v as c_int;
    }
    if let Some(v) = config.get("max_insert").and_then(|v| v.as_i64()) {
        bt2.max_insert = v as c_int;
    }
    if let Some(s) = config.get("mate_orientation").and_then(|v| v.as_str()) {
        bt2.mate_orientation = parse_mate_orientation(s)?;
    }
    if let Some(v) = config.get("no_mixed").and_then(|v| v.as_bool()) {
        bt2.no_mixed = if v { 1 } else { 0 };
    }
    if let Some(v) = config.get("no_discordant").and_then(|v| v.as_bool()) {
        bt2.no_discordant = if v { 1 } else { 0 };
    }
    if let Some(v) = config.get("dovetail").and_then(|v| v.as_bool()) {
        bt2.dovetail = if v { 1 } else { 0 };
    }
    if let Some(v) = config.get("no_contain").and_then(|v| v.as_bool()) {
        bt2.no_contain = if v { 1 } else { 0 };
    }
    if let Some(v) = config.get("no_overlap").and_then(|v| v.as_bool()) {
        bt2.no_overlap = if v { 1 } else { 0 };
    }

    // Strand
    if let Some(v) = config.get("nofw").and_then(|v| v.as_bool()) {
        bt2.nofw = if v { 1 } else { 0 };
    }
    if let Some(v) = config.get("norc").and_then(|v| v.as_bool()) {
        bt2.norc = if v { 1 } else { 0 };
    }

    // Effort
    if let Some(v) = config.get("seed_mismatches").and_then(|v| v.as_i64()) {
        bt2.seed_mismatches = v as c_int;
    }
    if let Some(v) = config.get("seed_length").and_then(|v| v.as_i64()) {
        bt2.seed_length = v as c_int;
    }
    if let Some(v) = config.get("max_dp_failures").and_then(|v| v.as_i64()) {
        bt2.max_dp_failures = v as c_int;
    }
    if let Some(v) = config.get("max_seed_rounds").and_then(|v| v.as_i64()) {
        bt2.max_seed_rounds = v as c_int;
    }

    // SAM output
    if let Some(v) = config.get("no_unal").and_then(|v| v.as_bool()) {
        bt2.no_unal = if v { 1 } else { 0 };
    }
    if let Some(v) = config.get("xeq").and_then(|v| v.as_bool()) {
        bt2.xeq = if v { 1 } else { 0 };
    }
    if let Some(s) = config.get("rg_id").and_then(|v| v.as_str()) {
        let c = CString::new(s).map_err(|_| "rg_id contains interior null byte")?;
        bt2.rg_id = c.as_ptr();
        cstrings.push(c);
    }

    // Other
    if let Some(v) = config.get("ignore_quals").and_then(|v| v.as_bool()) {
        bt2.ignore_quals = if v { 1 } else { 0 };
    }
    if let Some(v) = config.get("reorder").and_then(|v| v.as_bool()) {
        bt2.reorder = if v { 1 } else { 0 };
    }

    // Log callback
    bt2.quiet = 1;
    if verbose {
        bt2.log_fn = Some(stderr_log_callback);
    }

    Ok((bt2, cstrings))
}

// ---------------------------------------------------------------------------
// Streaming context
// ---------------------------------------------------------------------------

/// Holds a bowtie2 alignment context for batched streaming. The .bt2 index is
/// loaded once during create and reused across all run_batch calls.
struct Bowtie2StreamingContext {
    ctx: *mut bt2_align_ctx_t,
    /// CStrings backing the config's raw pointers. Kept alive for the session
    /// in case the C library retains references (safe even if it deep-copies).
    _config_cstrings: Vec<CString>,
}

// Safety: The underlying C context is not thread-safe, but no shared
// references to self.ctx are handed out. Only &mut self methods exist
// (run_batch), so exclusive access is enforced at the type level.
unsafe impl Send for Bowtie2StreamingContext {}

impl Drop for Bowtie2StreamingContext {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            unsafe { bt2_align_destroy(self.ctx) };
        }
    }
}

impl StreamingContext for Bowtie2StreamingContext {
    fn run_batch(&mut self, shm_input: &str, shm_input_size: usize) -> Response {
        let input_data = match Bowtie2AlignTool::read_input(shm_input, shm_input_size) {
            Ok(data) => data,
            Err(e) => return Response::error(e),
        };

        if input_data.read_ids.is_empty() {
            return Response::error("At least 1 read required");
        }

        // Build per-batch C input arrays (ephemeral — dropped after bt2_align_run)
        let name_cstrings: Vec<CString> = match input_data
            .read_ids
            .iter()
            .map(|s| CString::new(s.as_str()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(_) => return Response::error("read_id contains interior null byte"),
        };
        let name_ptrs: Vec<*const c_char> = name_cstrings.iter().map(|s| s.as_ptr()).collect();

        let seq1_cstrings: Vec<CString> = match input_data
            .seqs1
            .iter()
            .map(|s| CString::new(s.as_str()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(_) => return Response::error("sequence1 contains interior null byte"),
        };
        let seq1_ptrs: Vec<*const c_char> = seq1_cstrings.iter().map(|s| s.as_ptr()).collect();

        let qual1_cstrings: Vec<Option<CString>> = match input_data
            .quals1
            .iter()
            .map(|opt| match opt {
                Some(s) => CString::new(s.as_str()).map(Some),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(_) => return Response::error("qual1 contains interior null byte"),
        };
        let qual1_ptrs: Vec<*const c_char> = qual1_cstrings
            .iter()
            .map(|opt| match opt {
                Some(c) => c.as_ptr(),
                None => ptr::null(),
            })
            .collect();
        let all_quals_null = qual1_ptrs.iter().all(|p| p.is_null());

        // Paired-end mate 2
        let seq2_cstrings: Option<Vec<CString>> = match &input_data.seqs2 {
            Some(seqs) => match seqs
                .iter()
                .map(|s| CString::new(s.as_str()))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(v) => Some(v),
                Err(_) => return Response::error("sequence2 contains interior null byte"),
            },
            None => None,
        };
        let seq2_ptrs: Option<Vec<*const c_char>> = seq2_cstrings
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_ptr()).collect());

        let qual2_cstrings: Option<Vec<Option<CString>>> = match &input_data.quals2 {
            Some(quals) => match quals
                .iter()
                .map(|opt| match opt {
                    Some(s) => CString::new(s.as_str()).map(Some),
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(v) => Some(v),
                Err(_) => return Response::error("qual2 contains interior null byte"),
            },
            None => None,
        };
        let qual2_ptrs: Option<Vec<*const c_char>> = qual2_cstrings.as_ref().map(|v| {
            v.iter()
                .map(|opt| match opt {
                    Some(c) => c.as_ptr(),
                    None => ptr::null(),
                })
                .collect()
        });

        unsafe {
            let mut bt2_input: Bt2Input = std::mem::zeroed();
            bt2_input_init(&mut bt2_input);
            bt2_input.names = name_ptrs.as_ptr();
            bt2_input.seqs = seq1_ptrs.as_ptr();
            bt2_input.quals = if all_quals_null {
                ptr::null()
            } else {
                qual1_ptrs.as_ptr()
            };
            bt2_input.n_reads = input_data.read_ids.len();

            if let Some(ref s2_ptrs) = seq2_ptrs {
                bt2_input.names2 = name_ptrs.as_ptr();
                bt2_input.seqs2 = s2_ptrs.as_ptr();
                bt2_input.quals2 = match qual2_ptrs.as_ref() {
                    Some(q2) => q2.as_ptr(),
                    None => ptr::null(),
                };
                bt2_input.n_reads2 = input_data.read_ids.len();
            }

            // Run alignment on the pre-loaded context
            let mut output: *mut Bt2AlignOutput = ptr::null_mut();
            let mut stats: Bt2AlignStats = std::mem::zeroed();

            let rc = bt2_align_run(self.ctx, &bt2_input, &mut output, &mut stats);

            if rc != BT2_OK {
                let cat = CStr::from_ptr(bt2_strerror(rc))
                    .to_string_lossy()
                    .into_owned();
                let detail = CStr::from_ptr(bt2_align_last_error(self.ctx))
                    .to_string_lossy()
                    .into_owned();
                return Response::error(format!("{cat}: {detail}"));
            }

            let batch = match soa_to_record_batch(&*output) {
                Ok(b) => b,
                Err(e) => {
                    bt2_align_output_free(output);
                    return Response::error(e);
                }
            };

            let shm_out = match crate::arrow_ipc::write_batch_to_output_shm(&batch, "alignments") {
                Ok(v) => v,
                Err(e) => {
                    bt2_align_output_free(output);
                    return Response::error(e);
                }
            };

            let result = serde_json::json!({
                "n_reads": stats.n_reads,
                "n_aligned": stats.n_aligned,
                "n_unaligned": stats.n_unaligned,
                "n_aligned_concordant": stats.n_aligned_concordant,
                "elapsed_ms": stats.elapsed_ms,
            });

            bt2_align_output_free(output);

            Response::ok(result, vec![shm_out])
        }
    }
}

/// SAM flag bit for an unmapped read (BAM_FUNMAP).
const SAM_FLAG_UNMAPPED: u16 = 0x4;

/// Sum the reference-consuming op lengths in a CIGAR string.
///
/// Reference-consuming ops per the SAM spec: M, D, N, =, X. The other ops
/// (I, S, H, P) consume the read but not the reference. Returns 0 for the
/// unmapped sentinel "*" or for any unparsable token (the latter is a
/// defensive choice: bowtie2 controls the CIGAR string and won't produce
/// junk, but a 0 fallback keeps the derived `stop_position` from going
/// negative if something upstream ever does).
fn cigar_reference_length(cigar: &str) -> i64 {
    if cigar == "*" || cigar.is_empty() {
        return 0;
    }
    let mut total: i64 = 0;
    let mut num: i64 = 0;
    for b in cigar.bytes() {
        match b {
            b'0'..=b'9' => {
                num = num * 10 + (b - b'0') as i64;
            }
            b'M' | b'D' | b'N' | b'=' | b'X' => {
                total += num;
                num = 0;
            }
            b'I' | b'S' | b'H' | b'P' => {
                num = 0;
            }
            _ => return 0,
        }
    }
    total
}

/// Convert bt2_align_output_t SOA arrays into an Arrow RecordBatch.
///
/// Skips seq and qual from the C output (caller already has the reads).
/// Handles nullable tags:
///   - `tag_as` / `tag_nm` / `tag_md` / `tag_yt`: NULL pointer = all-null column.
///   - `tag_xs` / `tag_ys`: per-record `INT32_MIN` sentinel → NULL (bowtie2's
///     "absent" convention for those score fields).
///   - `tag_xn` / `tag_xm` / `tag_xo` / `tag_xg`: bowtie2 returns 0 for unaligned
///     records; we re-map those to NULL using the SAM unmapped flag (0x4) so
///     downstream SQL `IS NULL` semantics match miint's prior HTSlib path.
///   - `tag_sa`: bowtie2 never emits SA. Always NULL; carried for schema parity.
unsafe fn soa_to_record_batch(output: &Bt2AlignOutput) -> Result<RecordBatch, String> {
    let n = output.n_records;

    // When n == 0 the C library returns NULL array pointers. from_raw_parts
    // requires non-null even for zero-length slices, so return empty batch.
    if n == 0 {
        let schema = Arc::new(output_schema());
        return RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(UInt16Array::from(Vec::<u16>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(UInt8Array::from(Vec::<u8>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(Int32Builder::new().finish()),
                Arc::new(Int32Builder::new().finish()),
                Arc::new(Int32Builder::new().finish()),
                Arc::new(Int32Builder::new().finish()),
                Arc::new(Int32Builder::new().finish()),
                Arc::new(Int32Builder::new().finish()),
                Arc::new(Int32Builder::new().finish()),
                Arc::new(Int32Builder::new().finish()),
                Arc::new(StringBuilder::new().finish()),
                Arc::new(StringBuilder::new().finish()),
                Arc::new(StringBuilder::new().finish()),
            ],
        )
        .map_err(|e| format!("Failed to create empty Arrow RecordBatch: {e}"));
    }

    let qname_ptrs = std::slice::from_raw_parts(output.qname, n);
    let flags = std::slice::from_raw_parts(output.flag, n);
    let rname_ptrs = std::slice::from_raw_parts(output.rname, n);
    let pos = std::slice::from_raw_parts(output.pos, n);
    let mapq = std::slice::from_raw_parts(output.mapq, n);
    let cigar_ptrs = std::slice::from_raw_parts(output.cigar, n);
    let rnext_ptrs = std::slice::from_raw_parts(output.rnext, n);
    let pnext = std::slice::from_raw_parts(output.pnext, n);
    let tlen = std::slice::from_raw_parts(output.tlen, n);

    // Mandatory string columns
    let read_ids: Vec<&str> = qname_ptrs
        .iter()
        .map(|&p| {
            if p.is_null() {
                ""
            } else {
                CStr::from_ptr(p).to_str().unwrap_or("")
            }
        })
        .collect();

    let references: Vec<&str> = rname_ptrs
        .iter()
        .map(|&p| {
            if p.is_null() {
                "*"
            } else {
                CStr::from_ptr(p).to_str().unwrap_or("*")
            }
        })
        .collect();

    let cigars: Vec<&str> = cigar_ptrs
        .iter()
        .map(|&p| {
            if p.is_null() {
                "*"
            } else {
                CStr::from_ptr(p).to_str().unwrap_or("*")
            }
        })
        .collect();

    let mate_refs: Vec<&str> = rnext_ptrs
        .iter()
        .map(|&p| {
            if p.is_null() {
                "*"
            } else {
                CStr::from_ptr(p).to_str().unwrap_or("*")
            }
        })
        .collect();

    // flags: i32 -> u16
    let flags_u16: Vec<u16> = flags.iter().map(|&f| f as u16).collect();

    // stop_position: 1-based inclusive end on the reference. 0 when unmapped
    // (pos == 0 OR the unmapped flag is set OR cigar == "*"). Derived once
    // here so downstream coverage filters don't have to re-parse CIGAR.
    let stop_positions: Vec<i64> = (0..n)
        .map(|i| {
            if pos[i] == 0 || (flags_u16[i] & SAM_FLAG_UNMAPPED) != 0 || cigars[i] == "*" {
                0
            } else {
                pos[i] + cigar_reference_length(cigars[i]) - 1
            }
        })
        .collect();

    // Nullable integer tags
    let tag_as_col = build_nullable_i32_tag(output.tag_as, n, None);
    let tag_xs_col = build_nullable_i32_tag(output.tag_xs, n, Some(i32::MIN));
    let tag_ys_col = build_nullable_i32_tag(output.tag_ys, n, Some(i32::MIN));
    let tag_xn_col = build_unmapped_aware_i32_tag(output.tag_xn, n, &flags_u16);
    let tag_xm_col = build_unmapped_aware_i32_tag(output.tag_xm, n, &flags_u16);
    let tag_xo_col = build_unmapped_aware_i32_tag(output.tag_xo, n, &flags_u16);
    let tag_xg_col = build_unmapped_aware_i32_tag(output.tag_xg, n, &flags_u16);
    let tag_nm_col = build_nullable_i32_tag(output.tag_nm, n, None);

    // Nullable string tags
    let tag_yt_col = build_nullable_string_tag(output.tag_yt, n);
    let tag_md_col = build_nullable_string_tag(output.tag_md, n);

    // tag_sa: always NULL — bowtie2 never emits SA. Stub for parity with
    // miint's prior 21-column HTSlib-based schema.
    let tag_sa_col = {
        let mut b = StringBuilder::with_capacity(n, 0);
        for _ in 0..n {
            b.append_null();
        }
        b.finish()
    };

    let schema = Arc::new(output_schema());
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(read_ids)),
            Arc::new(UInt16Array::from(flags_u16)),
            Arc::new(StringArray::from(references)),
            Arc::new(Int64Array::from(pos.to_vec())),
            Arc::new(Int64Array::from(stop_positions)),
            Arc::new(UInt8Array::from(mapq.to_vec())),
            Arc::new(StringArray::from(cigars)),
            Arc::new(StringArray::from(mate_refs)),
            Arc::new(Int64Array::from(pnext.to_vec())),
            Arc::new(Int64Array::from(tlen.to_vec())),
            Arc::new(tag_as_col),
            Arc::new(tag_xs_col),
            Arc::new(tag_ys_col),
            Arc::new(tag_xn_col),
            Arc::new(tag_xm_col),
            Arc::new(tag_xo_col),
            Arc::new(tag_xg_col),
            Arc::new(tag_nm_col),
            Arc::new(tag_yt_col),
            Arc::new(tag_md_col),
            Arc::new(tag_sa_col),
        ],
    )
    .map_err(|e| format!("Failed to create Arrow RecordBatch: {e}"))
}

/// Build a nullable Int32 Arrow array from a C int32_t* pointer.
/// If the pointer is null, the entire column is null.
/// If `per_record_sentinel` is Some(val), individual elements equal to val
/// are treated as null.
unsafe fn build_nullable_i32_tag(
    ptr: *const i32,
    n: usize,
    per_record_sentinel: Option<i32>,
) -> Int32Array {
    let mut builder = Int32Builder::with_capacity(n);
    if ptr.is_null() {
        for _ in 0..n {
            builder.append_null();
        }
    } else {
        let vals = std::slice::from_raw_parts(ptr, n);
        for &v in vals {
            if per_record_sentinel == Some(v) {
                builder.append_null();
            } else {
                builder.append_value(v);
            }
        }
    }
    builder.finish()
}

/// Build a nullable Int32 Arrow array from a C int32_t* pointer, mapping
/// values to NULL when the corresponding SAM flag has the unmapped bit
/// (0x4) set. Used for tags that bowtie2 stores as 0 on unaligned records
/// (`XN`/`XM`/`XO`/`XG`) — we re-NULL those to match HTSlib semantics so
/// downstream SQL `IS NULL` filters behave like miint's prior pipeline.
/// A NULL pointer still maps to an all-null column.
unsafe fn build_unmapped_aware_i32_tag(ptr: *const i32, n: usize, flags: &[u16]) -> Int32Array {
    let mut builder = Int32Builder::with_capacity(n);
    if ptr.is_null() {
        for _ in 0..n {
            builder.append_null();
        }
    } else {
        let vals = std::slice::from_raw_parts(ptr, n);
        for (i, &v) in vals.iter().enumerate() {
            if (flags[i] & SAM_FLAG_UNMAPPED) != 0 {
                builder.append_null();
            } else {
                builder.append_value(v);
            }
        }
    }
    builder.finish()
}

/// Build a nullable Utf8 Arrow array from a C char** pointer.
/// If the pointer is null, the entire column is null.
/// Individual null element pointers are treated as null values.
unsafe fn build_nullable_string_tag(ptr: *const *const c_char, n: usize) -> StringArray {
    let mut builder = StringBuilder::with_capacity(n, n * 8);
    if ptr.is_null() {
        for _ in 0..n {
            builder.append_null();
        }
    } else {
        let ptrs = std::slice::from_raw_parts(ptr, n);
        for &p in ptrs {
            if p.is_null() {
                builder.append_null();
            } else {
                builder.append_value(CStr::from_ptr(p).to_str().unwrap_or(""));
            }
        }
    }
    builder.finish()
}

// ---------------------------------------------------------------------------
// GplTool implementation
// ---------------------------------------------------------------------------

impl GplTool for Bowtie2AlignTool {
    fn name(&self) -> &str {
        "bowtie2-align"
    }

    fn version(&self) -> String {
        BT2_VERSION.to_string()
    }

    fn schema_version(&self) -> u32 {
        2
    }

    fn describe(&self) -> ToolDescription {
        ToolDescription {
            name: "bowtie2-align",
            version: self.version(),
            schema_version: self.schema_version(),
            describe_version: 2,
            description: "Short read alignment against a bowtie2 index",
            config_params: vec![
                // Core
                ConfigParam {
                    name: "index_path",
                    param_type: "string",
                    default: serde_json::json!(null),
                    description: "Filesystem path to bowtie2 index basename (required)",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "seed",
                    param_type: "integer",
                    default: serde_json::json!(0),
                    description: "Random seed for reproducibility",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "nthreads",
                    param_type: "integer",
                    default: serde_json::json!(1),
                    description: "Number of alignment threads",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "preset",
                    param_type: "string",
                    default: serde_json::json!("sensitive"),
                    description: "Alignment preset (sensitivity/speed tradeoff)",
                    allowed_values: vec![
                        "very-fast",
                        "fast",
                        "sensitive",
                        "very-sensitive",
                        "very-fast-local",
                        "fast-local",
                        "sensitive-local",
                        "very-sensitive-local",
                    ],
                },
                ConfigParam {
                    name: "local_align",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Local alignment mode; overrides preset's local flag when set explicitly",
                    allowed_values: vec![],
                },
                // Reporting
                ConfigParam {
                    name: "k",
                    param_type: "integer",
                    default: serde_json::json!(0),
                    description: "Report up to k alignments per read; 0 = default (1)",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "report_all",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Report all alignments",
                    allowed_values: vec![],
                },
                // Trimming
                ConfigParam {
                    name: "trim5",
                    param_type: "integer",
                    default: serde_json::json!(0),
                    description: "Trim N bases from 5' end of each read",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "trim3",
                    param_type: "integer",
                    default: serde_json::json!(0),
                    description: "Trim N bases from 3' end of each read",
                    allowed_values: vec![],
                },
                // Scoring
                ConfigParam {
                    name: "match_bonus",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Match bonus (--ma); null = mode-dependent default",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "mismatch_penalty",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Max mismatch penalty (--mp); null = mode-dependent default",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "n_penalty",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Ambiguous character penalty (--np); null = mode-dependent default",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "read_gap_open",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Read gap open penalty (--rdg arg1); null = default",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "read_gap_extend",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Read gap extend penalty (--rdg arg2); null = default",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "ref_gap_open",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Reference gap open penalty (--rfg arg1); null = default",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "ref_gap_extend",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Reference gap extend penalty (--rfg arg2); null = default",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "score_min",
                    param_type: "string",
                    default: serde_json::json!(null),
                    description: "Min acceptable alignment score function (--score-min), e.g. \"L,-0.6,-0.6\"",
                    allowed_values: vec![],
                },
                // Paired-end
                ConfigParam {
                    name: "min_insert",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Minimum fragment length for paired-end (--minins); null = default",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "max_insert",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Maximum fragment length for paired-end (--maxins); null = default",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "mate_orientation",
                    param_type: "string",
                    default: serde_json::json!("fr"),
                    description: "Expected mate pair orientation",
                    allowed_values: vec!["fr", "rf", "ff"],
                },
                ConfigParam {
                    name: "no_mixed",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Suppress unpaired alignments for paired reads",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "no_discordant",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Suppress discordant pair alignments",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "dovetail",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Allow dovetail mate overlap",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "no_contain",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Disallow one mate entirely containing the other",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "no_overlap",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Disallow any mate overlap",
                    allowed_values: vec![],
                },
                // Strand
                ConfigParam {
                    name: "nofw",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Do not align forward strand",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "norc",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Do not align reverse complement strand",
                    allowed_values: vec![],
                },
                // Effort
                ConfigParam {
                    name: "seed_mismatches",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Max seed mismatches, 0 or 1 (-N); null = preset-dependent",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "seed_length",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Seed substring length, 1-32 (-L); null = preset-dependent",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "max_dp_failures",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Max consecutive extend failures (-D); null = preset-dependent",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "max_seed_rounds",
                    param_type: "integer",
                    default: serde_json::json!(null),
                    description: "Max seed rounds (-R); null = preset-dependent",
                    allowed_values: vec![],
                },
                // SAM output
                ConfigParam {
                    name: "no_unal",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Suppress unaligned reads in output",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "xeq",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Use =/X in CIGAR instead of M",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "rg_id",
                    param_type: "string",
                    default: serde_json::json!(null),
                    description: "Read group ID (--rg-id)",
                    allowed_values: vec![],
                },
                // Other
                ConfigParam {
                    name: "ignore_quals",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Treat all quality scores as Phred 30",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "reorder",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Preserve input read order in output",
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
                    description: "Read identifier",
                },
                FieldDescription {
                    name: "sequence1",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Mate 1 (or unpaired) DNA sequence",
                },
                FieldDescription {
                    name: "sequence2",
                    arrow_type: "Utf8",
                    nullable: true,
                    description: "Mate 2 sequence; absence or all-null = single-end",
                },
                FieldDescription {
                    name: "qual1",
                    arrow_type: "Utf8",
                    nullable: true,
                    description: "Mate 1 Phred+33 quality string; null = FASTA (default quality)",
                },
                FieldDescription {
                    name: "qual2",
                    arrow_type: "Utf8",
                    nullable: true,
                    description: "Mate 2 quality string; null = FASTA",
                },
            ],
            output_schema: vec![
                FieldDescription {
                    name: "read_id",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Read name (QNAME)",
                },
                FieldDescription {
                    name: "flags",
                    arrow_type: "UInt16",
                    nullable: false,
                    description: "SAM flags",
                },
                FieldDescription {
                    name: "reference",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Reference sequence name (RNAME); * if unmapped",
                },
                FieldDescription {
                    name: "position",
                    arrow_type: "Int64",
                    nullable: false,
                    description: "1-based leftmost position (POS); 0 if unmapped",
                },
                FieldDescription {
                    name: "stop_position",
                    arrow_type: "Int64",
                    nullable: false,
                    description: "1-based inclusive end on reference; \
                                  derived from POS + reference-length(CIGAR) - 1; \
                                  0 if unmapped",
                },
                FieldDescription {
                    name: "mapq",
                    arrow_type: "UInt8",
                    nullable: false,
                    description: "Mapping quality (MAPQ)",
                },
                FieldDescription {
                    name: "cigar",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "CIGAR string; * if unmapped",
                },
                FieldDescription {
                    name: "mate_reference",
                    arrow_type: "Utf8",
                    nullable: false,
                    description: "Mate reference name (RNEXT); * if unavailable",
                },
                FieldDescription {
                    name: "mate_position",
                    arrow_type: "Int64",
                    nullable: false,
                    description: "Mate position (PNEXT); 0 if unavailable",
                },
                FieldDescription {
                    name: "template_length",
                    arrow_type: "Int64",
                    nullable: false,
                    description: "Template length (TLEN); 0 if unavailable",
                },
                FieldDescription {
                    name: "tag_as",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "AS:i alignment score; null if absent",
                },
                FieldDescription {
                    name: "tag_xs",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "XS:i second-best alignment score; null if absent",
                },
                FieldDescription {
                    name: "tag_ys",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "YS:i mate alignment score (paired-end); \
                                  null on single-end or when mate is unaligned",
                },
                FieldDescription {
                    name: "tag_xn",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "XN:i ambiguous bases in covered reference; \
                                  null if read is unmapped",
                },
                FieldDescription {
                    name: "tag_xm",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "XM:i mismatches; null if read is unmapped",
                },
                FieldDescription {
                    name: "tag_xo",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "XO:i gap opens; null if read is unmapped",
                },
                FieldDescription {
                    name: "tag_xg",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "XG:i gap extensions (incl. opens); \
                                  null if read is unmapped",
                },
                FieldDescription {
                    name: "tag_nm",
                    arrow_type: "Int32",
                    nullable: true,
                    description: "NM:i edit distance; null if absent",
                },
                FieldDescription {
                    name: "tag_yt",
                    arrow_type: "Utf8",
                    nullable: true,
                    description: "YT:Z pairing type (UU/CP/DP/UP); null if absent",
                },
                FieldDescription {
                    name: "tag_md",
                    arrow_type: "Utf8",
                    nullable: true,
                    description: "MD:Z mismatch string; null if absent",
                },
                FieldDescription {
                    name: "tag_sa",
                    arrow_type: "Utf8",
                    nullable: true,
                    description: "SA:Z chimeric alignment string; always null \
                                  (bowtie2 does not emit SA). Carried for parity \
                                  with HTSlib-derived schemas.",
                },
            ],
            response_metadata: vec![
                FieldDescription {
                    name: "n_reads",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Total reads processed",
                },
                FieldDescription {
                    name: "n_aligned",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Reads with at least one alignment",
                },
                FieldDescription {
                    name: "n_unaligned",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Reads with no alignment",
                },
                FieldDescription {
                    name: "n_aligned_concordant",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Concordant pairs (paired-end only)",
                },
                FieldDescription {
                    name: "elapsed_ms",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Wall-clock time in milliseconds",
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
        let verbose = config
            .get("verbose")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Read input from shared memory
        let input_data = match Self::read_input(shm_input, shm_input_size) {
            Ok(data) => data,
            Err(e) => return Response::error(e),
        };

        if input_data.read_ids.is_empty() {
            return Response::error("At least 1 read required");
        }

        // Build config
        let (bt2_config, _config_cstrings) = match build_config(config, verbose) {
            Ok(v) => v,
            Err(e) => return Response::error(e),
        };

        // Build C input arrays
        let name_cstrings: Vec<CString> = match input_data
            .read_ids
            .iter()
            .map(|s| CString::new(s.as_str()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(_) => return Response::error("read_id contains interior null byte"),
        };
        let name_ptrs: Vec<*const c_char> = name_cstrings.iter().map(|s| s.as_ptr()).collect();

        let seq1_cstrings: Vec<CString> = match input_data
            .seqs1
            .iter()
            .map(|s| CString::new(s.as_str()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(_) => return Response::error("sequence1 contains interior null byte"),
        };
        let seq1_ptrs: Vec<*const c_char> = seq1_cstrings.iter().map(|s| s.as_ptr()).collect();

        // qual1: build CStrings for non-null values, null pointer for None
        let qual1_cstrings: Vec<Option<CString>> = match input_data
            .quals1
            .iter()
            .map(|opt| match opt {
                Some(s) => CString::new(s.as_str()).map(Some),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(_) => return Response::error("qual1 contains interior null byte"),
        };
        let qual1_ptrs: Vec<*const c_char> = qual1_cstrings
            .iter()
            .map(|opt| match opt {
                Some(c) => c.as_ptr(),
                None => ptr::null(),
            })
            .collect();
        // If all quals are null (FASTA), pass null array pointer
        let all_quals_null = qual1_cstrings.iter().all(|c| c.is_none());

        // Paired-end mate 2 arrays
        let seq2_cstrings: Option<Vec<CString>> = input_data
            .seqs2
            .as_ref()
            .map(|seqs| {
                seqs.iter()
                    .map(|s| CString::new(s.as_str()))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .unwrap_or(None);
        if input_data.seqs2.is_some() && seq2_cstrings.is_none() {
            return Response::error("sequence2 contains interior null byte");
        }
        let seq2_ptrs: Option<Vec<*const c_char>> = seq2_cstrings
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_ptr()).collect());

        let qual2_cstrings: Option<Vec<Option<CString>>> = input_data
            .quals2
            .as_ref()
            .map(|quals| {
                quals
                    .iter()
                    .map(|opt| match opt {
                        Some(s) => CString::new(s.as_str()).map(Some),
                        None => Ok(None),
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
            .unwrap_or(None);
        if input_data.quals2.is_some() && qual2_cstrings.is_none() {
            return Response::error("qual2 contains interior null byte");
        }
        let qual2_ptrs: Option<Vec<*const c_char>> = qual2_cstrings.as_ref().map(|v| {
            v.iter()
                .map(|opt| match opt {
                    Some(c) => c.as_ptr(),
                    None => ptr::null(),
                })
                .collect()
        });

        unsafe {
            // Initialize bt2_input_t
            let mut bt2_input: Bt2Input = std::mem::zeroed();
            bt2_input_init(&mut bt2_input);
            bt2_input.names = name_ptrs.as_ptr();
            bt2_input.seqs = seq1_ptrs.as_ptr();
            bt2_input.quals = if all_quals_null {
                ptr::null()
            } else {
                qual1_ptrs.as_ptr()
            };
            bt2_input.n_reads = input_data.read_ids.len();

            if let Some(ref s2_ptrs) = seq2_ptrs {
                bt2_input.names2 = name_ptrs.as_ptr(); // same names for mate 2
                bt2_input.seqs2 = s2_ptrs.as_ptr();
                bt2_input.quals2 = match qual2_ptrs.as_ref() {
                    Some(q2) => q2.as_ptr(),
                    None => ptr::null(),
                };
                bt2_input.n_reads2 = input_data.read_ids.len();
            }

            // Create context
            let mut error_code: c_int = 0;
            let ctx = bt2_align_create(&bt2_config, &mut error_code);
            if ctx.is_null() {
                let cat = CStr::from_ptr(bt2_strerror(error_code))
                    .to_string_lossy()
                    .into_owned();
                return Response::error(format!("Failed to create bowtie2 context: {cat}"));
            }

            // Run alignment
            let mut output: *mut Bt2AlignOutput = ptr::null_mut();
            let mut stats: Bt2AlignStats = std::mem::zeroed();

            let rc = bt2_align_run(ctx, &bt2_input, &mut output, &mut stats);

            if rc != BT2_OK {
                let cat = CStr::from_ptr(bt2_strerror(rc))
                    .to_string_lossy()
                    .into_owned();
                let detail = CStr::from_ptr(bt2_align_last_error(ctx))
                    .to_string_lossy()
                    .into_owned();
                bt2_align_destroy(ctx);
                return Response::error(format!("{cat}: {detail}"));
            }

            // Convert output to Arrow
            let batch = match soa_to_record_batch(&*output) {
                Ok(b) => b,
                Err(e) => {
                    bt2_align_output_free(output);
                    bt2_align_destroy(ctx);
                    return Response::error(e);
                }
            };

            let shm_out = match crate::arrow_ipc::write_batch_to_output_shm(&batch, "alignments") {
                Ok(v) => v,
                Err(e) => {
                    bt2_align_output_free(output);
                    bt2_align_destroy(ctx);
                    return Response::error(e);
                }
            };

            // Build response metadata
            let result = serde_json::json!({
                "n_reads": stats.n_reads,
                "n_aligned": stats.n_aligned,
                "n_unaligned": stats.n_unaligned,
                "n_aligned_concordant": stats.n_aligned_concordant,
                "elapsed_ms": stats.elapsed_ms,
            });

            // Cleanup
            bt2_align_output_free(output);
            bt2_align_destroy(ctx);

            Response::ok(result, vec![shm_out])
        }
    }

    fn create_streaming_context(
        &self,
        config: &serde_json::Value,
    ) -> Result<Option<Box<dyn StreamingContext>>, String> {
        let verbose = config
            .get("verbose")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let (bt2_config, config_cstrings) = build_config(config, verbose)?;

        unsafe {
            let mut error_code: c_int = 0;
            let ctx = bt2_align_create(&bt2_config, &mut error_code);
            if ctx.is_null() {
                let cat = CStr::from_ptr(bt2_strerror(error_code))
                    .to_string_lossy()
                    .into_owned();
                return Err(format!("Failed to create bowtie2 context: {cat}"));
            }

            Ok(Some(Box::new(Bowtie2StreamingContext {
                ctx,
                _config_cstrings: config_cstrings,
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
    use crate::tools::bowtie2_build::{
        bt2_build_config_init, bt2_build_create, bt2_build_destroy, bt2_build_run, Bt2BuildConfig,
        Bt2BuildStats,
    };

    // -- Test input helpers --

    fn make_single_end_input(
        read_ids: &[&str],
        sequences: &[&str],
        quals: &[Option<&str>],
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("read_id", DataType::Utf8, false),
            Field::new("sequence1", DataType::Utf8, false),
            Field::new("qual1", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(read_ids.to_vec())),
                Arc::new(StringArray::from(sequences.to_vec())),
                Arc::new(StringArray::from(quals.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_paired_end_input(
        read_ids: &[&str],
        sequences1: &[&str],
        sequences2: &[&str],
        quals1: &[Option<&str>],
        quals2: &[Option<&str>],
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("read_id", DataType::Utf8, false),
            Field::new("sequence1", DataType::Utf8, false),
            Field::new("sequence2", DataType::Utf8, true),
            Field::new("qual1", DataType::Utf8, true),
            Field::new("qual2", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(read_ids.to_vec())),
                Arc::new(StringArray::from(sequences1.to_vec())),
                Arc::new(StringArray::from(sequences2.to_vec())),
                Arc::new(StringArray::from(quals1.to_vec())),
                Arc::new(StringArray::from(quals2.to_vec())),
            ],
        )
        .unwrap()
    }

    // -- Test fixture: build a tiny bowtie2 index --

    /// Synthetic 200bp reference for test alignments.
    const TEST_REF: &str = "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT\
TGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCATGCA\
AAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCCGGGTTTAAACCG\
GCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAG";

    fn build_test_index() -> (tempfile::TempDir, String) {
        build_test_index_with_ref(TEST_REF)
    }

    fn build_test_index_with_ref(ref_seq: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        let ref_path = dir.path().join("ref.fa");
        std::fs::write(&ref_path, format!(">ref1\n{ref_seq}\n"))
            .expect("Failed to write test reference");

        let index_prefix = dir.path().join("idx").to_string_lossy().into_owned();

        let ref_path_c = CString::new(ref_path.to_str().unwrap()).unwrap();
        let output_base_c = CString::new(index_prefix.as_str()).unwrap();
        let ref_ptrs: [*const c_char; 1] = [ref_path_c.as_ptr()];

        unsafe {
            let mut config: Bt2BuildConfig = std::mem::zeroed();
            bt2_build_config_init(&mut config);
            config.ref_paths = ref_ptrs.as_ptr();
            config.n_ref_paths = 1;
            config.output_base = output_base_c.as_ptr();
            config.quiet = 1;

            let mut error_code: c_int = 0;
            let ctx = bt2_build_create(&config, &mut error_code);
            assert!(
                !ctx.is_null(),
                "bt2_build_create failed with error code {error_code}"
            );

            let mut stats: Bt2BuildStats = std::mem::zeroed();
            let rc = bt2_build_run(ctx, &mut stats);
            assert_eq!(rc, BT2_OK, "bt2_build_run failed");

            bt2_build_destroy(ctx);
        }

        (dir, index_prefix)
    }

    // ---------------------------------------------------------------
    // ABI size checks
    // ---------------------------------------------------------------

    #[test]
    fn test_config_struct_abi_size() {
        let mut config: Bt2AlignConfig = unsafe { std::mem::zeroed() };
        unsafe { bt2_align_config_init(&mut config) };
        assert_eq!(
            config.struct_size,
            std::mem::size_of::<Bt2AlignConfig>(),
            "ABI mismatch: Rust Bt2AlignConfig ({} bytes) vs C bt2_align_config_t ({} bytes). \
             Check field types and padding against ext/bowtie2/bt2_api.h.",
            std::mem::size_of::<Bt2AlignConfig>(),
            config.struct_size,
        );
    }

    #[test]
    fn test_input_struct_abi_size() {
        let mut input: Bt2Input = unsafe { std::mem::zeroed() };
        unsafe { bt2_input_init(&mut input) };
        assert_eq!(
            input.struct_size,
            std::mem::size_of::<Bt2Input>(),
            "ABI mismatch: Rust Bt2Input ({} bytes) vs C bt2_input_t ({} bytes). \
             Check field types and padding against ext/bowtie2/bt2_api.h.",
            std::mem::size_of::<Bt2Input>(),
            input.struct_size,
        );
    }

    #[test]
    fn test_output_struct_abi_size() {
        let (_dir, index_prefix) = build_test_index();

        let read_seq = &TEST_REF[10..37];
        let read_qual = &"I".repeat(27);

        let input_name = unique_shm_name("bt2-abi-out");
        let batch = make_single_end_input(&["r1"], &[read_seq], &[Some(read_qual)]);
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({"index_path": index_prefix, "seed": 42});
        let response = tool.execute(&config, &input_name, shm_holder_size);
        assert!(response.success, "Alignment failed: {:?}", response.error);

        // The struct_size check happens inside execute() implicitly via
        // soa_to_record_batch reading the output. But we also verify it
        // directly by running the C API and checking the returned struct.
        let index_path_c = CString::new(index_prefix.as_str()).unwrap();
        let name_c = CString::new("r1").unwrap();
        let seq_c = CString::new(read_seq).unwrap();
        let qual_c = CString::new(read_qual.as_str()).unwrap();
        let name_ptrs = [name_c.as_ptr()];
        let seq_ptrs = [seq_c.as_ptr()];
        let qual_ptrs = [qual_c.as_ptr()];

        unsafe {
            let mut cfg: Bt2AlignConfig = std::mem::zeroed();
            bt2_align_config_init(&mut cfg);
            cfg.index_path = index_path_c.as_ptr();
            cfg.quiet = 1;

            let mut err: c_int = 0;
            let ctx = bt2_align_create(&cfg, &mut err);
            assert!(!ctx.is_null());

            let mut input: Bt2Input = std::mem::zeroed();
            bt2_input_init(&mut input);
            input.names = name_ptrs.as_ptr();
            input.seqs = seq_ptrs.as_ptr();
            input.quals = qual_ptrs.as_ptr();
            input.n_reads = 1;

            let mut output: *mut Bt2AlignOutput = ptr::null_mut();
            let rc = bt2_align_run(ctx, &input, &mut output, ptr::null_mut());
            assert_eq!(rc, BT2_OK);
            assert!(!output.is_null());

            assert_eq!(
                (*output).struct_size,
                std::mem::size_of::<Bt2AlignOutput>(),
                "ABI mismatch: Rust Bt2AlignOutput ({} bytes) vs C bt2_align_output_t ({} bytes). \
                 Check field types and padding against ext/bowtie2/bt2_api.h.",
                std::mem::size_of::<Bt2AlignOutput>(),
                (*output).struct_size,
            );

            bt2_align_output_free(output);
            bt2_align_destroy(ctx);
        }

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    // ---------------------------------------------------------------
    // Output schema
    // ---------------------------------------------------------------

    #[test]
    fn test_output_schema_field_count_and_types() {
        let schema = output_schema();
        assert_eq!(schema.fields().len(), 21);

        assert_eq!(
            schema.field_with_name("read_id").unwrap().data_type(),
            &DataType::Utf8
        );
        assert!(!schema.field_with_name("read_id").unwrap().is_nullable());

        assert_eq!(
            schema.field_with_name("flags").unwrap().data_type(),
            &DataType::UInt16
        );
        assert_eq!(
            schema.field_with_name("mapq").unwrap().data_type(),
            &DataType::UInt8
        );
        assert_eq!(
            schema.field_with_name("position").unwrap().data_type(),
            &DataType::Int64
        );

        // stop_position: non-null Int64, sibling of `position`.
        let stop = schema.field_with_name("stop_position").unwrap();
        assert_eq!(stop.data_type(), &DataType::Int64);
        assert!(!stop.is_nullable());

        assert_eq!(
            schema.field_with_name("tag_as").unwrap().data_type(),
            &DataType::Int32
        );
        assert!(schema.field_with_name("tag_as").unwrap().is_nullable());
        assert!(schema.field_with_name("tag_yt").unwrap().is_nullable());

        // v0.3 tag columns — all Int32 nullable.
        for name in ["tag_ys", "tag_xn", "tag_xm", "tag_xo", "tag_xg"] {
            let f = schema.field_with_name(name).unwrap();
            assert_eq!(f.data_type(), &DataType::Int32, "{name} should be Int32");
            assert!(f.is_nullable(), "{name} should be nullable");
        }

        // tag_sa: always-null Utf8 stub for schema parity.
        let sa = schema.field_with_name("tag_sa").unwrap();
        assert_eq!(sa.data_type(), &DataType::Utf8);
        assert!(sa.is_nullable());

        // seq and qual are NOT in the output schema
        assert!(schema.field_with_name("seq").is_err());
        assert!(schema.field_with_name("qual").is_err());
    }

    #[test]
    fn test_cigar_reference_length() {
        // "*" sentinel
        assert_eq!(cigar_reference_length("*"), 0);
        assert_eq!(cigar_reference_length(""), 0);
        // Pure match: 50M consumes 50 ref bases
        assert_eq!(cigar_reference_length("50M"), 50);
        // Insert is read-only, doesn't advance ref
        assert_eq!(cigar_reference_length("10M5I10M"), 20);
        // Deletion consumes ref
        assert_eq!(cigar_reference_length("10M5D10M"), 25);
        // Soft/hard clip don't consume ref
        assert_eq!(cigar_reference_length("5S40M5H"), 40);
        // Equal / mismatch are ref-consuming
        assert_eq!(cigar_reference_length("10=5X10="), 25);
        // Skip (N) is ref-consuming (intron)
        assert_eq!(cigar_reference_length("10M100N10M"), 120);
        // Unknown op falls back to 0 (defensive)
        assert_eq!(cigar_reference_length("10M5Z10M"), 0);
    }

    // ---------------------------------------------------------------
    // Config mapping
    // ---------------------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let config_json = serde_json::json!({"index_path": "/tmp/test"});
        let (bt2, _cstrings) = build_config(&config_json, false).unwrap();
        assert_eq!(bt2.nthreads, 1);
        assert_eq!(bt2.seed, 0);
        assert_eq!(bt2.preset, BT2_PRESET_SENSITIVE);
        assert_eq!(bt2.local_align, 0);
        assert_eq!(bt2.match_bonus, -1);
        assert_eq!(bt2.mismatch_penalty, -1);
        assert_eq!(bt2.quiet, 1);
    }

    #[test]
    fn test_config_custom_values() {
        let config_json = serde_json::json!({
            "index_path": "/tmp/test",
            "nthreads": 4,
            "preset": "very-sensitive-local",
            "k": 5,
            "trim5": 10,
            "seed": 42,
        });
        let (bt2, _cstrings) = build_config(&config_json, false).unwrap();
        assert_eq!(bt2.nthreads, 4);
        assert_eq!(bt2.preset, BT2_PRESET_VERY_SENSITIVE);
        assert_eq!(bt2.local_align, 1);
        assert_eq!(bt2.k, 5);
        assert_eq!(bt2.trim5, 10);
        assert_eq!(bt2.seed, 42);
    }

    #[test]
    fn test_config_missing_index_path() {
        let config_json = serde_json::json!({});
        let result = build_config(&config_json, false);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.contains("index_path"),
            "Expected index_path error, got: {err}"
        );
    }

    // ---------------------------------------------------------------
    // Test fixture
    // ---------------------------------------------------------------

    #[test]
    fn test_build_test_index_creates_bt2_files() {
        let (_dir, prefix) = build_test_index();
        for suffix in &[
            ".1.bt2",
            ".2.bt2",
            ".3.bt2",
            ".4.bt2",
            ".rev.1.bt2",
            ".rev.2.bt2",
        ] {
            let path = format!("{prefix}{suffix}");
            assert!(
                std::path::Path::new(&path).exists(),
                "Missing index file: {path}"
            );
        }
    }

    // ---------------------------------------------------------------
    // Error paths
    // ---------------------------------------------------------------

    #[test]
    fn test_bowtie2_align_bad_input_shm() {
        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({"index_path": "/nonexistent/index"});
        let response = tool.execute(&config, "/nonexistent-shm-name", 0);
        assert!(!response.success);
        assert!(response
            .error
            .as_ref()
            .unwrap()
            .contains("Failed to open shm"));
    }

    #[test]
    fn test_bowtie2_align_missing_index_path() {
        let input_name = unique_shm_name("bt2-err-idx");
        let batch = make_single_end_input(&["r1"], &["ACGT"], &[Some("IIII")]);
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let tool = Bowtie2AlignTool;
        let response = tool.execute(&serde_json::json!({}), &input_name, shm_holder_size);
        assert!(!response.success);
        assert!(
            response.error.as_ref().unwrap().contains("index_path"),
            "Expected error about index_path, got: {:?}",
            response.error
        );
    }

    #[test]
    fn test_bowtie2_align_invalid_index_path() {
        let input_name = unique_shm_name("bt2-err-bad");
        let batch = make_single_end_input(&["r1"], &["ACGT"], &[Some("IIII")]);
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({"index_path": "/nonexistent/path/idx"});
        let response = tool.execute(&config, &input_name, shm_holder_size);
        assert!(!response.success);
    }

    #[test]
    fn test_bowtie2_align_empty_input() {
        let (_dir, index_prefix) = build_test_index();
        let input_name = unique_shm_name("bt2-empty");
        let batch = make_single_end_input(&[], &[], &[]);
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({"index_path": index_prefix});
        let response = tool.execute(&config, &input_name, shm_holder_size);
        assert!(!response.success);
        assert!(response.error.as_ref().unwrap().contains("At least 1"));
    }

    #[test]
    fn test_bowtie2_align_no_unal_zero_records() {
        let (_dir, index_prefix) = build_test_index();

        // A read that won't align to the reference
        let garbage_read = "NNNNNNNNNNNNNNNNNNNNNNNNNNN";
        let garbage_qual = &"!".repeat(27);

        let input_name = unique_shm_name("bt2-noul");
        let batch = make_single_end_input(&["garbage"], &[garbage_read], &[Some(garbage_qual)]);
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({
            "index_path": index_prefix,
            "no_unal": true,
            "seed": 42,
        });
        let response = tool.execute(&config, &input_name, shm_holder_size);
        assert!(response.success, "Failed: {:?}", response.error);
        assert_eq!(response.shm_outputs.len(), 1);

        // Verify the zero-records path works (soa_to_record_batch handles
        // n_records==0 without UB from null pointer dereference).
        let batches =
            read_arrow_from_shm(&response.shm_outputs[0].name, response.shm_outputs[0].size);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 21);

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    // ---------------------------------------------------------------
    // Single-end FASTQ roundtrip
    // ---------------------------------------------------------------

    #[test]
    fn test_bowtie2_align_single_end_fastq_roundtrip() {
        let (_dir, index_prefix) = build_test_index();

        let read_seq = &TEST_REF[10..37]; // 27bp substring
        let read_qual = &"I".repeat(27);

        let input_name = unique_shm_name("bt2-se");
        let batch = make_single_end_input(&["read1"], &[read_seq], &[Some(read_qual)]);
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({
            "index_path": index_prefix,
            "nthreads": 1,
            "seed": 42,
        });
        let response = tool.execute(&config, &input_name, shm_holder_size);

        assert!(response.success, "Failed: {:?}", response.error);
        assert_eq!(response.shm_outputs.len(), 1);
        assert_eq!(response.shm_outputs[0].label, "alignments");
        assert!(response.shm_outputs[0].name.starts_with("/gb-"));
        assert!(response.shm_outputs[0].size > 0);

        let result = response.result.unwrap();
        assert_eq!(result["n_reads"], 1);
        assert_eq!(result["n_aligned"], 1);
        assert_eq!(result["n_unaligned"], 0);

        let batches =
            read_arrow_from_shm(&response.shm_outputs[0].name, response.shm_outputs[0].size);
        assert_eq!(batches.len(), 1);
        let out = &batches[0];
        assert_eq!(out.num_columns(), 21);
        assert!(out.num_rows() >= 1);

        let read_id_col = out
            .column_by_name("read_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(read_id_col.value(0), "read1");

        let flags_col = out
            .column_by_name("flags")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert!(
            flags_col.value(0) & 0x4 == 0,
            "Expected mapped (not unmapped)"
        );

        let ref_col = out
            .column_by_name("reference")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ref_col.value(0), "ref1");

        let pos_col = out
            .column_by_name("position")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(pos_col.value(0) > 0, "Expected positive position");

        let cigar_col = out
            .column_by_name("cigar")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!cigar_col.value(0).is_empty(), "Expected non-empty CIGAR");

        // stop_position is the 1-based inclusive end on the reference. For
        // this perfect 27bp match alignment, that's position + 27 - 1.
        let stop_col = out
            .column_by_name("stop_position")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            stop_col.value(0),
            pos_col.value(0) + cigar_reference_length(cigar_col.value(0)) - 1,
            "stop_position must equal position + ref-len(CIGAR) - 1"
        );

        // Aligned records: the new int32 tags are emitted (not NULL), even
        // when the value is 0 (no mismatches / no gaps). Single-end: tag_ys
        // must be NULL (no mate).
        for n in ["tag_xn", "tag_xm", "tag_xo", "tag_xg"] {
            let c = out
                .column_by_name(n)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert!(
                c.is_valid(0),
                "{n} must be set (not NULL) on an aligned record"
            );
        }
        let ys_col = out
            .column_by_name("tag_ys")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(
            ys_col.is_null(0),
            "tag_ys must be NULL on single-end alignments"
        );

        // tag_sa: always NULL — bowtie2 never emits SA. Schema-parity stub.
        let sa_col = out
            .column_by_name("tag_sa")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(sa_col.is_null(0), "tag_sa must always be NULL");

        // seq and qual are NOT in the output
        assert!(out.column_by_name("seq").is_none());
        assert!(out.column_by_name("qual").is_none());

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    // ---------------------------------------------------------------
    // Single-end FASTA roundtrip (no quality scores)
    // ---------------------------------------------------------------

    #[test]
    fn test_bowtie2_align_single_end_fasta_roundtrip() {
        let (_dir, index_prefix) = build_test_index();

        let read_seq = &TEST_REF[10..37];

        let input_name = unique_shm_name("bt2-fa");
        let batch = make_single_end_input(&["read1"], &[read_seq], &[None]);
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({
            "index_path": index_prefix,
            "nthreads": 1,
            "ignore_quals": true,
        });
        let response = tool.execute(&config, &input_name, shm_holder_size);
        assert!(response.success, "Failed: {:?}", response.error);

        let batches =
            read_arrow_from_shm(&response.shm_outputs[0].name, response.shm_outputs[0].size);
        assert!(batches[0].num_rows() >= 1);

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    // ---------------------------------------------------------------
    // Paired-end roundtrip
    // ---------------------------------------------------------------

    #[test]
    fn test_bowtie2_align_paired_end_roundtrip() {
        let (_dir, index_prefix) = build_test_index();

        // Forward read from start, reverse read from further in the reference
        let read1 = &TEST_REF[0..30];
        let read2 = &TEST_REF[100..130];
        let qual = &"I".repeat(30);

        let input_name = unique_shm_name("bt2-pe");
        let batch = make_paired_end_input(
            &["pair1"],
            &[read1],
            &[read2],
            &[Some(qual.as_str())],
            &[Some(qual.as_str())],
        );
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({
            "index_path": index_prefix,
            "nthreads": 1,
            "seed": 42,
        });
        let response = tool.execute(&config, &input_name, shm_holder_size);
        assert!(response.success, "Failed: {:?}", response.error);

        let result = response.result.unwrap();
        // bowtie2 counts n_reads as the number of read pairs for paired-end
        assert!(result["n_reads"].as_i64().unwrap() >= 1);
        let n_concordant = result["n_aligned_concordant"].as_i64().unwrap();

        let batches =
            read_arrow_from_shm(&response.shm_outputs[0].name, response.shm_outputs[0].size);
        let out = &batches[0];
        // Paired-end produces 2 records (one per mate)
        assert!(out.num_rows() >= 1);

        let flags_col = out
            .column_by_name("flags")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        let has_paired_flag = (0..out.num_rows()).any(|i| flags_col.value(i) & 0x1 != 0);
        assert!(
            has_paired_flag,
            "Expected paired flag in at least one record"
        );

        // YS:i (opposite mate's score) is set on a record iff this record
        // was emitted as part of a paired alignment where the mate also has
        // a valid score (bowtie2's `summ.paired() && rs->oscore().valid()`
        // condition; see ext/bowtie2/aln_sink_columnar.cpp). The TEST_REF
        // fixture is intentionally repetitive, so concordant alignment is
        // not guaranteed across bowtie2 versions — only check YS when the
        // run actually produced a concordant pair.
        if n_concordant >= 1 {
            let ys_col = out
                .column_by_name("tag_ys")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let aligned_paired = (0..out.num_rows()).any(|i| {
                (flags_col.value(i) & 0x4) == 0
                    && (flags_col.value(i) & 0x1) != 0
                    && ys_col.is_valid(i)
            });
            assert!(
                aligned_paired,
                "n_aligned_concordant >= 1 but no record carries a tag_ys value"
            );
        }

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    // ---------------------------------------------------------------
    // Paired-end YS positive case (non-repetitive synthetic ref so the
    // pair aligns concordantly; bowtie2 then sets YS:i to the opposite
    // mate's AS:i score and the columnar path must carry it through).
    // ---------------------------------------------------------------

    #[test]
    fn test_bowtie2_align_paired_end_tag_ys_set() {
        // 240bp non-repetitive synthetic reference. Picked to be unique
        // enough at 30bp granularity that bowtie2 reports a single
        // concordant pair under default parameters.
        const UNIQUE_REF: &str = "GATTACAGTACCAGCTAGGTCAGCGAATCGTACGCAGTACCATTG\
ACGTACGGTACGAATCAGCTGAGCTAGCATCGATCGTAGCATGAC\
ATGCATGCAATGCATTGCATGCAGGTACGATCAGACGTACGCAGT\
ACCAGTACAGCTAGCATCGATCAGCTAGCGATTGCATCAGCATGC\
AGCAGCTAGCATCGATCAGCTAGCGATTGAGCATGCATGAGGCAT\
CGATCATGCATGCAGGTACGATCAGACAGGCTAGCATCGATCAGC";

        let (_dir, index_prefix) = build_test_index_with_ref(UNIQUE_REF);

        fn reverse_complement(s: &str) -> String {
            s.chars()
                .rev()
                .map(|c| match c {
                    'A' => 'T',
                    'T' => 'A',
                    'C' => 'G',
                    'G' => 'C',
                    'N' => 'N',
                    other => other,
                })
                .collect()
        }

        // FR orientation: mate1 forward, mate2 reverse-complement of a
        // downstream region. With ~90bp between them, this is a textbook
        // concordant pair under bowtie2's default insert range.
        let read1 = UNIQUE_REF[10..40].to_string();
        let read2 = reverse_complement(&UNIQUE_REF[100..130]);
        let qual = "I".repeat(30);

        let input_name = unique_shm_name("bt2-pe-ys");
        let batch = make_paired_end_input(
            &["pair1"],
            &[read1.as_str()],
            &[read2.as_str()],
            &[Some(qual.as_str())],
            &[Some(qual.as_str())],
        );
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({
            "index_path": index_prefix,
            "nthreads": 1,
            "seed": 42,
        });
        let response = tool.execute(&config, &input_name, shm_holder_size);
        assert!(response.success, "Failed: {:?}", response.error);

        let result = response.result.unwrap();
        assert!(
            result["n_aligned_concordant"].as_i64().unwrap() >= 1,
            "non-repetitive PE fixture must produce a concordant pair, got result={result}"
        );

        let batches =
            read_arrow_from_shm(&response.shm_outputs[0].name, response.shm_outputs[0].size);
        let out = &batches[0];

        let flags_col = out
            .column_by_name("flags")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        let ys_col = out
            .column_by_name("tag_ys")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let as_col = out
            .column_by_name("tag_as")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Cross-mate parity: for a properly-paired alignment, YS on mate A
        // equals AS on mate B. Same invariant bowtie2's test_align_tag_fields.c
        // verifies on the lambda fixture — just doing it once here so the
        // pointer plumbing is end-to-end checked from Rust.
        let mut checked_pairs = 0;
        for i in 0..out.num_rows() {
            if (flags_col.value(i) & 0x4) != 0 {
                continue; // unmapped
            }
            if (flags_col.value(i) & 0x2) == 0 {
                continue; // not properly paired
            }
            assert!(
                ys_col.is_valid(i),
                "tag_ys must be set on a properly-paired mapped record (row {i})"
            );
            // Find the sibling mate (same read_id, different mate bit).
            let sibling = (0..out.num_rows()).find(|&j| {
                j != i && (flags_col.value(j) & 0x4) == 0 && (flags_col.value(j) & 0x2) != 0
            });
            if let Some(j) = sibling {
                assert!(as_col.is_valid(j), "sibling AS must be set when YS was set");
                assert_eq!(
                    ys_col.value(i),
                    as_col.value(j),
                    "YS on row {i} must equal AS on sibling row {j}"
                );
                checked_pairs += 1;
            }
        }
        assert!(
            checked_pairs >= 1,
            "Expected to check at least one mate pair for YS==AS_sibling"
        );

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    // ---------------------------------------------------------------
    // Unaligned record: tag_xn/xm/xo/xg/ys must be NULL, stop_position == 0
    // ---------------------------------------------------------------

    #[test]
    fn test_bowtie2_align_unaligned_nulls() {
        let (_dir, index_prefix) = build_test_index();

        // A garbage read that won't align. Without no_unal, bowtie2 emits
        // one unmapped SAM record per read; that's what we want to inspect.
        let garbage_read = "TTTTTTTTTTTTTTTTTTTTTTTTTTT";
        let garbage_qual = &"!".repeat(27);

        let input_name = unique_shm_name("bt2-una");
        let batch = make_single_end_input(&["garbage"], &[garbage_read], &[Some(garbage_qual)]);
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({
            "index_path": index_prefix,
            "seed": 42,
        });
        let response = tool.execute(&config, &input_name, shm_holder_size);
        assert!(response.success, "Failed: {:?}", response.error);

        let batches =
            read_arrow_from_shm(&response.shm_outputs[0].name, response.shm_outputs[0].size);
        let out = &batches[0];
        assert_eq!(out.num_rows(), 1, "Expected 1 unmapped record");

        let flags_col = out
            .column_by_name("flags")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert!(
            (flags_col.value(0) & 0x4) != 0,
            "Unaligned record must have SAM unmapped flag set"
        );

        // stop_position must be 0 (no reference span)
        let stop_col = out
            .column_by_name("stop_position")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(stop_col.value(0), 0, "stop_position must be 0 on unmapped");

        // All five new int32 tags must be NULL on the unmapped row.
        for n in ["tag_ys", "tag_xn", "tag_xm", "tag_xo", "tag_xg"] {
            let c = out
                .column_by_name(n)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert!(
                c.is_null(0),
                "{n} must be NULL on an unmapped record (was: {:?})",
                c.value(0)
            );
        }

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    // ---------------------------------------------------------------
    // describe() completeness
    // ---------------------------------------------------------------

    #[test]
    fn test_describe_completeness() {
        let tool = Bowtie2AlignTool;
        let desc = tool.describe();
        assert_eq!(desc.name, "bowtie2-align");
        assert_eq!(desc.schema_version, 2);

        assert_eq!(desc.input_schema.len(), 5);
        assert_eq!(desc.output_schema.len(), 21);

        // Spot-check the v0.3-derived columns are advertised.
        let output_names: Vec<&str> = desc.output_schema.iter().map(|f| f.name).collect();
        for n in [
            "stop_position",
            "tag_ys",
            "tag_xn",
            "tag_xm",
            "tag_xo",
            "tag_xg",
            "tag_sa",
        ] {
            assert!(output_names.contains(&n), "describe() missing {n}");
        }

        let param_names: Vec<&str> = desc.config_params.iter().map(|p| p.name).collect();
        assert!(param_names.contains(&"index_path"));
        assert!(param_names.contains(&"nthreads"));
        assert!(param_names.contains(&"preset"));
        assert!(param_names.contains(&"seed"));
        assert!(param_names.contains(&"k"));
        assert!(param_names.contains(&"trim5"));
        assert!(param_names.contains(&"match_bonus"));
        assert!(param_names.contains(&"min_insert"));
        assert!(param_names.contains(&"mate_orientation"));
        assert!(param_names.contains(&"seed_mismatches"));
        assert!(param_names.contains(&"no_unal"));
        assert!(param_names.contains(&"xeq"));
        assert!(param_names.contains(&"ignore_quals"));
        assert!(param_names.contains(&"reorder"));
        assert!(param_names.contains(&"verbose"));

        let meta_names: Vec<&str> = desc.response_metadata.iter().map(|m| m.name).collect();
        assert!(meta_names.contains(&"n_reads"));
        assert!(meta_names.contains(&"n_aligned"));
        assert!(meta_names.contains(&"elapsed_ms"));
    }

    // -- Streaming context tests --

    #[test]
    fn test_bowtie2_create_streaming_context() {
        let (_dir, prefix) = build_test_index();
        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({"index_path": prefix});
        let result = tool.create_streaming_context(&config);
        assert!(
            result.is_ok(),
            "create_streaming_context failed: {:?}",
            result.err()
        );
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_bowtie2_streaming_bad_index_path() {
        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({"index_path": "/nonexistent/path"});
        match tool.create_streaming_context(&config) {
            Err(e) => assert!(
                e.contains("Failed") || e.contains("context"),
                "Unexpected error: {e}"
            ),
            Ok(_) => panic!("Expected error for bad index path"),
        }
    }

    #[test]
    fn test_bowtie2_streaming_run_batch() {
        let (_dir, prefix) = build_test_index();
        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({"index_path": prefix});
        let mut ctx = tool
            .create_streaming_context(&config)
            .expect("context creation failed")
            .expect("tool should support streaming");

        // Single-end read from TEST_REF[10..37]
        let read_seq = &TEST_REF[10..37];
        let input_name = unique_shm_name("bt2-str-in");
        let batch = make_single_end_input(
            &["read1"],
            &[read_seq],
            &[Some(&"I".repeat(read_seq.len()))],
        );
        let shm_holder = write_arrow_to_shm(&input_name, &batch);
        let shm_holder_size = shm_holder.len();

        let response = ctx.run_batch(&input_name, shm_holder_size);
        assert!(response.success, "run_batch failed: {:?}", response.error);
        assert_eq!(response.shm_outputs.len(), 1);
        assert_eq!(response.shm_outputs[0].label, "alignments");

        let result = response.result.unwrap();
        assert_eq!(result["n_reads"], 1);
        assert_eq!(result["n_aligned"], 1);

        let out_batches =
            read_arrow_from_shm(&response.shm_outputs[0].name, response.shm_outputs[0].size);
        assert_eq!(out_batches.len(), 1);
        assert!(out_batches[0].num_rows() > 0);

        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);
    }

    #[test]
    fn test_bowtie2_streaming_two_batches() {
        let (_dir, prefix) = build_test_index();
        let tool = Bowtie2AlignTool;
        let config = serde_json::json!({"index_path": prefix});
        let mut ctx = tool
            .create_streaming_context(&config)
            .expect("context creation failed")
            .expect("tool should support streaming");

        // Batch 1: read from positions 10-37
        let read1 = &TEST_REF[10..37];
        let input1 = unique_shm_name("bt2-str-b1");
        let batch1 =
            make_single_end_input(&["read_a"], &[read1], &[Some(&"I".repeat(read1.len()))]);
        let shm_holder1 = write_arrow_to_shm(&input1, &batch1);
        let shm_holder1_size = shm_holder1.len();
        let resp1 = ctx.run_batch(&input1, shm_holder1_size);
        assert!(resp1.success, "Batch 1 failed: {:?}", resp1.error);
        assert_eq!(resp1.result.as_ref().unwrap()["n_aligned"], 1);
        let _ = SharedMemory::unlink(&resp1.shm_outputs[0].name);

        // Batch 2: read from positions 50-77
        let read2 = &TEST_REF[50..77];
        let input2 = unique_shm_name("bt2-str-b2");
        let batch2 =
            make_single_end_input(&["read_b"], &[read2], &[Some(&"I".repeat(read2.len()))]);
        let shm_holder2 = write_arrow_to_shm(&input2, &batch2);
        let shm_holder2_size = shm_holder2.len();
        let resp2 = ctx.run_batch(&input2, shm_holder2_size);
        assert!(resp2.success, "Batch 2 failed: {:?}", resp2.error);
        assert_eq!(resp2.result.as_ref().unwrap()["n_aligned"], 1);
        let _ = SharedMemory::unlink(&resp2.shm_outputs[0].name);
    }
}
