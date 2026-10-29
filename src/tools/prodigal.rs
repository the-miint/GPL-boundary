//! Prodigal adapter: reads Arrow IPC from shared memory, runs prodigal
//! gene prediction, writes SOA genes as Arrow IPC back to shared memory.

use crate::protocol::Response;
use crate::tools::{
    config_bool, config_cint, config_f64, config_knobs_known, config_str, ConfigParam,
    FieldDescription, GplTool, StreamingContext, ToolDescription,
};
use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::ffi::CStr;
use std::os::raw::{c_char, c_double, c_int, c_void};
use std::path::PathBuf;
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
    "prodigal_seq_info_t" => pub struct ProdigalSeqInfo {
        pub length: i32,
        pub gc_content: c_double,
    }
}

c_mirror! {
    "prodigal_training_info_t" => pub struct ProdigalTrainingInfo {
        pub translation_table: i32,
        pub uses_sd: i32,
        pub gc_content: c_double,
        pub start_weight: c_double,
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
    fn prodigal_get_seq_info(ctx: *const prodigal_ctx_t, info: *mut ProdigalSeqInfo) -> c_int;
    fn prodigal_train(ctx: *mut prodigal_ctx_t) -> c_int;
    fn prodigal_load_training(ctx: *mut prodigal_ctx_t, data: *const c_void, len: usize) -> c_int;
    fn prodigal_export_training(
        ctx: *const prodigal_ctx_t,
        data_out: *mut *mut c_void,
        len_out: *mut usize,
    ) -> c_int;
    fn prodigal_training_info(
        data: *const c_void,
        len: usize,
        info: *mut ProdigalTrainingInfo,
        msg: *mut c_char,
        msg_len: usize,
    ) -> c_int;
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

/// The linked library's own version (prodigal.h's runtime query).
pub(super) fn library_version() -> String {
    unsafe { CStr::from_ptr(prodigal_version_string()) }
        .to_string_lossy()
        .into_owned()
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

/// The input's columns, as --describe lists them (prodigal and
/// prodigal-train).
pub(super) fn input_fields() -> Vec<FieldDescription> {
    vec![
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
            description: "Nucleotide sequence (sequences may have different lengths); a \
                          character outside 'A'..'z', which Prodigal would drop, is an error",
        },
    ]
}

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

    /// Read C SOA arrays into Rust Vecs. Caller must ensure soa is valid.
    unsafe fn append_from_soa(&mut self, seq_name: &str, soa: &ProdigalGenesSoa) {
        // With no genes the arrays may be NULL
        if soa.n_genes <= 0 {
            return;
        }
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

/// The translation tables the library takes (single-genome mode;
/// metagenomic mode takes 11 only).
pub(super) const TRANS_TABLES: [c_int; 10] = [4, 5, 9, 11, 13, 15, 21, 25, 26, 32];

/// The model's knobs, which prodigal and prodigal-train share, strictly
/// typed on the library's defaults (what the JSON omits keeps them); any
/// other knob must be in `params`, the tool's --describe.
pub(super) fn model_config(
    json: &serde_json::Value,
    params: &[ConfigParam],
) -> Result<ProdigalConfig, String> {
    config_knobs_known(json, params)?;
    let mut c = unsafe { ProdigalConfig::library_defaults()? };
    if let Some(table) = config_cint(json, "trans_table")? {
        c.trans_table = table;
    }
    for (name, field) in [
        ("closed_ends", &mut c.closed_ends),
        ("mask_regions", &mut c.mask_regions),
        ("force_nonsd", &mut c.force_nonsd),
    ] {
        if let Some(on) = config_bool(json, name)? {
            *field = on.into();
        }
    }
    if let Some(weight) = config_f64(json, "start_weight")? {
        c.start_weight = weight;
    }
    if config_bool(json, "verbose")?.unwrap_or(false) {
        c.log_callback = Some(stderr_log_callback);
    }
    Ok(c)
}

/// The library's reason for refusing a config (prodigal_config_validate).
pub(super) fn validate(config: &ProdigalConfig) -> Result<(), String> {
    let mut msg: [c_char; 256] = [0; 256];
    if unsafe { prodigal_config_validate(config, msg.as_mut_ptr(), msg.len()) } != PRODIGAL_OK {
        let why = c_buf_to_string(&msg);
        return Err(format!("Invalid prodigal config: {why}"));
    }
    Ok(())
}

/// A model file, and the SHA-256 it must have.
struct Model {
    path: PathBuf,
    sha256: String,
}

/// A request's config, checked: the library's config, which
/// prodigal_config_validate accepts, and single-genome mode's model file.
struct Settings {
    config: ProdigalConfig,
    model: Option<Model>,
}

impl Settings {
    /// Parse and check a request's config, before any input is read.
    fn parse(json: &serde_json::Value) -> Result<Self, String> {
        let settings = Self::unchecked(json)?;
        validate(&settings.config)?;
        Ok(settings)
    }

    /// The JSON's settings, before the library checks the config.
    fn unchecked(json: &serde_json::Value) -> Result<Self, String> {
        let mut config = model_config(json, &ProdigalTool.describe().config_params)?;
        let meta_mode = config_bool(json, "meta_mode")?.unwrap_or(true);
        let train_on_batch = config_bool(json, "train_on_batch")?.unwrap_or(false);
        let path = config_str(json, "training_path")?;
        let sha256 = config_str(json, "training_sha256")?;
        config.meta_mode = meta_mode.into();

        // A batch may hold part of a genome, so single-genome mode needs a
        // model, or to be told that each batch is a whole genome
        if meta_mode && (train_on_batch || path.is_some()) {
            return Err(
                "train_on_batch and training_path apply to single-genome mode \
                 (meta_mode=false)"
                    .into(),
            );
        }
        if train_on_batch && path.is_some() {
            return Err("train_on_batch and training_path are alternatives: set one".into());
        }
        if !meta_mode && !train_on_batch && path.is_none() {
            return Err(
                "single-genome mode (meta_mode=false) needs one of training_path \
                 (a model from prodigal-train) or train_on_batch=true (train on \
                 each batch, which must then hold a whole genome)"
                    .into(),
            );
        }
        let model = match path {
            None if sha256.is_some() => {
                return Err("training_sha256 goes with training_path".into());
            }
            None => None,
            Some(path) => {
                // The model carries them
                let knobs = ["trans_table", "start_weight", "force_nonsd"];
                if let Some(knob) = knobs.into_iter().find(|&k| !json[k].is_null()) {
                    return Err(format!(
                        "{knob} comes from training_path's model: leave it out"
                    ));
                }
                let sha256 = sha256.ok_or(
                    "training_path needs training_sha256, the model's SHA-256 \
                     (prodigal-train reports it)",
                )?;
                if sha256.len() != 64 || !sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(format!(
                        "training_sha256 must be 64 hexadecimal digits, got {sha256:?}"
                    ));
                }
                Some(Model {
                    path: path.into(),
                    sha256: sha256.to_ascii_lowercase(),
                })
            }
        };
        Ok(Self { config, model })
    }

    fn meta_mode(&self) -> bool {
        self.config.meta_mode != 0
    }
}

/// A file's SHA-256, in lowercase hexadecimal.
pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    format!("{:x}", sha2::Sha256::digest(bytes))
}

/// A model's settings, as the library reads them from its blob.
pub(super) fn training_info(blob: &[u8]) -> Result<ProdigalTrainingInfo, String> {
    let mut info: ProdigalTrainingInfo = unsafe { std::mem::zeroed() };
    let mut msg: [c_char; 256] = [0; 256];
    let rc = unsafe {
        prodigal_training_info(
            blob.as_ptr().cast(),
            blob.len(),
            &mut info,
            msg.as_mut_ptr(),
            msg.len(),
        )
    };
    if rc != PRODIGAL_OK {
        return Err(format!("the model is refused: {}", c_buf_to_string(&msg)));
    }
    Ok(info)
}

/// A batch's contigs, read in place from the input mapping: its (name,
/// sequence) columns, which hold no nulls and no character the library
/// drops.
pub(super) struct Contigs(Vec<(StringArray, StringArray)>);

impl Contigs {
    pub(super) fn read(shm_input: &str, shm_input_size: usize) -> Result<Self, String> {
        let batches = crate::arrow_ipc::read_batches_from_shm(shm_input, shm_input_size)?;
        let mut columns = Vec::with_capacity(batches.len());
        let mut row = 0;
        for batch in &batches {
            let column = |name: &str| {
                let col = batch
                    .column_by_name(name)
                    .ok_or_else(|| format!("Input missing '{name}' column"))?;
                let col = col.as_any().downcast_ref::<StringArray>();
                col.cloned()
                    .ok_or_else(|| format!("'{name}' column is not Utf8"))
            };
            let (names, seqs) = (column("name")?, column("sequence")?);
            for i in 0..batch.num_rows() {
                // 1-based, as positions in a sequence
                let row = row + i + 1;
                if names.is_null(i) {
                    return Err(format!("input row {row}: null name"));
                }
                let name = names.value(i);
                if seqs.is_null(i) {
                    return Err(format!("contig '{name}' (input row {row}): null sequence"));
                }
                if let Some(why) = dropped_character(seqs.value(i)) {
                    return Err(format!("contig '{name}': {why}"));
                }
            }
            row += batch.num_rows();
            columns.push((names, seqs));
        }
        if row == 0 {
            return Err("At least 1 sequence required".into());
        }
        Ok(Self(columns))
    }

    /// (name, sequence) of each contig, in input order.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        let batches = self.0.iter();
        batches.flat_map(|(names, seqs)| (0..names.len()).map(|i| (names.value(i), seqs.value(i))))
    }
}

/// A sequence's length as the library takes it. Utf8 offsets are i32, so
/// an Arrow value's length always fits.
fn c_len(seq: &str) -> i32 {
    seq.len() as i32
}

/// A sequence's first character outside 'A'..'z', which the library would
/// drop (shifting every coordinate after it), as an error.
fn dropped_character(seq: &str) -> Option<String> {
    let at = seq.bytes().position(|b| !(b'A'..=b'z').contains(&b))?;
    // Every byte before it is ASCII, so it starts a character
    let c = seq[at..].chars().next().unwrap();
    Some(format!(
        "{c:?} at position {} is not a base or an ambiguity code (Prodigal \
         would drop it, shifting every coordinate after it)",
        at + 1
    ))
}

/// An owned prodigal context, destroyed on drop.
pub(super) struct Context(*mut prodigal_ctx_t);

impl Drop for Context {
    fn drop(&mut self) {
        unsafe { prodigal_destroy(self.0) };
    }
}

impl Context {
    /// A context for a config validate() accepts.
    pub(super) fn new(config: &ProdigalConfig) -> Result<Self, String> {
        let ctx = unsafe { prodigal_create(config) };
        if ctx.is_null() {
            return Err("Failed to create prodigal context (out of memory)".into());
        }
        Ok(Self(ctx))
    }

    /// A call's result: the library's error code and message if it failed.
    fn check(&self, rc: c_int) -> Result<(), String> {
        if rc == PRODIGAL_OK {
            return Ok(());
        }
        let (code, msg) = unsafe {
            (
                CStr::from_ptr(prodigal_strerror(rc)),
                CStr::from_ptr(prodigal_last_error(self.0)),
            )
        };
        Err(format!(
            "{}: {}",
            code.to_string_lossy(),
            msg.to_string_lossy()
        ))
    }

    /// Train on every contig, as the CLI's single-genome mode does.
    pub(super) fn train(&mut self, contigs: &Contigs) -> Result<(), String> {
        let (seqs, lens): (Vec<*const c_char>, Vec<i32>) = contigs
            .iter()
            .map(|(_, s)| (s.as_ptr().cast(), c_len(s)))
            .unzip();
        let n = i32::try_from(seqs.len()).map_err(|_| "too many contigs to train on")?;
        unsafe {
            let rc = prodigal_set_training_sequences(
                self.0,
                seqs.as_ptr(),
                ptr::null(),
                lens.as_ptr(),
                n,
            );
            self.check(rc)
                .and_then(|()| self.check(prodigal_train(self.0)))
        }
        .map_err(|e| format!("training on the batch: {e}"))
    }

    /// The model, as the CLI's -t writes it.
    pub(super) fn export(&self) -> Result<Vec<u8>, String> {
        let mut data: *mut c_void = ptr::null_mut();
        let mut len = 0;
        if unsafe { prodigal_export_training(self.0, &mut data, &mut len) } != PRODIGAL_OK {
            // No message: the context is const
            return Err("prodigal_export_training failed".into());
        }
        let blob = unsafe { std::slice::from_raw_parts(data as *const u8, len) }.to_vec();
        unsafe { libc::free(data) };
        Ok(blob)
    }

    /// Load a model file, refusing it unless its SHA-256 is the one asked
    /// for: a file retrained since is not served.
    fn load_model(&mut self, model: &Model) -> Result<(), String> {
        // Larger than any model (558,392 bytes on 64-bit builds), so a wrong
        // file is refused before it is read
        const MAX_MODEL_BYTES: u64 = 1 << 20;
        let meta = std::fs::metadata(&model.path).map_err(|e| e.to_string())?;
        if !meta.is_file() || meta.len() > MAX_MODEL_BYTES {
            return Err(format!(
                "not a Prodigal model (a file of at most {MAX_MODEL_BYTES} bytes)"
            ));
        }
        let blob = std::fs::read(&model.path).map_err(|e| e.to_string())?;
        let sha256 = sha256_hex(&blob);
        if sha256 != model.sha256 {
            return Err(format!(
                "its SHA-256 is {sha256}, not training_sha256 {} (retrained since?)",
                model.sha256
            ));
        }
        let rc = unsafe { prodigal_load_training(self.0, blob.as_ptr().cast(), blob.len()) };
        self.check(rc)
    }

    /// Load a contig, checking the library kept all of it.
    fn load(&mut self, seq: &str) -> Result<(), String> {
        unsafe {
            let rc = prodigal_set_sequence(self.0, seq.as_ptr().cast(), c_len(seq), ptr::null());
            self.check(rc)?;
            let mut info: ProdigalSeqInfo = std::mem::zeroed();
            if prodigal_get_seq_info(self.0, &mut info) != PRODIGAL_OK {
                return Err("prodigal_get_seq_info failed".into());
            }
            // Contigs::read refused what the library drops: this catches a
            // change in that rule
            if info.length != c_len(seq) {
                let kept = info.length;
                return Err(format!(
                    "Prodigal kept {kept} of its {} characters",
                    seq.len()
                ));
            }
        }
        Ok(())
    }

    /// Find the genes of every contig, and respond with them.
    fn predict(&mut self, contigs: &Contigs) -> Result<Response, String> {
        let mut accumulator = GeneAccumulator::new();
        let mut last_stats: ProdigalStats = unsafe { std::mem::zeroed() };
        for (name, seq) in contigs.iter() {
            let contig = |e: String| format!("contig '{name}': {e}");
            self.load(seq).map_err(contig)?;
            let mut genes: *mut ProdigalGenesSoa = ptr::null_mut();
            unsafe {
                let rc = prodigal_find_genes(self.0, &mut genes, &mut last_stats);
                self.check(rc).map_err(contig)?;
                if !genes.is_null() {
                    accumulator.append_from_soa(name, &*genes);
                    prodigal_genes_free(genes);
                }
            }
        }

        let n_genes = accumulator.n_genes();
        let batch = accumulator.into_record_batch()?;
        let shm_out = crate::arrow_ipc::write_batch_to_output_shm(&batch, "genes")?;
        let result = serde_json::json!({
            "n_genes": n_genes,
            "n_sequences": contigs.iter().count(),
            "stats": {
                "gc_content": last_stats.gc_content,
                "translation_table": last_stats.translation_table,
                "uses_sd": last_stats.uses_sd,
                "best_meta_bin": last_stats.best_meta_bin,
                "best_meta_desc": c_buf_to_string(&last_stats.best_meta_desc),
            }
        });
        Ok(Response::ok(result, vec![shm_out]))
    }
}

/// What a request's batches run on: execute() runs one, a streaming
/// session every batch of its config.
enum Session {
    /// One context for every batch: metagenomic mode (the bins are set up
    /// at the first contig and reused for every batch after), or
    /// single-genome mode with training_path's model
    Shared(Context),
    /// A context for each batch, trained on the batch's contigs
    /// (single-genome mode, train_on_batch)
    TrainOnBatch(ProdigalConfig),
}

// Safety: The C context is not thread-safe, but no references to it are
// handed out: only Session's &mut self methods reach it, so exclusive
// access is enforced at the type level. The config's pointers are null or
// a static function.
unsafe impl Send for Session {}

impl Session {
    fn new(settings: Settings) -> Result<Self, String> {
        if !settings.meta_mode() && settings.model.is_none() {
            return Ok(Self::TrainOnBatch(settings.config));
        }
        let mut ctx = Context::new(&settings.config)?;
        if let Some(model) = &settings.model {
            let path = model.path.display();
            ctx.load_model(model)
                .map_err(|e| format!("training_path {path}: {e}"))?;
        }
        Ok(Self::Shared(ctx))
    }

    fn run(&mut self, contigs: &Contigs) -> Result<Response, String> {
        match self {
            Self::Shared(ctx) => ctx.predict(contigs),
            Self::TrainOnBatch(config) => {
                let mut ctx = Context::new(config)?;
                ctx.train(contigs)?;
                ctx.predict(contigs)
            }
        }
    }
}

impl StreamingContext for Session {
    fn run_batch(&mut self, shm_input: &str, shm_input_size: usize) -> Response {
        Contigs::read(shm_input, shm_input_size)
            .and_then(|contigs| self.run(&contigs))
            .unwrap_or_else(Response::error)
    }
}

impl GplTool for ProdigalTool {
    fn name(&self) -> &str {
        "prodigal"
    }

    fn version(&self) -> String {
        library_version()
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
            // 3: train_on_batch; trans_table's allowed values are integers;
            // the input's nulls and dropped characters are refused.
            // 4: training_path + training_sha256 (a model from prodigal-train).
            describe_version: 4,
            description: "Prokaryotic gene prediction for metagenomic and single-genome sequences",
            config_params: vec![
                ConfigParam {
                    name: "meta_mode",
                    param_type: "boolean",
                    default: serde_json::json!(true),
                    description: "Use pre-trained metagenomic models (true), or a model of \
                                  one genome (false: needs training_path or train_on_batch)",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "training_path",
                    param_type: "string",
                    default: serde_json::Value::Null,
                    description: "Single-genome mode: the model file prodigal-train wrote \
                                  (the CLI's -t format), loaded once per worker. Needs \
                                  training_sha256; the model sets trans_table, start_weight \
                                  and force_nonsd, so they are refused with it. closed_ends \
                                  and mask_regions are not in the model: pass the values it \
                                  was trained with",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "training_sha256",
                    param_type: "string",
                    default: serde_json::Value::Null,
                    description: "The SHA-256 of training_path's model, in hexadecimal \
                                  as prodigal-train reports it. A worker loads the file only \
                                  if its hash matches, and then serves that model for this \
                                  hash",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "train_on_batch",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Single-genome mode without training_path: train on each \
                                  batch's own contigs, so each batch must hold a whole genome",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "trans_table",
                    param_type: "integer",
                    default: serde_json::json!(11),
                    description: "NCBI translation table number (11: bacteria, archaea, \
                                  plant plastids). Metagenomic mode takes 11 only (its bins \
                                  choose 11 or 4 per contig); the others need single-genome mode",
                    allowed_values: TRANS_TABLES.map(Into::into).to_vec(),
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
            input_schema: input_fields(),
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
        match Settings::parse(config).and_then(Session::new) {
            Ok(mut session) => session.run_batch(shm_input, shm_input_size),
            Err(e) => Response::error(e),
        }
    }

    fn create_streaming_context(
        &self,
        config: &serde_json::Value,
    ) -> Result<Option<Box<dyn StreamingContext>>, String> {
        let session = Session::new(Settings::parse(config)?)?;
        Ok(Some(Box::new(session)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shm::SharedMemory;
    use crate::test_util::{
        anthus, gene_coordinates, genes_of, gff_coordinates, prodigal_batch, read_arrow_from_shm,
        scratch_dir, unique_shm_name, write_arrow_to_shm, PRODIGAL_REF_TRAIN as REF_TRAIN,
        PRODIGAL_REF_TRAIN_SHA256 as REF_TRAIN_SHA256,
    };

    // Test sequences from Prodigal's anthus_aco.fas (960 bp each)
    const TEST_SEQ_1: &str = "ACAGGTTAGAAACTACTCTGTTTTCTGGCTCCTTGTTTAATGCCCTGTCCTATTTTATTGCGAAAATTGTCTGTTTTTCACAGAAAACTGAGAGTAGTCAAGGGATTCCTTGTCCTTTGCTTTGGTCTGCACAGCTGTCTTGTTTTAAGGCCAAGTGGAATGAGACAGCTGACTCTTCAGGTGTGAAAACTTGGATGTAGGTGTAGATTGGCTCTCAGTTGACCTCCAGCTGGTGCAGATTCTTCAGTTTGTTTGATGGAGCTTTGAGAAGTCCTTTCAGACTGAAGGATACTCTGAATTTTAGCTATGGAGATAATGTGGATGTTGTGTGTTTAAGTCCTGTTTGCAGTTTTTTTCTGTTCAGTCAGTTATTTTACTGTGTGAGTCAGGACCCTTAGAAGCCCTCAGTGGCAACCACAGAGCGCACAGTTAATTTTCTGTGCAAGAAAATTAAGATCATACTCTGTGTCCAGGAAAGTCAAGAATATTCCTGGTTTTCTCTACTGTAAAATTTTATCTTGTAACTTGTGTTTGGGTCTGCATGATTATTCAAAAATCTTAGTAGATTTGGAAGGATGTTGCATATTATGGAAACAAAGTTGGAAAAAGTTTGTATCAGTTGCAGTATTTCTTCACATCATTTNTTAACNNCNTNNNNNNNNNGCTTCTGCCACTTGAAAAGACAAATTAAAAACNAATTTATAATGCTTATATGCTTTAGTTACATTNGGGTCTTTCAGTAACTTTAGTGCTTTTGATAGCCATACCTGTGAGNTTGACAGTGTCTAAAATTAGAAGTGTTCCTTTTCTTCTGCTCTTCCCATTCTCGTGTGTCTTCAATAGTTTCTGCAAATAATGATGTGCAGACTTAGCATTGATTCAACAGCAGAGGTAAGCATACCTGTGGCTTACTTGGCTTCAGCTTATCCAGCAGTGCCAACCACTCTCTGTTTGTCTTAC";
    const TEST_SEQ_2: &str = "ACAGGTTAGAAACTACTCTGTTTTCTGGCTGCTTGTTTAATGCCCTCTCCTATTTTATTGTGACGATTGTCTGTTTTTCACAGAAAACTGAGAGTAGTCAAGGGATTCCTTGTCCTTTGCTTTGGTGTGCACAGCTGTCTTGTTTTAAGGCCCAGTGGAATGAGACAGCTGACTCTTCAGGTGTGAAAACTTGGATGTACGTGTAGATTGGCTCTCAGTTGACCTCCAGCTGGTGCAGATTCTTCAGTTTGTTTGATGGAGCTTTGAGAAGTCCTTTCAGACTGAAGGATACTCTGAATTTTAGCTATGGAAATAATGTGGATGTTGTGTGTTTAAGCACTGTTTGCAGTTTTTTTCTGTTCAGTCAGTTATTTTACTGTGTGAGTCAGGACTCTTAGAAGCCCTCTGTGGCAACCACAGAGCGCATAGTTAATTTTCTGTACAAGAAAATTAAGATCCTACTCAGTGTTCAGGAAAGTCAAGAATATTCCTGGTTTTCTCTACTGTAAAATTTTATCTTGTAACTTGTGTTTGGGTCTGCATGATTATTCAAAAATCTTAGTAGATTTGGAAGGATGTTGCATTTTATGGAAACAAAGTTGGGAAAAGTTTGTATCAGTTCCAGTATTTCTTCACATCATTTNTTAACNNCNTNNNNNNNNNGCTTCTACCACTTGAAAAGACAAATTAAAAACNAATTTATAATGCTTATATGCTTTAGTTACATTNGGGTCTTTCAGTAACTTTAGTGCTTTACTTAGCCATACCTGTGAGCNTGGCAGTGTCTAAAATTAGAAGTGTTCCTTTTCTTCTGCTCTTCCCATTCTCGTGTGTCTTCAATAGTTTCTGCAAATAATGATGTGCAGACTTAGCATTGGTTCAACAGCAGAGGTAAAAATACCTGTGGCTTACCCGGCTTCAGCTTATCCAGCAGTGCCAACCACTCTCTGTTTGTCTTAC";

    /// An input batch whose columns may hold nulls.
    fn make_nullable_batch(names: &[Option<&str>], sequences: &[Option<&str>]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(sequences.to_vec())),
            ],
        )
        .unwrap()
    }

    /// execute() on a batch: the result JSON and the genes, or the error.
    fn predict_batch(
        config: serde_json::Value,
        batch: &RecordBatch,
    ) -> Result<(serde_json::Value, RecordBatch), String> {
        let input_name = unique_shm_name("pd-run");
        let input = write_arrow_to_shm(&input_name, batch);
        genes_of(ProdigalTool.execute(&config, &input_name, input.len()))
    }

    /// A batch run through a streaming session: the result JSON and the
    /// genes, or the error.
    fn predict_in(
        session: &mut Box<dyn StreamingContext>,
        batch: &RecordBatch,
    ) -> Result<(serde_json::Value, RecordBatch), String> {
        let input_name = unique_shm_name("pd-sess");
        let input = write_arrow_to_shm(&input_name, batch);
        genes_of(session.run_batch(&input_name, input.len()))
    }

    /// Single-genome mode from REF_TRAIN, with `knobs` added or replaced.
    fn from_model(knobs: serde_json::Value) -> serde_json::Value {
        let mut config = serde_json::json!({
            "meta_mode": false,
            "training_path": REF_TRAIN,
            "training_sha256": REF_TRAIN_SHA256,
        });
        for (knob, value) in knobs.as_object().unwrap() {
            config[knob] = value.clone();
        }
        config
    }

    fn predict(
        config: serde_json::Value,
        names: &[&str],
        sequences: &[&str],
    ) -> Result<(serde_json::Value, RecordBatch), String> {
        predict_batch(config, &prodigal_batch(names, sequences))
    }

    #[test]
    fn test_prodigal_meta_roundtrip() {
        let input_name = unique_shm_name("pd-meta-in");

        let batch = prodigal_batch(&["contig_1", "contig_2"], &[TEST_SEQ_1, TEST_SEQ_2]);

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
        let batch = prodigal_batch(&["contig_1"], &[TEST_SEQ_1]);
        let input_shm = write_arrow_to_shm(&input_name, &batch);
        let input_shm_size = input_shm.len();

        let tool = ProdigalTool;
        let config = serde_json::json!({
            "meta_mode": false,
            "train_on_batch": true,
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
    fn test_prodigal_streaming_run_batch() {
        let tool = ProdigalTool;
        let config = serde_json::json!({"meta_mode": true, "trans_table": 11});
        let mut ctx = tool
            .create_streaming_context(&config)
            .expect("context creation failed")
            .expect("tool should support streaming");

        let input_name = unique_shm_name("pd-str-in");
        let batch = prodigal_batch(&["contig_1", "contig_2"], &[TEST_SEQ_1, TEST_SEQ_2]);
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
        let batch1 = prodigal_batch(&["contig_1"], &[TEST_SEQ_1]);
        let shm_holder1 = write_arrow_to_shm(&input1, &batch1);
        let shm_holder1_size = shm_holder1.len();
        let resp1 = ctx.run_batch(&input1, shm_holder1_size);
        assert!(resp1.success, "Batch 1 failed: {:?}", resp1.error);
        let genes1 = resp1.result.as_ref().unwrap()["n_genes"].as_i64().unwrap();
        let _ = SharedMemory::unlink(&resp1.shm_outputs[0].name);

        // Batch 2: different sequence
        let input2 = unique_shm_name("pd-str-b2");
        let batch2 = prodigal_batch(&["contig_2"], &[TEST_SEQ_2]);
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
            ProdigalSeqInfo::c_layout(),
            ProdigalTrainingInfo::c_layout(),
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
        let pd = Settings::parse(&serde_json::json!({})).unwrap().config;
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
    /// (single-genome mode; metagenomic mode takes 11 only), as integers.
    #[test]
    fn test_describe_trans_tables_are_the_librarys() {
        let desc = ProdigalTool.describe();
        let param = desc.config_params.iter().find(|p| p.name == "trans_table");
        let listed = serde_json::to_value(&param.expect("trans_table param").allowed_values);
        let accepted: Vec<c_int> = (1..=99)
            .filter(|&t| unsafe {
                let mut pd = ProdigalConfig::library_defaults().unwrap();
                pd.trans_table = t;
                prodigal_config_validate(&pd, ptr::null_mut(), 0) == PRODIGAL_OK
            })
            .collect();
        assert_eq!(listed.unwrap(), serde_json::json!(accepted));
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
            let pd = Settings::unchecked(config).unwrap().config;
            let mut msg: [c_char; 256] = [0; 256];
            let rc = prodigal_config_validate(&pd, msg.as_mut_ptr(), msg.len());
            assert_ne!(rc, PRODIGAL_OK, "{config}: the library accepts it");
            c_buf_to_string(&msg)
        };
        let tool = ProdigalTool;
        let input_name = unique_shm_name("pd-refused");
        let input_shm = write_arrow_to_shm(&input_name, &prodigal_batch(&["c1"], &[TEST_SEQ_1]));
        for config in [
            serde_json::json!({"meta_mode": true, "trans_table": 4}),
            serde_json::json!({"meta_mode": true, "force_nonsd": true}),
            serde_json::json!({"meta_mode": true, "start_weight": 5.0}),
            serde_json::json!({"meta_mode": false, "train_on_batch": true, "trans_table": 1}),
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

    /// A knob of the wrong JSON type, an integer out of C int range, an
    /// unknown knob or a config that is not an object is an error naming
    /// it, before the input is read, on both routes; null is the default.
    #[test]
    fn test_config_is_strict() {
        let tool = ProdigalTool;
        for (config, named) in [
            (serde_json::json!({"meta_mode": "false"}), "meta_mode"),
            (serde_json::json!({"trans_table": "11"}), "trans_table"),
            (serde_json::json!({"trans_table": 11.0}), "trans_table"),
            // 2^32 + 11: as a C int it would wrap to 11
            (
                serde_json::json!({"trans_table": 4294967307_i64}),
                "trans_table",
            ),
            (serde_json::json!({"closed_ends": 1}), "closed_ends"),
            (serde_json::json!({"mask_regions": "yes"}), "mask_regions"),
            (serde_json::json!({"force_nonsd": 0}), "force_nonsd"),
            (serde_json::json!({"start_weight": "4.35"}), "start_weight"),
            (serde_json::json!({"verbose": 1}), "verbose"),
            (
                serde_json::json!({"meta_mode": false, "train_on_batch": "yes"}),
                "train_on_batch",
            ),
            (serde_json::json!({"metamode": false}), "metamode"),
            (serde_json::json!(["meta_mode"]), "object"),
        ] {
            let response = tool.execute(&config, "/gb-no-such-input", 1);
            let err = response.error.expect("refused");
            assert!(err.contains(named), "{config}: {err}");
            let err = tool.create_streaming_context(&config).err();
            assert!(err.expect("refused").contains(named), "{config}");
        }
        // An integer too large for a C int says so, however large
        for big in [
            serde_json::json!(4294967307_i64),
            serde_json::json!(u64::MAX),
        ] {
            let config = serde_json::json!({"trans_table": big});
            let err = tool
                .create_streaming_context(&config)
                .err()
                .expect("refused");
            let wanted = format!("trans_table={big} exceeds C int range");
            assert!(err.contains(&wanted), "{err}");
        }
        for config in [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!({"trans_table": null, "closed_ends": null, "verbose": null}),
        ] {
            let ctx = tool.create_streaming_context(&config);
            assert!(ctx.expect("accepted").is_some(), "{config}");
        }
    }

    /// Single-genome mode trains on the batch only when asked to: a
    /// batch may hold part of a genome. With train_on_batch there is no
    /// shared context, so the daemon runs each batch through execute().
    #[test]
    fn test_single_mode_needs_train_on_batch() {
        let tool = ProdigalTool;
        for config in [
            serde_json::json!({"meta_mode": false}),
            serde_json::json!({"meta_mode": false, "train_on_batch": false}),
        ] {
            let err = tool.create_streaming_context(&config).err();
            assert!(err.expect("refused").contains("train_on_batch"), "{config}");
            let err = predict(config, &["c1"], &[TEST_SEQ_1]).unwrap_err();
            assert!(err.contains("train_on_batch"), "{err}");
        }
        let config = serde_json::json!({"meta_mode": false, "train_on_batch": true});
        let ctx = tool.create_streaming_context(&config);
        assert!(ctx.expect("accepted").is_some());

        // Metagenomic mode does not train
        let config = serde_json::json!({"train_on_batch": true});
        let err = tool
            .create_streaming_context(&config)
            .err()
            .expect("refused");
        assert!(
            err.contains("train_on_batch") && err.contains("meta_mode"),
            "{err}"
        );
    }

    /// Trained on the whole batch, single-genome mode calls the CLI's genes
    /// (the fork's ref_single.gff: `prodigal -p single` on anthus).
    #[test]
    fn test_single_mode_trains_on_the_batch() {
        let contigs = anthus();
        let names: Vec<&str> = contigs.iter().map(|c| c.0.as_str()).collect();
        let seqs: Vec<&str> = contigs.iter().map(|c| c.1.as_str()).collect();
        let config = serde_json::json!({"meta_mode": false, "train_on_batch": true});
        let (result, genes) = predict(config, &names, &seqs).unwrap();

        let expected = gff_coordinates(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/ext/prodigal/testdata/ground_truth/ref_single.gff"
        )));
        assert_eq!(gene_coordinates(&genes), expected);
        assert_eq!(result["n_genes"], expected.len());
    }

    /// A null name or sequence is an error naming the row or the contig.
    #[test]
    fn test_null_input_is_an_error() {
        let batch = make_nullable_batch(&[Some("c1"), None], &[Some(TEST_SEQ_1), Some(TEST_SEQ_2)]);
        let err = predict_batch(serde_json::json!({}), &batch).unwrap_err();
        assert!(err.contains("row 2") && err.contains("name"), "{err}");

        let batch = make_nullable_batch(&[Some("c1"), Some("c2")], &[Some(TEST_SEQ_1), None]);
        let err = predict_batch(serde_json::json!({}), &batch).unwrap_err();
        assert!(err.contains("'c2'") && err.contains("sequence"), "{err}");
    }

    /// Prodigal drops characters outside 'A'..'z', which would shift every
    /// coordinate after them, so a contig holding one is an error naming
    /// the contig, the character and its position (1-based), on both
    /// routes. Ambiguity codes, lowercase and the rest of 'A'..'z' are
    /// kept.
    #[test]
    fn test_dropped_character_is_an_error() {
        let tool = ProdigalTool;
        let with = |seq: &str, at: usize, c: char| format!("{}{c}{}", &seq[..at], &seq[at..]);
        let mut meta = tool
            .create_streaming_context(&serde_json::json!({}))
            .unwrap()
            .unwrap();
        for (c, at) in [
            ('-', 100),
            ('7', 0),
            (' ', 959),
            ('\0', 500),
            ('é', 300),
            ('*', 960),
        ] {
            let bad = with(TEST_SEQ_2, at, c);
            let input_name = unique_shm_name("pd-drop");
            let batch = prodigal_batch(&["c1", "c2"], &[TEST_SEQ_1, &bad]);
            let input = write_arrow_to_shm(&input_name, &batch);
            let err = meta
                .run_batch(&input_name, input.len())
                .error
                .expect("refused");
            let wanted = format!("{c:?} at position {}", at + 1);
            assert!(err.contains("'c2'") && err.contains(&wanted), "{err}");
        }
        // The context is still usable
        let input_name = unique_shm_name("pd-drop");
        let input = write_arrow_to_shm(&input_name, &prodigal_batch(&["c1"], &[TEST_SEQ_1]));
        let response = meta.run_batch(&input_name, input.len());
        assert!(response.success, "{:?}", response.error);
        let _ = SharedMemory::unlink(&response.shm_outputs[0].name);

        // Single-genome mode
        let mut contigs = anthus();
        contigs[5].1 = with(&contigs[5].1, 42, '.');
        let names: Vec<&str> = contigs.iter().map(|c| c.0.as_str()).collect();
        let seqs: Vec<&str> = contigs.iter().map(|c| c.1.as_str()).collect();
        let config = serde_json::json!({"meta_mode": false, "train_on_batch": true});
        let err = predict(config, &names, &seqs).unwrap_err();
        let wanted = format!("'{}'", names[5]);
        assert!(
            err.contains(&wanted) && err.contains("'.' at position 43"),
            "{err}"
        );

        let kept = TEST_SEQ_1.replace("ACAG", "acRN").replace("GGTT", "[_^Y");
        let result = predict(serde_json::json!({}), &["c1"], &[&kept]);
        assert!(result.is_ok(), "{:?}", result.err());
    }

    /// Any other error a contig causes names the contig.
    #[test]
    fn test_contig_errors_name_the_contig() {
        for bad in ["", "0123456789"] {
            let err = predict(serde_json::json!({}), &["c1", "c2"], &[TEST_SEQ_1, bad]);
            let err = err.unwrap_err();
            assert!(err.contains("'c2'"), "{bad:?}: {err}");
        }
    }

    /// Each contig is read in place, within its own bounds: a contig's
    /// genes are the same alone and followed by another in the batch.
    #[test]
    fn test_contigs_are_read_within_bounds() {
        let config = serde_json::json!({});
        let (_, alone) = predict(config.clone(), &["c1"], &[TEST_SEQ_1]).unwrap();
        let (_, both) = predict(config, &["c1", "c2"], &[TEST_SEQ_1, TEST_SEQ_2]).unwrap();
        let c1: Vec<_> = gene_coordinates(&both)
            .into_iter()
            .filter(|g| g.0 == "c1")
            .collect();
        assert!(!c1.is_empty());
        assert_eq!(gene_coordinates(&alone), c1);
    }

    /// --describe advertises single-genome mode's routes.
    #[test]
    fn test_describe_lists_single_mode_routes() {
        let desc = ProdigalTool.describe();
        for (name, param_type, default) in [
            ("train_on_batch", "boolean", serde_json::json!(false)),
            ("training_path", "string", serde_json::Value::Null),
            ("training_sha256", "string", serde_json::Value::Null),
        ] {
            let param = desc.config_params.iter().find(|p| p.name == name);
            let param = param.expect(name);
            assert_eq!(param.param_type, param_type, "{name}");
            assert_eq!(param.default, default, "{name}");
        }
    }

    /// From a model file, single-genome mode calls the CLI's genes
    /// (ref_trained.gff: `prodigal -t ref_train.bin` on anthus). A session
    /// loads the model once and predicts every batch from it: half of
    /// anthus is too short to train on.
    #[test]
    fn test_training_path_predicts_as_the_cli() {
        let expected = gff_coordinates(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/ext/prodigal/testdata/ground_truth/ref_trained.gff"
        )));
        let contigs = anthus();
        let names: Vec<&str> = contigs.iter().map(|c| c.0.as_str()).collect();
        let seqs: Vec<&str> = contigs.iter().map(|c| c.1.as_str()).collect();
        let batch = prodigal_batch(&names, &seqs);
        let config = from_model(serde_json::json!({}));
        let (_, genes) = predict_batch(config.clone(), &batch).unwrap();
        assert_eq!(gene_coordinates(&genes), expected);

        let mut session = ProdigalTool
            .create_streaming_context(&config)
            .unwrap()
            .unwrap();
        let mut got = Vec::new();
        for half in [batch.slice(0, 11), batch.slice(11, 11)] {
            let (_, genes) = predict_in(&mut session, &half).unwrap();
            got.extend(gene_coordinates(&genes));
        }
        assert_eq!(got, expected);
    }

    /// training_path needs training_sha256, the file's (in either case), so
    /// a model retrained in place is refused rather than served.
    #[test]
    fn test_training_path_checks_the_sha256() {
        let tool = ProdigalTool;
        let refused = |config: serde_json::Value| {
            let err = tool.create_streaming_context(&config).err();
            err.unwrap_or_else(|| panic!("{config}: accepted"))
        };
        let err = refused(from_model(serde_json::json!({"training_sha256": null})));
        assert!(err.contains("training_sha256"), "{err}");
        let err = refused(from_model(serde_json::json!({"training_sha256": "abc"})));
        assert!(err.contains("training_sha256"), "{err}");
        let sha_only = serde_json::json!({
            "meta_mode": false,
            "train_on_batch": true,
            "training_sha256": REF_TRAIN_SHA256,
        });
        assert!(refused(sha_only).contains("training_path"));

        let other = "0".repeat(64);
        let err = refused(from_model(serde_json::json!({"training_sha256": other})));
        assert!(
            err.contains(&other) && err.contains(REF_TRAIN_SHA256),
            "{err}"
        );

        let upper = REF_TRAIN_SHA256.to_uppercase();
        let upper = from_model(serde_json::json!({"training_sha256": upper}));
        assert!(tool.create_streaming_context(&upper).is_ok());
    }

    /// A model file that cannot be read, is not a file, is too large to be a
    /// model (refused before it is read) or that the library refuses is an
    /// error naming the path.
    #[test]
    fn test_training_path_refused_model() {
        let dir = scratch_dir();
        let short = dir.path().join("short.bin");
        let bytes = std::fs::read(REF_TRAIN).unwrap();
        std::fs::write(&short, &bytes[..1000]).unwrap();
        let sha = sha256_hex(&bytes[..1000]);
        let large = dir.path().join("large.bin");
        std::fs::write(&large, vec![0u8; 2 << 20]).unwrap();
        let missing = dir.path().join("missing.bin");
        let not_a_file = dir.path().to_path_buf();
        for path in [&short, &large, &missing, &not_a_file] {
            let config =
                from_model(serde_json::json!({"training_path": path, "training_sha256": sha}));
            let err = ProdigalTool.create_streaming_context(&config).err();
            let err = err.expect("refused");
            assert!(err.contains(path.to_str().unwrap()), "{err}");
            let unread = path == &large || path == &not_a_file;
            assert_eq!(err.contains("not a Prodigal model"), unread, "{err}");
        }
    }

    /// training_path is single-genome mode's other route. The model carries
    /// the translation table, start weight and SD choice, so setting them
    /// with it is an error; closed_ends and mask_regions apply as usual.
    #[test]
    fn test_training_path_conflicts() {
        for (knobs, named) in [
            (serde_json::json!({"meta_mode": true}), "meta_mode"),
            (serde_json::json!({"meta_mode": null}), "meta_mode"),
            (serde_json::json!({"train_on_batch": true}), "alternatives"),
            (serde_json::json!({"trans_table": 11}), "trans_table"),
            (serde_json::json!({"start_weight": 4.35}), "start_weight"),
            (serde_json::json!({"force_nonsd": false}), "force_nonsd"),
            (serde_json::json!({"training_path": 7}), "training_path"),
        ] {
            let config = from_model(knobs);
            let err = ProdigalTool.create_streaming_context(&config).err();
            let err = err.unwrap_or_else(|| panic!("{config}: accepted"));
            assert!(err.contains(named), "{config}: {err}");
        }
        let config = from_model(serde_json::json!({"closed_ends": true, "mask_regions": true}));
        assert!(ProdigalTool.create_streaming_context(&config).is_ok());
    }
}
