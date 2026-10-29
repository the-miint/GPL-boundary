//! Prodigal adapter: reads contigs as Arrow IPC from shared memory, runs
//! prodigal gene prediction, and writes the genes and a table of the contigs
//! as Arrow IPC back to shared memory.

use crate::arrow_ipc::ShmOutputs;
use crate::protocol::Response;
use crate::tools::{
    arrow_schema, config_bool, config_cint, config_f64, config_knobs_known, config_str,
    ConfigParam, FieldDescription, GplTool, OutputDescription, StreamingContext, ToolDescription,
};
use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, StringArray, StringBuilder,
};
use arrow::record_batch::RecordBatch;
use std::ffi::CStr;
use std::io::Read;
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

/// The input's columns (prodigal and prodigal-train), as --describe lists
/// them.
pub(super) const INPUT_FIELDS: &[FieldDescription] = &[
    FieldDescription::new("name", "Utf8", false, "Sequence (contig) identifier"),
    FieldDescription::new(
        "sequence",
        "Utf8",
        false,
        "Nucleotide sequence (sequences may have different lengths); an empty \
         sequence, or a character outside 'A'..'z', which Prodigal would drop, is \
         an error",
    ),
];

/// The input's Arrow schema: contig sequences from miint.
#[cfg(test)]
pub fn input_schema() -> arrow::datatypes::Schema {
    arrow_schema(INPUT_FIELDS).expect("INPUT_FIELDS are Arrow types")
}

/// The genes output: one row per gene, in the order the CLI lists them.
/// Its columns, as --describe lists them, make its Arrow schema.
const GENES: OutputDescription = OutputDescription {
    label: "genes",
    schema: &[
        FieldDescription::new(
            "seq_index",
            "Int32",
            false,
            "The gene's contig: its 1-based position in the batch (contigs.seq_index)",
        ),
        FieldDescription::new("seq_name", "Utf8", false, "The gene's contig: its name"),
        FieldDescription::new(
            "gene_index",
            "Int32",
            false,
            "1-based number of the gene within its contig (the CLI's ID is \
         <seqnum>_<gene_index>)",
        ),
        FieldDescription::new(
            "begin",
            "Int32",
            false,
            "Left coordinate (1-based, inclusive; begin < end on both strands)",
        ),
        FieldDescription::new(
            "end",
            "Int32",
            false,
            "Right coordinate (1-based, inclusive), the stop codon included",
        ),
        FieldDescription::new("strand", "Int32", false, "Strand (+1 or -1)"),
        FieldDescription::new(
            "partial_left",
            "Boolean",
            false,
            "Gene runs off the left edge",
        ),
        FieldDescription::new(
            "partial_right",
            "Boolean",
            false,
            "Gene runs off the right edge",
        ),
        FieldDescription::new(
            "start_type",
            "Int32",
            false,
            "Start codon type (0=ATG, 1=GTG, 2=TTG, 3=Edge)",
        ),
        FieldDescription::new(
            "score",
            "Float64",
            false,
            "Gene score, cscore + sscore (the CLI's score=)",
        ),
        FieldDescription::new("cscore", "Float64", false, "Coding score"),
        FieldDescription::new("sscore", "Float64", false, "Start score"),
        FieldDescription::new("rscore", "Float64", false, "RBS score"),
        FieldDescription::new("uscore", "Float64", false, "Upstream score"),
        FieldDescription::new("tscore", "Float64", false, "Start type score"),
        FieldDescription::new("confidence", "Float64", false, "Gene confidence [50, 100]"),
        FieldDescription::new(
            "gc_cont",
            "Float64",
            false,
            "GC content of the gene as Prodigal counts it",
        ),
        FieldDescription::new(
            "rbs_motif",
            "Utf8",
            true,
            "Ribosome binding site motif as Prodigal prints it (e.g. \"AGGAG\", or an \
         upstream motif such as \"TAA\"); NULL where Prodigal prints \"None\"",
        ),
        FieldDescription::new(
            "rbs_spacer",
            "Utf8",
            true,
            "RBS spacer (e.g. \"5-10bp\"); NULL where Prodigal prints \"None\"",
        ),
        FieldDescription::new(
            "protein",
            "Utf8",
            false,
            "The protein (the CLI's -a record, unwrapped): M for a start codon, * for a \
         stop codon, X for a codon with an ambiguous base; no M or * where the gene \
         runs off an edge",
        ),
    ],
};

/// The contigs output: one row per input contig, with or without genes.
/// Its columns, as --describe lists them, make its Arrow schema.
const CONTIGS: OutputDescription = OutputDescription {
    label: "contigs",
    schema: &[
        FieldDescription::new(
            "seq_index",
            "Int32",
            false,
            "1-based position of the contig in the batch (the CLI's seqnum for a batch \
         holding a whole file)",
        ),
        FieldDescription::new("seq_name", "Utf8", false, "Contig identifier (input name)"),
        FieldDescription::new("length", "Int32", false, "Contig length in bases"),
        FieldDescription::new(
            "gc_content",
            "Float64",
            false,
            "The contig's GC fraction (ambiguous bases count as not GC)",
        ),
        FieldDescription::new(
            "translation_table",
            "Int32",
            false,
            "Translation table the genes were called with: the model's, or the \
         metagenomic bin's",
        ),
        FieldDescription::new(
            "uses_sd",
            "Boolean",
            false,
            "Whether that model or bin scores Shine-Dalgarno motifs",
        ),
        FieldDescription::new(
            "meta_bin",
            "Int32",
            true,
            "Metagenomic mode: the bin that called the genes, 0..49 (the first field of \
         model_desc); NULL in single-genome mode",
        ),
        FieldDescription::new(
            "model_desc",
            "Utf8",
            true,
            "Metagenomic mode: that bin's description (the CLI's Model Data model=); NULL \
         in single-genome mode",
        ),
        FieldDescription::new(
            "n_genes",
            "Int32",
            false,
            "Genes found on the contig (0 for none)",
        ),
    ],
};

// --- Output builders ---

/// The genes output, built contig by contig.
#[derive(Default)]
struct GeneColumns {
    seq_index: Int32Builder,
    seq_name: StringBuilder,
    gene_index: Int32Builder,
    begin: Int32Builder,
    end: Int32Builder,
    strand: Int32Builder,
    partial_left: BooleanBuilder,
    partial_right: BooleanBuilder,
    start_type: Int32Builder,
    score: Float64Builder,
    cscore: Float64Builder,
    sscore: Float64Builder,
    rscore: Float64Builder,
    uscore: Float64Builder,
    tscore: Float64Builder,
    confidence: Float64Builder,
    gc_cont: Float64Builder,
    rbs_motif: StringBuilder,
    rbs_spacer: StringBuilder,
    protein: StringBuilder,
}

/// An RBS motif or spacer as the output holds it: NULL for the library's
/// "None".
unsafe fn rbs_text<'a>(text: *const c_char) -> Option<std::borrow::Cow<'a, str>> {
    let text = (!text.is_null()).then(|| CStr::from_ptr(text).to_string_lossy())?;
    (text != "None").then_some(text)
}

impl GeneColumns {
    /// Append a contig's genes. Caller must ensure soa is the library's,
    /// from a config with want_proteins.
    unsafe fn append(
        &mut self,
        seq_index: i32,
        seq_name: &str,
        soa: &ProdigalGenesSoa,
    ) -> Result<(), String> {
        let n = usize::try_from(soa.n_genes)
            .map_err(|_| format!("the library returned a gene count of {}", soa.n_genes))?;
        // With no genes the arrays may be NULL
        if n == 0 {
            return Ok(());
        }
        if soa.protein_offsets.is_null() {
            return Err("the library returned no proteins".into());
        }
        let ints = |p: *const i32| std::slice::from_raw_parts(p, n);
        let floats = |p: *const c_double| std::slice::from_raw_parts(p, n);
        let flags = |p| ints(p).iter().map(|&v| v != 0);
        let texts = |p: *const *const c_char| std::slice::from_raw_parts(p, n);
        // Gene i's protein is letters[offsets[i]..offsets[i + 1]]: the
        // offsets start at 0 and never decrease
        let offsets = std::slice::from_raw_parts(soa.protein_offsets, n + 1);
        if offsets[0] != 0 || offsets.windows(2).any(|w| w[0] > w[1]) {
            return Err("the library returned malformed protein offsets".into());
        }
        let letters =
            std::slice::from_raw_parts(soa.protein_data.cast::<u8>(), offsets[n] as usize);

        for i in 0..n {
            self.seq_index.append_value(seq_index);
            self.seq_name.append_value(seq_name);
            // n_genes is an i32
            self.gene_index.append_value(i as i32 + 1);
            let protein = &letters[offsets[i] as usize..offsets[i + 1] as usize];
            let protein = std::str::from_utf8(protein).map_err(|e| format!("protein: {e}"))?;
            self.protein.append_value(protein);
        }
        self.begin.append_slice(ints(soa.begin));
        self.end.append_slice(ints(soa.end));
        self.strand.append_slice(ints(soa.strand));
        flags(soa.partial_left).for_each(|v| self.partial_left.append_value(v));
        flags(soa.partial_right).for_each(|v| self.partial_right.append_value(v));
        self.start_type.append_slice(ints(soa.start_type));
        // The CLI's score: cscore + sscore
        let (cscore, sscore) = (floats(soa.cscore), floats(soa.sscore));
        let scores = cscore.iter().zip(sscore).map(|(c, s)| c + s);
        scores.for_each(|v| self.score.append_value(v));
        self.cscore.append_slice(cscore);
        self.sscore.append_slice(sscore);
        self.rscore.append_slice(floats(soa.rscore));
        self.uscore.append_slice(floats(soa.uscore));
        self.tscore.append_slice(floats(soa.tscore));
        self.confidence.append_slice(floats(soa.confidence));
        self.gc_cont.append_slice(floats(soa.gc_cont));
        for &text in texts(soa.rbs_motif) {
            self.rbs_motif.append_option(rbs_text(text));
        }
        for &text in texts(soa.rbs_spacer) {
            self.rbs_spacer.append_option(rbs_text(text));
        }
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch, String> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.seq_index.finish()),
            Arc::new(self.seq_name.finish()),
            Arc::new(self.gene_index.finish()),
            Arc::new(self.begin.finish()),
            Arc::new(self.end.finish()),
            Arc::new(self.strand.finish()),
            Arc::new(self.partial_left.finish()),
            Arc::new(self.partial_right.finish()),
            Arc::new(self.start_type.finish()),
            Arc::new(self.score.finish()),
            Arc::new(self.cscore.finish()),
            Arc::new(self.sscore.finish()),
            Arc::new(self.rscore.finish()),
            Arc::new(self.uscore.finish()),
            Arc::new(self.tscore.finish()),
            Arc::new(self.confidence.finish()),
            Arc::new(self.gc_cont.finish()),
            Arc::new(self.rbs_motif.finish()),
            Arc::new(self.rbs_spacer.finish()),
            Arc::new(self.protein.finish()),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema(GENES.schema)?), columns)
            .map_err(|e| format!("Failed to create the genes batch: {e}"))
    }
}

/// The contigs output, built contig by contig.
#[derive(Default)]
struct ContigColumns {
    seq_index: Int32Builder,
    seq_name: StringBuilder,
    length: Int32Builder,
    gc_content: Float64Builder,
    translation_table: Int32Builder,
    uses_sd: BooleanBuilder,
    meta_bin: Int32Builder,
    model_desc: StringBuilder,
    n_genes: Int32Builder,
}

impl ContigColumns {
    /// Append a contig, with find_genes' stats for it.
    fn append(&mut self, seq_index: i32, seq_name: &str, length: i32, stats: &ProdigalStats) {
        self.seq_index.append_value(seq_index);
        self.seq_name.append_value(seq_name);
        self.length.append_value(length);
        self.gc_content.append_value(stats.gc_content);
        self.translation_table.append_value(stats.translation_table);
        self.uses_sd.append_value(stats.uses_sd != 0);
        // -1 in single-genome mode, which has no bins
        let bin = (stats.best_meta_bin >= 0).then_some(stats.best_meta_bin);
        self.meta_bin.append_option(bin);
        let desc = bin.map(|_| c_buf_to_string(&stats.best_meta_desc));
        self.model_desc.append_option(desc);
        self.n_genes.append_value(stats.n_genes);
    }

    fn finish(mut self) -> Result<RecordBatch, String> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.seq_index.finish()),
            Arc::new(self.seq_name.finish()),
            Arc::new(self.length.finish()),
            Arc::new(self.gc_content.finish()),
            Arc::new(self.translation_table.finish()),
            Arc::new(self.uses_sd.finish()),
            Arc::new(self.meta_bin.finish()),
            Arc::new(self.model_desc.finish()),
            Arc::new(self.n_genes.finish()),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema(CONTIGS.schema)?), columns)
            .map_err(|e| format!("Failed to create the contigs batch: {e}"))
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
        // The genes output always has the proteins
        config.want_proteins = 1;

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
                // One spelling, prodigal-train's, not one fingerprint per case
                let lower_hex = |b: u8| b.is_ascii_digit() || (b'a'..=b'f').contains(&b);
                if sha256.len() != 64 || !sha256.bytes().all(lower_hex) {
                    return Err(format!(
                        "training_sha256 must be 64 lowercase hexadecimal digits, as \
                         prodigal-train reports it, got {sha256:?}"
                    ));
                }
                Some(Model {
                    path: path.into(),
                    sha256: sha256.into(),
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

/// A model's settings, as prodigal and prodigal-train report them.
pub(super) fn model_json(info: &ProdigalTrainingInfo) -> serde_json::Value {
    serde_json::json!({
        "translation_table": info.translation_table,
        "uses_sd": info.uses_sd != 0,
        "gc_content": info.gc_content,
        "start_weight": info.start_weight,
    })
}

/// The model's settings in a result (`model_json`), as --describe lists
/// them; nullable where the result may have no model.
pub(super) fn model_fields(nullable: bool) -> Vec<FieldDescription> {
    let field = |name, arrow_type, description| {
        FieldDescription::new(name, arrow_type, nullable, description)
    };
    vec![
        field(
            "model.translation_table",
            "integer",
            "The model's translation table",
        ),
        field(
            "model.uses_sd",
            "boolean",
            "Whether the model scores Shine-Dalgarno motifs",
        ),
        field(
            "model.gc_content",
            "float64",
            "The model's GC fraction (the CLI's Model Data gc_cont)",
        ),
        field("model.start_weight", "float64", "The model's start weight"),
    ]
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
/// sequence) columns, which hold no nulls, no empty sequence and no
/// character the library drops.
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
                let seq = seqs.value(i);
                if seq.is_empty() {
                    return Err(format!("contig '{name}' (input row {row}): empty sequence"));
                }
                if let Some(why) = dropped_character(seq) {
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
        "{c:?} at position {} is outside 'A'..'z' (Prodigal would drop it, \
         shifting every coordinate after it)",
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
        // With several contigs, a 12-base spacer follows each
        let spacers = if n > 1 { 12 * i64::from(n) } else { 0 };
        let length = lens.iter().map(|&len| i64::from(len)).sum::<i64>() + spacers;
        let rc = unsafe {
            prodigal_set_training_sequences(self.0, seqs.as_ptr(), ptr::null(), lens.as_ptr(), n)
        };
        self.check(rc)
            .and_then(|()| self.check_kept(length))
            .and_then(|()| self.check(unsafe { prodigal_train(self.0) }))
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
    /// for: a file retrained since is not served. Returns its settings.
    fn load_model(&mut self, model: &Model) -> Result<ProdigalTrainingInfo, String> {
        // Larger than any model (558,392 bytes on 64-bit builds), so a wrong
        // file is read no further
        const MAX_MODEL_BYTES: u64 = 1 << 20;
        let not_a_model =
            || format!("not a Prodigal model (a file of at most {MAX_MODEL_BYTES} bytes)");
        // Refused unopened: opening a FIFO or a device waits or acts
        let meta = std::fs::metadata(&model.path).map_err(|e| e.to_string())?;
        if !meta.is_file() {
            return Err(not_a_model());
        }
        let file = std::fs::File::open(&model.path).map_err(|e| e.to_string())?;
        let mut blob = Vec::new();
        file.take(MAX_MODEL_BYTES + 1)
            .read_to_end(&mut blob)
            .map_err(|e| e.to_string())?;
        if blob.len() as u64 > MAX_MODEL_BYTES {
            return Err(not_a_model());
        }
        let sha256 = sha256_hex(&blob);
        if sha256 != model.sha256 {
            return Err(format!(
                "its SHA-256 is {sha256}, not training_sha256 {} (retrained since?)",
                model.sha256
            ));
        }
        let rc = unsafe { prodigal_load_training(self.0, blob.as_ptr().cast(), blob.len()) };
        self.check(rc)?;
        training_info(&blob)
    }

    /// Load a contig, checking the library kept all of it.
    fn load(&mut self, seq: &str) -> Result<(), String> {
        let rc =
            unsafe { prodigal_set_sequence(self.0, seq.as_ptr().cast(), c_len(seq), ptr::null()) };
        self.check(rc)?;
        self.check_kept(c_len(seq).into())
    }

    /// Check the loaded sequence is `length` characters long. Contigs::read
    /// refused what the library drops: this catches a change in that rule.
    fn check_kept(&self, length: i64) -> Result<(), String> {
        let mut info: ProdigalSeqInfo = unsafe { std::mem::zeroed() };
        if unsafe { prodigal_get_seq_info(self.0, &mut info) } != PRODIGAL_OK {
            return Err("prodigal_get_seq_info failed".into());
        }
        if i64::from(info.length) != length {
            return Err(format!(
                "Prodigal kept {} of {length} characters",
                info.length
            ));
        }
        Ok(())
    }

    /// Find the genes of every contig, and respond with them and the
    /// contigs, and the model's settings in single-genome mode.
    fn predict(
        &mut self,
        contigs: &Contigs,
        model: Option<&ProdigalTrainingInfo>,
    ) -> Result<Response, String> {
        let (mut genes, mut table) = (GeneColumns::default(), ContigColumns::default());
        for (i, (name, seq)) in contigs.iter().enumerate() {
            let contig = |e: String| format!("contig '{name}': {e}");
            // 1-based, as the CLI numbers sequences
            let seq_index = i32::try_from(i + 1).map_err(|_| "too many contigs in a batch")?;
            self.load(seq).map_err(contig)?;
            let mut stats: ProdigalStats = unsafe { std::mem::zeroed() };
            let mut soa: *mut ProdigalGenesSoa = ptr::null_mut();
            unsafe {
                let rc = prodigal_find_genes(self.0, &mut soa, &mut stats);
                self.check(rc).map_err(contig)?;
                // A successful call returns one, with or without genes
                if soa.is_null() {
                    return Err(contig("prodigal_find_genes returned no genes table".into()));
                }
                let appended = genes.append(seq_index, name, &*soa);
                prodigal_genes_free(soa);
                appended.map_err(contig)?;
            }
            table.append(seq_index, name, c_len(seq), &stats);
        }

        let (genes, table) = (genes.finish()?, table.finish()?);
        let mut result = serde_json::json!({
            "n_genes": genes.num_rows(),
            "n_sequences": table.num_rows(),
        });
        if let Some(model) = model {
            result["model"] = model_json(model);
        }
        let mut outputs = ShmOutputs::default();
        outputs.write(&genes, GENES.label)?;
        outputs.write(&table, CONTIGS.label)?;
        Ok(Response::ok(result, outputs.into_vec()))
    }
}

/// What a request's batches run on: execute() runs one, a streaming
/// session every batch of its config.
enum Session {
    /// One context for every batch: metagenomic mode (the bins are set up
    /// at the first contig and reused for every batch after), or
    /// single-genome mode with training_path's model, and its settings
    Shared(Context, Option<ProdigalTrainingInfo>),
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
        let info = match &settings.model {
            None => None,
            Some(model) => {
                let path = model.path.display();
                let info = ctx.load_model(model);
                Some(info.map_err(|e| format!("training_path {path}: {e}"))?)
            }
        };
        Ok(Self::Shared(ctx, info))
    }

    fn run(&mut self, contigs: &Contigs) -> Result<Response, String> {
        match self {
            Self::Shared(ctx, model) => ctx.predict(contigs, model.as_ref()),
            Self::TrainOnBatch(config) => {
                let mut ctx = Context::new(config)?;
                ctx.train(contigs)?;
                let model = training_info(&ctx.export()?)?;
                ctx.predict(contigs, Some(&model))
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

    /// 2: genes gains seq_index, gene_index, score and protein, and its
    /// rbs_motif / rbs_spacer are NULL for "None"; the contigs output.
    fn schema_version(&self) -> u32 {
        2
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
            // 5: schema 2 (outputs genes and contigs, listed by label); the
            // result's model replaces the last contig's stats.
            describe_version: 5,
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
                    description: "The SHA-256 of training_path's model, in lowercase \
                                  hexadecimal as prodigal-train reports it. A worker loads the file only \
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
            input_schema: INPUT_FIELDS.to_vec(),
            outputs: vec![GENES, CONTIGS],
            response_metadata: vec![
                FieldDescription {
                    name: "n_genes",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Genes in the batch (rows of genes)",
                },
                FieldDescription {
                    name: "n_sequences",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Contigs in the batch (rows of contigs)",
                },
                FieldDescription {
                    name: "model",
                    arrow_type: "object",
                    nullable: true,
                    description: "Single-genome mode: the model the genes were called \
                                      with (training_path's, or the one trained on the \
                                      batch); absent in metagenomic mode",
                },
            ]
            .into_iter()
            .chain(model_fields(true))
            .collect(),
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
    use crate::test_util::{
        anthus, anthus_batch, assert_anthus_model, column, contigs_batch, fasta_records,
        gene_coordinates, gff_coordinates, gff_genes, gff_sequences, prediction_of, prodigal_batch,
        prodigal_truth, scratch_dir, unique_shm_name, write_arrow_to_shm, Prediction,
        PRODIGAL_REF_TRAIN as REF_TRAIN, PRODIGAL_REF_TRAIN_SHA256 as REF_TRAIN_SHA256,
    };
    use arrow::array::{BooleanArray, Float64Array, Int32Array};
    use arrow::compute::{filter_record_batch, kernels::cmp::eq};
    use arrow::datatypes::{DataType, Field, Schema};

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

    /// execute() on a batch: the result JSON and the outputs, or the error.
    fn predict_batch(config: serde_json::Value, batch: &RecordBatch) -> Result<Prediction, String> {
        let input_name = unique_shm_name("pd-run");
        let input = write_arrow_to_shm(&input_name, batch);
        prediction_of(ProdigalTool.execute(&config, &input_name, input.len()))
    }

    /// A batch run through a streaming session: the result JSON and the
    /// outputs, or the error.
    fn predict_in(
        session: &mut Box<dyn StreamingContext>,
        batch: &RecordBatch,
    ) -> Result<Prediction, String> {
        let input_name = unique_shm_name("pd-sess");
        let input = write_arrow_to_shm(&input_name, batch);
        prediction_of(session.run_batch(&input_name, input.len()))
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
    ) -> Result<Prediction, String> {
        predict_batch(config, &prodigal_batch(names, sequences))
    }

    /// anthus's genes and contigs in metagenomic mode, trained on the batch,
    /// and from the CLI's model of anthus, with the CLI's output for each:
    /// (config, GFF, proteins).
    fn cli_runs() -> [(serde_json::Value, &'static str, &'static str); 3] {
        let trained = serde_json::json!({"meta_mode": false, "train_on_batch": true});
        [
            (serde_json::json!({}), "ref_meta.gff", "ref_meta.proteins"),
            (trained, "ref_single.gff", "ref_single.proteins"),
            // ref_trained.gff's genes are ref_single.gff's
            (
                from_model(serde_json::json!({})),
                "ref_trained.gff",
                "ref_single.proteins",
            ),
        ]
    }

    /// A batch's outputs (prediction_of checks their labels and order) are
    /// the ones --describe lists, with its columns: name, type as
    /// described, nullability.
    #[test]
    fn test_outputs_are_described() {
        let batch = prodigal_batch(&["contig_1", "contig_2"], &[TEST_SEQ_1, TEST_SEQ_2]);
        let outputs = predict_batch(serde_json::json!({}), &batch).unwrap();
        let desc = ProdigalTool.describe();
        let described: Vec<&str> = desc.outputs.iter().map(|o| o.label).collect();
        assert_eq!(described, ["genes", "contigs"]);
        let column = |f: &Field| (f.name().clone(), f.data_type().to_string(), f.is_nullable());
        for (batch, output) in [&outputs.genes, &outputs.contigs]
            .into_iter()
            .zip(&desc.outputs)
        {
            let columns: Vec<_> = batch.schema().fields().iter().map(|f| column(f)).collect();
            let listed: Vec<_> = (output.schema.iter())
                .map(|f| (f.name.to_string(), f.arrow_type.to_string(), f.nullable))
                .collect();
            assert_eq!(columns, listed, "{}", output.label);
        }
        assert!(outputs.genes.num_rows() > 0);
        assert_eq!(outputs.contigs.num_rows(), 2);
    }

    /// The genes are the CLI's, in both modes, column by column: each GFF
    /// CDS line (ID = <seqnum>_<gene_index>, the place, partial,
    /// start_type, the RBS motif and spacer, NULL where the CLI prints
    /// "None", gc_cont, and the scores, score= being cscore + sscore), and
    /// its protein (the -a record).
    #[test]
    fn test_genes_are_the_clis() {
        const START_TYPES: [&str; 4] = ["ATG", "GTG", "TTG", "Edge"];
        // (column, GFF attribute, the CLI's printed precision)
        const VALUES: [(&str, &str, f64); 8] = [
            ("score", "score", 0.005),
            ("cscore", "cscore", 0.005),
            ("sscore", "sscore", 0.005),
            ("rscore", "rscore", 0.005),
            ("uscore", "uscore", 0.005),
            ("tscore", "tscore", 0.005),
            ("confidence", "conf", 0.005),
            ("gc_cont", "gc_cont", 0.0005),
        ];
        for (config, gff, proteins) in cli_runs() {
            let Prediction { result, genes, .. } = predict_batch(config, &anthus_batch()).unwrap();
            let expected = gff_genes(&prodigal_truth(gff));
            assert_eq!(result["n_genes"], expected.len(), "{gff}");
            let proteins = fasta_records(&prodigal_truth(proteins));
            assert_eq!(genes.num_rows(), expected.len(), "{gff}");
            assert_eq!(proteins.len(), expected.len(), "{gff}");

            let int = |name| column::<Int32Array>(&genes, name).values();
            let flag = |name, i| column::<BooleanArray>(&genes, name).value(i);
            let float = |name| column::<Float64Array>(&genes, name).values();
            let text = |name| column::<StringArray>(&genes, name);
            let mut nulls = 0;
            for (i, (gene, (_, letters))) in expected.iter().zip(&proteins).enumerate() {
                let at = format!("{gff} gene {i}");
                let attr = |name: &str| gene.attributes[name].as_str();
                let id = format!("{}_{}", int("seq_index")[i], int("gene_index")[i]);
                assert_eq!(id, attr("ID"), "{at}");
                assert_eq!(text("seq_name").value(i), gene.seq_name, "{at}");
                let place = (int("begin")[i], int("end")[i], int("strand")[i]);
                assert_eq!(place, (gene.begin, gene.end, gene.strand), "{at}");
                let partial = ["partial_left", "partial_right"].map(|p| u8::from(flag(p, i)));
                assert_eq!(
                    format!("{}{}", partial[0], partial[1]),
                    attr("partial"),
                    "{at}"
                );
                let start_type = START_TYPES[int("start_type")[i] as usize];
                assert_eq!(start_type, attr("start_type"), "{at}");
                for (name, attribute, precision) in VALUES {
                    let cli: f64 = attr(attribute).parse().unwrap();
                    let diff = (float(name)[i] - cli).abs();
                    assert!(diff <= precision + 1e-9, "{at} {name}: {diff}");
                }
                for name in ["rbs_motif", "rbs_spacer"] {
                    let col = text(name);
                    let ours = (!col.is_null(i)).then(|| col.value(i));
                    let cli = Some(attr(name)).filter(|&v| v != "None");
                    assert_eq!(ours, cli, "{at} {name}");
                    nulls += usize::from(col.is_null(i));
                }
                assert_eq!(text("protein").value(i), letters, "{at}");
            }
            assert!(nulls > 0, "{gff}: no \"None\" to check");
        }
    }

    /// A negative gene count, or protein offsets that don't start at 0 or
    /// go backwards, are an error, not no genes or a slice out of bounds.
    #[test]
    fn test_malformed_genes_are_an_error() {
        let (ints, floats) = ([1i32; 2], [0.0f64; 2]);
        let none = c"None".as_ptr();
        let texts = [none; 2];
        let letters = b"MKV*MK*";
        let soa = |offsets: &[i32; 3]| ProdigalGenesSoa {
            n_genes: 2,
            begin: ints.as_ptr(),
            end: ints.as_ptr(),
            strand: ints.as_ptr(),
            partial_left: ints.as_ptr(),
            partial_right: ints.as_ptr(),
            start_type: ints.as_ptr(),
            cscore: floats.as_ptr(),
            sscore: floats.as_ptr(),
            rscore: floats.as_ptr(),
            uscore: floats.as_ptr(),
            tscore: floats.as_ptr(),
            confidence: floats.as_ptr(),
            gc_cont: floats.as_ptr(),
            rbs_motif: texts.as_ptr(),
            rbs_spacer: texts.as_ptr(),
            protein_offsets: offsets.as_ptr(),
            protein_data: letters.as_ptr().cast(),
            _base: ptr::null_mut(),
        };
        let append =
            |offsets: &[i32; 3]| unsafe { GeneColumns::default().append(1, "c", &soa(offsets)) };
        assert!(append(&[0, 4, 7]).is_ok());
        for bad in [[1, 4, 7], [0, 4, 3], [0, -4, 7]] {
            let err = append(&bad).unwrap_err();
            assert!(err.contains("protein offsets"), "{bad:?}: {err}");
        }
        let negative = ProdigalGenesSoa {
            n_genes: -1,
            ..soa(&[0, 4, 7])
        };
        let err = unsafe { GeneColumns::default().append(1, "c", &negative) }.unwrap_err();
        assert!(err.contains("gene count"), "{err}");
    }

    /// One contigs row per input contig, in order, with the CLI's
    /// sequence and model data (its GFF header lines), and the genes the
    /// contig has.
    #[test]
    fn test_contigs_are_the_clis() {
        let input = anthus();
        for (config, gff, _) in cli_runs() {
            let contigs = predict_batch(config, &anthus_batch()).unwrap().contigs;
            let truth = prodigal_truth(gff);
            let (expected, genes) = (gff_sequences(&truth), gff_genes(&truth));
            assert_eq!(contigs.num_rows(), expected.len(), "{gff}");

            let int = |name| column::<Int32Array>(&contigs, name);
            let names = column::<StringArray>(&contigs, "seq_name");
            let gc = column::<Float64Array>(&contigs, "gc_content");
            let uses_sd = column::<BooleanArray>(&contigs, "uses_sd");
            let (bin, desc) = (
                int("meta_bin"),
                column::<StringArray>(&contigs, "model_desc"),
            );
            for (i, cli) in expected.iter().enumerate() {
                let at = format!("{gff} contig {i}");
                assert_eq!(int("seq_index").value(i).to_string(), cli["seqnum"], "{at}");
                // anthus_aco.fas has CRLF lines; the CLI keeps the CR
                assert_eq!(names.value(i), cli["seqhdr"].trim_end(), "{at}");
                assert_eq!(int("length").value(i).to_string(), cli["seqlen"], "{at}");
                let table = int("translation_table").value(i).to_string();
                assert_eq!(table, cli["transl_table"], "{at}");
                assert_eq!(uses_sd.value(i), cli["uses_sd"] == "1", "{at}");
                let n_genes = genes
                    .iter()
                    .filter(|g| g.seq_name == names.value(i))
                    .count();
                assert_eq!(int("n_genes").value(i) as usize, n_genes, "{at}");
                if cli["run_type"] == "Metagenomic" {
                    assert_eq!(desc.value(i), cli["model"], "{at}");
                    let number = cli["model"].split('|').next().unwrap();
                    assert_eq!(bin.value(i).to_string(), number, "{at}");
                } else {
                    assert!(bin.is_null(i) && desc.is_null(i), "{at}");
                }
                // The sequence's GC (the GFF has the model's)
                let seq = input[i].1.as_bytes();
                let n_gc = seq.iter().filter(|b| b"GCgc".contains(b)).count();
                let want = n_gc as f64 / seq.len() as f64;
                assert!((gc.value(i) - want).abs() < 1e-12, "{at}");
            }
        }
    }

    /// A contig without genes still has its contigs row, and the genes'
    /// seq_index joins each gene to its contig's row, within each batch.
    #[test]
    fn test_contigs_include_contigs_without_genes() {
        let names = ["c1", "tiny", "c2"];
        let batch = prodigal_batch(&names, &[TEST_SEQ_1, "ACGTACGTAC", TEST_SEQ_2]);
        let tiny = prodigal_batch(&["tiny"], &["ACGTACGTAC"]);
        for config in [serde_json::json!({}), from_model(serde_json::json!({}))] {
            let mut session = ProdigalTool
                .create_streaming_context(&config)
                .unwrap()
                .unwrap();
            for (batch, names) in [(&batch, &names[..]), (&tiny, &names[1..2])] {
                let outputs = predict_in(&mut session, batch).unwrap();
                let (genes, contigs) = (&outputs.genes, &outputs.contigs);
                let at = format!("{config} {names:?}");
                assert_eq!(contigs.num_rows(), names.len(), "{at}");
                let n_genes = column::<Int32Array>(contigs, "n_genes");
                let seq_index = column::<Int32Array>(contigs, "seq_index");
                let gene_seq = column::<Int32Array>(genes, "seq_index");
                let seq_names = column::<StringArray>(genes, "seq_name");
                for (i, name) in names.iter().enumerate() {
                    let index = seq_index.value(i);
                    assert_eq!(index as usize, i + 1, "{at}");
                    let of_contig: Vec<_> = (0..genes.num_rows())
                        .filter(|&g| gene_seq.value(g) == index)
                        .collect();
                    assert_eq!(of_contig.len(), n_genes.value(i) as usize, "{at} {name}");
                    assert!(of_contig.iter().all(|&g| seq_names.value(g) == *name));
                    assert_eq!(n_genes.value(i) == 0, *name == "tiny", "{at} {name}");
                }
                assert_eq!(outputs.result["n_sequences"], names.len(), "{at}");
                assert_eq!(outputs.result["n_genes"], genes.num_rows(), "{at}");
            }
        }
    }

    /// A contig whose start/stop nodes outgrow the library's node array (an
    /// ATG repeat: one node per codon, past the 4096-node floor and the
    /// slen/8 budget, so the array doubles and shrinks back) changes no
    /// other contig's genes, in the batch or after it, in either mode. The
    /// model run uses Prodigal 2.6.4's scale (110,000 nodes, past the
    /// 100,000 it held and overflowed); it costs 0.2 s there.
    #[test]
    fn test_low_complexity_contig_changes_no_other_genes() {
        let alone = prodigal_batch(&["c1"], &[TEST_SEQ_1]);
        // A contig's genes, every column but seq_index (c1's is 2 after the
        // repeat)
        let genes_of = |genes: &RecordBatch, name: &str| {
            let names = column::<StringArray>(genes, "seq_name");
            let mask = eq(names, &StringArray::new_scalar(name)).unwrap();
            let mut genes = filter_record_batch(genes, &mask).unwrap();
            genes.remove_column(genes.schema().index_of("seq_index").unwrap());
            genes
        };
        for (config, codons) in [
            (serde_json::json!({}), 12_000),
            (from_model(serde_json::json!({})), 110_000),
        ] {
            let repeat = "ATG".repeat(codons);
            let after = prodigal_batch(&["atg", "c1"], &[&repeat, TEST_SEQ_1]);
            let mut session = ProdigalTool
                .create_streaming_context(&config)
                .unwrap()
                .unwrap();
            let before = predict_in(&mut session, &alone).unwrap().genes;
            let mixed = predict_in(&mut session, &after).unwrap();
            let again = predict_in(&mut session, &alone).unwrap().genes;
            let expected = genes_of(&before, "c1");
            assert!(expected.num_rows() > 0, "{config}");
            assert_eq!(genes_of(&mixed.genes, "c1"), expected, "{config}");
            assert_eq!(genes_of(&again, "c1"), expected, "{config}");
            let n_genes = column::<Int32Array>(&mixed.contigs, "n_genes");
            let atg_genes = genes_of(&mixed.genes, "atg").num_rows();
            assert_eq!(n_genes.value(0) as usize, atg_genes, "{config}");
        }
    }

    /// Each contig reports the table its genes were called with: the
    /// model's.
    #[test]
    fn test_contigs_report_the_table() {
        let config =
            serde_json::json!({"meta_mode": false, "train_on_batch": true, "trans_table": 4});
        let outputs = predict_batch(config, &anthus_batch()).unwrap();
        let tables = column::<Int32Array>(&outputs.contigs, "translation_table");
        assert_eq!(tables.len(), 22);
        assert!(tables.iter().all(|t| t == Some(4)), "{tables:?}");
        assert_eq!(outputs.result["model"]["translation_table"], 4);
    }

    /// The result reports the batch's counts, and in single-genome mode the
    /// model's settings; no contig's stats.
    #[test]
    fn test_result_reports_the_model() {
        let meta = predict_batch(serde_json::json!({}), &anthus_batch())
            .unwrap()
            .result;
        let keys: Vec<&String> = meta.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["n_genes", "n_sequences"]);

        // anthus's model, from the file or trained on the batch
        let from_file = predict_batch(from_model(serde_json::json!({})), &anthus_batch());
        let trained = serde_json::json!({"meta_mode": false, "train_on_batch": true});
        let trained = predict_batch(trained, &anthus_batch());
        for result in [from_file.unwrap().result, trained.unwrap().result] {
            assert_anthus_model(&result["model"]);
            let keys: Vec<&String> = result.as_object().unwrap().keys().collect();
            assert_eq!(keys, ["model", "n_genes", "n_sequences"]);
        }
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

        let batch = prodigal_batch(&["contig_1", "contig_2"], &[TEST_SEQ_1, TEST_SEQ_2]);
        let outputs = predict_in(&mut ctx, &batch).unwrap();
        assert!(outputs.result["n_genes"].as_i64().unwrap() > 0);
        assert_eq!(outputs.result["n_sequences"], 2);
        assert!(outputs.genes.num_rows() > 0);
    }

    #[test]
    fn test_prodigal_streaming_two_batches() {
        let tool = ProdigalTool;
        let config = serde_json::json!({"meta_mode": true});
        let mut ctx = tool
            .create_streaming_context(&config)
            .expect("context creation failed")
            .expect("tool should support streaming");

        // Both batches find genes independently
        for (name, seq) in [("contig_1", TEST_SEQ_1), ("contig_2", TEST_SEQ_2)] {
            let outputs = predict_in(&mut ctx, &prodigal_batch(&[name], &[seq])).unwrap();
            assert!(outputs.genes.num_rows() > 0, "{name} found no genes");
        }
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
    /// batch may hold part of a genome. With train_on_batch the session
    /// trains a fresh context on each batch.
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
            let wanted = format!("{c:?} at position {} is outside 'A'..'z'", at + 1);
            assert!(err.contains("'c2'") && err.contains(&wanted), "{err}");
        }
        // The context is still usable
        predict_in(&mut meta, &prodigal_batch(&["c1"], &[TEST_SEQ_1])).unwrap();

        // Single-genome mode
        let mut contigs = anthus();
        contigs[5].1 = with(&contigs[5].1, 42, '.');
        let config = serde_json::json!({"meta_mode": false, "train_on_batch": true});
        let err = predict_batch(config, &contigs_batch(&contigs)).unwrap_err();
        let wanted = format!("'{}'", contigs[5].0);
        assert!(
            err.contains(&wanted) && err.contains("'.' at position 43"),
            "{err}"
        );

        let kept = TEST_SEQ_1.replace("ACAG", "acRN").replace("GGTT", "[_^Y");
        let result = predict(serde_json::json!({}), &["c1"], &[&kept]);
        assert!(result.is_ok(), "{:?}", result.err());
    }

    /// A contig with an empty sequence is refused, naming it, on both
    /// routes: Prodigal finds nothing in it (the CLI stops on one), and
    /// training would count it but add only a spacer.
    #[test]
    fn test_empty_contig_is_refused() {
        let mut contigs = anthus();
        contigs.insert(3, ("empty".into(), String::new()));
        let batch = contigs_batch(&contigs);
        for config in [
            serde_json::json!({}),
            serde_json::json!({"meta_mode": false, "train_on_batch": true}),
        ] {
            let err = predict_batch(config.clone(), &batch).unwrap_err();
            assert!(
                err.contains("'empty' (input row 4): empty sequence"),
                "{config}: {err}"
            );
        }
    }

    /// Contigs::read refuses what the library drops. Should the library's
    /// rule change, a contig it doesn't keep whole is an error, when
    /// predicting and when training, not shifted coordinates or a model of
    /// other bases.
    #[test]
    fn test_kept_length_is_checked() {
        let config = serde_json::json!({"meta_mode": false, "train_on_batch": true});
        let mut ctx = Context::new(&Settings::parse(&config).unwrap().config).unwrap();
        let dropped = format!("{TEST_SEQ_2}-");
        let err = ctx.load(&dropped).unwrap_err();
        assert!(err.contains("kept"), "{err}");

        // As Contigs::read would never return them
        let mut contigs = anthus();
        contigs[5].1.push('-');
        let names = StringArray::from_iter_values(contigs.iter().map(|c| &c.0));
        let seqs = StringArray::from_iter_values(contigs.iter().map(|c| &c.1));
        let err = ctx.train(&Contigs(vec![(names, seqs)])).unwrap_err();
        assert!(err.contains("kept"), "{err}");
    }

    /// Each contig is read in place, within its own bounds: a contig's
    /// genes are the same alone and followed by another in the batch.
    #[test]
    fn test_contigs_are_read_within_bounds() {
        let config = serde_json::json!({});
        let alone = predict(config.clone(), &["c1"], &[TEST_SEQ_1])
            .unwrap()
            .genes;
        let both = predict(config, &["c1", "c2"], &[TEST_SEQ_1, TEST_SEQ_2]);
        let both = both.unwrap().genes;
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

    /// From a model file, a session loads the model once and predicts every
    /// batch from it, as the CLI (ref_trained.gff: `prodigal -t
    /// ref_train.bin` on anthus): half of anthus is too short to train on.
    /// (test_genes_are_the_clis checks a whole-file batch column by column.)
    #[test]
    fn test_training_path_predicts_as_the_cli() {
        let expected = gff_coordinates(&prodigal_truth("ref_trained.gff"));
        let batch = anthus_batch();
        let config = from_model(serde_json::json!({}));
        let mut session = ProdigalTool
            .create_streaming_context(&config)
            .unwrap()
            .unwrap();
        let mut got = Vec::new();
        for half in [batch.slice(0, 11), batch.slice(11, 11)] {
            let genes = predict_in(&mut session, &half).unwrap().genes;
            got.extend(gene_coordinates(&genes));
        }
        assert_eq!(got, expected);
    }

    /// training_path needs training_sha256, the file's, spelt as
    /// prodigal-train reports it (lowercase: one model, one worker), so a
    /// model retrained in place is refused rather than served.
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
        let err = refused(from_model(serde_json::json!({"training_sha256": upper})));
        assert!(err.contains("lowercase"), "{err}");
    }

    /// A model file that cannot be read, is not a regular file (refused
    /// unopened: a FIFO doesn't block), is too large to be a model (read no
    /// further: a sparse terabyte is refused at once) or that the library
    /// refuses is an error naming the path.
    #[test]
    fn test_training_path_refused_model() {
        let dir = scratch_dir();
        let short = dir.path().join("short.bin");
        let bytes = std::fs::read(REF_TRAIN).unwrap();
        std::fs::write(&short, &bytes[..1000]).unwrap();
        let sha = sha256_hex(&bytes[..1000]);
        let large = dir.path().join("large.bin");
        std::fs::File::create(&large)
            .unwrap()
            .set_len(1 << 40)
            .unwrap();
        let missing = dir.path().join("missing.bin");
        let not_a_file = dir.path().to_path_buf();
        let fifo = dir.path().join("fifo.bin");
        let c_fifo = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_fifo.as_ptr(), 0o600) }, 0);
        let endless = PathBuf::from("/dev/zero");
        for path in [&short, &large, &missing, &not_a_file, &fifo, &endless] {
            let config =
                from_model(serde_json::json!({"training_path": path, "training_sha256": sha}));
            let err = ProdigalTool.create_streaming_context(&config).err();
            let err = err.expect("refused");
            assert!(err.contains(path.to_str().unwrap()), "{err}");
            let not_a_model = [&large, &not_a_file, &fifo, &endless].contains(&path);
            assert_eq!(err.contains("not a Prodigal model"), not_a_model, "{err}");
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
