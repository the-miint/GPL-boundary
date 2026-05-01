use std::ffi::{CStr, CString};
use std::io::Write;
use std::os::raw::{c_char, c_int, c_void};

use arrow::array::StringArray;

use crate::protocol::Response;
use crate::tools::{ConfigParam, FieldDescription, GplTool, ToolDescription, ToolRegistration};

// ---------------------------------------------------------------------------
// FFI bindings — mirror ext/bowtie2/bt2_api.h (build API)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct Bt2BuildConfig {
    pub struct_size: usize,
    pub ref_paths: *const *const c_char,
    pub n_ref_paths: usize,
    pub output_base: *const c_char,
    pub nthreads: c_int,
    pub seed: i64,
    pub offrate: c_int,
    pub packed: c_int,
    pub quiet: c_int,
    pub log_fn: Option<unsafe extern "C" fn(*mut c_void, c_int, *const c_char)>,
    pub log_user_data: *mut c_void,
}

#[repr(C)]
pub struct Bt2BuildStats {
    pub elapsed_ms: i64,
}

#[allow(non_camel_case_types)]
pub type bt2_build_ctx_t = c_void;

extern "C" {
    pub fn bt2_build_config_init(config: *mut Bt2BuildConfig);
    pub fn bt2_build_create(
        config: *const Bt2BuildConfig,
        error_out: *mut c_int,
    ) -> *mut bt2_build_ctx_t;
    pub fn bt2_build_run(ctx: *mut bt2_build_ctx_t, stats_out: *mut Bt2BuildStats) -> c_int;
    pub fn bt2_build_destroy(ctx: *mut bt2_build_ctx_t);
    pub fn bt2_build_last_error(ctx: *const bt2_build_ctx_t) -> *const c_char;
}

use crate::tools::bowtie2_ffi::{bt2_strerror, BT2_OK, BT2_VERSION};

const INDEX_SUFFIXES_SMALL: &[&str] = &[
    ".1.bt2",
    ".2.bt2",
    ".3.bt2",
    ".4.bt2",
    ".rev.1.bt2",
    ".rev.2.bt2",
];

const INDEX_SUFFIXES_LARGE: &[&str] = &[
    ".1.bt2l",
    ".2.bt2l",
    ".3.bt2l",
    ".4.bt2l",
    ".rev.1.bt2l",
    ".rev.2.bt2l",
];

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
// Tool struct + registration
// ---------------------------------------------------------------------------

pub struct Bowtie2BuildTool;

inventory::submit! {
    ToolRegistration {
        create: || Box::new(Bowtie2BuildTool),
    }
}

// ---------------------------------------------------------------------------
// Input reading + FASTA materialization
// ---------------------------------------------------------------------------

struct BuildInput {
    names: Vec<String>,
    seqs: Vec<String>,
    n_bases: i64,
}

fn read_input(shm_input: &str, shm_input_size: usize) -> Result<BuildInput, String> {
    let batches = crate::arrow_ipc::read_batches_from_shm(shm_input, shm_input_size)?;
    let mut names = Vec::new();
    let mut seqs = Vec::new();
    let mut n_bases: i64 = 0;
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
            let n = name_col.value(i);
            let s = seq_col.value(i);
            // FASTA convention is that bowtie2-build splits names on the
            // first whitespace character — `>chr1 dna:primary` becomes
            // reference name `chr1` in the index, silently dropping the
            // suffix. That would make the names emitted by bowtie2-align
            // differ from what miint sent in, with no error to point at.
            // Reject *any* whitespace (and '\n' / '>' which would corrupt
            // the FASTA record boundary) up front so the failure mode is
            // a clear error rather than silent name truncation.
            if n.is_empty() {
                return Err(format!("Sequence name {i} is empty"));
            }
            if n.chars().any(|c| c.is_whitespace()) || n.contains('>') {
                return Err(format!(
                    "Sequence name {i} contains whitespace or '>'; bowtie2 \
                     would silently truncate it (FASTA name = first token). \
                     Strip these before submitting.",
                ));
            }
            if s.contains('\n') || s.contains('\r') || s.contains('>') {
                return Err(format!(
                    "Sequence body {i} contains a newline or '>' character"
                ));
            }
            n_bases += s.len() as i64;
            names.push(n.to_string());
            seqs.push(s.to_string());
        }
    }
    Ok(BuildInput {
        names,
        seqs,
        n_bases,
    })
}

fn write_fasta(path: &std::path::Path, input: &BuildInput) -> Result<(), String> {
    let mut f =
        std::fs::File::create(path).map_err(|e| format!("Failed to create FASTA tempfile: {e}"))?;
    for (n, s) in input.names.iter().zip(input.seqs.iter()) {
        writeln!(f, ">{n}").map_err(|e| format!("Failed to write FASTA: {e}"))?;
        // Wrap at 80 columns to match conventional FASTA line length;
        // bowtie2-build doesn't care, but it keeps the tempfile readable
        // when debugging build failures.
        for chunk in s.as_bytes().chunks(80) {
            f.write_all(chunk)
                .map_err(|e| format!("Failed to write FASTA: {e}"))?;
            f.write_all(b"\n")
                .map_err(|e| format!("Failed to write FASTA: {e}"))?;
        }
    }
    f.flush()
        .map_err(|e| format!("Failed to flush FASTA: {e}"))?;
    Ok(())
}

fn collect_index_files(index_path: &str) -> Vec<String> {
    let mut files: Vec<String> = INDEX_SUFFIXES_SMALL
        .iter()
        .chain(INDEX_SUFFIXES_LARGE.iter())
        .filter_map(|sfx| {
            let p = format!("{index_path}{sfx}");
            if std::path::Path::new(&p).exists() {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    files.sort();
    files
}

// ---------------------------------------------------------------------------
// GplTool implementation
// ---------------------------------------------------------------------------

impl GplTool for Bowtie2BuildTool {
    fn name(&self) -> &str {
        "bowtie2-build"
    }

    fn version(&self) -> String {
        BT2_VERSION.to_string()
    }

    fn schema_version(&self) -> u32 {
        1
    }

    fn describe(&self) -> ToolDescription {
        ToolDescription {
            name: "bowtie2-build",
            version: self.version(),
            schema_version: self.schema_version(),
            describe_version: 1,
            description: "Build a bowtie2 index from in-memory FASTA-style sequences",
            config_params: vec![
                ConfigParam {
                    name: "index_path",
                    param_type: "string",
                    default: serde_json::json!(null),
                    description: "Output index basename (required); .bt2 files written here",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "nthreads",
                    param_type: "integer",
                    default: serde_json::json!(1),
                    description: "Number of build threads",
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
                    name: "offrate",
                    param_type: "integer",
                    default: serde_json::json!(4),
                    description: "Suffix-array sampling rate: 1 in 2^N",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "packed",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Use packed strings (less RAM, slower build)",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "verbose",
                    param_type: "boolean",
                    default: serde_json::json!(false),
                    description: "Write build log to stderr",
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
                    description: "DNA sequence",
                },
            ],
            // bowtie2-build emits index files on disk, not Arrow output.
            output_schema: vec![],
            response_metadata: vec![
                FieldDescription {
                    name: "elapsed_ms",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Wall-clock build time",
                },
                FieldDescription {
                    name: "n_sequences",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Sequences in input",
                },
                FieldDescription {
                    name: "n_bases",
                    arrow_type: "integer",
                    nullable: false,
                    description: "Total bases in input",
                },
                FieldDescription {
                    name: "index_files",
                    arrow_type: "array<string>",
                    nullable: false,
                    description: "Paths of .bt2 / .bt2l files written",
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
        let index_path = match config.get("index_path").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => {
                return Response::error("index_path is required (output basename for .bt2 files)");
            }
        };

        let verbose = config
            .get("verbose")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let input = match read_input(shm_input, shm_input_size) {
            Ok(v) => v,
            Err(e) => return Response::error(e),
        };

        if input.names.is_empty() {
            return Response::error("At least 1 sequence required");
        }
        let n_seqs = input.names.len() as i64;
        let n_bases = input.n_bases;

        // Materialize a tempfile FASTA. Suboptimal: bowtie2's C builder API
        // only accepts disk paths today (`ref_paths`). Phase 6 adds an
        // in-memory path — see GUIDANCE_BT2_BUILD_API.md.
        let tmpfile = match tempfile::Builder::new()
            .prefix("gb-bt2-build-")
            .suffix(".fa")
            .tempfile()
        {
            Ok(f) => f,
            Err(e) => return Response::error(format!("Failed to create FASTA tempfile: {e}")),
        };
        if let Err(e) = write_fasta(tmpfile.path(), &input) {
            return Response::error(e);
        }

        let ref_path_c = match CString::new(tmpfile.path().to_string_lossy().as_ref()) {
            Ok(c) => c,
            Err(_) => return Response::error("FASTA tempfile path contains a null byte"),
        };
        let output_base_c = match CString::new(index_path.as_str()) {
            Ok(c) => c,
            Err(_) => return Response::error("index_path contains a null byte"),
        };
        let ref_ptrs: [*const c_char; 1] = [ref_path_c.as_ptr()];

        // SAFETY: `bt2.ref_paths`, `bt2.output_base`, and the `ref_ptrs`
        // array contain raw pointers into `ref_path_c`, `output_base_c`, and
        // `ref_ptrs` itself. All three bindings are owned by this function
        // and live through the entire `unsafe` block (no early move, no
        // shadowing), so the pointers remain valid for the duration of every
        // FFI call below. `tmpfile` is similarly held until end-of-scope so
        // the FASTA file referenced by `ref_path_c` is not unlinked
        // prematurely. If you refactor any of these out of this function,
        // bundle them so the lifetime invariant stays visible.
        let stats = unsafe {
            let mut bt2: Bt2BuildConfig = std::mem::zeroed();
            bt2_build_config_init(&mut bt2);
            bt2.ref_paths = ref_ptrs.as_ptr();
            bt2.n_ref_paths = 1;
            bt2.output_base = output_base_c.as_ptr();

            if let Some(v) = config.get("nthreads").and_then(|v| v.as_i64()) {
                bt2.nthreads = v as c_int;
            }
            if let Some(v) = config.get("seed").and_then(|v| v.as_i64()) {
                bt2.seed = v;
            }
            if let Some(v) = config.get("offrate").and_then(|v| v.as_i64()) {
                bt2.offrate = v as c_int;
            }
            if let Some(v) = config.get("packed").and_then(|v| v.as_bool()) {
                bt2.packed = if v { 1 } else { 0 };
            }

            bt2.quiet = 1;
            if verbose {
                bt2.log_fn = Some(stderr_log_callback);
            }

            let mut error_code: c_int = 0;
            let ctx = bt2_build_create(&bt2, &mut error_code);
            if ctx.is_null() {
                let cat = CStr::from_ptr(bt2_strerror(error_code))
                    .to_string_lossy()
                    .into_owned();
                return Response::error(format!("Failed to create bowtie2 build context: {cat}"));
            }

            let mut stats: Bt2BuildStats = std::mem::zeroed();
            let rc = bt2_build_run(ctx, &mut stats);
            if rc != BT2_OK {
                let cat = CStr::from_ptr(bt2_strerror(rc))
                    .to_string_lossy()
                    .into_owned();
                let detail = CStr::from_ptr(bt2_build_last_error(ctx))
                    .to_string_lossy()
                    .into_owned();
                bt2_build_destroy(ctx);
                return Response::error(format!("{cat}: {detail}"));
            }

            bt2_build_destroy(ctx);
            stats
        };

        // bt2_build_run can report success while having written zero (or
        // partial) index files — e.g. if the output directory is unwritable
        // and the C code's error reporting misses it. A complete index is
        // always 6 files of one variant; anything less is a real failure
        // that we must surface so miint doesn't try to align against a
        // non-existent index.
        let index_files = collect_index_files(&index_path);
        let small_count = INDEX_SUFFIXES_SMALL
            .iter()
            .filter(|s| index_files.iter().any(|f| f.ends_with(*s)))
            .count();
        let large_count = INDEX_SUFFIXES_LARGE
            .iter()
            .filter(|s| index_files.iter().any(|f| f.ends_with(*s)))
            .count();
        if small_count != INDEX_SUFFIXES_SMALL.len() && large_count != INDEX_SUFFIXES_LARGE.len() {
            return Response::error(format!(
                "bowtie2-build returned success but only {} of {} index files \
                 were found at {} (found: {:?}). The index is incomplete.",
                index_files.len(),
                INDEX_SUFFIXES_SMALL.len(),
                index_path,
                index_files,
            ));
        }

        let result = serde_json::json!({
            "elapsed_ms": stats.elapsed_ms,
            "n_sequences": n_seqs,
            "n_bases": n_bases,
            "index_files": index_files,
        });
        Response::ok(result, vec![])
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{unique_shm_name, write_arrow_to_shm};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_build_config_struct_abi_size() {
        let mut config: Bt2BuildConfig = unsafe { std::mem::zeroed() };
        unsafe { bt2_build_config_init(&mut config) };
        assert_eq!(
            config.struct_size,
            std::mem::size_of::<Bt2BuildConfig>(),
            "ABI mismatch: Rust Bt2BuildConfig ({} bytes) vs C bt2_build_config_t ({} bytes). \
             Check field types and padding against ext/bowtie2/bt2_api.h.",
            std::mem::size_of::<Bt2BuildConfig>(),
            config.struct_size,
        );
    }

    #[test]
    fn test_describe_lists_all_params() {
        let tool = Bowtie2BuildTool;
        let desc = tool.describe();
        assert_eq!(desc.name, "bowtie2-build");
        for required in &[
            "index_path",
            "nthreads",
            "seed",
            "offrate",
            "packed",
            "verbose",
        ] {
            assert!(
                desc.config_params.iter().any(|p| p.name == *required),
                "describe() missing config param '{required}'"
            );
        }
        assert!(desc.output_schema.is_empty(), "build emits no Arrow output");
    }

    #[test]
    fn test_missing_index_path() {
        // Validates that index_path is rejected before we touch the shm at
        // all — pass a name that doesn't exist, and rely on the validation
        // failing first. If the field-order in execute() is ever reshuffled
        // and shm is read before config validation, this test will start
        // failing with an "open shm" error and that's a deliberate signal
        // to put validation back in front.
        let tool = Bowtie2BuildTool;
        let resp = tool.execute(&serde_json::json!({}), "/never-created-bt2-shm", 0);
        assert!(!resp.success);
        assert!(resp.error.unwrap().contains("index_path"));
    }

    fn make_batch(names: &[&str], seqs: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("sequence", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(seqs.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_rejects_name_with_newline() {
        let shm = unique_shm_name("bt2b-bad-name-nl");
        let batch = make_batch(&["bad\nname"], &["ACGT"]);
        let holder = write_arrow_to_shm(&shm, &batch);
        let err = match read_input(&shm, holder.len()) {
            Err(e) => e,
            Ok(_) => panic!("Expected error for name containing newline"),
        };
        let _ = crate::shm::SharedMemory::unlink(&shm);
        assert!(err.contains("whitespace"), "Unexpected error: {err}");
    }

    #[test]
    fn test_rejects_name_with_space() {
        // bowtie2 silently truncates `chr1 dna:primary` to `chr1`. Detect
        // the bad input here so the user gets an explanation, not a quiet
        // mismatch between input names and aligned output names.
        let shm = unique_shm_name("bt2b-bad-name-sp");
        let batch = make_batch(&["chr1 dna:primary"], &["ACGT"]);
        let holder = write_arrow_to_shm(&shm, &batch);
        let err = match read_input(&shm, holder.len()) {
            Err(e) => e,
            Ok(_) => panic!("Expected error for name containing space"),
        };
        let _ = crate::shm::SharedMemory::unlink(&shm);
        assert!(err.contains("whitespace"), "Unexpected error: {err}");
        assert!(err.contains("truncate"), "Error should explain why: {err}");
    }

    #[test]
    fn test_rejects_empty_name() {
        let shm = unique_shm_name("bt2b-empty-name");
        let batch = make_batch(&[""], &["ACGT"]);
        let holder = write_arrow_to_shm(&shm, &batch);
        let err = match read_input(&shm, holder.len()) {
            Err(e) => e,
            Ok(_) => panic!("Expected error for empty name"),
        };
        let _ = crate::shm::SharedMemory::unlink(&shm);
        assert!(err.contains("empty"), "Unexpected error: {err}");
    }

    #[test]
    fn test_collect_index_files_returns_existing() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().join("idx");
        let base_str = base.to_string_lossy().into_owned();
        // Create three of the six expected files
        for sfx in &[".1.bt2", ".2.bt2", ".rev.1.bt2"] {
            std::fs::write(format!("{base_str}{sfx}"), b"x").unwrap();
        }
        let files = collect_index_files(&base_str);
        assert_eq!(files.len(), 3);
        assert!(files.iter().any(|f| f.ends_with(".1.bt2")));
        assert!(files.iter().any(|f| f.ends_with(".2.bt2")));
        assert!(files.iter().any(|f| f.ends_with(".rev.1.bt2")));
    }
}
