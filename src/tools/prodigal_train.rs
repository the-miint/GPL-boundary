//! prodigal-train: trains a single-genome Prodigal model on the input's
//! contigs (one genome) and writes it, as the CLI's -t does, for
//! prodigal's training_path.

use crate::protocol::Response;
use crate::tools::prodigal::{
    input_fields, library_version, model_config, sha256_hex, training_info, validate, Context,
    Contigs, TRANS_TABLES,
};
use crate::tools::{config_str, ConfigParam, FieldDescription, GplTool, ToolDescription};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Instant;

pub struct ProdigalTrainTool;

inventory::submit! {
    crate::tools::ToolRegistration {
        create: || Box::new(ProdigalTrainTool),
    }
}

impl GplTool for ProdigalTrainTool {
    fn name(&self) -> &str {
        "prodigal-train"
    }

    fn version(&self) -> String {
        library_version()
    }

    fn schema_version(&self) -> u32 {
        1
    }

    fn describe(&self) -> ToolDescription {
        let flag = |name, description| ConfigParam {
            name,
            param_type: "boolean",
            default: serde_json::json!(false),
            description,
            allowed_values: vec![],
        };
        let result = |name, arrow_type, description| FieldDescription {
            name,
            arrow_type,
            nullable: false,
            description,
        };
        ToolDescription {
            name: "prodigal-train",
            version: self.version(),
            schema_version: self.schema_version(),
            describe_version: 1,
            description: "Train a single-genome Prodigal model on the input's contigs (one \
                          genome) and write it for prodigal's training_path",
            config_params: vec![
                ConfigParam {
                    name: "training_path",
                    param_type: "string",
                    default: serde_json::Value::Null,
                    description: "Required: the file to write the model to (the CLI's -t \
                                  format), replaced atomically through any symlinks; its \
                                  directory must exist",
                    allowed_values: vec![],
                },
                ConfigParam {
                    name: "trans_table",
                    param_type: "integer",
                    default: serde_json::json!(11),
                    description: "NCBI translation table number the model uses (11: \
                                  bacteria, archaea, plant plastids)",
                    allowed_values: TRANS_TABLES.map(Into::into).to_vec(),
                },
                flag(
                    "closed_ends",
                    "Do not allow genes to run off sequence edges while training (not \
                     stored in the model: pass prodigal the same value)",
                ),
                flag(
                    "mask_regions",
                    "Mask runs of N's as unknown regions while training (not stored in \
                     the model: pass prodigal the same value)",
                ),
                flag(
                    "force_nonsd",
                    "Train the non-Shine-Dalgarno RBS finder whatever the genome",
                ),
                ConfigParam {
                    name: "start_weight",
                    param_type: "float",
                    default: serde_json::json!(4.35),
                    description: "Weight for start codon score, finite and > 0",
                    allowed_values: vec![],
                },
                flag("verbose", "Write tool log output to stderr for diagnostics"),
            ],
            input_schema: input_fields(),
            output_schema: vec![],
            response_metadata: vec![
                result(
                    "training_path",
                    "string",
                    "The model file written (canonical path), for prodigal",
                ),
                result(
                    "training_sha256",
                    "string",
                    "The model's SHA-256, for prodigal's training_sha256",
                ),
                result(
                    "translation_table",
                    "integer",
                    "The model's translation table",
                ),
                result(
                    "uses_sd",
                    "integer",
                    "1 if the model scores Shine-Dalgarno motifs, else 0",
                ),
                result("gc_content", "float64", "The model's GC fraction"),
                result("start_weight", "float64", "The model's start weight"),
                result(
                    "closed_ends",
                    "boolean",
                    "closed_ends as trained: pass prodigal the same",
                ),
                result(
                    "mask_regions",
                    "boolean",
                    "mask_regions as trained: pass prodigal the same",
                ),
                result("n_sequences", "integer", "Contigs trained on"),
                result("n_bases", "integer", "Bases trained on"),
                result("elapsed_ms", "integer", "Wall-clock time"),
            ],
        }
    }

    fn execute(
        &self,
        config: &serde_json::Value,
        shm_input: &str,
        shm_input_size: usize,
    ) -> Response {
        train(config, shm_input, shm_input_size).unwrap_or_else(Response::error)
    }
}

fn train(
    json: &serde_json::Value,
    shm_input: &str,
    shm_input_size: usize,
) -> Result<Response, String> {
    let started = Instant::now();
    let config = model_config(json, &ProdigalTrainTool.describe().config_params)?;
    let path = config_str(json, "training_path")?
        .ok_or("training_path is required: the file to write the model to")?;
    validate(&config)?;
    let target = model_target(Path::new(path))?;
    let contigs = Contigs::read(shm_input, shm_input_size)?;
    let mut ctx = Context::new(&config)?;
    ctx.train(&contigs)?;
    let blob = ctx.export()?;
    let info = training_info(&blob)?;
    write_atomically(&target, &blob).map_err(|e| format!("training_path {target}: {e}"))?;

    let n_bases: usize = contigs.iter().map(|(_, seq)| seq.len()).sum();
    let result = serde_json::json!({
        "training_path": target,
        "training_sha256": sha256_hex(&blob),
        "translation_table": info.translation_table,
        "uses_sd": info.uses_sd,
        "gc_content": info.gc_content,
        "start_weight": info.start_weight,
        "closed_ends": config.closed_ends != 0,
        "mask_regions": config.mask_regions != 0,
        "n_sequences": contigs.iter().count(),
        "n_bases": n_bases,
        "elapsed_ms": started.elapsed().as_millis() as u64,
    });
    Ok(Response::ok(result, vec![]))
}

/// Where training_path's model goes, found before any training: the file
/// its symlinks lead to (the CLI writes through them), in a directory that
/// exists. Canonical, and UTF-8 as prodigal's training_path must be.
fn model_target(path: &Path) -> Result<String, String> {
    let fail = |why: &dyn std::fmt::Display| format!("training_path {}: {why}", path.display());
    let target = match std::fs::canonicalize(path) {
        Ok(target) if target.is_dir() => return Err(fail(&"is a directory")),
        Ok(target) => target,
        // A new file
        Err(_) => {
            let name = path.file_name().ok_or_else(|| fail(&"names no file"))?;
            let dir = path.parent().filter(|dir| !dir.as_os_str().is_empty());
            std::fs::canonicalize(dir.unwrap_or(Path::new(".")))
                .map_err(|e| fail(&e))?
                .join(name)
        }
    };
    target
        .into_os_string()
        .into_string()
        .map_err(|_| fail(&"its resolved path is not UTF-8"))
}

/// Write `blob` to `target` (a model_target) atomically: a temporary file
/// in the same directory, synced, renamed over it, and the rename synced.
/// The file's mode is the CLI's: 0666 less the umask.
fn write_atomically(target: &str, blob: &[u8]) -> std::io::Result<()> {
    let dir = Path::new(target).parent().expect("a canonical file path");
    let mut file = tempfile::Builder::new()
        .prefix(".prodigal-train-")
        .permissions(std::fs::Permissions::from_mode(0o666))
        .tempfile_in(dir)?;
    file.write_all(blob)?;
    file.as_file().sync_all()?;
    file.persist(target)?;
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::ProdigalTrainTool;
    use crate::protocol::{BatchRequest, Response};
    use crate::test_util::{
        gene_coordinates, genes_of, gff_coordinates, prodigal_batch, scratch_dir, unique_shm_name,
        write_arrow_to_shm, PRODIGAL_REF_TRAIN as REF_TRAIN,
        PRODIGAL_REF_TRAIN_SHA256 as REF_TRAIN_SHA256,
    };
    use crate::tools::prodigal::{sha256_hex, ProdigalTool};
    use crate::tools::GplTool;
    use arrow::record_batch::RecordBatch;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    /// macOS training writes different bytes, with the same settings and
    /// genes: seen on ARM64, and Intel macOS is unchecked
    /// (ext/prodigal/docs/testing.md).
    const TRAIN_BYTES_MATCH: bool = !cfg!(target_os = "macos");

    /// The fork's ground-truth input as one batch.
    fn anthus() -> RecordBatch {
        let contigs = crate::test_util::anthus();
        let (names, seqs): (Vec<&str>, Vec<&str>) = contigs
            .iter()
            .map(|(name, seq)| (name.as_str(), seq.as_str()))
            .unzip();
        prodigal_batch(&names, &seqs)
    }

    fn run(tool: &str, config: serde_json::Value, batch: &RecordBatch) -> Response {
        let input_name = unique_shm_name("pd-train");
        let input = write_arrow_to_shm(&input_name, batch);
        crate::tools::dispatch(&BatchRequest {
            tool: tool.to_string(),
            config,
            shm_input: input_name,
            shm_input_size: input.len(),
            batch_id: None,
            metrics: false,
            received_at: None,
        })
    }

    /// Files in `dir`, sorted.
    fn files(dir: &Path) -> Vec<String> {
        let entries = std::fs::read_dir(dir).unwrap();
        let mut names: Vec<String> = entries
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// Trained on anthus, the model is the CLI's (`-t`): the same settings
    /// everywhere and the same bytes where training writes them, and the
    /// result describes it.
    #[test]
    fn test_train_writes_the_clis_model() {
        let dir = scratch_dir();
        let path = dir.path().join("anthus.bin");
        let config = serde_json::json!({"training_path": path});
        let response = run("prodigal-train", config, &anthus());
        assert!(response.success, "{:?}", response.error);
        assert!(response.shm_outputs.is_empty());
        let result = response.result.unwrap();

        let written = std::fs::read(&path).unwrap();
        let reference = std::fs::read(REF_TRAIN).unwrap();
        assert_eq!(written.len(), reference.len());
        if TRAIN_BYTES_MATCH {
            assert!(written == reference, "the model differs from ref_train.bin");
            assert_eq!(result["training_sha256"], REF_TRAIN_SHA256);
        }
        let canonical = std::fs::canonicalize(&path).unwrap();
        assert_eq!(result["training_path"], canonical.to_str().unwrap());
        // The model's settings, as the CLI trained them on anthus
        assert_eq!(result["translation_table"], 11);
        assert_eq!(result["start_weight"], 4.35);
        assert!(result["uses_sd"].is_i64());
        let gc = result["gc_content"].as_f64().unwrap();
        assert!((0.0..=1.0).contains(&gc), "{gc}");
        assert_eq!(result["closed_ends"], false);
        assert_eq!(result["mask_regions"], false);
        assert_eq!(result["n_sequences"], 22);
        assert_eq!(result["n_bases"], 22 * 960);
        assert!(result["elapsed_ms"].is_u64());
        // Written atomically: nothing else is left in the directory
        assert_eq!(files(dir.path()), ["anthus.bin"]);
    }

    /// The reported SHA-256 is the file's, and retraining in place replaces
    /// the file, atomically: a model trained with other settings has
    /// another hash.
    #[test]
    fn test_retrain_replaces_the_model() {
        let dir = scratch_dir();
        let path = dir.path().join("model.bin");
        let train = |config: serde_json::Value| {
            let response = run("prodigal-train", config, &anthus());
            assert!(response.success, "{:?}", response.error);
            response.result.unwrap()
        };
        let first = train(serde_json::json!({"training_path": path}));
        let second = train(serde_json::json!({"training_path": path, "closed_ends": true}));
        assert_eq!(second["closed_ends"], true);
        assert_ne!(first["training_sha256"], second["training_sha256"]);
        let file = std::fs::read(&path).unwrap();
        assert_eq!(second["training_sha256"], sha256_hex(&file));
        assert_eq!(files(dir.path()), ["model.bin"]);
    }

    /// training_path through a symlink writes the file it leads to, as the
    /// CLI does, and the result names that file.
    #[test]
    fn test_train_writes_through_a_symlink() {
        let dir = scratch_dir();
        let target = dir.path().join("v1.bin");
        std::fs::write(&target, b"old").unwrap();
        let link = dir.path().join("current.bin");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let config = serde_json::json!({"training_path": link});
        let result = run("prodigal-train", config, &anthus()).result.unwrap();
        let link_type = std::fs::symlink_metadata(&link).unwrap().file_type();
        assert!(link_type.is_symlink());
        let target = std::fs::canonicalize(&target).unwrap();
        assert_eq!(result["training_path"], target.to_str().unwrap());
        let written = std::fs::read(&target).unwrap();
        assert_eq!(result["training_sha256"], sha256_hex(&written));
    }

    /// The model's mode is a new file's (0666 less the umask), as the CLI
    /// writes it, not the temporary file's 0600.
    #[test]
    fn test_model_mode_is_a_new_files() {
        let dir = scratch_dir();
        let plain = dir.path().join("plain");
        std::fs::write(&plain, b"").unwrap();
        let path = dir.path().join("model.bin");
        let response = run(
            "prodigal-train",
            serde_json::json!({"training_path": path}),
            &anthus(),
        );
        assert!(response.success, "{:?}", response.error);
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&path), mode(&plain));
    }

    /// The model knobs prodigal-train shares with prodigal are the same
    /// knobs: type, default and allowed values.
    #[test]
    fn test_shared_knobs_agree() {
        let (train, predict) = (ProdigalTrainTool.describe(), ProdigalTool.describe());
        for knob in train
            .config_params
            .iter()
            .filter(|p| p.name != "training_path")
        {
            let same = predict.config_params.iter().find(|p| p.name == knob.name);
            let same = same.expect(knob.name);
            assert_eq!(knob.param_type, same.param_type, "{}", knob.name);
            assert_eq!(knob.default, same.default, "{}", knob.name);
            assert_eq!(knob.allowed_values, same.allowed_values, "{}", knob.name);
        }
    }

    /// A failed training writes nothing and leaves an existing model alone.
    #[test]
    fn test_failed_training_keeps_the_old_model() {
        let dir = scratch_dir();
        let path = dir.path().join("model.bin");
        std::fs::write(&path, b"old").unwrap();
        // One contig of 960 bp: training needs 20,000
        let short = anthus().slice(0, 1);
        let response = run(
            "prodigal-train",
            serde_json::json!({"training_path": path}),
            &short,
        );
        let err = response.error.expect("refused");
        assert!(err.contains("training"), "{err}");
        assert_eq!(std::fs::read(&path).unwrap(), b"old");
        assert_eq!(files(dir.path()), ["model.bin"]);
    }

    /// The config is strict, as prodigal's: training_path is required and a
    /// string, and prodigal's mode knobs are unknown here.
    #[test]
    fn test_train_config_is_strict() {
        let dir = scratch_dir();
        let path = dir.path().join("model.bin");
        for (config, named) in [
            (serde_json::json!({}), "training_path"),
            (serde_json::json!({"training_path": 7}), "training_path"),
            (
                serde_json::json!({"training_path": path, "meta_mode": false}),
                "meta_mode",
            ),
            (
                serde_json::json!({"training_path": path, "train_on_batch": true}),
                "train_on_batch",
            ),
            (
                serde_json::json!({"training_path": path, "training_sha256": REF_TRAIN_SHA256}),
                "training_sha256",
            ),
            (
                serde_json::json!({"training_path": path, "closed_ends": "yes"}),
                "closed_ends",
            ),
            // The library's reason for a table it refuses
            (
                serde_json::json!({"training_path": path, "trans_table": 1}),
                "Invalid prodigal config",
            ),
        ] {
            let response = run("prodigal-train", config.clone(), &anthus());
            let err = response.error.expect("refused");
            assert!(err.contains(named), "{config}: {err}");
        }
        assert!(files(dir.path()).is_empty());
    }

    /// A directory that does not exist is an error naming the path.
    #[test]
    fn test_train_needs_an_existing_directory() {
        let dir = scratch_dir();
        let path = dir.path().join("missing").join("model.bin");
        let response = run(
            "prodigal-train",
            serde_json::json!({"training_path": path}),
            &anthus(),
        );
        let err = response.error.expect("refused");
        assert!(err.contains(path.to_str().unwrap()), "{err}");
    }

    /// A model trained here predicts the CLI's genes from it
    /// (ref_trained.gff: `prodigal -t ref_train.bin` on anthus).
    #[test]
    fn test_trained_model_predicts_the_clis_genes() {
        let dir = scratch_dir();
        let path = dir.path().join("model.bin");
        let response = run(
            "prodigal-train",
            serde_json::json!({"training_path": path}),
            &anthus(),
        );
        let result = response.result.expect("trained");
        let config = serde_json::json!({
            "meta_mode": false,
            "training_path": result["training_path"],
            "training_sha256": result["training_sha256"],
        });
        let (_, genes) = genes_of(run("prodigal", config, &anthus())).unwrap();
        let expected = gff_coordinates(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/ext/prodigal/testdata/ground_truth/ref_trained.gff"
        )));
        assert_eq!(gene_coordinates(&genes), expected);
    }
}
