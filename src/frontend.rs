//! The native front end: runs a transport-free tool call on shared memory.
//!
//! It maps the request's input segment and decodes it without copying
//! (`arrow_ipc::map_shm`, `arrow_ipc::decode_stream`), so the tool gets
//! views of the mapping. It writes each output batch to a new segment
//! through `arrow_ipc::ShmOutputs`, all or none. A segment is registered
//! for signal-handler cleanup when it is created (`ShmWriter`); the front
//! end never deregisters one, since `main.rs` does that once the response
//! is on stdout.

use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;

use crate::arrow_ipc::{self, ShmOutputs};
use crate::protocol::Response;
use crate::tools::ToolOutput;

/// Run `call` on the Arrow stream in the `shm_input` segment, whose first
/// `shm_input_size` bytes hold it, and write its outputs to new segments.
/// An input that cannot be mapped or decoded fails before `call` runs.
pub(crate) fn run_on_shm(
    shm_input: &str,
    shm_input_size: usize,
    call: impl FnOnce(Vec<RecordBatch>) -> Result<ToolOutput, String>,
) -> Response {
    match arrow_ipc::map_shm(shm_input, shm_input_size) {
        Ok(input) => run_on_buffer(input, call),
        Err(e) => Response::error(e),
    }
}

/// `run_on_shm` from the mapped input on.
fn run_on_buffer(
    input: Buffer,
    call: impl FnOnce(Vec<RecordBatch>) -> Result<ToolOutput, String>,
) -> Response {
    let input = match arrow_ipc::decode_stream(input) {
        Ok(batches) => batches,
        Err(e) => return Response::error(e),
    };
    let output = match call(input) {
        Ok(output) => output,
        Err(e) => return Response::error(e),
    };
    let mut written = ShmOutputs::default();
    for (label, batch) in output.outputs {
        if let Err(e) = written.write(&batch, label) {
            return Response::error(e);
        }
    }
    Response::ok(output.result, written.into_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow_ipc::map_shm;
    use crate::protocol::BatchRequest;
    use crate::shm::{self, SharedMemory};
    use crate::test_util::column;
    use crate::test_util::shm::{
        take_outputs, unique_shm_name, write_arrow_to_shm, write_batches_to_shm,
    };
    use crate::tools::{GplTool, Runner, StreamingContext, ToolDescription, ToolOutput};
    use arrow::array::{Array, ArrayData, Int64Array, ListArray, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use serde_json::json;
    use std::sync::Arc;

    fn names_batch(names: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names.to_vec()))]).unwrap()
    }

    fn names_of(batch: &RecordBatch) -> Vec<Option<String>> {
        let col: &StringArray = column(batch, "name");
        col.iter().map(|v| v.map(str::to_string)).collect()
    }

    /// A batch request for an input segment, as the coordinator gets it.
    fn request(shm_input: &str, shm_input_size: usize, batch_id: u64) -> BatchRequest {
        BatchRequest {
            tool: "fe-row-counter".to_string(),
            config: json!({}),
            shm_input: shm_input.to_string(),
            shm_input_size,
            batch_id: Some(batch_id),
            metrics: false,
            received_at: None,
        }
    }

    /// The number the next output segment name takes, less one: the
    /// probe uses one up.
    fn output_counter() -> u32 {
        let name = shm::output_shm_name("x").unwrap();
        let prefix = format!("/gb-{}-", std::process::id());
        let rest = name.strip_prefix(&prefix).unwrap();
        rest.strip_suffix("-x").unwrap().parse().unwrap()
    }

    #[test]
    fn test_outputs_are_written_in_order_under_their_labels() {
        let _registry = shm::REGISTRY_TEST_LOCK.lock().unwrap();
        let name = unique_shm_name("fe-order");
        let holder = write_arrow_to_shm(&name, &names_batch(&[Some("a")]));
        let first = names_batch(&[Some("x"), Some("y")]);
        let second = names_batch(&[Some("z")]);
        let outputs = vec![("fe-first", first.clone()), ("fe-second", second.clone())];

        let response = run_on_shm(&name, holder.len(), |input| {
            Ok(ToolOutput {
                outputs,
                result: json!({"rows": input[0].num_rows()}),
            })
        });

        assert!(response.success, "{:?}", response.error);
        assert_eq!(response.result, Some(json!({"rows": 1})));
        for out in &response.shm_outputs {
            assert!(shm::is_registered(&out.name), "{} not registered", out.name);
        }
        let written = take_outputs(&response);
        let labels: Vec<&str> = written.iter().map(|(l, _)| l.as_str()).collect();
        assert_eq!(labels, ["fe-first", "fe-second"]);
        assert_eq!(written[0].1, first);
        assert_eq!(written[1].1, second);
    }

    #[test]
    fn test_a_failed_output_write_leaves_no_segment() {
        let name = unique_shm_name("fe-partial");
        let batch = names_batch(&[Some("a")]);
        let holder = write_arrow_to_shm(&name, &batch);

        let before = output_counter();
        let response = run_on_shm(&name, holder.len(), |_| {
            Ok(ToolOutput {
                outputs: vec![("fe-kept", batch.clone()), ("Not A Label", batch.clone())],
                result: json!({}),
            })
        });
        let after = output_counter();

        assert!(!response.success);
        assert!(response.error.as_deref().unwrap().contains("label"));
        assert!(response.shm_outputs.is_empty());
        // The window holds the number the kept write drew, and any that
        // other test threads drew meanwhile, whose labels differ.
        assert!(after > before + 1, "no name was drawn");
        let pid = std::process::id();
        let left: Vec<String> = (before + 1..after)
            .map(|n| format!("/gb-{pid}-{n}-fe-kept"))
            .filter(|n| SharedMemory::open_readonly(n).is_ok() || shm::is_registered(n))
            .collect();
        assert!(left.is_empty(), "left behind: {left:?}");
    }

    #[test]
    fn test_a_tool_error_becomes_the_error_response() {
        let name = unique_shm_name("fe-err");
        let holder = write_arrow_to_shm(&name, &names_batch(&[Some("a")]));

        let response = run_on_shm(&name, holder.len(), |_| {
            Err("fe: the tool refused".to_string())
        });

        assert!(!response.success);
        assert_eq!(response.error.as_deref(), Some("fe: the tool refused"));
        assert!(response.shm_outputs.is_empty());
        assert!(response.result.is_none());
    }

    #[test]
    fn test_an_unreadable_input_fails_before_the_tool_runs() {
        let mut ran = false;
        let response = run_on_shm("/gb-fe-no-such-input", 0, |_| {
            ran = true;
            Err("ran".to_string())
        });
        assert!(!ran);
        assert!(!response.success);
        let error = response.error.unwrap();
        assert!(error.contains("Failed to open shm"), "{error}");

        let name = unique_shm_name("fe-garbage");
        let mut garbage = SharedMemory::create(&name, 16).unwrap();
        garbage.as_mut_slice()[..16].copy_from_slice(b"not an arrow ipc");
        let mut ran = false;
        let response = run_on_shm(&name, 16, |_| {
            ran = true;
            Err("ran".to_string())
        });
        assert!(!ran);
        assert!(!response.success);
        let error = response.error.unwrap();
        assert!(error.contains("Arrow IPC"), "{error}");
    }

    #[test]
    fn test_an_empty_input_reaches_the_tool_as_no_batches() {
        // A zero-byte segment (SharedMemory::create cannot map none)
        let name = unique_shm_name("fe-empty");
        let c_name = std::ffi::CString::new(name.as_str()).unwrap();
        // SAFETY: plain libc calls on a name this test owns.
        let fd = unsafe {
            libc::shm_open(
                c_name.as_ptr(),
                libc::O_CREAT | libc::O_RDWR | libc::O_EXCL,
                0o600,
            )
        };
        assert!(fd >= 0, "{}", std::io::Error::last_os_error());
        unsafe { libc::close(fd) };
        let mut seen = None;
        let response = run_on_shm(&name, 0, |input| {
            seen = Some(input.len());
            Ok(ToolOutput {
                outputs: vec![],
                result: json!({}),
            })
        });
        let _ = SharedMemory::unlink(&name);
        assert!(response.success, "{:?}", response.error);
        assert_eq!(seen, Some(0));

        // A stream with a schema and no batches
        let schema = names_batch(&[]).schema();
        let mut stream = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut stream, &schema).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let name = unique_shm_name("fe-schema-only");
        let mut holder = SharedMemory::create(&name, stream.len()).unwrap();
        holder.as_mut_slice().copy_from_slice(&stream);
        let mut seen = None;
        let response = run_on_shm(&name, holder.len(), |input| {
            seen = Some(input.len());
            Ok(ToolOutput {
                outputs: vec![],
                result: json!({}),
            })
        });
        assert!(response.success, "{:?}", response.error);
        assert_eq!(seen, Some(0));
    }

    /// The tool reads the segment itself: a write to the segment, made
    /// through the writer's own mapping during the call, shows in the
    /// tool's input. A copy anywhere between the segment and the tool
    /// would not see it.
    #[test]
    fn test_the_tool_reads_the_input_segment_in_place() {
        let name = unique_shm_name("fe-in-place");
        let mut holder = write_arrow_to_shm(&name, &names_batch(&[Some("mark-a")]));
        let size = holder.len();
        let at = holder
            .as_slice()
            .windows(6)
            .position(|w| w == b"mark-a")
            .unwrap();
        let mut seen = Vec::new();
        let response = run_on_shm(&name, size, |input| {
            holder.as_mut_slice()[at..at + 6].copy_from_slice(b"mark-b");
            seen = names_of(&input[0]);
            Ok(ToolOutput {
                outputs: vec![],
                result: json!({}),
            })
        });
        assert!(response.success, "{:?}", response.error);
        assert_eq!(seen, [Some("mark-b".to_string())]);
    }

    /// The (address, length) of every non-empty buffer an array holds:
    /// its own, its null buffer's and its children's.
    fn buffer_ranges(data: &ArrayData, out: &mut Vec<(usize, usize)>) {
        let nulls = data.nulls().map(|n| n.buffer());
        for buffer in data.buffers().iter().chain(nulls) {
            if !buffer.is_empty() {
                out.push((buffer.as_ptr() as usize, buffer.len()));
            }
        }
        for child in data.child_data() {
            buffer_ranges(child, out);
        }
    }

    #[test]
    fn test_the_tool_receives_views_of_the_input_mapping() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]));
        let batch = |names: Vec<Option<&str>>, tags: Vec<Option<Vec<Option<i32>>>>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(names)),
                    Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(tags)),
                ],
            )
            .unwrap()
        };
        let one = batch(
            vec![Some("a"), None, Some("c")],
            vec![Some(vec![Some(1), None]), None, Some(vec![])],
        );
        let two = batch(vec![Some("d")], vec![Some(vec![Some(4), Some(5)])]);
        let name = unique_shm_name("fe-views");
        let holder = write_batches_to_shm(&name, &[one, two]);

        let mapping = map_shm(&name, holder.len()).unwrap();
        let start = mapping.as_ptr() as usize;
        let end = start + mapping.len();
        let mut names = Vec::new();
        let mut ranges = Vec::new();
        let response = run_on_buffer(mapping, |input| {
            for batch in &input {
                names.push(names_of(batch));
                for column in batch.columns() {
                    buffer_ranges(&column.to_data(), &mut ranges);
                }
            }
            Ok(ToolOutput {
                outputs: vec![],
                result: json!({}),
            })
        });

        assert!(response.success, "{:?}", response.error);
        assert_eq!(
            names,
            [
                vec![Some("a".to_string()), None, Some("c".to_string())],
                vec![Some("d".to_string())],
            ]
        );
        // name: offsets, values, nulls; tags: offsets, nulls, child values
        // and child nulls, in the first batch alone.
        assert!(ranges.len() >= 7, "{ranges:?}");
        let outside: Vec<_> = ranges
            .iter()
            .filter(|&&(at, len)| at < start || at + len > end)
            .collect();
        assert!(
            outside.is_empty(),
            "buffers outside the mapping [{start:#x}, {end:#x}): {outside:?}"
        );
    }

    /// A test tool: `execute` counts its input rows; its context keeps a
    /// running total across batches.
    struct RowCounter;

    struct RunningTotal(i64);

    fn rows(input: &[RecordBatch]) -> i64 {
        input.iter().map(|b| b.num_rows() as i64).sum()
    }

    fn count_output(n: i64) -> Vec<(&'static str, RecordBatch)> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "rows",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![n]))]).unwrap();
        vec![("fe-rows", batch)]
    }

    fn count_of(batch: &RecordBatch) -> i64 {
        column::<Int64Array>(batch, "rows").value(0)
    }

    impl GplTool for RowCounter {
        fn name(&self) -> &str {
            "fe-row-counter"
        }

        fn version(&self) -> String {
            "0.0.1".to_string()
        }

        fn schema_version(&self) -> u32 {
            7
        }

        fn describe(&self) -> ToolDescription {
            ToolDescription {
                name: "fe-row-counter",
                version: self.version(),
                schema_version: self.schema_version(),
                describe_version: 1,
                description: "Counts rows",
                config_params: vec![],
                input_schema: vec![],
                outputs: vec![],
                response_metadata: vec![],
            }
        }

        fn execute(
            &self,
            _config: &serde_json::Value,
            input: Vec<RecordBatch>,
        ) -> Result<ToolOutput, String> {
            let n = rows(&input);
            Ok(ToolOutput {
                outputs: count_output(n),
                result: json!({"rows": n}),
            })
        }

        fn create_streaming_context(
            &self,
            _config: &serde_json::Value,
        ) -> Result<Option<Box<dyn StreamingContext>>, String> {
            Ok(Some(Box::new(RunningTotal(0))))
        }
    }

    impl StreamingContext for RunningTotal {
        fn run_batch(&mut self, input: Vec<RecordBatch>) -> Result<ToolOutput, String> {
            self.0 += rows(&input);
            Ok(ToolOutput {
                outputs: count_output(self.0),
                result: json!({"total": self.0}),
            })
        }
    }

    /// fasttree through the front end, on real shared memory, gives what
    /// a direct `execute` gives.
    #[test]
    fn test_fasttree_on_shm() {
        use crate::tools::fasttree::{input_schema, FastTreeTool};
        let batch = RecordBatch::try_new(
            Arc::new(input_schema()),
            vec![
                Arc::new(StringArray::from(vec!["SeqA", "SeqB", "SeqC", "SeqD"])),
                Arc::new(StringArray::from(vec![
                    "ACGTACGTACGTACGTACGT",
                    "ACGTACGTACGTACGTACGA",
                    "TGCATGCATGCATGCATGCA",
                    "TGCATGCATGCATGCATGCG",
                ])),
            ],
        )
        .unwrap();
        let config = json!({"seq_type": "nucleotide", "seed": 12345});
        let direct = FastTreeTool.execute(&config, vec![batch.clone()]).unwrap();

        let name = unique_shm_name("fe-fasttree");
        let holder = write_arrow_to_shm(&name, &batch);
        let response = respond(&request(&name, holder.len(), 1), 2, |input| {
            FastTreeTool.execute(&config, input)
        });

        assert!(response.success, "{:?}", response.error);
        assert_eq!(response.result.as_ref(), Some(&direct.result));
        let written = take_outputs(&response);
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].0, "tree");
        assert_eq!(written[0].1, direct.outputs[0].1);
    }

    /// A response carries its request's batch_id, and on success the
    /// tool's schema_version: the error response has no schema_version.
    #[test]
    fn test_respond_stamps_batch_id_and_schema_version() {
        let name = unique_shm_name("fe-stamp");
        let holder = write_arrow_to_shm(&name, &names_batch(&[Some("a")]));
        let ok = respond(&request(&name, holder.len(), 41), 9, |_| {
            Ok(ToolOutput {
                outputs: vec![],
                result: json!({}),
            })
        });
        assert!(ok.success, "{:?}", ok.error);
        assert_eq!((ok.batch_id, ok.schema_version), (Some(41), Some(9)));

        let refused = respond(&request(&name, holder.len(), 42), 9, |_| {
            Err("fe: refused".to_string())
        });
        assert!(!refused.success);
        assert_eq!((refused.batch_id, refused.schema_version), (Some(42), None));

        let unreadable = respond(&request("/gb-fe-no-such-input", 0, 43), 9, |_| {
            Err("ran".to_string())
        });
        assert!(unreadable.error.unwrap().contains("Failed to open shm"));
        assert_eq!(
            (unreadable.batch_id, unreadable.schema_version),
            (Some(43), None)
        );
    }

    /// The tools take and return record batches: no tool module names the
    /// transport, which is the front end's and arrow_ipc's alone.
    #[test]
    fn test_tool_modules_are_transport_free() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/tools");
        let mut files: Vec<_> = (std::fs::read_dir(dir).unwrap())
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
            .collect();
        files.sort();
        assert!(files.len() >= 7, "{files:?}");
        let mut found = Vec::new();
        for path in &files {
            let file = path.file_name().unwrap().to_string_lossy();
            for (i, line) in std::fs::read_to_string(path).unwrap().lines().enumerate() {
                let lower = line.to_lowercase();
                if ["shm", "shared memory", "arrow_ipc"]
                    .iter()
                    .any(|word| lower.contains(word))
                {
                    found.push(format!("{file}:{}: {}", i + 1, line.trim()));
                }
            }
        }
        assert!(
            found.is_empty(),
            "tool modules name the transport:\n{}",
            found.join("\n")
        );
    }

    /// A streaming runner keeps its context across batches, a failed batch
    /// included; a per-batch runner runs the tool afresh.
    #[test]
    fn test_a_streaming_runner_keeps_its_context_across_batches() {
        let config = json!({});

        // Three rows in two batches, then one row.
        let three = unique_shm_name("fe-three");
        let three_holder = write_batches_to_shm(
            &three,
            &[
                names_batch(&[Some("a"), Some("b")]),
                names_batch(&[Some("c")]),
            ],
        );
        let one = unique_shm_name("fe-one");
        let one_holder = write_arrow_to_shm(&one, &names_batch(&[Some("d")]));

        let context = RowCounter.create_streaming_context(&config).unwrap();
        let mut runner = Runner::Streaming(context.unwrap());
        let first = respond(&request(&three, three_holder.len(), 1), 7, |input| {
            runner.run(&config, input)
        });
        assert!(first.success, "{:?}", first.error);
        assert_eq!(first.result, Some(json!({"total": 3})));
        let first = take_outputs(&first);
        assert_eq!(first[0].0, "fe-rows");
        assert_eq!(count_of(&first[0].1), 3);

        // A batch that fails leaves the context as it was
        let failed = respond(&request("/gb-fe-no-such-input", 0, 2), 7, |input| {
            runner.run(&config, input)
        });
        assert!(!failed.success);

        let second = respond(&request(&one, one_holder.len(), 3), 7, |input| {
            runner.run(&config, input)
        });
        assert!(second.success, "{:?}", second.error);
        assert_eq!(second.result, Some(json!({"total": 4})));
        assert_eq!(count_of(&take_outputs(&second)[0].1), 4);

        let mut per_batch = Runner::PerBatch(Box::new(RowCounter));
        let single = respond(&request(&three, three_holder.len(), 4), 7, |input| {
            per_batch.run(&config, input)
        });
        assert!(single.success, "{:?}", single.error);
        assert_eq!(single.result, Some(json!({"rows": 3})));
        assert_eq!(count_of(&take_outputs(&single)[0].1), 3);
    }
}
