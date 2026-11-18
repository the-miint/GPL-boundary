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

use crate::protocol::Response;
use crate::tools::ToolOutput;

/// Run `call` on the Arrow stream in the `shm_input` segment, whose first
/// `shm_input_size` bytes hold it, and write its outputs to new segments.
/// An input that cannot be mapped or decoded fails before `call` runs.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn run_on_shm(
    shm_input: &str,
    shm_input_size: usize,
    call: impl FnOnce(Vec<RecordBatch>) -> Result<ToolOutput, String>,
) -> Response {
    let _ = (shm_input, shm_input_size, call);
    unimplemented!()
}

/// `run_on_shm` from the mapped input on.
#[cfg_attr(not(test), allow(dead_code))]
fn run_on_buffer(
    input: Buffer,
    call: impl FnOnce(Vec<RecordBatch>) -> Result<ToolOutput, String>,
) -> Response {
    let _ = (input, call);
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow_ipc::map_shm;
    use crate::shm::{self, SharedMemory};
    use crate::test_util::{
        read_arrow_from_shm, unique_shm_name, write_arrow_to_shm, write_batches_to_shm,
    };
    use crate::tools::{GplTool, OnShm, ShmTool, StreamingContext, ToolDescription, ToolOutput};
    use arrow::array::{Array, ArrayData, Int64Array, ListArray, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use serde_json::json;
    use std::sync::Arc;

    fn names_batch(names: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names.to_vec()))]).unwrap()
    }

    fn names_of(batch: &RecordBatch) -> Vec<Option<String>> {
        let col = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        col.iter().map(|v| v.map(str::to_string)).collect()
    }

    /// Every output of a response, read, then unlinked: (label, batch).
    fn take_outputs(response: &Response) -> Vec<(String, RecordBatch)> {
        response
            .shm_outputs
            .iter()
            .map(|out| {
                let mut batches = read_arrow_from_shm(&out.name, out.size);
                let _ = SharedMemory::unlink(&out.name);
                assert_eq!(batches.len(), 1, "{}", out.name);
                (out.label.clone(), batches.remove(0))
            })
            .collect()
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
        assert!(after > before + 1, "no output was written");
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
        let col = batch.column(0).as_any().downcast_ref::<Int64Array>();
        col.unwrap().value(0)
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

    #[test]
    fn test_on_shm_serves_a_context_across_batches() {
        let tool = OnShm(RowCounter);
        assert_eq!(tool.name(), "fe-row-counter");
        assert_eq!(tool.schema_version(), 7);
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

        let mut ctx = tool.create_streaming_context(&config).unwrap().unwrap();
        let first = ctx.run_batch(&three, three_holder.len());
        assert!(first.success, "{:?}", first.error);
        assert_eq!(first.result, Some(json!({"total": 3})));
        let first = take_outputs(&first);
        assert_eq!(first[0].0, "fe-rows");
        assert_eq!(count_of(&first[0].1), 3);

        let second = ctx.run_batch(&one, one_holder.len());
        assert!(second.success, "{:?}", second.error);
        assert_eq!(second.result, Some(json!({"total": 4})));
        assert_eq!(count_of(&take_outputs(&second)[0].1), 4);

        let single = tool.execute(&config, &three, three_holder.len());
        assert!(single.success, "{:?}", single.error);
        assert_eq!(single.result, Some(json!({"rows": 3})));
        assert_eq!(count_of(&take_outputs(&single)[0].1), 3);
    }
}
