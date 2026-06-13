use arrow_array::RecordBatch;

use crate::Result;
use crate::aio::async_processor::{Processor, ProcessorContext};

#[derive(Default)]
pub struct AsyncSystem {
    processors: Vec<Box<dyn Processor>>,
}

impl AsyncSystem {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_processor<P>(&mut self, processor: P)
    where
        P: Processor + 'static,
    {
        self.processors.push(Box::new(processor));
    }

    pub fn execute(&self, mut batch: RecordBatch, ctx: &ProcessorContext) -> Result<RecordBatch> {
        let mut processors = self.processors.iter().collect::<Vec<_>>();
        processors.sort_by_key(|processor| processor.priority());

        for processor in processors {
            if processor_matches(processor.as_ref(), &batch) {
                batch = processor.process(batch, ctx)?;
            }
        }

        Ok(batch)
    }
}

pub type NativeSystem = AsyncSystem;

fn processor_matches(processor: &dyn Processor, batch: &RecordBatch) -> bool {
    processor
        .required_columns()
        .iter()
        .all(|name| batch.schema().index_of(name).is_ok())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int32Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    struct AddOne;

    impl Processor for AddOne {
        fn name(&self) -> &str {
            "add_one"
        }

        fn required_columns(&self) -> &[&'static str] {
            &["value"]
        }

        fn process(&self, batch: RecordBatch, _ctx: &ProcessorContext) -> Result<RecordBatch> {
            let values = batch
                .column(batch.schema().index_of("value")?)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let next = (0..batch.num_rows())
                .map(|idx| values.value(idx) + 1.0)
                .collect::<Vec<_>>();
            RecordBatch::try_new(batch.schema(), vec![Arc::new(Float64Array::from(next))])
                .map_err(Into::into)
        }
    }

    struct Skipped;

    impl Processor for Skipped {
        fn name(&self) -> &str {
            "skipped"
        }

        fn required_columns(&self) -> &[&'static str] {
            &["missing"]
        }

        fn process(&self, _batch: RecordBatch, _ctx: &ProcessorContext) -> Result<RecordBatch> {
            panic!("processor with missing required columns should not run")
        }
    }

    #[test]
    fn async_system_runs_matching_processors_in_priority_order() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.0, 2.0]))],
        )
        .unwrap();
        let mut system = AsyncSystem::new();
        system.add_processor(Skipped);
        system.add_processor(AddOne);

        let out = system
            .execute(batch, &ProcessorContext::new("world", "run", 0))
            .unwrap();
        let values = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(values.value(0), 2.0);
        assert_eq!(values.value(1), 3.0);
    }

    #[test]
    fn async_system_preserves_batch_when_no_processor_matches() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let mut system = AsyncSystem::new();
        system.add_processor(Skipped);

        let out = system
            .execute(batch, &ProcessorContext::new("world", "run", 0))
            .unwrap();

        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.schema().field(0).name(), "other");
    }
}
