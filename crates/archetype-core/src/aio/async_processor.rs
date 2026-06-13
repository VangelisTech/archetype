use arrow_array::RecordBatch;

use crate::Result;

#[derive(Debug, Clone)]
pub struct ProcessorContext {
    pub world_id: String,
    pub run_id: String,
    pub tick: i32,
}

impl ProcessorContext {
    pub fn new(world_id: impl Into<String>, run_id: impl Into<String>, tick: i32) -> Self {
        Self {
            world_id: world_id.into(),
            run_id: run_id.into(),
            tick,
        }
    }
}

pub trait Processor: Send + Sync {
    fn name(&self) -> &str;

    fn priority(&self) -> i32 {
        0
    }

    fn required_columns(&self) -> &[&'static str];

    fn process(&self, batch: RecordBatch, ctx: &ProcessorContext) -> Result<RecordBatch>;
}
