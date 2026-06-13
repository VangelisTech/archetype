use arrow_array::RecordBatch;
use async_trait::async_trait;

use crate::Result;

#[derive(Debug, Clone, Default)]
pub struct ReadFilter {
    pub world_id: Option<String>,
    pub run_id: Option<String>,
    pub ticks: Option<Vec<i32>>,
    pub entity_ids: Option<Vec<i32>>,
    pub active_only: bool,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn append(&self, table_name: &str, batch: &RecordBatch) -> Result<()>;

    async fn read_table(&self, table_name: &str, filter: ReadFilter) -> Result<Vec<RecordBatch>>;
}
