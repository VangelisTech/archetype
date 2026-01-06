// Copyright 2025 Vangelis Technologies Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Query manager for reading archetype data.
//!
//! The Querier abstracts read operations from the underlying storage,
//! providing filtered access to entity data by tick, entity_id, or
//! component projections.

use std::sync::Arc;

use arrow::compute::take;
use arrow_array::{BooleanArray, Int32Array, RecordBatch};
use async_trait::async_trait;

use crate::archetype::{Archetype, ArchetypeSignature};
use crate::error::Result;
use crate::store::AsyncStore;

/// Trait for async query operations.
#[async_trait]
pub trait AsyncQuerier: Send + Sync {
    /// Gets all data for an archetype in a world/run.
    async fn get_archetype(
        &self,
        sig: &ArchetypeSignature,
        world_id: &str,
        run_id: &str,
    ) -> Result<Vec<RecordBatch>>;

    /// Queries archetype data with optional filters.
    async fn query_archetype(
        &self,
        sig: &ArchetypeSignature,
        world_id: &str,
        run_id: &str,
        ticks: Option<&[i32]>,
        entity_ids: Option<&[i32]>,
    ) -> Result<Vec<RecordBatch>>;
}

/// Query manager implementation backed by an AsyncStore.
pub struct QueryManager<S: AsyncStore> {
    store: Arc<S>,
}

impl<S: AsyncStore> QueryManager<S> {
    /// Creates a new query manager with the given store.
    pub fn new(store: Arc<S>) -> Self {
        QueryManager { store }
    }
}

#[async_trait]
impl<S: AsyncStore> AsyncQuerier for QueryManager<S> {
    async fn get_archetype(
        &self,
        sig: &ArchetypeSignature,
        world_id: &str,
        run_id: &str,
    ) -> Result<Vec<RecordBatch>> {
        self.store.get_archetype_df(sig, world_id, run_id).await
    }

    async fn query_archetype(
        &self,
        sig: &ArchetypeSignature,
        world_id: &str,
        run_id: &str,
        ticks: Option<&[i32]>,
        entity_ids: Option<&[i32]>,
    ) -> Result<Vec<RecordBatch>> {
        let batches = self.store.get_archetype_df(sig, world_id, run_id).await?;

        // Apply filters
        let filtered: Vec<RecordBatch> = batches
            .into_iter()
            .filter_map(|batch| filter_batch(&batch, ticks, entity_ids).ok())
            .filter(|batch| batch.num_rows() > 0)
            .collect();

        // If no batches, return empty batch with correct schema
        if filtered.is_empty() {
            let schema = Archetype::get_schema(sig);
            return Ok(vec![RecordBatch::new_empty(Arc::new(schema))]);
        }

        Ok(filtered)
    }
}

/// Filters a record batch by is_active, ticks, and entity_ids.
fn filter_batch(
    batch: &RecordBatch,
    ticks: Option<&[i32]>,
    entity_ids: Option<&[i32]>,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut mask = vec![true; num_rows];

    // Filter by is_active
    if let Some(is_active) = batch
        .column_by_name("is_active")
        .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
    {
        for i in 0..num_rows {
            if !is_active.value(i) {
                mask[i] = false;
            }
        }
    }

    // Filter by ticks
    if let Some(tick_filter) = ticks {
        if let Some(tick_col) = batch
            .column_by_name("tick")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        {
            for i in 0..num_rows {
                if mask[i] && !tick_filter.contains(&tick_col.value(i)) {
                    mask[i] = false;
                }
            }
        }
    }

    // Filter by entity_ids
    if let Some(entity_filter) = entity_ids {
        if let Some(entity_col) = batch
            .column_by_name("entity_id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        {
            for i in 0..num_rows {
                if mask[i] && !entity_filter.contains(&entity_col.value(i)) {
                    mask[i] = false;
                }
            }
        }
    }

    // Apply mask using arrow filter
    let indices: Vec<usize> = mask
        .iter()
        .enumerate()
        .filter_map(|(i, &keep)| if keep { Some(i) } else { None })
        .collect();

    if indices.is_empty() {
        return Ok(RecordBatch::new_empty(batch.schema()));
    }

    // Build filtered columns by slicing
    let filtered_columns: Vec<Arc<dyn arrow_array::Array>> = batch
        .columns()
        .iter()
        .map(|col| {
            let idx_array = Int32Array::from(indices.iter().map(|&i| i as i32).collect::<Vec<_>>());
            take(col, &idx_array, None)
                .unwrap_or_else(|_| col.clone())
        })
        .collect();

    Ok(RecordBatch::try_new(batch.schema(), filtered_columns)?)
}

/// Type alias for shared querier.
pub type SharedQuerier = Arc<dyn AsyncQuerier>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::ComponentMeta;
    use crate::store::InMemoryStore;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};

    fn test_signature() -> ArchetypeSignature {
        let meta = ComponentMeta::new("TestComponent", "testcomponent__");
        let schema = Schema::new(vec![Field::new("testcomponent__value", DataType::Int32, false)]);
        ArchetypeSignature::new(vec![(meta, schema)])
    }

    fn test_batch(
        world_id: &str,
        run_id: &str,
        entity_id: i32,
        tick: i32,
        is_active: bool,
    ) -> RecordBatch {
        let sig = test_signature();
        let schema = Archetype::get_schema(&sig);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![world_id])),
                Arc::new(StringArray::from(vec![run_id])),
                Arc::new(Int32Array::from(vec![entity_id])),
                Arc::new(Int32Array::from(vec![tick])),
                Arc::new(BooleanArray::from(vec![is_active])),
                Arc::new(Int32Array::from(vec![42])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_query_manager_basic() {
        let store = Arc::new(InMemoryStore::new());
        let sig = test_signature();

        store.ensure_table(&sig).await.unwrap();
        store
            .append(&sig, test_batch("world1", "run1", 1, 0, true))
            .await
            .unwrap();

        let querier = QueryManager::new(store);
        let result = querier
            .get_archetype(&sig, "world1", "run1")
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_query_with_tick_filter() {
        let store = Arc::new(InMemoryStore::new());
        let sig = test_signature();

        store.ensure_table(&sig).await.unwrap();
        store
            .append(&sig, test_batch("world1", "run1", 1, 0, true))
            .await
            .unwrap();
        store
            .append(&sig, test_batch("world1", "run1", 1, 1, true))
            .await
            .unwrap();
        store
            .append(&sig, test_batch("world1", "run1", 1, 2, true))
            .await
            .unwrap();

        let querier = QueryManager::new(store);
        let result = querier
            .query_archetype(&sig, "world1", "run1", Some(&[1]), None)
            .await
            .unwrap();

        // Should only get tick 1
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_query_filters_inactive() {
        let store = Arc::new(InMemoryStore::new());
        let sig = test_signature();

        store.ensure_table(&sig).await.unwrap();
        store
            .append(&sig, test_batch("world1", "run1", 1, 0, true))
            .await
            .unwrap();
        store
            .append(&sig, test_batch("world1", "run1", 2, 0, false))
            .await
            .unwrap();

        let querier = QueryManager::new(store);
        let result = querier
            .query_archetype(&sig, "world1", "run1", None, None)
            .await
            .unwrap();

        // Should only get the active entity
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }
}
