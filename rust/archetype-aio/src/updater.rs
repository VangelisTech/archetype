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

//! Update manager for writing archetype data.
//!
//! The Updater handles all write operations, stamping tick/world/run
//! metadata and appending to persistent storage.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_schema::DataType;
use async_trait::async_trait;
use tracing::info;

use crate::archetype::{Archetype, ArchetypeSignature};
use crate::error::Result;
use crate::store::AsyncStore;

/// Trait for async update operations.
#[async_trait]
pub trait AsyncUpdater: Send + Sync {
    /// Persists entity state changes to storage.
    ///
    /// Stamps the batch with tick, world_id, and run_id metadata before
    /// appending to the store.
    async fn update(
        &self,
        batch: RecordBatch,
        sig: &ArchetypeSignature,
        tick: i32,
        world_id: &str,
        run_id: &str,
    ) -> Result<RecordBatch>;
}

/// Update manager implementation backed by an AsyncStore.
pub struct UpdateManager<S: AsyncStore> {
    store: Arc<S>,
    validate: bool,
}

impl<S: AsyncStore> UpdateManager<S> {
    /// Creates a new update manager with the given store.
    pub fn new(store: Arc<S>) -> Self {
        UpdateManager {
            store,
            validate: false,
        }
    }

    /// Creates a new update manager with validation enabled.
    pub fn with_validation(store: Arc<S>, validate: bool) -> Self {
        UpdateManager { store, validate }
    }

    /// Stamps metadata columns on a record batch.
    fn stamp_batch(
        &self,
        batch: &RecordBatch,
        tick: i32,
        world_id: &str,
        run_id: &str,
    ) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        let schema = batch.schema();

        // Create stamped columns
        let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(schema.fields().len());

        for (i, field) in schema.fields().iter().enumerate() {
            let col = match field.name().as_str() {
                "tick" => {
                    Arc::new(Int32Array::from(vec![tick; num_rows])) as Arc<dyn arrow_array::Array>
                }
                "world_id" => {
                    Arc::new(StringArray::from(vec![world_id; num_rows]))
                        as Arc<dyn arrow_array::Array>
                }
                "run_id" => {
                    Arc::new(StringArray::from(vec![run_id; num_rows]))
                        as Arc<dyn arrow_array::Array>
                }
                "entity_id" => {
                    // Ensure entity_id is Int32
                    let existing = batch.column(i);
                    if existing.data_type() != &DataType::Int32 {
                        cast(existing, &DataType::Int32)?
                    } else {
                        existing.clone()
                    }
                }
                _ => batch.column(i).clone(),
            };
            columns.push(col);
        }

        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

#[async_trait]
impl<S: AsyncStore> AsyncUpdater for UpdateManager<S> {
    async fn update(
        &self,
        batch: RecordBatch,
        sig: &ArchetypeSignature,
        tick: i32,
        world_id: &str,
        run_id: &str,
    ) -> Result<RecordBatch> {
        let start = Instant::now();

        // Stamp metadata
        let stamped = self.stamp_batch(&batch, tick, world_id, run_id)?;

        // Append to store
        self.store.append(sig, stamped.clone()).await?;

        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        info!(
            archetype = %Archetype::get_name(sig),
            world_id = %world_id,
            run_id = %run_id,
            tick = %tick,
            rows = %stamped.num_rows(),
            duration_ms = %format!("{:.3}", duration_ms),
            "Append committed"
        );

        Ok(stamped)
    }
}

/// Type alias for shared updater.
pub type SharedUpdater = Arc<dyn AsyncUpdater>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::ComponentMeta;
    use crate::store::InMemoryStore;
    use arrow_array::BooleanArray;
    use arrow_schema::{DataType, Field, Schema};

    fn test_signature() -> ArchetypeSignature {
        let meta = ComponentMeta::new("TestComponent", "testcomponent__");
        let schema = Schema::new(vec![Field::new("testcomponent__value", DataType::Int32, false)]);
        ArchetypeSignature::new(vec![(meta, schema)])
    }

    fn test_batch_unstamped() -> RecordBatch {
        let sig = test_signature();
        let schema = Archetype::get_schema(&sig);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![""])),  // world_id (placeholder)
                Arc::new(StringArray::from(vec![""])),  // run_id (placeholder)
                Arc::new(Int32Array::from(vec![1])),    // entity_id
                Arc::new(Int32Array::from(vec![0])),    // tick (placeholder)
                Arc::new(BooleanArray::from(vec![true])), // is_active
                Arc::new(Int32Array::from(vec![42])),   // testcomponent__value
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_update_manager_stamps_metadata() {
        let store = Arc::new(InMemoryStore::new());
        let sig = test_signature();

        store.ensure_table(&sig).await.unwrap();

        let updater = UpdateManager::new(store.clone());
        let batch = test_batch_unstamped();

        let result = updater
            .update(batch, &sig, 5, "test_world", "test_run")
            .await
            .unwrap();

        // Check stamped values
        let world_id = result
            .column_by_name("world_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(world_id.value(0), "test_world");

        let run_id = result
            .column_by_name("run_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(run_id.value(0), "test_run");

        let tick = result
            .column_by_name("tick")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(tick.value(0), 5);
    }

    #[tokio::test]
    async fn test_update_persists_to_store() {
        let store = Arc::new(InMemoryStore::new());
        let sig = test_signature();

        store.ensure_table(&sig).await.unwrap();

        let updater = UpdateManager::new(store.clone());
        let batch = test_batch_unstamped();

        updater
            .update(batch, &sig, 0, "world1", "run1")
            .await
            .unwrap();

        assert_eq!(store.row_count(), 1);
    }
}
