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

//! Storage backend for archetype tables.
//!
//! The Store manages physical storage of entity data, providing append-only
//! semantics for time-travel queries and efficient columnar storage.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::info;

use crate::archetype::{Archetype, ArchetypeSignature};
use crate::error::Result;

/// Trait for async storage backends.
#[async_trait]
pub trait AsyncStore: Send + Sync {
    /// Gets the DataFrame for an archetype signature.
    async fn get_archetype_df(
        &self,
        sig: &ArchetypeSignature,
        world_id: &str,
        run_id: &str,
    ) -> Result<Vec<RecordBatch>>;

    /// Appends a record batch to the archetype table.
    async fn append(&self, sig: &ArchetypeSignature, batch: RecordBatch) -> Result<()>;

    /// Ensures a table exists for the archetype signature.
    async fn ensure_table(&self, sig: &ArchetypeSignature) -> Result<Schema>;

    /// Shuts down the store, flushing any pending writes.
    async fn shutdown(&self) -> Result<()>;
}

/// In-memory store for testing and development.
#[derive(Debug, Default)]
pub struct InMemoryStore {
    /// Tables keyed by archetype name.
    tables: RwLock<HashMap<String, Vec<RecordBatch>>>,
    /// Schemas keyed by archetype name.
    schemas: RwLock<HashMap<String, Schema>>,
}

impl InMemoryStore {
    /// Creates a new in-memory store.
    pub fn new() -> Self {
        InMemoryStore {
            tables: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Gets the number of tables in the store.
    pub fn table_count(&self) -> usize {
        self.tables.read().len()
    }

    /// Gets the total number of batches across all tables.
    pub fn batch_count(&self) -> usize {
        self.tables.read().values().map(|v| v.len()).sum()
    }

    /// Gets the total number of rows across all tables.
    pub fn row_count(&self) -> usize {
        self.tables
            .read()
            .values()
            .flat_map(|v| v.iter())
            .map(|b| b.num_rows())
            .sum()
    }
}

#[async_trait]
impl AsyncStore for InMemoryStore {
    async fn get_archetype_df(
        &self,
        sig: &ArchetypeSignature,
        world_id: &str,
        run_id: &str,
    ) -> Result<Vec<RecordBatch>> {
        let name = Archetype::get_name(sig);
        let tables = self.tables.read();

        if let Some(batches) = tables.get(&name) {
            // Filter batches by world_id and run_id
            let filtered: Vec<RecordBatch> = batches
                .iter()
                .filter(|batch| {
                    // Get world_id and run_id columns and filter
                    if let (Ok(world_col), Ok(run_col)) = (
                        batch
                            .column_by_name("world_id")
                            .ok_or(())
                            .and_then(|c| {
                                c.as_any()
                                    .downcast_ref::<arrow_array::StringArray>()
                                    .ok_or(())
                            }),
                        batch.column_by_name("run_id").ok_or(()).and_then(|c| {
                            c.as_any()
                                .downcast_ref::<arrow_array::StringArray>()
                                .ok_or(())
                        }),
                    ) {
                        // Check if any row matches
                        (0..batch.num_rows()).any(|i| {
                            world_col.value(i) == world_id && run_col.value(i) == run_id
                        })
                    } else {
                        false
                    }
                })
                .cloned()
                .collect();
            Ok(filtered)
        } else {
            Ok(Vec::new())
        }
    }

    async fn append(&self, sig: &ArchetypeSignature, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            info!(
                archetype = %Archetype::get_name(sig),
                "Skipping append of empty batch"
            );
            return Ok(());
        }

        let name = Archetype::get_name(sig);
        let mut tables = self.tables.write();

        tables.entry(name.clone()).or_default().push(batch.clone());

        info!(
            archetype = %name,
            rows = batch.num_rows(),
            "Appended batch to in-memory store"
        );

        Ok(())
    }

    async fn ensure_table(&self, sig: &ArchetypeSignature) -> Result<Schema> {
        let name = Archetype::get_name(sig);
        let schema = Archetype::get_schema(sig);

        let mut schemas = self.schemas.write();
        schemas.entry(name.clone()).or_insert_with(|| schema.clone());

        let mut tables = self.tables.write();
        tables.entry(name).or_default();

        Ok(schema)
    }

    async fn shutdown(&self) -> Result<()> {
        // In-memory store doesn't need cleanup
        Ok(())
    }
}

/// A store wrapper that provides caching functionality.
pub struct CachedStore<S: AsyncStore> {
    inner: S,
    cache: RwLock<HashMap<String, Vec<RecordBatch>>>,
    cache_enabled: bool,
}

impl<S: AsyncStore> CachedStore<S> {
    /// Creates a new cached store wrapping an inner store.
    pub fn new(inner: S, cache_enabled: bool) -> Self {
        CachedStore {
            inner,
            cache: RwLock::new(HashMap::new()),
            cache_enabled,
        }
    }

    /// Clears the cache.
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }

    /// Returns the inner store.
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

#[async_trait]
impl<S: AsyncStore> AsyncStore for CachedStore<S> {
    async fn get_archetype_df(
        &self,
        sig: &ArchetypeSignature,
        world_id: &str,
        run_id: &str,
    ) -> Result<Vec<RecordBatch>> {
        if self.cache_enabled {
            let cache_key = format!("{}:{}:{}", Archetype::get_name(sig), world_id, run_id);
            if let Some(cached) = self.cache.read().get(&cache_key) {
                return Ok(cached.clone());
            }
        }

        let result = self.inner.get_archetype_df(sig, world_id, run_id).await?;

        if self.cache_enabled {
            let cache_key = format!("{}:{}:{}", Archetype::get_name(sig), world_id, run_id);
            self.cache.write().insert(cache_key, result.clone());
        }

        Ok(result)
    }

    async fn append(&self, sig: &ArchetypeSignature, batch: RecordBatch) -> Result<()> {
        // Invalidate cache on writes
        if self.cache_enabled {
            self.cache.write().clear();
        }
        self.inner.append(sig, batch).await
    }

    async fn ensure_table(&self, sig: &ArchetypeSignature) -> Result<Schema> {
        self.inner.ensure_table(sig).await
    }

    async fn shutdown(&self) -> Result<()> {
        self.cache.write().clear();
        self.inner.shutdown().await
    }
}

/// Type alias for Arc-wrapped stores.
pub type SharedStore = Arc<dyn AsyncStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::ComponentMeta;
    use arrow_array::{BooleanArray, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_signature() -> ArchetypeSignature {
        let meta = ComponentMeta::new("TestComponent", "testcomponent__");
        let schema = Schema::new(vec![Field::new("testcomponent__value", DataType::Int32, false)]);
        ArchetypeSignature::new(vec![(meta, schema)])
    }

    fn test_batch(world_id: &str, run_id: &str, entity_id: i32, tick: i32) -> RecordBatch {
        let sig = test_signature();
        let schema = Archetype::get_schema(&sig);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![world_id])),
                Arc::new(StringArray::from(vec![run_id])),
                Arc::new(Int32Array::from(vec![entity_id])),
                Arc::new(Int32Array::from(vec![tick])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int32Array::from(vec![42])), // testcomponent__value
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_in_memory_store_append() {
        let store = InMemoryStore::new();
        let sig = test_signature();

        store.ensure_table(&sig).await.unwrap();

        let batch = test_batch("world1", "run1", 1, 0);
        store.append(&sig, batch).await.unwrap();

        assert_eq!(store.table_count(), 1);
        assert_eq!(store.batch_count(), 1);
        assert_eq!(store.row_count(), 1);
    }

    #[tokio::test]
    async fn test_in_memory_store_query() {
        let store = InMemoryStore::new();
        let sig = test_signature();

        store.ensure_table(&sig).await.unwrap();

        // Add batches for different world/run combinations
        store
            .append(&sig, test_batch("world1", "run1", 1, 0))
            .await
            .unwrap();
        store
            .append(&sig, test_batch("world1", "run2", 2, 0))
            .await
            .unwrap();
        store
            .append(&sig, test_batch("world2", "run1", 3, 0))
            .await
            .unwrap();

        // Query specific world/run
        let result = store.get_archetype_df(&sig, "world1", "run1").await.unwrap();
        assert_eq!(result.len(), 1);

        let result = store.get_archetype_df(&sig, "world1", "run2").await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_cached_store() {
        let inner = InMemoryStore::new();
        let store = CachedStore::new(inner, true);
        let sig = test_signature();

        store.ensure_table(&sig).await.unwrap();
        store
            .append(&sig, test_batch("world1", "run1", 1, 0))
            .await
            .unwrap();

        // First query should populate cache
        let result1 = store.get_archetype_df(&sig, "world1", "run1").await.unwrap();
        assert_eq!(result1.len(), 1);

        // Second query should hit cache
        let result2 = store.get_archetype_df(&sig, "world1", "run1").await.unwrap();
        assert_eq!(result2.len(), 1);
    }

    #[tokio::test]
    async fn test_empty_batch_skip() {
        let store = InMemoryStore::new();
        let sig = test_signature();
        let schema = Archetype::get_schema(&sig);

        store.ensure_table(&sig).await.unwrap();

        // Create empty batch
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        store.append(&sig, empty_batch).await.unwrap();

        assert_eq!(store.batch_count(), 0);
    }
}
