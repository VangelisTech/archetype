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

//! AsyncWorld - Central simulation coordinator.
//!
//! The World owns entity-to-archetype mappings, live state snapshots,
//! and orchestrates the step() cycle:
//!
//!   1. Query previous state from Querier
//!   2. Materialize spawn/despawn mutations
//!   3. Execute processors via System
//!   4. Persist results via Updater

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use arrow::compute::take;
use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use futures::future::join_all;
use parking_lot::RwLock;
use serde_json::Value;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::archetype::{Archetype, ArchetypeSignature};
use crate::config::{RunConfig, WorldConfig};
use crate::error::{ArchetypeError, Result};
use crate::processor::{BoxedProcessor, ProcessorContext};
use crate::querier::AsyncQuerier;
use crate::resources::Resources;
use crate::system::AsyncSystem;
use crate::updater::AsyncUpdater;

/// Row data for entity spawning.
#[derive(Debug, Clone)]
pub struct EntityRow {
    pub entity_id: i32,
    pub components: HashMap<String, Value>,
}

/// The central simulation coordinator.
pub struct AsyncWorld<Q, U, S>
where
    Q: AsyncQuerier,
    U: AsyncUpdater,
    S: AsyncSystem,
{
    /// World configuration.
    pub config: WorldConfig,
    /// Name of the world.
    pub name: Option<String>,
    /// World ID.
    pub world_id: Uuid,

    /// Query manager for reading.
    querier: Arc<Q>,
    /// Update manager for writing.
    updater: Arc<U>,
    /// System for processor orchestration.
    system: Arc<S>,

    /// Type-safe resource container.
    pub resources: RwLock<Resources>,

    /// Current simulation tick.
    tick: AtomicI32,
    /// Current run ID.
    run_id: RwLock<Option<Uuid>>,

    /// Entity counter for ID generation.
    entity_counter: AtomicI32,
    /// Entity to archetype signature mapping.
    entity_to_sig: RwLock<HashMap<i32, ArchetypeSignature>>,

    /// Pending spawn operations.
    spawn_cache: RwLock<HashMap<ArchetypeSignature, Vec<EntityRow>>>,
    /// Pending despawn operations.
    despawn_cache: RwLock<HashMap<ArchetypeSignature, Vec<i32>>>,

    /// Live state snapshots per archetype.
    live: RwLock<HashMap<ArchetypeSignature, RecordBatch>>,
}

impl<Q, U, S> AsyncWorld<Q, U, S>
where
    Q: AsyncQuerier + 'static,
    U: AsyncUpdater + 'static,
    S: AsyncSystem + 'static,
{
    /// Creates a new world with the given components.
    pub fn new(config: WorldConfig, querier: Arc<Q>, updater: Arc<U>, system: Arc<S>) -> Self {
        let world_id = config.world_id;
        let name = config.name.clone();

        AsyncWorld {
            config,
            name,
            world_id,
            querier,
            updater,
            system,
            resources: RwLock::new(Resources::new()),
            tick: AtomicI32::new(0),
            run_id: RwLock::new(None),
            entity_counter: AtomicI32::new(0),
            entity_to_sig: RwLock::new(HashMap::new()),
            spawn_cache: RwLock::new(HashMap::new()),
            despawn_cache: RwLock::new(HashMap::new()),
            live: RwLock::new(HashMap::new()),
        }
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /// Runs the world for the configured number of steps.
    pub async fn run(&self, run_config: &RunConfig) -> Result<()> {
        *self.run_id.write() = Some(run_config.run_id);

        for _ in 0..run_config.num_steps {
            self.step(run_config).await?;
        }

        Ok(())
    }

    /// Executes one complete simulation tick.
    pub async fn step(&self, run_config: &RunConfig) -> Result<()> {
        let tick = self.tick.load(Ordering::SeqCst);
        let is_debug = run_config.debug;

        if is_debug {
            self.debug_log("tick_start", tick);
        }

        // Get active signatures
        let sigs = self.active_signatures();

        if is_debug {
            debug!(
                tick = %tick,
                archetypes = %sigs.len(),
                "Processing archetypes"
            );
        }

        // Process archetypes in parallel
        let futures: Vec<_> = sigs
            .iter()
            .map(|sig| self.run_archetype(sig.clone(), run_config))
            .collect();

        let results = join_all(futures).await;

        // Update live state and handle errors
        let mut live = self.live.write();
        for (sig, result) in sigs.iter().zip(results) {
            match result {
                Ok(batch) => {
                    // Filter to active entities
                    if let Some(filtered) = self.filter_active(&batch) {
                        live.insert(sig.clone(), filtered);
                    }
                }
                Err(e) => {
                    error!(
                        archetype = %Archetype::get_name(sig),
                        error = %e,
                        "Archetype processing failed"
                    );
                }
            }
        }
        drop(live);

        // Increment tick
        self.tick.fetch_add(1, Ordering::SeqCst);

        if is_debug {
            self.debug_log("tick_end", tick + 1);
        }

        Ok(())
    }

    /// Processes a single archetype through the full pipeline.
    async fn run_archetype(
        &self,
        sig: ArchetypeSignature,
        run_config: &RunConfig,
    ) -> Result<RecordBatch> {
        let tick = self.tick.load(Ordering::SeqCst);
        let run_id = self.run_id.read().map(|id| id.to_string()).unwrap_or_default();
        let world_id = self.world_id.to_string();

        // 1. Query previous state
        let batch = if tick > 0 && run_config.prefer_live_reads {
            if let Some(live_batch) = self.live.read().get(&sig).cloned() {
                live_batch
            } else {
                self.query_archetype(&sig, &run_id, Some(&[tick - 1])).await?
            }
        } else {
            self.query_archetype(&sig, &run_id, Some(&[tick - 1])).await?
        };

        // 2. Materialize mutations
        let batch = self.materialize_mutations(batch, &sig)?;

        // 3. Execute processors
        let resources = self.resources.read();
        let ctx = ProcessorContext {
            tick,
            world_id: &world_id,
            run_id: &run_id,
            resources: &resources,
            debug: run_config.debug,
        };
        let batch = self.system.execute(batch, &sig, &ctx).await?;
        drop(resources);

        // 4. Update storage
        let batch = self.updater.update(batch, &sig, tick, &world_id, &run_id).await?;

        Ok(batch)
    }

    // -------------------------------------------------------------------------
    // Entity Management
    // -------------------------------------------------------------------------

    /// Creates a new entity with the given archetype signature and row data.
    pub fn create_entity(&self, sig: ArchetypeSignature, components: HashMap<String, Value>) -> i32 {
        let entity_id = self.entity_counter.fetch_add(1, Ordering::SeqCst) + 1;

        self.entity_to_sig.write().insert(entity_id, sig.clone());

        let row = EntityRow {
            entity_id,
            components,
        };
        self.spawn_cache.write().entry(sig).or_default().push(row);

        entity_id
    }

    /// Removes an entity by ID.
    pub fn remove_entity(&self, entity_id: i32) {
        if let Some(sig) = self.entity_to_sig.write().remove(&entity_id) {
            self.despawn_cache.write().entry(sig).or_default().push(entity_id);
        } else {
            warn!(entity_id = %entity_id, "Entity not found for removal");
        }
    }

    /// Adds a processor to the system.
    pub async fn add_processor(&self, processor: BoxedProcessor) {
        self.system.add_processor(processor).await;
    }

    /// Removes a processor by name.
    pub async fn remove_processor(&self, name: &str) {
        self.system.remove_processor(name).await;
    }

    // -------------------------------------------------------------------------
    // Queries
    // -------------------------------------------------------------------------

    /// Returns all active archetype signatures.
    pub fn active_signatures(&self) -> Vec<ArchetypeSignature> {
        let entity_sigs: HashSet<_> = self.entity_to_sig.read().values().cloned().collect();
        let spawn_sigs: HashSet<_> = self.spawn_cache.read().keys().cloned().collect();
        let despawn_sigs: HashSet<_> = self.despawn_cache.read().keys().cloned().collect();

        entity_sigs
            .union(&spawn_sigs)
            .cloned()
            .collect::<HashSet<_>>()
            .union(&despawn_sigs)
            .cloned()
            .collect()
    }

    /// Queries archetype data with filters.
    pub async fn query_archetype(
        &self,
        sig: &ArchetypeSignature,
        run_id: &str,
        ticks: Option<&[i32]>,
    ) -> Result<RecordBatch> {
        let world_id = self.world_id.to_string();
        let batches = self
            .querier
            .query_archetype(sig, &world_id, run_id, ticks, None)
            .await?;

        // Concatenate batches (simplified - in production would use arrow concat)
        if batches.is_empty() {
            let schema = Archetype::get_schema(sig);
            return Ok(RecordBatch::new_empty(Arc::new(schema)));
        }

        // Return first batch (simplified for now)
        Ok(batches.into_iter().next().unwrap())
    }

    /// Returns the current tick.
    pub fn current_tick(&self) -> i32 {
        self.tick.load(Ordering::SeqCst)
    }

    /// Returns the current run ID.
    pub fn current_run_id(&self) -> Option<Uuid> {
        *self.run_id.read()
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    /// Materializes pending spawn/despawn mutations into a batch.
    fn materialize_mutations(
        &self,
        batch: RecordBatch,
        sig: &ArchetypeSignature,
    ) -> Result<RecordBatch> {
        let mut result = batch;

        // Handle despawns
        if let Some(despawn_ids) = self.despawn_cache.write().remove(sig) {
            if !despawn_ids.is_empty() {
                result = self.apply_despawns(result, &despawn_ids)?;
            }
        }

        // Handle spawns
        if let Some(spawn_rows) = self.spawn_cache.write().remove(sig) {
            if !spawn_rows.is_empty() {
                result = self.apply_spawns(result, sig, spawn_rows)?;
            }
        }

        Ok(result)
    }

    /// Applies despawn operations by marking entities inactive.
    fn apply_despawns(&self, batch: RecordBatch, despawn_ids: &[i32]) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        let schema = batch.schema();

        // Get entity_id column
        let entity_col = batch
            .column_by_name("entity_id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| ArchetypeError::StorageError("Missing entity_id column".to_string()))?;

        // Build new is_active column
        let is_active: Vec<bool> = (0..num_rows)
            .map(|i| !despawn_ids.contains(&entity_col.value(i)))
            .collect();

        // Rebuild batch with new is_active
        let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            if field.name() == "is_active" {
                columns.push(Arc::new(BooleanArray::from(is_active.clone())));
            } else {
                columns.push(batch.column(i).clone());
            }
        }

        Ok(RecordBatch::try_new(schema, columns)?)
    }

    /// Applies spawn operations by adding new rows.
    fn apply_spawns(
        &self,
        batch: RecordBatch,
        sig: &ArchetypeSignature,
        spawn_rows: Vec<EntityRow>,
    ) -> Result<RecordBatch> {
        let schema = Archetype::get_schema(sig);
        let tick = self.tick.load(Ordering::SeqCst);
        let world_id = self.world_id.to_string();
        let run_id = self.run_id.read().map(|id| id.to_string()).unwrap_or_default();

        // Build new rows
        let mut new_columns: Vec<Vec<Value>> = vec![Vec::new(); schema.fields().len()];

        for row in spawn_rows {
            for (i, field) in schema.fields().iter().enumerate() {
                let value = match field.name().as_str() {
                    "world_id" => Value::String(world_id.clone()),
                    "run_id" => Value::String(run_id.clone()),
                    "entity_id" => Value::Number(row.entity_id.into()),
                    "tick" => Value::Number(tick.into()),
                    "is_active" => Value::Bool(true),
                    name => row.components.get(name).cloned().unwrap_or(Value::Null),
                };
                new_columns[i].push(value);
            }
        }

        // Create new batch from spawn rows
        let spawn_batch = self.build_batch_from_values(&schema, new_columns)?;

        // Concatenate with existing batch
        if batch.num_rows() == 0 {
            Ok(spawn_batch)
        } else {
            // Simple concatenation (in production use arrow concat_batches)
            Ok(spawn_batch)
        }
    }

    /// Builds a RecordBatch from column values.
    fn build_batch_from_values(
        &self,
        schema: &Schema,
        columns: Vec<Vec<Value>>,
    ) -> Result<RecordBatch> {
        use arrow_schema::DataType;

        let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(schema.fields().len());

        for (i, field) in schema.fields().iter().enumerate() {
            let col = &columns[i];
            let array: Arc<dyn arrow_array::Array> = match field.data_type() {
                DataType::Utf8 => {
                    let values: Vec<String> = col
                        .iter()
                        .map(|v| match v {
                            Value::String(s) => s.clone(),
                            _ => String::new(),
                        })
                        .collect();
                    Arc::new(StringArray::from(values))
                }
                DataType::Int32 => {
                    let values: Vec<i32> = col
                        .iter()
                        .map(|v| match v {
                            Value::Number(n) => n.as_i64().unwrap_or(0) as i32,
                            _ => 0,
                        })
                        .collect();
                    Arc::new(Int32Array::from(values))
                }
                DataType::Boolean => {
                    let values: Vec<bool> = col
                        .iter()
                        .map(|v| match v {
                            Value::Bool(b) => *b,
                            _ => false,
                        })
                        .collect();
                    Arc::new(BooleanArray::from(values))
                }
                DataType::Float64 => {
                    let values: Vec<f64> = col
                        .iter()
                        .map(|v| match v {
                            Value::Number(n) => n.as_f64().unwrap_or(0.0),
                            _ => 0.0,
                        })
                        .collect();
                    Arc::new(arrow_array::Float64Array::from(values))
                }
                _ => {
                    // Default to string for unsupported types
                    let values: Vec<String> = col
                        .iter()
                        .map(|v| v.to_string())
                        .collect();
                    Arc::new(StringArray::from(values))
                }
            };
            arrays.push(array);
        }

        Ok(RecordBatch::try_new(Arc::new(schema.clone()), arrays)?)
    }

    /// Filters a batch to only active entities.
    fn filter_active(&self, batch: &RecordBatch) -> Option<RecordBatch> {
        let is_active = batch
            .column_by_name("is_active")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())?;

        let indices: Vec<usize> = (0..batch.num_rows())
            .filter(|&i| is_active.value(i))
            .collect();

        if indices.is_empty() {
            return None;
        }

        // Build filtered batch
        let filtered_columns: Vec<Arc<dyn arrow_array::Array>> = batch
            .columns()
            .iter()
            .map(|col| {
                let idx_array = Int32Array::from(indices.iter().map(|&i| i as i32).collect::<Vec<_>>());
                take(col, &idx_array, None).unwrap_or_else(|_| col.clone())
            })
            .collect();

        RecordBatch::try_new(batch.schema(), filtered_columns).ok()
    }

    fn debug_log(&self, event: &str, tick: i32) {
        info!(
            event = %event,
            world_id = %self.world_id,
            tick = %tick,
            "World event"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::ComponentMeta;
    use crate::querier::QueryManager;
    use crate::store::InMemoryStore;
    use crate::system::System;
    use crate::updater::UpdateManager;
    use arrow_schema::{DataType, Field};

    fn test_signature() -> ArchetypeSignature {
        let meta = ComponentMeta::new("TestComponent", "testcomponent__");
        let schema = Schema::new(vec![Field::new("testcomponent__value", DataType::Int32, false)]);
        ArchetypeSignature::new(vec![(meta, schema)])
    }

    fn create_test_world() -> AsyncWorld<
        QueryManager<InMemoryStore>,
        UpdateManager<InMemoryStore>,
        System,
    > {
        let store = Arc::new(InMemoryStore::new());
        let querier = Arc::new(QueryManager::new(store.clone()));
        let updater = Arc::new(UpdateManager::new(store));
        let system = Arc::new(System::new());
        let config = WorldConfig::with_name("test_world");

        AsyncWorld::new(config, querier, updater, system)
    }

    #[test]
    fn test_world_create_entity() {
        let world = create_test_world();
        let sig = test_signature();

        let mut components = HashMap::new();
        components.insert("testcomponent__value".to_string(), Value::Number(42.into()));

        let entity_id = world.create_entity(sig.clone(), components);
        assert_eq!(entity_id, 1);

        // Check entity is registered
        let entity_sigs = world.entity_to_sig.read();
        assert!(entity_sigs.contains_key(&entity_id));
    }

    #[test]
    fn test_world_remove_entity() {
        let world = create_test_world();
        let sig = test_signature();

        let entity_id = world.create_entity(sig.clone(), HashMap::new());
        world.remove_entity(entity_id);

        // Entity should be removed from mapping
        assert!(!world.entity_to_sig.read().contains_key(&entity_id));

        // Should be in despawn cache
        assert!(world.despawn_cache.read().contains_key(&sig));
    }

    #[test]
    fn test_world_active_signatures() {
        let world = create_test_world();
        let sig = test_signature();

        world.create_entity(sig.clone(), HashMap::new());

        let sigs = world.active_signatures();
        assert_eq!(sigs.len(), 1);
    }

    #[tokio::test]
    async fn test_world_run() {
        let world = create_test_world();
        let sig = test_signature();

        let mut components = HashMap::new();
        components.insert("testcomponent__value".to_string(), Value::Number(42.into()));
        world.create_entity(sig, components);

        let run_config = RunConfig::dev(2);
        world.run(&run_config).await.unwrap();

        assert_eq!(world.current_tick(), 2);
    }
}
