//! Tick lifecycle (per table within one `step_profiled` call):
//!
//! ```text
//! Tick N:
//!   1. Read prior active rows (live snapshot from tick N-1, or store).
//!   2. Apply staged despawns: tombstone matching rows in prior.
//!      Result = processor_input (only previously-live rows, with despawned ones
//!      marked is_active=false).
//!   3. Execute processors over processor_input.  Spawns queued this tick are NOT
//!      visible to processors — they land raw.
//!   4. Read staged spawns: deduplicate (last-write-wins, first-seen order), concat
//!      raw spawn rows AFTER processor output.
//!      Invariant: x₀ is the raw spawn value; x_{t+1} = f(x_t).
//!   5. Stamp tick/world/run metadata on the full concatenated batch.
//!   6. Persist the stamped batch.
//!   7. Consume that table's mutations and update its live snapshot. The snapshot
//!      is active_snapshot(stamped), which includes raw spawn
//!      rows (is_active=true) so they are transformed for the first time on tick N+1.
//! ```
//!
//! A world prepares every active table before the first append. Store appends
//! are atomic per table, not across tables; failure after an earlier table was
//! published poisons the world so tick N cannot be retried and duplicated.
//! `step_table` and `step_table_profiled` are complete-tick conveniences for
//! single-table worlds only: they reject multi-table use before any append and
//! advance the shared tick after a successful append.
//!
//! Same-tick spawn-cancel (despawn while spawn is still pending) removes the
//! spawn row entirely without writing a tombstone — the entity is never observed.
//! Despawn of a previously-live entity produces a tombstone row (is_active=false)
//! that is processed and persisted on the despawn tick, then excluded from the
//! live snapshot.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, SchemaRef};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use uuid::Uuid;

use crate::aio::async_processor::ProcessorContext;
use crate::aio::async_store::{ReadFilter, Store};
use crate::aio::async_system::AsyncSystem;
use crate::archetype::{ArchetypeTable, ENTITY_ID, IS_ACTIVE, RUN_ID, TICK, WORLD_ID};
use crate::error::{ArchetypeCoreError, Result};

pub type EntityId = i32;

#[derive(Debug, Clone, Default)]
pub struct TableStepProfile {
    pub table_name: String,
    pub read_prior_sec: f64,
    pub materialize_sec: f64,
    pub process_sec: f64,
    pub append_sec: f64,
    pub live_snapshot_sec: f64,
    pub table_sec: f64,
}

#[derive(Debug, Clone, Default)]
pub struct StepProfile {
    pub tick: i32,
    pub tables: Vec<TableStepProfile>,
    pub tick_sec: f64,
}

struct PreparedTableStep {
    batch: RecordBatch,
    live_snapshot: RecordBatch,
    profile: TableStepProfile,
}

#[derive(Debug, Clone)]
struct PartialTickFailure {
    tick: i32,
    committed_tables: usize,
    cause: String,
}

impl PartialTickFailure {
    fn as_error(&self) -> ArchetypeCoreError {
        ArchetypeCoreError::PartialTick {
            tick: self.tick,
            committed_tables: self.committed_tables,
            cause: self.cause.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct MutationBuffer {
    spawns: HashMap<String, Vec<RecordBatch>>,
    despawns: HashMap<String, Vec<EntityId>>,
}

#[derive(Debug)]
pub struct WorldState {
    world_id: String,
    run_id: String,
    tick: i32,
    next_entity_id: EntityId,
    entity2table: HashMap<EntityId, String>,
    mutations: MutationBuffer,
    partial_failure: Option<PartialTickFailure>,
}

impl WorldState {
    pub fn new(world_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            world_id: world_id.into(),
            run_id: run_id.into(),
            tick: 0,
            next_entity_id: 1,
            entity2table: HashMap::new(),
            mutations: MutationBuffer::default(),
            partial_failure: None,
        }
    }

    pub fn new_uuid() -> Self {
        Self::new(Uuid::now_v7().to_string(), Uuid::now_v7().to_string())
    }

    pub fn world_id(&self) -> &str {
        &self.world_id
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn tick(&self) -> i32 {
        self.tick
    }

    pub fn active_tables(&self) -> HashSet<String> {
        self.entity2table
            .values()
            .cloned()
            .chain(self.mutations.spawns.keys().cloned())
            .chain(self.mutations.despawns.keys().cloned())
            .collect()
    }

    pub fn queue_spawn_batch(
        &mut self,
        table_name: impl Into<String>,
        table_schema: SchemaRef,
        component_batch: RecordBatch,
    ) -> Result<Vec<EntityId>> {
        self.ensure_healthy()?;
        let table_name = table_name.into();
        let rows = component_batch.num_rows();
        let entity_ids = (0..rows)
            .map(|idx| self.next_entity_id + idx as EntityId)
            .collect::<Vec<_>>();

        self.queue_spawn_batch_with_entity_ids(
            table_name,
            table_schema,
            component_batch,
            &entity_ids,
        )?;
        Ok(entity_ids)
    }

    pub fn queue_spawn_batch_with_entity_ids(
        &mut self,
        table_name: impl Into<String>,
        table_schema: SchemaRef,
        component_batch: RecordBatch,
        entity_ids: &[EntityId],
    ) -> Result<()> {
        self.ensure_healthy()?;
        let table_name = table_name.into();
        let spawn = stamp_spawn_batch(
            table_schema,
            component_batch,
            entity_ids,
            self.tick,
            &self.world_id,
            &self.run_id,
        )?;
        self.queue_stamped_spawn_batch(table_name, spawn, entity_ids);
        Ok(())
    }

    fn queue_stamped_spawn_batch(
        &mut self,
        table_name: String,
        spawn: RecordBatch,
        entity_ids: &[EntityId],
    ) {
        for entity_id in entity_ids {
            self.entity2table.insert(*entity_id, table_name.clone());
            self.next_entity_id = self.next_entity_id.max(*entity_id + 1);
        }
        self.mutations
            .spawns
            .entry(table_name)
            .or_default()
            .push(spawn);
    }

    fn queue_migration_batch(
        &mut self,
        table_name: impl Into<String>,
        table_schema: SchemaRef,
        component_batch: RecordBatch,
        entity_ids: &[EntityId],
    ) -> Result<()> {
        self.ensure_healthy()?;
        let table_name = table_name.into();
        let spawn = stamp_spawn_batch(
            table_schema,
            component_batch,
            entity_ids,
            self.tick,
            &self.world_id,
            &self.run_id,
        )?;

        let mut source_entities = HashMap::<String, Vec<EntityId>>::new();
        for entity_id in entity_ids {
            let source = self
                .entity2table
                .get(entity_id)
                .ok_or(ArchetypeCoreError::UnknownEntity(*entity_id))?;
            source_entities
                .entry(source.clone())
                .or_default()
                .push(*entity_id);
        }

        let mut retained_spawns = HashMap::new();
        let mut cancelled_spawns = HashSet::new();
        for (source, ids) in &source_entities {
            let Some(batches) = self.mutations.spawns.get(source) else {
                continue;
            };
            let targets = ids.iter().copied().collect::<HashSet<_>>();
            let (retained, removed) = remove_entities_from_spawn_batches(batches, &targets)?;
            retained_spawns.insert(source.clone(), retained);
            cancelled_spawns.extend(removed);
        }

        for (source, retained) in retained_spawns {
            if retained.is_empty() {
                self.mutations.spawns.remove(&source);
            } else {
                self.mutations.spawns.insert(source, retained);
            }
        }
        for (source, ids) in source_entities {
            let tombstones = ids
                .into_iter()
                .filter(|entity_id| !cancelled_spawns.contains(entity_id))
                .collect::<Vec<_>>();
            if !tombstones.is_empty() {
                self.mutations
                    .despawns
                    .entry(source)
                    .or_default()
                    .extend(tombstones);
            }
        }
        self.queue_stamped_spawn_batch(table_name, spawn, entity_ids);
        Ok(())
    }

    pub fn queue_despawn(&mut self, entity_id: EntityId) -> Result<()> {
        self.ensure_healthy()?;
        let table_name = self
            .entity2table
            .get(&entity_id)
            .cloned()
            .ok_or(ArchetypeCoreError::UnknownEntity(entity_id))?;
        let cancelled = self.cancel_pending_spawn(&table_name, entity_id)?;
        self.entity2table.remove(&entity_id);
        if cancelled {
            return Ok(());
        }
        self.mutations
            .despawns
            .entry(table_name)
            .or_default()
            .push(entity_id);
        Ok(())
    }

    fn cancel_pending_spawn(&mut self, table_name: &str, entity_id: EntityId) -> Result<bool> {
        let Some(batches) = self.mutations.spawns.get(table_name) else {
            return Ok(false);
        };
        let targets = HashSet::from([entity_id]);
        let (retained, removed) = remove_entities_from_spawn_batches(batches, &targets)?;
        if retained.is_empty() {
            self.mutations.spawns.remove(table_name);
        } else {
            self.mutations
                .spawns
                .insert(table_name.to_string(), retained);
        }
        Ok(removed.contains(&entity_id))
    }

    /// Phase 1 of the tick lifecycle: apply queued despawns to `prior` and
    /// return the batch that processors will run on.
    ///
    /// Drains the despawn buffer for `table_name`.  If any entity IDs are
    /// pending despawn, their rows in `prior` are tombstoned (is_active=false);
    /// all other rows pass through unchanged.  The returned batch is the
    /// processor input for the current tick.
    pub fn apply_despawns_to_prior(
        &mut self,
        table_name: &str,
        table_schema: SchemaRef,
        prior: RecordBatch,
    ) -> Result<RecordBatch> {
        self.ensure_healthy()?;
        let materialized = self.staged_despawns_to_prior(table_name, table_schema, prior)?;
        self.mutations.despawns.remove(table_name);
        Ok(materialized)
    }

    fn staged_despawns_to_prior(
        &self,
        table_name: &str,
        table_schema: SchemaRef,
        prior: RecordBatch,
    ) -> Result<RecordBatch> {
        if prior.schema().fields() != table_schema.fields() {
            return Err(ArchetypeCoreError::SchemaMismatch {
                table_name: table_name.to_string(),
            });
        }
        let Some(despawns) = self.mutations.despawns.get(table_name) else {
            return Ok(prior);
        };
        if despawns.is_empty() {
            Ok(prior)
        } else {
            apply_despawns(prior, despawns)
        }
    }

    /// Phase 2 of the tick lifecycle: drain the spawn buffer for `table_name`
    /// and return the deduplicated raw spawn rows that will be concatenated
    /// AFTER processor output.
    ///
    /// Deduplication is last-write-wins within the same entity, preserving
    /// first-seen insertion order — matching the Python dict-based spawn
    /// dedupe.  Returns an empty `Vec` if nothing was queued.
    pub fn drain_spawn_frames(&mut self, table_name: &str) -> Result<Vec<RecordBatch>> {
        self.ensure_healthy()?;
        let frames = self.staged_spawn_frames(table_name)?;
        self.mutations.spawns.remove(table_name);
        Ok(frames)
    }

    fn staged_spawn_frames(&self, table_name: &str) -> Result<Vec<RecordBatch>> {
        let Some(spawns) = self.mutations.spawns.get(table_name) else {
            return Ok(Vec::new());
        };
        // Dedupe: last-write-wins for duplicate entity IDs, first-seen order.
        let mut order = Vec::<EntityId>::new();
        let mut latest_by_entity = HashMap::<EntityId, RecordBatchRow>::new();
        let mut total_rows = 0usize;
        for batch in spawns {
            let entities = entity_column(batch)?;
            for row_idx in 0..batch.num_rows() {
                let entity_id = entities.value(row_idx);
                if latest_by_entity
                    .insert(
                        entity_id,
                        RecordBatchRow {
                            batch: batch.clone(),
                            row_idx,
                        },
                    )
                    .is_none()
                {
                    order.push(entity_id);
                }
                total_rows += 1;
            }
        }
        if latest_by_entity.len() == total_rows {
            Ok(spawns.clone())
        } else {
            order
                .into_iter()
                .map(|entity_id| {
                    let row = &latest_by_entity[&entity_id];
                    take_single_row(&row.batch, row.row_idx)
                })
                .collect()
        }
    }

    fn consume_table_mutations(&mut self, table_name: &str) {
        self.mutations.spawns.remove(table_name);
        self.mutations.despawns.remove(table_name);
    }

    /// Convenience wrapper used by unit tests and migration helpers: applies
    /// despawns then drains spawns into a single concatenated batch.
    ///
    /// In the full tick path (`prepare_table_profiled`) these two phases are
    /// called separately so that processors run only on prior rows and spawns
    /// land raw after processor output.
    pub fn materialize_table(
        &mut self,
        table_name: &str,
        table_schema: SchemaRef,
        prior: RecordBatch,
    ) -> Result<RecordBatch> {
        self.ensure_healthy()?;
        let processor_input =
            self.staged_despawns_to_prior(table_name, table_schema.clone(), prior)?;
        let spawn_frames = self.staged_spawn_frames(table_name)?;
        let mut batches = vec![processor_input];
        batches.extend(spawn_frames);
        let materialized = concat_batches(&table_schema, &batches)?;
        self.consume_table_mutations(table_name);
        Ok(materialized)
    }

    pub fn advance_tick(&mut self) -> Result<()> {
        self.ensure_healthy()?;
        self.tick += 1;
        Ok(())
    }

    fn poison(&mut self, failure: PartialTickFailure) {
        self.partial_failure = Some(failure);
    }

    fn ensure_healthy(&self) -> Result<()> {
        match &self.partial_failure {
            Some(failure) => Err(failure.as_error()),
            None => Ok(()),
        }
    }
}

pub struct AsyncWorld<S>
where
    S: Store,
{
    state: WorldState,
    store: S,
    system: AsyncSystem,
    tables: HashMap<String, SchemaRef>,
    live_tables: HashMap<String, RecordBatch>,
}

impl<S> AsyncWorld<S>
where
    S: Store,
{
    pub fn new(state: WorldState, store: S, system: AsyncSystem) -> Self {
        Self {
            state,
            store,
            system,
            tables: HashMap::new(),
            live_tables: HashMap::new(),
        }
    }

    pub fn from_ids(
        world_id: impl Into<String>,
        run_id: impl Into<String>,
        store: S,
        system: AsyncSystem,
    ) -> Self {
        Self::new(WorldState::new(world_id, run_id), store, system)
    }

    pub fn state(&self) -> &WorldState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut WorldState {
        &mut self.state
    }

    pub fn system(&self) -> &AsyncSystem {
        &self.system
    }

    pub fn system_mut(&mut self) -> &mut AsyncSystem {
        &mut self.system
    }

    pub fn register_table(&mut self, table: &ArchetypeTable) {
        self.tables
            .insert(table.table_name().to_string(), table.schema());
    }

    pub fn queue_spawn_batch(
        &mut self,
        table: &ArchetypeTable,
        component_batch: RecordBatch,
    ) -> Result<Vec<EntityId>> {
        self.ensure_healthy()?;
        let entity_ids =
            self.state
                .queue_spawn_batch(table.table_name(), table.schema(), component_batch)?;
        self.register_table(table);
        Ok(entity_ids)
    }

    pub fn queue_spawn_batch_with_entity_ids(
        &mut self,
        table: &ArchetypeTable,
        component_batch: RecordBatch,
        entity_ids: &[EntityId],
    ) -> Result<()> {
        self.ensure_healthy()?;
        self.state.queue_spawn_batch_with_entity_ids(
            table.table_name(),
            table.schema(),
            component_batch,
            entity_ids,
        )?;
        self.register_table(table);
        Ok(())
    }

    pub fn queue_despawn(&mut self, entity_id: EntityId) -> Result<()> {
        self.ensure_healthy()?;
        self.state.queue_despawn(entity_id)
    }

    pub fn queue_migration_batch(
        &mut self,
        new_table: &ArchetypeTable,
        entity_ids: &[EntityId],
        new_component_batch: RecordBatch,
    ) -> Result<()> {
        self.ensure_healthy()?;
        self.state.queue_migration_batch(
            new_table.table_name(),
            new_table.schema(),
            new_component_batch,
            entity_ids,
        )?;
        self.register_table(new_table);
        Ok(())
    }

    pub async fn step(&mut self) -> Result<()> {
        self.step_profiled().await.map(|_| ())
    }

    pub async fn step_profiled(&mut self) -> Result<StepProfile> {
        self.ensure_healthy()?;
        let tick_start = Instant::now();
        let tick = self.state.tick();
        let mut table_names = self.state.active_tables().into_iter().collect::<Vec<_>>();
        table_names.sort();

        let mut prepared = Vec::with_capacity(table_names.len());
        for table_name in &table_names {
            prepared.push(self.prepare_table_profiled(table_name).await?);
        }

        let mut committed_tables = 0;
        for table in &mut prepared {
            match self.append_prepared_table(table).await {
                Ok(()) => committed_tables += usize::from(table.batch.num_rows() > 0),
                Err(error) if committed_tables == 0 => return Err(error),
                Err(error) => {
                    let failure = PartialTickFailure {
                        tick,
                        committed_tables,
                        cause: error.to_string(),
                    };
                    let error = failure.as_error();
                    self.state.poison(failure);
                    return Err(error);
                }
            }
        }

        let tables = prepared
            .into_iter()
            .map(|table| self.finalize_prepared_table(table).1)
            .collect();
        self.state.advance_tick()?;
        Ok(StepProfile {
            tick,
            tables,
            tick_sec: tick_start.elapsed().as_secs_f64(),
        })
    }

    pub async fn run(&mut self, steps: i32) -> Result<()> {
        for _ in 0..steps {
            self.step().await?;
        }
        Ok(())
    }

    pub async fn step_table(&mut self, table_name: &str) -> Result<RecordBatch> {
        self.step_table_profiled(table_name)
            .await
            .map(|(batch, _profile)| batch)
    }

    pub async fn step_table_profiled(
        &mut self,
        table_name: &str,
    ) -> Result<(RecordBatch, TableStepProfile)> {
        self.ensure_healthy()?;
        let mut active_tables = self.state.active_tables().into_iter().collect::<Vec<_>>();
        active_tables.sort();
        if active_tables.len() != 1 || active_tables[0] != table_name {
            return Err(ArchetypeCoreError::InvalidTableStep {
                table_name: table_name.to_string(),
                active_tables,
            });
        }

        let mut prepared = self.prepare_table_profiled(table_name).await?;
        self.append_prepared_table(&mut prepared).await?;
        let completed = self.finalize_prepared_table(prepared);
        self.state.advance_tick()?;
        Ok(completed)
    }

    async fn prepare_table_profiled(&self, table_name: &str) -> Result<PreparedTableStep> {
        let table_start = Instant::now();
        let schema = self
            .tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| ArchetypeCoreError::UnknownTable(table_name.to_string()))?;

        let read_prior_start = Instant::now();
        let prior = self.read_prior_table(table_name, schema.clone()).await?;
        let read_prior_sec = read_prior_start.elapsed().as_secs_f64();

        let materialize_start = Instant::now();
        let processor_input =
            self.state
                .staged_despawns_to_prior(table_name, schema.clone(), prior)?;
        let materialize_sec = materialize_start.elapsed().as_secs_f64();

        let process_start = Instant::now();
        let processed = self.system.execute(
            processor_input,
            &ProcessorContext::new(
                self.state.world_id().to_string(),
                self.state.run_id().to_string(),
                self.state.tick(),
            ),
        )?;
        let process_sec = process_start.elapsed().as_secs_f64();

        let spawn_frames = self.state.staged_spawn_frames(table_name)?;
        let combined = if spawn_frames.is_empty() {
            processed
        } else {
            let mut batches = vec![processed];
            batches.extend(spawn_frames);
            concat_batches(&schema, &batches)?
        };

        let stamped = stamp_batch_metadata(
            combined,
            self.state.tick(),
            self.state.world_id(),
            self.state.run_id(),
        )?;
        let live_snapshot_start = Instant::now();
        let live_snapshot = active_snapshot(&stamped)?;
        let live_snapshot_sec = live_snapshot_start.elapsed().as_secs_f64();
        let profile = TableStepProfile {
            table_name: table_name.to_string(),
            read_prior_sec,
            materialize_sec,
            process_sec,
            append_sec: 0.0,
            live_snapshot_sec,
            table_sec: table_start.elapsed().as_secs_f64(),
        };
        Ok(PreparedTableStep {
            batch: stamped,
            live_snapshot,
            profile,
        })
    }

    async fn append_prepared_table(&self, prepared: &mut PreparedTableStep) -> Result<()> {
        let append_start = Instant::now();
        if prepared.batch.num_rows() > 0 {
            self.store
                .append(&prepared.profile.table_name, &prepared.batch)
                .await?;
        }
        prepared.profile.append_sec = append_start.elapsed().as_secs_f64();
        prepared.profile.table_sec += prepared.profile.append_sec;
        Ok(())
    }

    fn finalize_prepared_table(
        &mut self,
        prepared: PreparedTableStep,
    ) -> (RecordBatch, TableStepProfile) {
        let table_name = prepared.profile.table_name.clone();
        self.state.consume_table_mutations(&table_name);
        self.live_tables.insert(table_name, prepared.live_snapshot);
        (prepared.batch, prepared.profile)
    }

    fn ensure_healthy(&self) -> Result<()> {
        self.state.ensure_healthy()
    }

    pub async fn query_table(
        &self,
        table_name: &str,
        mut filter: ReadFilter,
    ) -> Result<Vec<RecordBatch>> {
        filter
            .world_id
            .get_or_insert_with(|| self.state.world_id().to_string());
        filter
            .run_id
            .get_or_insert_with(|| self.state.run_id().to_string());
        self.store.read_table(table_name, filter).await
    }

    async fn read_prior_table(&self, table_name: &str, schema: SchemaRef) -> Result<RecordBatch> {
        if let Some(live) = self.live_tables.get(table_name) {
            return Ok(live.clone());
        }

        let prior_tick = self.state.tick() - 1;
        let batches = self
            .store
            .read_table(
                table_name,
                ReadFilter {
                    world_id: Some(self.state.world_id().to_string()),
                    run_id: Some(self.state.run_id().to_string()),
                    ticks: Some(vec![prior_tick]),
                    active_only: true,
                    ..ReadFilter::default()
                },
            )
            .await?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }

        concat_batches(&schema, &batches).map_err(Into::into)
    }
}

#[derive(Clone)]
struct RecordBatchRow {
    batch: RecordBatch,
    row_idx: usize,
}

fn remove_entities_from_spawn_batches(
    batches: &[RecordBatch],
    targets: &HashSet<EntityId>,
) -> Result<(Vec<RecordBatch>, HashSet<EntityId>)> {
    let mut retained = Vec::with_capacity(batches.len());
    let mut removed = HashSet::new();
    for batch in batches {
        let entities = entity_column(batch)?;
        let keep = BooleanArray::from_iter((0..batch.num_rows()).map(|idx| {
            let entity_id = entities.value(idx);
            let remove = targets.contains(&entity_id);
            if remove {
                removed.insert(entity_id);
            }
            Some(!remove)
        }));
        if keep.true_count() == batch.num_rows() {
            retained.push(batch.clone());
        } else {
            let filtered = filter_record_batch(batch, &keep)?;
            if filtered.num_rows() > 0 {
                retained.push(filtered);
            }
        }
    }
    Ok((retained, removed))
}

fn stamp_spawn_batch(
    table_schema: SchemaRef,
    component_batch: RecordBatch,
    entity_ids: &[EntityId],
    tick: i32,
    world_id: &str,
    run_id: &str,
) -> Result<RecordBatch> {
    if component_batch.num_rows() != entity_ids.len() {
        return Err(ArchetypeCoreError::InvalidRowCount {
            expected: entity_ids.len(),
            actual: component_batch.num_rows(),
        });
    }

    let rows = component_batch.num_rows();
    let columns = table_schema
        .fields()
        .iter()
        .map(|field| {
            build_column(
                field,
                rows,
                &component_batch,
                entity_ids,
                tick,
                world_id,
                run_id,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(table_schema, columns).map_err(Into::into)
}

fn build_column(
    field: &Field,
    rows: usize,
    component_batch: &RecordBatch,
    entity_ids: &[EntityId],
    tick: i32,
    world_id: &str,
    run_id: &str,
) -> Result<ArrayRef> {
    match field.name().as_str() {
        WORLD_ID => Ok(Arc::new(StringArray::from_iter_values(
            std::iter::repeat_n(world_id, rows),
        ))),
        RUN_ID => Ok(Arc::new(StringArray::from_iter_values(
            std::iter::repeat_n(run_id, rows),
        ))),
        ENTITY_ID => Ok(Arc::new(Int32Array::from(entity_ids.to_vec()))),
        TICK => Ok(Arc::new(Int32Array::from(vec![tick; rows]))),
        IS_ACTIVE => Ok(Arc::new(BooleanArray::from(vec![true; rows]))),
        other => {
            let idx = component_batch
                .schema()
                .index_of(other)
                .map_err(|_| ArchetypeCoreError::MissingColumn(other.to_string()))?;
            Ok(component_batch.column(idx).clone())
        }
    }
}

fn apply_despawns(batch: RecordBatch, entity_ids: &[EntityId]) -> Result<RecordBatch> {
    let despawn_set = entity_ids.iter().copied().collect::<HashSet<_>>();
    let entity_col = entity_column(&batch)?;

    let active_idx = batch.schema().index_of(IS_ACTIVE)?;
    let active_col = batch
        .column(active_idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ArchetypeCoreError::InvalidColumnType {
            column: IS_ACTIVE,
            expected: DataType::Boolean,
            actual: batch
                .schema()
                .field_with_name(IS_ACTIVE)
                .unwrap()
                .data_type()
                .clone(),
        })?;

    let mut active_builder = BooleanBuilder::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        if entity_col.is_null(row_idx) || active_col.is_null(row_idx) {
            active_builder.append_null();
        } else if despawn_set.contains(&entity_col.value(row_idx)) {
            active_builder.append_value(false);
        } else {
            active_builder.append_value(active_col.value(row_idx));
        }
    }

    let mut columns = batch.columns().to_vec();
    columns[active_idx] = Arc::new(active_builder.finish());
    RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
}

fn entity_column(batch: &RecordBatch) -> Result<&Int32Array> {
    batch
        .column(batch.schema().index_of(ENTITY_ID)?)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| ArchetypeCoreError::InvalidColumnType {
            column: ENTITY_ID,
            expected: DataType::Int32,
            actual: batch
                .schema()
                .field_with_name(ENTITY_ID)
                .unwrap()
                .data_type()
                .clone(),
        })
}

fn take_single_row(batch: &RecordBatch, row_idx: usize) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|col| {
            arrow_select::take::take(
                col.as_ref(),
                &arrow_array::UInt32Array::from(vec![row_idx as u32]),
                None,
            )
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
}

fn stamp_batch_metadata(
    batch: RecordBatch,
    tick: i32,
    world_id: &str,
    run_id: &str,
) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    columns[schema.index_of(WORLD_ID)?] = Arc::new(StringArray::from_iter_values(
        std::iter::repeat_n(world_id, rows),
    ));
    columns[schema.index_of(RUN_ID)?] = Arc::new(StringArray::from_iter_values(
        std::iter::repeat_n(run_id, rows),
    ));
    columns[schema.index_of(TICK)?] = Arc::new(Int32Array::from(vec![tick; rows]));
    columns[schema.index_of(ENTITY_ID)?] = Arc::new(
        batch
            .column(schema.index_of(ENTITY_ID)?)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ArchetypeCoreError::InvalidColumnType {
                column: ENTITY_ID,
                expected: DataType::Int32,
                actual: schema
                    .field_with_name(ENTITY_ID)
                    .unwrap()
                    .data_type()
                    .clone(),
            })?
            .clone(),
    );
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

fn active_snapshot(batch: &RecordBatch) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let active = batch
        .column(batch.schema().index_of(IS_ACTIVE)?)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ArchetypeCoreError::InvalidColumnType {
            column: IS_ACTIVE,
            expected: DataType::Boolean,
            actual: batch
                .schema()
                .field_with_name(IS_ACTIVE)
                .unwrap()
                .data_type()
                .clone(),
        })?;

    filter_record_batch(batch, active).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use arrow_array::{Float64Array, Int32Array};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;

    use super::*;
    use crate::aio::async_processor::{Processor, ProcessorContext};
    use crate::archetype::ArchetypeTable;

    fn position_table() -> ArchetypeTable {
        let component = Arc::new(Schema::new(vec![Field::new(
            "position__x",
            DataType::Float64,
            false,
        )]));
        ArchetypeTable::new("position", component).unwrap()
    }

    fn position_velocity_table() -> ArchetypeTable {
        let component = Arc::new(Schema::new(vec![
            Field::new("position__x", DataType::Float64, false),
            Field::new("velocity__dx", DataType::Float64, false),
        ]));
        ArchetypeTable::new("position_velocity", component).unwrap()
    }

    fn empty_prior(schema: SchemaRef) -> RecordBatch {
        RecordBatch::new_empty(schema)
    }

    #[derive(Clone, Default)]
    struct MemoryStore {
        appends: Arc<Mutex<Vec<RecordBatch>>>,
        read_calls: Arc<Mutex<usize>>,
        read_filters: Arc<Mutex<Vec<(String, ReadFilter)>>>,
    }

    #[async_trait]
    impl Store for MemoryStore {
        async fn append(&self, _table_name: &str, batch: &RecordBatch) -> Result<()> {
            self.appends.lock().unwrap().push(batch.clone());
            Ok(())
        }

        async fn read_table(
            &self,
            table_name: &str,
            filter: ReadFilter,
        ) -> Result<Vec<RecordBatch>> {
            *self.read_calls.lock().unwrap() += 1;
            self.read_filters
                .lock()
                .unwrap()
                .push((table_name.to_string(), filter));
            Ok(Vec::new())
        }
    }

    struct AddOne;

    impl Processor for AddOne {
        fn name(&self) -> &str {
            "add_one"
        }

        fn required_columns(&self) -> &[&'static str] {
            &["position__x"]
        }

        fn process(&self, batch: RecordBatch, _ctx: &ProcessorContext) -> Result<RecordBatch> {
            let values = batch
                .column(batch.schema().index_of("position__x")?)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let next = (0..batch.num_rows())
                .map(|idx| values.value(idx) + 1.0)
                .collect::<Vec<_>>();
            let mut columns = batch.columns().to_vec();
            columns[batch.schema().index_of("position__x")?] = Arc::new(Float64Array::from(next));
            RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
        }
    }

    #[test]
    fn spawn_batch_is_stamped_and_materialized() {
        let table = position_table();
        let components = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let mut world = WorldState::new("world-a", "run-a");

        let ids = world
            .queue_spawn_batch(table.table_name(), table.schema(), components)
            .unwrap();
        assert_eq!(ids, vec![1]);

        let materialized = world
            .materialize_table(
                table.table_name(),
                table.schema(),
                empty_prior(table.schema()),
            )
            .unwrap();

        assert_eq!(materialized.num_rows(), 1);
        assert_eq!(
            materialized
                .column(materialized.schema().index_of(ENTITY_ID).unwrap())
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[test]
    fn despawn_marks_prior_row_inactive() {
        let table = position_table();
        let component_schema = Arc::new(Schema::new(vec![Field::new(
            "position__x",
            DataType::Float64,
            false,
        )]));
        let components = RecordBatch::try_new(
            component_schema,
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let mut world = WorldState::new("world-a", "run-a");
        world
            .queue_spawn_batch(table.table_name(), table.schema(), components)
            .unwrap();
        let prior = world
            .materialize_table(
                table.table_name(),
                table.schema(),
                empty_prior(table.schema()),
            )
            .unwrap();

        world.queue_despawn(1).unwrap();
        let next = world
            .materialize_table(table.table_name(), table.schema(), prior)
            .unwrap();
        let active = next
            .column(next.schema().index_of(IS_ACTIVE).unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert!(!active.value(0));
    }

    #[test]
    fn despawn_in_same_tick_cancels_pending_spawn() {
        let table = position_table();
        let components = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5, 2.5]))],
        )
        .unwrap();
        let mut world = WorldState::new("world-a", "run-a");
        let ids = world
            .queue_spawn_batch(table.table_name(), table.schema(), components)
            .unwrap();
        assert_eq!(ids, vec![1, 2]);

        world.queue_despawn(1).unwrap();
        let materialized = world
            .materialize_table(
                table.table_name(),
                table.schema(),
                empty_prior(table.schema()),
            )
            .unwrap();

        // Entity 1 never becomes observable: no active row, no tombstone.
        assert_eq!(materialized.num_rows(), 1);
        let entities = entity_column(&materialized).unwrap();
        assert_eq!(entities.value(0), 2);
        let active = materialized
            .column(materialized.schema().index_of(IS_ACTIVE).unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active.value(0));
    }

    #[test]
    fn spawn_materialization_preserves_insertion_order() {
        let table = position_table();
        let mut world = WorldState::new("world-a", "run-a");
        for x in [1.0, 2.0, 3.0] {
            let components = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "position__x",
                    DataType::Float64,
                    false,
                )])),
                vec![Arc::new(Float64Array::from(vec![x]))],
            )
            .unwrap();
            world
                .queue_spawn_batch(table.table_name(), table.schema(), components)
                .unwrap();
        }

        let materialized = world
            .materialize_table(
                table.table_name(),
                table.schema(),
                empty_prior(table.schema()),
            )
            .unwrap();

        let entities = entity_column(&materialized).unwrap();
        assert_eq!(
            (0..materialized.num_rows())
                .map(|idx| entities.value(idx))
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn duplicate_spawn_same_tick_is_last_write_wins() {
        let table = position_table();
        let mut world = WorldState::new("world-a", "run-a");

        let first = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let ids = world
            .queue_spawn_batch(table.table_name(), table.schema(), first)
            .unwrap();
        assert_eq!(ids, vec![1]);

        // Mirrors the Python parity case where duplicate commands target the
        // same entity before the tick materializes. The later row wins.
        world.next_entity_id = 1;
        let second = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![99.0]))],
        )
        .unwrap();
        let duplicate_ids = world
            .queue_spawn_batch(table.table_name(), table.schema(), second)
            .unwrap();
        assert_eq!(duplicate_ids, vec![1]);

        let materialized = world
            .materialize_table(
                table.table_name(),
                table.schema(),
                empty_prior(table.schema()),
            )
            .unwrap();

        assert_eq!(materialized.num_rows(), 1);
        let entities = entity_column(&materialized).unwrap();
        let x = materialized
            .column(materialized.schema().index_of("position__x").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(entities.value(0), 1);
        assert_eq!(x.value(0), 99.0);
    }

    #[test]
    fn explicit_spawn_entity_ids_preserve_reserved_ids_and_bump_allocator() {
        let table = position_table();
        let mut world = WorldState::new("world-a", "run-a");
        let reserved = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![42.0]))],
        )
        .unwrap();

        world
            .queue_spawn_batch_with_entity_ids(table.table_name(), table.schema(), reserved, &[7])
            .unwrap();
        let generated = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![43.0]))],
        )
        .unwrap();
        let ids = world
            .queue_spawn_batch(table.table_name(), table.schema(), generated)
            .unwrap();

        assert_eq!(ids, vec![8]);

        let materialized = world
            .materialize_table(
                table.table_name(),
                table.schema(),
                empty_prior(table.schema()),
            )
            .unwrap();
        let entities = entity_column(&materialized).unwrap();
        assert_eq!(
            (0..materialized.num_rows())
                .map(|idx| entities.value(idx))
                .collect::<Vec<_>>(),
            vec![7, 8]
        );
    }

    #[test]
    fn spawn_with_missing_component_column_is_an_error() {
        let component = Arc::new(Schema::new(vec![
            Field::new("position__x", DataType::Float64, false),
            Field::new("position__y", DataType::Float64, false),
        ]));
        let table = ArchetypeTable::new("position", component).unwrap();
        let partial = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let mut world = WorldState::new("world-a", "run-a");

        let err = world
            .queue_spawn_batch(table.table_name(), table.schema(), partial)
            .unwrap_err();
        assert!(matches!(
            err,
            ArchetypeCoreError::MissingColumn(column) if column == "position__y"
        ));
    }

    // Tick-zero contract: spawn x=1.5 at tick 0.  The processor (AddOne) runs
    // over the empty prior — no rows processed.  The raw spawn row is
    // concatenated AFTER processor output and persisted unchanged at tick 0.
    // x₀ = 1.5 (given); x₁ = f(x₀) = 2.5 on the next step.
    #[tokio::test]
    async fn async_world_steps_spawned_table_through_system_and_store() {
        let table = position_table();
        let components = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let store = MemoryStore::default();
        let appends = store.appends.clone();
        let mut system = AsyncSystem::new();
        system.add_processor(AddOne);
        let mut world = AsyncWorld::from_ids("world-a", "run-a", store, system);

        world.queue_spawn_batch(&table, components).unwrap();
        world.step().await.unwrap();

        assert_eq!(world.state().tick(), 1);
        let appended = appends.lock().unwrap();
        assert_eq!(appended.len(), 1);
        let batch = &appended[0];
        let x = batch
            .column(batch.schema().index_of("position__x").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let tick = batch
            .column(batch.schema().index_of(TICK).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Spawn lands raw at its materialization tick — x₀ is preserved.
        assert_eq!(x.value(0), 1.5);
        assert_eq!(tick.value(0), 0);
    }

    #[tokio::test]
    async fn step_profiled_reports_table_phase_timings() {
        let table = position_table();
        let components = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let mut system = AsyncSystem::new();
        system.add_processor(AddOne);
        let mut world = AsyncWorld::from_ids("world-a", "run-a", MemoryStore::default(), system);

        world.queue_spawn_batch(&table, components).unwrap();
        let profile = world.step_profiled().await.unwrap();

        assert_eq!(profile.tick, 0);
        assert_eq!(world.state().tick(), 1);
        assert_eq!(profile.tables.len(), 1);
        assert_eq!(profile.tables[0].table_name, "position");
        assert!(profile.tables[0].table_sec >= profile.tables[0].process_sec);
        assert!(profile.tick_sec >= profile.tables[0].table_sec);
    }

    #[tokio::test]
    async fn async_world_persists_world_run_and_tick_metadata() {
        let table = position_table();
        let components = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let store = MemoryStore::default();
        let appends = store.appends.clone();
        let mut world = AsyncWorld::from_ids("world-a", "run-a", store, AsyncSystem::new());

        world.queue_spawn_batch(&table, components).unwrap();
        world.run(2).await.unwrap();

        let appended = appends.lock().unwrap();
        assert_eq!(appended.len(), 2);
        let world_ids = appended[1]
            .column(appended[1].schema().index_of(WORLD_ID).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let run_ids = appended[1]
            .column(appended[1].schema().index_of(RUN_ID).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ticks = appended[1]
            .column(appended[1].schema().index_of(TICK).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(world_ids.value(0), "world-a");
        assert_eq!(run_ids.value(0), "run-a");
        assert_eq!(ticks.value(0), 1);
        assert_eq!(world.state().tick(), 2);
    }

    // Tick-zero contract: spawn x=1.5 at tick 0 → lands raw (x=1.5, tick 0).
    // Despawn entity 1.  Tick 1: prior x=1.5 is tombstoned (is_active=false),
    // then processor AddOne runs → x=2.5.  Tombstone persisted at tick 1.
    #[tokio::test]
    async fn despawn_only_tick_processes_signature_and_persists_tombstone() {
        let table = position_table();
        let components = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let store = MemoryStore::default();
        let appends = store.appends.clone();
        let mut system = AsyncSystem::new();
        system.add_processor(AddOne);
        let mut world = AsyncWorld::from_ids("world-a", "run-a", store, system);

        let ids = world.queue_spawn_batch(&table, components).unwrap();
        world.step().await.unwrap();
        world.queue_despawn(ids[0]).unwrap();
        world.step().await.unwrap();

        let appended = appends.lock().unwrap();
        assert_eq!(appended.len(), 2);
        let tombstone = &appended[1];
        let active = tombstone
            .column(tombstone.schema().index_of(IS_ACTIVE).unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let x = tombstone
            .column(tombstone.schema().index_of("position__x").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ticks = tombstone
            .column(tombstone.schema().index_of(TICK).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(tombstone.num_rows(), 1);
        assert!(!active.value(0));
        // Tick 0: spawn x=1.5 raw.  Tick 1: prior x=1.5 tombstoned then
        // AddOne applied → x=2.5 (tombstone carries the f(x) value).
        assert_eq!(x.value(0), 2.5);
        assert_eq!(ticks.value(0), 1);
    }

    #[tokio::test]
    async fn migration_persists_old_table_tombstone_and_new_table_active_row() {
        let old_table = position_table();
        let new_table = position_velocity_table();
        let initial = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let store = MemoryStore::default();
        let appends = store.appends.clone();
        let mut world = AsyncWorld::from_ids("world-a", "run-a", store, AsyncSystem::new());

        let ids = world.queue_spawn_batch(&old_table, initial).unwrap();
        world.step().await.unwrap();

        let migrated = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("position__x", DataType::Float64, false),
                Field::new("velocity__dx", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Float64Array::from(vec![1.5])),
                Arc::new(Float64Array::from(vec![2.0])),
            ],
        )
        .unwrap();
        world
            .queue_migration_batch(&new_table, &ids, migrated)
            .unwrap();
        world.step().await.unwrap();

        let appended = appends.lock().unwrap();
        assert_eq!(appended.len(), 3);

        let old_tombstone = &appended[1];
        let old_active = old_tombstone
            .column(old_tombstone.schema().index_of(IS_ACTIVE).unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let old_entities = entity_column(old_tombstone).unwrap();
        assert_eq!(old_entities.value(0), ids[0]);
        assert!(!old_active.value(0));

        let new_active_row = &appended[2];
        let new_active = new_active_row
            .column(new_active_row.schema().index_of(IS_ACTIVE).unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let new_entities = entity_column(new_active_row).unwrap();
        let velocity = new_active_row
            .column(new_active_row.schema().index_of("velocity__dx").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(new_entities.value(0), ids[0]);
        assert!(new_active.value(0));
        assert_eq!(velocity.value(0), 2.0);
    }

    #[tokio::test]
    async fn same_tick_migration_from_pending_spawn_materializes_only_new_table() {
        let old_table = position_table();
        let new_table = position_velocity_table();
        let initial = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let store = MemoryStore::default();
        let appends = store.appends.clone();
        let mut world = AsyncWorld::from_ids("world-a", "run-a", store, AsyncSystem::new());

        let ids = world.queue_spawn_batch(&old_table, initial).unwrap();
        let migrated = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("position__x", DataType::Float64, false),
                Field::new("velocity__dx", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Float64Array::from(vec![1.5])),
                Arc::new(Float64Array::from(vec![2.0])),
            ],
        )
        .unwrap();
        world
            .queue_migration_batch(&new_table, &ids, migrated)
            .unwrap();
        world.step().await.unwrap();

        let appended = appends.lock().unwrap();
        assert_eq!(appended.len(), 1);
        assert!(appended[0].schema().index_of("velocity__dx").is_ok());
        assert_eq!(entity_column(&appended[0]).unwrap().value(0), ids[0]);
    }

    // Tick-zero contract: tick 0 spawns x=1.5 raw into the ledger (processor
    // ran on the empty prior).  The live snapshot carries x=1.5 so tick 1
    // reads from memory (no store read).  Tick 1 applies AddOne → x=2.5.
    #[tokio::test]
    async fn async_world_uses_live_snapshot_after_first_step() {
        let table = position_table();
        let components = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let store = MemoryStore::default();
        let appends = store.appends.clone();
        let read_calls = store.read_calls.clone();
        let mut system = AsyncSystem::new();
        system.add_processor(AddOne);
        let mut world = AsyncWorld::from_ids("world-a", "run-a", store, system);

        world.queue_spawn_batch(&table, components).unwrap();
        world.run(2).await.unwrap();

        assert_eq!(world.state().tick(), 2);
        // Tick 0 reads from store (no live snapshot yet); tick 1 uses the
        // live snapshot from tick 0 — exactly one store read total.
        assert_eq!(*read_calls.lock().unwrap(), 1);

        let appended = appends.lock().unwrap();
        assert_eq!(appended.len(), 2);
        let final_batch = &appended[1];
        let x = final_batch
            .column(final_batch.schema().index_of("position__x").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let tick = final_batch
            .column(final_batch.schema().index_of(TICK).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Tick 0: spawn x=1.5 raw; tick 1: AddOne → x=2.5.
        assert_eq!(x.value(0), 2.5);
        assert_eq!(tick.value(0), 1);
    }

    #[tokio::test]
    async fn live_snapshot_after_despawn_excludes_inactive_entities_from_next_tick() {
        let table = position_table();
        let components = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let store = MemoryStore::default();
        let appends = store.appends.clone();
        let read_calls = store.read_calls.clone();
        let mut system = AsyncSystem::new();
        system.add_processor(AddOne);
        let mut world = AsyncWorld::from_ids("world-a", "run-a", store, system);

        let ids = world.queue_spawn_batch(&table, components).unwrap();
        world.step().await.unwrap();
        world.queue_despawn(ids[0]).unwrap();
        world.run(2).await.unwrap();

        assert_eq!(*read_calls.lock().unwrap(), 1);
        let appended = appends.lock().unwrap();
        assert_eq!(appended.len(), 2);
        assert_eq!(world.state().tick(), 3);
    }

    #[tokio::test]
    async fn query_table_scopes_to_current_world_and_run_by_default() {
        let store = MemoryStore::default();
        let read_filters = store.read_filters.clone();
        let world = AsyncWorld::from_ids("world-a", "run-a", store, AsyncSystem::new());

        world
            .query_table(
                "position",
                ReadFilter {
                    ticks: Some(vec![2]),
                    entity_ids: Some(vec![7]),
                    active_only: true,
                    ..ReadFilter::default()
                },
            )
            .await
            .unwrap();

        let filters = read_filters.lock().unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].0, "position");
        assert_eq!(filters[0].1.world_id.as_deref(), Some("world-a"));
        assert_eq!(filters[0].1.run_id.as_deref(), Some("run-a"));
        assert_eq!(filters[0].1.ticks.as_deref(), Some(&[2][..]));
        assert_eq!(filters[0].1.entity_ids.as_deref(), Some(&[7][..]));
        assert!(filters[0].1.active_only);
    }

    #[tokio::test]
    async fn first_tick_prior_read_filters_to_previous_tick_and_active_rows() {
        let table = position_table();
        let components = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let store = MemoryStore::default();
        let read_filters = store.read_filters.clone();
        let mut world = AsyncWorld::from_ids("world-a", "run-a", store, AsyncSystem::new());

        world.queue_spawn_batch(&table, components).unwrap();
        world.step().await.unwrap();

        let filters = read_filters.lock().unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].0, "position");
        assert_eq!(filters[0].1.world_id.as_deref(), Some("world-a"));
        assert_eq!(filters[0].1.run_id.as_deref(), Some("run-a"));
        assert_eq!(filters[0].1.ticks.as_deref(), Some(&[-1][..]));
        assert!(filters[0].1.active_only);
    }

    // ── Tick-zero contract tests ──────────────────────────────────────────────
    //
    // These mirror tests/core/test_initial_conditions_contract.py.
    // Contract: x₀ is given; x_{t+1} = f(x_t).
    // The ledger contains the full sequence x₀, f(x₀), f²(x₀), …
    // Initial conditions are preserved because spawns land raw AFTER the
    // processor runs on the prior rows, not before.

    /// A store that persists all appended batches in memory and filters them
    /// by tick / active status on read, mirroring the ledger semantics needed
    /// to verify the initial-conditions contract without touching disk.
    #[derive(Clone, Default)]
    struct LedgerStore {
        ledger: Arc<Mutex<Vec<(String, RecordBatch)>>>,
    }

    #[async_trait]
    impl Store for LedgerStore {
        async fn append(&self, table_name: &str, batch: &RecordBatch) -> Result<()> {
            self.ledger
                .lock()
                .unwrap()
                .push((table_name.to_string(), batch.clone()));
            Ok(())
        }

        async fn read_table(
            &self,
            table_name: &str,
            filter: ReadFilter,
        ) -> Result<Vec<RecordBatch>> {
            let ledger = self.ledger.lock().unwrap();
            let mut result = Vec::new();
            for (tname, batch) in ledger.iter() {
                if tname != table_name {
                    continue;
                }
                // Tick filter
                if let Some(ticks) = &filter.ticks {
                    let tick_col = batch
                        .column(batch.schema().index_of(TICK).unwrap())
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    // Check if any row in the batch matches one of the
                    // requested ticks.
                    let has_tick =
                        (0..batch.num_rows()).any(|idx| ticks.contains(&tick_col.value(idx)));
                    if !has_tick {
                        continue;
                    }
                }
                // Active-only filter
                if filter.active_only {
                    let active_col = batch
                        .column(batch.schema().index_of(IS_ACTIVE).unwrap())
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap();
                    let keep = BooleanArray::from_iter(
                        (0..batch.num_rows()).map(|idx| Some(active_col.value(idx))),
                    );
                    let filtered =
                        filter_record_batch(batch, &keep).expect("ledger store filter failed");
                    if filtered.num_rows() > 0 {
                        result.push(filtered);
                    }
                } else {
                    result.push(batch.clone());
                }
            }
            Ok(result)
        }
    }

    /// Reads all rows for a given tick from the ledger and returns a map of
    /// entity_id → position__x, used in contract assertions.
    async fn ledger_x_at_tick(
        world: &AsyncWorld<LedgerStore>,
        table_name: &str,
        tick: i32,
    ) -> HashMap<EntityId, f64> {
        let batches = world
            .query_table(
                table_name,
                ReadFilter {
                    ticks: Some(vec![tick]),
                    active_only: false,
                    ..ReadFilter::default()
                },
            )
            .await
            .unwrap();
        let mut map = HashMap::new();
        for batch in &batches {
            let entities = entity_column(batch).unwrap();
            let x_col = batch
                .column(batch.schema().index_of("position__x").unwrap())
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let tick_col = batch
                .column(batch.schema().index_of(TICK).unwrap())
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for idx in 0..batch.num_rows() {
                if tick_col.value(idx) == tick {
                    map.insert(entities.value(idx), x_col.value(idx));
                }
            }
        }
        map
    }

    // Mirrors test_initial_conditions_persist_at_spawn_tick from
    // tests/core/test_initial_conditions_contract.py.
    //
    // Spawn entity with x=5.0.  Run 3 steps.
    // Expected ledger:  tick 0 → x=5.0  (raw, x₀)
    //                   tick 1 → x=6.0  (f(x₀))
    //                   tick 2 → x=7.0  (f²(x₀))
    #[tokio::test]
    async fn tick_zero_contract_first_spawn_persists_raw() {
        let table = position_table();
        let components = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![5.0]))],
        )
        .unwrap();
        let mut system = AsyncSystem::new();
        system.add_processor(AddOne);
        let mut world = AsyncWorld::from_ids("world-a", "run-a", LedgerStore::default(), system);

        world.queue_spawn_batch(&table, components).unwrap();
        world.run(3).await.unwrap();

        let by_tick: HashMap<i32, f64> = {
            let mut m = HashMap::new();
            for t in 0..3i32 {
                let rows = ledger_x_at_tick(&world, table.table_name(), t).await;
                // Exactly one entity per tick.
                assert_eq!(
                    rows.len(),
                    1,
                    "tick {t}: expected 1 row, got {}",
                    rows.len()
                );
                m.insert(t, *rows.values().next().unwrap());
            }
            m
        };

        // Strict equality — x₀ preserved, f applied on subsequent ticks.
        assert_eq!(
            by_tick[&0], 5.0,
            "tick 0 must be raw spawn (x₀=5.0); got {}",
            by_tick[&0]
        );
        assert_eq!(
            by_tick[&1], 6.0,
            "tick 1 must be f(x₀)=6.0; got {}",
            by_tick[&1]
        );
        assert_eq!(
            by_tick[&2], 7.0,
            "tick 2 must be f²(x₀)=7.0; got {}",
            by_tick[&2]
        );
    }

    // Mirrors test_mid_run_spawn_persists_initial_conditions from
    // tests/core/test_initial_conditions_contract.py.
    //
    // Entity "first" spawned with x=0.0 before tick 0; run 2 steps
    // (tick 0 raw, tick 1 f applied).  Then "second" spawned with x=100.0;
    // run 2 more steps.
    // Expected:
    //   tick 2: first→2.0  (f²(0))  second→100.0  (raw, x₀)
    //   tick 3: first→3.0  (f³(0))  second→101.0  (f(x₀))
    #[tokio::test]
    async fn tick_zero_contract_mid_run_spawn_lands_raw_older_entities_keep_advancing() {
        let table = position_table();
        let store = LedgerStore::default();
        let mut system = AsyncSystem::new();
        system.add_processor(AddOne);
        let mut world = AsyncWorld::from_ids("world-a", "run-a", store, system);

        // Spawn "first" entity (x=0.0)
        let first_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![0.0]))],
        )
        .unwrap();
        let first_ids = world.queue_spawn_batch(&table, first_batch).unwrap();
        let first_id = first_ids[0];

        // Run 2 steps: tick 0 (x=0.0 raw), tick 1 (x=1.0)
        world.run(2).await.unwrap();

        // Spawn "second" entity (x=100.0) mid-run
        let second_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position__x",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![100.0]))],
        )
        .unwrap();
        let second_ids = world.queue_spawn_batch(&table, second_batch).unwrap();
        let second_id = second_ids[0];

        // Run 2 more steps: tick 2 and tick 3
        world.run(2).await.unwrap();

        // Verify tick 2: first at 2.0 (f²(0)), second at 100.0 (raw x₀)
        let t2 = ledger_x_at_tick(&world, table.table_name(), 2).await;
        assert_eq!(
            t2[&first_id], 2.0,
            "tick 2: first entity must be at f²(0)=2.0; got {}",
            t2[&first_id]
        );
        assert_eq!(
            t2[&second_id], 100.0,
            "tick 2: mid-run spawn must land raw (x₀=100.0); got {}",
            t2[&second_id]
        );

        // Verify tick 3: first at 3.0 (f³(0)), second at 101.0 (f(x₀))
        let t3 = ledger_x_at_tick(&world, table.table_name(), 3).await;
        assert_eq!(
            t3[&first_id], 3.0,
            "tick 3: first entity must be at f³(0)=3.0; got {}",
            t3[&first_id]
        );
        assert_eq!(
            t3[&second_id], 101.0,
            "tick 3: second entity must be at f(x₀)=101.0; got {}",
            t3[&second_id]
        );
    }
}
