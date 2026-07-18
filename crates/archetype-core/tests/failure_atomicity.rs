use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use archetype_core::archetype::IS_ACTIVE;
use archetype_core::{
    ArchetypeCoreError, ArchetypeTable, AsyncSystem, AsyncWorld, Processor, ProcessorContext,
    ReadFilter, Result, Store,
};
use arrow_array::{BooleanArray, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;

fn position_table() -> ArchetypeTable {
    ArchetypeTable::new(
        "position",
        Arc::new(Schema::new(vec![Field::new(
            "position__x",
            DataType::Float64,
            false,
        )])),
    )
    .unwrap()
}

fn position_velocity_table() -> ArchetypeTable {
    ArchetypeTable::new(
        "position_velocity",
        Arc::new(Schema::new(vec![
            Field::new("position__x", DataType::Float64, false),
            Field::new("velocity__dx", DataType::Float64, false),
        ])),
    )
    .unwrap()
}

fn position_batch(value: f64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "position__x",
            DataType::Float64,
            false,
        )])),
        vec![Arc::new(Float64Array::from(vec![value]))],
    )
    .unwrap()
}

fn position_velocity_batch(position: f64, velocity: f64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("position__x", DataType::Float64, false),
            Field::new("velocity__dx", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Float64Array::from(vec![position])),
            Arc::new(Float64Array::from(vec![velocity])),
        ],
    )
    .unwrap()
}

#[derive(Clone, Default)]
struct ControlledStore {
    appends: Arc<Mutex<Vec<(String, RecordBatch)>>>,
    fail_next: Arc<AtomicBool>,
    fail_table: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl Store for ControlledStore {
    async fn append(&self, table_name: &str, batch: &RecordBatch) -> Result<()> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(ArchetypeCoreError::Store("injected append failure".into()));
        }
        let should_fail = {
            let mut fail_table = self.fail_table.lock().unwrap();
            let matches = fail_table.as_deref() == Some(table_name);
            if matches {
                fail_table.take();
            }
            matches
        };
        if should_fail {
            return Err(ArchetypeCoreError::Store("injected append failure".into()));
        }
        self.appends
            .lock()
            .unwrap()
            .push((table_name.to_string(), batch.clone()));
        Ok(())
    }

    async fn read_table(&self, _table_name: &str, _filter: ReadFilter) -> Result<Vec<RecordBatch>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn later_append_failure_poisons_partial_tick_against_retry() {
    let position = position_table();
    let position_velocity = position_velocity_table();
    let store = ControlledStore::default();
    let appends = store.appends.clone();
    *store.fail_table.lock().unwrap() = Some(position_velocity.table_name().to_string());
    let mut world = AsyncWorld::from_ids("world-a", "run-a", store, AsyncSystem::new());
    let position_ids = world
        .queue_spawn_batch(&position, position_batch(1.5))
        .unwrap();
    world
        .queue_spawn_batch(&position_velocity, position_velocity_batch(2.5, 3.5))
        .unwrap();

    let error = world.step().await.unwrap_err();
    assert!(matches!(
        error,
        ArchetypeCoreError::PartialTick {
            tick: 0,
            committed_tables: 1,
            ..
        }
    ));
    assert_eq!(world.state().tick(), 0);
    assert_eq!(appends.lock().unwrap().len(), 1);

    assert!(matches!(
        world.step().await.unwrap_err(),
        ArchetypeCoreError::PartialTick {
            tick: 0,
            committed_tables: 1,
            ..
        }
    ));
    assert_eq!(appends.lock().unwrap().len(), 1);
    assert!(matches!(
        world.queue_despawn(position_ids[0]).unwrap_err(),
        ArchetypeCoreError::PartialTick { tick: 0, .. }
    ));
    assert!(matches!(
        world
            .state_mut()
            .queue_despawn(position_ids[0])
            .unwrap_err(),
        ArchetypeCoreError::PartialTick { tick: 0, .. }
    ));
    assert!(matches!(
        world.state_mut().advance_tick().unwrap_err(),
        ArchetypeCoreError::PartialTick { tick: 0, .. }
    ));
    assert_eq!(world.state().tick(), 0);
}

struct FailVelocity {
    enabled: Arc<AtomicBool>,
}

impl Processor for FailVelocity {
    fn name(&self) -> &str {
        "fail_velocity"
    }

    fn required_columns(&self) -> &[&'static str] {
        &["velocity__dx"]
    }

    fn process(&self, batch: RecordBatch, _ctx: &ProcessorContext) -> Result<RecordBatch> {
        if self.enabled.load(Ordering::SeqCst) {
            Err(ArchetypeCoreError::Store(
                "injected processor failure".into(),
            ))
        } else {
            Ok(batch)
        }
    }
}

#[tokio::test]
async fn processor_failure_prevents_every_table_append_and_preserves_retry() {
    let position = position_table();
    let position_velocity = position_velocity_table();
    let store = ControlledStore::default();
    let appends = store.appends.clone();
    let failure = Arc::new(AtomicBool::new(true));
    let mut system = AsyncSystem::new();
    system.add_processor(FailVelocity {
        enabled: failure.clone(),
    });
    let mut world = AsyncWorld::from_ids("world-a", "run-a", store, system);
    world
        .queue_spawn_batch(&position, position_batch(1.5))
        .unwrap();
    world
        .queue_spawn_batch(&position_velocity, position_velocity_batch(2.5, 3.5))
        .unwrap();

    let error = world.step().await.unwrap_err();
    assert!(
        matches!(error, ArchetypeCoreError::Store(message) if message == "injected processor failure")
    );
    assert_eq!(world.state().tick(), 0);
    assert!(appends.lock().unwrap().is_empty());

    failure.store(false, Ordering::SeqCst);
    world.step().await.unwrap();
    assert_eq!(world.state().tick(), 1);
    assert_eq!(appends.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn append_failure_preserves_spawn_and_despawn_for_retry() {
    let table = position_table();
    let store = ControlledStore::default();
    let appends = store.appends.clone();
    let fail_next = store.fail_next.clone();
    let mut world = AsyncWorld::from_ids("world-a", "run-a", store, AsyncSystem::new());
    let ids = world
        .queue_spawn_batch(&table, position_batch(1.5))
        .unwrap();

    fail_next.store(true, Ordering::SeqCst);
    assert!(world.step().await.is_err());
    assert_eq!(world.state().tick(), 0);
    assert!(appends.lock().unwrap().is_empty());

    world.step().await.unwrap();
    assert_eq!(world.state().tick(), 1);
    assert_eq!(appends.lock().unwrap().len(), 1);

    world.queue_despawn(ids[0]).unwrap();
    fail_next.store(true, Ordering::SeqCst);
    assert!(world.step().await.is_err());
    assert_eq!(world.state().tick(), 1);
    assert_eq!(appends.lock().unwrap().len(), 1);

    world.step().await.unwrap();
    let appended = appends.lock().unwrap();
    assert_eq!(world.state().tick(), 2);
    assert_eq!(appended.len(), 2);
    let active = appended[1]
        .1
        .column(appended[1].1.schema().index_of(IS_ACTIVE).unwrap())
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(!active.value(0));
}

#[tokio::test]
async fn per_table_step_rejects_multi_table_ticks_before_any_append() {
    let position = position_table();
    let position_velocity = position_velocity_table();
    let store = ControlledStore::default();
    let appends = store.appends.clone();
    let mut world = AsyncWorld::from_ids("world-a", "run-a", store, AsyncSystem::new());
    world
        .queue_spawn_batch(&position, position_batch(1.5))
        .unwrap();
    world
        .queue_spawn_batch(&position_velocity, position_velocity_batch(2.5, 3.5))
        .unwrap();

    let error = world.step_table(position.table_name()).await.unwrap_err();
    assert!(matches!(
        error,
        ArchetypeCoreError::InvalidTableStep {
            table_name,
            active_tables,
        } if table_name == position.table_name() && active_tables.len() == 2
    ));
    assert_eq!(world.state().tick(), 0);
    assert!(appends.lock().unwrap().is_empty());

    world.step().await.unwrap();
    assert_eq!(world.state().tick(), 1);
    assert_eq!(appends.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn per_table_step_is_a_complete_retryable_single_table_tick() {
    let table = position_table();
    let store = ControlledStore::default();
    let appends = store.appends.clone();
    let fail_next = store.fail_next.clone();
    let mut world = AsyncWorld::from_ids("world-a", "run-a", store, AsyncSystem::new());
    world
        .queue_spawn_batch(&table, position_batch(1.5))
        .unwrap();

    fail_next.store(true, Ordering::SeqCst);
    assert!(world.step_table(table.table_name()).await.is_err());
    assert_eq!(world.state().tick(), 0);
    assert!(appends.lock().unwrap().is_empty());

    let batch = world.step_table(table.table_name()).await.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(world.state().tick(), 1);
    assert_eq!(appends.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn rejected_migration_leaves_pending_source_entity_unchanged() {
    let old_table = position_table();
    let new_table = position_velocity_table();
    let invalid = RecordBatch::new_empty(Arc::new(Schema::new(vec![
        Field::new("position__x", DataType::Float64, false),
        Field::new("velocity__dx", DataType::Float64, false),
    ])));
    let store = ControlledStore::default();
    let appends = store.appends.clone();
    let mut world = AsyncWorld::from_ids("world-a", "run-a", store, AsyncSystem::new());
    let ids = world
        .queue_spawn_batch(&old_table, position_batch(1.5))
        .unwrap();

    let error = world
        .queue_migration_batch(&new_table, &ids, invalid)
        .unwrap_err();
    assert!(matches!(
        error,
        ArchetypeCoreError::InvalidRowCount {
            expected: 1,
            actual: 0
        }
    ));

    world.step().await.unwrap();
    let appended = appends.lock().unwrap();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0].0, old_table.table_name());
    assert_eq!(appended[0].1.num_rows(), 1);
    assert!(appended[0].1.schema().index_of("velocity__dx").is_err());
}
