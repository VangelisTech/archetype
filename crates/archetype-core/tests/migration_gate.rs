//! Phase-6 entry gate: migration contract suite.
//!
//! Every behavioral contract the Python async reference pins is encoded here,
//! one test per contract, annotated with the mirroring Python test.  Gaps
//! where the Rust kernel has no implementation yet are marked `#[ignore]`
//! with a rationale rather than omitted.
//!
//! Contract areas covered (in order):
//!
//! 1. x₀ raw at spawn tick / f applied next tick  (first and mid-run)
//! 2. Same-tick spawn-cancel: no tombstone, no active row
//! 3. Spawn materialization order deterministic (first-seen, last-write-wins)
//! 4. Despawn: tombstone at current tick; active_only reads exclude it
//! 5. Update / overlay semantics  [#[ignore] — not yet implemented in kernel]
//! 6. Metadata stamping (world/run/tick) on every persisted row
//!
//! Python reference:
//!   tests/core/test_initial_conditions_contract.py   — contracts 1, 6
//!   tests/core/test_async_world_duplicate_spawn_overwrite.py — contract 3
//!   crates/archetype-core/src/aio/async_world.rs (inline tests) — contracts 2, 4

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_array::{BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;

use archetype_core::aio::async_processor::{Processor, ProcessorContext};
use archetype_core::aio::async_store::{ReadFilter, Store};
use archetype_core::aio::async_system::AsyncSystem;
use archetype_core::aio::async_world::AsyncWorld;
use archetype_core::archetype::{ArchetypeTable, ENTITY_ID, IS_ACTIVE, RUN_ID, TICK, WORLD_ID};
use archetype_core::{EntityId, Result};

// ── Shared helpers ────────────────────────────────────────────────────────────

fn position_table() -> ArchetypeTable {
    let component = Arc::new(Schema::new(vec![Field::new(
        "position__x",
        DataType::Float64,
        false,
    )]));
    ArchetypeTable::new("position", component).unwrap()
}

fn component_batch(x_values: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "position__x",
            DataType::Float64,
            false,
        )])),
        vec![Arc::new(Float64Array::from(x_values.to_vec()))],
    )
    .unwrap()
}

fn entity_ids(batch: &RecordBatch) -> Vec<i32> {
    let col = batch
        .column(batch.schema().index_of(ENTITY_ID).unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    (0..batch.num_rows()).map(|i| col.value(i)).collect()
}

fn x_values(batch: &RecordBatch) -> Vec<f64> {
    let col = batch
        .column(batch.schema().index_of("position__x").unwrap())
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows()).map(|i| col.value(i)).collect()
}

fn active_flags(batch: &RecordBatch) -> Vec<bool> {
    let col = batch
        .column(batch.schema().index_of(IS_ACTIVE).unwrap())
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    (0..batch.num_rows()).map(|i| col.value(i)).collect()
}

fn tick_values(batch: &RecordBatch) -> Vec<i32> {
    let col = batch
        .column(batch.schema().index_of(TICK).unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    (0..batch.num_rows()).map(|i| col.value(i)).collect()
}

// ── In-memory ledger store ────────────────────────────────────────────────────
//
// Persists every append; filters by tick and active status on read.  Used in
// contract tests that need to inspect the full historical ledger.

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

    async fn read_table(&self, table_name: &str, filter: ReadFilter) -> Result<Vec<RecordBatch>> {
        let ledger = self.ledger.lock().unwrap();
        let mut result = Vec::new();
        for (tname, batch) in ledger.iter() {
            if tname != table_name {
                continue;
            }
            if let Some(ticks) = &filter.ticks {
                let tick_col = batch
                    .column(batch.schema().index_of(TICK).unwrap())
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let has_tick = (0..batch.num_rows()).any(|i| ticks.contains(&tick_col.value(i)));
                if !has_tick {
                    continue;
                }
            }
            if filter.active_only {
                let active_col = batch
                    .column(batch.schema().index_of(IS_ACTIVE).unwrap())
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                let mask = BooleanArray::from_iter(
                    (0..batch.num_rows()).map(|i| Some(active_col.value(i))),
                );
                let filtered =
                    filter_record_batch(batch, &mask).expect("ledger store filter failed");
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

/// Read all rows for a given tick from the world's ledger.
/// Returns entity_id → position__x mapping.
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
        let eids = entity_ids(batch);
        let xs = x_values(batch);
        let tick_col = batch
            .column(batch.schema().index_of(TICK).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if tick_col.value(i) == tick {
                map.insert(eids[i], xs[i]);
            }
        }
    }
    map
}

// ── AddOne processor ─────────────────────────────────────────────────────────

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
        let next: Vec<f64> = (0..batch.num_rows())
            .map(|i| values.value(i) + 1.0)
            .collect();
        let mut columns = batch.columns().to_vec();
        columns[batch.schema().index_of("position__x")?] = Arc::new(Float64Array::from(next));
        RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Contract 1a: x₀ raw at spawn tick, f applied next tick (first spawn)
//
// Mirrors: tests/core/test_initial_conditions_contract.py
//          ::test_initial_conditions_persist_at_spawn_tick
//
// Python contract: "The first persisted row is x_0, not f(x_0)."
// Ledger must contain x₀=5.0 at tick 0, f(x₀)=6.0 at tick 1,
// f²(x₀)=7.0 at tick 2.
// ════════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn contract1a_x0_raw_at_spawn_tick_f_applied_next_tick() {
    let table = position_table();
    let mut system = AsyncSystem::new();
    system.add_processor(AddOne);
    let mut world = AsyncWorld::from_ids("world-a", "run-a", LedgerStore::default(), system);

    world
        .queue_spawn_batch(&table, component_batch(&[5.0]))
        .unwrap();
    world.run(3).await.unwrap();

    let at_tick: HashMap<i32, f64> = {
        let mut m = HashMap::new();
        for t in 0i32..3 {
            let rows = ledger_x_at_tick(&world, table.table_name(), t).await;
            assert_eq!(
                rows.len(),
                1,
                "tick {t}: expected 1 ledger row, got {}",
                rows.len()
            );
            m.insert(t, *rows.values().next().unwrap());
        }
        m
    };

    // Strict equality enforced by the Python test.
    assert_eq!(
        at_tick[&0], 5.0,
        "tick 0 must be raw spawn (x₀=5.0); got {}",
        at_tick[&0]
    );
    assert_eq!(
        at_tick[&1], 6.0,
        "tick 1 must be f(x₀)=6.0; got {}",
        at_tick[&1]
    );
    assert_eq!(
        at_tick[&2], 7.0,
        "tick 2 must be f²(x₀)=7.0; got {}",
        at_tick[&2]
    );
}

// ════════════════════════════════════════════════════════════════════════════
// Contract 1b: x₀ raw at spawn tick, f applied next tick (mid-run spawn)
//
// Mirrors: tests/core/test_initial_conditions_contract.py
//          ::test_mid_run_spawn_persists_initial_conditions
//
// Python contract: "An entity spawned mid-simulation lands raw at its spawn
// tick and is first transformed on the next tick — while older entities
// keep advancing uninterrupted."
//
// Timeline:
//   tick 0: first → 0.0 (raw x₀)
//   tick 1: first → 1.0 (f applied)
//   tick 2: first → 2.0, second → 100.0 (raw x₀ for mid-run spawn)
//   tick 3: first → 3.0, second → 101.0 (f applied to second)
// ════════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn contract1b_mid_run_spawn_lands_raw_older_entities_keep_advancing() {
    let table = position_table();
    let mut system = AsyncSystem::new();
    system.add_processor(AddOne);
    let mut world = AsyncWorld::from_ids("world-b", "run-b", LedgerStore::default(), system);

    let first_ids = world
        .queue_spawn_batch(&table, component_batch(&[0.0]))
        .unwrap();
    let first_id = first_ids[0];
    world.run(2).await.unwrap(); // ticks 0 (raw) and 1 (f applied)

    let second_ids = world
        .queue_spawn_batch(&table, component_batch(&[100.0]))
        .unwrap();
    let second_id = second_ids[0];
    world.run(2).await.unwrap(); // ticks 2 and 3

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

// ════════════════════════════════════════════════════════════════════════════
// Contract 2: same-tick spawn-cancel leaves no tombstone and no active row
//
// Mirrors: crates/archetype-core/src/aio/async_world.rs
//          ::tests::despawn_in_same_tick_cancels_pending_spawn
//          (which mirrors AsyncWorld.remove_entity same-tick cancel semantics)
//
// Python analogue: AsyncWorld.remove_entity called in the same command batch
//   as create_entity for the same entity — entity is never observed in the
//   ledger (no active row, no tombstone).
//
// The invariant: a same-tick cancel removes the pending spawn row entirely.
// The materialized batch must contain only surviving entities.
// ════════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn contract2_same_tick_spawn_cancel_no_tombstone_no_active_row() {
    // Simple in-memory store that counts every append so we can verify
    // that a cancelled entity never reaches the store.
    #[derive(Clone, Default)]
    struct AppendStore {
        appends: Arc<Mutex<Vec<RecordBatch>>>,
    }

    #[async_trait]
    impl Store for AppendStore {
        async fn append(&self, _table_name: &str, batch: &RecordBatch) -> Result<()> {
            self.appends.lock().unwrap().push(batch.clone());
            Ok(())
        }

        async fn read_table(
            &self,
            _table_name: &str,
            _filter: ReadFilter,
        ) -> Result<Vec<RecordBatch>> {
            Ok(vec![])
        }
    }

    let table = position_table();
    let store = AppendStore::default();
    let appends = store.appends.clone();
    let mut world = AsyncWorld::from_ids("world-c", "run-c", store, AsyncSystem::new());

    // Spawn two entities, then immediately cancel the first.
    let ids = world
        .queue_spawn_batch(&table, component_batch(&[1.5, 2.5]))
        .unwrap();
    world.queue_despawn(ids[0]).unwrap(); // same-tick cancel of entity 0
    world.step().await.unwrap();

    let stored = appends.lock().unwrap();
    // Exactly one append must have happened (entity 1 only).
    assert_eq!(stored.len(), 1);
    let batch = &stored[0];
    let eids = entity_ids(batch);
    let flags = active_flags(batch);

    // Only entity ids[1] is present — ids[0] was cancelled entirely.
    assert_eq!(eids.len(), 1, "only the surviving entity reaches the store");
    assert_eq!(eids[0], ids[1], "surviving entity is ids[1]");
    assert!(flags[0], "surviving entity must be active");

    // No tombstone for the cancelled entity: it must not appear in any row.
    let all_cancelled: bool = eids.iter().all(|&e| e != ids[0]);
    assert!(
        all_cancelled,
        "cancelled entity must not appear in any stored row"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// Contract 3: spawn materialization order deterministic
//             (first-seen, last-write-wins for duplicates)
//
// Mirrors: tests/core/test_async_world_duplicate_spawn_overwrite.py
//          ::test_duplicate_spawn_same_entity_overwrites
//
// Python contract: "Simulate duplicate spawn commands for the same entity
// within the same tick; ensure last write wins."
//
// Three distinct entities are spawned in order x=1, 2, 3.  Their entity IDs
// must appear in first-seen insertion order (1, 2, 3) in the materialized
// batch.  A duplicate spawn for entity 1 (value 99) must overwrite the
// first entry (last-write-wins) while preserving the original first-seen
// position.
// ════════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn contract3_spawn_order_deterministic_first_seen_last_write_wins() {
    use archetype_core::aio::async_world::WorldState;

    let table = position_table();
    let mut state = WorldState::new("world-d", "run-d");

    // Three sequential spawns → entity IDs 1, 2, 3.
    for x in [1.0f64, 2.0, 3.0] {
        state
            .queue_spawn_batch(table.table_name(), table.schema(), component_batch(&[x]))
            .unwrap();
    }

    let frames = state.drain_spawn_frames(table.table_name()).unwrap();
    let batch = arrow_select::concat::concat_batches(&table.schema(), &frames).unwrap();

    // Insertion order preserved.
    let eids = entity_ids(&batch);
    assert_eq!(
        eids,
        vec![1, 2, 3],
        "entities must appear in first-seen order"
    );
    let xs = x_values(&batch);
    assert_eq!(xs, vec![1.0, 2.0, 3.0]);

    // Now test last-write-wins: re-spawn entity 1 with value 99.
    // Use queue_spawn_batch_with_entity_ids to force a duplicate entity ID,
    // mirroring the Python test which sets world.next_entity_id = eid.
    let mut state2 = WorldState::new("world-d2", "run-d2");
    state2
        .queue_spawn_batch(table.table_name(), table.schema(), component_batch(&[1.0]))
        .unwrap();
    // Spawn again with the same entity ID (1) — simulates duplicate command targeting
    // the same entity before materialization.
    state2
        .queue_spawn_batch_with_entity_ids(
            table.table_name(),
            table.schema(),
            component_batch(&[99.0]),
            &[1],
        )
        .unwrap();

    let frames2 = state2.drain_spawn_frames(table.table_name()).unwrap();
    let batch2 = arrow_select::concat::concat_batches(&table.schema(), &frames2).unwrap();

    assert_eq!(
        batch2.num_rows(),
        1,
        "duplicate entity collapsed to one row"
    );
    let eids2 = entity_ids(&batch2);
    let xs2 = x_values(&batch2);
    assert_eq!(eids2[0], 1, "entity ID preserved");
    assert_eq!(xs2[0], 99.0, "last-write-wins: value must be 99.0, not 1.0");
}

// ════════════════════════════════════════════════════════════════════════════
// Contract 4: despawn tombstones at current tick;
//             active_only reads exclude despawned entities
//
// Mirrors: crates/archetype-core/src/aio/async_world.rs
//          ::tests::despawn_marks_prior_row_inactive
//          ::tests::live_snapshot_after_despawn_excludes_inactive_entities
//
// Python analogue: world.remove_entity(eid) → next step writes tombstone
//   (is_active=false) for that entity; subsequent get_components active_only
//   reads do not include it.
//
// Two-phase test:
//   Phase A: spawn entity x=1.5, step → tick 0 persisted active.
//   Phase B: despawn, step → tick 1 persisted inactive (tombstone).
//   Active-only query at tick 1 returns 0 rows.
// ════════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn contract4_despawn_tombstone_at_current_tick_active_only_excludes_it() {
    let table = position_table();
    let store = LedgerStore::default();
    let ledger = store.ledger.clone();
    let mut world = AsyncWorld::from_ids("world-e", "run-e", store, AsyncSystem::new());

    let ids = world
        .queue_spawn_batch(&table, component_batch(&[1.5]))
        .unwrap();
    world.step().await.unwrap(); // tick 0: active row persisted

    world.queue_despawn(ids[0]).unwrap();
    world.step().await.unwrap(); // tick 1: tombstone persisted

    // Verify the ledger has exactly 2 appends: active at tick 0, tombstone at tick 1.
    let rows_all = {
        let l = ledger.lock().unwrap();
        l.iter()
            .filter(|(t, _)| t == table.table_name())
            .map(|(_, b)| b.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        rows_all.len(),
        2,
        "two persisted batches (tick 0 active, tick 1 tombstone)"
    );

    let tick0 = &rows_all[0];
    let tick1 = &rows_all[1];

    // Tick 0: active row.
    assert_eq!(active_flags(tick0), vec![true]);
    assert_eq!(tick_values(tick0), vec![0]);

    // Tick 1: tombstone (is_active=false).
    assert_eq!(
        active_flags(tick1),
        vec![false],
        "despawn must produce tombstone"
    );
    assert_eq!(tick_values(tick1), vec![1], "tombstone must be at tick 1");

    // active_only read at tick 1 must return 0 rows.
    let active_rows = world
        .query_table(
            table.table_name(),
            ReadFilter {
                ticks: Some(vec![1]),
                active_only: true,
                ..ReadFilter::default()
            },
        )
        .await
        .unwrap();
    let total_active: usize = active_rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_active, 0,
        "active_only read must exclude the tombstoned entity"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// Contract 5: update / overlay semantics
//
// Python reference: AsyncWorld.update_entity overlays values without changing
//   archetype/table identity (packages/archetype-ecs/src/archetype/core/aio/async_world.py).
//
// Status: NOT IMPLEMENTED in the Rust kernel.
//   The Rust WorldState has no `update_entity` or equivalent overlay
//   operation.  The kernel supports component migration (via
//   `queue_migration_batch`) but not in-place overlay.
//
// Gap rationale: overlay semantics require locating the entity's current
//   component values in the live snapshot, merging the provided patch, and
//   queuing a re-spawn at the current tick.  This is deferred pending the
//   split-step FFI design (ζ2/C2) which governs how Python-side patch data
//   crosses the Arrow C boundary.
//
// When implemented, the contract to assert is:
//   - queue_update(entity_id, patch_batch) for a live entity
//   - After step(), the persisted row at the current tick reflects the
//     merged values.
//   - The entity remains on the same table (archetype unchanged).
//   - The overlay does not produce a tombstone for the old values.
// ════════════════════════════════════════════════════════════════════════════
#[test]
#[ignore = "Update/overlay not yet implemented in the Rust kernel; \
            deferred pending split-step FFI design (ζ2/C2). \
            When implemented: assert merged patch values, same table, no tombstone."]
fn contract5_update_overlay_semantics_gap() {
    // Placeholder — this test body will be replaced when the kernel gains
    // queue_update / overlay support.
    todo!("implement when update_entity is ported to Rust")
}

// ════════════════════════════════════════════════════════════════════════════
// Contract 6: metadata stamping (world_id, run_id, tick) on every row
//
// Mirrors: tests/core/test_initial_conditions_contract.py (implicit in all
//   query assertions that rely on correct tick values)
//   crates/archetype-core/src/aio/async_world.rs
//   ::tests::async_world_persists_world_run_and_tick_metadata
//
// Python contract: every persisted row carries the correct world_id, run_id,
//   and tick values so that historical queries can filter by time and run.
//
// This test spawns one entity and runs 3 steps.  It then reads the ledger
// at each tick and asserts that world_id, run_id, and tick are correct on
// every row.
// ════════════════════════════════════════════════════════════════════════════
#[tokio::test]
async fn contract6_metadata_stamped_on_every_persisted_row() {
    let table = position_table();
    let store = LedgerStore::default();
    let ledger_ref = store.ledger.clone();
    let mut world = AsyncWorld::from_ids("world-f", "run-f", store, AsyncSystem::new());

    world
        .queue_spawn_batch(&table, component_batch(&[0.0]))
        .unwrap();
    world.run(3).await.unwrap();

    let rows_all = {
        let l = ledger_ref.lock().unwrap();
        l.iter()
            .filter(|(t, _)| t == table.table_name())
            .map(|(_, b)| b.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(rows_all.len(), 3, "three persisted batches (ticks 0, 1, 2)");

    for (expected_tick, batch) in rows_all.iter().enumerate() {
        let world_ids = batch
            .column(batch.schema().index_of(WORLD_ID).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let run_ids = batch
            .column(batch.schema().index_of(RUN_ID).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ticks_col = batch
            .column(batch.schema().index_of(TICK).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(batch.num_rows(), 1);

        assert_eq!(
            world_ids.value(0),
            "world-f",
            "row at tick {expected_tick}: world_id must be 'world-f'"
        );
        assert_eq!(
            run_ids.value(0),
            "run-f",
            "row at tick {expected_tick}: run_id must be 'run-f'"
        );
        assert_eq!(
            ticks_col.value(0),
            expected_tick as i32,
            "row at tick {expected_tick}: tick column must match"
        );
    }
}
