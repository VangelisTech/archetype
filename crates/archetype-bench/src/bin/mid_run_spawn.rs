//! Mid-run-spawn benchmark scenario.
//!
//! Exercises the tick-zero-correct spawn ordering with entities that arrive
//! at different ticks during the simulation, providing a fair performance
//! comparison against the all-at-start scenario in `movement_compare`.
//!
//! Timeline:
//!   tick 0:       entities 0..initial_entities spawn raw (x₀ preserved).
//!   tick spawn_at: entities initial_entities..total_entities spawn mid-run raw.
//!   Final tick (ticks-1): all surviving entities have been processed correctly.
//!
//! Correctness check (tick-zero contract):
//!   An entity spawned at tick T with initial value xᵢ should have value
//!   xᵢ + (final_tick - T) * dt at the final tick, because processors first
//!   apply on tick T+1.
//!
//!   early  entities (spawn tick 0):  xᵢ + (ticks-1) * dt
//!   late   entities (spawn tick S):  xᵢ + (ticks-1-S) * dt
//!
//! The benchmark reports correctness fields alongside timing so results
//! cannot be considered valid unless `correct` is true.

use std::env;
use std::sync::Arc;
use std::time::Instant;

use archetype_core::archetype::ArchetypeTable;
use archetype_core::processors::movement::MovementProcessor;
use archetype_core::{AsyncSystem, AsyncWorld, ReadFilter};
use archetype_parquet::ParquetStore;
use arrow_array::{Array, ArrayRef, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde_json::json;

fn component_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("position__x", DataType::Float64, false),
        Field::new("position__y", DataType::Float64, false),
        Field::new("velocity__dx", DataType::Float64, false),
        Field::new("velocity__dy", DataType::Float64, false),
    ]))
}

fn spawn_batch(component_schema: SchemaRef, start_idx: usize, count: usize) -> RecordBatch {
    let x: Vec<f64> = (start_idx..start_idx + count).map(|i| i as f64).collect();
    RecordBatch::try_new(
        component_schema,
        vec![
            Arc::new(Float64Array::from(x)) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.0; count])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0; count])) as ArrayRef,
            Arc::new(Float64Array::from(vec![-0.5; count])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn float64_sum(batch: &RecordBatch, column_name: &str) -> f64 {
    let values = batch
        .column(batch.schema().index_of(column_name).unwrap())
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .sum()
}

#[tokio::main]
async fn main() {
    // Defaults: 1 000 initial entities, 200 late-spawn entities, spawn at tick 2, 5 ticks total.
    let mut initial_entities = 1000usize;
    let mut late_entities = 200usize;
    let mut spawn_at_tick = 2i32;
    let mut ticks = 5i32;
    let mut root = env::temp_dir().join(format!(
        "archetype-rust-mid-run-spawn-{}",
        std::process::id()
    ));

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--initial-entities" => initial_entities = args.next().unwrap().parse().unwrap(),
            "--late-entities" => late_entities = args.next().unwrap().parse().unwrap(),
            "--spawn-at-tick" => spawn_at_tick = args.next().unwrap().parse().unwrap(),
            "--ticks" => ticks = args.next().unwrap().parse().unwrap(),
            "--root" => root = args.next().unwrap().into(),
            _ => panic!("unknown argument: {arg}"),
        }
    }

    assert!(
        spawn_at_tick < ticks,
        "--spawn-at-tick ({spawn_at_tick}) must be less than --ticks ({ticks})"
    );

    let total_entities = initial_entities + late_entities;
    let store = ParquetStore::new(&root, "mid_run_spawn_bench");
    let table = ArchetypeTable::new("position_velocity", component_schema()).unwrap();

    let setup_start = Instant::now();
    let mut system = AsyncSystem::new();
    system.add_processor(MovementProcessor { dt: 1.0 });
    let mut world = AsyncWorld::from_ids("mid-run-world", "mid-run-run", store, system);

    // Spawn initial entities at tick 0.
    let initial_batch = spawn_batch(component_schema(), 0, initial_entities);
    world.queue_spawn_batch(&table, initial_batch).unwrap();
    let setup_sec = setup_start.elapsed().as_secs_f64();

    let run_start = Instant::now();
    let mut read_prior_sec = 0.0;
    let mut materialize_sec = 0.0;
    let mut process_sec = 0.0;
    let mut append_sec = 0.0;
    let mut live_snapshot_sec = 0.0;
    let mut profiled_tick_sec = 0.0;

    for tick in 0..ticks {
        if tick == spawn_at_tick {
            // Mid-run spawn: these entities land raw this tick; processor
            // first applies on tick (spawn_at_tick + 1).
            let late_batch = spawn_batch(component_schema(), initial_entities, late_entities);
            world.queue_spawn_batch(&table, late_batch).unwrap();
        }

        let profile = world.step_profiled().await.unwrap();
        profiled_tick_sec += profile.tick_sec;
        for table_profile in profile.tables {
            read_prior_sec += table_profile.read_prior_sec;
            materialize_sec += table_profile.materialize_sec;
            process_sec += table_profile.process_sec;
            append_sec += table_profile.append_sec;
            live_snapshot_sec += table_profile.live_snapshot_sec;
        }
    }
    let run_sec = run_start.elapsed().as_secs_f64();

    let query_start = Instant::now();
    let final_batches = world
        .query_table(
            table.table_name(),
            ReadFilter {
                ticks: Some(vec![ticks - 1]),
                active_only: true,
                ..ReadFilter::default()
            },
        )
        .await
        .unwrap();
    let rows = final_batches.iter().map(|b| b.num_rows()).sum::<usize>();
    let sum_position_x = final_batches
        .iter()
        .map(|b| float64_sum(b, "position__x"))
        .sum::<f64>();
    let sum_position_y = final_batches
        .iter()
        .map(|b| float64_sum(b, "position__y"))
        .sum::<f64>();
    let query_sec = query_start.elapsed().as_secs_f64();

    // Tick-zero contract correctness check.
    //
    // Early entities (spawn tick 0): position__x[i] = i + (ticks-1) * dt
    // Late  entities (spawn tick S): position__x[i] = i + (ticks-1-S) * dt
    //
    // sum_x (early) = Σ_{i=0}^{n_e - 1}  i  +  n_e * (ticks-1) * dt
    //               = n_e*(n_e-1)/2          +  n_e * (ticks-1)
    //
    // sum_x (late)  = Σ_{i=n_e}^{n_tot-1} i  +  n_l * (ticks-1-S) * dt
    //               = n_l*(2*n_e + n_l - 1)/2  +  n_l * (ticks-1-S)
    //
    // sum_y = n_e * (ticks-1) * dy  +  n_l * (ticks-1-S) * dy
    //       (dy = -0.5)
    let n_e = initial_entities as f64;
    let n_l = late_entities as f64;
    let n_tot = total_entities as f64;
    let t_final = (ticks - 1) as f64;
    let t_late_steps = (ticks - 1 - spawn_at_tick) as f64; // steps applied to late entities
    let _ = n_tot;

    // sum_x for early entities: Σi for i in 0..n_e + n_e * t_final
    let expected_sum_x_early = n_e * (n_e - 1.0) / 2.0 + n_e * t_final;
    // sum_x for late entities: Σi for i in n_e..n_e+n_l + n_l * t_late_steps
    let expected_sum_x_late = n_l * (2.0 * n_e + n_l - 1.0) / 2.0 + n_l * t_late_steps;
    let expected_sum_x = expected_sum_x_early + expected_sum_x_late;

    // sum_y for early: n_e * t_final * dy
    let expected_sum_y_early = n_e * t_final * -0.5;
    // sum_y for late: n_l * t_late_steps * dy
    let expected_sum_y_late = n_l * t_late_steps * -0.5;
    let expected_sum_y = expected_sum_y_early + expected_sum_y_late;

    let sum_x_abs_error = (sum_position_x - expected_sum_x).abs();
    let sum_y_abs_error = (sum_position_y - expected_sum_y).abs();
    let correct = rows == total_entities && sum_x_abs_error < 1e-9 && sum_y_abs_error < 1e-9;

    println!(
        "{}",
        json!({
            "backend": "rust-arrow-parquet-mid-run-spawn",
            "scenario": "mid_run_spawn",
            "initial_entities": initial_entities,
            "late_entities": late_entities,
            "total_entities": total_entities,
            "ticks": ticks,
            "spawn_at_tick": spawn_at_tick,
            "rows": rows,
            "setup_sec": setup_sec,
            "run_sec": run_sec,
            "query_sec": query_sec,
            "total_sec": setup_sec + run_sec + query_sec,
            "phases_sec": {
                "setup": setup_sec,
                "run": run_sec,
                "read_prior": read_prior_sec,
                "materialize": materialize_sec,
                "process": process_sec,
                "append": append_sec,
                "live_snapshot": live_snapshot_sec,
                "profiled_tick": profiled_tick_sec,
                "query": query_sec,
                "total": setup_sec + run_sec + query_sec,
            },
            "correctness": {
                "expected_rows": total_entities,
                "rows_match": rows == total_entities,
                "sum_position_x": sum_position_x,
                "sum_position_y": sum_position_y,
                "expected_sum_position_x": expected_sum_x,
                "expected_sum_position_y": expected_sum_y,
                "sum_x_abs_error": sum_x_abs_error,
                "sum_y_abs_error": sum_y_abs_error,
                "position_sums_match": sum_x_abs_error < 1e-9 && sum_y_abs_error < 1e-9,
                "correct": correct,
            },
            "correct": correct,
        })
    );

    assert!(correct, "mid-run-spawn correctness check failed");
}
