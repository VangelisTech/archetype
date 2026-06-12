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

fn initial_component_batch(component_schema: SchemaRef, entities: usize) -> RecordBatch {
    let x = (0..entities).map(|idx| idx as f64).collect::<Vec<_>>();

    RecordBatch::try_new(
        component_schema,
        vec![
            Arc::new(Float64Array::from(x)) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.0; entities])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0; entities])) as ArrayRef,
            Arc::new(Float64Array::from(vec![-0.5; entities])) as ArrayRef,
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
    let mut entities = 1000usize;
    let mut ticks = 3i32;
    let mut root = env::temp_dir().join(format!("archetype-rust-movement-{}", std::process::id()));

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--entities" => entities = args.next().unwrap().parse().unwrap(),
            "--ticks" => ticks = args.next().unwrap().parse().unwrap(),
            "--root" => root = args.next().unwrap().into(),
            _ => panic!("unknown argument: {arg}"),
        }
    }

    let store = ParquetStore::new(&root, "movement_bench");
    let table = ArchetypeTable::new("position_velocity", component_schema()).unwrap();

    let setup_start = Instant::now();
    let mut system = AsyncSystem::new();
    system.add_processor(MovementProcessor { dt: 1.0 });
    let mut world = AsyncWorld::from_ids("movement-world", "movement-run", store, system);
    let batch = initial_component_batch(component_schema(), entities);
    world.queue_spawn_batch(&table, batch).unwrap();
    let setup_sec = setup_start.elapsed().as_secs_f64();

    let run_start = Instant::now();
    let mut read_prior_sec = 0.0;
    let mut materialize_sec = 0.0;
    let mut process_sec = 0.0;
    let mut append_sec = 0.0;
    let mut live_snapshot_sec = 0.0;
    let mut profiled_tick_sec = 0.0;
    for _ in 0..ticks {
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
    let rows = final_batches
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    let sum_position_x = final_batches
        .iter()
        .map(|batch| float64_sum(batch, "position__x"))
        .sum::<f64>();
    let sum_position_y = final_batches
        .iter()
        .map(|batch| float64_sum(batch, "position__y"))
        .sum::<f64>();
    let query_sec = query_start.elapsed().as_secs_f64();
    let expected_sum_position_x =
        (entities * entities.saturating_sub(1)) as f64 / 2.0 + entities as f64 * ticks as f64;
    let expected_sum_position_y = entities as f64 * ticks as f64 * -0.5;
    let sum_position_x_abs_error = (sum_position_x - expected_sum_position_x).abs();
    let sum_position_y_abs_error = (sum_position_y - expected_sum_position_y).abs();
    let correct =
        rows == entities && sum_position_x_abs_error < 1e-9 && sum_position_y_abs_error < 1e-9;

    println!(
        "{}",
        json!({
            "backend": "rust-arrow-parquet-prototype",
            "entities": entities,
            "ticks": ticks,
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
                "expected_rows": entities,
                "rows_match": rows == entities,
                "sum_position_x": sum_position_x,
                "sum_position_y": sum_position_y,
                "expected_sum_position_x": expected_sum_position_x,
                "expected_sum_position_y": expected_sum_position_y,
                "sum_position_x_abs_error": sum_position_x_abs_error,
                "sum_position_y_abs_error": sum_position_y_abs_error,
                "position_sums_match": sum_position_x_abs_error < 1e-9 && sum_position_y_abs_error < 1e-9,
                "correct": correct,
            },
            "correct": correct,
        })
    );
}
