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

//! Benchmarks for the Archetype AIO engine.

use std::collections::HashMap;
use std::sync::Arc;

use archetype_aio::archetype::ArchetypeSignature;
use archetype_aio::component::ComponentMeta;
use archetype_aio::config::{RunConfig, WorldConfig};
use archetype_aio::querier::QueryManager;
use archetype_aio::store::InMemoryStore;
use archetype_aio::system::System;
use archetype_aio::updater::UpdateManager;
use archetype_aio::world::AsyncWorld;
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_json::Value;
use tokio::runtime::Runtime;

fn test_signature() -> ArchetypeSignature {
    let meta = ComponentMeta::new("BenchComponent", "benchcomponent__");
    let schema = Schema::new(vec![
        Field::new("benchcomponent__x", DataType::Float64, false),
        Field::new("benchcomponent__y", DataType::Float64, false),
        Field::new("benchcomponent__z", DataType::Float64, false),
    ]);
    ArchetypeSignature::new(vec![(meta, schema)])
}

fn create_world() -> AsyncWorld<QueryManager<InMemoryStore>, UpdateManager<InMemoryStore>, System> {
    let store = Arc::new(InMemoryStore::new());
    let querier = Arc::new(QueryManager::new(store.clone()));
    let updater = Arc::new(UpdateManager::new(store));
    let system = Arc::new(System::new());
    let config = WorldConfig::with_name("benchmark");

    AsyncWorld::new(config, querier, updater, system)
}

fn bench_entity_creation(c: &mut Criterion) {
    c.bench_function("create_100_entities", |b| {
        b.iter(|| {
            let world = create_world();
            let sig = test_signature();

            for i in 0..100 {
                let mut components = HashMap::new();
                components.insert(
                    "benchcomponent__x".to_string(),
                    Value::Number(serde_json::Number::from_f64(i as f64).unwrap()),
                );
                components.insert(
                    "benchcomponent__y".to_string(),
                    Value::Number(serde_json::Number::from_f64(i as f64 * 2.0).unwrap()),
                );
                components.insert(
                    "benchcomponent__z".to_string(),
                    Value::Number(serde_json::Number::from_f64(i as f64 * 3.0).unwrap()),
                );
                black_box(world.create_entity(sig.clone(), components));
            }
        });
    });
}

fn bench_world_step(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("world_step_100_entities", |b| {
        b.iter(|| {
            let world = create_world();
            let sig = test_signature();

            // Create entities
            for i in 0..100 {
                let mut components = HashMap::new();
                components.insert(
                    "benchcomponent__x".to_string(),
                    Value::Number(serde_json::Number::from_f64(i as f64).unwrap()),
                );
                world.create_entity(sig.clone(), components);
            }

            // Run one step
            let run_config = RunConfig::benchmark(1);
            rt.block_on(async {
                black_box(world.step(&run_config).await.unwrap());
            });
        });
    });
}

criterion_group!(benches, bench_entity_creation, bench_world_step);
criterion_main!(benches);
