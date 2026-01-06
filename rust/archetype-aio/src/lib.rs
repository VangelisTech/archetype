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

//! # Archetype AIO Engine
//!
//! A data-centric Entity-Component-System (ECS) runtime built on Arrow and Tokio.
//!
//! ## Core Concepts
//!
//! - **Component**: Data attached to entities (pure data, no behavior)
//! - **Archetype**: A unique combination of component types
//! - **Processor**: Transforms entity data for specific archetypes
//! - **System**: Orchestrates processor execution in priority order
//! - **World**: Central simulation coordinator
//!
//! ## Architecture
//!
//! The engine uses a columnar data model where:
//! - Entity state is stored as Arrow RecordBatches
//! - Each tick is an append-only write to storage
//! - Processors are pure DataFrame transforms
//!
//! This enables:
//! - Time-travel queries (query any tick)
//! - World forking (branch futures)
//! - Parallel archetype processing

pub mod archetype;
pub mod component;
pub mod config;
pub mod error;
pub mod processor;
pub mod querier;
pub mod resources;
pub mod store;
pub mod system;
pub mod updater;
pub mod world;

pub use archetype::{Archetype, ArchetypeSignature};
pub use component::Component;
pub use config::{RunConfig, WorldConfig};
pub use error::{ArchetypeError, Result};
pub use processor::AsyncProcessor;
pub use querier::AsyncQuerier;
pub use resources::Resources;
pub use store::AsyncStore;
pub use system::AsyncSystem;
pub use updater::AsyncUpdater;
pub use world::AsyncWorld;
