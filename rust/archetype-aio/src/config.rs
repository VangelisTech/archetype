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

//! Configuration types for the Archetype engine.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Configuration for a simulation run.
///
/// A run represents a sequence of world steps with specific runtime options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    /// Unique identifier for this run (UUID v7 for time-ordering).
    pub run_id: Uuid,
    /// Number of steps to execute.
    pub num_steps: u32,
    /// Enable debug logging.
    pub debug: bool,
    /// Enable validation mode.
    pub enable_validation: bool,
    /// Max rows to display in debug panels.
    pub show_rows: u32,
    /// Whether to render DataFrame logical plans in debug.
    pub explain: bool,
    /// Use in-memory live snapshot for reads instead of storage.
    pub prefer_live_reads: bool,
    /// Optional suite/experiment label.
    pub suite: Option<String>,
    /// Optional trial index for ensemble runs.
    pub trial: Option<u32>,
    /// Arbitrary metadata for experiment tracking.
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

impl Default for RunConfig {
    fn default() -> Self {
        RunConfig {
            run_id: Uuid::now_v7(),
            num_steps: 1,
            debug: false,
            enable_validation: false,
            show_rows: 3,
            explain: false,
            prefer_live_reads: false,
            suite: None,
            trial: None,
            metadata: None,
        }
    }
}

impl RunConfig {
    /// Creates a new run configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a development configuration.
    ///
    /// Optimized for rapid iteration with debug enabled and live reads.
    pub fn dev(steps: u32) -> Self {
        RunConfig {
            num_steps: steps,
            debug: true,
            prefer_live_reads: true,
            show_rows: 5,
            ..Default::default()
        }
    }

    /// Creates a benchmark configuration.
    ///
    /// Optimized for performance measurement with minimal overhead.
    pub fn benchmark(steps: u32) -> Self {
        RunConfig {
            num_steps: steps,
            debug: false,
            prefer_live_reads: false,
            show_rows: 0,
            suite: Some("benchmark".to_string()),
            ..Default::default()
        }
    }

    /// Creates a validation configuration.
    ///
    /// Enables validation checks for debugging data integrity.
    pub fn validate(steps: u32) -> Self {
        RunConfig {
            num_steps: steps,
            enable_validation: true,
            debug: true,
            suite: Some("validate".to_string()),
            ..Default::default()
        }
    }

    /// Builder method to set the number of steps.
    pub fn with_steps(mut self, steps: u32) -> Self {
        self.num_steps = steps;
        self
    }

    /// Builder method to enable debug mode.
    pub fn with_debug(mut self, debug: bool) -> Self {
        self.debug = debug;
        self
    }

    /// Builder method to set prefer_live_reads.
    pub fn with_live_reads(mut self, prefer_live_reads: bool) -> Self {
        self.prefer_live_reads = prefer_live_reads;
        self
    }

    /// Builder method to set metadata.
    pub fn with_metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// Configuration for a world instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorldConfig {
    /// Unique identifier for the world (UUID v7).
    pub world_id: Uuid,
    /// Human-readable name for the world.
    pub name: Option<String>,
}

impl Default for WorldConfig {
    fn default() -> Self {
        WorldConfig {
            world_id: Uuid::now_v7(),
            name: None,
        }
    }
}

impl WorldConfig {
    /// Creates a new world configuration with a generated ID.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a world configuration with a specific name.
    pub fn with_name(name: impl Into<String>) -> Self {
        WorldConfig {
            world_id: Uuid::now_v7(),
            name: Some(name.into()),
        }
    }

    /// Builder method to set the name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

/// Storage backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// URI for storage location.
    pub uri: String,
    /// Namespace for archetype tables.
    pub namespace: String,
    /// Storage backend type.
    pub backend: StorageBackend,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            uri: "./archetype_data".to_string(),
            namespace: "archetypes".to_string(),
            backend: StorageBackend::InMemory,
        }
    }
}

/// Storage backend type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// In-memory storage (for testing/development).
    InMemory,
    /// LanceDB storage.
    LanceDb,
    /// Iceberg storage.
    Iceberg,
}

/// Cache configuration for the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Number of rows to trigger a flush.
    pub flush_rows: usize,
    /// Megabytes to trigger a flush.
    pub flush_mb: usize,
    /// Global cache size in megabytes.
    pub global_mb: usize,
    /// Idle seconds before flushing.
    pub idle_sec: f64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            flush_rows: 1_000_000,
            flush_mb: 512,
            global_mb: 1024,
            idle_sec: 30.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_config_default() {
        let config = RunConfig::default();
        assert_eq!(config.num_steps, 1);
        assert!(!config.debug);
        assert!(!config.prefer_live_reads);
    }

    #[test]
    fn test_run_config_dev() {
        let config = RunConfig::dev(10);
        assert_eq!(config.num_steps, 10);
        assert!(config.debug);
        assert!(config.prefer_live_reads);
    }

    #[test]
    fn test_run_config_benchmark() {
        let config = RunConfig::benchmark(100);
        assert_eq!(config.num_steps, 100);
        assert!(!config.debug);
        assert_eq!(config.suite, Some("benchmark".to_string()));
    }

    #[test]
    fn test_world_config() {
        let config = WorldConfig::with_name("test_world");
        assert_eq!(config.name, Some("test_world".to_string()));
    }

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();
        assert_eq!(config.backend, StorageBackend::InMemory);
        assert_eq!(config.namespace, "archetypes");
    }
}
