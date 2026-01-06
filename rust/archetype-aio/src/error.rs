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

//! Error types for the Archetype AIO engine.

use thiserror::Error;

/// Result type alias for Archetype operations.
pub type Result<T> = std::result::Result<T, ArchetypeError>;

/// Errors that can occur during Archetype operations.
#[derive(Error, Debug)]
pub enum ArchetypeError {
    /// Entity not found in the world.
    #[error("Entity {0} not found")]
    EntityNotFound(i32),

    /// Component type not found.
    #[error("Component type '{0}' not found")]
    ComponentNotFound(String),

    /// Resource not registered in the container.
    #[error("Resource '{0}' not registered")]
    ResourceNotFound(String),

    /// Schema mismatch between expected and actual.
    #[error("Schema mismatch: expected {expected}, got {actual}")]
    SchemaMismatch { expected: String, actual: String },

    /// Invalid archetype signature.
    #[error("Invalid archetype signature: {0}")]
    InvalidSignature(String),

    /// Storage operation failed.
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Processor execution failed.
    #[error("Processor '{processor}' failed: {message}")]
    ProcessorError { processor: String, message: String },

    /// Arrow operation failed.
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// JSON serialization/deserialization failed.
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// IO operation failed.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// World is already running.
    #[error("World is already running")]
    WorldAlreadyRunning,

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    ConfigError(String),
}
