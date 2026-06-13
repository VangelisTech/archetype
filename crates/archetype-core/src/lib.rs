//! Arrow-native core primitives for Archetype.
//!
//! This crate is intentionally independent from Python component classes and
//! table-name hashing. Callers provide Arrow schemas and table names; the core
//! owns world state, mutation buffers, and tick materialization.

pub mod aio;
pub mod archetype;
pub mod error;
pub mod processors;

pub use aio::{
    AsyncSystem, AsyncWorld, EntityId, NativeSystem, Processor, ProcessorContext, ReadFilter,
    StepProfile, Store, TableStepProfile, WorldState,
};
pub use archetype::{ArchetypeTable, BASE_COLUMN_NAMES, base_schema};
pub use error::{ArchetypeCoreError, Result};
