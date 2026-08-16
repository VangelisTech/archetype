//! Async core boundary modules.
//!
//! The file names intentionally mirror `packages/archetype-ecs/src/archetype/core/aio` in the Python
//! package. Rust currently has a synchronous processor trait and world-state
//! kernel inside this async boundary; the store trait is already async, and the
//! world executor will grow here.

pub mod async_processor;
pub mod async_store;
pub mod async_system;
pub mod async_world;

pub use async_processor::{Processor, ProcessorContext};
pub use async_store::{ReadFilter, Store};
pub use async_system::{AsyncSystem, NativeSystem};
pub use async_world::{AsyncWorld, EntityId, StepProfile, TableStepProfile, WorldState};
