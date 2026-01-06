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

//! Processor trait for ECS behaviors.
//!
//! Processors are the behavioral building blocks of a simulation.
//! Each processor declares which Components it operates on and a priority
//! for execution ordering within the System.
//!
//! The process() method receives Arrow RecordBatches containing all entities
//! matching the archetype signature and returns a transformed RecordBatch.

use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;

use crate::archetype::ArchetypeSignature;
use crate::error::Result;
use crate::resources::Resources;

/// Context passed to processors during execution.
#[derive(Debug)]
pub struct ProcessorContext<'a> {
    /// Current simulation tick.
    pub tick: i32,
    /// World identifier.
    pub world_id: &'a str,
    /// Run identifier.
    pub run_id: &'a str,
    /// Shared resources.
    pub resources: &'a Resources,
    /// Debug mode flag.
    pub debug: bool,
}

/// Trait for async processors that transform entity data.
#[async_trait]
pub trait AsyncProcessor: Send + Sync {
    /// Returns the name of this processor for debugging.
    fn name(&self) -> &str;

    /// Returns the component types this processor operates on.
    fn components(&self) -> &ArchetypeSignature;

    /// Returns the execution priority (lower = earlier).
    fn priority(&self) -> i32 {
        10
    }

    /// Processes a batch of entities, returning a transformed batch.
    ///
    /// This is the main entry point for processor logic. The batch contains
    /// all entities matching the archetype signature.
    async fn process(
        &self,
        batch: RecordBatch,
        ctx: &ProcessorContext<'_>,
    ) -> Result<RecordBatch>;
}

/// A boxed processor for dynamic dispatch.
pub type BoxedProcessor = Arc<dyn AsyncProcessor>;

/// A simple processor that applies a function to batches.
pub struct FnProcessor<F>
where
    F: Fn(RecordBatch, &ProcessorContext<'_>) -> Result<RecordBatch> + Send + Sync,
{
    name: String,
    components: ArchetypeSignature,
    priority: i32,
    process_fn: F,
}

impl<F> FnProcessor<F>
where
    F: Fn(RecordBatch, &ProcessorContext<'_>) -> Result<RecordBatch> + Send + Sync,
{
    /// Creates a new function-based processor.
    pub fn new(
        name: impl Into<String>,
        components: ArchetypeSignature,
        priority: i32,
        process_fn: F,
    ) -> Self {
        FnProcessor {
            name: name.into(),
            components,
            priority,
            process_fn,
        }
    }
}

#[async_trait]
impl<F> AsyncProcessor for FnProcessor<F>
where
    F: Fn(RecordBatch, &ProcessorContext<'_>) -> Result<RecordBatch> + Send + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn components(&self) -> &ArchetypeSignature {
        &self.components
    }

    fn priority(&self) -> i32 {
        self.priority
    }

    async fn process(
        &self,
        batch: RecordBatch,
        ctx: &ProcessorContext<'_>,
    ) -> Result<RecordBatch> {
        (self.process_fn)(batch, ctx)
    }
}

/// A no-op processor for testing.
pub struct NoOpProcessor {
    name: String,
    components: ArchetypeSignature,
    priority: i32,
}

impl NoOpProcessor {
    /// Creates a new no-op processor.
    pub fn new(name: impl Into<String>, components: ArchetypeSignature) -> Self {
        NoOpProcessor {
            name: name.into(),
            components,
            priority: 10,
        }
    }

    /// Sets the priority.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
}

#[async_trait]
impl AsyncProcessor for NoOpProcessor {
    fn name(&self) -> &str {
        &self.name
    }

    fn components(&self) -> &ArchetypeSignature {
        &self.components
    }

    fn priority(&self) -> i32 {
        self.priority
    }

    async fn process(
        &self,
        batch: RecordBatch,
        _ctx: &ProcessorContext<'_>,
    ) -> Result<RecordBatch> {
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::ComponentMeta;
    use crate::resources::Resources;
    use arrow_array::{Int32Array, StringArray, BooleanArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_signature() -> ArchetypeSignature {
        let meta = ComponentMeta::new("TestComponent", "testcomponent__");
        let schema = Schema::new(vec![Field::new("testcomponent__value", DataType::Int32, false)]);
        ArchetypeSignature::new(vec![(meta, schema)])
    }

    fn test_batch() -> RecordBatch {
        let sig = test_signature();
        let schema = crate::archetype::Archetype::get_schema(&sig);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec!["world1"])),
                Arc::new(StringArray::from(vec!["run1"])),
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int32Array::from(vec![42])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_noop_processor() {
        let sig = test_signature();
        let processor = NoOpProcessor::new("TestNoOp", sig);

        let resources = Resources::new();
        let ctx = ProcessorContext {
            tick: 0,
            world_id: "world1",
            run_id: "run1",
            resources: &resources,
            debug: false,
        };

        let batch = test_batch();
        let result = processor.process(batch.clone(), &ctx).await.unwrap();

        assert_eq!(result.num_rows(), batch.num_rows());
    }

    #[tokio::test]
    async fn test_fn_processor() {
        let sig = test_signature();
        let processor = FnProcessor::new(
            "TestFn",
            sig,
            5,
            |batch, _ctx| Ok(batch),
        );

        assert_eq!(processor.priority(), 5);

        let resources = Resources::new();
        let ctx = ProcessorContext {
            tick: 0,
            world_id: "world1",
            run_id: "run1",
            resources: &resources,
            debug: false,
        };

        let batch = test_batch();
        let result = processor.process(batch.clone(), &ctx).await.unwrap();

        assert_eq!(result.num_rows(), batch.num_rows());
    }

    #[test]
    fn test_processor_priority() {
        let sig = test_signature();
        let processor = NoOpProcessor::new("Test", sig).with_priority(1);

        assert_eq!(processor.priority(), 1);
    }
}
