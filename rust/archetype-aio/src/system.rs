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

//! System for orchestrating processor execution.
//!
//! The System manages a collection of Processors and executes them
//! on matching archetypes during each simulation step. Lower priority
//! values execute first.

use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use parking_lot::RwLock;
use tracing::{debug, error};

use crate::archetype::ArchetypeSignature;
use crate::error::Result;
use crate::processor::{AsyncProcessor, BoxedProcessor, ProcessorContext};

/// Trait for async system operations.
#[async_trait]
pub trait AsyncSystem: Send + Sync {
    /// Adds a processor to the system.
    async fn add_processor(&self, processor: BoxedProcessor);

    /// Removes all processors with the given name.
    async fn remove_processor(&self, name: &str);

    /// Executes all matching processors on an archetype.
    async fn execute(
        &self,
        batch: RecordBatch,
        sig: &ArchetypeSignature,
        ctx: &ProcessorContext<'_>,
    ) -> Result<RecordBatch>;
}

/// System implementation that executes processors in priority order.
#[derive(Default)]
pub struct System {
    processors: RwLock<Vec<BoxedProcessor>>,
}

impl System {
    /// Creates a new empty system.
    pub fn new() -> Self {
        System {
            processors: RwLock::new(Vec::new()),
        }
    }

    /// Returns the number of registered processors.
    pub fn processor_count(&self) -> usize {
        self.processors.read().len()
    }

    /// Checks if a processor matches an archetype signature.
    fn processor_matches(processor: &dyn AsyncProcessor, sig: &ArchetypeSignature) -> bool {
        processor.components().type_ids().is_subset(sig.type_ids())
    }
}

#[async_trait]
impl AsyncSystem for System {
    async fn add_processor(&self, processor: BoxedProcessor) {
        let mut processors = self.processors.write();
        processors.push(processor);
        // Sort by priority (lower first)
        processors.sort_by_key(|p| p.priority());
    }

    async fn remove_processor(&self, name: &str) {
        let mut processors = self.processors.write();
        processors.retain(|p| p.name() != name);
    }

    async fn execute(
        &self,
        mut batch: RecordBatch,
        sig: &ArchetypeSignature,
        ctx: &ProcessorContext<'_>,
    ) -> Result<RecordBatch> {
        // Clone processors to avoid holding lock across await
        let processors: Vec<BoxedProcessor> = self.processors.read().clone();

        for processor in processors.iter() {
            // Check if processor's components are a subset of the archetype
            if !Self::processor_matches(processor.as_ref(), sig) {
                continue;
            }

            let proc_name = processor.name();

            if ctx.debug {
                debug!(
                    processor = %proc_name,
                    priority = %processor.priority(),
                    "processor_start"
                );
            }

            match processor.process(batch.clone(), ctx).await {
                Ok(result) => {
                    if ctx.debug {
                        debug!(
                            processor = %proc_name,
                            rows_out = %result.num_rows(),
                            "processor_end"
                        );
                    }
                    batch = result;
                }
                Err(e) => {
                    error!(
                        processor = %proc_name,
                        error = %e,
                        "Processor execution failed"
                    );
                    // Continue with original batch on error
                }
            }
        }

        Ok(batch)
    }
}

/// Type alias for shared system.
pub type SharedSystem = Arc<dyn AsyncSystem>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archetype::Archetype;
    use crate::component::ComponentMeta;
    use crate::processor::NoOpProcessor;
    use crate::resources::Resources;
    use arrow_array::{BooleanArray, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_signature() -> ArchetypeSignature {
        let meta = ComponentMeta::new("TestComponent", "testcomponent__");
        let schema = Schema::new(vec![Field::new("testcomponent__value", DataType::Int32, false)]);
        ArchetypeSignature::new(vec![(meta, schema)])
    }

    fn test_batch() -> RecordBatch {
        let sig = test_signature();
        let schema = Archetype::get_schema(&sig);

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
    async fn test_system_add_processor() {
        let system = System::new();
        let sig = test_signature();
        let processor = Arc::new(NoOpProcessor::new("Test", sig));

        system.add_processor(processor).await;
        assert_eq!(system.processor_count(), 1);
    }

    #[tokio::test]
    async fn test_system_remove_processor() {
        let system = System::new();
        let sig = test_signature();
        let processor = Arc::new(NoOpProcessor::new("Test", sig));

        system.add_processor(processor).await;
        system.remove_processor("Test").await;
        assert_eq!(system.processor_count(), 0);
    }

    #[tokio::test]
    async fn test_system_execute() {
        let system = System::new();
        let sig = test_signature();
        let processor = Arc::new(NoOpProcessor::new("Test", sig.clone()));

        system.add_processor(processor).await;

        let resources = Resources::new();
        let ctx = ProcessorContext {
            tick: 0,
            world_id: "world1",
            run_id: "run1",
            resources: &resources,
            debug: false,
        };

        let batch = test_batch();
        let result = system.execute(batch.clone(), &sig, &ctx).await.unwrap();

        assert_eq!(result.num_rows(), batch.num_rows());
    }

    #[tokio::test]
    async fn test_system_priority_order() {
        let system = System::new();
        let sig = test_signature();

        // Add processors in reverse priority order
        let p1 = Arc::new(NoOpProcessor::new("High", sig.clone()).with_priority(10));
        let p2 = Arc::new(NoOpProcessor::new("Low", sig.clone()).with_priority(1));
        let p3 = Arc::new(NoOpProcessor::new("Medium", sig.clone()).with_priority(5));

        system.add_processor(p1).await;
        system.add_processor(p2).await;
        system.add_processor(p3).await;

        // Verify they're sorted by priority
        let processors = system.processors.read();
        assert_eq!(processors[0].name(), "Low");
        assert_eq!(processors[1].name(), "Medium");
        assert_eq!(processors[2].name(), "High");
    }
}
