# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
from typing import List

import ray
from daft import DataFrame

from archetype.core.aio.async_interfaces import iAsyncQuerier, iAsyncUpdater
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.base import BaseProcessor
from archetype.core.interfaces import ArchetypeSignature
from archetype.core.processor import Processor as SyncProcessor


@ray.remote
class RaySystem:
    """
    Shared Ray actor for processing archetypes across multiple worlds.
    
    This actor is stateless and can process any archetype for any world.
    Multiple RaySystem actors form a pool managed by RayUniverse.
    """

    def __init__(
        self,
        querier: iAsyncQuerier,
        updater: iAsyncUpdater,
        processors: List[BaseProcessor],
        semaphore_limit: int = 10,
    ):
        self.querier = querier
        self.updater = updater
        self.processors = processors
        self.semaphore = asyncio.Semaphore(semaphore_limit)

    async def process_archetype(
        self,
        sig: ArchetypeSignature,
        step: int, 
        world_id: str, 
        run_id: str, 
        *args,
        **kwargs
    ) -> None:
        """
        Process a single archetype for a given world and step.

        This is the main entry point called by RayWorld actors.
        """
        # Get the archetype data
        df = await self.querier.get_archetype(sig, step, world_id, run_id)
        
        # If no data, nothing to process
        if df is None or len(df) == 0:
            return
        
        # Process through all relevant processors
        processed_df = await self._execute_processors(df, sig, *args, **kwargs)
        
        # Update the store with results
        if processed_df is not None:
            await self.updater.update(processed_df, sig, step, world_id, run_id)

    async def _execute_processors(self, df: DataFrame, sig: ArchetypeSignature, *args, **kwargs) -> DataFrame:
        """
        Execute all relevant processors on the dataframe.
        """
        for proc_instance in sorted(self.processors, key=lambda x: x.priority):
            if set(proc_instance.components).issubset(set(sig)):
                try:
                    if isinstance(proc_instance, AsyncProcessor):
                        df = await proc_instance.process(df, self.semaphore, *args, **kwargs)
                    elif isinstance(proc_instance, SyncProcessor):
                        df = proc_instance.process(df, *args, **kwargs)
                except Exception as e:
                    # In a distributed context, we should handle errors gracefully
                    print(f"Error processing archetype {sig} with {proc_instance}: {e}")
                    return None
        return df

    async def add_processor(self, processor: BaseProcessor) -> None:
        """Add a processor to this system actor."""
        self.processors.append(processor)

    async def remove_processor(self, processor_type: type) -> None:
        """Remove all processors of a given type."""
        self.processors = [p for p in self.processors if not isinstance(p, processor_type)]

    def get_processor_count(self) -> int:
        """Get the number of processors in this system."""
        return len(self.processors)