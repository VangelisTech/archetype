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


import time
from itertools import count
from typing import List, Type, Optional, Dict, Any, Tuple, Set
import ulid
import asyncio

from daft import DataFrame

from archetype.core import Component, Archetype, ArchetypeSignature
from archetype.core.base import BaseWorld
from archetype.core.aio.async_interfaces import iAsyncStore, iAsyncWorld, iAsyncQuerier, iAsyncUpdater, iAsyncSystem, iAsyncProcessor

from logging import getLogger

logger = getLogger(__name__)

class AsyncWorld(BaseWorld, iAsyncWorld):
    def __init__(self,
        store: iAsyncStore,
        querier: iAsyncQuerier,
        updater: iAsyncUpdater,
        system: iAsyncSystem,
        world_id: str = None,
        run_id: str = None,
        semaphore: asyncio.Semaphore = None,
        debug: bool = False,
    ):
        """
        Initialize async world using shared base functionality.
        """
        # Initialize base world functionality
        super().__init__(world_id, run_id, debug)

        # Inject async dependencies
        self.querier = querier
        self.updater = updater
        self.async_system = system

        # Set semaphore for concurrent archetype processing
        self.semaphore = semaphore or asyncio.Semaphore(10)

    async def step(self, *args, **kwargs):
        """
        Async version of step() with concurrent archetype processing.
        """
        # Materialize any pending spawns before the first step
        start = time.time()

        await self.materialize_spawns()

        # Get all active archetypes with their component signatures (single query)
        active_signatures = self.get_active_signatures()

        async def process_with_semaphore(sig: ArchetypeSignature, *args, **kwargs) -> None:
            async with self.semaphore:
                df = await self.get_archetype(sig, self.current_step)
                processed_df = await self.async_system.execute(df, sig, self.semaphore, *args, **kwargs)
                await self.update((sig, processed_df), self.current_step + 1, self.world_id, self.run_id)

        tasks = [
            process_with_semaphore(sig, *args, **kwargs)
            for sig in active_signatures
        ]

        await asyncio.gather(*tasks)

        end = time.time()
        print(f"Async Step {self.current_step} done in {end-start:.3f}s")

        self.current_step += 1

    def spawn(self, *components: Component, step: Optional[int] = None) -> int:
        """Create a new entity with these components."""
        assert len(components) != 0, "Cannot create an entity with no components"

        # Use store to add entity and get ID
        entity_id = self.store.add_entity(list(components), step or self.current_step, self.world_id, self.run_id)
        sig = Archetype.sig_from_components(components)

        # Create row dict using base class helper
        row_dict = self._create_spawn_row_dict(entity_id, list(components), step or self.current_step)

        # Add to spawn cache using base class helper
        self._add_to_spawn_cache(sig, row_dict)

        return entity_id

    async def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        """Mark an entity dead (is_active=False)."""
        await self.store.remove_entity(entity_id, step or self.current_step, self.world_id, self.run_id)

    # ---------------------------------------------------------------------
    # iAsyncSystem Facade methods
    # ---------------------------------------------------------------------

    def add_processor(self, proc: iAsyncProcessor) -> None:
        """Add an async processor to the system."""
        self.async_system.add_processor(proc)

    def remove_processor(self, proc: iAsyncProcessor) -> None:
        """Remove an async processor from the system."""
        self.async_system.remove_processor(proc)

    async def execute(self, querier: iAsyncQuerier, step: int, *args, **kwargs) -> None:
        """
        Execute the system.
        """
        await self.async_system.execute(querier, step, *args, **kwargs)

    # ---------------------------------------------------------------------
    # iAsyncQuerier Facade methods
    # ---------------------------------------------------------------------

    async def get_archetype(self, sig: ArchetypeSignature, step: int) -> DataFrame:
        """Get an archetype by signature and step."""
        df = await self.querier.get_archetype(sig, step, self.world_id, self.run_id)
        return df

    async def get_archetype_for_entity(self, entity_id: int, *component_types: Type[Component]) -> DataFrame:
        """Get an archetype for an entity by id and component types."""
        return await self.querier.get_archetype_for_entity(entity_id, *component_types)

    # ---------------------------------------------------------------------
    # iAsyncUpdater Facade methods
    # ---------------------------------------------------------------------

    async def update(self, archetypes: List[Tuple[ArchetypeSignature, DataFrame]]) -> None:
        """Update the store with the given archetypes."""
        await self.updater.update(archetypes, self.current_step, self.world_id, self.run_id)

    async def materialize_spawns(self) -> None:
        """
        Write the entity spawn cache to the tables for simulation initialization.
        """
        await self.updater.materialize_spawns(self._spawn_cache, self.world_id, self.run_id)

        # Clear the cache for this signature
        self._spawn_cache.clear()
