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

from archetype.core.interfaces import Component, Archetype, ArchetypeSignature
from archetype.core.base import BaseWorld
from archetype.core.aio.async_interfaces import iAsyncQuerier, iAsyncUpdater, iAsyncSystem, iAsyncProcessor

from logging import getLogger

logger = getLogger(__name__)

class AsyncWorld(BaseWorld):
    def __init__(self,
        querier: iAsyncQuerier,
        updater: iAsyncUpdater,
        async_system: iAsyncSystem,
        world_id: str = None,
        run_id: str = None,
        debug: bool = False,
        semaphore: Optional[asyncio.Semaphore] = None,
    ):
        """
        Initialize async world using shared base functionality.
        """
        self.querier = querier
        self.updater = updater
        self.async_system = async_system

        # Set World Properties
        self.world_id = world_id or f"world_{str(ulid.ULID())}"
        self.run_id = run_id or f"run_{str(ulid.ULID())}"
        self.current_step = 0

        # Initialize internal caches
        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {}  # ArchetypeSignature -> List[row_dict]
        self._entity2sig: Dict[int, ArchetypeSignature] = {}  # entity_id -> ArchetypeSignature
        self._entity_counter = count(start=1)

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
        logger.info(f"Async Step {self.current_step} done in {end-start:.3f}s")
        self.current_step += 1

    def get_active_signatures(self) -> Set[Any]:
        """Get all active archetype signatures. Must be implemented by subclasses."""
        raise set(self._entity2sig.values())

    def _new_archetype_row(self,
        entity_id: int,
        step: int,
        components: List[Component],
        world_id: str,
        run_id: str) -> Dict[str, Any]:
        """
        Convert entity components to a row dictionary for archetype storage.

        Args:
            entity_id: The unique entity identifier
            step: The simulation step
            components: List of component instances
            world_id: The world identifier
            run_id: The run identifier

        Returns:
            Dict containing the row data for this entity
        """
        row_dict = {
            "world_id": world_id,
            "run_id": run_id,
            "entity_id": entity_id,
            "step": step,
            "is_active": True
        }

        for c in components:
            prefix = c.get_prefix()
            row_dict.update({prefix + key: value for key, value in c.model_dump().items()})

        return row_dict

    def spawn(self, *components: Component, step: Optional[int] = None) -> int:
        """Create a new entity with these components."""
        assert len(components) != 0, "Cannot create an entity with no components"

        # Get the entity id and signature
        entity_id = next(self._entity_counter)
        sig = Archetype.sig_from_components(components) # Just using the Archetype as a convenience class here
        self._entity2sig[entity_id] = sig

        # Create the row dict
        row_dict = self._new_archetype_row(entity_id, step, components, self.world_id, self.run_id)

        # Add row to the spawn cache
        if sig not in self._spawn_cache:
            self._spawn_cache[sig] = []
        self._spawn_cache[sig].append(row_dict)

        return entity_id

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
        if self._spawn_cache:
            await self.updater.materialize_spawns(self._spawn_cache, self.world_id, self.run_id)
            self._spawn_cache.clear()

    async def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        """Mark an entity dead (is_active=False)."""
        sig = self._entity2sig[entity_id]
        await self.updater.remove_entity(entity_id, sig, step or self.current_step, self.world_id, self.run_id)

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
