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
from typing import List, Type, Optional, Dict, Any, Tuple, Set, Union
import ulid
import asyncio
from uuid import UUID

import daft
from daft import DataFrame, lit, col

from archetype.core.interfaces import Component, Archetype, ArchetypeSignature
from archetype.core.base import BaseWorld
from archetype.core.aio.async_interfaces import iAsyncQueryManager, iAsyncUpdateManager, iAsyncSystem, iAsyncProcessor
from archetype.core.command import Command
from archetype.core.auth import reset_tick_counters 
from archetype.core.auth import ActorCtx

from logging import getLogger

logger = getLogger(__name__)

class AsyncWorld(BaseWorld):
    def __init__(self,
        querier: iAsyncQueryManager,
        updater: iAsyncUpdateManager,
        system: iAsyncSystem,
        world_id: str = None,
        run_id: str = None,
        debug: bool = False, 
        validate: bool = False,
        semaphore: Optional[asyncio.Semaphore] = None,
    ):
        """
        Initialize the fully parallel async world.
        """
        self.querier = querier
        self.updater = updater 
        self.system = system

        # World Properties
        self.world_id = world_id 
        self.run_id = run_id 
        self.debug = debug 
        self.validate = validate
        self.tick = 0

        # Semaphore for controlling concurrency
        self.semaphore = semaphore or asyncio.Semaphore(10)

        # Internal State
        self._entity_counter = count(start=1)
        self._entity2sig: Dict[int, ArchetypeSignature] = {}

        # In-memory caches that represent the "plan" for the current tick
        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {}
        self._despawn_cache: Dict[ArchetypeSignature, List[int]] = {}
        self._add_component_cache: Dict[Tuple[ArchetypeSignature, ArchetypeSignature], List[Tuple[int, List[Component]]]] = {}
        self._remove_component_cache: Dict[Tuple[ArchetypeSignature, ArchetypeSignature], List[Tuple[int, List[Type[Component]]]]] = {}        
        

    @property
    def loggerplate(self):
        return f"world={self.world_id} run={self.run_id} tick={self.tick} |"

    async def step(self, **kwargs):
        """
        Executes one full, parallel simulation tick.
        """
        start_time = time.time()

        all_sigs_this_tick = self._get_active_archetypes()
        
        tasks = [self._run_archetype(sig, **kwargs) for sig in sorted(all_sigs_this_tick)]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Finalize tick
        self._clear_caches()
        self.tick += 1
        logger.info(f"{self.loggerplate} tick {self.tick-1} completed in {time.time() - start_time:.4f}s")

        return results

    # ---------------------------------------------------------------------
    #  Step Planning
    # ---------------------------------------------------------------------

    def _get_active_archetypes(self) -> Set[ArchetypeSignature]:
        """Get the union of all archetypes that need processing this tick."""
        active_sigs = set(self._entity2sig.values())
        spawned_sigs = set(self._spawn_cache.keys())
        transmute_sigs = {old_sig for old_sig, _ in self._transmute_cache} | \
                         {new_sig for _, new_sig in self._transmute_cache}
        return active_sigs | spawned_sigs | transmute_sigs

    async def _run_archetype(self, sig: ArchetypeSignature, *args, **kwargs) -> Tuple[DataFrame, ArchetypeSignature]:
        """
        The core parallel task. It performs a pure, in-memory DataFrame
        transformation for a single archetype.
        """
        async with self.semaphore:
            # 1. Fetch previous state
            df = await self.get_archetype(sig, self.tick - 1)

            # 

            # 4. Execute Processors for this archetype via system 
            df = await self.execute(df, sig, *args, **kwargs)

            # 5. Update
            await self.updater.update(df, sig, self.tick, self.world_id, self.run_id)

            return df, sig
        
    def _clear_caches(self):
        self._spawn_cache.clear()
        self._despawn_cache.clear()
        self._add_component_cache.clear()
        self._remove_component_cache.clear()

    # ---------------------------------------------------------------------
    # World Mutation Commands
    # ---------------------------------------------------------------------

    async def create_entity(self, entity_id: int, components: List[Component]) -> int:
        sig = Archetype.sig_from_components(components)
        self._entity2sig[entity_id] = sig

        row_dict = Archetype.to_row_dict(entity_id, self.tick, components, self.world_id, self.run_id)
        self._spawn_cache.setdefault(sig, []).append(row_dict)
        return entity_id

    async def remove_entity(self, payload: Dict):
        entity_id = payload["entity_id"]
        sig = self._entity2sig.pop(entity_id, None)
        if sig:
            self._despawn_cache[sig].append(entity_id)
        else:
            logger.warn(f"{self.loggerplate} Entity Removal Failed: No entity: {entity_id}")

    async def add_processor(self, payload: Dict):
        await self.system.add_processor(payload["processor"])

    async def remove_processor(self, payload: Dict):
        await self.system.remove_processor(payload["processor"])

    # ---------------------------------------------------------------------
    # Updater, Querier, System Facade methods
    # ---------------------------------------------------------------------

   
    async def get_archetype(self, sig: ArchetypeSignature, tick: int) -> DataFrame:
        """Get an archetype by signature and tick."""
        return await self.querier.get_archetype(
            sig, 
            tick or self.tick, 
            self.world_id, 
            self.run_id
        )

    async def get_archetype_for_entity(self, entity_id: int, tick: Optional[int] = None) -> DataFrame:
        """Get an archetype for an entity by id and component types."""
        return await self.querier.get_archetype_for_entity(
            entity_id, 
            self._entity2sig[entity_id], 
            tick or self.tick, 
            self.world_id, 
            self.run_id
        )
    
    async def execute(self, df: DataFrame, sig: ArchetypeSignature, *args, **kwargs) -> DataFrame:
        """
        Execute the system.
        """
        return await self.system.execute(df, sig, self.semaphore, self.broker, *args, **kwargs)

    async def update(self, df: DataFrame, sig: ArchetypeSignature, tick: Optional[int] = None) -> None:
        """Update the store with the given archetypes."""
        await self.updater.update(df, sig, tick or self.tick, self.world_id, self.run_id)
