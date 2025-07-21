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
from daft import DataFrame

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
        
        world_id: str = None,
        run_id: str = None,
        querier: iAsyncQueryManager = None,
        updater: iAsyncUpdateManager = None,
        system: iAsyncSystem = None,
        semaphore: Optional[asyncio.Semaphore] = None,
    ):
        """
        Initialize the fully parallel async world.
        """
        self.querier = querier
        self.updater = updater
        self.system = system

        # World Properties
        self.world_id = world_id or f"world_{str(ulid.ULID())}"
        self.run_id = run_id or f"run_{str(ulid.ULID())}"
        self.tick = 0

        # Semaphore for controlling concurrency
        self.semaphore = semaphore or asyncio.Semaphore(10)

        # Internal State
        self._entity_counter = count(start=1)
        self._entity2sig: Dict[int, ArchetypeSignature] = {}

        # In-memory caches that represent the "plan" for the current tick
        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {}
        self._despawn_cache: Dict[ArchetypeSignature, List[int]] = {}
        self._transmute_cache: Dict[Tuple[ArchetypeSignature, ArchetypeSignature], List[Dict[str, Any]]] = {}

    async def step(self, *args, **kwargs):
        """
        Executes one full, parallel simulation tick.
        """
        start_time = time.time()

        # PHASE 1: PLAN
        # Ingest commands and prepare the in-memory caches. This creates the
        # "plan" for the tick without performing any blocking I/O.
        await self.apply_commands()

        # PHASE 2: PARALLEL ACCUMULATION OF DATAFRAME TRANSFORMATIONS
        all_sigs_this_tick = self._get_all_involved_archetypes()
        tasks = [self._run_archetype_transformation(sig, *args, **kwargs) for sig in sorted(all_sigs_this_tick)]
        results: List[Tuple[DataFrame, ArchetypeSignature]] = await asyncio.gather(*tasks, return_exceptions=True)

        # PHASE 3: Send to updater for materialization, then passed to store for 
        await self.updater.update(results, self.tick, self.world_id, self.run_id)

        # Finalize tick
        self._clear_caches()
        self.tick += 1
        logger.info(f"Async tick {self.tick-1} completed in {time.time() - start_time:.4f}s")

    # ---------------------------------------------------------------------
    # Internal Methods for Step Planning
    # ---------------------------------------------------------------------

    async def apply_commands(self, cmds: List[Command]) -> List[UUID]:
        """
        Dequeues commands and populates the in-memory caches to create the plan for the tick.
        This includes pre-fetching data for transmutations.
        """       
        # Seperate State and Behavior mutations
        processed_cmd_ids = [c.id for c in cmds]
        data_cmds = [c for c in cmds if not c.op.endswith("_processor")]
        proc_cmds = [c for c in cmds if c.op.endswith("_processor")]

        # Apply data commands to populate spawn, despawn, and transmute caches
        for cmd in data_cmds:
            handler = getattr(self, f"_handle_{cmd.op}", None)
            if handler:
                await handler(cmd.payload)

        # Apply processor commands directly (hot-swapping for next tick)
        for cmd in proc_cmds:
            handler = getattr(self, f"_handle_{cmd.op}", None)
            if handler:
                await handler(cmd.payload)

        return processed_cmd_ids

    def _get_all_involved_archetypes(self) -> Set[ArchetypeSignature]:
        """Get the union of all archetypes that need processing this tick."""
        active_sigs = set(self._entity2sig.values())
        spawned_sigs = set(self._spawn_cache.keys())
        transmute_sigs = {old_sig for old_sig, _ in self._transmute_cache} | \
                         {new_sig for _, new_sig in self._transmute_cache}
        return active_sigs | spawned_sigs | transmute_sigs

    async def _run_archetype_transformation(self, sig: ArchetypeSignature, *args, **kwargs) -> Tuple[DataFrame, ArchetypeSignature]:
        """
        The core parallel task. It performs a pure, in-memory DataFrame
        transformation for a single archetype.
        """
        async with self.semaphore:
            # 1. Fetch previous state
            df = await self.get_archetype(sig, self.tick - 1)

            # 2. Apply logic via systems
            df = await self.execute(df, sig, *args, **kwargs)

            # 3. Apply despawns and transmute departures (removals)
            departures = self._despawn_cache.get(sig, [])
            for old_s, new_s in self._transmute_cache:
                if old_s == sig:
                    departures.extend([row['entity_id'] for row in self._transmute_cache[(old_s, new_s)]])
            if departures:
                df = df.where(~df["entity_id"].is_in(departures))

            # 4. Apply spawns and transmute arrivals (additions)
            arrivals = self._spawn_cache.get(sig, [])
            for old_s, new_s in self._transmute_cache:
                if new_s == sig:
                    arrivals.extend(self._transmute_cache[(old_s, new_s)])
            if arrivals:
                arrivals_df = daft.from_pylist(arrivals)
                df = df.concat(arrivals_df)

            return df, sig
        
    def _clear_caches(self):
        self._spawn_cache.clear()
        self._despawn_cache.clear()
        self._transmute_cache.clear()

    

    # ---------------------------------------------------------------------
    # Internal Command Handlers (Populate Caches)
    # ---------------------------------------------------------------------

    async def _handle_create_entity(self, payload: Dict) -> int:
        components = [Component.from_dict(c) for c in payload["components"]]
        entity_id = next(self._entity_counter)
        sig = Archetype.sig_from_components(components)
        self._entity2sig[entity_id] = sig

        row_dict = Archetype.to_row_dict(entity_id, self.tick, components, self.world_id, self.run_id)
        self._spawn_cache.setdefault(sig, []).append(row_dict)
        return entity_id

    async def _handle_remove_entity(self, payload: Dict):
        entity_id = payload["entity_id"]
        sig = self._entity2sig.pop(entity_id, None)
        if sig:
            self._despawn_cache.setdefault(sig, []).append(entity_id)

    async def _handle_add_component(self, payload: Dict):
        entity_id = payload["entity_id"]
        components = [Component.from_dict(c) for c in payload["components"]]
        old_sig = self._entity2sig.get(entity_id)
        if not old_sig: return

        new_sig = Archetype.add_components(old_sig, components)
        if new_sig == old_sig: return

        # Pre-fetch data for transmutation
        entity_df = await self.querier.get_archetype_for_entity(entity_id, old_sig, self.tick - 1)

        old_data = entity_df.to_pydict()
        new_data = {k: v[0] for k, v in old_data.items()} # Convert list to single value
        
        for comp in components:
            new_data.update(comp.to_row_dict()) # Add new component data

        self._transmute_cache.setdefault((old_sig, new_sig), []).append(new_data)
        self._entity2sig[entity_id] = new_sig

    async def _handle_remove_component(self, payload: Dict):

        # Retrieve Dependencies
        entity_id = payload["entity_id"]
        component_types_to_remove = [Component.get_type_by_name(name) for name in payload["component_types"]]
        old_sig = self._entity2sig.get(entity_id)
        if not old_sig: return

        new_sig = Archetype.remove_components(old_sig, component_types_to_remove)
        if new_sig == old_sig: return 

        # Pre-fetch data for transmutation
        entity_df = await self.querier.get_archetype_for_entity(entity_id, old_sig, self.tick - 1)
        if entity_df.count_rows() == 0: return

        old_data = entity_df.to_pydict()
        new_data = {k: v[0] for k, v in old_data.items()} # Convert list to single value

        # In a real implementation, we would remove the fields of the removed components
        # from new_data. For this example, we assume the new archetype's schema
        # will simply ignore the extra fields.

        self._transmute_cache.setdefault((old_sig, new_sig), []).append(new_data)
        self._entity2sig[entity_id] = new_sig


    async def _handle_add_processor(self, payload: Dict):
        await self.system.add_processor(payload["processor"])

    async def _handle_remove_processor(self, payload: Dict):
        await self.system.remove_processor(payload["processor"])

    # ---------------------------------------------------------------------
    # Updater, Querier, System Facade methods
    # ---------------------------------------------------------------------

    async def update(self, df: DataFrame, sig: ArchetypeSignature, tick: Optional[int] = None) -> None:
        """Update the store with the given archetypes."""
        await self.updater.update(df, sig, tick or self.tick, self.world_id, self.run_id)

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

