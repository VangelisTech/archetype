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

import daft
from daft import DataFrame, lit

import pyarrow as pa

from archetype.core.interfaces import Component, Archetype, ArchetypeSignature
from archetype.core.base import BaseWorld
from archetype.core.aio.async_interfaces import iAsyncQuerier, iAsyncUpdater, iAsyncSystem, iAsyncProcessor, iBroker
from archetype.core.aio.command import Command 
from archetype.core.aio.broker import InMemoryBroker

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
        broker: iBroker = None, 
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
        self.tick = 0

        # Initialize internal caches
        self._entity2sig: Dict[int, ArchetypeSignature] = {}  # entity_id -> ArchetypeSignature
        self._entity_counter = count(start=1)
        
        # Spawn Cache
        self._broker = broker or InMemoryBroker()
        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {} 
        self._despawn_cache: Dict[ArchetypeSignature, List[int]] = {}  # sig -> list of entity ids to despawn
        
        # Set semaphore for concurrent archetype processing
        self.semaphore = semaphore or asyncio.Semaphore(10)

    async def step(self, *args, **kwargs):
        """
        Async version of step() with concurrent archetype processing.
        """
        # Materialize any pending spawns before the first tick
        start = time.time()
        await self.materialize_cmds()
        cmd_end = time.time()
        await self.process_archetypes(*args, **kwargs)
        end = time.time()
        logger.info(f"Async tick {self.tick} | cmds:  {cmd_end-start:.3f}s | processors:  {end-cmd_end:.3f}s | total: {end-start:.3f}s")

    
    async def process_archetypes(self, *args, **kwargs):
        async def process_with_semaphore(sig: ArchetypeSignature, *args, **kwargs) -> None:
            async with self.semaphore:
                cmd_updates_df: DataFrame = await self.materialize_cmds(sig)
                last_tick_df: DataFrame = await self.get_archetype(sig, self.tick-1)
                combined_df: DataFrame = cmd_updates_df.concat(last_tick_df)
                
                despawned_df: DataFrame = await self.materialize_despawns(combined_df, sig)

                df = 
                # Execute the system for this archetype
                processed_df = await self.execute(last_tick_df, sig, *args, **kwargs)
                await self.update(processed_df, sig, self.tick)

        tasks = [
            process_with_semaphore(sig, *args, **kwargs)
            for sig in set(self._entity2sig.values())
        ]

        await asyncio.gather(*tasks)


    
    async def materialize_cmds(self) -> None:
        """
        Materialize any pending commands like spawns, despawns, etc.
        """
        # Materialize Command Queues
        cmds = await self._cmd_queue.drain()
        async def materialize_cmd(cmd: Dict[str, Any]) -> None:
            # Handle each command type
            if cmd["op"] == "spawn_entities":
                return await self._spawn(*cmd["components"])
                
            elif cmd["op"] == "despawn_entities":
                return await self._despawn(cmd["entity_id"])

            elif cmd["op"] == "add_components":
                eid = cmd["entity_id"]
                components = cmd["component"]
                sig = self._entity2sig[eid]
                await self._add_components(eid, components, sig)

            elif cmd["op"] == "remove_component":
                eid = cmd["entity_id"]
                component_type = cmd["component_type"]
                sig = self._entity2sig[eid]
                await self._remove_component(eid, component_type, sig, self.tick, self.world_id, self.run_id)

            elif cmd["op"] == "add_processors":
                proc = cmd["processor"]
                self.add_processors(proc)

            elif cmd["op"] == "remove_processor":
                proc = cmd["processor"]
                self.remove_processors(proc)
                    

        tasks = [
            materialize_cmd(self, cmd)
            for cmd in await self.context.drain()
        ]

        await asyncio.gather(*tasks)

    async def materialize_spawns(self) -> None:
        """
        Write the entity spawn cache to the tables for simulation initialization.
        """
        # Handle spawn cache
        
        for sig, rows in self._spawn_cache.items():
            # Coerce List of PyDicts to PyArrow table
            pyarrow_schema: pa.Schema = Archetype.get_archetype_schema(sig)
            empty_table = pyarrow_schema.empty_table()
            arrow_table = empty_table.from_pylist(rows)
            df = daft.from_arrow(arrow_table)

            self._spawn_cache.clear()
        
    async def materialize_despawns(self, df: DataFrame, sig: ArchetypeSignature) -> None:    
        
        entities = self._despawn_cache.get(sig, [])
        if len(entities) > 0: 
            df = df.with_column("is_active", df["entity_id"].is_in(entities).if_else(lit(False), lit(True)))
        
        self._despawn_cache.clear()

    # ---------------------------------------------------------------------
    # iAsyncEcsCommand Facade methods
    # ---------------------------------------------------------------------

    async def create_entity(self, *components: Component) -> int:
        assert len(components) != 0, "Cannot create an entity with no components"
        await self._broker.enqueue(Command(
            op="spawn_entity",
            payload={"components": [c.to_dict() for c in components]}
        ))
    
    async def delete_entity(self, entity_id: int) -> None:
        if isinstance(entity_ids, int):
            entity_ids = [entity_ids]
        
        await self._broker.enqueue(Command(
            op="delete_entity",
            payload={"entity_id": [entity_ids] if isinstance(entity_ids, int) else entity_ids}
        ))

    async def add_component(self, entity_ids: List[int], *components: Component) -> None:
        """Queue a component to be added to an entity at the end of the tick."""
        await self.cmd_queue.add_component(entity_ids, *components)

    async def remove_component(self, entity_id: int, component) -> None:
        """Queue a component to be removed from an entity at the end of the tick."""
        await self.cmd_queue.remove_component([entity_id], component)

    async def _create_entity(self, *components: Component, entity_id= Optional[int] = None) -> int:
        """Create a new entity with these components."""
        assert len(components) != 0, "Cannot create an entity with no components"

        eid = entity_id or next(self._entity_counter)
        archetype = Archetype(components)  # Just using the Archetype as a convenience class here
        self._entity2sig[eid] = archetype.sig

        # Create the row dict
        row_dict = archetype.to_row_dict(
            entity_id=eid,
            tick=self.tick,
            components=components,
            world_id=self.world_id,
            run_id=self.run_id
        )

        # Add row to the spawn cache
        if archetype.sig not in self._spawn_cache:
            self._spawn_cache[archetype.sig] = []
        self._spawn_cache[archetype.sig].append(row_dict)

        return eid
    
    async def _delete_entity(self, entity_id: int, tick: Optional[int] = None) -> None:
        """Mark an entity dead (is_active=False)."""
        sig = self._entity2sig[entity_id]
        self._entity2sig.pop(entity_id, None)
        self._despawn_cache[sig].append(entity_id)

    async def add_components(self, entity_id: int, components: List[Component], sig: ArchetypeSignature) -> None:
        """Queue a component to be added to an entity at the end of the tick."""
        # For adding components to an entity we need to transition 
        # the entity of one archetype signature to another
        self._create_entity(components,
        self.despawn(entity_id) 

        
    # ---------------------------------------------------------------------
    # iAsyncUpdater Facade methods
    # ---------------------------------------------------------------------

    async def update(self, df: DataFrame, sig: ArchetypeSignature, tick: int) -> None:
        """Update the store with the given archetypes."""
        await self.updater.update(df, sig, tick or self.tick, self.world_id, self.run_id)



    
    # ---------------------------------------------------------------------
    # iAsyncQuerier Facade methods
    # ---------------------------------------------------------------------

    async def get_archetype(self, sig: ArchetypeSignature, tick: int) -> DataFrame:
        """Get an archetype by signature and tick."""
        return await self.querier.get_archetype(
            sig, 
            tick or self.tick, 
            self.world_id, 
            self.run_id
        )

    async def get_archetype_for_entity(self, entity_id: int, tick: int) -> DataFrame:
        """Get an archetype for an entity by id and component types."""
        return await self.querier.get_archetype_for_entity(
            entity_id, 
            self._entity2sig[entity_id], 
            tick or self.tick, 
            self.world_id, 
            self.run_id
        )

    async def get_component(self, component_type: Type[Component], tick: int) -> DataFrame:
        """Get a component by type."""
        matching_sigs = 
        return await self.querier.get_component(component_type, sig, tick or self.tick, self.world_id, self.run_id)

    async def get_component_for_entity(self, component_type: Type[Component], tick: int) -> DataFrame:
        """
        Get all entites for a component type.
        """
        return await self.querier.get_component_for_entity(component_type, sig, tick or self.tick, self.world_id, self.run_id)

    # ---------------------------------------------------------------------
    # iAsyncSystem Facade methods
    # ---------------------------------------------------------------------

    def _add_processors(self, *processors: iAsyncProcessor) -> None:
        """Add an async processor to the system."""
        self.async_system.add_processor(processors)

    def _remove_processors(self, *processors: iAsyncProcessor) -> None:
        """Remove an async processor from the system."""
        self.async_system.remove_processor(processors)

    async def execute(self, df: DataFrame, sig: ArchetypeSignature, *args, **kwargs) -> DataFrame:
        """
        Execute the system.
        """
        return await self.async_system.execute(df, sig, self.semaphore, self.cmd_queue *args, **kwargs)
