from typing import Dict, List, Union, Optional 
import uuid_utils as uuid
from uuid_utils import UUID, uuid7

import daft
from daft import col, DataFrame, Schema
from daft.expressions import lit
from daft.session import Session
from daft.catalog import Catalog, Table
from daft.io import IOConfig
from pyiceberg.catalog.sql import SqlCatalog

from archetype.core import SyncStore, SyncSystem, SyncWorld, QueryManager, UpdateManager
from archetype.core.interfaces import Component, ArchetypeSignature, Archetype, iSystem, iQueryManager, iUpdateManager, iWorld, iStore
from archetype.core.base import BaseWorld, BaseProcessor
from archetype.core.aio.async_interfaces import iAsyncWorld, iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager, iAsyncSystem, iAsyncCommandBroker
from archetype.core.aio import AsyncStore, AsyncQueryManager, AsyncUpdateManager, AsyncSystem, AsyncWorld
from archetype.core.command import Command





class WorldOrchestrator:
    """
    Orchestrator for the Archetype application, managing the lifecycle of components and services.
    This class is responsible for initializing, starting, and stopping the application components.
    
    Created AsyncWorlds are managed here, while synchronous worlds are generated and managed by the user for dubugging and learning purposes. 
    It also handles the registration of services and components, ensuring that they are properly initialized and can interact with each other.
    The orchestrator acts as a central hub for the application, coordinating the various parts of the application and ensuring that they work together seamlessly.
    
    """

    def __init__(self,
        uri: str,
        namespace: str,
        is_async: bool,
        debug: bool, 
        dry_run: bool,
        catalog: Catalog, 
        io_config: IOConfig, 
        querier: Union[iQueryManager, iAsyncQueryManager],
        updater: Union[iUpdateManager, iAsyncUpdateManager],
    ):        
        # Initialize Singletons
        self.querier = querier
        self.updater = updater

        # Init world dict
        self._worlds: Dict[str, iAsyncWorld] = {}

    def spawn_world(self, world_id: UUID, run_id: UUID = None, is_async: bool = True) -> BaseWorld:
        """Returns a new World instance. AsyncWorlds are added to internal world dictionary, while SyncWorlds are returned unmanaged."""
        if is_async: 
            world = AsyncWorld(
                world_id =  world_id or uuid7(),
                run_id = ,
                querier = self.querier,
                updater = self.updater,
                system = AsyncSystem()
            )

            # Add async world to worlds dict
            self._worlds[world_id] = world
        else: 
            world = SyncWorld(
                world_id = world_id or f"world_{str(ulid.ULID())}",
                run_id = run_id or f"run_{str(ulid.ULID())}",
                querier = self.querier,
                updater = self.updater,
                system = SyncSystem()
            )
            # Dont add sync world to worlds dict, as it is super slow and is more meant for debugging and learning purposes

        return world

    def remove_world(self, world_id: str) -> None:
        w: AsyncWorld = self._worlds.pop(world_id)
        w._clear_caches()
        del w

    def list_worlds(self) -> List[str]:
        return self._worlds.keys()

    async def run_worlds(self,  steps: int, world_ids: Optional[List[str]] = None):
        if not world_ids:
            world_ids = self._worlds.keys()
        
        for self._worlds[world_ids]:

        for step in steps:
            await self._worlds[world_ids].step()

    # ---------------------------------------------------------------------
    # Command Handling
    # ---------------------------------------------------------------------

    async def apply_commands(self, world_id: str, cmds: List[Command]) -> List[UUID]:
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
                await handler(world_id, cmd.payload)


        # Apply processor commands directly (hot-swapping for next tick)
        for cmd in proc_cmds:
            handler = getattr(self, f"_handle_{cmd.op}", None)
            if handler:
                await handler(world_id, cmd.payload)

        return processed_cmd_ids
    
    async def _handle_create_entity(self, world_id: str, payload: Dict) -> int:
        "Handle Entity Creation Delegation to designated world"
        components = [Component.from_dict(c) for c in payload["components"]]
        return await self._worlds[world_id].create_entity(components)

    async def _handle_remove_entity(self, world_id: str, payload: Dict):
        return await self._worlds[world_id].remove_entity(payload["entity_id"])

    async def _handle_add_component(self, world_id: str, payload: Dict):
        entity_id = payload["entity_id"]
        components = [Component.from_dict(c) for c in payload["components"]]

        return await self._worlds[world_id].add_component(entity_id,components)

    async def _handle_remove_component(self, world_id: str, payload: Dict):
        entity_id = payload["entity_id"]
        component_types_to_remove = [Component.get_type_by_name(name) for name in payload["component_types"]]

        return await self._worlds[world_id].remove_component(entity_id, component_types_to_remove)

    async def _handle_add_processor(self, world_id: str, payload: Dict):
        await self._worlds[world_id].add_processor(payload["processor"])

    async def _handle_remove_processor(self, world_id: str, payload: Dict):
        await self._worlds[world_id].remove_processor(payload["processor"])



    