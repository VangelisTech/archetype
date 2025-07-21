from typing import Dict, List
from uuid import uu
from ulid import ULID

import daft
from daft import col, DataFrame, Schema
from daft.expressions import lit
from daft.session import Session
from daft.catalog import Catalog, Table
from daft.io import IOConfig
from pyiceberg.catalog.sql import SqlCatalog

from archetype.core import SyncStore, SyncSystem, SyncWorld, QueryManager, UpdateManager
from archetype.core.interfaces import Component, ArchetypeSignature, Archetype
from archetype.core.base import BaseWorld, BaseProcessor
from archetype.core.aio.async_interfaces import iAsyncWorld, iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager, iAsyncSystem, iAsyncCommandBroker
from archetype.core.aio import AsyncStore, AsyncQueryManager, AsyncUpdateManager, AsyncSystem, AsyncWorld






class WorldOrchestrator:
    """
    Orchestrator for the Archetype application, managing the lifecycle of components and services.
    This class is responsible for initializing, starting, and stopping the application components.
    
    Created AsyncWorlds are managed here, while synchronous worlds are generated and managed by the user for dubugging and learning purposes. 
    It also handles the registration of services and components, ensuring that they are properly initialized and can interact with each other.
    The orchestrator acts as a central hub for the application, coordinating the various parts of the application and ensuring that they work together seamlessly.
    
    """

    def __init__(self,
        uri: str = None,
        namespace: str = None,
        catalog: Catalog = None, 
        io_config: IOConfig = None, 
        debug: bool = False
    ):
        self.uri = uri or ".archetype_data"
        self.namespace = namespace or "archetypes"
        self.catalog = catalog or Catalog.from_iceberg(
            SqlCatalog(
                "default",
                **{
                    "uri": f"sqlite:///{self.uri}/catalog.db",
                    "warehouse": f"file://{self.uri}",
                },
            )
        )
        self.io_config = io_config or IOConfig()
        self.debug = debug
        
        # Initialize Singletons
        self.async_store:   iAsyncStore = AsyncStore(uri=uri, namespace=namespace, catalog=catalog, io_config=io_config,debug=debug)
        self.async_querier: iAsyncQueryManager = AsyncQueryManager(self.store)
        self.async_updater: iAsyncUpdateManager = AsyncUpdateManager(self.store)

        # Init world dict
        self._worlds: Dict[str, iAsyncWorld] = {}

    def spawn_world(self, world_id: str = None, run_id: str = None, is_async: bool = True) -> BaseWorld:
        """Returns SyncWorld instances or adds AsyncWorld instances to internal map"""
        if is_async: 
            world = AsyncWorld(
                world_id =  world_id or f"world_{str(ULID())}",
                run_id = run_id or f"run_{str(ULID())}",
                querier = self.async_querier,
                updater = self.async_updater,
                system = system or AsyncSystem() 

            )

            # Add async world to worlds dict
            self._worlds[world_id] = world
        else: 
            sync_store = SyncStore(
                uri = self.uri,

            world = SyncWorld(
                world_id = world_id or f"world_{str(ulid.ULID())}",
                run_id = run_id or f"run_{str(ulid.ULID())}",
                querier = QueryManager(sync_store),
                updater = UpdateManager(sync_store),
                system = AsyncSystem()
            )
            # Dont add sync world to worlds dict, as it is super slow and is more meant for debugging and learning purposes

        return world

    def remove_world(self, world_id: str) -> None:
        w: AsyncWorld = self._worlds.pop(world_id)
        w._clear_caches()
        del w

    def list_worlds(self) -> List[str]:
        return self._worlds.keys()

    async def step_world(self, world_id: str):
        return await self._worlds[world_id].step()



    