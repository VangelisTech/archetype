import asyncio
import time
from typing import List, Type, Optional, Dict, Any, Tuple, Set
import ulid
from itertools import count
from functools import lru_cache

import daft
from daft import DataFrame

from archetype.core.interfaces import ArchetypeSignature
from ..base import Component
from .async_interfaces import iAsyncStore, iAsyncWorld, iAsyncQuerier, iAsyncUpdater, iAsyncSystem, iAsyncProcessor
from archetype.core.store import get_component_prefix, sig_from_components
from logging import getLogger

logger = getLogger(__name__)

class AsyncWorld(iAsyncWorld):
    def __init__(self,
        store: iAsyncStore,
        querier: iAsyncQuerier,
        updater: iAsyncUpdater,
        system: iAsyncSystem,
        world_id: str = None,
        run_id: str = None,
        max_concurrent_archetypes: int = 10,
        debug: bool = False,
    ):
        """
        Initialize async world by wrapping an existing sync world.
        This lets us reuse all the existing infrastructure.
        """

        # Reuse sync world's infrastructure
        self.store = store
        self.querier = querier
        self.updater = updater
        self.async_system = system

        # Set World Properties
        self.world_id = world_id or f"world_{str(ulid.ULID())}"
        self.run_id = run_id or f"run_{str(ulid.ULID())}"
        self.debug = debug
        self.semaphore = asyncio.Semaphore(max_concurrent_archetypes)
        self.current_step = 0


        # Initialize internal properties
        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {}


    async def step(self, *args, **kwargs):
        """
        Async version of step() with concurrent archetype processing.
        """
        # Materialize any pending spawns before the first step
        await self.materialize_spawns()
            
        start = time.time()

        # Get all active archetypes with their component signatures (single query)
        active_signatures = self.get_active_signatures()
        
        async def process_with_semaphore(sig: ArchetypeSignature, *args, **kwargs) -> None:
            async with self.semaphore:
                sig_with_df = await self.get_archetype(sig, self.current_step)
                processed_sig, processed_df = await self.async_system.execute(sig_with_df[1], sig_with_df[0], *args, **kwargs)
                await self.updater.update([(processed_sig, processed_df)], self.current_step + 1, self.world_id, self.run_id)
            
        tasks = [
            process_with_semaphore(sig, *args, **kwargs)
            for sig in active_signatures
        ]

        await asyncio.gather(*tasks)
        
        end = time.time()
        print(f"Async Step {self.current_step} done in {end-start:.3f}s")

        self.current_step += 1

    def get_active_signatures(self) -> Set[ArchetypeSignature]:
        """
        Get all active signatures.
        """
        return self.store.get_active_signatures()
    
    async def _new_archetype_row(self, entity_id: int, step: int, components: List[Component], world_id: str, run_id: str) -> Dict[str, Any]:
        """
        Convert the single entity dictionary to a columnar dict for PyArrow
        Ensure the order of keys matches the schema for from_pydict if schema wasn't passed
        BUT since we pass the schema explicitly, the order in columnar_data doesn't strictly matter,
        although maintaining it is good practice.
        We need values to be lists.
        """
        # Create the base archetype from archetype arrow schema
        #df = daft.from_arrow(schema.empty_table())
        row_dict = {
            "world_id": world_id,
            "run_id": run_id,
            "entity_id": entity_id,
            "step": step,
            "is_active": True
        }

        for c in components:
            prefix = get_component_prefix(type(c))
            row_dict.update({prefix + key: value for key, value in c.model_dump().items()})


        return row_dict

    def spawn(self, *components: Component, step: Optional[int] = None) -> int:
        """Create a new entity with these components."""
        assert len(components) != 0, "Cannot create an entity with no components"
        
        # Use store to add entity and get ID
        entity_id = self.store.add_entity(list(components), step or self.current_step, self.world_id, self.run_id)
        sig = sig_from_components(components)
        
        # Create row dict for spawn cache (this needs to be sync for now)
        row_dict = {
            "world_id": self.world_id,
            "run_id": self.run_id,
            "entity_id": entity_id,
            "step": step or self.current_step,
            "is_active": True
        }

        for c in components:
            prefix = get_component_prefix(type(c))
            row_dict.update({prefix + key: value for key, value in c.model_dump().items()})
        
        # Add row to the spawn cache
        if sig not in self._spawn_cache:
            self._spawn_cache[sig] = []
        self._spawn_cache[sig].append(row_dict)
        
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

    async def get_archetype(self, sig: ArchetypeSignature, step: Optional[int] = None) -> Tuple[ArchetypeSignature, DataFrame]:
        """Get an archetype by signature and step."""
        return await self.querier.get_archetype(sig, step or self.current_step, self.world_id, self.run_id)
    
    async def get_archetype_for_entity(self, entity_id: int, *component_types: Type[Component]) -> DataFrame:
        """Get an archetype for an entity by id and component types."""
        return await self.querier.get_archetype_for_entity(entity_id, *component_types)
    
    # ---------------------------------------------------------------------
    # iAsyncUpdater Facade methods
    # ---------------------------------------------------------------------

    async def update(self, archetypes: List[Tuple[ArchetypeSignature, DataFrame]]) -> None:
        """Update the store with the given archetypes."""
        await self.updater.update(archetypes)

    async def materialize_spawns(self) -> None:
        """
        Write the entity spawn cache to the tables for simulation initialization. 
        """
        await self.updater.materialize_spawns(self._spawn_cache, self.world_id, self.run_id)
        
        # Clear the cache for this signature
        self._spawn_cache.clear()