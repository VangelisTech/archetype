from typing import List, Tuple, Type

from daft import DataFrame

from archetype.core.base import Component
from archetype.core.interfaces import ArchetypeSignature
from archetype.core.aio.async_interfaces import iAsyncStore, iAsyncQuerier

class AsyncQueryManager(iAsyncQuerier):
    def __init__(self, store: iAsyncStore):
        self._store = store

    async def get_active_signatures(self) -> List[ArchetypeSignature]:
        """
        Get all active signatures.
        """
        return await self._store.get_active_signatures()
    
    async def get_archetype(self, sig: ArchetypeSignature, current_step: int) -> Tuple[ArchetypeSignature, DataFrame]:
        """
        Get all archetypes that contain all of the specified component types.
        """
        return await self._store.get_archetype(sig, current_step-1)
        
    async def get_archetype_for_entity(self, entity_id: int, *component_types: Type[Component]) -> DataFrame:
        """Get a component for an entity."""
        return await self._store.get_archetype_for_entity(entity_id, *component_types)
        