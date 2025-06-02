# Python
from daft import col, DataFrame
from typing import List, Type, Tuple, Sequence

from .base import Component
from .interfaces import iQuerier, iStore
from .store import ArchetypeSignature

class QueryManager(iQuerier):
    def __init__(self, store: iStore):
        self._store = store

    def _filter_on_step_and_active(self,
        archetypes: List[Tuple[ArchetypeSignature, DataFrame]],
          step: int
        ) -> List[Tuple[ArchetypeSignature, DataFrame]]:

        return [
            (
                sig,
                df.where(col("step").is_in([step])).where(df["is_active"])
            )
            for sig, df in archetypes
        ]

    def get_archetypes(self, step: int) -> List[Tuple[ArchetypeSignature, DataFrame]]:
        """
        Get all archetypes that contain all of the specified component types.
        """
        active_archetypes =  self._store.get_archetypes()
        if step == -1:
            return active_archetypes
        
        return self._filter_on_step_and_active(active_archetypes, step-1)
        
    def archetype_for_entity(self,
        entity_id: int,
        component_types: Sequence[Type[Component]],
        step: int
        ) -> DataFrame:
        """Get a component for an entity."""
        df = self._store.get_archetype_for_entity(entity_id, *component_types)
        if step == -1:
            return df
        return self._filter_on_step_and_active(df, step)