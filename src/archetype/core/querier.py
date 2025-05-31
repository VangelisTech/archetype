# Python
from daft import col, DataFrame
from typing import List, Type, Dict, Tuple, Sequence

from .base import Component
from .interfaces import iQuerier, iStore

class QueryManager(iQuerier):
    def __init__(self, store: iStore):
        self._store = store

    def get_matching_archetypes(self, component_types: Tuple[Type[Component], ...], step: int) -> Dict[str, DataFrame]:
        """
        Get all archetypes that contain all of the specified component types.
        """
        archetypes =  self._store.get_matching_archetypes(component_types)
        archetypes = {
            name: df.where(col("step").is_in([step])) \
                    .where(df["is_active"]) \
                    
            for name, df in archetypes.items()
        }

        return archetypes
    
    # An Async version of the above would use a semphore over a list of events mapping each archetype to the processor and back through the updater. 

    def archetype_for_entity(self,
        entity_id: int,
        component_types: Sequence[Type[Component]],
        step: int
        ) -> DataFrame:
        """Get a component for an entity."""
        df = self._store.get_archetype_for_entity(entity_id, *component_types)

        df = df.where(col("step").is_in([step]))

        return df