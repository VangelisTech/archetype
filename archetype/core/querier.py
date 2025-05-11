# Python
from daft import col, DataFrame
from typing import List, Type, Optional, Union

from .store import ArchetypeStore
from .base import Component
from .interfaces import iQuerier

class QueryManager(iQuerier):
    def __init__(self, store: ArchetypeStore):
        self._store = store

    def __call__(self,
        *component_types: Type[Component], 
        steps: Union[int, List[int]],
        entities: Optional[Union[int, List[int]]] = None 
        ) -> DataFrame:
        
        if isinstance(steps, int):
            steps = [steps]

        if isinstance(entities, int):
            entities = [entities]

        return self.query(*component_types, steps=steps, entities=entities)

    def query(self,
        *component_types: Type[Component], 
        steps: Union[int, List[int]],
        entities: Optional[Union[int, List[int]]] = None 
        ) -> DataFrame:

        archetypes = self._store.get_archetypes(*component_types)

        if entities is None:
            for sig, df in archetypes.items():
                df = df.where(col("step").is_in(steps)) 
                df = df.where(col("is_active"))
                archetypes[sig] = df
        else:
            for sig, df in archetypes.items():
                df = df.where(col("step").is_in(steps)) 
                df = df.where(col("entity_id").is_in(entities)) 
                df = df.where(col("is_active"))
                archetypes[sig] = df

        return archetypes
    
    

    


