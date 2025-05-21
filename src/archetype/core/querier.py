# Python
from daft import col, DataFrame
from typing import List, Type, Dict, Sequence

from .base import Component
from .interfaces import iQuerier, iStore

class QueryManager(iQuerier):
    def __init__(self, store: iStore):
        self._store = store

    def __call__(self,
        component_types: Sequence[Type[Component]],
        step: int,
        ) -> Dict[str, DataFrame]:

        return self.query(*component_types, step=[step])

    def query(self,
        *component_types: Type[Component], 
        step: List[int],
        ) -> Dict[str, DataFrame]:

        archetypes =  self._store.get_archetypes(*component_types)

        archetypes = {
            name: df.where(col("step").is_in(step)) \
                    .where(df["is_active"]) \
                    
            for name, df in archetypes.items()
        }

        return archetypes

    


