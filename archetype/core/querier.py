# Python
from daft import col, DataFrame
from typing import List, Type, Optional, Union, Dict, Sequence

from .base import Component
from .interfaces import iQuerier, iStore

class QueryManager(iQuerier):
    def __init__(self, store: iStore):
        self._store = store

    async def __call__(self,
        component_types: Sequence[Type[Component]],
        step: Union[int, List[int]],
        ) -> Dict[str, DataFrame]:
        
        if isinstance(step, int):
            current_step_list = [step]
        else:
            current_step_list = step

        return await self.query(*component_types, step=current_step_list)

    async def query(self,
        *component_types: Type[Component], 
        step: List[int],
        ) -> Dict[str, DataFrame]:

        archetypes = await self._store.get_archetypes(*component_types)

        for sig_hash, df in archetypes.items():
            filtered_df = df.where(col("step").is_in(step)) \
                            .where(col("is_active"))
            archetypes[sig_hash] = filtered_df

        return archetypes
    
    def get_history(self, *component_types: Type[Component], include_all_runs: bool = False) -> DataFrame:
        """Fetch the history of these components at the given step."""
        archetypes = self._store.get_history(*component_types, include_all_runs=include_all_runs)
        
        return archetypes

    


