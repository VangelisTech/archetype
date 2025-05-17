from daft import DataFrame
from typing import List, Union, Dict, Type
from .base import BaseSystem
from .processor import Processor
from .interfaces import iQuerier

class SimpleSystem(BaseSystem):
    def __init__(self, querier: iQuerier):
        self.processors: List[Processor] = []
        self.querier = querier

    def add_processor(self, proc: Processor):
        self.processors.append(proc)

    def remove_processor(self, proc: Type[Processor]):
        processor_to_remove = None
        for p_instance in self.processors:
            if isinstance(p_instance, proc):
                processor_to_remove = p_instance
                break
        if processor_to_remove:
            self.processors.remove(processor_to_remove)
        else:
            pass
    
    async def execute(self, step: Union[int, List[int]], dt: float) -> Dict[str, DataFrame]:
        """
        Executes all registered processors sequentially by priority.
        Modifications by a processor to an archetype are visible to subsequent processors
        within the same execution step if they query the same archetype.
        Returns a dictionary mapping archetype signature hashes to their final
        DataFrame state after all processors have run for this step. Only includes
        archetypes that were actually modified by at least one processor.

        Daft Dataframes are immutable, so we are essentially aggregating all the changes into a single dataframe.

        Processors SHALL NOT return an empty dataframe. 
        """

        modified_archetypes: Dict[str, DataFrame] = {}

        for proc_instance in sorted(self.processors, key=lambda x: x.priority):
            queried_archetypes = await proc_instance.preprocess(self.querier, step)

            for archetype_hash, queried_df in queried_archetypes.items():
                df_to_process = modified_archetypes.get(archetype_hash, queried_df)
                
                transformed_df = proc_instance.process(df_to_process, dt)

                if transformed_df is None:
                    raise ValueError(f"Processor {type(proc_instance).__name__} returned None. It should return the input DataFrame if no changes were intended.")
                
                modified_archetypes[archetype_hash] = transformed_df
            

                    

        return modified_archetypes

