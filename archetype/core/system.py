from daft import DataFrame
from typing import List, Union, Dict, TYPE_CHECKING
from .base import BaseSystem
from .processor import Processor

# Added for World type hint to resolve linter error
if TYPE_CHECKING:
    from .world import World

class System(BaseSystem):
    def __init__(self):
        self.processors: List[Processor] = []

    def add_processor(self, proc: Processor):
        self.processors.append(proc)

    def remove_processor(self, proc: Processor):
        self.processors.remove(proc)

    def execute(self, world: 'World', step: Union[int, List[int]]) -> Dict[str, DataFrame]:
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

        for proc in sorted(self.processors, key=lambda x: x.priority):
            queried_archetypes = proc.query(world, step)

            for hash, queried_df in queried_archetypes.items():
                df = modified_archetypes.get(hash, queried_df)
                transformed_df = proc.process(df)

                if transformed_df is None:
                    raise ValueError("Processor returned an empty dataframe. Please return the input dataframe if processor.process() logically chooses to make no changes.")
                
                modified_archetypes[hash] = transformed_df
            
            
                    

        return modified_archetypes

