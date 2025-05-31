
from typing import List, Dict, Type
from itertools import count

from daft import DataFrame

from .base import BaseSystem
from .processor import Processor
from .interfaces import iQuerier

class SimpleSystem(BaseSystem):
    def __init__(self):
        self.processors: List[Processor] = []

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
    
    def execute(self, querier: iQuerier, step: int, dt: float) -> Dict[str, DataFrame]:
        """
        Executes all registered processors using archetype-first iteration.
        Each archetype is processed through all relevant processors in priority order.
        This eliminates redundant queries and enables better performance optimization.
        
        Returns a dictionary mapping archetype signature hashes to their final
        DataFrame state after all processors have run for this step. Only includes
        archetypes that were actually modified by at least one processor.

        Processors SHALL NOT return an empty dataframe. 
        """
        if not self.processors:
            return {}

        modified_archetypes: Dict[str, DataFrame] = {}
        
        # Get all active archetypes with their component signatures (single query)
        active_archetypes = querier._store.get_active_archetypes_with_signatures()
        
        # Process each archetype through all relevant processors
        for archetype_hash, (df, archetype_signature) in active_archetypes.items():
            current_df = df
            archetype_modified = False
            
            # Apply all relevant processors in priority order
            for proc_instance in sorted(self.processors, key=lambda x: x.priority):
                if proc_instance.can_process_archetype(archetype_signature):
                    processed_df = proc_instance.process(current_df, dt)
                    processed_df = proc_instance.postprocess(processed_df, step)
                    current_df = processed_df
                    archetype_modified = True
            
            # Only include archetypes that were actually processed by at least one processor
            if archetype_modified:
                modified_archetypes[archetype_hash] = current_df

        return modified_archetypes
