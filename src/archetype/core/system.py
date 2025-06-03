from typing import List, Dict, Type, Tuple
from itertools import count

from daft import DataFrame

from .base import BaseSystem
from .processor import Processor
from .interfaces import iQuerier
from .store import ArchetypeSignature

class SyncSystem(BaseSystem):
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
    
    def execute(self, querier: iQuerier, step: int, *args, **kwargs) -> List[Tuple[ArchetypeSignature, DataFrame]]:
        """
        Execute all processors on all active archetypes in priority order.
        Returns a list of tuples of (archetype_signature, modified_df)

        Embrace the immutability bro. 
        """
        if not self.processors:
            return []

        modified_archetypes: List[Tuple[ArchetypeSignature, DataFrame]] = []
        
        # Get all active archetypes with their component signatures (single query)
        active_archetypes: List[Tuple[ArchetypeSignature, DataFrame]] = querier.get_archetypes(step)
        
        # Process each archetype through all relevant processors in priority order
        for archetype_signature, df in active_archetypes:
            for proc_instance in sorted(self.processors, key=lambda x: x.priority):
                # Build the modified archetype list if the processor has the components to matching the archetype signature 
                if set(proc_instance.components).issubset(set(archetype_signature)):
                    processed_df = proc_instance.process(df, *args, **kwargs)
                    modified_archetypes.append((archetype_signature, processed_df))

        return modified_archetypes

