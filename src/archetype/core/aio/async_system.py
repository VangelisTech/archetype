import asyncio
from typing import List, Dict, Type, Tuple
from daft import DataFrame

from ..base import BaseSystem, Component
from ..processor import Processor
from ..interfaces import iQuerier


class AsyncProcessor(Processor):
    """
    Async version of Processor with identical semantics.
    The only difference is that process() is now async to enable concurrent I/O.
    """
    
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        """Async version of process method. Override this in subclasses."""
        # Default: just call the sync version for backward compatibility
        return super().process(df, dt)


def async_processor(*component_types: Type[Component], priority: int = 0):
    """
    Decorator for AsyncProcessor classes.
    Identical to @processor decorator but for async processors.
    """
    def wrap(cls: Type[AsyncProcessor]):
        cls.components = component_types
        cls.priority = priority
        return cls
    return wrap


class AsyncSystem(BaseSystem):
    """
    Async version of SimpleSystem that processes archetypes concurrently.
    
    Key innovation: Each archetype is processed through all its relevant processors
    concurrently with other archetypes, while maintaining priority-based ordering
    within each archetype.
    
    This provides the same semantic guarantees as SimpleSystem but with
    per-archetype parallelism.
    """
    
    def __init__(self, max_concurrent_archetypes: int = 10):
        self.processors: List[AsyncProcessor] = []
        self.archetype_semaphore = asyncio.Semaphore(max_concurrent_archetypes)
        
    def add_processor(self, proc: AsyncProcessor):
        """Add an async processor to the system."""
        self.processors.append(proc)
        
    def remove_processor(self, proc_type: Type[AsyncProcessor]):
        """Remove all processors of the given type."""
        self.processors = [p for p in self.processors if not isinstance(p, proc_type)]
    
    async def execute(self, querier: iQuerier, step: int, dt: float) -> Dict[str, DataFrame]:
        """
        Async execution with concurrent archetype processing.
        
        Same semantics as SimpleSystem.execute() but with concurrency:
        - Each archetype processes through all relevant processors in priority order
        - Multiple archetypes can process concurrently 
        - Semaphore prevents resource exhaustion
        """
        if not self.processors:
            return {}
        
        # Get all active archetypes (same as sync version)
        active_archetypes = querier._store.get_active_archetypes_with_signatures()
        
        if not active_archetypes:
            return {}
        
        # Process each archetype concurrently
        tasks = []
        for archetype_hash, (df, archetype_signature) in active_archetypes.items():
            task = asyncio.create_task(
                self._process_single_archetype(archetype_hash, df, archetype_signature, step, dt)
            )
            tasks.append(task)
        
        # Wait for all archetypes to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect successful results
        modified_archetypes: Dict[str, DataFrame] = {}
        for result in results:
            if isinstance(result, Exception):
                # Log error but don't fail entire step
                print(f"Error processing archetype: {result}")
                continue
            
            archetype_hash, processed_df = result
            if processed_df is not None:
                modified_archetypes[archetype_hash] = processed_df
        
        return modified_archetypes
    
    async def _process_single_archetype(
        self, 
        archetype_hash: str,
        df: DataFrame, 
        archetype_signature: Tuple[Type[Component], ...],
        step: int,
        dt: float
    ) -> Tuple[str, DataFrame]:
        """
        Process a single archetype through all relevant processors.
        
        This is where the concurrency happens - each archetype gets its own task
        but within each archetype, processors run in priority order (same as sync).
        """
        async with self.archetype_semaphore:
            current_df = df
            archetype_modified = False
            
            # Apply all relevant processors in priority order (same logic as sync)
            for proc_instance in sorted(self.processors, key=lambda x: x.priority):
                if proc_instance.can_process_archetype(archetype_signature):
                    # Async process call - this is where I/O can happen concurrently
                    processed_df = await proc_instance.process(current_df, dt)
                    processed_df = proc_instance.postprocess(processed_df, step)
                    current_df = processed_df
                    archetype_modified = True
            
            # Only return archetype if it was actually modified
            if archetype_modified:
                return (archetype_hash, current_df)
            else:
                return (archetype_hash, None)


# Example async processor will be defined in test files