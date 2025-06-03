import asyncio
from typing import List, Dict, Type, Tuple
from daft import DataFrame

from ..base import BaseSystem
from ..processor import Processor as SyncProcessor
from .async_interfaces import iAsyncProcessor
from .async_processor import AsyncProcessor
from ..store import ArchetypeSignature
import logging

logger = logging.getLogger(__name__)


class AsyncSystem(BaseSystem):
    """
    Async version of SyncSystem that processes archetypes concurrently.
    
    Key innovation: Each archetype is processed through all its relevant processors
    concurrently with other archetypes, while maintaining priority-based ordering
    within each archetype.
    
    This provides the same semantic guarantees as SyncSystem but with
    per-archetype parallelism.
    """
    
    def __init__(self):
        self.processors: List[AsyncProcessor] = []
        
    def add_processor(self, proc: AsyncProcessor):
        """Add an async processor to the system."""
        self.processors.append(proc)
        
    def remove_processor(self, proc: AsyncProcessor):
        """Remove all processors of the given type."""
        self.processors = [p for p in self.processors if not isinstance(p, type(proc))]
    
    async def execute(
        self, 
        df: DataFrame,
        sig: ArchetypeSignature,
        *args, 
        **kwargs
    ) -> Tuple[ArchetypeSignature, DataFrame]:
        """
        Process a single archetype through all relevant processors.
        
        This is where the concurrency happens - each archetype gets its own task
        but within each archetype, processors run in priority order (same as sync).
        """
        if not self.processors:
            return (sig, df)

        for proc_instance in sorted(self.processors, key=lambda x: x.priority):
            if set(proc_instance.components).issubset(set(sig)):

                # Gracefully handle errors in processors. 
                # Dataframes are immutable so we are continuously returning an updated variant of the original.
                try: 
                    if isinstance(proc_instance, AsyncProcessor):
                        df = await proc_instance.process(df, asyncio.Semaphore(10), *args, **kwargs)
                    elif isinstance(proc_instance, SyncProcessor):
                        df = proc_instance.process(df, *args, **kwargs)
                except Exception as e:
                    logger.error(f"Error processing archetype {sig}: {e} with processor {proc_instance} of type {type(proc_instance)}")
                    df = None
                    break
        return (sig, df)
    
    # TODO: Eventually we will need to add an if statement on the type of processor 
    # IE Ray Remote actor. 
                
    
        
        
