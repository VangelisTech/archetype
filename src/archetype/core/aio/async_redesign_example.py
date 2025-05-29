"""
Example of Async Architecture Redesign for Archetype Processing

This demonstrates how moving from dictionary-based to stream-based processing
fundamentally changes the system architecture and enables true concurrent processing.
"""

import asyncio
from typing import AsyncGenerator, Tuple, Type, List, Protocol
from daft import DataFrame, col
from abc import ABC, abstractmethod

# Example interfaces showing the architectural shift

class StreamProcessor(Protocol):
    """
    Processors in the async architecture work on individual archetype streams
    rather than dictionaries of all archetypes.
    """
    async def process_archetype_stream(
        self, 
        archetype_name: str, 
        df: DataFrame, 
        dt: float
    ) -> DataFrame:
        """Process a single archetype's data as it arrives."""
        ...

class AsyncStreamSystem:
    """
    System that processes archetype streams concurrently as they arrive.
    
    Key differences from synchronous system:
    1. No dictionary aggregation - processes streams individually
    2. True concurrent processing of different archetypes
    3. Each archetype gets its own processing pipeline
    4. Backpressure handling through semaphores
    """
    
    def __init__(self, max_concurrent_archetypes: int = 10):
        self.processors: List[StreamProcessor] = []
        self.archetype_semaphore = asyncio.Semaphore(max_concurrent_archetypes)
        
    async def execute_streaming(
        self, 
        store: 'AsyncArchetypeStore',
        component_types: Tuple[Type['Component'], ...],
        step: int,
        dt: float
    ) -> AsyncGenerator[Tuple[str, DataFrame], None]:
        """
        Execute processors on archetype streams as they arrive.
        
        This is fundamentally different from the sync version:
        - No Dict[str, DataFrame] accumulation
        - Each archetype is processed independently
        - Results are yielded as soon as they're ready
        """
        
        async def process_single_archetype(
            archetype_name: str, 
            df: DataFrame
        ) -> Tuple[str, DataFrame]:
            """Process a single archetype through all processors."""
            async with self.archetype_semaphore:
                current_df = df
                
                # Apply each processor in sequence to this archetype
                for processor in sorted(self.processors, key=lambda x: x.priority):
                    current_df = await processor.process_archetype_stream(
                        archetype_name, 
                        current_df, 
                        dt
                    )
                
                return (archetype_name, current_df)
        
        # Stream archetypes and process them concurrently
        async_tasks = []
        
        async for archetype_name, df in store.async_get_archetypes_stream(*component_types):
            # Create a task for each archetype as it arrives
            task = asyncio.create_task(
                process_single_archetype(archetype_name, df)
            )
            async_tasks.append(task)
            
            # Yield results as they complete (not in order)
            done, pending = await asyncio.wait(
                async_tasks, 
                return_when=asyncio.FIRST_COMPLETED
            )
            
            for task in done:
                result = await task
                yield result
                async_tasks.remove(task)
        
        # Process any remaining tasks
        for task in asyncio.as_completed(async_tasks):
            result = await task
            yield result


class AsyncBatchSystem:
    """
    Alternative async design that processes archetypes in configurable batches.
    
    This provides a middle ground between:
    - Full streaming (one at a time)
    - Full batching (all at once like sync)
    """
    
    def __init__(self, batch_size: int = 5):
        self.batch_size = batch_size
        self.processors: List[StreamProcessor] = []
    
    async def execute_batched(
        self,
        store: 'AsyncArchetypeStore',
        component_types: Tuple[Type['Component'], ...],
        step: int,
        dt: float
    ) -> AsyncGenerator[List[Tuple[str, DataFrame]], None]:
        """
        Process archetypes in batches for better throughput/latency balance.
        """
        batch = []
        
        async for archetype_name, df in store.async_get_archetypes_stream(*component_types):
            batch.append((archetype_name, df))
            
            if len(batch) >= self.batch_size:
                # Process batch concurrently
                tasks = [
                    self._process_archetype(name, df, dt)
                    for name, df in batch
                ]
                
                results = await asyncio.gather(*tasks)
                yield results
                
                batch = []
        
        # Process remaining items
        if batch:
            tasks = [
                self._process_archetype(name, df, dt)
                for name, df in batch
            ]
            results = await asyncio.gather(*tasks)
            yield results
    
    async def _process_archetype(
        self, 
        archetype_name: str, 
        df: DataFrame, 
        dt: float
    ) -> Tuple[str, DataFrame]:
        """Process single archetype through all processors."""
        current_df = df
        for processor in self.processors:
            current_df = await processor.process_archetype_stream(
                archetype_name, 
                current_df, 
                dt
            )
        return (archetype_name, current_df)


# Example of how QueryManager could be redesigned for streaming
class StreamingQueryManager:
    """
    Query manager that supports streaming results rather than batch dictionaries.
    """
    
    def __init__(self, store: 'AsyncArchetypeStore'):
        self._store = store
    
    async def query_stream(
        self,
        *component_types: Type['Component'],
        step: List[int]
    ) -> AsyncGenerator[Tuple[str, DataFrame], None]:
        """
        Stream query results as they become available.
        
        Key architectural change: No Dict accumulation, pure streaming.
        """
        async for archetype_name, df in self._store.async_get_archetypes_stream(*component_types):
            # Apply filters as data streams in
            filtered_df = df.where(col("step").is_in(step)) \
                           .where(df["is_active"])
            
            if not filtered_df.is_empty():  # Only yield non-empty results
                yield (archetype_name, filtered_df)


# Example usage showing the architectural difference
async def example_usage():
    """
    Demonstrates how the async streaming architecture fundamentally differs
    from the synchronous dictionary-based approach.
    """
    
    # Synchronous approach (current):
    # archetypes = store.get_archetypes(Component1, Component2)  # Dict[str, DataFrame]
    # for name, df in archetypes.items():
    #     processed = processor.process(df)
    #     results[name] = processed
    
    # Asynchronous streaming approach (proposed):
    store = AsyncArchetypeStore(uri="...", max_concurrent_requests=10)
    system = AsyncStreamSystem(max_concurrent_archetypes=5)
    
    # Process archetypes as they arrive, not waiting for all
    async for archetype_name, processed_df in system.execute_streaming(
        store, 
        (Component1, Component2), 
        step=1, 
        dt=0.1
    ):
        # Results arrive as soon as they're processed
        # No need to wait for all archetypes to be queried first
        print(f"Processed {archetype_name}")
        
        # Could even write results immediately
        await store.async_append(archetype_name, processed_df)


"""
Key Architectural Insights:

1. **Fundamental Query Model Change**: 
   - From: Dict[str, DataFrame] (batch)
   - To: AsyncGenerator[Tuple[str, DataFrame]] (stream)

2. **Concurrency Model**:
   - Each archetype query gets its own semaphore slot
   - Processors can work on archetypes as they arrive
   - No need to wait for all queries to complete

3. **Memory Efficiency**:
   - No need to hold all archetype DataFrames in memory
   - Process and release as you go

4. **Latency Improvements**:
   - First results available much sooner
   - Can start processing before all queries complete

5. **Backpressure Handling**:
   - Semaphores at multiple levels (query, processing)
   - Natural flow control through async generators

This represents a fundamental shift from batch to stream processing,
which is exactly what you identified in your question!
""" 