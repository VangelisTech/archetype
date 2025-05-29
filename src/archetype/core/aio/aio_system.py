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
    

"""
Episode System Concept for Async Streaming Architecture

The Episode System manages temporal coordination across concurrent archetype streams,
replacing the traditional step-based synchronization with a more flexible episode model.
"""

import asyncio
from typing import AsyncGenerator, Dict, Set, Optional, Protocol, List, Tuple

from datetime import datetime
from daft import DataFrame

from ..core.base import Component 
from pydantic import Field


class Episode(Component):
    """
    An Episode represents a temporal boundary in the simulation.
    Unlike fixed steps, episodes can have variable duration and
    different archetypes can progress at different rates.
    """
    id: str 
    start_step: int
    end_step: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.timestamp(timezone.utc))
    completed_archetypes: Set[str] = Field(default_factory=set)


class EpisodeCoordinator:
    """
    Coordinates episode progression across concurrent archetype streams.
    
    Key innovations:
    1. Archetypes can progress at different rates
    2. Episodes provide synchronization points
    3. Supports both real-time and turn-based simulations
    """
    
    def __init__(self, episode_size: int = 10):
        self.episode_size = episode_size
        self.current_episodes: Dict[str, Episode] = {}  # archetype -> episode
        self.episode_completion = asyncio.Condition()
        
    async def get_episode_for_archetype(self, archetype_name: str, current_step: int) -> Episode:
        """
        Get or create the appropriate episode for an archetype at a given step.
        """
        if archetype_name not in self.current_episodes:
            episode_id = f"ep_{current_step // self.episode_size}"
            self.current_episodes[archetype_name] = Episode(
                id=episode_id,
                start_step=current_step,
                end_step=current_step + self.episode_size - 1
            )
        
        return self.current_episodes[archetype_name]
    
    async def mark_archetype_complete(self, archetype_name: str, episode_id: str):
        """
        Mark an archetype as complete for an episode.
        This enables coordination without blocking fast archetypes.
        """
        async with self.episode_completion:
            episode = self.current_episodes.get(archetype_name)
            if episode and episode.id == episode_id:
                episode.completed_archetypes.add(archetype_name)
                self.episode_completion.notify_all()
    
    async def wait_for_episode_completion(
        self, 
        episode_id: str, 
        required_archetypes: Optional[Set[str]] = None,
        timeout: Optional[float] = None
    ) -> bool:
        """
        Wait for specific archetypes to complete an episode.
        Returns True if completed, False if timeout.
        """
        async with self.episode_completion:
            def check_completion():
                if required_archetypes is None:
                    return True  # Don't wait if no requirements
                
                completed = set()
                for arch_name, episode in self.current_episodes.items():
                    if episode.id == episode_id and arch_name in episode.completed_archetypes:
                        completed.add(arch_name)
                
                return required_archetypes.issubset(completed)
            
            try:
                await asyncio.wait_for(
                    self.episode_completion.wait_for(check_completion),
                    timeout=timeout
                )
                return True
            except asyncio.TimeoutError:
                return False


class TemporalProcessor(Protocol):
    """
    Processors that understand episodes and can handle temporal boundaries.
    """
    async def process_episode(
        self,
        archetype_name: str,
        df: DataFrame,
        episode: Episode,
        dt: float
    ) -> DataFrame:
        """Process data within an episode context."""
        ...
    
    async def on_episode_boundary(
        self,
        archetype_name: str,
        old_episode: Episode,
        new_episode: Episode
    ):
        """Handle transitions between episodes."""
        ...


class AsyncEpisodeSystem:
    """
    System that processes archetype streams with episode-based coordination.
    
    This replaces rigid step-based synchronization with flexible episodes,
    allowing different archetypes to progress at different rates while
    maintaining coordination points when needed.
    """
    
    def __init__(
        self, 
        episode_coordinator: EpisodeCoordinator,
        max_concurrent_archetypes: int = 10
    ):
        self.episode_coordinator = episode_coordinator
        self.processors: List[TemporalProcessor] = []
        self.archetype_semaphore = asyncio.Semaphore(max_concurrent_archetypes)
    
    async def execute_with_episodes(
        self,
        store: 'AsyncArchetypeStore',
        component_types: Tuple[type, ...],
        start_step: int,
        num_steps: int,
        dt: float
    ) -> AsyncGenerator[Tuple[str, DataFrame, Episode], None]:
        """
        Execute processors on archetype streams with episode coordination.
        
        Key differences from step-based execution:
        1. Archetypes can be in different episodes
        2. Synchronization only when needed
        3. Natural handling of fast/slow archetypes
        """
        
        async def process_archetype_episodes(
            archetype_name: str,
            initial_df: DataFrame
        ) -> AsyncGenerator[Tuple[str, DataFrame, Episode], None]:
            """Process one archetype through multiple episodes."""
            
            current_df = initial_df
            
            for step in range(start_step, start_step + num_steps):
                # Get or create episode for this archetype
                episode = await self.episode_coordinator.get_episode_for_archetype(
                    archetype_name, step
                )
                
                # Check for episode boundary
                if step == episode.start_step and step > start_step:
                    # Handle episode transition
                    old_episode = await self.episode_coordinator.get_episode_for_archetype(
                        archetype_name, step - 1
                    )
                    for processor in self.processors:
                        await processor.on_episode_boundary(
                            archetype_name, old_episode, episode
                        )
                
                # Process through all processors for this episode
                async with self.archetype_semaphore:
                    for processor in sorted(self.processors, key=lambda x: x.priority):
                        current_df = await processor.process_episode(
                            archetype_name, current_df, episode, dt
                        )
                
                # Mark completion if at episode end
                if step == episode.end_step:
                    await self.episode_coordinator.mark_archetype_complete(
                        archetype_name, episode.id
                    )
                    
                    # Yield completed episode results
                    yield (archetype_name, current_df, episode)
        
        # Create tasks for all archetypes
        tasks = []
        async for archetype_name, df in store.async_get_archetypes_stream(*component_types):
            task = asyncio.create_task(
                process_archetype_episodes(archetype_name, df).__anext__()
            )
            tasks.append((archetype_name, task))
        
        # Yield results as episodes complete
        while tasks:
            done, pending = await asyncio.wait(
                [task for _, task in tasks],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Process completed episodes
            remaining_tasks = []
            for archetype_name, task in tasks:
                if task in done:
                    try:
                        result = await task
                        yield result
                        
                        # Create next episode task for this archetype
                        # (implementation would continue the generator)
                    except StopAsyncIteration:
                        # This archetype is done
                        pass
                else:
                    remaining_tasks.append((archetype_name, task))
            
            tasks = remaining_tasks


"""
Benefits of Episode-Based Architecture:

1. **Flexible Synchronization**:
   - Fast archetypes don't wait for slow ones
   - Synchronization only at episode boundaries
   - Can skip synchronization entirely for independent archetypes

2. **Natural Checkpointing**:
   - Episodes provide natural save points
   - Can checkpoint different archetypes at different times
   - Supports incremental saves

3. **Better Resource Utilization**:
   - CPUs stay busy processing available work
   - No global barriers forcing idle time
   - Natural load balancing

4. **Temporal Flexibility**:
   - Variable episode sizes for different archetypes
   - Support for real-time and turn-based modes
   - Easy to implement time dilation

5. **Perfect Threading Model**:
   As you noted, this gives us a perfectly threaded application where:
   - Each archetype is a natural work unit
   - No shared mutable state
   - Expression building is cheap
   - Only materialize at episode boundaries

This architecture truly leverages the archetype pattern's inherent parallelism!
""" 



# Async System
###################
class AsyncSystem(BaseSystem):
    """
    System that processes archetype streams concurrently using AsyncProcessors.
    The `execute_streaming` method yields (archetype_name, processed_dataframe) tuples 
    as they complete, allowing for asynchronous updates to the store.
    """
    def __init__(self, max_concurrent_processors: int = 10):
        self.processors: List[AsyncProcessor] = [] # Changed to List for easier sorting
        self.processor_semaphore = asyncio.Semaphore(max_concurrent_processors)
        self._tasks: Set[asyncio.Task] = set() # To keep track of running tasks

    def add_processor(self, proc: AsyncProcessor):
        # Consider checking for duplicate processor types or managing unique instances
        self.processors.append(proc)
        # Keep processors sorted by priority for deterministic application order per archetype
        self.processors.sort(key=lambda p: p.priority)

    def remove_processor(self, proc_type: Type[AsyncProcessor]):
        self.processors = [p for p in self.processors if not isinstance(p, proc_type)]
    
    async def _process_single_archetype_stream(
        self, 
        archetype_name: str, 
        df: DataFrame, 
        step: int, 
        dt: float
    ) -> Tuple[str, DataFrame]:
        """Helper to process one archetype's DataFrame through all processors."""
        current_df = df
        for proc_instance in self.processors: # Processors are pre-sorted by priority
            # The semaphore here controls how many archetypes are being processed by *this system* concurrently.
            # Individual UDFs within processors might have their own semaphores for I/O, etc.
            async with self.processor_semaphore:
                if len(current_df) == 0: # Optimization: skip if df is empty
                    break
                processed_df = await proc_instance.process_stream(archetype_name, current_df, dt)
                if processed_df is None or len(processed_df) == 0:
                    current_df = DataFrame.from_pydict({}) # Represent as empty if processor removed all entities
                    break # Stop processing this archetype if it becomes empty
                current_df = processed_df
        
        # Postprocess the final DataFrame for this archetype
        if len(current_df) > 0:
             final_df = await self.processors[-1].postprocess_stream(current_df, step) # Assuming at least one processor
        else:
            # Create an empty DataFrame with the correct schema if it became empty.
            # This is tricky as the schema might have changed. For now, return an empty dict-based DF.
            # A better approach would be to get the schema from the last valid processor output.
            final_df = current_df # Which is an empty DF

        return archetype_name, final_df

    async def execute_streaming(
        self, 
        querier: iAsyncQuerier, 
        store: iAsyncStore, # Added store for potential direct updates
        step: int, 
        dt: float
    ) -> None: # Changed: System now directly appends to store, doesn't yield DFs
        """
        Executes all registered async processors. 
        Archetype data is streamed from the querier.
        Each archetype is processed concurrently through the processor pipeline.
        Processed DataFrames are directly appended back to the store.
        """
        if not self.processors:
            return

        pending_tasks: Dict[str, asyncio.Task] = {}
        processed_archetypes_this_step: Set[str] = set()

        # Iterate through processors by priority for initial query generation
        # This part is tricky - the first processor dictates the initial stream
        # But all processors need to act on the components they care about.
        # The querier.components will be the union of all processor components for this system.
        # For now, let's assume querier is configured for ALL components this system cares about.
        
        # Simplified: Get a stream of (archetype_name, df) from the first processor's preprocess
        # This needs careful thought on how to combine component requirements for the initial query.
        # A better model: querier is configured for the system, not individual processors.
        # Processors then filter/select columns they need from the streamed DataFrame.

        # Let's assume the querier provides streams for all relevant archetypes.
        # The AsyncQueryManager will yield (archetype_name, DataFrame) tuples

        async def task_done_callback(task: asyncio.Task, archetype_name: str):
            try:
                name, processed_df = await task
                if processed_df is not None and len(processed_df) > 0:
                    # Append directly to the store
                    await store.async_append(name, processed_df)
                    processed_archetypes_this_step.add(name)
                elif processed_df is not None and len(processed_df) == 0:
                    # Handle deletion/inactivation if processor returns empty DF
                    # This might involve creating a new DataFrame with is_active=False
                    # For now, we can append an empty df or a special marker if store supports it.
                    # Or, more simply, ensure the store.append can handle empty DFs as deletions.
                    await store.async_append(name, processed_df) # Assuming append handles empty as deletion/update
                    processed_archetypes_this_step.add(name)

            except Exception as e:
                print(f"Error processing archetype {archetype_name}: {e}") # Proper logging needed
            finally:
                pending_tasks.pop(archetype_name, None)
                self._tasks.remove(task)

        # This assumes querier fetches based on *all* components the system *might* process.
        # Each processor inside _process_single_archetype_stream will then select its relevant columns.
        # This needs to be aligned with how AsyncQueryManager.preprocess_stream is structured.
        # The querier now directly streams (archetype_name, df) tuples.
        
        async for archetype_name, df in querier(component_types=self.get_system_components(), step=step):
            if archetype_name in pending_tasks: # Avoid reprocessing if already being handled
                continue

            task = asyncio.create_task(self._process_single_archetype_stream(archetype_name, df, step, dt))
            self._tasks.add(task)
            pending_tasks[archetype_name] = task
            task.add_done_callback(lambda t, name=archetype_name: asyncio.create_task(task_done_callback(t, name)))

        # Wait for all initial tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True) # Handle exceptions
        
        # Potentially, here you could also handle archetypes that were *not* modified
        # but need their `step` column advanced if that's part of the system logic.
        # This is complex and depends on exact requirements.

    def get_system_components(self) -> Tuple[Type[Component], ...]:
        """Returns a unique set of all components required by processors in this system."""
        all_components: Set[Type[Component]] = set()
        for proc in self.processors:
            if proc.components:
                all_components.update(proc.components)
        return tuple(all_components)

    async def shutdown(self):
        """Cancel all running tasks during shutdown."""
        for task in list(self._tasks): # Iterate over a copy
            if not task.done():
                task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()