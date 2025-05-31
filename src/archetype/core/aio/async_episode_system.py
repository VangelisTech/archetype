import asyncio
import time
from typing import List, Dict, Type, Tuple, Optional, AsyncGenerator

from daft import DataFrame

from ..base import BaseSystem, Component
from .async_system import AsyncProcessor
from .episode import Episode, EpisodeCoordinator, EpisodeProcessor, EpisodeExecutionResult
from ..interfaces import iQuerier


class AsyncEpisodeSystem(BaseSystem):
    """
    System that processes archetype streams with episode-based coordination.
    
    This replaces rigid step-based synchronization with flexible episodes,
    allowing different archetypes to progress at different rates while
    maintaining coordination points when needed.
    
    Key innovations:
    1. Each archetype can be in a different episode
    2. Synchronization only when explicitly required
    3. Natural handling of fast/slow archetypes
    4. Episode boundaries provide checkpointing opportunities
    """
    
    def __init__(
        self, 
        episode_coordinator: EpisodeCoordinator,
        max_concurrent_archetypes: int = 10
    ):
        self.episode_coordinator = episode_coordinator
        self.processors: List[AsyncProcessor] = []
        self.episode_processors: List[EpisodeProcessor] = []
        self.archetype_semaphore = asyncio.Semaphore(max_concurrent_archetypes)
        
    def add_processor(self, proc: AsyncProcessor):
        """Add a processor to the system."""
        if isinstance(proc, EpisodeProcessor):
            self.episode_processors.append(proc)
        else:
            self.processors.append(proc)
    
    def remove_processor(self, proc_type: Type[AsyncProcessor]):
        """Remove all processors of the given type."""
        self.processors = [p for p in self.processors if not isinstance(p, proc_type)]
        self.episode_processors = [p for p in self.episode_processors if not isinstance(p, proc_type)]
    
    def execute(self, querier: iQuerier, step: int, dt: float) -> Dict[str, DataFrame]:
        """
        Synchronous execute method for BaseSystem compatibility.
        This is a simplified interface that runs a single episode synchronously.
        """
        # For compatibility, run a single episode synchronously
        import asyncio
        return asyncio.run(self.execute_single_episode(querier, 1, dt))
    
    async def execute_with_episodes(
        self,
        querier: iQuerier,
        start_step: int,
        num_steps: int,
        dt: float,
        synchronization_points: Optional[List[int]] = None
    ) -> AsyncGenerator[EpisodeExecutionResult, None]:
        """
        Execute processors on archetype streams with episode coordination.
        
        Key differences from step-based execution:
        1. Archetypes can be in different episodes
        2. Synchronization only at specified points
        3. Natural handling of fast/slow archetypes
        4. Results yielded as episodes complete
        
        Args:
            querier: Data source interface
            start_step: Starting simulation step
            num_steps: Number of steps to execute
            dt: Time delta per step
            synchronization_points: Steps where all archetypes must sync (optional)
        
        Yields:
            EpisodeExecutionResult objects as episodes complete
        """
        sync_points = set(synchronization_points or [])
        
        # Get all active archetypes
        active_archetypes = querier._store.get_active_archetypes_with_signatures()
        
        # Create tasks for each archetype's episode progression
        archetype_tasks = {}
        
        for archetype_name, (df, archetype_signature) in active_archetypes.items():
            # Create a wrapper coroutine that collects generator results
            async def collect_results(arch_name, df, sig):
                results = []
                async for result in self._process_archetype_episodes(
                    arch_name, df, sig, start_step, num_steps, dt, sync_points
                ):
                    results.append(result)
                return results
            
            task = asyncio.create_task(collect_results(archetype_name, df, archetype_signature))
            archetype_tasks[archetype_name] = task
        
        # Yield results as episodes complete
        try:
            while archetype_tasks:
                # Wait for at least one task to complete
                done, pending = await asyncio.wait(
                    archetype_tasks.values(),
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Process completed episodes
                for completed_task in done:
                    # Find which archetype completed
                    completed_archetype = None
                    for arch_name, task in archetype_tasks.items():
                        if task == completed_task:
                            completed_archetype = arch_name
                            break
                    
                    if completed_archetype:
                        del archetype_tasks[completed_archetype]
                        
                        try:
                            results = await completed_task
                            for result in results:
                                yield result
                        except Exception as e:
                            # Yield error result
                            yield EpisodeExecutionResult(
                                archetype_name=completed_archetype,
                                processed_df=None,
                                episode=Episode(id="error", start_step=start_step, world_id="error"),
                                processing_time=0.0,
                                success=False,
                                error=e
                            )
        
        finally:
            # Cancel any remaining tasks
            for task in archetype_tasks.values():
                if not task.done():
                    task.cancel()
    
    async def _process_archetype_episodes(
        self,
        archetype_name: str,
        initial_df: DataFrame,
        archetype_signature: Tuple[Type[Component], ...],
        start_step: int,
        num_steps: int,
        dt: float,
        sync_points: set
    ) -> AsyncGenerator[EpisodeExecutionResult, None]:
        """
        Process one archetype through multiple episodes.
        
        This is where the episode magic happens:
        - Each archetype progresses through its own episodes
        - Synchronization only at episode boundaries when required
        - Fast archetypes don't wait for slow ones
        """
        
        current_df = initial_df
        
        for step in range(start_step, start_step + num_steps):
            start_time = time.time()
            
            try:
                # Get or create episode for this archetype at this step
                episode = await self.episode_coordinator.get_episode_for_archetype(
                    archetype_name, step
                )
                
                # Check for episode boundary (transition between episodes)
                if step > start_step and step == episode.start_step:
                    # We're starting a new episode - handle transition
                    old_episode = await self.episode_coordinator.get_episode_for_archetype(
                        archetype_name, step - 1
                    )
                    
                    # Notify episode processors of boundary
                    for processor in self.episode_processors:
                        if processor.can_process_archetype(archetype_signature):
                            await processor.on_episode_boundary(
                                archetype_name, old_episode, episode
                            )
                
                # Process through all relevant processors for this step
                async with self.archetype_semaphore:
                    # Apply regular async processors
                    for processor in self.processors:
                        if processor.can_process_archetype(archetype_signature):
                            current_df = await processor.process(current_df, dt)
                            current_df = processor.postprocess(current_df, step)
                    
                    # Apply episode-aware processors
                    for processor in self.episode_processors:
                        if processor.can_process_archetype(archetype_signature):
                            current_df = await processor.process_episode(
                                archetype_name, current_df, episode, dt
                            )
                            current_df = processor.postprocess(current_df, step)
                
                # Handle synchronization points
                if step in sync_points:
                    episode.synchronization_required = True
                
                # Check if we've completed an episode
                if step == episode.end_step:
                    await self.episode_coordinator.mark_archetype_complete(
                        archetype_name, episode.id
                    )
                    
                    processing_time = time.time() - start_time
                    
                    # Yield completed episode result
                    yield EpisodeExecutionResult(
                        archetype_name=archetype_name,
                        processed_df=current_df,
                        episode=episode,
                        processing_time=processing_time,
                        success=True
                    )
                    
                    # Handle synchronization if required
                    if episode.synchronization_required:
                        # Wait for other archetypes to complete this episode
                        # (In a real implementation, you'd specify which archetypes to wait for)
                        success = await self.episode_coordinator.wait_for_episode_completion(
                            episode.id, timeout=10.0
                        )
                        if not success:
                            print(f"Warning: Synchronization timeout for episode {episode.id}")
            
            except Exception as e:
                processing_time = time.time() - start_time
                yield EpisodeExecutionResult(
                    archetype_name=archetype_name,
                    processed_df=current_df,
                    episode=episode if 'episode' in locals() else Episode(
                        id="error", start_step=step, world_id="error"
                    ),
                    processing_time=processing_time,
                    success=False,
                    error=e
                )
    
    async def execute_single_episode(
        self,
        querier: iQuerier,
        episode_size: int,
        dt: float
    ) -> Dict[str, DataFrame]:
        """
        Execute a single episode across all archetypes.
        
        This is a simpler interface for cases where you want traditional
        step-like behavior but with episode coordination benefits.
        """
        results = {}
        
        async for result in self.execute_with_episodes(
            querier, 0, episode_size, dt
        ):
            if result.success:
                results[result.archetype_name] = result.processed_df
            else:
                print(f"Error processing {result.archetype_name}: {result.error}")
        
        return results