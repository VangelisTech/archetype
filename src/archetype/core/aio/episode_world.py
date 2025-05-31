import asyncio
import time
from typing import List, Type, Optional, Set, Dict

from ..base import Component
from ..world import SimpleWorld
from .episode import Episode, EpisodeCoordinator
from .async_episode_system import AsyncEpisodeSystem, EpisodeExecutionResult
from .async_system import AsyncProcessor


class EpisodeWorld:
    """
    World that uses episode-based temporal coordination instead of rigid steps.
    
    Key features:
    1. Archetypes can progress at different rates
    2. Synchronization only when needed
    3. Episode boundaries provide natural checkpointing
    4. Supports both real-time and turn-based modes
    """
    
    def __init__(
        self, 
        sync_world: SimpleWorld, 
        episode_size: int = 10,
        max_concurrent_archetypes: int = 10
    ):
        """Initialize episode world by wrapping an existing sync world."""
        # Reuse sync world's infrastructure
        self.store = sync_world.store
        self.querier = sync_world.querier 
        self.updater = sync_world.updater
        self.current_step = sync_world.current_step
        self.id = sync_world.id
        
        # Episode coordination
        self.episode_coordinator = EpisodeCoordinator(
            episode_size=episode_size, 
            world_id=self.id
        )
        self.episode_system = AsyncEpisodeSystem(
            self.episode_coordinator, 
            max_concurrent_archetypes
        )
        
        # State tracking
        self.episode_size = episode_size
        self.completed_episodes: List[Episode] = []
        
    def add_processor(self, proc: AsyncProcessor) -> None:
        """Add a processor to the episode system."""
        self.episode_system.add_processor(proc)
        
    def remove_processor(self, proc: AsyncProcessor) -> None:
        """Remove a processor from the episode system."""
        self.episode_system.remove_processor(type(proc))
    
    # Delegate sync methods to underlying world
    def spawn(self, *components: Component, step: Optional[int] = None) -> int:
        """Create a new entity with these components."""
        return self.store.add_entity(list(components), step or self.current_step)
    
    def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        """Mark an entity dead (is_active=False)."""
        self.store.remove_entity(entity_id, step or self.current_step)
    
    def materialize_spawns(self) -> None:
        """Materialize any pending spawns before the first step."""
        self.store.materialize_spawns()
        
    async def run_episode(self, dt: float = 0.1) -> Dict[str, any]:
        """
        Run a single episode across all archetypes.
        
        Returns episode statistics and results.
        """
        self.materialize_spawns()
        
        start_time = time.time()
        episode_results = {}
        
        print(f"🎬 Starting episode at step {self.current_step} (size: {self.episode_size})")
        
        # Execute episode
        async for result in self.episode_system.execute_with_episodes(
            self.querier, 
            self.current_step, 
            self.episode_size, 
            dt
        ):
            if result.success:
                episode_results[result.archetype_name] = result.processed_df
                self.completed_episodes.append(result.episode)
                
                print(f"  ✅ {result.archetype_name} completed episode {result.episode.id} "
                      f"in {result.processing_time:.3f}s")
            else:
                print(f"  ❌ {result.archetype_name} failed: {result.error}")
        
        # Update store with results
        if episode_results:
            self.updater(episode_results)
        
        # Advance world state
        self.current_step += self.episode_size
        
        total_time = time.time() - start_time
        
        episode_stats = {
            "episode_duration": total_time,
            "archetypes_processed": len(episode_results),
            "successful_episodes": len([r for r in episode_results.values() if r is not None]),
            "steps_advanced": self.episode_size,
            "current_step": self.current_step
        }
        
        print(f"🏁 Episode completed in {total_time:.3f}s - advanced {self.episode_size} steps")
        
        return episode_stats
    
    async def run_with_sync_points(
        self, 
        num_episodes: int, 
        dt: float = 0.1,
        sync_every: int = 3
    ) -> List[Dict[str, any]]:
        """
        Run multiple episodes with periodic synchronization points.
        
        This demonstrates how episode coordination enables both:
        - Independent progression most of the time
        - Synchronization when coordination is needed
        """
        episode_stats = []
        
        print(f"🚀 Running {num_episodes} episodes with sync every {sync_every} episodes")
        
        for episode_num in range(num_episodes):
            # Determine if this is a sync point
            is_sync_point = (episode_num + 1) % sync_every == 0
            
            if is_sync_point:
                print(f"⏸️  Episode {episode_num + 1}: SYNCHRONIZATION POINT")
                # In a real implementation, you'd specify which archetypes to sync
                sync_points = [self.current_step + self.episode_size - 1]
            else:
                sync_points = None
            
            # Run episode
            stats = await self.run_episode(dt)
            stats["episode_number"] = episode_num + 1
            stats["was_sync_point"] = is_sync_point
            episode_stats.append(stats)
            
            # Brief pause between episodes for demo purposes
            await asyncio.sleep(0.1)
        
        print(f"🎯 Completed {num_episodes} episodes!")
        return episode_stats
    
    def get_episode_status(self) -> Dict[str, any]:
        """Get detailed status of all episodes."""
        active_episodes = self.episode_coordinator.get_active_episodes()
        
        return {
            "current_step": self.current_step,
            "episode_size": self.episode_size,
            "active_episodes": len(active_episodes),
            "completed_episodes": len(self.completed_episodes),
            "episode_details": {
                episode_id: self.episode_coordinator.get_episode_status(episode_id)
                for episode_id in active_episodes.keys()
            }
        }
    
    async def wait_for_episode_sync(
        self, 
        episode_id: str, 
        required_archetypes: Set[str],
        timeout: float = 30.0
    ) -> bool:
        """
        Wait for specific archetypes to complete an episode.
        
        This is the key coordination primitive that enables selective synchronization.
        """
        return await self.episode_coordinator.wait_for_episode_completion(
            episode_id, required_archetypes, timeout
        )