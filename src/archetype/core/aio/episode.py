import asyncio
from datetime import datetime, timezone
from typing import Dict, Set, Optional, Type, List, Tuple
from dataclasses import dataclass, field

from ..base import Component


@dataclass
class Episode:
    """
    An Episode represents a temporal boundary in the simulation.
    Unlike fixed steps, episodes can have variable duration and
    different archetypes can progress at different rates.
    """
    id: str
    start_step: int
    end_step: Optional[int] = None
    world_id: str = "default"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_archetypes: Set[str] = field(default_factory=set)
    synchronization_required: bool = False


class EpisodeCoordinator:
    """
    Coordinates episode progression across concurrent archetype streams.
    
    Key innovations:
    1. Archetypes can progress at different rates
    2. Episodes provide synchronization points when needed
    3. Supports both real-time and turn-based simulations
    """
    
    def __init__(self, episode_size: int = 10, world_id: str = "default"):
        self.episode_size = episode_size
        self.world_id = world_id
        self.current_episodes: Dict[str, Episode] = {}  # archetype_name -> episode
        self.episode_completion = asyncio.Condition()
        self.global_episodes: Dict[str, Episode] = {}  # episode_id -> episode
        
    async def get_episode_for_archetype(self, archetype_name: str, current_step: int) -> Episode:
        """
        Get or create the appropriate episode for an archetype at a given step.
        
        Episodes are created based on step boundaries and episode_size.
        Different archetypes can be in different episodes simultaneously.
        """
        episode_number = current_step // self.episode_size
        episode_id = f"{self.world_id}_ep_{episode_number}"
        
        # Check if archetype already has an active episode
        if archetype_name in self.current_episodes:
            current_episode = self.current_episodes[archetype_name]
            
            # If we're still within the current episode, return it
            if (current_episode.end_step is None or 
                current_step <= current_episode.end_step):
                return current_episode
        
        # Create new episode for this archetype
        episode = Episode(
            id=episode_id,
            start_step=episode_number * self.episode_size,
            end_step=(episode_number + 1) * self.episode_size - 1,
            world_id=self.world_id
        )
        
        self.current_episodes[archetype_name] = episode
        self.global_episodes[episode_id] = episode
        
        return episode
    
    async def mark_archetype_complete(self, archetype_name: str, episode_id: str):
        """
        Mark an archetype as complete for an episode.
        This enables coordination without blocking fast archetypes.
        """
        async with self.episode_completion:
            if episode_id in self.global_episodes:
                episode = self.global_episodes[episode_id]
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
        
        This is the key coordination primitive that enables:
        - Synchronization points when needed
        - Independent progression when not needed
        """
        async with self.episode_completion:
            def check_completion():
                if required_archetypes is None:
                    return True  # Don't wait if no requirements
                
                if episode_id not in self.global_episodes:
                    return False
                
                episode = self.global_episodes[episode_id]
                return required_archetypes.issubset(episode.completed_archetypes)
            
            if check_completion():
                return True
            
            try:
                await asyncio.wait_for(
                    self.episode_completion.wait_for(check_completion),
                    timeout=timeout
                )
                return True
            except asyncio.TimeoutError:
                return False
    
    def get_active_episodes(self) -> Dict[str, Episode]:
        """Get all currently active episodes across all archetypes."""
        return self.global_episodes.copy()
    
    def get_episode_status(self, episode_id: str) -> Optional[Dict[str, any]]:
        """Get detailed status of a specific episode."""
        if episode_id not in self.global_episodes:
            return None
            
        episode = self.global_episodes[episode_id]
        return {
            "id": episode.id,
            "start_step": episode.start_step,
            "end_step": episode.end_step,
            "world_id": episode.world_id,
            "created_at": episode.created_at,
            "completed_archetypes": len(episode.completed_archetypes),
            "completed_archetype_names": list(episode.completed_archetypes),
            "synchronization_required": episode.synchronization_required
        }


class EpisodeProcessor:
    """
    Base class for processors that understand episodes and can handle temporal boundaries.
    
    This extends AsyncProcessor with episode awareness for fine-grained temporal control.
    """
    
    async def process_episode(
        self,
        archetype_name: str,
        df,  # DataFrame
        episode: Episode,
        dt: float
    ):
        """
        Process data within an episode context.
        
        This method receives the current episode information,
        enabling processors to make decisions based on temporal boundaries.
        """
        # Default implementation delegates to regular process method
        return await self.process(df, dt)
    
    async def on_episode_boundary(
        self,
        archetype_name: str,
        old_episode: Episode,
        new_episode: Episode
    ):
        """
        Handle transitions between episodes.
        
        This is called when an archetype transitions from one episode to another,
        enabling cleanup, state transitions, or coordination logic.
        """
        # Default implementation does nothing
        pass


@dataclass 
class EpisodeExecutionResult:
    """Result of executing processors within an episode context."""
    archetype_name: str
    processed_df: any  # DataFrame
    episode: Episode
    processing_time: float
    success: bool
    error: Optional[Exception] = None