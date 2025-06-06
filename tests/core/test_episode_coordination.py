import pytest
import asyncio
import time
from typing import Dict, Set
from daft import DataFrame, col

import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core.base import Component
from archetype.core.aio.episode import Episode, EpisodeCoordinator, EpisodeProcessor, EpisodeExecutionResult
from archetype.core.aio.async_system import AsyncProcessor, async_processor


# Test Components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float

class Health(Component):
    hp: int
    max_hp: int


class TestEpisode:
    """Test the Episode data structure."""
    
    def test_episode_creation(self):
        """Test that episodes are created with correct properties."""
        episode = Episode(
            id="test_ep_1",
            start_step=0,
            end_step=9,
            world_id="test_world"
        )
        
        assert episode.id == "test_ep_1"
        assert episode.start_step == 0
        assert episode.end_step == 9
        assert episode.world_id == "test_world"
        assert len(episode.completed_archetypes) == 0
        assert not episode.synchronization_required
        assert episode.created_at is not None
    
    def test_episode_archetype_completion(self):
        """Test tracking of completed archetypes."""
        episode = Episode(id="test", start_step=0)
        
        episode.completed_archetypes.add("archetype_a")
        episode.completed_archetypes.add("archetype_b")
        
        assert len(episode.completed_archetypes) == 2
        assert "archetype_a" in episode.completed_archetypes
        assert "archetype_b" in episode.completed_archetypes


class TestEpisodeCoordinator:
    """Test the EpisodeCoordinator for temporal coordination."""
    
    @pytest.fixture
    def coordinator(self):
        """Create a test EpisodeCoordinator."""
        return EpisodeCoordinator(episode_size=10, world_id="test_world")
    
    @pytest.mark.asyncio
    async def test_get_episode_for_archetype_new(self, coordinator):
        """Test getting episode for archetype creates new episode correctly."""
        episode = await coordinator.get_episode_for_archetype("arch_a", 0)
        
        assert episode.id == "test_world_ep_0"
        assert episode.start_step == 0
        assert episode.end_step == 9
        assert episode.world_id == "test_world"
    
    @pytest.mark.asyncio
    async def test_get_episode_for_archetype_existing(self, coordinator):
        """Test getting episode for archetype returns existing episode when within bounds."""
        # Get episode for step 5 (should be episode 0)
        episode1 = await coordinator.get_episode_for_archetype("arch_a", 5)
        episode2 = await coordinator.get_episode_for_archetype("arch_a", 7)
        
        # Should be the same episode
        assert episode1.id == episode2.id
        assert episode1 is episode2  # Same object
    
    @pytest.mark.asyncio
    async def test_get_episode_for_archetype_new_episode(self, coordinator):
        """Test that new episode is created when step exceeds current episode."""
        # Get episode for step 5 (episode 0)
        episode1 = await coordinator.get_episode_for_archetype("arch_a", 5)
        
        # Get episode for step 15 (episode 1)
        episode2 = await coordinator.get_episode_for_archetype("arch_a", 15)
        
        assert episode1.id != episode2.id
        assert episode1.id == "test_world_ep_0"
        assert episode2.id == "test_world_ep_1"
        assert episode2.start_step == 10
        assert episode2.end_step == 19
    
    @pytest.mark.asyncio
    async def test_different_archetypes_independent_episodes(self, coordinator):
        """Test that different archetypes can be in different episodes."""
        episode_a = await coordinator.get_episode_for_archetype("arch_a", 5)
        episode_b = await coordinator.get_episode_for_archetype("arch_b", 15)
        
        # Should be in different episodes
        assert episode_a.id != episode_b.id
        assert episode_a.start_step == 0
        assert episode_b.start_step == 10
    
    @pytest.mark.asyncio
    async def test_mark_archetype_complete(self, coordinator):
        """Test marking archetypes as complete for episodes."""
        episode = await coordinator.get_episode_for_archetype("arch_a", 5)
        
        await coordinator.mark_archetype_complete("arch_a", episode.id)
        
        assert "arch_a" in episode.completed_archetypes
    
    @pytest.mark.asyncio
    async def test_wait_for_episode_completion_immediate(self, coordinator):
        """Test waiting for episode completion when already complete."""
        episode = await coordinator.get_episode_for_archetype("arch_a", 5)
        await coordinator.mark_archetype_complete("arch_a", episode.id)
        
        # Should return immediately since arch_a is complete
        result = await coordinator.wait_for_episode_completion(
            episode.id, 
            required_archetypes={"arch_a"},
            timeout=1.0
        )
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_wait_for_episode_completion_timeout(self, coordinator):
        """Test waiting for episode completion with timeout."""
        episode = await coordinator.get_episode_for_archetype("arch_a", 5)
        
        # Don't mark as complete, should timeout
        start_time = time.time()
        result = await coordinator.wait_for_episode_completion(
            episode.id,
            required_archetypes={"arch_a"},
            timeout=0.1
        )
        elapsed = time.time() - start_time
        
        assert result is False
        assert elapsed >= 0.1
        assert elapsed < 0.2  # Should timeout quickly
    
    @pytest.mark.asyncio
    async def test_wait_for_episode_completion_no_requirements(self, coordinator):
        """Test waiting with no requirements returns immediately."""
        episode = await coordinator.get_episode_for_archetype("arch_a", 5)
        
        result = await coordinator.wait_for_episode_completion(
            episode.id,
            required_archetypes=None,
            timeout=1.0
        )
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_wait_for_episode_completion_concurrent(self, coordinator):
        """Test concurrent waiting and completion."""
        episode = await coordinator.get_episode_for_archetype("arch_a", 5)
        
        async def mark_complete_later():
            await asyncio.sleep(0.05)
            await coordinator.mark_archetype_complete("arch_a", episode.id)
        
        # Start marking complete in background
        complete_task = asyncio.create_task(mark_complete_later())
        
        # Wait for completion
        start_time = time.time()
        result = await coordinator.wait_for_episode_completion(
            episode.id,
            required_archetypes={"arch_a"},
            timeout=1.0
        )
        elapsed = time.time() - start_time
        
        await complete_task
        
        assert result is True
        assert elapsed >= 0.05  # Should wait for the mark_complete_later
        assert elapsed < 0.1    # But not too long
    
    def test_get_active_episodes(self, coordinator):
        """Test getting all active episodes."""
        # Should start empty
        active = coordinator.get_active_episodes()
        assert len(active) == 0
    
    @pytest.mark.asyncio
    async def test_get_episode_status(self, coordinator):
        """Test getting detailed episode status."""
        episode = await coordinator.get_episode_for_archetype("arch_a", 5)
        await coordinator.mark_archetype_complete("arch_a", episode.id)
        
        status = coordinator.get_episode_status(episode.id)
        
        assert status is not None
        assert status["id"] == episode.id
        assert status["start_step"] == 0
        assert status["end_step"] == 9
        assert status["world_id"] == "test_world"
        assert status["completed_archetypes"] == 1
        assert "arch_a" in status["completed_archetype_names"]
        assert not status["synchronization_required"]
    
    def test_get_episode_status_nonexistent(self, coordinator):
        """Test getting status for nonexistent episode."""
        status = coordinator.get_episode_status("nonexistent")
        assert status is None


class TestEpisodeProcessor:
    """Test the EpisodeProcessor base class."""
    
    def test_episode_processor_inheritance(self):
        """Test that EpisodeProcessor can be used as base class."""
        class TestEpisodeProcessor(EpisodeProcessor):
            components = (Position, Velocity)
            priority = 1
            
            def can_process_archetype(self, archetype_signature):
                return set(self.components).issubset(set(archetype_signature))
        
        processor = TestEpisodeProcessor()
        
        assert processor.components == (Position, Velocity)
        assert processor.priority == 1
    
    @pytest.mark.asyncio
    async def test_episode_processor_default_methods(self):
        """Test default implementations of episode processor methods."""
        class TestEpisodeProcessor(EpisodeProcessor):
            components = (Position,)
            
            def can_process_archetype(self, archetype_signature):
                return Position in archetype_signature
            
            async def process(self, df, dt):
                return df
        
        processor = TestEpisodeProcessor()
        episode = Episode(id="test", start_step=0)
        
        # Create a simple DataFrame
        import daft
        df = daft.from_pydict({"entity_id": [1], "position__x": [0.0], "position__y": [0.0]})
        
        # Default process_episode should delegate to process
        result = await processor.process_episode("arch_a", df, episode, 0.1)
        assert result is not None
        
        # Default on_episode_boundary should do nothing (not raise exception)
        old_episode = Episode(id="old", start_step=0)
        new_episode = Episode(id="new", start_step=10)
        await processor.on_episode_boundary("arch_a", old_episode, new_episode)


class TestEpisodeExecutionResult:
    """Test the EpisodeExecutionResult data structure."""
    
    def test_execution_result_success(self):
        """Test creating successful execution result."""
        episode = Episode(id="test", start_step=0)
        
        import daft
        df = daft.from_pydict({"entity_id": [1]})
        
        result = EpisodeExecutionResult(
            archetype_name="test_arch",
            processed_df=df,
            episode=episode,
            processing_time=0.1,
            success=True
        )
        
        assert result.archetype_name == "test_arch"
        assert result.processed_df is df
        assert result.episode is episode
        assert result.processing_time == 0.1
        assert result.success is True
        assert result.error is None
    
    def test_execution_result_failure(self):
        """Test creating failed execution result."""
        episode = Episode(id="test", start_step=0)
        error = ValueError("Test error")
        
        result = EpisodeExecutionResult(
            archetype_name="test_arch",
            processed_df=None,
            episode=episode,
            processing_time=0.05,
            success=False,
            error=error
        )
        
        assert result.archetype_name == "test_arch"
        assert result.processed_df is None
        assert result.episode is episode
        assert result.processing_time == 0.05
        assert result.success is False
        assert result.error is error


class TestEpisodeIntegration:
    """Integration tests for episode coordination primitives."""
    
    @pytest.mark.asyncio
    async def test_multiple_archetypes_different_episodes(self):
        """Test multiple archetypes progressing through different episodes."""
        coordinator = EpisodeCoordinator(episode_size=5, world_id="integration")
        
        # Archetype A progresses quickly
        episode_a1 = await coordinator.get_episode_for_archetype("fast_arch", 2)
        episode_a2 = await coordinator.get_episode_for_archetype("fast_arch", 7)  # Next episode
        
        # Archetype B progresses slowly
        episode_b1 = await coordinator.get_episode_for_archetype("slow_arch", 1)
        
        # Fast archetype should be in episode 1, slow still in episode 0
        assert episode_a1.id == "integration_ep_0"
        assert episode_a2.id == "integration_ep_1"
        assert episode_b1.id == "integration_ep_0"
        
        # They should be different episode objects even though same ID
        assert episode_a1 is not episode_b1  # Different archetype episodes
    
    @pytest.mark.asyncio
    async def test_synchronization_coordination(self):
        """Test coordination between multiple archetypes at sync points."""
        coordinator = EpisodeCoordinator(episode_size=10, world_id="sync_test")
        
        # Both archetypes start in same episode
        episode_a = await coordinator.get_episode_for_archetype("arch_a", 5)
        episode_b = await coordinator.get_episode_for_archetype("arch_b", 7)
        
        assert episode_a.id == episode_b.id  # Same episode ID
        
        # Mark one complete
        await coordinator.mark_archetype_complete("arch_a", episode_a.id)
        
        # Waiting for both should timeout since only one is complete
        result = await coordinator.wait_for_episode_completion(
            episode_a.id,
            required_archetypes={"arch_a", "arch_b"},
            timeout=0.1
        )
        assert result is False
        
        # Mark second complete
        await coordinator.mark_archetype_complete("arch_b", episode_b.id)
        
        # Now waiting for both should succeed
        result = await coordinator.wait_for_episode_completion(
            episode_a.id,
            required_archetypes={"arch_a", "arch_b"},
            timeout=0.1
        )
        assert result is True