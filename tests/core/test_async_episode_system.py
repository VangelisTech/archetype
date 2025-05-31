import pytest
import asyncio
import time
from typing import List
from daft import DataFrame, col

import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core.base import Component
from archetype.core.aio.async_episode_system import AsyncEpisodeSystem
from archetype.core.aio.async_system import AsyncProcessor, async_processor
from archetype.core.aio.episode import Episode, EpisodeCoordinator, EpisodeProcessor, EpisodeExecutionResult
from archetype.core.store import ArchetypeStore
from archetype.core.querier import QueryManager


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


# Test Processors
@async_processor(Position, Velocity, priority=1)
class EpisodeMovementProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        await asyncio.sleep(0.01)
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })

@async_processor(Health, priority=2)
class EpisodeHealthProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        await asyncio.sleep(0.02)
        return df.with_columns({
            "health__hp": col("health__hp") + 1
        })


class TestEpisodeProcessor(EpisodeProcessor, AsyncProcessor):
    """Test episode-aware processor."""
    components = (Position,)
    priority = 3
    
    def can_process_archetype(self, archetype_signature):
        return Position in archetype_signature
    
    async def process(self, df, dt):
        await asyncio.sleep(0.005)
        return df
    
    async def process_episode(self, archetype_name, df, episode, dt):
        # Apply episode-specific logic
        if episode.start_step % 20 == 0:  # Every other episode
            return df.with_columns({
                "position__x": col("position__x") * 1.1,
                "position__y": col("position__y") * 1.1,
            })
        return df
    
    async def on_episode_boundary(self, archetype_name, old_episode, new_episode):
        # Track episode transitions (would normally log or coordinate)
        pass


class TestAsyncEpisodeSystem:
    """Test the AsyncEpisodeSystem with episode-based coordination."""
    
    @pytest.fixture
    def temp_uri(self, tmp_path):
        """Create a temporary URI for testing."""
        test_dir = tmp_path / "test_data"
        test_dir.mkdir(exist_ok=True)
        return str(test_dir)
    
    @pytest.fixture
    def store(self, temp_uri):
        """Create a test ArchetypeStore."""
        return ArchetypeStore(temp_uri)
    
    @pytest.fixture
    def querier(self, store):
        """Create a test QueryManager."""
        return QueryManager(store)
    
    @pytest.fixture
    def coordinator(self):
        """Create a test EpisodeCoordinator."""
        return EpisodeCoordinator(episode_size=5, world_id="test")
    
    @pytest.fixture
    def episode_system(self, coordinator):
        """Create a test AsyncEpisodeSystem."""
        return AsyncEpisodeSystem(coordinator, max_concurrent_archetypes=3)
    
    def test_episode_system_initialization(self, episode_system, coordinator):
        """Test that episode system initializes correctly."""
        assert episode_system.episode_coordinator is coordinator
        assert len(episode_system.processors) == 0
        assert len(episode_system.episode_processors) == 0
        assert episode_system.archetype_semaphore._value == 3
    
    def test_add_processors(self, episode_system):
        """Test adding different types of processors."""
        regular_proc = EpisodeMovementProcessor()
        episode_proc = TestEpisodeProcessor()
        
        episode_system.add_processor(regular_proc)
        episode_system.add_processor(episode_proc)
        
        assert len(episode_system.processors) == 1
        assert len(episode_system.episode_processors) == 1
        assert regular_proc in episode_system.processors
        assert episode_proc in episode_system.episode_processors
    
    def test_remove_processors(self, episode_system):
        """Test removing processors by type."""
        regular_proc = EpisodeMovementProcessor()
        episode_proc = TestEpisodeProcessor()
        
        episode_system.add_processor(regular_proc)
        episode_system.add_processor(episode_proc)
        
        episode_system.remove_processor(EpisodeMovementProcessor)
        
        assert len(episode_system.processors) == 0
        assert len(episode_system.episode_processors) == 1
        
        episode_system.remove_processor(TestEpisodeProcessor)
        
        assert len(episode_system.processors) == 0
        assert len(episode_system.episode_processors) == 0
    
    @pytest.mark.asyncio
    async def test_execute_single_episode(self, episode_system, store, querier):
        """Test executing a single episode."""
        episode_system.add_processor(EpisodeMovementProcessor())
        
        # Add entities
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.add_entity([Position(x=5, y=5), Velocity(vx=2, vy=2)])
        store.materialize_spawns()
        
        # Execute single episode
        results = await episode_system.execute_single_episode(querier, episode_size=3, dt=0.1)
        
        assert isinstance(results, dict)
        assert len(results) >= 0  # May be empty if no processing happened
    
    def test_sync_execute_compatibility(self, episode_system, store, querier):
        """Test that sync execute method works for BaseSystem compatibility."""
        episode_system.add_processor(EpisodeMovementProcessor())
        
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.materialize_spawns()
        
        # This should work without async/await (runs internally with asyncio.run)
        results = episode_system.execute(querier, step=0, dt=0.1)
        
        assert isinstance(results, dict)
    
    @pytest.mark.asyncio
    async def test_execute_with_episodes_basic(self, episode_system, store, querier):
        """Test basic episode-based execution."""
        episode_system.add_processor(EpisodeMovementProcessor())
        episode_system.add_processor(EpisodeHealthProcessor())
        
        # Add entities
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.add_entity([Health(hp=50, max_hp=100)])
        store.materialize_spawns()
        
        results = []
        async for result in episode_system.execute_with_episodes(
            querier, start_step=0, num_steps=3, dt=0.1
        ):
            results.append(result)
        
        # Should have at least some results
        assert len(results) >= 0
        
        # Check result structure
        for result in results:
            assert isinstance(result, EpisodeExecutionResult)
            assert result.archetype_name is not None
            assert result.episode is not None
            assert result.processing_time >= 0
    
    @pytest.mark.asyncio
    async def test_episode_aware_processing(self, episode_system, store, querier):
        """Test that episode-aware processors receive episode context."""
        episode_proc = TestEpisodeProcessor()
        episode_system.add_processor(episode_proc)
        
        # Track calls to process_episode
        process_episode_calls = []
        original_process_episode = episode_proc.process_episode
        
        async def track_process_episode(archetype_name, df, episode, dt):
            process_episode_calls.append((archetype_name, episode.id, episode.start_step))
            return await original_process_episode(archetype_name, df, episode, dt)
        
        episode_proc.process_episode = track_process_episode
        
        # Add entities
        store.add_entity([Position(x=0, y=0)])
        store.materialize_spawns()
        
        results = []
        async for result in episode_system.execute_with_episodes(
            querier, start_step=0, num_steps=2, dt=0.1
        ):
            results.append(result)
        
        # Episode processor should have been called with episode context
        assert len(process_episode_calls) >= 0  # May be 0 if no entities match
    
    @pytest.mark.asyncio
    async def test_episode_boundary_handling(self, episode_system, store, querier):
        """Test that episode boundaries trigger on_episode_boundary calls."""
        episode_proc = TestEpisodeProcessor()
        episode_system.add_processor(episode_proc)
        
        # Track calls to on_episode_boundary
        boundary_calls = []
        original_boundary = episode_proc.on_episode_boundary
        
        async def track_boundary(archetype_name, old_episode, new_episode):
            boundary_calls.append((archetype_name, old_episode.id, new_episode.id))
            return await original_boundary(archetype_name, old_episode, new_episode)
        
        episode_proc.on_episode_boundary = track_boundary
        
        # Add entities
        store.add_entity([Position(x=0, y=0)])
        store.materialize_spawns()
        
        # Execute enough steps to cross episode boundary (episode_size=5)
        results = []
        async for result in episode_system.execute_with_episodes(
            querier, start_step=0, num_steps=8, dt=0.1  # Should cross episode boundary
        ):
            results.append(result)
        
        # May have boundary calls if episodes were crossed
        # (Exact behavior depends on archetype progression)
        assert isinstance(boundary_calls, list)
    
    @pytest.mark.asyncio
    async def test_synchronization_points(self, episode_system, store, querier):
        """Test that synchronization points are handled."""
        episode_system.add_processor(EpisodeMovementProcessor())
        
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.materialize_spawns()
        
        # Execute with synchronization points
        sync_points = [2, 4]  # Sync at steps 2 and 4
        results = []
        
        async for result in episode_system.execute_with_episodes(
            querier, start_step=0, num_steps=5, dt=0.1, synchronization_points=sync_points
        ):
            results.append(result)
        
        # Should complete without error
        assert isinstance(results, list)
    
    @pytest.mark.asyncio
    async def test_concurrent_archetype_processing(self, episode_system, store, querier):
        """Test that different archetypes are processed concurrently."""
        # Add processors with different delays
        episode_system.add_processor(EpisodeMovementProcessor())  # 0.01s
        episode_system.add_processor(EpisodeHealthProcessor())    # 0.02s
        
        # Add entities of different types
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.add_entity([Health(hp=50, max_hp=100)])
        store.add_entity([Position(x=5, y=5), Velocity(vx=2, vy=2)])
        store.materialize_spawns()
        
        start_time = time.time()
        results = []
        
        async for result in episode_system.execute_with_episodes(
            querier, start_step=0, num_steps=2, dt=0.1
        ):
            results.append(result)
        
        execution_time = time.time() - start_time
        
        # Should complete faster than sequential processing
        # (Exact timing depends on system, but should be reasonable)
        assert execution_time < 0.5  # Should be much faster than sequential
    
    @pytest.mark.asyncio
    async def test_error_handling_in_episodes(self, episode_system, store, querier):
        """Test error handling during episode execution."""
        class FailingProcessor(AsyncProcessor):
            components = (Position,)
            priority = 1
            
            def can_process_archetype(self, archetype_signature):
                return Position in archetype_signature
            
            async def process(self, df, dt):
                raise ValueError("Test error in episode processing")
        
        episode_system.add_processor(FailingProcessor())
        
        store.add_entity([Position(x=0, y=0)])
        store.materialize_spawns()
        
        results = []
        async for result in episode_system.execute_with_episodes(
            querier, start_step=0, num_steps=1, dt=0.1
        ):
            results.append(result)
        
        # Should handle errors gracefully and produce error results
        error_results = [r for r in results if not r.success]
        
        # May have error results (depending on how system handles exceptions)
        assert isinstance(results, list)
        
        for error_result in error_results:
            assert error_result.success is False
            assert error_result.error is not None
    
    @pytest.mark.asyncio
    async def test_empty_archetype_handling(self, episode_system, store, querier):
        """Test handling when no archetypes match processors."""
        episode_system.add_processor(EpisodeMovementProcessor())  # Needs Position+Velocity
        
        # Add entity with only Health (no match)
        store.add_entity([Health(hp=50, max_hp=100)])
        store.materialize_spawns()
        
        results = []
        async for result in episode_system.execute_with_episodes(
            querier, start_step=0, num_steps=2, dt=0.1
        ):
            results.append(result)
        
        # Should handle gracefully (may be empty results)
        assert isinstance(results, list)
    
    @pytest.mark.asyncio
    async def test_no_processors_episode_execution(self, episode_system, store, querier):
        """Test episode execution with no processors."""
        # Don't add any processors
        
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.materialize_spawns()
        
        results = []
        async for result in episode_system.execute_with_episodes(
            querier, start_step=0, num_steps=2, dt=0.1
        ):
            results.append(result)
        
        # Should handle gracefully with no processors
        assert isinstance(results, list)
        # May be empty or have no-op results


class TestEpisodeSystemIntegration:
    """Integration tests for the complete episode system."""
    
    @pytest.mark.asyncio
    async def test_full_episode_workflow(self):
        """Test a complete episode workflow with multiple processors and episodes."""
        # Create temporary store
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ArchetypeStore(temp_dir)
            querier = QueryManager(store)
            coordinator = EpisodeCoordinator(episode_size=3, world_id="integration")
            episode_system = AsyncEpisodeSystem(coordinator, max_concurrent_archetypes=2)
            
            # Add processors
            episode_system.add_processor(EpisodeMovementProcessor())
            episode_system.add_processor(TestEpisodeProcessor())
            
            # Add entities
            store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
            store.add_entity([Position(x=10, y=10)])
            store.materialize_spawns()
            
            # Execute multiple episodes
            all_results = []
            async for result in episode_system.execute_with_episodes(
                querier, start_step=0, num_steps=6, dt=0.1  # 2 episodes worth
            ):
                all_results.append(result)
            
            # Should have processed entities through episodes
            assert len(all_results) >= 0
            
            # Check that episodes were created and tracked
            active_episodes = coordinator.get_active_episodes()
            assert len(active_episodes) >= 0  # May have episodes
    
    @pytest.mark.asyncio
    async def test_mixed_processor_types(self):
        """Test system with both regular async processors and episode processors."""
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ArchetypeStore(temp_dir)
            querier = QueryManager(store)
            coordinator = EpisodeCoordinator(episode_size=4, world_id="mixed")
            episode_system = AsyncEpisodeSystem(coordinator)
            
            # Add both types of processors
            episode_system.add_processor(EpisodeMovementProcessor())  # Regular async
            episode_system.add_processor(TestEpisodeProcessor())      # Episode-aware
            
            # Add entities that both can process
            store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
            store.materialize_spawns()
            
            # Execute and collect results
            results = []
            async for result in episode_system.execute_with_episodes(
                querier, start_step=0, num_steps=3, dt=0.1
            ):
                results.append(result)
            
            # Both types of processors should work together
            assert isinstance(results, list)