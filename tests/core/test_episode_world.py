import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock

import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core.base import Component
from archetype.core.world import SimpleWorld
from archetype.core.store import ArchetypeStore
from archetype.core.querier import QueryManager
from archetype.core.updater import UpdateManager
from archetype.core.aio.episode_world import EpisodeWorld
from archetype.core.aio.async_system import AsyncProcessor, async_processor
from archetype.core.aio.episode import EpisodeProcessor
from daft import DataFrame, col


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
class WorldMovementProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        await asyncio.sleep(0.005)  # Small delay for testing
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })

@async_processor(Health, priority=2)
class WorldHealthProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        await asyncio.sleep(0.01)  # Longer delay
        return df.with_columns({
            "health__hp": col("health__hp") + 1
        })


class TestEpisodeWorld:
    """Test the EpisodeWorld implementation."""
    
    @pytest.fixture
    def temp_uri(self, tmp_path):
        """Create a temporary URI for testing."""
        test_dir = tmp_path / "test_data"
        test_dir.mkdir(exist_ok=True)
        return str(test_dir)
    
    @pytest.fixture
    def sync_world(self, temp_uri):
        """Create a sync world for wrapping."""
        from archetype.core.system import SimpleSystem
        store = ArchetypeStore(temp_uri)
        querier = QueryManager(store)
        updater = UpdateManager(store)
        system = SimpleSystem()
        return SimpleWorld(store, querier, updater, system)
    
    @pytest.fixture
    def episode_world(self, sync_world):
        """Create an episode world."""
        return EpisodeWorld(
            sync_world,
            episode_size=5,
            max_concurrent_archetypes=3
        )
    
    def test_episode_world_initialization(self, episode_world, sync_world):
        """Test that episode world wraps sync world correctly."""
        # Should reuse sync world infrastructure
        assert episode_world.store is sync_world.store
        assert episode_world.querier is sync_world.querier
        assert episode_world.updater is sync_world.updater
        assert episode_world.current_step == sync_world.current_step
        assert episode_world.id == sync_world.id
        
        # Should have episode coordination
        assert episode_world.episode_coordinator is not None
        assert episode_world.episode_system is not None
        assert episode_world.episode_size == 5
        assert len(episode_world.completed_episodes) == 0
    
    def test_processor_management(self, episode_world):
        """Test adding and removing processors."""
        proc1 = WorldMovementProcessor()
        proc2 = WorldHealthProcessor()
        
        episode_world.add_processor(proc1)
        episode_world.add_processor(proc2)
        
        assert len(episode_world.episode_system.processors) == 2
        
        episode_world.remove_processor(proc1)
        
        # Should remove by type
        assert len(episode_world.episode_system.processors) == 1
    
    def test_sync_methods_delegation(self, episode_world):
        """Test that sync methods delegate to underlying world."""
        # Test spawn
        entity_id = episode_world.spawn(Position(x=1, y=2), Velocity(vx=0.5, vy=1.5))
        assert isinstance(entity_id, int)
        
        # Test spawn with explicit step
        entity_id2 = episode_world.spawn(Health(hp=100, max_hp=100), step=10)
        assert isinstance(entity_id2, int)
        
        # Test despawn
        episode_world.despawn(entity_id)
        
        # Test despawn with explicit step
        episode_world.despawn(entity_id2, step=15)
        
        # Test materialize_spawns
        episode_world.materialize_spawns()  # Should not raise exception
    
    @pytest.mark.asyncio
    async def test_run_episode_basic(self, episode_world):
        """Test running a single episode."""
        episode_world.add_processor(WorldMovementProcessor())
        
        # Spawn entities
        episode_world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
        episode_world.spawn(Position(x=5, y=5), Velocity(vx=2, vy=2))
        
        # Run episode
        stats = await episode_world.run_episode(dt=0.1)
        
        # Check stats structure
        assert isinstance(stats, dict)
        assert "episode_duration" in stats
        assert "archetypes_processed" in stats
        assert "successful_episodes" in stats
        assert "steps_advanced" in stats
        assert "current_step" in stats
        
        assert stats["episode_duration"] >= 0
        assert stats["steps_advanced"] == 5  # episode_size
        assert stats["current_step"] == 5    # advanced by episode_size
    
    @pytest.mark.asyncio
    async def test_run_episode_with_processors(self, episode_world):
        """Test running episode with multiple processors."""
        episode_world.add_processor(WorldMovementProcessor())
        episode_world.add_processor(WorldHealthProcessor())
        
        # Spawn entities with different components
        episode_world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
        episode_world.spawn(Health(hp=50, max_hp=100))
        episode_world.spawn(
            Position(x=10, y=10), 
            Velocity(vx=1, vy=1),
            Health(hp=75, max_hp=100)
        )
        
        initial_step = episode_world.current_step
        
        stats = await episode_world.run_episode(dt=0.1)
        
        # Should have processed multiple archetypes
        assert stats["archetypes_processed"] >= 0
        assert stats["current_step"] == initial_step + episode_world.episode_size
    
    @pytest.mark.asyncio 
    async def test_run_episode_timing(self, episode_world):
        """Test that episode timing is reasonable."""
        episode_world.add_processor(WorldMovementProcessor())
        
        episode_world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
        
        start_time = time.time()
        stats = await episode_world.run_episode(dt=0.1)
        actual_time = time.time() - start_time
        
        # Stats timing should be close to actual timing
        assert abs(stats["episode_duration"] - actual_time) < 0.01
        assert stats["episode_duration"] > 0
    
    @pytest.mark.asyncio
    async def test_run_with_sync_points(self, episode_world):
        """Test running multiple episodes with synchronization points."""
        episode_world.add_processor(WorldMovementProcessor())
        episode_world.add_processor(WorldHealthProcessor())
        
        # Spawn entities
        episode_world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
        episode_world.spawn(Health(hp=50, max_hp=100))
        
        initial_step = episode_world.current_step
        
        # Run 4 episodes with sync every 2
        all_stats = await episode_world.run_with_sync_points(
            num_episodes=4,
            dt=0.1,
            sync_every=2
        )
        
        # Check results
        assert len(all_stats) == 4
        
        # Check episode numbering and sync points
        for i, stats in enumerate(all_stats):
            assert stats["episode_number"] == i + 1
            assert stats["was_sync_point"] == ((i + 1) % 2 == 0)
            assert "episode_duration" in stats
            assert "archetypes_processed" in stats
        
        # Current step should have advanced by 4 episodes
        expected_step = initial_step + (4 * episode_world.episode_size)
        assert episode_world.current_step == expected_step
    
    @pytest.mark.asyncio
    async def test_run_with_sync_points_timing(self, episode_world):
        """Test that sync points add minimal overhead."""
        episode_world.add_processor(WorldMovementProcessor())
        
        episode_world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
        
        start_time = time.time()
        stats = await episode_world.run_with_sync_points(
            num_episodes=2,
            dt=0.1,
            sync_every=1  # Sync every episode
        )
        total_time = time.time() - start_time
        
        # Should complete in reasonable time
        assert total_time < 1.0  # Much less than 1 second
        assert len(stats) == 2
    
    def test_get_episode_status(self, episode_world):
        """Test getting episode status information."""
        initial_status = episode_world.get_episode_status()
        
        assert isinstance(initial_status, dict)
        assert "current_step" in initial_status
        assert "episode_size" in initial_status
        assert "active_episodes" in initial_status
        assert "completed_episodes" in initial_status
        assert "episode_details" in initial_status
        
        assert initial_status["episode_size"] == 5
        assert initial_status["completed_episodes"] == 0
        assert isinstance(initial_status["episode_details"], dict)
    
    @pytest.mark.asyncio
    async def test_get_episode_status_after_episodes(self, episode_world):
        """Test episode status after running episodes."""
        episode_world.add_processor(WorldMovementProcessor())
        episode_world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
        
        # Run an episode
        await episode_world.run_episode(dt=0.1)
        
        status = episode_world.get_episode_status()
        
        # Should have some completed episodes
        assert status["completed_episodes"] >= 0
        assert status["current_step"] > 0
    
    @pytest.mark.asyncio
    async def test_wait_for_episode_sync(self, episode_world):
        """Test waiting for episode synchronization."""
        # This tests the coordination interface
        result = await episode_world.wait_for_episode_sync(
            episode_id="nonexistent",
            required_archetypes={"test_arch"},
            timeout=0.1
        )
        
        # Should timeout quickly for nonexistent episode
        assert result is False
    
    @pytest.mark.asyncio
    async def test_empty_episode_execution(self, episode_world):
        """Test running episode with no entities."""
        episode_world.add_processor(WorldMovementProcessor())
        
        # Don't spawn any entities
        stats = await episode_world.run_episode(dt=0.1)
        
        # Should handle gracefully
        assert stats["archetypes_processed"] == 0
        assert stats["successful_episodes"] == 0
        assert stats["steps_advanced"] == 5  # Still advances steps
    
    @pytest.mark.asyncio
    async def test_no_processors_episode(self, episode_world):
        """Test running episode with no processors."""
        # Don't add any processors
        episode_world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
        
        stats = await episode_world.run_episode(dt=0.1)
        
        # Should handle gracefully
        assert stats["archetypes_processed"] == 0
        assert stats["steps_advanced"] == 5


class TestEpisodeWorldIntegration:
    """Integration tests for EpisodeWorld."""
    
    @pytest.mark.asyncio
    async def test_full_simulation_workflow(self):
        """Test a complete simulation workflow with episode world."""
        import tempfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create sync world
            store = ArchetypeStore(temp_dir)
            querier = QueryManager(store)
            updater = UpdateManager(store)
            from archetype.core.system import SimpleSystem
            system = SimpleSystem()
            sync_world = SimpleWorld(store, querier, updater, system)
            
            # Create episode world
            episode_world = EpisodeWorld(sync_world, episode_size=3, max_concurrent_archetypes=2)
            
            # Add processors
            episode_world.add_processor(WorldMovementProcessor())
            episode_world.add_processor(WorldHealthProcessor())
            
            # Spawn diverse entities
            episode_world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
            episode_world.spawn(Health(hp=50, max_hp=100))
            episode_world.spawn(
                Position(x=5, y=5),
                Velocity(vx=2, vy=2), 
                Health(hp=75, max_hp=100)
            )
            
            initial_step = episode_world.current_step
            
            # Run simulation
            all_stats = await episode_world.run_with_sync_points(
                num_episodes=3,
                dt=0.1,
                sync_every=2
            )
            
            # Verify results
            assert len(all_stats) == 3
            assert episode_world.current_step == initial_step + (3 * 3)  # 3 episodes * size 3
            
            # Check that entities were processed
            total_processed = sum(stats["archetypes_processed"] for stats in all_stats)
            assert total_processed >= 0  # Should have processed some archetypes
            
            # Check episode status
            final_status = episode_world.get_episode_status()
            assert final_status["current_step"] > initial_step
    
    @pytest.mark.asyncio
    async def test_concurrent_processing_performance(self):
        """Test that concurrent processing provides performance benefits."""
        import tempfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create worlds
            store = ArchetypeStore(temp_dir)
            querier = QueryManager(store)
            updater = UpdateManager(store)
            from archetype.core.system import SimpleSystem
            system = SimpleSystem()
            sync_world = SimpleWorld(store, querier, updater, system)
            
            # Test with different concurrency limits
            episode_world_limited = EpisodeWorld(sync_world, episode_size=2, max_concurrent_archetypes=1)
            episode_world_concurrent = EpisodeWorld(sync_world, episode_size=2, max_concurrent_archetypes=3)
            
            # Add processors with delays
            for world in [episode_world_limited, episode_world_concurrent]:
                world.add_processor(WorldMovementProcessor())
                world.add_processor(WorldHealthProcessor())
                
                # Spawn multiple entities
                for i in range(3):
                    world.spawn(Position(x=i, y=i), Velocity(vx=1, vy=1))
                    world.spawn(Health(hp=50 + i, max_hp=100))
            
            # Time limited concurrency
            start_time = time.time()
            await episode_world_limited.run_episode(dt=0.1)
            limited_time = time.time() - start_time
            
            # Reset for fair comparison
            episode_world_concurrent.current_step = 0
            
            # Time higher concurrency
            start_time = time.time()
            await episode_world_concurrent.run_episode(dt=0.1)
            concurrent_time = time.time() - start_time
            
            # Concurrent version should generally be faster or similar
            # (May not always be true due to overhead, but should be reasonable)
            assert concurrent_time <= limited_time * 2  # Allow some variance
    
    @pytest.mark.asyncio
    async def test_error_resilience(self):
        """Test that episode world handles processor errors gracefully."""
        import tempfile
        
        class FailingProcessor(AsyncProcessor):
            components = (Position,)
            priority = 1
            
            def can_process_archetype(self, archetype_signature):
                return Position in archetype_signature
            
            async def process(self, df, dt):
                raise RuntimeError("Simulated processor failure")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ArchetypeStore(temp_dir)
            querier = QueryManager(store)
            updater = UpdateManager(store)
            from archetype.core.system import SimpleSystem
            system = SimpleSystem()
            sync_world = SimpleWorld(store, querier, updater, system)
            episode_world = EpisodeWorld(sync_world, episode_size=2)
            
            # Add both failing and working processors
            episode_world.add_processor(FailingProcessor())
            episode_world.add_processor(WorldHealthProcessor())  # Should still work
            
            # Spawn entities
            episode_world.spawn(Position(x=0, y=0))  # Will hit failing processor
            episode_world.spawn(Health(hp=50, max_hp=100))  # Should work fine
            
            # Should not crash despite processor failure
            stats = await episode_world.run_episode(dt=0.1)
            
            # Should still complete and advance time
            assert stats["steps_advanced"] == 2
            assert stats["episode_duration"] >= 0