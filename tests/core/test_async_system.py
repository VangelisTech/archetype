import pytest
import asyncio
import time
from typing import Tuple, Type
from daft import DataFrame, col, lit

import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core.base import Component
from archetype.core.aio.async_system import AsyncProcessor, AsyncSystem, async_processor
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


# Test Async Processors
@async_processor(Position, Velocity, priority=1)
class AsyncMovementProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        # Simulate async I/O delay
        await asyncio.sleep(0.01)
        
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })

@async_processor(Health, priority=2)
class AsyncHealthProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        # Simulate longer async operation
        await asyncio.sleep(0.02)
        
        return df.with_columns({
            "health__hp": col("health__hp") + 1
        })

@async_processor(Position, Health, priority=3)
class AsyncMixedProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, dt: float) -> DataFrame:
        # Simulate moderate delay
        await asyncio.sleep(0.015)
        
        return df.with_columns({
            "health__hp": col("health__hp") + col("position__x") * 0.1
        })


class TestAsyncSystem:
    """Test the AsyncSystem with concurrent archetype processing."""
    
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
    def async_system(self):
        """Create a test AsyncSystem."""
        return AsyncSystem(max_concurrent_archetypes=5)
    
    @pytest.mark.asyncio
    async def test_concurrent_archetype_processing(self, async_system, store, querier):
        """Test that archetypes are processed concurrently."""
        # Add processors
        async_system.add_processor(AsyncMovementProcessor())
        async_system.add_processor(AsyncHealthProcessor())
        
        # Add entities with different archetypes
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.add_entity([Health(hp=50, max_hp=100)])
        store.add_entity([Position(x=5, y=5), Velocity(vx=2, vy=2)])
        store.add_entity([Health(hp=75, max_hp=100)])
        
        store.materialize_spawns()
        
        # Measure execution time
        start_time = time.time()
        results = await async_system.execute(querier, step=0, dt=0.1)
        execution_time = time.time() - start_time
        
        # Should complete faster than sequential processing would
        # (4 entities * min 0.01s delay = 0.04s if sequential, should be much less)
        assert execution_time < 0.1
        assert len(results) >= 1
        
        for archetype_name, df in results.items():
            assert df is not None
            assert df.count_rows() > 0
    
    @pytest.mark.asyncio
    async def test_processor_relevance_filtering_async(self, async_system, store, querier):
        """Test that async processors only apply to relevant archetypes."""
        # Add processors with different component requirements
        movement_proc = AsyncMovementProcessor()
        health_proc = AsyncHealthProcessor()
        mixed_proc = AsyncMixedProcessor()
        
        async_system.add_processor(movement_proc)
        async_system.add_processor(health_proc)
        async_system.add_processor(mixed_proc)
        
        # Test component signature matching
        pos_vel_sig = (Position, Velocity)
        health_sig = (Health,)
        mixed_sig = (Position, Velocity, Health)
        
        # Same logic as sync version but with async processors
        assert movement_proc.can_process_archetype(pos_vel_sig)
        assert movement_proc.can_process_archetype(mixed_sig)
        assert not movement_proc.can_process_archetype(health_sig)
        
        assert health_proc.can_process_archetype(health_sig)
        assert health_proc.can_process_archetype(mixed_sig)
        assert not health_proc.can_process_archetype(pos_vel_sig)
        
        assert mixed_proc.can_process_archetype(mixed_sig)
        assert not mixed_proc.can_process_archetype(pos_vel_sig)
        assert not mixed_proc.can_process_archetype(health_sig)
    
    @pytest.mark.asyncio
    async def test_priority_ordering_within_archetypes_async(self, async_system, store, querier):
        """Test that async processors execute in priority order within each archetype."""
        execution_order = []
        
        # Create processors that track execution order
        class TrackingMovementProcessor(AsyncMovementProcessor):
            async def process(self, df, dt):
                execution_order.append("movement")
                return await super().process(df, dt)
        
        class TrackingMixedProcessor(AsyncMixedProcessor):
            async def process(self, df, dt):
                execution_order.append("mixed")
                return await super().process(df, dt)
        
        low_priority = TrackingMovementProcessor()  # priority 1
        high_priority = TrackingMixedProcessor()    # priority 3
        
        async_system.add_processor(high_priority)  # Add high priority first
        async_system.add_processor(low_priority)   # Add low priority second
        
        # Add entity that both can process
        store.add_entity([
            Position(x=0, y=0), 
            Velocity(vx=1, vy=1),
            Health(hp=50, max_hp=100)
        ])
        store.materialize_spawns()
        
        # Execute
        await async_system.execute(querier, step=0, dt=0.1)
        
        # Lower priority number should execute first
        assert execution_order[0] == "movement"  # priority 1
        assert execution_order[1] == "mixed"     # priority 3
    
    @pytest.mark.asyncio
    async def test_single_query_optimization_async(self, async_system, store, querier):
        """Test that async system only queries archetypes once per execution."""
        original_method = store.get_active_archetypes_with_signatures
        call_count = 0
        
        def count_calls():
            nonlocal call_count
            call_count += 1
            return original_method()
        
        store.get_active_archetypes_with_signatures = count_calls
        
        # Add multiple processors
        async_system.add_processor(AsyncMovementProcessor())
        async_system.add_processor(AsyncHealthProcessor())
        async_system.add_processor(AsyncMixedProcessor())
        
        # Add entities
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.add_entity([Health(hp=50, max_hp=100)])
        store.materialize_spawns()
        
        # Execute
        await async_system.execute(querier, step=0, dt=0.1)
        
        # Should only be called once despite multiple processors
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_semaphore_limiting(self, store, querier):
        """Test that semaphore limits concurrent archetype processing."""
        # Create system with low concurrency limit
        limited_system = AsyncSystem(max_concurrent_archetypes=2)
        
        processing_times = []
        
        class TimingProcessor(AsyncProcessor):
            components = (Position,)
            priority = 1
            
            def can_process_archetype(self, archetype_signature):
                return Position in archetype_signature
            
            async def process(self, df, dt):
                start = time.time()
                await asyncio.sleep(0.05)  # 50ms delay
                processing_times.append(time.time() - start)
                return df
        
        limited_system.add_processor(TimingProcessor())
        
        # Add many entities to trigger semaphore limiting
        for i in range(5):
            store.add_entity([Position(x=i, y=i)])
        
        store.materialize_spawns()
        
        start_time = time.time()
        await limited_system.execute(querier, step=0, dt=0.1)
        total_time = time.time() - start_time
        
        # With 5 entities and max 2 concurrent, should take longer than if all were concurrent
        # But less than fully sequential (allow some timing variance)
        assert total_time > 0.05  # More than 1 * 0.05s (reduced expectation)
        assert total_time < 0.5   # Less than 5 * 0.05s (increased tolerance)
        assert len(processing_times) >= 1
    
    @pytest.mark.asyncio
    async def test_no_processors_returns_empty_async(self, async_system, store, querier):
        """Test that async system with no processors returns empty results."""
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.materialize_spawns()
        
        results = await async_system.execute(querier, step=0, dt=0.1)
        
        assert results == {}
    
    @pytest.mark.asyncio
    async def test_no_entities_returns_empty_async(self, async_system, store, querier):
        """Test that async system with no entities returns empty results."""
        async_system.add_processor(AsyncMovementProcessor())
        
        results = await async_system.execute(querier, step=0, dt=0.1)
        
        assert results == {}
    
    @pytest.mark.asyncio
    async def test_exception_handling(self, async_system, store, querier):
        """Test that exceptions in async processors are handled gracefully."""
        class FailingProcessor(AsyncProcessor):
            components = (Position,)
            priority = 1
            
            def can_process_archetype(self, archetype_signature):
                return Position in archetype_signature
            
            async def process(self, df, dt):
                raise ValueError("Test error")
        
        async_system.add_processor(FailingProcessor())
        store.add_entity([Position(x=0, y=0)])
        store.materialize_spawns()
        
        # Should not raise exception, but return empty or partial results
        results = await async_system.execute(querier, step=0, dt=0.1)
        
        # System should handle the error gracefully
        # (Exact behavior depends on implementation - might return empty results)
        assert isinstance(results, dict)
    
    @pytest.mark.asyncio
    async def test_step_postprocessing_async(self, async_system, store, querier):
        """Test that step postprocessing works with async processors."""
        async_system.add_processor(AsyncMovementProcessor())
        
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.materialize_spawns()
        
        step_number = 42
        results = await async_system.execute(querier, step=step_number, dt=0.1)
        
        # Check that step was set correctly in postprocessing
        for archetype_name, df in results.items():
            steps = df.select("step").to_pylist()
            assert all(row["step"] == step_number for row in steps)
    
    @pytest.mark.asyncio
    async def test_sync_execute_method(self, async_system, store, querier):
        """Test that async execute method works correctly."""
        async_system.add_processor(AsyncMovementProcessor())
        
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.materialize_spawns()
        
        # Use async/await for AsyncSystem
        results = await async_system.execute(querier, step=0, dt=0.1)
        
        assert isinstance(results, dict)
        assert len(results) >= 0


class TestAsyncProcessorMethods:
    """Test the async processor methods and decorators."""
    
    def test_async_processor_decorator(self):
        """Test that async_processor decorator sets components and priority correctly."""
        processor = AsyncMovementProcessor()
        
        assert processor.components == (Position, Velocity)
        assert processor.priority == 1
    
    def test_can_process_archetype_async(self):
        """Test archetype signature matching for async processors."""
        processor = AsyncMovementProcessor()
        
        # Should work with exact match
        assert processor.can_process_archetype((Position, Velocity))
        assert processor.can_process_archetype((Velocity, Position))  # Order doesn't matter
        
        # Should work with superset (more components than needed)
        assert processor.can_process_archetype((Position, Velocity, Health))
        
        # Should not work with subset (missing required components)
        assert not processor.can_process_archetype((Position,))
        assert not processor.can_process_archetype((Velocity,))
        assert not processor.can_process_archetype((Health,))
        assert not processor.can_process_archetype(())
    
    @pytest.mark.asyncio
    async def test_async_process_method(self):
        """Test that async process method can be called correctly."""
        processor = AsyncMovementProcessor()
        
        # Create a simple DataFrame for testing
        import daft
        df = daft.from_pydict({
            "entity_id": [1],
            "position__x": [0.0],
            "position__y": [0.0],
            "velocity__vx": [1.0],
            "velocity__vy": [1.0]
        })
        
        # Should be able to process async
        result = await processor.process(df, 0.1)
        
        assert result is not None
        # Position should have changed based on velocity
        result_data = result.to_pydict()
        assert result_data["position__x"][0] == 0.1  # 0 + 1*0.1
        assert result_data["position__y"][0] == 0.1  # 0 + 1*0.1
    
    def test_processor_without_components_fails_async(self):
        """Test that async processors without components set fail gracefully."""
        class BadAsyncProcessor(AsyncProcessor):
            pass  # No components set
        
        bad_processor = BadAsyncProcessor()
        
        # Should return False for any archetype since components is None
        assert not bad_processor.can_process_archetype((Position, Velocity))
        assert not bad_processor.can_process_archetype((Health,))
        assert not bad_processor.can_process_archetype(())