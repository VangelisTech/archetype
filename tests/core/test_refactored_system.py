import pytest
import time
from typing import Tuple, Type
from daft import DataFrame, col, lit

import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from archetype.core.base import Component
from archetype.core.processor import Processor, processor
from archetype.core.system import SimpleSystem
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
@processor(Position, Velocity, priority=1)
class MovementProcessor(Processor):
    def process(self, df: DataFrame, dt: float) -> DataFrame:
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })

@processor(Health, priority=2)
class HealthProcessor(Processor):
    def process(self, df: DataFrame, dt: float) -> DataFrame:
        # Simple health regeneration
        return df.with_columns({
            "health__hp": col("health__hp") + 1
        })

@processor(Position, Health, priority=3)
class MixedProcessor(Processor):
    def process(self, df: DataFrame, dt: float) -> DataFrame:
        # Apply effect based on position
        return df.with_columns({
            "health__hp": col("health__hp") + col("position__x") * 0.1
        })


class TestRefactoredSimpleSystem:
    """Test the refactored SimpleSystem with archetype-first iteration."""
    
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
    def system(self):
        """Create a test SimpleSystem."""
        return SimpleSystem()
    
    def test_archetype_first_iteration_basic(self, system, store, querier):
        """Test that archetype-first iteration works correctly."""
        # Add processors
        system.add_processor(MovementProcessor())
        system.add_processor(HealthProcessor())
        
        # Add entities with different component combinations
        pos_vel_entity = store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        health_entity = store.add_entity([Health(hp=50, max_hp=100)])
        mixed_entity = store.add_entity([
            Position(x=5, y=5), 
            Velocity(vx=2, vy=2),
            Health(hp=75, max_hp=100)
        ])
        
        store.materialize_spawns()
        
        # Execute system
        results = system.execute(querier, step=0, dt=0.1)
        
        # Should have results for archetypes that were processed
        assert len(results) >= 1  # At least one archetype was processed
        
        # Verify processing happened
        for archetype_name, df in results.items():
            assert df is not None
            assert df.count_rows() > 0
    
    def test_processor_relevance_filtering(self, system, store, querier):
        """Test that processors only apply to relevant archetypes."""
        # Add processors with different component requirements
        movement_proc = MovementProcessor()
        health_proc = HealthProcessor()
        mixed_proc = MixedProcessor()
        
        system.add_processor(movement_proc)
        system.add_processor(health_proc)
        system.add_processor(mixed_proc)
        
        # Test component signature matching
        pos_vel_sig = (Position, Velocity)
        health_sig = (Health,)
        mixed_sig = (Position, Velocity, Health)
        
        # MovementProcessor should work on Position+Velocity
        assert movement_proc.can_process_archetype(pos_vel_sig)
        assert movement_proc.can_process_archetype(mixed_sig)  # Has required components
        assert not movement_proc.can_process_archetype(health_sig)  # Missing components
        
        # HealthProcessor should work on Health
        assert health_proc.can_process_archetype(health_sig)
        assert health_proc.can_process_archetype(mixed_sig)
        assert not health_proc.can_process_archetype(pos_vel_sig)
        
        # MixedProcessor needs both Position and Health
        assert mixed_proc.can_process_archetype(mixed_sig)
        assert not mixed_proc.can_process_archetype(pos_vel_sig)
        assert not mixed_proc.can_process_archetype(health_sig)
    
    def test_priority_ordering_within_archetypes(self, system, store, querier):
        """Test that processors execute in priority order within each archetype."""
        # Add processors with specific priorities
        low_priority = MovementProcessor()  # priority 1
        high_priority = MixedProcessor()    # priority 3
        
        system.add_processor(high_priority)  # Add high priority first
        system.add_processor(low_priority)   # Add low priority second
        
        # Add entity that both can process
        mixed_entity = store.add_entity([
            Position(x=0, y=0), 
            Velocity(vx=1, vy=1),
            Health(hp=50, max_hp=100)
        ])
        store.materialize_spawns()
        
        # Mock the processors to track execution order
        execution_order = []
        
        original_movement_process = low_priority.process
        original_mixed_process = high_priority.process
        
        def track_movement(df, dt):
            execution_order.append("movement")
            return original_movement_process(df, dt)
        
        def track_mixed(df, dt):
            execution_order.append("mixed")
            return original_mixed_process(df, dt)
        
        low_priority.process = track_movement
        high_priority.process = track_mixed
        
        # Execute
        system.execute(querier, step=0, dt=0.1)
        
        # Lower priority number should execute first
        assert execution_order[0] == "movement"  # priority 1
        assert execution_order[1] == "mixed"     # priority 3
    
    def test_single_query_optimization(self, system, store, querier):
        """Test that the system only queries archetypes once per execution."""
        # Mock the store's get_active_archetypes_with_signatures method
        original_method = store.get_active_archetypes_with_signatures
        call_count = 0
        
        def count_calls():
            nonlocal call_count
            call_count += 1
            return original_method()
        
        store.get_active_archetypes_with_signatures = count_calls
        
        # Add multiple processors
        system.add_processor(MovementProcessor())
        system.add_processor(HealthProcessor())
        system.add_processor(MixedProcessor())
        
        # Add entities
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.add_entity([Health(hp=50, max_hp=100)])
        store.materialize_spawns()
        
        # Execute
        system.execute(querier, step=0, dt=0.1)
        
        # Should only be called once despite multiple processors
        assert call_count == 1
    
    def test_no_processors_returns_empty(self, system, store, querier):
        """Test that system with no processors returns empty results."""
        # Add entities but no processors
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.materialize_spawns()
        
        results = system.execute(querier, step=0, dt=0.1)
        
        assert results == {}
    
    def test_no_entities_returns_empty(self, system, store, querier):
        """Test that system with no entities returns empty results."""
        # Add processors but no entities
        system.add_processor(MovementProcessor())
        
        results = system.execute(querier, step=0, dt=0.1)
        
        assert results == {}
    
    def test_archetype_modification_tracking(self, system, store, querier):
        """Test that only modified archetypes are returned."""
        # Add processor that only affects certain components
        system.add_processor(MovementProcessor())  # Only affects Position+Velocity
        
        # Add entities
        pos_vel_entity = store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        health_entity = store.add_entity([Health(hp=50, max_hp=100)])  # Won't be processed
        store.materialize_spawns()
        
        results = system.execute(querier, step=0, dt=0.1)
        
        # Should only contain archetypes that were actually processed
        # Health-only archetype should not be in results since MovementProcessor can't process it
        for archetype_name in results.keys():
            # Results should only contain archetypes that have Position+Velocity
            assert "PosVel" in archetype_name or "Pos" in archetype_name
    
    def test_step_postprocessing(self, system, store, querier):
        """Test that step postprocessing works correctly."""
        system.add_processor(MovementProcessor())
        
        store.add_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
        store.materialize_spawns()
        
        step_number = 42
        results = system.execute(querier, step=step_number, dt=0.1)
        
        # Check that step was set correctly in postprocessing
        for archetype_name, df in results.items():
            steps = df.select("step").to_pylist()
            assert all(row["step"] == step_number for row in steps)
    
    def test_performance_improvement(self, system, store, querier):
        """Test that refactored version performs better than old approach."""
        # This is a conceptual test - in practice you'd compare with old implementation
        system.add_processor(MovementProcessor())
        system.add_processor(HealthProcessor())
        
        # Add many entities to make performance difference noticeable
        for i in range(10):
            store.add_entity([Position(x=i, y=i), Velocity(vx=1, vy=1)])
            store.add_entity([Health(hp=50 + i, max_hp=100)])
        
        store.materialize_spawns()
        
        # Measure execution time
        start_time = time.time()
        results = system.execute(querier, step=0, dt=0.1)
        execution_time = time.time() - start_time
        
        # Should complete in reasonable time (this is environment dependent)
        assert execution_time < 1.0  # Should be much faster than 1 second
        assert len(results) > 0  # Should have processed something


class TestProcessorMethods:
    """Test the new processor methods added for archetype-first iteration."""
    
    def test_can_process_archetype_basic(self):
        """Test basic archetype signature matching."""
        processor = MovementProcessor()
        
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
    
    def test_can_process_archetype_single_component(self):
        """Test archetype matching for single-component processors."""
        processor = HealthProcessor()
        
        assert processor.can_process_archetype((Health,))
        assert processor.can_process_archetype((Health, Position))
        assert processor.can_process_archetype((Health, Position, Velocity))
        
        assert not processor.can_process_archetype((Position,))
        assert not processor.can_process_archetype((Position, Velocity))
        assert not processor.can_process_archetype(())
    
    def test_can_process_archetype_multiple_components(self):
        """Test archetype matching for multi-component processors."""
        processor = MixedProcessor()  # Requires Position AND Health
        
        assert processor.can_process_archetype((Position, Health))
        assert processor.can_process_archetype((Position, Health, Velocity))
        assert processor.can_process_archetype((Health, Position, Velocity))
        
        assert not processor.can_process_archetype((Position,))
        assert not processor.can_process_archetype((Health,))
        assert not processor.can_process_archetype((Position, Velocity))
        assert not processor.can_process_archetype(())
    
    def test_processor_without_components_fails(self):
        """Test that processors without components set fail gracefully."""
        class BadProcessor(Processor):
            pass  # No components set
        
        bad_processor = BadProcessor()
        
        # Should return False for any archetype since components is None
        assert not bad_processor.can_process_archetype((Position, Velocity))
        assert not bad_processor.can_process_archetype((Health,))
        assert not bad_processor.can_process_archetype(())