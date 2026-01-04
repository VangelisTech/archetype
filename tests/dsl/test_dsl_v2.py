# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for DSL v2 - DataFrame-first design.

These tests demonstrate how the new DSL:
1. Preserves DataFrame operations (no collect-and-loop)
2. Compiles behaviors to pure transforms
3. Makes archetypes explicit
4. Separates query from behavior
"""

import pytest

from archetype.core.component import Component
from archetype.dsl.v2 import (
    ArchetypeAccessor,
    Field,
    WorldV2,
    processor,
)


# =============================================================================
# Test Components
# =============================================================================


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Health(Component):
    current: int = 100
    max: int = 100


# =============================================================================
# Test Field Expression Building
# =============================================================================


class TestFieldExpressions:
    """Test that Fields compile to correct Daft expressions."""
    
    def test_field_wraps_column_name(self):
        field = Field("position__x")
        assert field._column_name == "position__x"
    
    def test_field_addition(self):
        field = Field("position__x")
        result = field + 10
        assert hasattr(result, 'to_expr')
    
    def test_field_multiplication(self):
        field = Field("velocity__vx")
        result = field * 2.0
        assert hasattr(result, 'to_expr')
    
    def test_field_comparison(self):
        field = Field("health__current")
        result = field > 50
        assert hasattr(result, 'to_expr')
    
    def test_complex_expression(self):
        x = Field("position__x")
        vx = Field("velocity__vx")
        # position__x + velocity__vx * dt
        result = x + vx * 0.1
        assert hasattr(result, 'to_expr')


class TestArchetypeAccessor:
    """Test component field access through archetype."""
    
    def test_accessor_provides_component_access(self):
        arch = ArchetypeAccessor((Position, Velocity))
        
        # Should provide position and velocity attributes
        assert hasattr(arch.position, '__getattr__')
        assert hasattr(arch.velocity, '__getattr__')
    
    def test_component_field_access(self):
        arch = ArchetypeAccessor((Position, Velocity))
        
        # Access fields
        x_field = arch.position.x
        vx_field = arch.velocity.vx
        
        assert isinstance(x_field, Field)
        assert x_field._column_name == "position__x"
        assert isinstance(vx_field, Field)
        assert vx_field._column_name == "velocity__vx"
    
    def test_missing_component_raises(self):
        arch = ArchetypeAccessor((Position,))
        
        with pytest.raises(AttributeError):
            _ = arch.velocity


# =============================================================================
# Test Processor Decorator
# =============================================================================


class TestProcessorDecorator:
    """Test processor decoration and compilation."""
    
    def test_processor_decorator_creates_spec(self):
        @processor
        class TestMove:
            requires = [Position, Velocity]
            priority = 5
            
            def transform(self, arch, tick, dt):
                return {
                    "position__x": arch.position.x + arch.velocity.vx * dt,
                }
        
        assert TestMove.name == "TestMove"
        assert TestMove.requires == (Position, Velocity)
        assert TestMove.priority == 5
    
    def test_processor_with_filter(self):
        @processor
        class TestMoving:
            requires = [Position, Velocity]
            
            @staticmethod
            def filter(arch):
                return arch.velocity.vx != 0
            
            def transform(self, arch, tick, dt):
                return {}
        
        assert TestMoving.filter_expr is not None


# =============================================================================
# Integration Tests - Full Workflow
# =============================================================================


@pytest.mark.asyncio
async def test_simple_movement_processor():
    """Test that movement processor works with DataFrame operations."""
    
    @processor
    class Move:
        requires = [Position, Velocity]
        priority = 10
        
        def transform(self, arch, tick, dt):
            return {
                "position__x": arch.position.x + arch.velocity.vx * dt,
                "position__y": arch.position.y + arch.velocity.vy * dt,
            }
    
    async with WorldV2("test_move") as world:
        world.register(Move)
        
        # Spawn entities
        await world.spawn(
            Position(x=0.0, y=0.0),
            Velocity(vx=1.0, vy=2.0),
        )
        await world.spawn(
            Position(x=10.0, y=20.0),
            Velocity(vx=-1.0, vy=-2.0),
        )
        
        # Run simulation
        await world.run(ticks=1, dt=1.0)
        
        # Query results
        agents = world.query(Position, Velocity)
        
        assert len(agents) == 2
        
        # Check first agent moved
        agent1 = agents[0]
        # Original: (0, 0) + (1, 2) * 1.0 = (1, 2)
        assert agent1.position.x == 1.0
        assert agent1.position.y == 2.0
        
        # Check second agent moved
        agent2 = agents[1]
        # Original: (10, 20) + (-1, -2) * 1.0 = (9, 18)
        assert agent2.position.x == 9.0
        assert agent2.position.y == 18.0


@pytest.mark.asyncio
async def test_multiple_processors_in_order():
    """Test that processors execute in priority order."""
    
    @processor
    class ApplyVelocity:
        requires = [Position, Velocity]
        priority = 10
        
        def transform(self, arch, tick, dt):
            return {
                "position__x": arch.position.x + arch.velocity.vx * dt,
            }
    
    @processor
    class ApplyFriction:
        requires = [Velocity]
        priority = 20  # Runs after ApplyVelocity
        
        def transform(self, arch, tick, dt):
            return {
                "velocity__vx": arch.velocity.vx * 0.9,
            }
    
    async with WorldV2("test_order") as world:
        world.register(ApplyVelocity)
        world.register(ApplyFriction)
        
        await world.spawn(
            Position(x=0.0, y=0.0),
            Velocity(vx=10.0, vy=0.0),
        )
        
        await world.run(ticks=1, dt=1.0)
        
        agents = world.query(Position, Velocity)
        assert len(agents) == 1
        
        agent = agents[0]
        # Position updated: 0 + 10 * 1 = 10
        assert agent.position.x == 10.0
        # Velocity reduced: 10 * 0.9 = 9.0
        assert agent.velocity.vx == 9.0


@pytest.mark.asyncio
async def test_processor_with_filter():
    """Test that filters compile to DataFrame.where()."""
    
    @processor
    class DamageMovingEntities:
        requires = [Health, Velocity]
        priority = 10
        
        @staticmethod
        def filter(arch):
            # Only damage entities that are moving
            return arch.velocity.vx != 0
        
        def transform(self, arch, tick, dt):
            return {
                "health__current": arch.health.current - 10,
            }
    
    async with WorldV2("test_filter") as world:
        world.register(DamageMovingEntities)
        
        # Spawn moving entity
        await world.spawn(
            Health(current=100, max=100),
            Velocity(vx=5.0, vy=0.0),
        )
        
        # Spawn stationary entity
        await world.spawn(
            Health(current=100, max=100),
            Velocity(vx=0.0, vy=0.0),
        )
        
        await world.run(ticks=1)
        
        agents = world.query(Health, Velocity)
        assert len(agents) == 2
        
        # Find moving vs stationary
        moving = [a for a in agents if a.velocity.vx != 0][0]
        stationary = [a for a in agents if a.velocity.vx == 0][0]
        
        # Moving entity should be damaged
        assert moving.health.current == 90
        
        # Stationary entity should be unchanged
        assert stationary.health.current == 100


@pytest.mark.asyncio
async def test_query_api_readonly():
    """Test that query API provides read-only views."""
    
    async with WorldV2("test_query") as world:
        await world.spawn(
            Position(x=5.0, y=10.0),
            Velocity(vx=1.0, vy=2.0),
        )
        
        agents = world.query(Position, Velocity)
        assert len(agents) == 1
        
        agent = agents[0]
        
        # Can read
        assert agent.position.x == 5.0
        assert agent.velocity.vx == 1.0
        
        # Cannot write (AgentView has no setters)
        with pytest.raises(AttributeError):
            agent.position.x = 100.0


@pytest.mark.asyncio
async def test_multiple_ticks():
    """Test simulation over multiple ticks."""
    
    @processor
    class Accelerate:
        requires = [Velocity]
        priority = 10
        
        def transform(self, arch, tick, dt):
            # Increase velocity each tick
            return {
                "velocity__vx": arch.velocity.vx + 1.0,
            }
    
    async with WorldV2("test_ticks") as world:
        world.register(Accelerate)
        
        await world.spawn(Velocity(vx=0.0, vy=0.0))
        
        # Run 5 ticks
        await world.run(ticks=5)
        
        agents = world.query(Velocity)
        agent = agents[0]
        
        # Should have accelerated 5 times: 0 + 1 + 1 + 1 + 1 + 1 = 5
        assert agent.velocity.vx == 5.0


# =============================================================================
# Comparison Test - Old vs New DSL
# =============================================================================


def test_dsl_comparison():
    """
    Document the key differences between DSL v1 and v2.
    This is a design documentation test, not a runtime test.
    """
    
    # OLD DSL (v1) - Collect and loop
    """
    @behavior
    class Move:
        requires = [Position, Velocity]
        
        async def act(self, agent, world, tick):
            # ❌ Operates on single agent (row-by-row)
            agent.position.x += agent.velocity.vx
            agent.position.y += agent.velocity.vy
    
    Implementation:
    - Collects DataFrame to list
    - Loops through each row
    - Creates AgentProxy per row
    - Tracks mutations
    - Applies mutations via Daft UDF
    
    Problems:
    - Defeats DataFrame batching
    - AgentProxy overhead per row
    - Mutation tracking complexity
    - Can't leverage Daft optimizations
    """
    
    # NEW DSL (v2) - DataFrame first
    """
    @processor
    class Move:
        requires = [Position, Velocity]
        
        def transform(self, arch, tick, dt):
            # ✅ Returns DataFrame expressions
            return {
                "position__x": arch.position.x + arch.velocity.vx * dt,
                "position__y": arch.position.y + arch.velocity.vy * dt,
            }
    
    Implementation:
    - arch.position.x compiles to Field("position__x")
    - Field operations compile to Daft expressions
    - transform() returns dict of column updates
    - Applied as df.with_columns(updates)
    
    Benefits:
    - Pure DataFrame transform
    - Leverages Daft optimization
    - No Python loops
    - Simpler implementation
    - Better performance
    """
    
    assert True  # Documentation test
