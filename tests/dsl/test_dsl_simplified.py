# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for the simplified DataFrame-first DSL.
"""

import pytest
from archetype.core.component import Component
from archetype.dsl import World, behavior, when, Field


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Health(Component):
    current: int = 100
    max: int = 100


def test_field_creation():
    """Test basic field creation."""
    field = Field("position__x")
    assert field._column_name == "position__x"
    assert hasattr(field.to_expr(), '__call__') or hasattr(field.to_expr(), '_data')


@pytest.mark.asyncio
async def test_simple_behavior():
    """Test basic behavior."""
    
    @behavior
    class Move:
        requires = [Position, Velocity]
        
        def transform(self, arch, tick, dt):
            return {
                "position__x": arch.position.x + arch.velocity.vx * dt,
            }
    
    async with World("test") as world:
        world.register(Move)
        await world.spawn(Position(x=0.0), Velocity(vx=1.0))
        await world.run(ticks=1, dt=1.0)
        
        agents = world.query(Position)
        assert len(agents) == 1
        assert agents[0].position.x == 1.0
