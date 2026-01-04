# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype DSL
=============

A DataFrame-first DSL that honors the core engine by compiling behaviors
to pure DataFrame transforms.

Features:
- Agent-centric syntax: arch.position.x
- Compiles to DataFrame operations: col("position__x")
- Conditional logic with when/otherwise
- spawn_world() for inner simulations / MCTS

Usage:
    from archetype.dsl import World, behavior, when
    
    @behavior
    class Move:
        requires = [Position, Velocity]
        
        def transform(self, arch, tick, dt):
            return {
                "position__x": arch.position.x + arch.velocity.vx * dt,
                "position__y": arch.position.y + arch.velocity.vy * dt,
            }
    
    async with World("sim") as world:
        world.register(Move)
        await world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=2))
        await world.run(ticks=10, dt=1.0)
"""

from archetype.dsl.core import (
    AgentView,
    ArchetypeAccessor,
    BehaviorSpec,
    ComponentView,
    Field,
    World,
    behavior,
    spawn_world,
    when,
)
from archetype.dsl.primitives import Inbox

__all__ = [
    "World",
    "behavior",
    "BehaviorSpec",
    "spawn_world",
    "when",
    "Field",
    "ArchetypeAccessor",
    "AgentView",
    "ComponentView",
    "Inbox",
]
