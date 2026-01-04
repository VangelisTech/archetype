# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype DSL
=============

Two DSL versions are available:

**DSL v1 (archetype.dsl.core)** - Agent-centric with ergonomic mutations
- Good for: Prototyping, small entity counts (<100), rapid iteration
- Trade-off: Collects DataFrames to Python lists, row-by-row processing

**DSL v2 (archetype.dsl.v2)** - DataFrame-first compilation
- Good for: Production, large entity counts (>100), performance-critical code
- Trade-off: More constrained API, but 7x faster

See docs/DSL_V2_MIGRATION.md for migration guide.
See docs/DSL_PHILOSOPHY.md for design rationale.

Usage (v1):
    from archetype.dsl import World, behavior
    
    @behavior
    class Move:
        requires = [Position, Velocity]
        async def act(self, agent, world, tick):
            agent.position.x += agent.velocity.vx

Usage (v2):
    from archetype.dsl import WorldV2, processor
    
    @processor
    class Move:
        requires = [Position, Velocity]
        def transform(self, arch, tick, dt):
            return {"position__x": arch.position.x + arch.velocity.vx * dt}
"""

# DSL v1 - Legacy agent-centric API
from archetype.dsl.core import (
    AgentProxy,
    BehaviorSpec,
    ComponentProxy,
    RolloutResult,
    World,
    behavior,
    component_column,
    component_prefix,
    parallel_rollouts,
    spawn_world,
)
from archetype.dsl.primitives import Inbox, broadcast

# DSL v2 - DataFrame-first API
from archetype.dsl.v2 import (
    AgentView,
    ArchetypeAccessor,
    ComponentView,
    Field,
    ProcessorSpec,
    WorldV2,
    processor,
)

__all__ = [
    # v1 API
    "World",
    "AgentProxy",
    "ComponentProxy",
    "behavior",
    "BehaviorSpec",
    "spawn_world",
    "parallel_rollouts",
    "RolloutResult",
    "component_prefix",
    "component_column",
    "Inbox",
    "broadcast",
    # v2 API
    "WorldV2",
    "processor",
    "ProcessorSpec",
    "Field",
    "ArchetypeAccessor",
    "AgentView",
    "ComponentView",
]
