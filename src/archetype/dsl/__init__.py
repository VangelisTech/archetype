# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype Agent DSL
===================

An ergonomic, agent-centric DSL that compiles to the DataFrame/Processor engine.

Features:
- Agent-centric access: agent.perspective.name
- Declarative behaviors: @behavior with runs_on, filter
- Auto message realization when Inbox present
- spawn_world() for inner simulations / MCTS / counterfactual reasoning
- parallel_rollouts() for exploring multiple scenarios

Usage:
    from archetype.dsl import World, behavior, spawn_world

    @behavior
    class Debater:
        requires = [Perspective, DebateState]
        
        async def act(self, agent, world, tick):
            response = await world.prompt(...)
            agent.debate_state.history.append(response)
            await world.broadcast(response, exclude=[agent])

    async with World("my_sim") as world:
        world.add_behavior(Debater)
        await world.spawn(Perspective(...), DebateState())
        await world.run(ticks=3)
    
    # Inner simulation / MCTS
    async with spawn_world("scenario", parent=world) as inner:
        await inner.spawn(...)
        await inner.run(ticks=5)
        result = analyze(inner.agents)
"""

from archetype.dsl.core import (
    World,
    AgentProxy,
    behavior,
    BehaviorSpec,
    spawn_world,
    parallel_rollouts,
    RolloutResult,
)
from archetype.dsl.primitives import broadcast, Inbox

__all__ = [
    # Core
    "World",
    "AgentProxy", 
    "behavior",
    "BehaviorSpec",
    # Inner simulation / MCTS
    "spawn_world",
    "parallel_rollouts",
    "RolloutResult",
    # Primitives
    "broadcast",
    "Inbox",
]
