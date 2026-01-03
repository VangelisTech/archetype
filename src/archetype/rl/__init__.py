# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype RL
============

Reinforcement learning primitives for the Archetype framework.

Components:
    - Observation, PolicyState, Reward: Core RL components for DSL behaviors
    - ActionProposal, ChosenAction: Multi-planner arbitration
    - TrainingState: GRPO/PPO training artifacts

GRPO:
    - compute_grpo_loss: Pure PyTorch GRPO loss computation
    - GrpoLossConfig: Configuration for clipping and KL penalty

Usage with DSL:
    from archetype.dsl import World, behavior, spawn_world
    from archetype.rl import Observation, PolicyState, Reward
    from archetype.rl.grpo import compute_grpo_loss

    @behavior
    class PolicyRollout:
        requires = [Observation, PolicyState]
        
        async def act(self, agent, world, tick):
            response = await world.prompt(
                agent.observation.raw_text,
                model="vllm://model",
                return_logprobs=True,
            )
            agent.policy_state.completion = response.text
            agent.policy_state.logprobs_json = json.dumps(response.logprobs)

    @behavior
    class MCTS_Exploration:
        requires = [PolicyState]
        runs_on = "final_tick"
        
        async def act(self, agent, world, tick):
            # Use spawn_world for action-sequence exploration
            async with spawn_world("rollout", parent=world) as inner:
                await inner.run(ticks=5)
                score = inner.agents[0].reward.value
"""

from archetype.rl.components import (
    ActionProposal,
    ChosenAction,
    Observation,
    PolicyState,
    Reward,
    TrainingState,
)
from archetype.rl.grpo import (
    GrpoLossConfig,
    compute_grpo_loss,
    forward_logprobs_for_causal_lm,
)

__all__ = [
    # Components
    "Observation",
    "PolicyState",
    "Reward",
    "ActionProposal",
    "ChosenAction",
    "TrainingState",
    # GRPO
    "GrpoLossConfig",
    "compute_grpo_loss",
    "forward_logprobs_for_causal_lm",
]
