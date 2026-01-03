# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
RL Components
=============

Core ECS components for reinforcement learning agents.
These compose with the DSL's @behavior decorator and spawn_world().

Agent Architecture:
    Observation → Policy → Action → Outcome

Usage with DSL:
    @behavior
    class PolicyRollout:
        requires = [Observation, PolicyState]
        
        async def act(self, agent, world, tick):
            obs = agent.observation.raw_text
            response = await world.prompt(obs, model="...")
            agent.policy_state.completion = response
"""

from archetype.core import Component


# =============================================================================
# Core RL Components
# =============================================================================


class Observation(Component):
    """
    What the agent perceives from the environment.
    
    Attributes:
        features_json: Structured features as JSON string
        raw_text: Raw text observation for LLM-based agents
    """
    features_json: str = "{}"
    raw_text: str = ""


class PolicyState(Component):
    """
    Current policy state and generated outputs.
    
    Attributes:
        completion: Generated text from policy
        completions_json: Multiple completions as JSON list (for GRPO K samples)
        logprobs_json: Token logprobs as JSON list
        policy_type: Identifier for policy variant
    """
    completion: str = ""
    completions_json: str = "[]"
    logprobs_json: str = "[]"
    policy_type: str = "default"


class Reward(Component):
    """
    Reward signal for training.
    
    Attributes:
        value: Scalar reward
        advantage: Computed advantage (e.g., group-relative for GRPO)
        info_json: Additional reward metadata
    """
    value: float = 0.0
    advantage: float = 0.0
    info_json: str = "{}"


class ActionProposal(Component):
    """
    A proposed action from a planner.
    
    Used when multiple planners propose actions and arbitration is needed.
    """
    action_name: str = ""
    params_json: str = "{}"
    priority: float = 0.0
    confidence: float = 0.5
    planner_id: str = "default"


class ChosenAction(Component):
    """
    The action selected for execution after arbitration.
    """
    action_name: str = ""
    params_json: str = "{}"
    source_planner: str = ""


# =============================================================================
# Training State Components
# =============================================================================


class TrainingState(Component):
    """
    Per-agent training state for policy gradient methods.
    
    Stores the artifacts needed for GRPO/PPO training steps.
    """
    prompt_token_ids_json: str = "[]"
    completion_token_ids_json: str = "[]"
    old_logprobs_json: str = "[]"
    is_training_sample: bool = True
