# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Base Agent Components
=====================

Fundamental ECS components for agent-based RL.
These derive directly from archetype.core.Component.

Agent Architecture:
    Observation → Policy → Action → Outcome

    - Observation: What the agent perceives
    - ActionProposal: Candidate actions from planners
    - ChosenAction: Selected action after arbitration
    - Outcome: Reward/cost feedback
"""

from archetype.core import Component

# =============================================================================
# Core Agent Components
# =============================================================================


class AgentState(Component):
    """
    Base agent state.

    Every agent has an ID and operates at a specific tick.
    """

    agent_id: str
    tick: int = 0


class Observation(Component):
    """
    What the agent perceives from the environment.

    Observations are the input to the policy.
    """

    agent_id: str
    tick: int
    features_json: str = "{}"  # Flexible feature storage
    raw_text: str = ""  # For LLM-based agents


class ActionProposal(Component):
    """
    A proposed action from a planner.

    Multiple planners can propose actions; arbitration selects one.
    """

    agent_id: str
    tick: int
    action_name: str
    params_json: str | None = None
    priority: float = 0.0
    planner_id: str = "default"
    confidence: float = 0.5


class ChosenAction(Component):
    """
    The action selected for execution.

    Result of arbitration over ActionProposals.
    """

    agent_id: str
    tick: int
    action_name: str
    params_json: str | None = None
    planner_id: str = ""


class Outcome(Component):
    """
    Result of action execution.

    Contains reward signal for training.
    """

    agent_id: str
    tick: int
    reward: float = 0.0
    cost: float = 0.0
    success: bool = True
    info_json: str = "{}"


# =============================================================================
# Policy State Components
# =============================================================================


class PolicyState(Component):
    """
    Base class for policy state.

    Subclasses define specific policy types (bandits, neural, etc.)
    """

    agent_id: str
    policy_type: str = "base"


class EpsilonGreedyState(PolicyState):
    """
    Epsilon-greedy exploration state.
    """

    policy_type: str = "epsilon_greedy"
    epsilon: float = 0.1
    decay_rate: float = 0.995
    min_epsilon: float = 0.01
