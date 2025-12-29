# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype RL Agents
===================

Agent implementations built on archetype's ECS core.

Agent Hierarchy:
    1. Bandits (entry-level) - No state, immediate rewards
    2. OODA (cognitive) - Observe → Orient → Decide → Act loops
    3. Custom - Build your own using base components

All agents use:
    - Components from archetype.core.Component
    - Processors from archetype.core.aio.AsyncProcessor
    - DataFrames for all computation (Daft native)

Example (Bandit):
    from archetype.rl.agents import (
        ArmState, BanditPolicy, UCBSelectProcessor
    )

    # Create arms
    arms = [ArmState(agent_id="a1", arm_name=f"arm_{i}") for i in range(10)]

    # Run UCB selection
    processor = UCBSelectProcessor()
    df = await processor.process(df)

Example (OODA):
    from archetype.rl.agents import (
        Observation, Orientation, Decision, Action,
        run_ooda_loop,
    )

    trajectory = await run_ooda_loop(
        df,
        goal="Complete the task",
        model="gpt-4",
    )
"""

# Base components (fundamental agent building blocks)
# Bandits (entry-level agents)
from archetype.rl.agents.bandits import (
    ActionResolveProcessor,
    # Components
    ArmState,
    ArmUpdateProcessor,
    BanditPolicy,
    ThompsonSelectProcessor,
    # Processors
    UCBSelectProcessor,
)
from archetype.rl.agents.base import (
    ActionProposal,
    AgentState,
    ChosenAction,
    EpsilonGreedyState,
    Observation,
    Outcome,
    PolicyState,
)

# Control surfaces (behavior specification)
from archetype.rl.agents.control import (
    # Controllability analysis
    ControllabilityMetrics,
    # Control surface
    ControlSurface,
    ObservabilityMetrics,
    # Pipeline
    Pipeline,
    PipelineNode,
    PipelineNodeType,
    Regime,
    RubricBias,
    Sandbox,
    SandboxResult,
    # Tools & Sandbox
    Tool,
    ToolCall,
    ToolParameter,
    analyze_controllability,
    analyze_observability,
    create_cautious_surface,
    create_coder_surface,
    # Factory functions
    create_decisive_surface,
)

# OODA agents (cognitive loops)
from archetype.rl.agents.ooda import (
    OODAAction,
    OODADecision,
    # Components (OODA-specific observations)
    OODAObservation,
    OODAOrientation,
    OODATrajectory,
    # Functions
    run_ooda_loop,
    run_ooda_step,
)

# Rubrics (structured reward computation)
from archetype.rl.agents.rubrics import (
    RubricWeights,
    compute_phase_scores,
    compute_rubric_reward,
)

# Trajectories (data collection for training)
from archetype.rl.agents.trajectories import (
    collect_trajectories,
    filter_good_trajectories,
    trajectories_to_training_data,
)

__all__ = [
    # Base components
    "AgentState",
    "Observation",
    "ActionProposal",
    "ChosenAction",
    "Outcome",
    "PolicyState",
    "EpsilonGreedyState",
    # Bandits
    "ArmState",
    "BanditPolicy",
    "UCBSelectProcessor",
    "ThompsonSelectProcessor",
    "ArmUpdateProcessor",
    "ActionResolveProcessor",
    # OODA
    "OODAObservation",
    "OODAOrientation",
    "OODADecision",
    "OODAAction",
    "OODATrajectory",
    "run_ooda_loop",
    "run_ooda_step",
    # Rubrics
    "RubricWeights",
    "compute_rubric_reward",
    "compute_phase_scores",
    # Trajectories
    "collect_trajectories",
    "filter_good_trajectories",
    "trajectories_to_training_data",
    # Control
    "Tool",
    "ToolParameter",
    "ToolCall",
    "Sandbox",
    "SandboxResult",
    "ControlSurface",
    "RubricBias",
    "Regime",
    "Pipeline",
    "PipelineNode",
    "PipelineNodeType",
    "create_decisive_surface",
    "create_cautious_surface",
    "create_coder_surface",
    "ControllabilityMetrics",
    "ObservabilityMetrics",
    "analyze_controllability",
    "analyze_observability",
]
