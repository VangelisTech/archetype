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
OODA Loop Agent
===============

Cognitive agent implementing Observe → Orient → Decide → Act loops.

OODA is a decision-making framework from military strategy, adapted for AI agents:
    - Observe: Perceive environment
    - Orient: Build situational awareness
    - Decide: Choose action
    - Act: Execute and observe outcome

Each phase has a Pydantic schema with:
    1. Cognitive output fields
    2. Self-evaluation rubric fields (for training signal)

Uses daft.functions.prompt() for structured LLM outputs.
"""

from typing import Any

import daft
from daft import col
from daft.functions import prompt
from pydantic import BaseModel, Field

# =============================================================================
# OODA Phase Components
# =============================================================================


class OODAObservation(BaseModel):
    """
    What the agent perceives from the environment.

    Rubric dimensions:
        - completeness: Did we capture all relevant info?
        - relevance: Is the info task-relevant?
        - accuracy: How confident are we in accuracy?
    """

    # Core perception
    entities: list[str] = Field(
        default_factory=list, description="Entities observed in the environment"
    )
    state: dict[str, Any] = Field(default_factory=dict, description="Key-value state observations")
    events: list[str] = Field(default_factory=list, description="Events or changes detected")
    raw_perception: str = Field(default="", description="Raw perception summary")

    # Self-evaluation rubric scores
    completeness: float = Field(default=0.5, ge=0.0, le=1.0)
    relevance: float = Field(default=0.5, ge=0.0, le=1.0)
    accuracy: float = Field(default=0.5, ge=0.0, le=1.0)


class OODAOrientation(BaseModel):
    """
    Agent's situational awareness and mental model.

    Rubric dimensions:
        - coherence: Is the mental model self-consistent?
        - integration: How well integrated with prior knowledge?
        - adaptability: How well does it handle novelty?
    """

    # Core orientation
    situation_summary: str = Field(default="")
    threats: list[str] = Field(default_factory=list)
    opportunities: list[str] = Field(default_factory=list)
    mental_model: str = Field(default="")
    hypotheses: list[str] = Field(default_factory=list)

    # Self-evaluation rubric scores
    coherence: float = Field(default=0.5, ge=0.0, le=1.0)
    integration: float = Field(default=0.5, ge=0.0, le=1.0)
    adaptability: float = Field(default=0.5, ge=0.0, le=1.0)


class OODADecision(BaseModel):
    """
    Selected action with reasoning.

    Rubric dimensions:
        - alignment: How well aligned with goals?
        - feasibility: How achievable is the action?
        - confidence: How confident in the choice?
        - risk_awareness: How well are risks considered?
    """

    # Core decision
    action: str = Field(default="")
    reasoning: str = Field(default="")
    alternatives: list[str] = Field(default_factory=list)
    expected_outcome: str = Field(default="")
    contingencies: list[str] = Field(default_factory=list)

    # Self-evaluation rubric scores
    alignment: float = Field(default=0.5, ge=0.0, le=1.0)
    feasibility: float = Field(default=0.5, ge=0.0, le=1.0)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    risk_awareness: float = Field(default=0.5, ge=0.0, le=1.0)


class OODAAction(BaseModel):
    """
    Executed action and observed result.

    Rubric dimensions:
        - execution_quality: How well was it executed?
        - outcome_match: Did outcome match expectations?
        - efficiency: Was it resource-efficient?
    """

    # Core action result
    action_taken: str = Field(default="")
    success: bool = Field(default=False)
    outcome: str = Field(default="")
    side_effects: list[str] = Field(default_factory=list)

    # Self-evaluation rubric scores
    execution_quality: float = Field(default=0.5, ge=0.0, le=1.0)
    outcome_match: float = Field(default=0.5, ge=0.0, le=1.0)
    efficiency: float = Field(default=0.5, ge=0.0, le=1.0)


class OODATrajectory(BaseModel):
    """
    A complete OODA loop trajectory for training.
    """

    trajectory_id: str = Field(default="")
    timestamp: float = Field(default=0.0)

    environment: str = Field(default="")
    goal: str = Field(default="")

    observation: OODAObservation | None = None
    orientation: OODAOrientation | None = None
    decision: OODADecision | None = None
    action: OODAAction | None = None

    total_reward: float = Field(default=0.0)
    trajectory_quality: str = Field(default="unknown")


# =============================================================================
# System Prompts
# =============================================================================

OBSERVE_SYSTEM = """You are an observation system. Perceive and report what you observe.
Be thorough but focused. Rate your observation quality honestly."""

ORIENT_SYSTEM = """You are an orientation system. Build situational awareness.
Synthesize observations into a coherent mental model. Rate your orientation honestly."""

DECIDE_SYSTEM = """You are a decision system. Choose the best action.
Consider alternatives and explain reasoning. Rate your decision honestly."""

ACT_SYSTEM = """You are an execution system. Report action results.
Note what actually happened including side effects. Rate execution honestly."""


# =============================================================================
# OODA Loop Functions
# =============================================================================


def run_ooda_step(
    df: daft.DataFrame,
    phase: str,
    model: str = "gpt-4",
    context_col: str = "context",
) -> daft.DataFrame:
    """
    Run a single OODA phase.

    Args:
        df: DataFrame with context column
        phase: One of "observe", "orient", "decide", "act"
        model: LLM model to use
        context_col: Column with context/prompt

    Returns:
        DataFrame with phase output column
    """
    phase_config = {
        "observe": (OBSERVE_SYSTEM, OODAObservation, "observation"),
        "orient": (ORIENT_SYSTEM, OODAOrientation, "orientation"),
        "decide": (DECIDE_SYSTEM, OODADecision, "decision"),
        "act": (ACT_SYSTEM, OODAAction, "action"),
    }

    system_prompt, return_format, output_col = phase_config[phase]

    df = df.with_column(
        output_col,
        prompt(
            col(context_col),
            model=model,
            system_message=system_prompt,
            return_format=return_format,
        ),
    )

    return df


async def run_ooda_loop(
    df: daft.DataFrame,
    model: str = "gpt-4",
    environment_col: str = "environment",
    goal_col: str = "goal",
) -> daft.DataFrame:
    """
    Run complete OODA loop.

    Args:
        df: DataFrame with environment and goal columns
        model: LLM model to use
        environment_col: Column with environment description
        goal_col: Column with agent goal

    Returns:
        DataFrame with all OODA phase outputs
    """
    # Build initial context
    df = df.with_column(
        "_observe_context",
        daft.functions.format(
            "Environment: {env}\nGoal: {goal}\n\nObserve the environment:",
            env=col(environment_col),
            goal=col(goal_col),
        ),
    )

    # Observe
    df = run_ooda_step(df, "observe", model, "_observe_context")

    # Build orient context
    df = df.with_column(
        "_orient_context",
        daft.functions.format(
            "Environment: {env}\nGoal: {goal}\nObservation: {obs}\n\nOrient:",
            env=col(environment_col),
            goal=col(goal_col),
            obs=col("observation.raw_perception"),
        ),
    )

    # Orient
    df = run_ooda_step(df, "orient", model, "_orient_context")

    # Build decide context
    df = df.with_column(
        "_decide_context",
        daft.functions.format(
            "Environment: {env}\nGoal: {goal}\nSituation: {sit}\n\nDecide:",
            env=col(environment_col),
            goal=col(goal_col),
            sit=col("orientation.situation_summary"),
        ),
    )

    # Decide
    df = run_ooda_step(df, "decide", model, "_decide_context")

    # Build act context
    df = df.with_column(
        "_act_context",
        daft.functions.format(
            "Goal: {goal}\nChosen action: {action}\nReasoning: {reasoning}\n\nExecute and report:",
            goal=col(goal_col),
            action=col("decision.action"),
            reasoning=col("decision.reasoning"),
        ),
    )

    # Act
    df = run_ooda_step(df, "act", model, "_act_context")

    # Clean up intermediate columns
    df = df.exclude("_observe_context", "_orient_context", "_decide_context", "_act_context")

    return df
