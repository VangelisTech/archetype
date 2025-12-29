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
Rubric-based Reward Computation
===============================

Rubrics are self-evaluation scores built into agent cognitive schemas.
This module computes rewards from those scores with configurable weights.

Works with OODA agents and any schema that includes rubric fields.
"""

from dataclasses import dataclass

import daft
from daft import col


@dataclass
class RubricWeights:
    """
    Configurable weights for rubric-based reward computation.

    Each cognitive phase has its own rubric dimensions with weights.
    Phase weights determine relative importance of each phase.
    """

    # Phase weights (should sum to 1.0)
    observe_weight: float = 0.15
    orient_weight: float = 0.25
    decide_weight: float = 0.40
    act_weight: float = 0.20

    # Observe rubric weights
    observe_completeness: float = 0.3
    observe_relevance: float = 0.4
    observe_accuracy: float = 0.3

    # Orient rubric weights
    orient_coherence: float = 0.4
    orient_integration: float = 0.3
    orient_adaptability: float = 0.3

    # Decide rubric weights
    decide_alignment: float = 0.35
    decide_feasibility: float = 0.25
    decide_confidence: float = 0.25
    decide_risk_awareness: float = 0.15

    # Act rubric weights
    act_execution_quality: float = 0.4
    act_outcome_match: float = 0.4
    act_efficiency: float = 0.2


def compute_phase_scores(df: daft.DataFrame, weights: RubricWeights = None) -> daft.DataFrame:
    """
    Compute weighted scores for each cognitive phase.

    Assumes the DataFrame has columns:
        - observation (struct with rubric fields)
        - orientation (struct with rubric fields)
        - decision (struct with rubric fields)
        - action (struct with rubric fields)

    Returns DataFrame with additional columns:
        - observe_score, orient_score, decide_score, act_score
    """
    if weights is None:
        weights = RubricWeights()

    # Observe score
    df = df.with_column(
        "observe_score",
        (
            col("observation.completeness") * weights.observe_completeness
            + col("observation.relevance") * weights.observe_relevance
            + col("observation.accuracy") * weights.observe_accuracy
        ),
    )

    # Orient score
    df = df.with_column(
        "orient_score",
        (
            col("orientation.coherence") * weights.orient_coherence
            + col("orientation.integration") * weights.orient_integration
            + col("orientation.adaptability") * weights.orient_adaptability
        ),
    )

    # Decide score
    df = df.with_column(
        "decide_score",
        (
            col("decision.alignment") * weights.decide_alignment
            + col("decision.feasibility") * weights.decide_feasibility
            + col("decision.confidence") * weights.decide_confidence
            + col("decision.risk_awareness") * weights.decide_risk_awareness
        ),
    )

    # Act score
    df = df.with_column(
        "act_score",
        (
            col("action.execution_quality") * weights.act_execution_quality
            + col("action.outcome_match") * weights.act_outcome_match
            + col("action.efficiency") * weights.act_efficiency
        ),
    )

    return df


def compute_rubric_reward(df: daft.DataFrame, weights: RubricWeights = None) -> daft.DataFrame:
    """
    Compute overall reward from rubric scores.

    Combines phase scores with phase weights into a single reward signal.
    """
    if weights is None:
        weights = RubricWeights()

    # First compute phase scores
    df = compute_phase_scores(df, weights)

    # Combine into overall reward
    df = df.with_column(
        "reward",
        (
            col("observe_score") * weights.observe_weight
            + col("orient_score") * weights.orient_weight
            + col("decide_score") * weights.decide_weight
            + col("act_score") * weights.act_weight
        ),
    )

    return df
