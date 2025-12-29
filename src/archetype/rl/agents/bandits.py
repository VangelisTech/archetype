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
Bandit Agents
=============

Entry-level RL agents implementing multi-armed bandit algorithms.
All computations are Daft DataFrame native.

Bandits are the simplest form of RL:
- No state transitions
- Immediate rewards
- Explore/exploit tradeoff

Algorithms:
- UCB (Upper Confidence Bound)
- Thompson Sampling
- LinUCB (contextual)
"""

from daft import DataFrame, col, lit

from archetype.core import Component
from archetype.core.aio import AsyncProcessor

from .base import ActionProposal, ChosenAction, Observation, Outcome

# =============================================================================
# Bandit Components
# =============================================================================


class ArmState(Component):
    """
    State of a single bandit arm.

    Tracks pull count and reward statistics.
    """

    agent_id: str
    arm_name: str
    pulls: int = 0
    total_reward: float = 0.0
    mean_reward: float = 0.0
    variance: float = 0.0
    est_cost: float = 0.0
    last_pull_tick: int | None = None


class BanditPolicy(Component):
    """
    Bandit policy configuration.

    Modes:
        - ucb: Upper Confidence Bound
        - thompson: Thompson Sampling
        - linucb: Linear UCB (contextual)
    """

    agent_id: str
    mode: str = "ucb"  # ucb, thompson, linucb

    # UCB parameters
    c_explore: float = 1.5  # Exploration coefficient
    lambda_cost: float = 0.0  # Cost penalty weight

    # Thompson parameters
    alpha_prior: float = 1.0
    beta_prior: float = 1.0

    # LinUCB parameters
    alpha_linucb: float = 0.5


# =============================================================================
# UCB Selection Processor
# =============================================================================


class UCBSelectProcessor(AsyncProcessor):
    """
    UCB bandit selection using pure DataFrame operations.

    UCB score = mean_reward + c * sqrt(2 * ln(t) / n) - lambda * est_cost

    Where:
        - mean_reward: Average reward for this arm
        - c: Exploration coefficient
        - t: Total pulls across all arms
        - n: Pulls for this arm
        - lambda: Cost penalty weight
        - est_cost: Estimated cost for this arm

    Components:
        - Observation: Current agent state
        - ArmState: State of each arm
        - BanditPolicy: Policy parameters

    Output:
        - ActionProposal: Selected arm as action
    """

    components = (Observation, ArmState, BanditPolicy)
    priority = 5

    def __init__(
        self,
        planner_id: str = "ucb_bandit",
        default_c: float = 1.5,
        default_lambda: float = 0.0,
    ):
        self.planner_id = planner_id
        self.default_c = default_c
        self.default_lambda = default_lambda

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Select best arm for each agent using UCB.
        """
        # Compute total pulls per agent
        pulls_per_agent = df.groupby(col("armstate__agent_id")).agg(
            col("armstate__pulls").sum().alias("total_pulls")
        )

        # Join to get total pulls for each row
        df = df.join(
            pulls_per_agent,
            left_on="armstate__agent_id",
            right_on="armstate__agent_id",
            how="left",
        )

        # Get policy parameters with defaults
        c_explore = col("banditpolicy__c_explore").fill_null(self.default_c)
        lambda_cost = col("banditpolicy__lambda_cost").fill_null(self.default_lambda)

        # Compute UCB score
        # UCB = mean_reward + c * sqrt(2 * ln(t+1) / (n+1)) - lambda * est_cost
        n = col("armstate__pulls") + lit(1)
        t = col("total_pulls").fill_null(0) + lit(1)

        exploration_term = c_explore * (2.0 * t.log() / n).sqrt()
        cost_term = lambda_cost * col("armstate__est_cost").fill_null(0.0)

        df = df.with_column(
            "_ucb_score", col("armstate__mean_reward") + exploration_term - cost_term
        )

        # Select best arm per agent
        # Sort by score descending, take first per agent
        ranked = df.sort(col("_ucb_score"), desc=True)

        winners = ranked.groupby(col("armstate__agent_id")).agg(
            col("armstate__arm_name").first().alias("chosen_arm"),
            col("observation__tick").first().alias("tick"),
            col("_ucb_score").first().alias("score"),
        )

        # Create ActionProposal columns
        winners = winners.with_columns(
            {
                "actionproposal__agent_id": col("armstate__agent_id"),
                "actionproposal__tick": col("tick"),
                "actionproposal__action_name": col("chosen_arm"),
                "actionproposal__params_json": lit("{}"),
                "actionproposal__priority": col("score"),
                "actionproposal__planner_id": lit(self.planner_id),
                "actionproposal__confidence": (col("score") / (col("score").abs() + lit(1))),
            }
        )

        # Join proposals back to original df
        return df.join(
            winners.select(
                "actionproposal__agent_id",
                "actionproposal__tick",
                "actionproposal__action_name",
                "actionproposal__params_json",
                "actionproposal__priority",
                "actionproposal__planner_id",
                "actionproposal__confidence",
            ),
            left_on="observation__agent_id",
            right_on="actionproposal__agent_id",
            how="left",
        )


# =============================================================================
# Thompson Sampling Processor
# =============================================================================


class ThompsonSelectProcessor(AsyncProcessor):
    """
    Thompson Sampling bandit selection.

    Samples from Beta(alpha + successes, beta + failures) for each arm,
    selects arm with highest sample.

    For continuous rewards, uses Gaussian approximation.
    """

    components = (Observation, ArmState, BanditPolicy)
    priority = 5

    def __init__(self, planner_id: str = "thompson_bandit"):
        self.planner_id = planner_id

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Select arm using Thompson sampling.

        Note: True Thompson sampling requires random sampling which
        is tricky in pure DataFrame ops. This uses a deterministic
        approximation based on UCB-style scoring.

        For true Thompson sampling, use a UDF.
        """
        # Approximate Thompson with optimistic sampling
        # Use mean + variance-scaled noise
        alpha = col("banditpolicy__alpha_prior").fill_null(1.0)
        beta = col("banditpolicy__beta_prior").fill_null(1.0)

        n = col("armstate__pulls") + lit(1)
        mean = col("armstate__mean_reward")

        # Posterior mean approximation: (alpha + n*mean) / (alpha + beta + n)
        posterior_mean = (alpha + n * mean) / (alpha + beta + n)

        # Posterior variance approximation (simplified)
        posterior_var = (1.0 / (alpha + beta + n)).sqrt()

        # Optimistic estimate (like UCB)
        df = df.with_column("_thompson_score", posterior_mean + 1.5 * posterior_var)

        # Select best arm per agent
        ranked = df.sort(col("_thompson_score"), desc=True)

        winners = ranked.groupby(col("armstate__agent_id")).agg(
            col("armstate__arm_name").first().alias("chosen_arm"),
            col("observation__tick").first().alias("tick"),
        )

        winners = winners.with_columns(
            {
                "actionproposal__agent_id": col("armstate__agent_id"),
                "actionproposal__tick": col("tick"),
                "actionproposal__action_name": col("chosen_arm"),
                "actionproposal__params_json": lit("{}"),
                "actionproposal__priority": lit(0.0),
                "actionproposal__planner_id": lit(self.planner_id),
                "actionproposal__confidence": lit(0.5),
            }
        )

        return df.join(
            winners.select(
                "actionproposal__agent_id",
                "actionproposal__tick",
                "actionproposal__action_name",
                "actionproposal__params_json",
                "actionproposal__priority",
                "actionproposal__planner_id",
                "actionproposal__confidence",
            ),
            left_on="observation__agent_id",
            right_on="actionproposal__agent_id",
            how="left",
        )


# =============================================================================
# Arm Update Processor
# =============================================================================


class ArmUpdateProcessor(AsyncProcessor):
    """
    Update arm statistics after receiving outcome.

    Updates:
        - pulls: Increment by 1
        - total_reward: Add received reward
        - mean_reward: Running average
        - variance: Welford's online algorithm
    """

    components = (ChosenAction, Outcome, ArmState)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Update arm statistics for chosen actions.
        """
        # Match outcomes to arms
        # Filter to rows where this arm was chosen
        chosen_arms = df.filter(col("chosenaction__action_name") == col("armstate__arm_name"))

        if chosen_arms.count().to_pylist()[0] == 0:
            return df

        # Compute updated statistics
        new_pulls = col("armstate__pulls") + lit(1)
        new_total = col("armstate__total_reward") + col("outcome__reward")
        new_mean = new_total / new_pulls

        # Welford's variance update (simplified)
        delta = col("outcome__reward") - col("armstate__mean_reward")
        new_variance = (
            col("armstate__variance")
            + (delta * (col("outcome__reward") - new_mean) - col("armstate__variance")) / new_pulls
        )

        chosen_arms = chosen_arms.with_columns(
            {
                "armstate__pulls": new_pulls,
                "armstate__total_reward": new_total,
                "armstate__mean_reward": new_mean,
                "armstate__variance": new_variance,
                "armstate__last_pull_tick": col("outcome__tick"),
            }
        )

        # Merge updated arms back
        # This is a simplification - proper implementation would use
        # conditional updates or a more sophisticated merge strategy
        return chosen_arms


# =============================================================================
# Action Resolution Processor
# =============================================================================


class ActionResolveProcessor(AsyncProcessor):
    """
    Resolve multiple ActionProposals to a single ChosenAction.

    Selection strategy: highest priority wins, ties broken by confidence.
    """

    components = (ActionProposal,)
    priority = 6

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Select highest priority action for each agent.
        """
        # Sort by priority (desc), then confidence (desc)
        ranked = df.sort(
            [col("actionproposal__priority"), col("actionproposal__confidence")],
            desc=[True, True],
        )

        # Take first (best) per agent
        chosen = ranked.groupby(col("actionproposal__agent_id")).agg(
            col("actionproposal__action_name").first().alias("action_name"),
            col("actionproposal__params_json").first().alias("params_json"),
            col("actionproposal__planner_id").first().alias("planner_id"),
            col("actionproposal__tick").first().alias("tick"),
        )

        # Create ChosenAction columns
        chosen = chosen.with_columns(
            {
                "chosenaction__agent_id": col("actionproposal__agent_id"),
                "chosenaction__tick": col("tick"),
                "chosenaction__action_name": col("action_name"),
                "chosenaction__params_json": col("params_json"),
                "chosenaction__planner_id": col("planner_id"),
            }
        )

        return df.join(
            chosen.select(
                "chosenaction__agent_id",
                "chosenaction__tick",
                "chosenaction__action_name",
                "chosenaction__params_json",
                "chosenaction__planner_id",
            ),
            left_on="actionproposal__agent_id",
            right_on="chosenaction__agent_id",
            how="left",
        )
