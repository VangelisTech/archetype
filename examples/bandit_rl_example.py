# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Bandit RL Example
=================

A complete working example of multi-armed bandit using archetype's ECS.

This demonstrates:
    - ECS Components for RL (Observation, ArmState, BanditPolicy)
    - Daft-native computation (all ops are DataFrame operations)
    - UCB algorithm (balances exploration/exploitation)
    - Processor pattern (UCBSelectProcessor extends AsyncProcessor)

Run:
    python examples/bandit_rl_example.py
"""

import asyncio
import random

import daft
from daft import col, lit

from archetype.core.aio import AsyncProcessor

# =============================================================================
# Components (inline for standalone example)
# =============================================================================
# In production, import from: archetype.rl.agents


# =============================================================================
# UCB Processor (simplified for example)
# =============================================================================


class UCBSelectProcessor(AsyncProcessor):
    """
    UCB bandit selection using pure DataFrame operations.

    UCB score = mean_reward + c * sqrt(2 * ln(t) / n)
    """

    components = ()  # We handle component matching manually here
    priority = 5

    def __init__(self, c_explore: float = 2.0):
        self.c_explore = c_explore

    async def process(self, df: daft.DataFrame, **kwargs) -> daft.DataFrame:
        # Compute total pulls per agent
        pulls_per_agent = df.groupby(col("agent_id")).agg(col("pulls").sum().alias("total_pulls"))

        # Join to get total pulls
        df = df.join(pulls_per_agent, on="agent_id", how="left")

        # UCB score: mean + c * sqrt(2 * ln(t+1) / (n+1))
        n = col("pulls") + lit(1)
        t = col("total_pulls").fill_null(0) + lit(1)

        exploration = lit(self.c_explore) * (lit(2.0) * t.log() / n).sqrt()

        df = df.with_column("ucb_score", col("mean_reward") + exploration)

        # Select best arm per agent (sort desc, take first)
        df = df.sort(col("ucb_score"), desc=True)

        # Mark the chosen arm (first per agent after sorting)
        # For simplicity, we'll compute this after collection

        return df


# =============================================================================
# Create World State
# =============================================================================


def create_bandit_world(num_arms: int = 5) -> dict:
    """
    Create initial state for a bandit problem.

    Returns dict with arm states and true (hidden) reward rates.
    """
    # True reward rates (agent doesn't know these)
    true_rates = [0.1, 0.3, 0.5, 0.7, 0.9][:num_arms]

    return {
        "agent_id": ["agent_0"] * num_arms,
        "arm_name": [f"arm_{i}" for i in range(num_arms)],
        "pulls": [0] * num_arms,
        "total_reward": [0.0] * num_arms,
        "mean_reward": [0.0] * num_arms,
        "_true_rate": true_rates,  # Hidden from agent
    }


# =============================================================================
# Main Loop
# =============================================================================


async def run_bandit_loop(num_steps: int = 100, num_arms: int = 5):
    """
    Run the bandit RL loop.

    At each step:
    1. UCB selects an arm
    2. Pull the arm (simulate reward)
    3. Update arm statistics
    """
    print("🎰 Bandit RL Loop")
    print(f"   Steps: {num_steps}, Arms: {num_arms}")
    print(f"   True rates: {[0.1, 0.3, 0.5, 0.7, 0.9][:num_arms]}")
    print("-" * 50)

    # Initialize state
    state = create_bandit_world(num_arms)

    # Create processor
    ucb = UCBSelectProcessor(c_explore=2.0)

    # Track metrics
    rewards_history = []

    for step in range(num_steps):
        # Create DataFrame from current state
        df = daft.from_pydict(state)

        # Run UCB selection
        df = await ucb.process(df)

        # Collect and find best arm
        results = df.collect().to_pydict()

        # Best arm is first after sorting by UCB score
        chosen_idx = 0  # First row after desc sort
        chosen_arm = results["arm_name"][chosen_idx]
        true_rate = results["_true_rate"][chosen_idx]

        # Simulate pulling the arm (Bernoulli reward)
        reward = 1.0 if random.random() < true_rate else 0.0
        rewards_history.append(reward)

        # Find the arm in original state and update
        arm_idx = state["arm_name"].index(chosen_arm)
        state["pulls"][arm_idx] += 1
        state["total_reward"][arm_idx] += reward
        state["mean_reward"][arm_idx] = state["total_reward"][arm_idx] / state["pulls"][arm_idx]

        # Log progress
        if step % 20 == 0 or step == num_steps - 1:
            cumulative = sum(rewards_history)
            avg = cumulative / (step + 1)
            print(
                f"Step {step:3d}: {chosen_arm} → reward={reward:.0f} | "
                f"cumulative={cumulative:.0f}, avg={avg:.2f}"
            )

    # Final summary
    print("-" * 50)
    print("📊 Results:")
    print(f"   Total reward: {sum(rewards_history):.0f} / {num_steps}")
    print(f"   Average reward: {sum(rewards_history) / num_steps:.3f}")

    # Optimal would be always pulling best arm
    best_rate = max(state["_true_rate"])
    optimal = best_rate * num_steps
    regret = optimal - sum(rewards_history)
    print(f"   Optimal (best arm): {optimal:.0f}")
    print(f"   Regret: {regret:.1f}")

    print("\n🎯 Arm Statistics:")
    for i in range(num_arms):
        arm = state["arm_name"][i]
        pulls = state["pulls"][i]
        mean = state["mean_reward"][i]
        true = state["_true_rate"][i]
        pct = pulls / num_steps * 100
        print(f"   {arm}: {pulls:3d} pulls ({pct:5.1f}%) | estimated={mean:.2f}, true={true:.1f}")

    return sum(rewards_history), state


# =============================================================================
# Entry Point
# =============================================================================


if __name__ == "__main__":
    asyncio.run(run_bandit_loop(num_steps=100, num_arms=5))
