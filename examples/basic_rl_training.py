#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
"""
Basic RL Training Example
=========================

A complete end-to-end example showing:
    1. Define Components (State, Action, Reward)
    2. Create environment dynamics processor
    3. Create a simple trainable policy
    4. Collect rollouts with archetype
    5. Compute group-relative advantages (GRPO-style)
    6. Plot loss curves

This is a bandit-style problem: learn which action maximizes reward.

Run:
    cd archetype
    uv run python examples/basic_rl_training.py
"""

import math
import os
import random
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import asyncio

import daft
from daft import DataFrame, col, lit

from archetype.app import ArchetypeApp
from archetype.core import AsyncProcessor, Component, RunConfig


class TrainingMetrics:
    """Tiny metrics helper for this example (keeps deps minimal)."""

    def __init__(self):
        self.rows: list[dict] = []

    def log(self, *, step: int, loss: float, reward: float, accuracy: float) -> None:
        self.rows.append(
            {
                "step": int(step),
                "loss": float(loss),
                "reward": float(reward),
                "accuracy": float(accuracy),
            }
        )

    @property
    def latest(self) -> dict:
        return self.rows[-1] if self.rows else {}

    def to_daft(self) -> daft.DataFrame:
        return daft.from_pylist(self.rows)

    def save(self, path: str) -> None:
        self.to_daft().write_parquet(path, write_mode="overwrite")


# =============================================================================
# 1. Define Components
# =============================================================================


class State(Component):
    """Observable state: a context vector."""

    context: int  # 0, 1, 2, or 3 - determines optimal action


class Action(Component):
    """Discrete action: 0, 1, 2, or 3."""

    value: int


class Reward(Component):
    """Reward signal."""

    value: float
    done: bool = False


# =============================================================================
# 2. Simple Trainable Policy
# =============================================================================


class SimplePolicy:
    """
    A simple tabular policy: P(action | context).

    Learns a 4x4 table of action preferences.
    """

    def __init__(self, num_contexts: int = 4, num_actions: int = 4):
        self.num_contexts = num_contexts
        self.num_actions = num_actions
        # Initialize uniform preferences
        self.logits = [[0.0] * num_actions for _ in range(num_contexts)]
        self.learning_rate = 0.1

    def get_action_probs(self, context: int) -> list[float]:
        """Get softmax probabilities for a context."""
        logits = self.logits[context]
        max_l = max(logits)
        exp_logits = [math.exp(logit - max_l) for logit in logits]
        total = sum(exp_logits)
        return [e / total for e in exp_logits]

    def sample_action(self, context: int) -> int:
        """Sample an action from the policy."""
        probs = self.get_action_probs(context)
        r = random.random()
        cumsum = 0.0
        for a, p in enumerate(probs):
            cumsum += p
            if r < cumsum:
                return a
        return self.num_actions - 1

    def log_prob(self, context: int, action: int) -> float:
        """Get log probability of an action given context."""
        probs = self.get_action_probs(context)
        return math.log(probs[action] + 1e-10)

    def update(self, context: int, action: int, advantage: float) -> float:
        """
        Update policy using REINFORCE-style gradient.

        ∇J = advantage * ∇log π(a|s)

        For softmax: ∇log π(a|s) = e_a - π(s) where e_a is one-hot
        """
        probs = self.get_action_probs(context)

        # Gradient for each action
        for a in range(self.num_actions):
            if a == action:
                # Chosen action: gradient is (1 - π(a|s))
                grad = 1.0 - probs[a]
            else:
                # Other actions: gradient is -π(a|s)
                grad = -probs[a]

            # Update logit
            self.logits[context][a] += self.learning_rate * advantage * grad

        # Return approximate loss (negative log prob weighted by advantage)
        return -self.log_prob(context, action) * advantage


# =============================================================================
# 3. Environment Dynamics
# =============================================================================


# Ground truth: optimal action for each context
OPTIMAL_ACTIONS = {0: 2, 1: 0, 2: 3, 3: 1}


class EnvironmentProcessor(AsyncProcessor):
    """
    Environment dynamics: gives reward based on context-action match.

    Reward = 1.0 if action matches optimal for context, else -0.5
    """

    components = (State, Action, Reward)
    priority = 1  # Runs after policy

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        rows = df.collect().to_pylist()

        for row in rows:
            context = row["state__context"]
            action = row["action__value"]
            optimal = OPTIMAL_ACTIONS.get(context, -1)
            row["reward__value"] = 1.0 if action == optimal else -0.5

        result = daft.from_pylist(rows)
        return result.select(*df.column_names)


# =============================================================================
# 4. Policy Processor (uses the trainable policy)
# =============================================================================


class PolicyProcessor(AsyncProcessor):
    """
    Policy processor that samples actions from our SimplePolicy.

    In a real setup, this would call a neural network.
    """

    components = (State, Action)
    priority = 0  # Runs first

    def __init__(self, policy: SimplePolicy):
        self.policy = policy

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        rows = df.collect().to_pylist()

        actions = []
        for row in rows:
            context = row["state__context"]
            action = self.policy.sample_action(context)
            actions.append(action)

        for i, row in enumerate(rows):
            row["action__value"] = actions[i]

        result = daft.from_pylist(rows)
        return result.select(*df.column_names)


# =============================================================================
# 5. Training Loop
# =============================================================================


async def train_policy(
    policy: SimplePolicy,
    num_iterations: int = 50,
    batch_size: int = 32,
) -> TrainingMetrics:
    """
    Train the policy using GRPO-style updates.
    """
    metrics = TrainingMetrics()

    # Setup archetype
    output_dir = os.path.abspath(".archetype_examples/basic_rl")
    os.makedirs(output_dir, exist_ok=True)

    app = await ArchetypeApp.create(storage_uri=output_dir)

    print(f"Training for {num_iterations} iterations...")
    print("Optimal policy: context → action")
    for ctx, act in OPTIMAL_ACTIONS.items():
        print(f"  {ctx} → {act}")
    print()

    for iteration in range(num_iterations):
        # Create a fresh world for each iteration
        world = await app.create_world(name=f"train_iter_{iteration}")

        # Add processors
        await world.add_processor(PolicyProcessor(policy))
        await world.add_processor(EnvironmentProcessor())

        # Spawn entities with random contexts
        for _i in range(batch_size):
            context = random.randint(0, 3)
            await world.create_entity(
                [
                    State(context=context),
                    Action(value=0),
                    Reward(value=0.0),
                ]
            )

        # Run one step (policy → environment)
        await world.step(RunConfig(num_steps=1))

        # Collect trajectory data
        traj = await world.get_components([State, Action, Reward])
        rows = traj.collect().to_pylist()

        # Assign groups by context for GRPO-style advantages
        for row in rows:
            row["group_id"] = row["state__context"]

        traj_df = daft.from_pylist(rows)

        # Compute group-relative advantages (per context).
        # Advantage = (r - mean_group) / (std_group + eps)
        eps = 1e-8
        mean_r = col("reward__value").mean().over("group_id")
        std_r = col("reward__value").stddev().over("group_id")
        with_advantages = traj_df.with_column(
            "advantage", (col("reward__value") - mean_r) / (std_r + lit(eps))
        )

        # Training step
        rows_with_adv = with_advantages.collect().to_pylist()

        total_loss = 0.0
        total_reward = 0.0
        correct_actions = 0

        for row in rows_with_adv:
            context = row["state__context"]
            action = row["action__value"]
            advantage = row["advantage"]
            reward = row["reward__value"]

            # Update policy
            loss = policy.update(context, action, advantage)
            total_loss += abs(loss)
            total_reward += reward

            # Track accuracy
            if action == OPTIMAL_ACTIONS[context]:
                correct_actions += 1

        avg_loss = total_loss / len(rows_with_adv)
        avg_reward = total_reward / len(rows_with_adv)
        accuracy = correct_actions / len(rows_with_adv)

        # Log metrics
        metrics.log(
            step=iteration,
            loss=avg_loss,
            reward=avg_reward,
            accuracy=accuracy,
        )

        if iteration % 10 == 0:
            print(
                f"  Iter {iteration:3d}: loss={avg_loss:.4f}, "
                f"reward={avg_reward:.4f}, accuracy={accuracy:.2%}"
            )

    await app.shutdown()
    return metrics


def show_learned_policy(policy: SimplePolicy):
    """Display the learned policy."""
    print("\nLearned policy P(action | context):")
    print("-" * 50)
    print(f"{'Context':<10} " + " ".join(f"A={a:<6}" for a in range(4)) + " Optimal")
    print("-" * 50)

    for ctx in range(4):
        probs = policy.get_action_probs(ctx)
        prob_str = " ".join(f"{p:.3f}  " for p in probs)
        best_action = probs.index(max(probs))
        optimal = OPTIMAL_ACTIONS[ctx]
        match = "✓" if best_action == optimal else "✗"
        print(f"{ctx:<10} {prob_str} {optimal} {match}")


# =============================================================================
# Main
# =============================================================================


async def main():
    print("=" * 70)
    print("Basic RL Training with Archetype")
    print("=" * 70)
    print()
    print("Task: Learn which action is optimal for each context (bandit problem)")
    print()

    # Create policy
    policy = SimplePolicy(num_contexts=4, num_actions=4)

    # Train
    metrics = await train_policy(policy, num_iterations=100, batch_size=64)

    # Show learned policy
    show_learned_policy(policy)

    # Export metrics
    output_dir = os.path.abspath(".archetype_examples/basic_rl")
    metrics.save(os.path.join(output_dir, "metrics.parquet"))
    print(f"Saved: {output_dir}/metrics.parquet")

    # Summary
    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)

    final = metrics.latest
    print(f"Final loss:     {final['loss']:.4f}")
    print(f"Final reward:   {final['reward']:.4f}")
    print(f"Final accuracy: {final['accuracy']:.2%}")

    print()
    print("Pipeline:")
    print("  1. Components: State, Action, Reward")
    print("  2. PolicyProcessor: samples actions from SimplePolicy")
    print("  3. EnvironmentProcessor: computes rewards")
    print("  4. Rollout: world.step() collects (s, a, r) tuples")
    print("  5. GRPO: compute_group_advantages() normalizes by context")
    print("  6. Update: policy.update(context, action, advantage)")
    print()


if __name__ == "__main__":
    asyncio.run(main())
