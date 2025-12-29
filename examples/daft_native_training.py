#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
"""
Daft-Native Training Example
============================

All training runs through Daft as the execution engine using:
    - @daft.func: Row-wise operations
    - @daft.func.batch: Batch operations for efficiency
    - @daft.cls: Class-based UDFs with state (models)

NO separate PyTorch training loop - everything is a Daft operation!

Reference:
    https://docs.getdaft.io/en/stable/custom-code/func/
    https://docs.getdaft.io/en/stable/custom-code/cls/

Run:
    cd archetype
    uv run python examples/daft_native_training.py
"""

import math
import os
import random
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import daft
from daft import DataType, Series, col
from pydantic import BaseModel

# =============================================================================
# Show Daft UDF Patterns
# =============================================================================


def show_daft_udf_patterns():
    """Demonstrate Daft's UDF system for RL training."""

    print("""
═══════════════════════════════════════════════════════════════════════
DAFT UDF PATTERNS FOR RL TRAINING
═══════════════════════════════════════════════════════════════════════

All training runs through Daft. No separate training loop!

1. @daft.func - Row-wise operations (simple transforms)
─────────────────────────────────────────────────────────

```python
@daft.func
def compute_advantage(reward: float, baseline: float) -> float:
    return reward - baseline

df = df.with_column("advantage", compute_advantage(col("reward"), col("baseline")))
```

2. @daft.func.batch - Batch operations (vectorized, efficient)
──────────────────────────────────────────────────────────────

```python
@daft.func.batch(return_dtype=DataType.float64())
def normalize_advantages(advantages: Series) -> Series:
    import numpy as np
    adv = np.array(advantages.to_pylist())
    normalized = (adv - adv.mean()) / (adv.std() + 1e-8)
    return Series.from_pylist(normalized.tolist())

df = df.with_column("norm_advantage", normalize_advantages(col("advantage")))
```

3. @daft.cls - Class-based UDFs with state (MODELS!)
────────────────────────────────────────────────────

```python
@daft.cls(return_dtype=DataType.struct({
    "action": DataType.int64(),
    "log_prob": DataType.float64(),
}))
class PolicyUDF:
    def __init__(self):
        self.model = load_model()  # State persists!

    def __call__(self, obs: Series) -> List[Dict]:
        # Run inference
        actions = self.model(obs.to_pylist())
        return [{"action": a, "log_prob": lp} for a, lp in actions]

policy = PolicyUDF()
df = df.with_column("policy_output", policy(col("obs")))
```

4. Pydantic for structured outputs (auto type inference!)
─────────────────────────────────────────────────────────

```python
from pydantic import BaseModel

class PolicyOutput(BaseModel):
    action: int
    log_prob: float
    entropy: float

@daft.func
def policy_forward(obs_x: float, obs_y: float) -> PolicyOutput:
    # Daft automatically converts to struct type!
    return PolicyOutput(action=1, log_prob=-0.5, entropy=0.3)
```
""")


# =============================================================================
# Simple Policy Example (No PyTorch)
# =============================================================================


class TabularPolicy:
    """Simple tabular policy for demonstration."""

    def __init__(self, num_states: int = 4, num_actions: int = 4):
        self.num_states = num_states
        self.num_actions = num_actions
        # Q-values
        self.q = [[0.0] * num_actions for _ in range(num_states)]
        self.lr = 0.1

    def get_action_probs(self, state: int) -> list[float]:
        q = self.q[state]
        max_q = max(q)
        exp_q = [math.exp(x - max_q) for x in q]
        total = sum(exp_q)
        return [e / total for e in exp_q]

    def sample(self, state: int) -> tuple:
        probs = self.get_action_probs(state)
        r = random.random()
        cumsum = 0.0
        for a, p in enumerate(probs):
            cumsum += p
            if r < cumsum:
                log_prob = math.log(p + 1e-10)
                return a, log_prob
        return self.num_actions - 1, math.log(probs[-1] + 1e-10)

    def update(self, state: int, action: int, advantage: float):
        probs = self.get_action_probs(state)
        for a in range(self.num_actions):
            grad = (1.0 - probs[a]) if a == action else -probs[a]
            self.q[state][a] += self.lr * advantage * grad


# Create global policy for UDF
POLICY = TabularPolicy()


# =============================================================================
# Daft UDFs for Training
# =============================================================================


# Pydantic model for policy output
class PolicyOutputModel(BaseModel):
    action: int
    log_prob: float


# Row-wise policy inference
@daft.func
def policy_forward(state: int) -> PolicyOutputModel:
    """Sample action from policy."""
    action, log_prob = POLICY.sample(state)
    return PolicyOutputModel(action=action, log_prob=log_prob)


# Batch reward computation
@daft.func.batch(return_dtype=DataType.float64())
def compute_rewards(states: Series, actions: Series) -> Series:
    """Compute rewards: +1 if action == state, else -0.5."""
    state_list = states.to_pylist()
    action_list = actions.to_pylist()

    rewards = [1.0 if a == s else -0.5 for s, a in zip(state_list, action_list, strict=False)]

    return Series.from_pylist(rewards)


# Batch advantage normalization
@daft.func.batch(return_dtype=DataType.float64())
def normalize_batch(values: Series) -> Series:
    """Normalize to zero mean, unit variance."""
    import numpy as np

    arr = np.array(values.to_pylist())
    if len(arr) > 1:
        normalized = (arr - arr.mean()) / (arr.std() + 1e-8)
    else:
        normalized = arr * 0
    return Series.from_pylist(normalized.tolist())


# Batch policy update (this is the training step!)
@daft.func.batch(return_dtype=DataType.float64())
def update_policy_batch(
    states: Series,
    actions: Series,
    advantages: Series,
) -> Series:
    """
    Update policy and return losses.

    This is where training happens - inside Daft!
    """
    state_list = states.to_pylist()
    action_list = actions.to_pylist()
    adv_list = advantages.to_pylist()

    losses = []
    for s, a, adv in zip(state_list, action_list, adv_list, strict=False):
        # Compute loss before update
        log_prob = math.log(POLICY.get_action_probs(s)[a] + 1e-10)
        loss = -log_prob * adv

        # Update policy
        POLICY.update(s, a, adv)

        losses.append(abs(loss))

    return Series.from_pylist(losses)


# =============================================================================
# Full Training Pipeline (Pure Daft)
# =============================================================================


def train_with_daft_only(num_iterations: int = 50, batch_size: int = 32):
    """
    Training loop using ONLY Daft operations.

    No separate PyTorch code - everything is a DataFrame operation!
    """
    print("=" * 70)
    print("Daft-Native Training")
    print("=" * 70)
    print()
    print("All operations are Daft UDFs:")
    print("  - policy_forward: @daft.func (row-wise inference)")
    print("  - compute_rewards: @daft.func.batch (vectorized)")
    print("  - normalize_batch: @daft.func.batch (vectorized)")
    print("  - update_policy_batch: @daft.func.batch (training step!)")
    print()

    metrics_history = []

    for iteration in range(num_iterations):
        # 1. Create batch of random states
        states = [random.randint(0, 3) for _ in range(batch_size)]
        df = daft.from_pydict({"state": states})

        # 2. Policy inference (Daft operation!)
        df = df.with_column("policy_output", policy_forward(col("state")))

        # 3. Extract action and log_prob from struct using unnest
        from daft.functions import unnest

        df = df.select(
            col("state"),
            unnest(col("policy_output")),  # Expands struct into columns
        )

        # 4. Compute rewards (Daft batch operation!)
        df = df.with_column("reward", compute_rewards(col("state"), col("action")))

        # 5. Normalize advantages (Daft batch operation!)
        df = df.with_column("advantage", normalize_batch(col("reward")))

        # 6. Training step - update policy (Daft batch operation!)
        df = df.with_column(
            "loss", update_policy_batch(col("state"), col("action"), col("advantage"))
        )

        # 7. Collect metrics
        stats = (
            df.agg(
                col("loss").mean().alias("mean_loss"),
                col("reward").mean().alias("mean_reward"),
            )
            .collect()
            .to_pylist()[0]
        )

        # Compute accuracy
        rows = df.select("state", "action").collect().to_pylist()
        correct = sum(1 for r in rows if r["action"] == r["state"])
        accuracy = correct / len(rows)

        stats["accuracy"] = accuracy
        stats["iteration"] = iteration
        metrics_history.append(stats)

        if iteration % 10 == 0:
            print(
                f"  Iter {iteration:3d}: loss={stats['mean_loss']:.4f}, "
                f"reward={stats['mean_reward']:.4f}, accuracy={accuracy:.2%}"
            )

    # Show learned policy
    print()
    print("Learned policy P(action | state):")
    print("-" * 50)
    for s in range(4):
        probs = POLICY.get_action_probs(s)
        prob_str = " ".join(f"{p:.3f}" for p in probs)
        best = probs.index(max(probs))
        match = "✓" if best == s else "✗"
        print(f"  State {s}: [{prob_str}] → Action {best} {match}")

    return daft.from_pylist(metrics_history)


# =============================================================================
# Main
# =============================================================================


def main():
    show_daft_udf_patterns()

    print()
    print("Running training...")
    print()

    metrics_df = train_with_daft_only(num_iterations=100, batch_size=64)

    print()
    print("=" * 70)
    print("KEY INSIGHT")
    print("=" * 70)
    print()
    print("The ENTIRE training loop is Daft operations:")
    print()
    print("  df = df.with_column('action', policy_forward(col('state')))")
    print("  df = df.with_column('reward', compute_rewards(...))")
    print("  df = df.with_column('advantage', normalize_batch(...))")
    print("  df = df.with_column('loss', update_policy_batch(...))  # Training!")
    print()
    print("Benefits:")
    print("  • Scales automatically with Ray/distributed")
    print("  • All data stays in Daft execution engine")
    print("  • No separate training framework needed")
    print("  • UDFs can hold model state (@daft.cls)")
    print()
    print("For PyTorch models, use @daft.cls:")
    print()
    print("  @daft.cls(return_dtype=...)")
    print("  class PolicyUDF:")
    print("      def __init__(self):")
    print("          self.model = torch.load('model.pt')  # State!")
    print("      def __call__(self, obs: Series) -> List[Dict]:")
    print("          return self.model(obs.to_pylist())")
    print()

    # Save metrics
    output_dir = os.path.abspath(".archetype_examples/daft_native")
    os.makedirs(output_dir, exist_ok=True)

    metrics_df.write_parquet(os.path.join(output_dir, "metrics.parquet"))
    print(f"Saved: {output_dir}/metrics.parquet")

    # Plot if matplotlib available
    try:
        import matplotlib.pyplot as plt

        rows = metrics_df.collect().to_pylist()

        fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(15, 4))

        iters = [r["iteration"] for r in rows]

        ax1.plot(iters, [r["mean_loss"] for r in rows])
        ax1.set_xlabel("Iteration")
        ax1.set_ylabel("Loss")
        ax1.set_title("Training Loss")
        ax1.grid(True, alpha=0.3)

        ax2.plot(iters, [r["mean_reward"] for r in rows])
        ax2.set_xlabel("Iteration")
        ax2.set_ylabel("Reward")
        ax2.set_title("Mean Reward")
        ax2.grid(True, alpha=0.3)

        ax3.plot(iters, [r["accuracy"] for r in rows])
        ax3.set_xlabel("Iteration")
        ax3.set_ylabel("Accuracy")
        ax3.set_title("Policy Accuracy")
        ax3.set_ylim(0, 1.1)
        ax3.grid(True, alpha=0.3)

        plt.suptitle("Daft-Native Training Progress", fontsize=14, fontweight="bold")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "training.png"), dpi=150)
        plt.close()

        print(f"Saved: {output_dir}/training.png")

    except ImportError:
        pass


if __name__ == "__main__":
    main()
