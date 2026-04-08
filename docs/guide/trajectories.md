---
title: Trajectory Analysis Pipeline
description: Evaluate and compare agent trajectories using LLM-based labeling
---

The trajectory pipeline evaluates recorded agent sessions (trajectories) using configurable LLM-based labeling techniques. It supports fork-based comparison for A/B testing different evaluation criteria.

## Core Concepts

### Trajectory

A `Trajectory` is a Component that stores a complete agent session:

```python
from archetype.trajectories.components import Trajectory, Turn

trajectory = Trajectory.from_turns(
    trajectory_id="session-abc123",
    turns=[
        Turn(role="user", content="Fix the login bug", tokens=12),
        Turn(role="assistant", content="I'll check auth.py", tokens=45),
        Turn(
            role="tool_call",
            content="",
            tool_name="Read",
            tool_input='{"path": "auth.py"}',
            tokens=8,
        ),
        Turn(
            role="tool_result",
            content="def login(): ...",
            tool_name="Read",
            tokens=120,
        ),
        Turn(role="assistant", content="Found the bug, applying fix", tokens=200),
    ],
    source="claude-code",
    outcome="success: fixed null check in login handler",
    tags=["bugfix", "auth"],
    metadata={"repo": "myapp", "duration_s": 45},
)
```

Key fields:

- **`trajectory_id`** -- External reference (e.g., session ID)
- **`source`** -- Origin system (`"claude-code"`, `"api"`, `"custom"`)
- **`turns_json`** -- JSON-serialized list of Turn dicts
- **`outcome`** -- Final result summary (`"success/failure/partial + description"`)
- **`tags_json`** -- JSON list of string tags for filtering
- **`total_turns`**, **`total_tokens`**, **`duration_seconds`** -- Denormalized metrics

### Turn

A dataclass representing one step in a trajectory:

| Field | Type | Description |
|-------|------|-------------|
| `role` | `str` | `"user"`, `"assistant"`, `"tool_call"`, `"tool_result"`, `"system"` |
| `content` | `str` | Main content of the turn |
| `tool_name` | `str \| None` | Tool called (for `tool_call`/`tool_result` roles) |
| `tool_input` | `str \| None` | JSON tool input |
| `tool_output` | `str \| None` | JSON tool output |
| `tokens` | `int` | Token count for this turn |
| `duration_ms` | `float` | Wall-clock duration |
| `error` | `str \| None` | Error message if present |
| `metadata` | `dict` | Arbitrary metadata |

### Label

A `Label` is a Component representing an evaluation dimension:

```python
from archetype.trajectories.components import Label

label = Label(
    technique="efficiency",
    description="Rate how directly the agent solved the task without backtracking",
)
```

After pipeline execution, the Label is populated with:

- **`value`** -- Categorical label (e.g., `"efficient"`, `"redundant"`)
- **`score`** -- Numeric score 0.0--1.0
- **`rationale`** -- LLM explanation of the rating

## Pipeline API

### Setup

```python
from archetype.trajectories.pipeline import TrajectoryPipeline

pipeline = TrajectoryPipeline(
    name="eval-run",
    storage_uri="./trajectory_data",
    model="gpt-5-mini",
)
```

### Add Labeling Techniques

```python
pipeline.label("efficiency", "Rate how directly the agent reached the solution (0=wasteful, 1=optimal)")
pipeline.label("correctness", "Did the agent produce a correct final result? (0=wrong, 1=correct)")
```

Each technique creates a separate Label entity per trajectory, so 3 trajectories with 2 techniques = 6 entities.

### Configure Sampling

```python
pipeline.sample(
    max_trajectories=100,   # Cap at 100 (0 = all)
    min_turns=3,            # Skip trivial sessions
    max_turns=500,          # Skip extremely long sessions
    require_tags=["bugfix"],# Must have all these tags
    exclude_tags=["wip"],   # Exclude if any present
    outcome_filter="success",# Substring match on outcome
)
```

Sampling marks entities as `sampled=True/False` without dropping rows, so unsampled trajectories still appear in results (with default label values).

### Ingest Trajectories

```python
trajectories = [trajectory_1, trajectory_2, trajectory_3]
cmd_ids = await pipeline.ingest(trajectories)
```

This creates `len(trajectories) * len(techniques)` entities via SPAWN commands.

### Run the Pipeline

```python
await pipeline.run(steps=1)
```

One step executes all three processors in priority order:

| Processor | Priority | Purpose |
|-----------|----------|---------|
| **SamplingProcessor** | 10 | Marks which trajectories to evaluate |
| **LabelingProcessor** | 20 | Calls LLM to produce value/score/rationale |
| **ScoringProcessor** | 30 | Clamps scores to [0, 1] |

### Collect Results

```python
results = await pipeline.results()
for r in results:
    print(f"[{r['technique']}] {r['trajectory_id']}: "
          f"score={r['score']:.2f} — {r['rationale'][:80]}")
```

For advanced queries, use the raw DataFrame:

```python
df = await pipeline.results_df()
```

### Shutdown

```python
await pipeline.shutdown()
```

## Fork-Based Comparison

Fork an existing pipeline to create an independent branch with different evaluation criteria:

```python
# Original pipeline
pipeline = TrajectoryPipeline(name="baseline", storage_uri="./data")
pipeline.label("correctness", "Did the agent produce correct output?")
await pipeline.ingest(trajectories)
await pipeline.run()

# Fork with stricter criteria
strict = await pipeline.fork("strict-eval")
strict.label("correctness", "Binary: 1.0 if exactly right, 0.0 otherwise. No partial credit.")
await strict.run()

# Compare
baseline_results = await pipeline.results()
strict_results = await strict.results()
```

Under the hood, `fork()`:

1. Creates a new `TrajectoryPipeline` with a fresh world
2. Calls `WorldService.fork_world()` to clone the source world's state
3. Copies label specs and sampling config
4. Allows you to override techniques before running

Both worlds persist to the same storage, enabling post-hoc comparison and analysis.

## When to Use Trajectories

| Scenario | Use Trajectories? |
|----------|-------------------|
| Evaluating recorded agent sessions | Yes |
| Comparing labeling criteria (A/B) | Yes, with `fork()` |
| Benchmarking prompt variations | Yes |
| Real-time agent processing per tick | No, use regular Processors |
| Simple data transforms | No, use DataFrame expressions |

## Full Example

```python
import asyncio
from archetype.trajectories.components import Trajectory, Turn
from archetype.trajectories.pipeline import TrajectoryPipeline


async def main():
    # Build trajectories from recorded sessions
    t1 = Trajectory.from_turns(
        trajectory_id="session-001",
        turns=[
            Turn(role="user", content="Add dark mode", tokens=8),
            Turn(role="assistant", content="I'll update the theme system", tokens=150),
            Turn(role="tool_call", tool_name="Edit", content="", tokens=10),
            Turn(role="assistant", content="Done, dark mode is ready", tokens=30),
        ],
        source="claude-code",
        outcome="success: dark mode implemented",
        tags=["feature", "frontend"],
    )

    t2 = Trajectory.from_turns(
        trajectory_id="session-002",
        turns=[
            Turn(role="user", content="Add dark mode", tokens=8),
            Turn(role="assistant", content="Let me try...", tokens=100),
            Turn(role="assistant", content="That didn't work, trying again", tokens=200),
            Turn(role="assistant", content="Third attempt...", tokens=300),
            Turn(role="assistant", content="Finally got it working", tokens=50),
        ],
        source="claude-code",
        outcome="success: dark mode implemented after retries",
        tags=["feature", "frontend"],
    )

    # Set up pipeline
    pipeline = TrajectoryPipeline(name="demo", storage_uri="./trajectory_data")
    pipeline.label("efficiency", "Rate how directly the agent solved the task")
    pipeline.label("correctness", "Did the agent produce a correct result?")

    # Run
    await pipeline.ingest([t1, t2])
    await pipeline.run()

    # Results
    for r in await pipeline.results():
        print(f"[{r['technique']}] {r['trajectory_id']}: {r['score']:.2f}")

    # Fork with different criteria
    fork = await pipeline.fork("strict")
    fork.label("efficiency", "Penalize any backtracking. 1.0 only if zero wasted turns.")
    await fork.run()

    for r in await fork.results():
        print(f"[strict/{r['technique']}] {r['trajectory_id']}: {r['score']:.2f}")

    await pipeline.shutdown()
    await fork.shutdown()


asyncio.run(main())
```
