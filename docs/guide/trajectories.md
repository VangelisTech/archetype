# Trajectory Pipeline

The `TrajectoryPipeline` turns agent session data into an Archetype world. Each trajectory becomes an entity. Processors label, score, and filter it. Fork the world to compare labeling techniques side-by-side.

## When to Use Trajectories vs Regular Processors

| Use trajectories when… | Use regular processors when… |
|------------------------|------------------------------|
| You have a fixed dataset of past agent sessions to evaluate | You're running a live simulation that evolves over time |
| You want LLM-based labeling or quality scoring | Your logic is purely deterministic |
| You need to compare multiple evaluation techniques | You have a single, stable processing pipeline |
| You want fork-based A/B comparison of labeling prompts | You don't need to branch worlds |

## Core Components

### `Trajectory`

A complete agent session stored as an entity. Complex nested data (turns, tags, metadata) is JSON-encoded for Arrow/LanceDB compatibility.

```python
from archetype.trajectories import Trajectory
from archetype.trajectories.components import Turn

trajectory = Trajectory.from_turns(
    trajectory_id="session-001",
    source="claude-code",
    outcome="success: implemented feature",
    tags=["feature", "python"],
    metadata={"repo": "acme/api", "model": "claude-sonnet-4-6"},
    turns=[
        Turn(role="user", content="Add a /health endpoint", tokens=12),
        Turn(role="assistant", content="I'll add that now.", tokens=8),
        Turn(
            role="tool_call",
            content="Reading app/main.py",
            tool_name="Read",
            tool_input='{"path": "app/main.py"}',
            tokens=5,
            duration_ms=120,
        ),
        Turn(role="tool_result", content="from fastapi import FastAPI...", tokens=50),
        Turn(role="assistant", content="Done. Endpoint returns {\"status\": \"ok\"}.", tokens=15),
    ],
)
```

Key fields:

| Field | Type | Description |
|-------|------|-------------|
| `trajectory_id` | `str` | External reference (session ID, run ID, etc.) |
| `source` | `str` | Origin system: `"claude-code"`, `"api"`, `"custom"` |
| `turns_json` | `str` | JSON-encoded list of `Turn` dicts |
| `total_turns` | `int` | Turn count (denormalized for fast filtering) |
| `total_tokens` | `int` | Sum of tokens across all turns |
| `duration_seconds` | `float` | Wall-clock session duration |
| `outcome` | `str` | Final outcome: `"success: ..."`, `"failure: ..."` |
| `tags_json` | `str` | JSON-encoded list of string tags |
| `metadata_json` | `str` | JSON-encoded dict of arbitrary metadata |

### `Turn`

A single turn in an agent session. Not a Component — used to build `Trajectory.turns_json`.

```python
from archetype.trajectories.components import Turn

Turn(
    role="tool_call",         # "user", "assistant", "tool_call", "tool_result", "system"
    content="Reading file",
    tool_name="Read",
    tool_input='{"path": "main.py"}',
    tokens=5,
    duration_ms=120,
    error=None,               # Set if the turn errored
)
```

### `Label`

Attached to a `Trajectory` entity — one per labeling technique. Filled in by `LabelingProcessor`.

```python
from archetype.trajectories.components import Label

label = Label(
    technique="efficiency",
    description="Rate how directly the agent reached the solution without backtracking.",
)
# After LabelingProcessor runs:
# label.value     → "high efficiency"
# label.score     → 0.85
# label.rationale → "Agent identified the correct approach immediately."
# label.sampled   → True (set by SamplingProcessor)
```

## Pipeline Processors

Three processors run in priority order within a single tick.

### `SamplingProcessor` (priority 10)

Selects which trajectories to evaluate. Sets `label__sampled = True/False`. Never drops rows — unsampled entities are preserved but skipped by downstream processors.

Controlled by `SamplingConfig` injected into `world.resources`:

```python
from archetype.trajectories.processors import SamplingConfig

pipeline.sample(
    min_turns=3,              # Only trajectories with ≥3 turns
    max_turns=50,             # Only trajectories with ≤50 turns
    max_trajectories=100,     # Cap at 100 (deterministic: first N)
    require_tags=["python"],  # Must have ALL of these tags
    exclude_tags=["failed"],  # Exclude ANY of these tags
    outcome_filter="success", # outcome must contain this substring
)
```

### `LabelingProcessor` (priority 20)

Calls an LLM on each sampled trajectory. Reads `label__description` as the evaluation prompt and writes `label__value`, `label__score`, and `label__rationale`.

The LLM prompt is structured as:
```
You are an expert evaluator of AI agent trajectories.

## Evaluation Technique
<technique>: <description>

## Trajectory
Source: <source>
Outcome: <outcome>
Total turns: <total_turns>
...

## Instructions
Evaluate this trajectory...
VALUE: <categorical label>
SCORE: <float 0.0 to 1.0>
RATIONALE: <1-2 sentence explanation>
```

Controlled by `LabelingConfig`:

```python
from archetype.trajectories.processors import LabelingConfig

# Injected automatically by TrajectoryPipeline with the model you specify:
LabelingConfig(model="gpt-5-mini", max_output_tokens=512)
```

### `ScoringProcessor` (priority 30)

Post-processes scores after labeling. Currently clamps scores to `[0.0, 1.0]`. Extend this for cross-trajectory normalization, percentile ranking, or aggregated metrics.

## `TrajectoryPipeline` API

The high-level interface. Each pipeline is backed by a world.

### Constructor

```python
from archetype.trajectories import TrajectoryPipeline

pipeline = TrajectoryPipeline(
    name="my-eval",          # World name (used as storage namespace)
    storage_uri="./traj_data",  # LanceDB storage path
    model="gpt-5-mini",      # LLM for labeling
)
```

### `.label(technique, description)` → `TrajectoryPipeline`

Add a labeling technique. One entity is created per (trajectory, technique) pair. Chainable.

```python
pipeline = (
    TrajectoryPipeline(name="eval")
    .label("efficiency", "Rate how directly the agent reached the solution.")
    .label("correctness", "Did the agent produce the correct final result?")
)
```

### `.sample(...)` → `TrajectoryPipeline`

Configure sampling. Chainable. Can be called before or after `ingest()`.

```python
pipeline.sample(min_turns=3, max_trajectories=50)
```

### `await pipeline.ingest(trajectories)` → `list[UUID]`

Spawn entities into the world — one per (trajectory, technique) pair. Returns command IDs.

```python
trajectories = [traj1, traj2, traj3]
await pipeline.ingest(trajectories)
# With 2 techniques: 3 × 2 = 6 entities created
```

### `await pipeline.run(steps=1)`

Run the pipeline. One step runs all three processors: sample → label → score.

```python
await pipeline.run()
```

### `await pipeline.results()` → `list[dict]`

Collect results as a list of dicts:

```python
results = await pipeline.results()
for r in results:
    print(f"[{r['technique']}] {r['trajectory_id']}: score={r['score']:.2f}")
    print(f"  value={r['value']!r}")
    print(f"  rationale={r['rationale']}")
```

Each dict has:

| Key | Description |
|-----|-------------|
| `trajectory_id` | External trajectory ID |
| `technique` | Labeling technique name |
| `description` | Labeling prompt used |
| `value` | Categorical label from LLM |
| `score` | Numeric score 0.0–1.0 |
| `rationale` | LLM explanation |
| `sampled` | Whether this entity was selected by the SamplingProcessor |
| `total_turns` | Turn count |
| `outcome` | Original outcome string |
| `source` | Origin system |

### `await pipeline.results_df()` → `DataFrame`

Returns the raw Daft DataFrame for advanced queries:

```python
df = await pipeline.results_df()
# Filter, aggregate, join — full Daft API available
high_scoring = df.where(df["label__score"] > 0.8).collect()
```

### `await pipeline.fork(new_name)` → `TrajectoryPipeline`

Fork this pipeline into a new world. The fork starts with the same entities and a copy of the current state. Modify the fork's labels and run it independently to compare results.

```python
fork = await pipeline.fork("strict-eval")
fork._labels = []  # Clear inherited labels
fork.label("correctness", "Binary only: 1.0 if exactly right, 0.0 otherwise")
await fork.run()

# Both worlds coexist — compare side by side
original_results = await pipeline.results()
strict_results   = await fork.results()
```

### `await pipeline.shutdown()`

Clean up the container and storage connections.

## Full Example

```python
import asyncio
from archetype.trajectories import Trajectory, TrajectoryPipeline
from archetype.trajectories.components import Turn


async def main():
    # 1. Build trajectories
    trajectories = [
        Trajectory.from_turns(
            trajectory_id="traj-001",
            source="claude-code",
            outcome="success: feature implemented",
            tags=["feature", "python"],
            turns=[
                Turn(role="user", content="Add /health endpoint", tokens=12),
                Turn(role="assistant", content="Done.", tokens=8),
            ],
        ),
        Trajectory.from_turns(
            trajectory_id="traj-002",
            source="claude-code",
            outcome="failure: gave up after 3 attempts",
            tags=["bugfix", "python"],
            turns=[
                Turn(role="user", content="Fix the 500 error", tokens=10),
                Turn(role="assistant", content="I can't fix this.", tokens=15),
                Turn(role="assistant", content="Giving up.", tokens=5),
            ],
        ),
    ]

    # 2. Build pipeline
    pipeline = (
        TrajectoryPipeline(name="my-eval", storage_uri="./eval_data")
        .sample(min_turns=2)
        .label("efficiency", "Rate how directly the agent reached the solution.")
        .label("correctness", "Did the agent produce the correct result?")
    )

    # 3. Ingest
    await pipeline.ingest(trajectories)

    # 4. Run (requires OPENAI_API_KEY or compatible model endpoint)
    try:
        await pipeline.run()
    except Exception as e:
        print(f"LLM call failed (expected without API key): {e}")

    # 5. Results
    for r in await pipeline.results():
        print(f"[{r['technique']}] {r['trajectory_id']}: {r['score']:.2f}")

    # 6. Fork to try a stricter labeling prompt
    fork = await pipeline.fork("strict-eval")
    fork._labels = [("correctness", "Binary only: 1.0 if output exactly matches expected.")]
    await fork.run()

    await pipeline.shutdown()
    await fork.shutdown()


asyncio.run(main())
```

Run the bundled example:

```bash
export OPENAI_API_KEY=sk-...
uv run python examples/trajectories/run.py
```

## Fork-Based Comparison Workflow

Forking lets you run the exact same trajectory dataset through different evaluation lenses:

```
Original world                Fork world
──────────────                ──────────
label: "efficiency"     →     label: "efficiency (strict)"
label: "correctness"    →     label: "correctness (binary)"
SamplingProcessor             SamplingProcessor (same config)
LabelingProcessor             LabelingProcessor (new prompts)
ScoringProcessor              ScoringProcessor
        │                             │
        ▼                             ▼
  results_df()              results_df()
        └──────────────────────────────┘
                  compare()
```

Both worlds live in LanceDB side by side. Query them independently or join on `trajectory_id` to compare scores.

## Storage Layout

Each pipeline stores data under `{storage_uri}/trajectories/`. Each entity is one row in the Arrow table, with columns:

```
entity_id          -- ECS entity identifier
world_id           -- which world (original or fork)
tick               -- tick this state was written at
is_active          -- True for live entities
trajectory__*      -- Trajectory component fields
label__*           -- Label component fields
```
