---
title: Trajectory Analysis
description: Evaluate and compare agent trajectories using LLM-based labeling
---

Trajectory analysis uses the recommended runtime script pattern: define
components for the data, stage processors and resources on a runtime world,
run the pipeline, then fork to compare evaluation criteria.

The full runnable example is in [`examples/06_trajectory_analysis.py`](https://github.com/VangelisTech/archetype/blob/main/examples/06_trajectory_analysis.py).

## Components

### Trajectory

Stores a complete agent session as JSON-encoded turns:

```python
class Trajectory(Component):
    trajectory_id: str = ""
    source: str = ""
    turns_json: str = "[]"
    total_turns: int = 0
    total_tokens: int = 0
    duration_seconds: float = 0.0
    outcome: str = ""
    tags_json: str = "[]"
    metadata_json: str = "{}"
```

Build from structured `Turn` dataclasses:

```python
trajectory = Trajectory.from_turns(
    trajectory_id="session-abc123",
    turns=[
        Turn(role="user", content="Fix the login bug", tokens=12),
        Turn(role="assistant", content="I'll check auth.py", tokens=45),
        Turn(role="tool_call", tool_name="Read",
             tool_input='{"path": "auth.py"}', content="", tokens=8),
        Turn(role="tool_result", content="def login(): ...", tokens=120),
        Turn(role="assistant", content="Found the bug, applying fix", tokens=200),
    ],
    source="claude-code",
    outcome="success: fixed null check in login handler",
    tags=["bugfix", "auth"],
)
```

### Turn

A dataclass (not a Component) representing one step in a trajectory:

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

An evaluation result attached to a trajectory:

```python
class Label(Component):
    technique: str = ""
    description: str = ""
    value: str = ""
    score: float = 0.0
    rationale: str = ""
    sampled: bool = True
```

Each `(Trajectory, Label)` entity represents one labeling technique applied to one trajectory. To compare techniques, fork the world and swap the `Label.description`.

## Processors

Three pipeline stages, priority-ordered within a single tick:

| Processor | Priority | Purpose |
|-----------|----------|---------|
| `SamplingProcessor` | 10 | Marks which trajectories to evaluate based on `SamplingConfig` |
| `LabelingProcessor` | 20 | Calls LLM to produce value/score/rationale for sampled entities |
| `ScoringProcessor` | 30 | Clamps scores to [0, 1] |

### SamplingProcessor

Reads `SamplingConfig` from resources and sets `label__sampled = True/False`. Never drops rows — all entities are preserved for post-hoc analysis.

```python
@dataclass
class SamplingConfig:
    max_trajectories: int = 0    # 0 = all
    min_turns: int = 0
    max_turns: int = 0           # 0 = no limit
    require_tags: list[str] | None = None
    exclude_tags: list[str] | None = None
    outcome_filter: str | None = None
```

### LabelingProcessor

Reads `LabelingConfig` from resources. Splits the DataFrame into sampled/unsampled, calls `daft.functions.prompt` on sampled rows with the evaluation prompt, parses the response into `label__value`, `label__score`, `label__rationale`, and rejoins.

```python
@dataclass
class LabelingConfig:
    model: str = "gpt-5-mini"
    max_output_tokens: int = 512
```

### ScoringProcessor

Clamps `label__score` to [0, 1].

## Wiring It Up

Recommended runtime setup:

```python
from daft.ai.openai.provider import OpenAIProvider

from archetype import ArchetypeRuntime
from archetype.core.config import RunConfig, StorageConfig

provider = OpenAIProvider(timeout=30.0, max_retries=2)

async with ArchetypeRuntime() as runtime:
    world = runtime.world(
        "trajectory-eval",
        storage=StorageConfig(uri="./trajectory_data", namespace="trajectories"),
        processors=[SamplingProcessor(), LabelingProcessor(provider), ScoringProcessor()],
        resources=[
            SamplingConfig(min_turns=3),
            LabelingConfig(model="gpt-5-mini"),
        ],
    )

    for trajectory in trajectories:
        for technique, description in label_specs:
            label = Label(technique=technique, description=description)
            await world.spawn(trajectory, label)

    await world.step(config=RunConfig(num_steps=1))

    df = await world.query(Trajectory, Label)
    rows = df.collect().to_pylist()
```

## Fork-Based Comparison

Clone the world and run an independent branch:

```python
fork = await world.fork(
    "strict-eval",
    storage=StorageConfig(uri="./trajectory_data", namespace="trajectories"),
)
await fork.step(config=RunConfig(num_steps=1))
```

Forks share resource instances by default. For a strict-vs-lenient comparison, stage distinct resources on separate worlds or attach replacement resources through the gated resource-management path before running the fork.

Both worlds persist to the same storage by default, partitioned by `world_id`. Query either one at any tick.

## When to Use

| Scenario | Trajectory analysis? |
|----------|---------------------|
| Evaluating recorded agent sessions | Yes |
| Comparing labeling criteria (A/B) | Yes, with `world.fork()` |
| Benchmarking prompt variations | Yes |
| Real-time agent processing per tick | No, use regular processors |
| Simple data transforms | No, use DataFrame expressions |
