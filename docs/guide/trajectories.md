---
title: Trajectory Analysis
description: Evaluate and compare agent trajectories using LLM-based labeling
---

Trajectory analysis uses the standard ECS pattern: define components for the data, processors for the pipeline stages, and run it all through a world. Fork the world to compare different evaluation criteria.

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

Standard ECS setup — no framework abstraction:

```python
container = ServiceContainer()
ctx = ActorCtx(id=uuid7(), roles={"operator"})

world = await container.world_service.create_world(
    WorldConfig(name="trajectory-eval"),
    StorageConfig(uri="./trajectory_data", namespace="trajectories"),
)

# Add processors
await world.system.add_processor(SamplingProcessor())
await world.system.add_processor(LabelingProcessor())
await world.system.add_processor(ScoringProcessor())

# Inject config
world.resources.insert(SamplingConfig(min_turns=3))
world.resources.insert(LabelingConfig(model="gpt-5-mini"))

# Spawn one entity per (trajectory, technique) pair
for trajectory in trajectories:
    for technique, description in label_specs:
        label = Label(technique=technique, description=description)
        cmd = Command(
            type=CommandType.SPAWN,
            payload={
                "components": [
                    {"type": "Trajectory", **trajectory.model_dump()},
                    {"type": "Label", **label.model_dump()},
                ],
            },
        )
        await container.command_service.submit(world.world_id, cmd, ctx)

# Run: one tick = sample -> label -> score
await container.simulation_service.step(
    world.world_id, RunConfig(num_steps=1, prefer_live_reads=True),
)

# Collect results
df = await world.get_components([Trajectory, Label])
rows = df.collect().to_pylist()
```

## Fork-Based Comparison

Clone the world, swap config, run independently:

```python
fork = await container.world_service.fork_world(
    source_world_id=world.world_id,
    name="strict-eval",
    storage_config=StorageConfig(uri="./trajectory_data", namespace="trajectories"),
)

# Re-add processors (not cloned)
await fork.system.add_processor(SamplingProcessor())
await fork.system.add_processor(LabelingProcessor())
await fork.system.add_processor(ScoringProcessor())

# Different config
fork.resources.insert(SamplingConfig(min_turns=3))
fork.resources.insert(LabelingConfig(model="gpt-5-mini"))

await container.simulation_service.step(
    fork.world_id, RunConfig(num_steps=1, prefer_live_reads=True),
)
```

Both worlds persist to the same storage. Query either one at any tick.

## When to Use

| Scenario | Trajectory analysis? |
|----------|---------------------|
| Evaluating recorded agent sessions | Yes |
| Comparing labeling criteria (A/B) | Yes, with `fork_world()` |
| Benchmarking prompt variations | Yes |
| Real-time agent processing per tick | No, use regular processors |
| Simple data transforms | No, use DataFrame expressions |
