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
from archetype import ArchetypeRuntime
from archetype.core.config import RunConfig, StorageConfig

async with ArchetypeRuntime() as runtime:
    world = runtime.world(
        "trajectory-eval",
        storage=StorageConfig(uri="./trajectory_data", namespace="trajectories"),
        processors=[SamplingProcessor(), LabelingProcessor(), ScoringProcessor()],
        resources=[
            SamplingConfig(min_turns=3),
            LabelingConfig(model="gpt-5-mini"),
        ],
    )

    for trajectory in trajectories:
        for technique, description in label_specs:
            label = Label(technique=technique, description=description)
            await world.spawn(trajectory, label)

    await world.step(config=RunConfig(num_steps=1, prefer_live_reads=True))

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
await fork.step(config=RunConfig(num_steps=1, prefer_live_reads=True))
```

Forks share resource instances by default. For a strict-vs-lenient comparison, stage distinct resources on separate worlds or attach replacement resources through the gated resource-management path before running the fork.

Both worlds persist to the same storage by default, partitioned by `world_id`. Query either one at any tick.

## Single Ego Trajectories

A single ego trajectory is a normalized path through attention, pressure,
agency, and commitment. It is useful when the source is not only a chat log:
a screen capture stream, browser session, robot episode, or desktop observer
can all emit the same row shape.

The ego surface lives in `archetype.experiments.ego` and is re-exported from
`archetype.experiments`. Capture libraries stay outside this module: adapters
only need to implement the `EgoObservationSource` protocol and return
normalized `EgoObservation` rows.

```python
from archetype.experiments import (
    EgoObservation,
    EgoObservationSource,
    derive_ego_labels,
    derive_ego_trajectory_pattern,
)

observations = [
    EgoObservation.from_screen_frame(
        "ego-1",
        0,
        "frames/000.png",
        focus="watching",
        salience=0.7,
        agency=0.2,
    ),
    EgoObservation.from_screen_frame(
        "ego-1",
        1,
        "frames/001.png",
        focus="metric",
        salience=0.8,
        effort=0.9,
        agency=0.2,
        external_pressure=0.9,
    ),
    EgoObservation.from_screen_frame(
        "ego-1",
        2,
        "frames/002.png",
        focus="question",
        salience=0.8,
        effort=0.7,
        agency=0.55,
        external_pressure=0.7,
    ),
]

labels = derive_ego_labels(observations)
pattern = derive_ego_trajectory_pattern(observations, labels)
```

### EgoObservation

`EgoObservation` is a primitive, Arrow-friendly row:

| Field | Meaning |
|-------|---------|
| `trajectory_id`, `seq` | Sequence identity |
| `subject_id` | Optional ego / actor identity |
| `modality` | Source channel, for example `"screen"` |
| `frame_uri` | URI or path to the captured artifact |
| `focus`, `context` | Human-readable attention fields |
| `salience`, `valence`, `arousal` | Perceptual / affective scores |
| `effort`, `agency`, `external_pressure` | Humanism-oriented control scores |

Scores are clamped to stable ranges: unit scores in `[0, 1]`, valence in
`[-1, 1]`.

### Canonical Labels

`derive_ego_label()` maps each observation into an `EgoLabel`:

| Phase | Meaning |
|-------|---------|
| `witness` | The ego is seeing the field |
| `orient` | Attention has found a salient object |
| `strain` | External pressure is high while agency is low |
| `question` | Agency rises inside pressure |
| `commit` | Agency and effort align |
| `depart` | Agency remains high after pressure drops |

The key pattern is `instrumentalized_intelligence`: high effort under high
external pressure with low agency. Archetype treats that as a captured
trajectory, not as a better optimization path.

### Canonical Patterns

`derive_ego_trajectory_pattern()` summarizes the path:

| Pattern | Meaning |
|---------|---------|
| `captured_dream` | Instrumentalized intelligence remains low-agency |
| `reclaimed_dream` | The trajectory passes through capture and agency rises |
| `self_authored_dream` | Agency is high and pressure no longer dominates |
| `observed_trajectory` | The path has not crossed a stronger boundary |

This is the derivation behind the question "if a dream forces you to game
your own intelligence, are you human anymore?" In Archetype terms, the answer
is not a binary type check. The label asks whether the trajectory is still
self-authored.

### Structured Output Prompt

Use the structured-output contract when an LLM or capture adapter needs to
turn a scene, screen recording, or trace into ego observations:

```python
from archetype.experiments import (
    EGO_OBSERVATION_JSON_SCHEMA,
    EGO_OBSERVATION_OUTPUT_GRAMMAR,
    EGO_OBSERVATION_PROMPT,
    derive_ego_trajectory_pattern,
    ego_observations_from_structured_output,
)
```

The grammar root is `ego_trajectory_output`:

```ebnf
ego_trajectory_output ::= {
  "trajectory_id": string,
  "subject_id": string,
  "source": {
    "modality": "screen" | "conversation" | "browser" | "robot" | "desktop" | "text",
    "artifact_uri": string,
    "description": string
  },
  "observations": [
    {
      "seq": integer,
      "frame_uri": string,
      "focus": string,
      "context": string,
      "captured_at_ms": integer,
      "salience": 0.0..1.0,
      "valence": -1.0..1.0,
      "arousal": 0.0..1.0,
      "effort": 0.0..1.0,
      "agency": 0.0..1.0,
      "external_pressure": 0.0..1.0
    }
  ]
}
```

The prompt deliberately asks the model to emit observations only. It must not
emit labels, phases, or final patterns:

```python
structured = call_model(
    prompt=EGO_OBSERVATION_PROMPT,
    schema=EGO_OBSERVATION_JSON_SCHEMA,
    grammar=EGO_OBSERVATION_OUTPUT_GRAMMAR,
    input=scene_or_trace,
)

observations = ego_observations_from_structured_output(structured)
pattern = derive_ego_trajectory_pattern(observations)
```

That split is the replication boundary: the model scores perception; Archetype
derives whether the path is `captured_dream`, `reclaimed_dream`, or
`self_authored_dream`.

## When to Use

| Scenario | Trajectory analysis? |
|----------|---------------------|
| Evaluating recorded agent sessions | Yes |
| Comparing labeling criteria (A/B) | Yes, with `world.fork()` |
| Benchmarking prompt variations | Yes |
| Real-time agent processing per tick | No, use regular processors |
| Simple data transforms | No, use DataFrame expressions |
