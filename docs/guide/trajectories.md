---
title: Mission Trajectories
description: Persist, select, and grade lightweight episode evidence
---

**Document type:** Contract and user guide.

`episode_id` is the one persistent identity of a bounded Mission or hosted
Physical-AI execution. Evidence rows are stored per episode; a trajectory is a
derived, learning-facing DataFrame view over that evidence and has no
persistent identity of its own. There is no persistent `trajectory_id`.

Evidence can be queried and graded, but it never decides whether a task
advances.

Archetype provides separate typed schemas for turns, commands, observations,
actions, and rewards; it does not hide evidence in one JSON document. Small,
already-safe evidence may be authored as Component rows. Raw coding-agent
transcripts use the separate [transcript ingestion contract](../missions/transcripts.md):
only a sanitized file enters the common artifact index, while normalized
narrative lives in a typed ingestion table. Transcript ingestion does not
silently add entities to the mission graph.

## Ownership

| File | Responsibility |
|---|---|
| `archetype.missions.trajectories.components` | Persistent Arrow-safe evidence schemas keyed by `episode_id` and `seq`. |
| `archetype.missions.trajectories.contracts` | In-memory authoring values, structural inputs, and typed selection. |
| `archetype.missions.trajectories.claude` | Claude source configuration and pure parsing of already-sanitized text. No file I/O or durability. |
| `archetype.missions.trajectories.transforms` | Pure row transforms and the derived `trajectory(...)` view; no service access. |
| `archetype.missions.trajectory_service` | Internal composition of persisted query access and evaluation graders. |
| `archetype.missions.transcript_service` | Internal snapshot, redaction, sanitized-file ingestion, and normalized-row workflow. |
| `MissionWorld.query_trajectory()` | Recommended filtered read path. |
| `MissionWorld.grade_trajectory()` | Recommended query-then-grade path. |
| `MissionWorld.ingest_claude_transcript()` | Recommended source-to-artifact workflow. |

The family-owned query and transcript services own no evidence truth. Persisted
storage remains authoritative for rows, evaluation remains authoritative for
grader execution and receipts, and mission processors remain authoritative for
task transitions.

## Persistent evidence rows

| Component | One row represents |
|---|---|
| `TrajectoryTurn` | One explicitly authored conversational/tool-use turn. Raw-transcript ingestion does not write it. |
| `TranscriptArtifactRef` | Optional explicitly authored Component link to a sanitized transcript artifact. Raw-transcript ingestion does not write it. |
| `TrajectoryCommandEvent` | One command or audit event. |
| `TrajectoryObservation` | One observed tick or external event. |
| `TrajectoryAction` | One action aligned to the evidence sequence. |
| `TrajectoryReward` | One reward observation. |

Every row carries `episode_id` and `seq`. This normalization keeps the tables
independently queryable and avoids rewriting a large payload whenever one
observation changes.

The world runtime's `EpisodeResult.episode_id` is a runtime lifecycle
identifier (a string). It is not the integer dataset-episode index in the
[evaluation ontology](dataset-eval-ontology.md); the two stay type-distinct.

## Author small, already-safe evidence

```python
from archetype import ArchetypeRuntime
from archetype.missions.trajectories import (
    TrajectoryReward,
    Turn,
    turns_to_components,
)

episode_id = "episode-auth-1"
turns = [
    Turn(role="user", content="Fix the login regression", tokens=6),
    Turn(role="assistant", content="Patched and validated", tokens=18),
]

async with ArchetypeRuntime() as runtime:
    world = runtime.world("mission-evidence", storage="./data")
    await world.spawn_many(
        [[row] for row in turns_to_components(episode_id, turns)]
    )
    await world.spawn(
        TrajectoryReward(episode_id=episode_id, reward=1.0)
    )
    await world.run(steps=1)
```

The spawned values become visible together at the tick commit boundary. This
is an authoring path, not a transcript loader. Use artifact ingestion for
large, externally sourced, or potentially secret-bearing material.

## Select one evidence table

`TrajectorySelection` is explicit and table-local: it filters one typed
evidence table by `episode_id`. Archetype does not perform an implicit join.

```python
from archetype.missions import MissionWorld
from archetype.missions.trajectories import (
    TrajectoryReward,
    TrajectorySelection,
)

evidence = MissionWorld(world)
selection = TrajectorySelection(episode_ids=("episode-auth-1",))
rewards = await evidence.query_trajectory(
    TrajectoryReward,
    selection=selection,
)
```

The result is a lazy Daft DataFrame. `TrajectoryService` asks
`archetype.world.query` for persisted rows, then applies selection as DataFrame
expressions; it does not collect the frame. A filter against a Component that
does not store `episode_id` fails with a precise error.

## Derive one trajectory

`trajectory(...)` is the derived view: one episode's seq-ordered evidence,
reconstructed lazily from a persisted evidence table.

```python
from archetype.missions.trajectories import TrajectoryTurn, trajectory

turns = await evidence.query_trajectory(TrajectoryTurn)
ordered = trajectory(turns, TrajectoryTurn, episode_id="episode-auth-1")
```

The view is a lazy DataFrame; nothing runs until the caller materializes it.

## Grade a selection

`grade_trajectory()` performs the same read and selection, then delegates the
lazy frame to the evaluation family's pure grader runner.

```python
def total_reward(frame):
    rows = frame.collect().to_pylist()  # the grader chooses its execution boundary
    return sum(row["trajectoryreward__reward"] for row in rows)

outputs = await evidence.grade_trajectory(
    TrajectoryReward,
    selection=selection,
    graders=[total_reward],
)
```

Those outputs are ephemeral analysis. Use `world.evaluate()` with a
`GraderContract` when the result must become a durable evaluation receipt.

`MissionWorld` is an async typed adapter. Installing Missions does not add
trajectory methods to generic async or sync world handles.

## Pure transforms

The family provides structural transforms for existing command, audit, tick,
action, and reward values. They accept only the fields they need and do not
import application DTOs:

```python
from archetype.missions.trajectories import audit_rows_to_events

events = audit_rows_to_events(audit_rows, episode_id="episode-auth-1")
```

This keeps reusable evidence construction below the application layer while
allowing application models that satisfy the structural contracts to pass
through without translation objects.

## Transcript ingestion

Raw coding-agent transcripts use a separate Missions-owned workflow for
pre-durability redaction, artifact publication, and normalized row storage.
See the normative [Transcript ingestion contract](../missions/transcripts.md).
Transcript ingestion remains distinct from deliberate `TrajectoryTurn`
authoring and never silently adds entities to the mission graph.
