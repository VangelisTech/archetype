---
title: Mission Trajectories
description: Persist, select, and grade lightweight mission evidence
---

A trajectory is a lightweight, typed index over what happened during a mission
or rollout. It is evidence: it can be queried and graded, but it never decides
whether a task advances.

Archetype provides separate schemas for headers, turns, commands, observations,
actions, and rewards; it does not hide trajectory evidence in one JSON
document. Small, already-safe evidence may be authored as Component rows. Raw
coding-agent transcripts use the artifact boundary described below: only a
lightweight Component index joins the mission graph, while sanitized narrative
lives in a typed artifact table.

## Ownership

| File | Responsibility |
|---|---|
| `archetype.missions.trajectories.components` | Persistent Arrow-safe schemas. |
| `archetype.missions.trajectories.contracts` | In-memory authoring values, structural inputs, and typed selection. |
| `archetype.missions.trajectories.claude` | Claude source configuration and pure parsing of already-sanitized text. No file I/O or durability. |
| `archetype.missions.trajectories.transforms` | Pure row and lazy DataFrame transforms; no service access. |
| `archetype.app.missions.trajectory_service` | Internal composition of persisted query access and evaluation graders. |
| `archetype.app.artifacts.transcript_service` | Internal file snapshot, redaction, claim, and typed-row workflow. |
| `RuntimeWorld.query_trajectory()` | Recommended filtered read path. |
| `RuntimeWorld.grade_trajectory()` | Recommended query-then-grade path. |
| `RuntimeWorld.ingest_claude_transcript()` | Recommended source-to-artifact workflow. |

The app service owns no trajectory truth. Query storage remains authoritative
for rows, evaluation remains authoritative for grader execution and receipts,
and mission processors remain authoritative for task transitions.

## Persistent rows

| Component | One row represents |
|---|---|
| `Trajectory` | Header and coordinates for one trajectory. |
| `TrajectoryTurn` | One historical or explicitly authored conversational/tool-use turn. New raw-transcript ingestion does not write it. |
| `TranscriptArtifactRef` | Lightweight trajectory/mission link to a sanitized typed transcript table and its source digest. |
| `TrajectoryCommandEvent` | One command or audit event. |
| `TrajectoryObservation` | One observed tick or external event. |
| `TrajectoryAction` | One action aligned to the trajectory sequence. |
| `TrajectoryReward` | One reward observation. |

Every child row carries `trajectory_id` and `seq`. This normalization keeps the
tables independently queryable and avoids rewriting a large payload whenever
one observation changes.

`Trajectory.episode_id` is retained as string-valued runtime metadata for
historical rows. It is not the integer dataset-episode identity in the
evaluation ontology.

## Author small, already-safe evidence

```python
from archetype import ArchetypeRuntime
from archetype.missions.trajectories import (
    Trajectory,
    TrajectoryReward,
    Turn,
    turns_to_components,
)

turns = [
    Turn(role="user", content="Fix the login regression", tokens=6),
    Turn(role="assistant", content="Patched and validated", tokens=18),
]
header = Trajectory.from_turns(
    "mission-42:task-auth:attempt-1",
    turns,
    run_id="run-42",
    task_id="auth",
    source="coding-agent",
    terminal=True,
    outcome="accepted",
)

async with ArchetypeRuntime() as runtime:
    world = runtime.world("mission-evidence", storage="./data")
    await world.spawn(header)
    await world.spawn_many(
        [[row] for row in turns_to_components(header.trajectory_id, turns)]
    )
    await world.spawn(
        TrajectoryReward(trajectory_id=header.trajectory_id, reward=1.0)
    )
    await world.run(steps=1)
```

The spawned values become visible together at the tick commit boundary. This
is an authoring path, not a transcript loader. Use artifact ingestion for
large, externally sourced, or potentially secret-bearing material.

## Select one trajectory table

`TrajectorySelection` is explicit and table-local. A filter must name a field
stored by the requested Component; Archetype does not perform an implicit join.

```python
from archetype.missions.trajectories import (
    TrajectoryReward,
    TrajectorySelection,
)

selection = TrajectorySelection(
    trajectory_ids=("mission-42:task-auth:attempt-1",),
)
rewards = await world.query_trajectory(
    TrajectoryReward,
    selection=selection,
)
```

The result is a lazy Daft DataFrame. The application service asks the query
service for persisted rows, then applies selection as DataFrame expressions;
it does not collect the frame.

For example, `TrajectoryReward` stores `trajectory_id` but not `task_id`.
Selecting rewards by `task_ids` therefore fails with a precise error. Query the
`Trajectory` header by task first, then carry the selected trajectory IDs into
the reward query.

## Grade a selection

`grade_trajectory()` performs the same read and selection, then delegates the
lazy frame to the evaluation service's grader runner.

```python
def total_reward(frame):
    rows = frame.collect().to_pylist()  # the grader chooses its execution boundary
    return sum(row["trajectoryreward__reward"] for row in rows)

outputs = await world.grade_trajectory(
    TrajectoryReward,
    selection=selection,
    graders=[total_reward],
)
```

Those outputs are ephemeral analysis. Use `world.evaluate()` with a
`GraderContract` when the result must become a durable evaluation receipt.

Sync scripts use the same names through `ArchetypeRuntime.sync()`.

## Pure transforms

The family provides structural transforms for existing command, audit,
episode, tick, action, and reward values. They accept only the fields they need
and do not import application DTOs:

```python
from archetype.missions.trajectories import (
    audit_rows_to_events,
    trajectory_from_episode_result,
)

header = trajectory_from_episode_result(episode, rollout_id="rollout-7")
events = audit_rows_to_events(audit_rows, trajectory_id=header.trajectory_id)
```

This keeps reusable evidence construction below the application layer while
allowing application models that satisfy the structural contracts to pass
through without translation objects.

## Transcript boundary

Raw coding-agent transcripts, tool inputs and outputs, frames, and other large
or secret-bearing content are artifacts. They require pre-durability redaction,
stable source/content identity, and typed artifact-table publication.

```python
from pathlib import Path

from archetype.missions.trajectories import ClaudeTranscriptSource

receipt = await world.ingest_claude_transcript(
    ClaudeTranscriptSource(
        path=Path("session.jsonl"),
        mission_id="mission-42",
    )
)
```

The missions family parses already-sanitized text into immutable `LoadedSession`
and `Turn` values. It neither opens the source path nor exposes a method that
turns the session into spawnable entities. The artifact-owned application
workflow performs the stable snapshot, quarantine/redaction, complete parse,
claim-backed publication, and typed Iceberg append.

The claim publishes one `Trajectory` header with `TranscriptArtifactRef` and
`AssetRef`. The normalized table then stores one session row plus ordered turn
rows linked by the same canonical `trajectory_id`. The local path is absent
from both. Query the lightweight header through `query_trajectory()` and the
narrative through `world.artifacts("coding_agent_transcript_rows")`.

`TrajectoryTurn` remains the one class identity for historical Component rows
and deliberate safe authoring. Compatibility does not authorize a new raw
transcript writer. See [Coding-agent transcript artifacts](artifacts.md#13-coding-agent-transcript-artifacts)
for ordering, redaction, replay, and recovery semantics.
