---
title: Mission Trajectories
description: Persist, select, and grade lightweight mission evidence
---

**Document type:** Contract and user guide.

**Migration status:** The persistent `trajectory_id` Component schema and
runtime methods documented here are the implemented current, pre-PR-9
contract. The [accepted v0.5 target](application-architecture.md#lifetime-and-workflow-ownership)
replaces persistent trajectory identity with authoritative episode evidence:
`episode_id` persists, while a trajectory becomes a derived learning-facing
DataFrame. Until that owning migration lands, the current API below remains
the executable contract; the target is not a compatibility alias or a second
simultaneous persistence model.

A trajectory is a lightweight, typed index over what happened during a mission
or rollout. It is evidence: it can be queried and graded, but it never decides
whether a task advances.

Archetype provides separate schemas for headers, turns, commands, observations,
actions, and rewards; it does not hide trajectory evidence in one JSON
document. Small, already-safe evidence may be authored as Component rows. Raw
coding-agent transcripts use the artifact boundary described below: only a
sanitized file enters the common artifact index, while normalized narrative
lives in a typed ingestion table. Transcript ingestion does not silently add
entities to the mission graph.

## Ownership

| File | Responsibility |
|---|---|
| `archetype.missions.trajectories.components` | Persistent Arrow-safe schemas. |
| `archetype.missions.trajectories.contracts` | In-memory authoring values, structural inputs, and typed selection. |
| `archetype.missions.trajectories.claude` | Claude source configuration and pure parsing of already-sanitized text. No file I/O or durability. |
| `archetype.missions.trajectories.transforms` | Pure row and lazy DataFrame transforms; no service access. |
| `archetype.app.missions.trajectory_service` | Internal composition of persisted query access and evaluation graders. |
| `archetype.app.missions.transcript_service` | Internal snapshot, redaction, sanitized-file ingestion, and normalized-row workflow. |
| `RuntimeWorld.query_trajectory()` | Recommended filtered read path. |
| `RuntimeWorld.grade_trajectory()` | Recommended query-then-grade path. |
| `RuntimeWorld.ingest_claude_transcript()` | Recommended source-to-artifact workflow. |

The app service owns no trajectory truth. Query storage remains authoritative
for rows, evaluation remains authoritative for grader execution and receipts,
and mission processors remain authoritative for task transitions.

## Current pre-PR-9 persistent rows

| Component | One row represents |
|---|---|
| `Trajectory` | Header and coordinates for one trajectory. |
| `TrajectoryTurn` | One historical or explicitly authored conversational/tool-use turn. New raw-transcript ingestion does not write it. |
| `TranscriptArtifactRef` | Optional explicitly authored Component link to a sanitized transcript artifact. Raw-transcript ingestion does not write it. |
| `TrajectoryCommandEvent` | One command or audit event. |
| `TrajectoryObservation` | One observed tick or external event. |
| `TrajectoryAction` | One action aligned to the trajectory sequence. |
| `TrajectoryReward` | One reward observation. |

Under the current pre-PR-9 schema, every child row carries `trajectory_id` and
`seq`. This normalization keeps the tables independently queryable and avoids
rewriting a large payload whenever one observation changes. It describes the
preservation baseline, not the accepted v0.5 episode-owned replacement.

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
lazy frame to the evaluation family's pure grader runner.

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
content identity, and typed ingestion.

```python
from pathlib import Path

from archetype.missions.trajectories import ClaudeTranscriptSource

result = await world.ingest_claude_transcript(
    ClaudeTranscriptSource(
        path=Path("session.jsonl"),
        mission_id="mission-42",
    )
)
```

The missions family parses already-sanitized text into immutable `LoadedSession`
and `Turn` values. It neither opens the source path nor exposes a method that
turns the session into spawnable entities. The mission-owned application
workflow performs the stable snapshot, quarantine/redaction, complete parse,
sanitized artifact ingestion, and typed Iceberg append.

The common artifact index stores the sanitized file occurrence and its
content-addressed object URI. The `coding_agent_transcript_rows` table stores
one session row plus ordered turn rows linked by the same canonical
`trajectory_id` and `source_artifact_id`. The submitted local path is absent
from both durable tables. `TranscriptIngestionResult` returns the portable
`ArtifactRef`, linkage, row count, and redaction outcome;
`world.transcript_rows()` reads the normalized rows for analysis.

`TrajectoryTurn` remains the one class identity for historical Component rows
and deliberate safe authoring. Compatibility does not authorize a new raw
transcript writer. See [Transcript ingestion](artifacts.md#8-transcript-ingestion)
for ordering, redaction, occurrence identity, and failure semantics.
