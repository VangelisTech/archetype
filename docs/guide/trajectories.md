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
transcripts use the artifact boundary described below: only a sanitized file
enters the common artifact index, while normalized narrative lives in a typed
ingestion table. Transcript ingestion does not silently add entities to the
mission graph.

## Ownership

| File | Responsibility |
|---|---|
| `archetype.missions.trajectories.components` | Persistent Arrow-safe evidence schemas keyed by `episode_id` and `seq`. |
| `archetype.missions.trajectories.contracts` | In-memory authoring values, structural inputs, and typed selection. |
| `archetype.missions.trajectories.claude` | Claude source configuration and pure parsing of already-sanitized text. No file I/O or durability. |
| `archetype.missions.trajectories.transforms` | Pure row transforms and the derived `trajectory(...)` view; no service access. |
| `archetype.app.missions.trajectory_service` | Internal composition of persisted query access and evaluation graders. |
| `archetype.app.missions.transcript_service` | Internal snapshot, redaction, sanitized-file ingestion, and normalized-row workflow. |
| `RuntimeWorld.query_trajectory()` | Recommended filtered read path. |
| `RuntimeWorld.grade_trajectory()` | Recommended query-then-grade path. |
| `RuntimeWorld.ingest_claude_transcript()` | Recommended source-to-artifact workflow. |

The app service owns no evidence truth. Query storage remains authoritative
for rows, evaluation remains authoritative for grader execution and receipts,
and mission processors remain authoritative for task transitions.

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
from archetype.missions.trajectories import (
    TrajectoryReward,
    TrajectorySelection,
)

selection = TrajectorySelection(episode_ids=("episode-auth-1",))
rewards = await world.query_trajectory(
    TrajectoryReward,
    selection=selection,
)
```

The result is a lazy Daft DataFrame. The application service asks the query
service for persisted rows, then applies selection as DataFrame expressions;
it does not collect the frame. A filter against a Component that does not
store `episode_id` fails with a precise error.

## Derive one trajectory

`trajectory(...)` is the derived view: one episode's seq-ordered evidence,
reconstructed lazily from a persisted evidence table.

```python
from archetype.missions.trajectories import TrajectoryTurn, trajectory

turns = await world.query_trajectory(TrajectoryTurn)
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

One ingested Claude session is one bounded execution; its stable
`episode_id` is the canonical `claude-session://` source URI
(`ClaudeTranscriptSource.episode_id`).

The common artifact index stores the sanitized file occurrence and its
content-addressed object URI. The `coding_agent_transcript_rows` table stores
one session row plus ordered turn rows linked by the same canonical
`episode_id` and `source_artifact_id`. The submitted local path is absent
from both durable tables. `TranscriptIngestionResult` returns the portable
`ArtifactRef`, linkage, row count, and redaction outcome;
`world.transcript_rows()` reads the normalized rows for analysis.

`TrajectoryTurn` remains the one class identity for deliberate safe authoring;
transcript ingestion does not write it. See
[Transcript ingestion](artifacts.md#8-transcript-ingestion)
for ordering, redaction, occurrence identity, and failure semantics.
