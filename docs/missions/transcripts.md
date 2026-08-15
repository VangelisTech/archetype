---
title: Transcript Ingestion
description: Sanitize, publish, and index coding-agent transcripts
---

## Transcript ingestion contract

**Document type:** Normative contract and user guide.

Raw coding-agent transcripts, tool inputs and outputs, frames, and other large
or secret-bearing content are artifacts. They require pre-durability redaction,
content identity, and typed ingestion.

```python
from pathlib import Path

from archetype.missions import MissionWorld
from archetype.missions.trajectories import ClaudeTranscriptSource

result = await MissionWorld(world).ingest_claude_transcript(
    ClaudeTranscriptSource(
        path=Path("session.jsonl"),
        mission_id="mission-42",
    )
)
```

The missions family parses already-sanitized text into immutable `LoadedSession`
and `Turn` values. It neither opens the source path nor exposes a method that
turns the session into spawnable entities. The Missions-owned family workflow
performs the stable snapshot, quarantine/redaction, complete parse,
sanitized artifact ingestion, and typed Iceberg append.

`TranscriptIngestionService` preserves this exact order:

1. `RedactionService` validates metadata and snapshots a sanitized file before
   durability
2. the Missions parser reads only that sanitized copy
3. the workflow redacts normalized session and turn rows
4. it computes the sanitized file digest
5. the framework artifact handler publishes the immutable object, typed
   indexes, and common artifact row
6. it verifies that the returned artifact SHA-256 equals the sanitized digest
7. `StorageService` appends normalized rows to
   `coding_agent_transcript_rows`

One ingested Claude session is one bounded execution; its stable
`episode_id` is the canonical `claude-session://` source URI
(`ClaudeTranscriptSource.episode_id`).

The common artifact index stores the sanitized file occurrence and its
content-addressed object URI. The `coding_agent_transcript_rows` table stores
one session row plus ordered turn rows linked by the same canonical
`episode_id` and `source_artifact_id`. The submitted local path is absent
from both durable tables. `TranscriptIngestionResult` returns the portable
`ArtifactRef`, linkage, row count, and redaction outcome;
`MissionWorld(world).transcript_rows()` reads the normalized rows for analysis.

`TrajectoryTurn` remains the one class identity for deliberate safe authoring;
transcript ingestion does not write it.

Quarantine, parse, and row-redaction failures occur before artifact publication
and publish nothing. A digest mismatch occurs after the honest artifact
boundary and therefore leaves the sanitized artifact visible but fails before
any transcript row append. Re-ingesting a valid transcript records another
artifact occurrence and another normalized row set scoped to that occurrence.
The original source digest may identify the input, while the artifact SHA-256
always describes the sanitized bytes actually stored.

Unsafe metadata, symlinks, unsupported containers, and unrewritable
secret-bearing inputs are quarantined before any object or catalog row becomes
durable. This is Missions-owned pre-durability policy over the generic
[Artifacts](../guide/artifacts.md) persistence boundary; raw narrative never crosses
that boundary.

The protected full-stack R2 proof additionally ingests one sanitized Claude
session, cold-reads three `coding_agent_transcript_rows`, and verifies that each
row joins to the sanitized `artifact_files` occurrence through
`source_artifact_id`. This Missions facet composes the framework artifact proof;
it does not make transcript rows part of the generic artifact schema.

The removed `TranscriptIngestionReceipt` surface is replaced by
`TranscriptIngestionResult`, which links the normalized rows to the sanitized
`ArtifactRef`; no compatibility alias retains the former receipt workflow.
