# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Mission-owned, redacted coding-agent transcript ingestion contracts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from archetype import ArchetypeRuntime
from archetype.app.container import ServiceContainer
from archetype.app.redaction import SecretQuarantineError
from archetype.core.config import StorageBackend, StorageConfig, WorldConfig
from archetype.missions.trajectories import (
    CLAUDE_TRANSCRIPT_TABLE,
    ClaudeTranscriptSource,
)

_TRANSCRIPT_ROWS = CLAUDE_TRANSCRIPT_TABLE


def _storage(tmp_path: Path, namespace: str) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "store"),
        namespace=namespace,
        backend=StorageBackend.ICEBERG,
    )


def _line(line_type: str, content, *, ts: str, **message_fields) -> str:
    message = {"role": line_type, "content": content, **message_fields}
    return json.dumps(
        {
            "type": line_type,
            "timestamp": ts,
            "cwd": "/private/repository",
            "gitBranch": "mission/transcripts",
            "version": "3.0.0",
            "message": message,
        }
    )


def _write_transcript(path: Path, *, user_text: str = "Fix the login regression") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                _line("user", user_text, ts="2026-07-19T10:00:00.000Z"),
                _line(
                    "assistant",
                    [{"type": "text", "text": "Patched and validated"}],
                    ts="2026-07-19T10:00:02.000Z",
                    model="claude-fable-5",
                    usage={"output_tokens": 12},
                ),
            ]
        ),
        encoding="utf-8",
    )


@pytest.mark.asyncio
@pytest.mark.contract("missions.transcripts.redacted_ingestion")
async def test_transcript_persists_sanitized_artifact_and_normalized_rows(
    tmp_path: Path,
) -> None:
    secret = "sk-proj-" + "Q" * 32
    transcript = tmp_path / "project-a" / "session-1.jsonl"
    _write_transcript(transcript, user_text=f"Fix login with credential {secret}")
    raw_digest = hashlib.sha256(transcript.read_bytes()).hexdigest()
    source = ClaudeTranscriptSource(path=transcript, mission_id="mission-7")
    storage = _storage(tmp_path, "transcripts")
    container = ServiceContainer()
    try:
        world = await container.world_service.create_world(
            WorldConfig(name="transcript-ingestion"), storage
        )
        result = await container.transcript_ingestion_service.ingest(str(world.world_id), source)
        rows = sorted(
            (
                await container.ingestion_service.read(str(world.world_id), _TRANSCRIPT_ROWS)
            ).to_pylist(),
            key=lambda row: row["seq"],
        )
        artifacts = (await container.artifact_service.index(str(world.world_id))).to_pylist()

        assert result.redaction_status == "redacted"
        assert result.redaction_count >= 1
        assert "openai-api-key" in result.redaction_rule_ids
        assert result.rows_written == 3
        assert result.artifact.sha256 != raw_digest

        assert [row["row_kind"] for row in rows] == ["session", "turn", "turn"]
        assert [row["seq"] for row in rows] == [-1, 0, 1]
        assert {row["trajectory_id"] for row in rows} == {source.source_uri}
        assert {row["mission_id"] for row in rows} == {"mission-7"}
        assert {row["project"] for row in rows} == {"project-a"}
        assert {row["session_id"] for row in rows} == {"session-1"}
        assert {row["source_content_hash"] for row in rows} == {raw_digest}
        assert {row["source_artifact_id"] for row in rows} == {result.artifact.artifact_id}
        assert "<redacted:openai-api-key>" in rows[1]["content"]

        assert len(artifacts) == 1
        assert artifacts[0]["artifact_id"] == result.artifact.artifact_id
        assert artifacts[0]["sha256"] == result.artifact.sha256
        assert artifacts[0]["logical_path"] == "claude/project-a/session-1.jsonl"
        durable = json.dumps({"rows": rows, "artifacts": artifacts}, sort_keys=True, default=str)
        assert secret not in durable
    finally:
        await container.shutdown()

    # Sanitization snapshots; it never mutates the submitted source.
    assert secret in transcript.read_text(encoding="utf-8")


@pytest.mark.asyncio
@pytest.mark.contract("missions.transcripts.fail_closed")
async def test_quarantine_and_parse_failure_publish_nothing(tmp_path: Path) -> None:
    target = tmp_path / "target.jsonl"
    _write_transcript(target)
    symlink = tmp_path / "project-b" / "linked.jsonl"
    symlink.parent.mkdir()
    symlink.symlink_to(target)

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("quarantined-transcript", storage=_storage(tmp_path, "quarantine"))
        with pytest.raises(SecretQuarantineError, match="unsupported-source-file"):
            await world.ingest_claude_transcript(ClaudeTranscriptSource(path=symlink))
        with pytest.raises(KeyError):
            await world.artifacts()

        noise = tmp_path / "project-b" / "noise.jsonl"
        noise.write_text('{"type":"queue-operation"}\nnot json', encoding="utf-8")
        with pytest.raises(ValueError, match="contains no dialogue turns"):
            await world.ingest_claude_transcript(ClaudeTranscriptSource(path=noise))
        with pytest.raises(KeyError):
            await world.artifacts()


@pytest.mark.asyncio
@pytest.mark.contract("missions.transcripts.occurrence_identity")
async def test_reingestion_records_a_new_artifact_occurrence(tmp_path: Path) -> None:
    transcript = tmp_path / "project-c" / "retry.jsonl"
    _write_transcript(transcript)
    source = ClaudeTranscriptSource(path=transcript)
    storage = _storage(tmp_path, "retry")
    container = ServiceContainer()
    try:
        world = await container.world_service.create_world(WorldConfig(name="retry"), storage)
        first = await container.transcript_ingestion_service.ingest(str(world.world_id), source)
        second = await container.transcript_ingestion_service.ingest(str(world.world_id), source)

        assert first.artifact.artifact_id != second.artifact.artifact_id
        assert first.artifact.uri == second.artifact.uri
        assert first.artifact.sha256 == second.artifact.sha256
        assert first.rows_written == second.rows_written == 3
        assert (
            await container.ingestion_service.read(str(world.world_id), _TRANSCRIPT_ROWS)
        ).count_rows() == 6
        assert (await container.artifact_service.index(str(world.world_id))).count_rows() == 2
    finally:
        await container.shutdown()


def test_sync_runtime_mirrors_transcript_ingestion(tmp_path: Path) -> None:
    transcript = tmp_path / "project-d" / "sync.jsonl"
    _write_transcript(transcript)

    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world("sync-transcript", storage=_storage(tmp_path, "sync_transcript"))
        result = world.ingest_claude_transcript(ClaudeTranscriptSource(path=transcript))

        assert result.rows_written == 3
        assert world.artifacts().count_rows() == 1
        assert world.transcript_rows().count_rows() == 3
