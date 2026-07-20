# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Redacted, claim-backed coding-agent transcript ingestion contracts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from archetype import ArchetypeRuntime
from archetype.app.container import ServiceContainer
from archetype.app.redaction import SecretQuarantineError
from archetype.app.storage.catalog import ClaimConflictError
from archetype.artifacts.components import ArtifactMeta, AssetRef
from archetype.core.config import StorageBackend, StorageConfig, WorldConfig
from archetype.missions.trajectories import (
    CLAUDE_TRANSCRIPT_TABLE,
    ClaudeTranscriptSource,
    Trajectory,
    TrajectoryTurn,
    TranscriptArtifactRef,
)


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
@pytest.mark.contract("artifacts.transcripts.redacted_claim")
async def test_runtime_ingests_sanitized_rows_and_only_lightweight_component_indexes(
    tmp_path: Path,
) -> None:
    secret = "sk-proj-" + "Q" * 32
    transcript = tmp_path / "project-a" / "session-1.jsonl"
    _write_transcript(transcript, user_text=f"Fix login with credential {secret}")
    raw_digest = hashlib.sha256(transcript.read_bytes()).hexdigest()
    source = ClaudeTranscriptSource(path=transcript, mission_id="mission-7")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("transcript-ingestion", storage=_storage(tmp_path, "transcripts"))
        receipt = await world.ingest_claude_transcript(source)
        rows = sorted(
            (await world.artifacts(CLAUDE_TRANSCRIPT_TABLE)).to_pylist(),
            key=lambda row: row["seq"],
        )
        indexes = (
            await world.query(Trajectory, TranscriptArtifactRef, AssetRef, ArtifactMeta)
        ).to_pylist()

        assert receipt.source_content_hash == raw_digest
        assert receipt.redaction_status == "redacted"
        assert receipt.redaction_count >= 1
        assert "openai-api-key" in receipt.redaction_rule_ids
        assert receipt.rows.rows_written == 3
        assert receipt.reference.duplicate is False

        assert [row["row_kind"] for row in rows] == ["session", "turn", "turn"]
        assert [row["seq"] for row in rows] == [-1, 0, 1]
        assert {row["trajectory_id"] for row in rows} == {source.source_uri}
        assert {row["mission_id"] for row in rows} == {"mission-7"}
        assert {row["project"] for row in rows} == {"project-a"}
        assert {row["session_id"] for row in rows} == {"session-1"}
        assert {row["source_content_hash"] for row in rows} == {raw_digest}
        assert {row["source_artifact_entity_id"] for row in rows} == {
            receipt.reference.artifact_entity_id
        }
        assert "<redacted:openai-api-key>" in rows[1]["content"]

        assert len(indexes) == 1
        index = indexes[0]
        assert index["trajectory__trajectory_id"] == source.source_uri
        assert index["transcriptartifactref__table_name"] == CLAUDE_TRANSCRIPT_TABLE
        assert index["transcriptartifactref__source_content_hash"] == raw_digest
        assert index["assetref__uri"] == source.source_uri
        assert index["assetref__digest"] == raw_digest
        assert str(transcript) not in json.dumps(index)

        durable = json.dumps({"rows": rows, "indexes": indexes}, sort_keys=True)
        assert secret not in durable
        assert "Fix login with credential" in durable
        with pytest.raises(KeyError):
            await world.query(TrajectoryTurn)

        retry = await world.ingest_claude_transcript(source)
        assert retry.duplicate is True
        assert retry.reference.artifact_entity_id == receipt.reference.artifact_entity_id
        assert retry.rows.rows_written == 0
        assert len((await world.artifacts(CLAUDE_TRANSCRIPT_TABLE)).to_pylist()) == 3

        _write_transcript(transcript, user_text="Different source bytes")
        with pytest.raises(ClaimConflictError):
            await world.ingest_claude_transcript(source)
        assert len((await world.artifacts(CLAUDE_TRANSCRIPT_TABLE)).to_pylist()) == 3

    # Sanitization snapshots; it never mutates the source artifact.
    assert "Different source bytes" in transcript.read_text(encoding="utf-8")


@pytest.mark.asyncio
@pytest.mark.contract("artifacts.transcripts.redacted_claim")
async def test_quarantine_and_parse_failure_leave_no_transcript_artifact(
    tmp_path: Path,
) -> None:
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
            await world.artifacts(CLAUDE_TRANSCRIPT_TABLE)
        assert (await world.query(ArtifactMeta)).count_rows() == 0

        noise = tmp_path / "project-b" / "noise.jsonl"
        noise.write_text('{"type":"queue-operation"}\nnot json', encoding="utf-8")
        with pytest.raises(ValueError, match="contains no dialogue turns"):
            await world.ingest_claude_transcript(ClaudeTranscriptSource(path=noise))
        with pytest.raises(KeyError):
            await world.artifacts(CLAUDE_TRANSCRIPT_TABLE)
        assert (await world.query(ArtifactMeta)).count_rows() == 0


@pytest.mark.asyncio
@pytest.mark.contract("artifacts.transcripts.redacted_claim")
async def test_retry_repairs_rows_after_the_source_claim_lands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transcript = tmp_path / "project-c" / "repair.jsonl"
    _write_transcript(transcript)
    source = ClaudeTranscriptSource(path=transcript)
    storage = _storage(tmp_path, "repair")
    container = ServiceContainer()
    try:
        world = await container.world_service.create_world(WorldConfig(name="repair"), storage)
        real_write = container.artifact_table_service.write_artifacts
        failed = False

        async def fail_once(*args, **kwargs):
            nonlocal failed
            if not failed:
                failed = True
                raise RuntimeError("injected typed-row failure")
            return await real_write(*args, **kwargs)

        monkeypatch.setattr(container.artifact_table_service, "write_artifacts", fail_once)
        with pytest.raises(RuntimeError, match="injected typed-row failure"):
            await container.transcript_ingestion_service.ingest(
                str(world.world_id),
                source,
                storage_config=storage,
            )

        claim_rows = await container.query_service.query_components(
            [ArtifactMeta],
            str(world.world_id),
            str(world.run_id),
            storage,
        )
        assert claim_rows.count_rows() == 1
        with pytest.raises(KeyError):
            await container.artifact_table_service.read_artifacts(
                str(world.world_id),
                CLAUDE_TRANSCRIPT_TABLE,
                storage_config=storage,
            )

        repaired = await container.transcript_ingestion_service.ingest(
            str(world.world_id),
            source,
            storage_config=storage,
        )
        complete_retry = await container.transcript_ingestion_service.ingest(
            str(world.world_id),
            source,
            storage_config=storage,
        )

        assert repaired.reference.duplicate is True
        assert repaired.rows.rows_written == 3
        assert repaired.duplicate is False
        assert complete_retry.duplicate is True
    finally:
        await container.shutdown()


def test_sync_runtime_mirrors_transcript_ingestion(tmp_path: Path) -> None:
    transcript = tmp_path / "project-d" / "sync.jsonl"
    _write_transcript(transcript)

    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world("sync-transcript", storage=_storage(tmp_path, "sync_transcript"))
        receipt = world.ingest_claude_transcript(ClaudeTranscriptSource(path=transcript))

        assert receipt.rows.rows_written == 3
        assert len(world.artifacts(CLAUDE_TRANSCRIPT_TABLE).to_pylist()) == 3
