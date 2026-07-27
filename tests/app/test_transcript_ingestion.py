# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Mission-owned, redacted coding-agent transcript ingestion contracts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

import pytest
from uuid_utils import uuid7

from archetype import ArchetypeRuntime
from archetype.app.missions.transcript_service import TranscriptIngestionService
from archetype.artifacts.models import ArtifactRef, QueryArtifacts
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.episodes.models import IngestClaudeTranscript, QueryTranscriptRows
from archetype.missions.trajectories import (
    ClaudeTranscriptSource,
)
from archetype.redaction import RedactionService, SecretQuarantineError
from archetype.storage.interfaces import iStorageService
from archetype.world.models import CreateWorld, Run
from tests._runtime import build_test_runtime


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


def _artifact_ref(*, logical_path: str, sha256: str, size_bytes: int) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=str(uuid7()),
        logical_path=logical_path,
        uri=f"file:///objects/{sha256}",
        sha256=sha256,
        xxhash3_64="0" * 16,
        media_type="application/json",
        size_bytes=size_bytes,
    )


@pytest.mark.asyncio
@pytest.mark.contract("missions.transcripts.fail_closed")
async def test_transcript_coordinates_fail_before_source_access(tmp_path: Path) -> None:
    source = ClaudeTranscriptSource(path=tmp_path / "never-read.jsonl")
    service = TranscriptIngestionService(
        RedactionService(),
        cast(iStorageService, object()),
    )

    with pytest.raises(TypeError, match="explicit StorageConfig"):
        await service.ingest("world-1", source)
    with pytest.raises(ValueError, match="StorageBackend.ICEBERG"):
        await service.ingest("world-1", source, storage_config=StorageConfig())
    with pytest.raises(TypeError, match="explicit StorageConfig"):
        await service.read("world-1")

    assert not source.path.exists()


@pytest.mark.asyncio
@pytest.mark.contract("missions.transcripts.fail_closed")
async def test_digest_mismatch_fails_after_artifact_before_transcript_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import archetype.app.missions.transcript_service as transcript_service

    transcript = tmp_path / "project-digest" / "session.jsonl"
    _write_transcript(transcript)
    events: list[str] = []

    class StorageTrap:
        async def append_world_rows(self, *_args, **_kwargs) -> int:
            events.append("rows")
            raise AssertionError("digest mismatch must reject before transcript append")

    async def publish_artifact(_storage, operation, *, store_config=None):
        del _storage, store_config
        events.append("artifact")
        artifact_source = operation.sources[0]
        return (
            _artifact_ref(
                logical_path=artifact_source.logical_path,
                sha256="0" * 64,
                size_bytes=Path(artifact_source.source_uri).stat().st_size,
            ),
        )

    monkeypatch.setattr(
        transcript_service.artifact_handlers,
        "ingest_artifacts",
        publish_artifact,
    )
    service = TranscriptIngestionService(
        RedactionService(),
        cast(iStorageService, StorageTrap()),
    )

    with pytest.raises(RuntimeError, match="digest changed after sanitization"):
        await service.ingest(
            "world-1",
            ClaudeTranscriptSource(path=transcript),
            storage_config=_storage(tmp_path, "digest_guard"),
        )

    assert events == ["artifact"]


@pytest.mark.asyncio
@pytest.mark.contract("missions.transcripts.fail_closed")
async def test_row_failure_reports_after_sanitized_artifact_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import archetype.app.missions.transcript_service as transcript_service

    secret = "sk-proj-" + "R" * 32
    transcript = tmp_path / "project-partial" / "session.jsonl"
    _write_transcript(transcript, user_text=f"Use credential {secret}")
    events: list[str] = []

    class FailingRowStorage:
        async def append_world_rows(
            self,
            _storage_config,
            _world_id,
            _table_name,
            rows,
            *,
            key_columns=(),
        ) -> int:
            del key_columns
            events.append("rows")
            assert secret not in json.dumps(rows.to_pylist(), sort_keys=True)
            raise RuntimeError("transcript rows unavailable")

    async def publish_artifact(_storage, operation, *, store_config=None):
        del _storage, store_config
        artifact_source = operation.sources[0]
        sanitized = Path(artifact_source.source_uri)
        payload = sanitized.read_bytes()
        assert secret.encode() not in payload
        events.append("artifact")
        return (
            _artifact_ref(
                logical_path=artifact_source.logical_path,
                sha256=hashlib.sha256(payload).hexdigest(),
                size_bytes=len(payload),
            ),
        )

    monkeypatch.setattr(
        transcript_service.artifact_handlers,
        "ingest_artifacts",
        publish_artifact,
    )
    service = TranscriptIngestionService(
        RedactionService(),
        cast(iStorageService, FailingRowStorage()),
    )

    with pytest.raises(RuntimeError, match="transcript rows unavailable"):
        await service.ingest(
            "world-1",
            ClaudeTranscriptSource(path=transcript),
            storage_config=_storage(tmp_path, "partial_commit"),
        )

    assert events == ["artifact", "rows"]
    assert secret in transcript.read_text(encoding="utf-8")


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
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="transcript-ingestion"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(Run(world_id=world.world_id, run_config=RunConfig(num_steps=1)))
        result = await dispatcher.apply(
            IngestClaudeTranscript(
                world_id=world.world_id,
                source=source,
                storage_config=storage,
            )
        )
        rows = sorted(
            (
                await dispatcher.apply(
                    QueryTranscriptRows(
                        world_id=world.world_id,
                        storage_config=storage,
                    )
                )
            ).to_pylist(),
            key=lambda row: row["seq"],
        )
        artifacts = (
            await dispatcher.apply(
                QueryArtifacts(
                    world_id=world.world_id,
                    storage_config=storage,
                )
            )
        ).to_pylist()

        assert result.redaction_status == "redacted"
        assert result.redaction_count >= 1
        assert "openai-api-key" in result.redaction_rule_ids
        assert result.rows_written == 3
        assert result.artifact.sha256 != raw_digest

        assert [row["row_kind"] for row in rows] == ["session", "turn", "turn"]
        assert [row["seq"] for row in rows] == [-1, 0, 1]
        assert {row["episode_id"] for row in rows} == {source.source_uri}
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
        await resources.aclose()

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
        await world.run(steps=1)
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
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    try:
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="retry"),
                storage_config=storage,
            )
        )
        await dispatcher.apply(Run(world_id=world.world_id, run_config=RunConfig(num_steps=1)))
        operation = IngestClaudeTranscript(
            world_id=world.world_id,
            source=source,
            storage_config=storage,
        )
        first = await dispatcher.apply(operation)
        second = await dispatcher.apply(operation)

        assert first.artifact.artifact_id != second.artifact.artifact_id
        assert first.artifact.uri == second.artifact.uri
        assert first.artifact.sha256 == second.artifact.sha256
        assert first.rows_written == second.rows_written == 3
        assert (
            await dispatcher.apply(
                QueryTranscriptRows(
                    world_id=world.world_id,
                    storage_config=storage,
                )
            )
        ).count_rows() == 6
        assert (
            await dispatcher.apply(
                QueryArtifacts(
                    world_id=world.world_id,
                    storage_config=storage,
                )
            )
        ).count_rows() == 2
    finally:
        await resources.aclose()


def test_sync_runtime_mirrors_transcript_ingestion(tmp_path: Path) -> None:
    transcript = tmp_path / "project-d" / "sync.jsonl"
    _write_transcript(transcript)

    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world("sync-transcript", storage=_storage(tmp_path, "sync_transcript"))
        world.run(steps=1)
        result = world.ingest_claude_transcript(ClaudeTranscriptSource(path=transcript))

        assert result.rows_written == 3
        assert world.artifacts().count_rows() == 1
        assert world.transcript_rows().count_rows() == 3
