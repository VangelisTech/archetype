# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Redacted coding-agent transcript workflow owned by Agent Missions."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import daft
from daft import DataFrame
from pydantic import JsonValue

from archetype.app.artifacts.interfaces import iArtifactService
from archetype.app.ingestion.interfaces import iIngestionService
from archetype.artifacts.contracts import ArtifactSource
from archetype.core.config import StorageBackend, StorageConfig
from archetype.missions.trajectories import (
    CLAUDE_TRANSCRIPT_TABLE,
    ClaudeTranscriptSource,
    LoadedSession,
    TranscriptIngestionResult,
    parse_claude_transcript,
)
from archetype.redaction import RedactionReceipt, RedactionService
from archetype.storage.interfaces import iStorageService
from archetype.world.interfaces import iWorldRegistry

_TRANSCRIPT_ROWS = CLAUDE_TRANSCRIPT_TABLE


def _canonical_digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _receipt_columns(prefix: str, receipt: RedactionReceipt) -> dict[str, Any]:
    return {
        f"{prefix}_redaction_policy_id": receipt.policy_id,
        f"{prefix}_redaction_status": receipt.status,
        f"{prefix}_redaction_count": receipt.redaction_count,
        f"{prefix}_redaction_rule_ids_json": json.dumps(list(receipt.rule_ids)),
    }


def _normalize_session(
    session: LoadedSession,
    redaction: RedactionService,
) -> tuple[list[dict[str, Any]], tuple[RedactionReceipt, ...]]:
    header = redaction.redact_record(
        {
            "project": session.project,
            "session_id": session.session_id,
            "git_branch": session.git_branch,
            "client_version": session.client_version,
            "models": list(session.models),
            "started_at": session.started_at,
            "ended_at": session.ended_at,
        },
        scope=f"transcript:{session.source_uri}:session",
    )
    header_value = header.value
    models = header_value.get("models") or []
    common = {
        "trajectory_id": session.trajectory.trajectory_id,
        "mission_id": session.mission_id,
        "project": str(header_value.get("project") or ""),
        "session_id": str(header_value.get("session_id") or ""),
        "git_branch": str(header_value.get("git_branch") or ""),
        "client_version": str(header_value.get("client_version") or ""),
        "model": str(models[0]) if isinstance(models, list) and models else "",
        "models_json": json.dumps(models),
        "started_at": str(header_value.get("started_at") or ""),
        "ended_at": str(header_value.get("ended_at") or ""),
        "sidechain_turns_skipped": session.sidechain_turns_skipped,
        "total_turns": session.trajectory.total_turns,
        "total_tokens": session.trajectory.total_tokens,
        "duration_seconds": session.trajectory.duration_seconds,
    }
    rows: list[dict[str, Any]] = [
        {
            **common,
            "row_kind": "session",
            "seq": -1,
            "role": "",
            "content": "",
            "tool_name": "",
            "tool_input": "",
            "tool_output": "",
            "tokens": 0,
            "duration_ms": 0.0,
            "error": "",
            **_receipt_columns("row", header.receipt),
        }
    ]
    receipts = [header.receipt]

    for seq, turn in enumerate(session.turns):
        record: dict[str, JsonValue] = {
            "role": turn.role,
            "content": turn.content,
            "tool_name": turn.tool_name,
            "tool_input": turn.tool_input,
            "tool_output": turn.tool_output,
            "tokens": turn.tokens,
            "duration_ms": turn.duration_ms,
            "error": turn.error,
        }
        redacted = redaction.redact_record(
            record,
            scope=f"transcript:{session.source_uri}:turn:{seq}",
        )
        value = redacted.value
        tokens = value.get("tokens")
        duration_ms = value.get("duration_ms")
        if isinstance(tokens, bool) or not isinstance(tokens, int):
            raise TypeError("redaction changed transcript tokens to a non-integer value")
        if isinstance(duration_ms, bool) or not isinstance(duration_ms, (int, float)):
            raise TypeError("redaction changed transcript duration to a non-numeric value")
        rows.append(
            {
                **common,
                "row_kind": "turn",
                "seq": seq,
                "role": str(value.get("role") or ""),
                "content": str(value.get("content") or ""),
                "tool_name": str(value.get("tool_name") or ""),
                "tool_input": str(value.get("tool_input") or ""),
                "tool_output": str(value.get("tool_output") or ""),
                "tokens": tokens,
                "duration_ms": float(duration_ms),
                "error": str(value.get("error") or ""),
                **_receipt_columns("row", redacted.receipt),
            }
        )
        receipts.append(redacted.receipt)
    return rows, tuple(receipts)


def _aggregate_redaction(
    source: RedactionReceipt,
    rows: tuple[RedactionReceipt, ...],
) -> tuple[str, int, tuple[str, ...]]:
    count = source.redaction_count + sum(receipt.redaction_count for receipt in rows)
    rules = set(source.rule_ids)
    for receipt in rows:
        rules.update(receipt.rule_ids)
    return ("redacted" if count else "clean"), count, tuple(sorted(rules))


def _artifact_rows(
    normalized: list[dict[str, Any]],
    *,
    source_uri: str,
    source_content_hash: str,
    source_receipt: RedactionReceipt,
    source_artifact_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_columns = {
        "source_artifact_uri": source_uri,
        "source_content_hash": source_content_hash,
        **_receipt_columns("source", source_receipt),
    }
    for row in normalized:
        payload = {**source_columns, **row}
        row_key = "session" if row["row_kind"] == "session" else f"turn-{row['seq']}"
        rows.append(
            {
                "source_uri": f"{source_uri}#row={row_key}",
                "content_hash": _canonical_digest(payload),
                "source_artifact_id": source_artifact_id,
                **payload,
            }
        )
    return rows


class TranscriptIngestionService:
    """Sanitize a session, persist its source artifact, and index normalized rows."""

    def __init__(
        self,
        artifact_service: iArtifactService,
        ingestion_service: iIngestionService,
        redaction_service: RedactionService,
        storage_service: iStorageService,
        world_registry: iWorldRegistry,
    ) -> None:
        self._artifacts = artifact_service
        self._ingestion = ingestion_service
        self._redaction = redaction_service
        self._storage = storage_service
        self._world_registry = world_registry

    async def ingest(
        self,
        world_id: str,
        source: ClaudeTranscriptSource,
        *,
        storage_config: StorageConfig | None = None,
    ) -> TranscriptIngestionResult:
        """Scan, parse, sanitize, and index one Claude JSONL source."""

        effective = await self._resolve_storage(world_id, storage_config)
        if effective.backend != StorageBackend.ICEBERG:
            raise ValueError("transcript ingestion requires StorageBackend.ICEBERG")

        self._redaction.assert_safe_metadata(
            source.source_uri,
            field="transcript.source_uri",
        )
        if source.mission_id:
            self._redaction.assert_safe_metadata(
                source.mission_id,
                field="transcript.mission_id",
            )

        with TemporaryDirectory(prefix="archetype-transcript-") as temporary:
            sanitized = self._redaction.sanitize_file(
                source.path,
                destination=Path(temporary) / "session.jsonl",
                logical_path=f"claude/{source.project}/{source.session_id}.jsonl",
            )
            session = parse_claude_transcript(
                sanitized.path.read_text(encoding="utf-8"),
                source,
            )
            if session is None:
                raise ValueError("Claude transcript contains no dialogue turns")
            normalized, row_receipts = _normalize_session(session, self._redaction)
            status, redaction_count, rule_ids = _aggregate_redaction(
                sanitized.receipt,
                row_receipts,
            )
            safe_project = str(normalized[0]["project"])
            trajectory = session.trajectory.model_copy(
                update={
                    "task_id": safe_project,
                    "model": str(normalized[0]["model"]),
                }
            )
            logical_path = f"claude/{safe_project}/{source.session_id}.jsonl"
            sanitized_digest = _file_digest(sanitized.path)
            (artifact,) = await self._artifacts.ingest(
                world_id,
                ArtifactSource(
                    source_uri=str(sanitized.path),
                    logical_path=logical_path,
                ),
                storage_config=effective,
            )
        if artifact.sha256 != sanitized_digest:
            raise RuntimeError("transcript artifact digest changed after sanitization")
        rows = _artifact_rows(
            normalized,
            source_uri=source.source_uri,
            source_content_hash=sanitized.source_digest,
            source_receipt=sanitized.receipt,
            source_artifact_id=artifact.artifact_id,
        )
        rows_written = await self._ingestion.append(
            world_id,
            _TRANSCRIPT_ROWS,
            daft.from_pylist(rows),
            storage_config=effective,
        )
        run_id = await self._run_id(world_id, effective)
        return TranscriptIngestionResult(
            world_id=str(world_id),
            run_id=run_id,
            trajectory_id=trajectory.trajectory_id,
            mission_id=source.mission_id,
            source_uri=source.source_uri,
            artifact=artifact,
            rows_written=rows_written,
            redaction_policy_id=self._redaction.policy_id,
            redaction_status=status,
            redaction_count=redaction_count,
            redaction_rule_ids=rule_ids,
        )

    async def read(
        self,
        world_id: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame:
        """Return normalized transcript rows for the current world run."""

        return await self._ingestion.read(
            str(world_id),
            _TRANSCRIPT_ROWS,
            storage_config=storage_config,
        )

    async def _run_id(self, world_id: str, storage: StorageConfig) -> str:
        record = await self._storage.get_control_catalog(storage).get_world(str(world_id))
        if record is None or not record.run_id:
            raise RuntimeError(f"world {world_id} has no recorded run")
        return str(record.run_id)

    async def _resolve_storage(
        self,
        world_id: str,
        storage_config: StorageConfig | None,
    ) -> StorageConfig:
        if storage_config is not None:
            return storage_config
        live = await self._world_registry.storage_record(world_id)
        return live[0] if live is not None else StorageConfig()
