# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Remote control catalog client (issue #281).

Speaks the archetype-control-catalog Worker's HTTP API (Cloudflare Durable
Objects: one directory object per storage identity, one commit object per
world). Implements the same surface as ``SqliteControlCatalog`` — which
remains the reference implementation and the default; this client is
selected by configuration (``ARCHETYPE_CONTROL_CATALOG_URL``) and lifts the
single-host authority limit documented in the atomic-visibility spec.

Error mapping is exact: the worker's typed error kinds raise the same
exceptions the SQLite implementation raises, so every caller (coordinator,
ingestion, resume, discovery) behaves identically against either backend.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from typing import cast
from urllib.parse import urlencode

import httpx

from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.app.storage.catalog import (
    _MAX_ARTIFACT_RETRY_DELAY_MS,
    _MAX_ARTIFACT_RETRY_WINDOW_MS,
    _MAX_PORTABLE_COUNTER,
    _RECOVERY_ERROR_CODES,
    _RECOVERY_EXCEPTION_STATUSES,
    _RECOVERY_KINDS,
    _RECOVERY_SWEEP_STATUSES,
    ArtifactPublicationCandidate,
    ArtifactPublicationConflictError,
    ArtifactPublicationExpiredError,
    ArtifactPublicationPendingError,
    ArtifactPublicationRecord,
    AttemptClaimConflictError,
    AttemptClaimPendingError,
    AttemptClaimRecord,
    AttemptClaimStaleError,
    CatalogConflictError,
    ClaimConflictError,
    ClaimPendingError,
    ClaimRecord,
    CommandAdmission,
    CommandConflictError,
    CommandRecord,
    ManifestRecord,
    OutboxRecord,
    RecoveryExceptionConflictError,
    RecoveryExceptionRecord,
    RecoverySweepConflictError,
    RecoverySweepPendingError,
    RecoverySweepRecord,
    RecoverySweepStaleError,
    SignatureRecord,
    WorldRecord,
    _require_artifact_lease_ms,
    _require_artifact_lease_seconds,
    _require_artifact_milliseconds,
    _require_bounded_text,
    _require_portable_counter,
    _require_recovery_delay,
    _require_recovery_kind,
    _require_recovery_lease,
    _require_sha256,
    _validate_attempt_claim_transition,
    _validate_recovery_error,
    artifact_publication_key,
    claim_scope_key,
    recovery_exception_key,
    recovery_sweep_key,
)
from archetype.app.storage.recovery_transitions import (
    RecoveryExceptionEvent,
    RecoveryExceptionStatus,
    RecoveryExceptionTransitionGraph,
    RecoverySweepEvent,
    RecoverySweepStatus,
    RecoverySweepTransitionGraph,
)
from archetype.core.interfaces import StaleWriterError

logger = logging.getLogger(__name__)

_ATTEMPT_CLAIM_PROTOCOL_VERSION = 4
_ATTEMPT_CLAIM_CAPABILITY = "attempt_claim_execution_v2"
_ARTIFACT_SNAPSHOT_PROTOCOL_VERSION = 3
_ARTIFACT_SNAPSHOT_CAPABILITY = "artifact_snapshot_decimal_v1"
_ARTIFACT_SERVER_CLOCK_PROTOCOL_VERSION = 6
_ARTIFACT_SERVER_CLOCK_CAPABILITY = "artifact_publication_server_clock_v1"
_RECOVERY_PROTOCOL_VERSION = 5
_RECOVERY_CAPABILITY = "fleet_recovery_v1"
_RECOVERY_PROTOCOL_PROBE_WORLD = "__fleet_recovery_protocol__"
_RECOVERY_LEASE_RESPONSE_FIELDS = frozenset({"outcome", "sweep"})
_RECOVERY_STATUS_RESPONSE_FIELDS = frozenset({"status", "catalog_protocol_version", "capabilities"})
_RECOVERY_MISSING_STATUS_RESPONSE_FIELDS = frozenset(
    {"error", "catalog_protocol_version", "capabilities"}
)
_RECOVERY_WORLD_RESPONSE_FIELDS = frozenset(
    {"world_id", "name", "run_id", "parent_world_id", "status", "tick_head"}
)
_RECOVERY_SWEEP_RESPONSE_FIELDS = frozenset(
    {
        "sweep_key",
        "storage_fingerprint",
        "world_id",
        "kind",
        "status",
        "cursor",
        "cycle",
        "claimant",
        "lease_expires_at_ms",
        "fence_epoch",
        "active_subject_key",
        "consecutive_failures",
        "max_consecutive_failures",
        "next_due_at_ms",
        "last_error_code",
        "last_error_detail",
        "created_at_ms",
        "updated_at_ms",
        "paused_at_ms",
    }
)
_RECOVERY_EXCEPTION_RESPONSE_FIELDS = frozenset(
    {
        "exception_key",
        "sweep_key",
        "storage_fingerprint",
        "world_id",
        "kind",
        "subject_key",
        "authority_key",
        "status",
        "attempt_count",
        "max_attempts",
        "retry_at_ms",
        "last_error_code",
        "last_error_detail",
        "created_at_ms",
        "updated_at_ms",
        "resolved_at_ms",
        "dead_lettered_at_ms",
    }
)

_ERROR_MAP: dict[str, type[Exception]] = {
    "attempt_claim_conflict": AttemptClaimConflictError,
    "attempt_claim_pending": AttemptClaimPendingError,
    "attempt_claim_stale": AttemptClaimStaleError,
    "artifact_publication_conflict": ArtifactPublicationConflictError,
    "artifact_publication_expired": ArtifactPublicationExpiredError,
    "artifact_publication_pending": ArtifactPublicationPendingError,
    "catalog_conflict": CatalogConflictError,
    "claim_conflict": ClaimConflictError,
    "claim_pending": ClaimPendingError,
    "command_conflict": CommandConflictError,
    "recovery_exception_conflict": RecoveryExceptionConflictError,
    "recovery_sweep_conflict": RecoverySweepConflictError,
    "recovery_sweep_pending": RecoverySweepPendingError,
    "recovery_sweep_stale": RecoverySweepStaleError,
    "stale_writer": StaleWriterError,
}


def _require_remote_sweep_transition_target(
    record: RecoverySweepRecord,
    operation: str,
    *edges: tuple[RecoverySweepStatus | None, RecoverySweepEvent],
) -> RecoverySweepRecord:
    expected = {
        RecoverySweepTransitionGraph.transition(source, event).value for source, event in edges
    }
    if record.status not in expected:
        raise RuntimeError(
            f"remote recovery {operation} returned status outside the transition graph"
        )
    return record


def _require_remote_exception_transition_target(
    record: RecoveryExceptionRecord,
    operation: str,
    *edges: tuple[RecoveryExceptionStatus | None, RecoveryExceptionEvent],
) -> RecoveryExceptionRecord:
    expected = {
        RecoveryExceptionTransitionGraph.transition(source, event).value for source, event in edges
    }
    if record.status not in expected:
        raise RuntimeError(
            f"remote recovery {operation} returned status outside the transition graph"
        )
    return record


def _require_remote_sweep_receipt(
    record: RecoverySweepRecord,
    operation: str,
    **expected: object,
) -> RecoverySweepRecord:
    for field, value in expected.items():
        if getattr(record, field) != value:
            raise RuntimeError(
                f"remote recovery {operation} returned a sweep with different {field}"
            )
    return record


def _require_remote_exception_receipt(
    record: RecoveryExceptionRecord,
    operation: str,
    **expected: object,
) -> RecoveryExceptionRecord:
    for field, value in expected.items():
        if getattr(record, field) != value:
            raise RuntimeError(
                f"remote recovery {operation} returned an exception with different {field}"
            )
    return record


def _require_remote_schedule_delta(
    *, operation: str, updated_at_ms: int, scheduled_at_ms: int, delay_ms: int
) -> None:
    if scheduled_at_ms - updated_at_ms != delay_ms:
        raise RuntimeError(f"remote recovery {operation} returned a different delay")


class RemoteControlCatalog:
    """One client per (worker url, storage identity namespace)."""

    def __init__(self, base_url: str, namespace: str, *, token: str | None = None) -> None:
        self._base = f"{base_url.rstrip('/')}/ns/{namespace}"
        headers = {"authorization": f"Bearer {token}"} if token else {}
        self._client = httpx.AsyncClient(headers=headers, timeout=30.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def _call(
        self,
        method: str,
        path: str,
        payload: dict | None = None,
        *,
        ignore_status: tuple[int, ...] = (),
    ) -> httpx.Response:
        # GETs retry transient platform errors (Durable Object cold starts
        # surface as one-off 5xx). Writes never blind-retry here — every
        # write in the protocol is CAS/idempotent at the catalog, but the
        # caller owns that decision, not the transport.
        attempts = 3 if method == "GET" else 1
        last: httpx.Response | None = None
        for attempt in range(attempts):
            response = await self._client.request(
                method, f"{self._base}{path}", json=payload if payload is not None else None
            )
            if response.status_code in ignore_status:
                return response
            if response.status_code in (409, 412, 423):
                body = response.json()
                error = _ERROR_MAP.get(body.get("error", ""))
                if error is not None:
                    raise error(body.get("message", body.get("error")))
            if response.status_code >= 500 and attempt < attempts - 1:
                last = response
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            response.raise_for_status()
            return response
        assert last is not None
        last.raise_for_status()
        return last

    async def _require_catalog_protocol(
        self,
        world_id: str,
        *,
        minimum_version: int,
        capability: str | tuple[str, ...],
        feature: str,
    ) -> None:
        """Fail closed before a versioned write reaches an older Worker."""

        response = await self._call(
            "GET",
            f"/w/{world_id}/status",
            ignore_status=(404,),
        )
        body = response.json()
        if not isinstance(body, dict):
            raise RuntimeError(f"remote control catalog does not support {feature}")
        capabilities = body.get("capabilities", ())
        protocol_version = body.get("catalog_protocol_version")
        required_capabilities = (capability,) if isinstance(capability, str) else capability
        if (
            not isinstance(protocol_version, int)
            or protocol_version < minimum_version
            or not isinstance(capabilities, list)
            or any(required not in capabilities for required in required_capabilities)
        ):
            raise RuntimeError(f"remote control catalog does not support {feature}")

    async def _require_attempt_claim_protocol(self, world_id: str) -> None:
        await self._require_catalog_protocol(
            world_id,
            minimum_version=_ATTEMPT_CLAIM_PROTOCOL_VERSION,
            capability=_ATTEMPT_CLAIM_CAPABILITY,
            feature="attempt-claim execution v2",
        )

    async def _require_artifact_snapshot_protocol(self, world_id: str) -> None:
        await self._require_catalog_protocol(
            world_id,
            minimum_version=_ARTIFACT_SNAPSHOT_PROTOCOL_VERSION,
            capability=_ARTIFACT_SNAPSHOT_CAPABILITY,
            feature="lossless artifact snapshot IDs",
        )

    async def _require_artifact_server_clock_protocol(self, world_id: str) -> None:
        await self._require_catalog_protocol(
            world_id,
            minimum_version=_ARTIFACT_SERVER_CLOCK_PROTOCOL_VERSION,
            capability=_ARTIFACT_SERVER_CLOCK_CAPABILITY,
            feature="artifact publication server-clock v1",
        )

    async def _require_artifact_mutation_protocol(self, world_id: str) -> None:
        await self._require_catalog_protocol(
            world_id,
            minimum_version=_ARTIFACT_SERVER_CLOCK_PROTOCOL_VERSION,
            capability=(
                _ARTIFACT_SNAPSHOT_CAPABILITY,
                _ARTIFACT_SERVER_CLOCK_CAPABILITY,
            ),
            feature="lease-fenced artifact mutation v2",
        )

    async def _require_recovery_protocol(self, world_id: str) -> None:
        response = await self._call(
            "GET",
            f"/w/{world_id}/status",
            ignore_status=(404,),
        )
        body = response.json()
        if not isinstance(body, dict):
            raise RuntimeError("remote control catalog does not support fleet recovery v1")
        expected_fields = (
            _RECOVERY_MISSING_STATUS_RESPONSE_FIELDS
            if response.status_code == 404
            else _RECOVERY_STATUS_RESPONSE_FIELDS
        )
        if set(body) != expected_fields:
            raise RuntimeError("remote control catalog returned an invalid recovery probe")
        if response.status_code == 404:
            if body.get("error") != "not_found":
                raise RuntimeError("remote control catalog returned an invalid recovery probe")
        elif body.get("status") not in {"active", "destroyed"}:
            raise RuntimeError("remote control catalog returned an invalid recovery probe")
        protocol_version = body.get("catalog_protocol_version")
        capabilities = body.get("capabilities")
        if (
            type(protocol_version) is not int
            or protocol_version < _RECOVERY_PROTOCOL_VERSION
            or not isinstance(capabilities, list)
            or any(type(item) is not str for item in capabilities)
            or len(capabilities) != len(set(capabilities))
            or _RECOVERY_CAPABILITY not in capabilities
        ):
            raise RuntimeError("remote control catalog does not support fleet recovery v1")

    # ── worlds ───────────────────────────────────────────────────────────────

    async def register_world(self, record: WorldRecord) -> None:
        await self._call(
            "POST",
            "/worlds",
            {
                "world_id": record.world_id,
                "name": record.name,
                "run_id": record.run_id,
                "parent_world_id": record.parent_world_id,
                "status": record.status,
                "tick_head": record.tick_head,
            },
        )

    async def set_world_status(self, world_id: str, status: str) -> None:
        await self._call("PATCH", f"/worlds/{world_id}", {"status": status})

    async def set_world_run(self, world_id: str, run_id: str) -> None:
        await self._call("PATCH", f"/worlds/{world_id}", {"run_id": run_id})

    async def get_world(self, world_id: str) -> WorldRecord | None:
        response = await self._call("GET", f"/worlds/{world_id}", ignore_status=(404,))
        if response.status_code == 404:
            return None
        return _world_from_json(response.json())

    async def list_worlds(self) -> list[WorldRecord]:
        response = await self._call("GET", "/worlds")
        return [_world_from_json(row) for row in response.json()]

    async def list_worlds_page(
        self,
        *,
        after_world_id: str = "",
        limit: int = 1000,
    ) -> list[WorldRecord]:
        if not isinstance(after_world_id, str):
            raise TypeError("world discovery cursor must be a string")
        if type(limit) is not int or limit < 1 or limit > 10_000:
            raise ValueError("world discovery page limit must be between 1 and 10000")
        await self._require_recovery_protocol(_RECOVERY_PROTOCOL_PROBE_WORLD)
        query = urlencode({"after_world_id": after_world_id, "limit": limit})
        response = await self._call("GET", f"/worlds?{query}")
        body = response.json()
        if not isinstance(body, list) or len(body) > limit:
            raise RuntimeError("remote world discovery returned an invalid page size")
        records = [_recovery_world_from_json(row) for row in body]
        previous = after_world_id
        for record in records:
            if record.world_id <= previous:
                raise RuntimeError(
                    "remote world discovery returned unordered or out-of-cursor rows"
                )
            previous = record.world_id
        return records

    # ── signatures ───────────────────────────────────────────────────────────

    async def register_signature(self, record: SignatureRecord) -> None:
        await self._call(
            "POST",
            "/signatures",
            {
                "table_id": record.table_id,
                "component_names": list(record.component_names),
                "schema_json": record.schema_json,
                "fingerprint": record.fingerprint,
            },
        )

    async def list_signatures(self) -> list[SignatureRecord]:
        response = await self._call("GET", "/signatures")
        return [
            SignatureRecord(
                table_id=row["table_id"],
                component_names=tuple(json.loads(row["component_names"])),
                schema_json=row["schema_json"],
                fingerprint=row["fingerprint"],
            )
            for row in response.json()
        ]

    # ── fence + manifests ────────────────────────────────────────────────────

    async def acquire_fence(self, world_id: str, holder: str) -> int:
        response = await self._call("POST", f"/w/{world_id}/fence", {"holder": holder})
        return int(response.json()["epoch"])

    async def current_fence_epoch(self, world_id: str) -> int | None:
        response = await self._call("GET", f"/w/{world_id}/fence")
        epoch = response.json().get("epoch")
        return int(epoch) if epoch is not None else None

    async def max_manifest_tick(self, world_id: str, run_id: str) -> int | None:
        response = await self._call("GET", f"/w/{world_id}/manifest-head?run={run_id}")
        tick = response.json().get("tick")
        return int(tick) if tick is not None else None

    async def publish_manifest(
        self,
        world_id: str,
        run_id: str,
        tick: int,
        commit_token: str,
        writer_epoch: int,
        table_ids: list[str],
        *,
        command_ids: list[str] | None = None,
        lease_owner: str | None = None,
    ) -> None:
        await self._call(
            "POST",
            f"/w/{world_id}/manifests",
            {
                "run_id": run_id,
                "tick": tick,
                "commit_token": commit_token,
                "writer_epoch": writer_epoch,
                "table_ids": table_ids,
                "command_ids": command_ids or [],
                "lease_owner": lease_owner,
            },
        )

    # ── durable commands + transactional outbox ────────────────────────────

    async def admit_commands(
        self, world_id: str, admissions: list[CommandAdmission]
    ) -> list[CommandRecord]:
        response = await self._call(
            "POST",
            f"/w/{world_id}/commands/admit",
            {"admissions": [record.__dict__ for record in admissions]},
        )
        return [_command_from_json(world_id, row) for row in response.json()]

    async def lease_commands(
        self,
        world_id: str,
        tick: int,
        owner: str,
        *,
        lease_seconds: float = 30.0,
        limit: int = 50_000,
    ) -> list[CommandRecord]:
        response = await self._call(
            "POST",
            f"/w/{world_id}/commands/lease",
            {
                "tick": tick,
                "owner": owner,
                "lease_seconds": lease_seconds,
                "limit": limit,
            },
        )
        return [_command_from_json(world_id, row) for row in response.json()]

    async def fail_command(
        self,
        world_id: str,
        command_id: str,
        owner: str,
        *,
        status: str,
        error_code: str,
        error_detail: str,
    ) -> CommandRecord:
        response = await self._call(
            "POST",
            f"/w/{world_id}/commands/{command_id}/fail",
            {
                "owner": owner,
                "status": status,
                "error_code": error_code,
                "error_detail": error_detail,
            },
        )
        return _command_from_json(world_id, response.json())

    async def release_commands(self, world_id: str, command_ids: list[str], owner: str) -> None:
        await self._call(
            "POST",
            f"/w/{world_id}/commands/release",
            {"command_ids": command_ids, "owner": owner},
        )

    async def list_commands(
        self,
        world_id: str,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[CommandRecord]:
        params = f"?limit={limit}"
        if status is not None:
            params += f"&status={status}"
        response = await self._call("GET", f"/w/{world_id}/commands{params}")
        return [_command_from_json(world_id, row) for row in response.json()]

    async def pending_command_count(self, world_id: str) -> int:
        response = await self._call("GET", f"/w/{world_id}/commands/pending-count")
        return int(response.json()["count"])

    async def max_reserved_entity_id(self, world_id: str) -> int | None:
        response = await self._call("GET", f"/w/{world_id}/commands/max-reserved")
        value = response.json().get("entity_id")
        return int(value) if value is not None else None

    async def cancel_commands(self, world_id: str, *, reason: str) -> int:
        response = await self._call("POST", f"/w/{world_id}/commands/cancel", {"reason": reason})
        return int(response.json()["count"])

    async def read_outbox(self, world_id: str, *, limit: int = 1000) -> list[OutboxRecord]:
        response = await self._call("GET", f"/w/{world_id}/outbox?limit={limit}")
        return [_outbox_from_json(world_id, row) for row in response.json()]

    async def mark_outbox_projected(self, world_id: str, event_ids: list[str]) -> None:
        await self._call("POST", f"/w/{world_id}/outbox/project", {"event_ids": event_ids})

    async def outbox_progress(self, world_id: str) -> tuple[int, int]:
        response = await self._call("GET", f"/w/{world_id}/outbox/progress")
        body = response.json()
        return int(body["watermark"]), int(body["pending"])

    async def visible_tokens(
        self, world_id: str, run_id: str, ticks: list[int] | None = None
    ) -> dict[int, list[str]] | None:
        params = f"?run={run_id}"
        if ticks is not None:
            params += "&ticks=" + ",".join(str(int(t)) for t in ticks)
        response = await self._call("GET", f"/w/{world_id}/visible{params}")
        visible = response.json()["visible"]
        if visible is None:
            return None
        return {int(tick): list(tokens) for tick, tokens in visible.items()}

    async def list_manifests(
        self, world_id: str, run_id: str | None = None
    ) -> list[ManifestRecord]:
        params = f"?run={run_id}" if run_id else ""
        response = await self._call("GET", f"/w/{world_id}/manifests{params}")
        return [
            ManifestRecord(
                world_id=world_id,
                run_id=row["run_id"],
                tick=int(row["tick"]),
                commit_token=row["commit_token"],
                writer_epoch=int(row["writer_epoch"]),
                table_ids=tuple(json.loads(row["tables_json"])),
                created_at=row["created_at"],
            )
            for row in response.json()
        ]

    # ── mission attempt claims ──────────────────────────────────────────────

    async def acquire_attempt_claim(
        self,
        *,
        claim_key: str,
        world_id: str,
        run_id: str,
        mission_id: str,
        task_id: str,
        attempt_id: str,
        idempotency_key: str,
        request_fingerprint: str,
        request_json: str,
        redaction_policy_id: str,
        redaction_evidence_json: str,
        provider: str,
        provider_request_fingerprint: str,
        supports_idempotent_replay: bool,
        supports_session_resume: bool,
        provider_idempotency_key: str,
        claimant: str,
        lease_seconds: float = 900.0,
    ) -> tuple[str, AttemptClaimRecord]:
        if not redaction_policy_id.strip():
            raise ValueError("attempt claim redaction_policy_id must not be empty")
        if not redaction_evidence_json.strip():
            raise ValueError("attempt claim redaction_evidence_json must not be empty")
        await self._require_attempt_claim_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/attempt-claims/acquire-v2",
            {
                "claim_key": claim_key,
                "run_id": run_id,
                "mission_id": mission_id,
                "task_id": task_id,
                "attempt_id": attempt_id,
                "idempotency_key": idempotency_key,
                "request_fingerprint": request_fingerprint,
                "request_json": request_json,
                "redaction_policy_id": redaction_policy_id,
                "redaction_evidence_json": redaction_evidence_json,
                "provider": provider,
                "provider_request_fingerprint": provider_request_fingerprint,
                "supports_idempotent_replay": supports_idempotent_replay,
                "supports_session_resume": supports_session_resume,
                "provider_idempotency_key": provider_idempotency_key,
                "claimant": claimant,
                "lease_seconds": lease_seconds,
            },
        )
        body = response.json()
        return body["outcome"], _attempt_claim_from_json(world_id, body["claim"])

    async def transition_attempt_claim(
        self,
        world_id: str,
        claim_key: str,
        claimant: str,
        fence_epoch: int,
        *,
        expected_status: str,
        target_status: str,
        execution_nonce: str = "",
        redaction_evidence_json: str = "",
        provider_session_id: str = "",
        provider_request_id: str = "",
        settlement_status: str = "",
        outcome_digest: str = "",
        outcome_json: str = "",
        artifact_request_json: str = "",
        artifact_request_digest: str = "",
        artifact_publication_key: str = "",
        last_error: str = "",
    ) -> AttemptClaimRecord:
        _validate_attempt_claim_transition(
            expected_status=expected_status,
            target_status=target_status,
            execution_nonce=execution_nonce,
            redaction_evidence_json=redaction_evidence_json,
            provider_session_id=provider_session_id,
            provider_request_id=provider_request_id,
            settlement_status=settlement_status,
            outcome_digest=outcome_digest,
            outcome_json=outcome_json,
            artifact_request_json=artifact_request_json,
            artifact_request_digest=artifact_request_digest,
            artifact_publication_key=artifact_publication_key,
            last_error=last_error,
        )
        await self._require_attempt_claim_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/attempt-claims/{claim_key}/transition-v2",
            {
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "expected_status": expected_status,
                "target_status": target_status,
                "execution_nonce": execution_nonce,
                "redaction_evidence_json": redaction_evidence_json,
                "provider_session_id": provider_session_id,
                "provider_request_id": provider_request_id,
                "settlement_status": settlement_status,
                "outcome_digest": outcome_digest,
                "outcome_json": outcome_json,
                "artifact_request_json": artifact_request_json,
                "artifact_request_digest": artifact_request_digest,
                "artifact_publication_key": artifact_publication_key,
                "last_error": last_error,
            },
        )
        return _attempt_claim_from_json(world_id, response.json())

    async def consume_attempt_execution(
        self,
        world_id: str,
        claim_key: str,
        claimant: str,
        fence_epoch: int,
        execution_nonce: str,
    ) -> AttemptClaimRecord:
        if not execution_nonce:
            raise ValueError("attempt execution nonce must not be empty")
        await self._require_attempt_claim_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/attempt-claims/{claim_key}/consume-v2",
            {
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "execution_nonce": execution_nonce,
            },
        )
        return _attempt_claim_from_json(world_id, response.json())

    async def renew_attempt_claim(
        self,
        world_id: str,
        claim_key: str,
        claimant: str,
        fence_epoch: int,
        *,
        lease_seconds: float,
    ) -> AttemptClaimRecord:
        response = await self._call(
            "POST",
            f"/w/{world_id}/attempt-claims/{claim_key}/renew",
            {
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "lease_seconds": lease_seconds,
            },
        )
        return _attempt_claim_from_json(world_id, response.json())

    async def get_attempt_claim(self, world_id: str, claim_key: str) -> AttemptClaimRecord | None:
        response = await self._call(
            "GET",
            f"/w/{world_id}/attempt-claims/{claim_key}",
            ignore_status=(404,),
        )
        if response.status_code == 404:
            return None
        return _attempt_claim_from_json(world_id, response.json())

    async def list_due_attempt_claims(
        self, world_id: str, *, now: float, limit: int = 100
    ) -> list[AttemptClaimRecord]:
        response = await self._call(
            "GET",
            f"/w/{world_id}/attempt-claims?due={now}&limit={limit}",
        )
        return [_attempt_claim_from_json(world_id, row) for row in response.json()]

    # ── artifact claims ─────────────────────────────────────────────────────

    async def acquire_claim(
        self,
        *,
        world_id: str,
        run_id: str,
        producer: str,
        external_id: str,
        payload_digest: str,
        claimant: str,
        tick: int,
        lease_seconds: float = 30.0,
    ) -> tuple[str, ClaimRecord]:
        scope = claim_scope_key(world_id, run_id, producer, external_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/claims/acquire",
            {
                "scope_key": scope,
                "run_id": run_id,
                "producer": producer,
                "external_id": external_id,
                "payload_digest": payload_digest,
                "claimant": claimant,
                "tick": tick,
                "lease_seconds": lease_seconds,
            },
        )
        body = response.json()
        return body["outcome"], _claim_from_json(world_id, body["claim"])

    async def record_claim_table(self, world_id: str, scope_key: str, table_id: str) -> None:
        await self._call("POST", f"/w/{world_id}/claims/{scope_key}/table", {"table_id": table_id})

    async def rearm_claim(
        self,
        world_id: str,
        scope_key: str,
        claimant: str,
        commit_token: str,
    ) -> ClaimRecord:
        response = await self._call(
            "POST",
            f"/w/{world_id}/claims/{scope_key}/rearm",
            {"claimant": claimant, "commit_token": commit_token},
        )
        return _claim_from_json(world_id, response.json())

    async def complete_claim(
        self, world_id: str, scope_key: str, claimant: str, table_id: str
    ) -> None:
        await self._call(
            "POST",
            f"/w/{world_id}/claims/{scope_key}/complete",
            {"claimant": claimant, "table_id": table_id},
        )

    async def get_claim(self, world_id: str, scope_key: str) -> ClaimRecord | None:
        response = await self._call(
            "GET", f"/w/{world_id}/claims/{scope_key}", ignore_status=(404,)
        )
        if response.status_code == 404:
            return None
        return _claim_from_json(world_id, response.json())

    # ── artifact publications ────────────────────────────────────────────────

    async def acquire_artifact_publication(
        self,
        *,
        world_id: str,
        run_id: str,
        attempt_id: str,
        idempotency_key: str,
        request_digest: str,
        request_json: str,
        claimant: str,
        retry_window_ms: int,
        retry_not_after_ms: int | None = None,
        lease_ms: int = 900_000,
    ) -> tuple[str, ArtifactPublicationRecord]:
        claimant = _require_bounded_text(
            claimant, field="artifact publication claimant", max_chars=1024
        )
        retry_window_ms = _require_artifact_milliseconds(
            retry_window_ms,
            field="artifact retry_window_ms",
            maximum=_MAX_ARTIFACT_RETRY_WINDOW_MS,
        )
        if retry_not_after_ms is not None:
            retry_not_after_ms = _require_portable_counter(
                retry_not_after_ms, field="artifact retry_not_after_ms"
            )
        lease_ms = _require_artifact_lease_ms(lease_ms)
        publication_key = artifact_publication_key(world_id, run_id, idempotency_key)
        await self._require_artifact_server_clock_protocol(world_id)
        payload: dict[str, object] = {
            "publication_key": publication_key,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "idempotency_key": idempotency_key,
            "request_digest": request_digest,
            "request_json": request_json,
            "claimant": claimant,
            "retry_window_ms": retry_window_ms,
            "lease_ms": lease_ms,
        }
        if retry_not_after_ms is not None:
            payload["retry_not_after_ms"] = retry_not_after_ms
        response = await self._call(
            "POST",
            f"/w/{world_id}/artifact-publications/acquire-v3",
            payload,
        )
        body = response.json()
        outcome, publication = _artifact_acquisition_from_json(world_id, body)
        if publication is None:
            raise RuntimeError("initial artifact acquisition returned no publication")
        if publication.publication_key != publication_key:
            raise RuntimeError("initial artifact acquisition returned a different publication")
        return outcome, publication

    async def recover_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        *,
        lease_ms: int,
    ) -> tuple[str, ArtifactPublicationRecord | None]:
        publication_key = _require_sha256(publication_key, field="publication_key")
        claimant = _require_bounded_text(
            claimant, field="artifact publication claimant", max_chars=1024
        )
        lease_ms = _require_artifact_lease_ms(lease_ms)
        await self._require_artifact_server_clock_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/artifact-publications/{publication_key}/recover-v1",
            {"claimant": claimant, "lease_ms": lease_ms},
        )
        outcome, publication = _artifact_acquisition_from_json(
            world_id, response.json(), allow_obsolete=True
        )
        if publication is not None and publication.publication_key != publication_key:
            raise RuntimeError("exact artifact recovery returned a different publication")
        return outcome, publication

    async def renew_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        *,
        lease_seconds: float,
    ) -> ArtifactPublicationRecord:
        lease_seconds = _require_artifact_lease_seconds(lease_seconds)
        await self._require_artifact_mutation_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/artifact-publications/{publication_key}/renew-v2",
            {"claimant": claimant, "lease_seconds": lease_seconds},
        )
        return _artifact_publication_from_json(world_id, response.json())

    async def record_artifact_uploads(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        records_json: str,
        manifest_uri: str,
    ) -> None:
        await self._require_artifact_mutation_protocol(world_id)
        await self._call(
            "POST",
            f"/w/{world_id}/artifact-publications/{publication_key}/uploads-v2",
            {
                "claimant": claimant,
                "records_json": records_json,
                "manifest_uri": manifest_uri,
            },
        )

    async def complete_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        index_snapshot_id: int,
    ) -> None:
        if (
            isinstance(index_snapshot_id, bool)
            or not isinstance(index_snapshot_id, int)
            or index_snapshot_id <= 0
            or index_snapshot_id > MAX_ICEBERG_SNAPSHOT_ID
        ):
            raise ValueError("index_snapshot_id must be a positive integer no greater than 2^63-1")
        await self._require_artifact_mutation_protocol(world_id)
        await self._call(
            "POST",
            f"/w/{world_id}/artifact-publications/{publication_key}/complete-v2",
            {"claimant": claimant, "index_snapshot_id": str(index_snapshot_id)},
        )

    async def fail_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        error: str,
        *,
        retry_delay_ms: int,
    ) -> None:
        claimant = _require_bounded_text(
            claimant, field="artifact publication claimant", max_chars=1024
        )
        if not isinstance(error, str):
            raise TypeError("artifact publication error must be a string")
        if len(error) > 8000:
            raise ValueError("artifact publication error exceeds 8000 characters")
        retry_delay_ms = _require_artifact_milliseconds(
            retry_delay_ms,
            field="artifact retry_delay_ms",
            maximum=_MAX_ARTIFACT_RETRY_DELAY_MS,
        )
        await self._require_artifact_server_clock_protocol(world_id)
        await self._call(
            "POST",
            f"/w/{world_id}/artifact-publications/{publication_key}/fail-v3",
            {
                "claimant": claimant,
                "error": error,
                "retry_delay_ms": retry_delay_ms,
            },
        )

    async def expire_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        error: str,
    ) -> None:
        claimant = _require_bounded_text(
            claimant, field="artifact publication claimant", max_chars=1024
        )
        if not isinstance(error, str):
            raise TypeError("artifact publication error must be a string")
        if len(error) > 8000:
            raise ValueError("artifact publication error exceeds 8000 characters")
        await self._require_artifact_mutation_protocol(world_id)
        await self._call(
            "POST",
            f"/w/{world_id}/artifact-publications/{publication_key}/expire-v2",
            {"claimant": claimant, "error": error},
        )

    async def get_artifact_publication(
        self, world_id: str, publication_key: str
    ) -> ArtifactPublicationRecord | None:
        response = await self._call(
            "GET",
            f"/w/{world_id}/artifact-publications/{publication_key}",
            ignore_status=(404,),
        )
        if response.status_code == 404:
            return None
        return _artifact_publication_from_json(world_id, response.json())

    async def list_due_artifact_publications(
        self,
        world_id: str,
        *,
        limit: int = 100,
        after_publication_key: str = "",
    ) -> list[ArtifactPublicationCandidate]:
        if type(limit) is not int or limit < 1 or limit > 10_000:
            raise ValueError("artifact publication page limit must be between 1 and 10000")
        if after_publication_key != "":
            after_publication_key = _require_sha256(
                after_publication_key, field="after_publication_key"
            )
        await self._require_artifact_server_clock_protocol(world_id)
        query: dict[str, str | int] = {"limit": limit}
        if after_publication_key != "":
            query["after_publication_key"] = after_publication_key
        response = await self._call(
            "GET",
            f"/w/{world_id}/artifact-publications/due-v1?{urlencode(query)}",
        )
        body = response.json()
        if not isinstance(body, list) or len(body) > limit:
            raise RuntimeError("remote artifact due list returned an invalid page size")
        records = [_artifact_candidate_from_json(row) for row in body]
        previous = after_publication_key
        for record in records:
            if record.publication_key <= previous:
                raise RuntimeError("remote artifact due list is not strictly ordered")
            previous = record.publication_key
        return records

    # ── fleet recovery coordination (issue #503) ───────────────────────────

    async def ensure_recovery_sweep(
        self,
        storage_fingerprint: str,
        world_id: str,
        kind: str,
        *,
        max_consecutive_failures: int,
        initial_delay_ms: int = 0,
    ) -> RecoverySweepRecord:
        storage_fingerprint = _require_sha256(storage_fingerprint, field="storage_fingerprint")
        kind = _require_recovery_kind(kind)
        initial_delay_ms = _require_recovery_delay(initial_delay_ms, field="initial_delay_ms")
        if (
            isinstance(max_consecutive_failures, bool)
            or not isinstance(max_consecutive_failures, int)
            or max_consecutive_failures < 1
            or max_consecutive_failures > 1_000_000
        ):
            raise ValueError("max_consecutive_failures must be between 1 and 1000000")
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/sweeps/ensure-v1",
            {
                "storage_fingerprint": storage_fingerprint,
                "kind": kind,
                "max_consecutive_failures": max_consecutive_failures,
                "initial_delay_ms": initial_delay_ms,
            },
        )
        return _require_remote_sweep_receipt(
            _recovery_sweep_from_json(world_id, response.json()),
            "sweep ensure",
            storage_fingerprint=storage_fingerprint,
            kind=kind,
            max_consecutive_failures=max_consecutive_failures,
        )

    async def lease_recovery_sweep(
        self,
        world_id: str,
        kind: str,
        claimant: str,
        *,
        lease_ms: int,
    ) -> tuple[str, RecoverySweepRecord]:
        kind = _require_recovery_kind(kind)
        claimant = _require_bounded_text(claimant, field="recovery claimant", max_chars=1024)
        lease_ms = _require_recovery_lease(lease_ms)
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/sweeps/lease-v1",
            {"kind": kind, "claimant": claimant, "lease_ms": lease_ms},
        )
        body = response.json()
        if not isinstance(body, dict):
            raise RuntimeError("remote recovery lease returned an invalid outcome")
        if set(body) != _RECOVERY_LEASE_RESPONSE_FIELDS:
            raise RuntimeError("remote recovery lease returned an invalid envelope")
        outcome = body.get("outcome")
        if not isinstance(outcome, str) or outcome not in {
            "acquired",
            "owned",
            "recovered",
            "not_due",
            "paused",
        }:
            raise RuntimeError("remote recovery lease returned an invalid outcome")
        record = _require_remote_sweep_receipt(
            _recovery_sweep_from_json(world_id, body.get("sweep")),
            "sweep lease",
            kind=kind,
        )
        if outcome in {"acquired", "owned", "recovered"} and record.claimant != claimant:
            raise RuntimeError("remote recovery lease returned a different claimant")
        if outcome == "acquired":
            _require_remote_sweep_transition_target(
                record,
                "lease",
                (RecoverySweepStatus.IDLE, RecoverySweepEvent.LEASE),
                (RecoverySweepStatus.RETRY_WAIT, RecoverySweepEvent.LEASE),
            )
            _require_remote_schedule_delta(
                operation="sweep lease",
                updated_at_ms=record.updated_at_ms,
                scheduled_at_ms=record.lease_expires_at_ms,
                delay_ms=lease_ms,
            )
        elif outcome == "recovered":
            _require_remote_sweep_transition_target(
                record,
                "lease takeover",
                (RecoverySweepStatus.LEASED, RecoverySweepEvent.TAKE_OVER),
            )
            _require_remote_schedule_delta(
                operation="sweep lease takeover",
                updated_at_ms=record.updated_at_ms,
                scheduled_at_ms=record.lease_expires_at_ms,
                delay_ms=lease_ms,
            )
        elif outcome == "owned" and record.status != RecoverySweepStatus.LEASED.value:
            raise RuntimeError("remote recovery owned lease returned a non-leased sweep")
        elif outcome == "paused" and record.status != RecoverySweepStatus.PAUSED.value:
            raise RuntimeError("remote recovery paused outcome returned a non-paused sweep")
        elif outcome == "not_due" and record.status not in {
            RecoverySweepStatus.IDLE.value,
            RecoverySweepStatus.RETRY_WAIT.value,
        }:
            raise RuntimeError("remote recovery not-due outcome returned an active sweep")
        return outcome, record

    async def renew_recovery_sweep(
        self,
        world_id: str,
        kind: str,
        claimant: str,
        fence_epoch: int,
        *,
        lease_ms: int,
    ) -> RecoverySweepRecord:
        kind = _require_recovery_kind(kind)
        claimant = _require_bounded_text(claimant, field="recovery claimant", max_chars=1024)
        fence_epoch = _require_portable_counter(fence_epoch)
        lease_ms = _require_recovery_lease(lease_ms)
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/sweeps/renew-v1",
            {
                "kind": kind,
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "lease_ms": lease_ms,
            },
        )
        record = _require_remote_sweep_receipt(
            _require_remote_sweep_transition_target(
                _recovery_sweep_from_json(world_id, response.json()),
                "renewal",
                (RecoverySweepStatus.LEASED, RecoverySweepEvent.RENEW),
            ),
            "sweep renewal",
            kind=kind,
            claimant=claimant,
            fence_epoch=fence_epoch,
        )
        _require_remote_schedule_delta(
            operation="sweep renewal",
            updated_at_ms=record.updated_at_ms,
            scheduled_at_ms=record.lease_expires_at_ms,
            delay_ms=lease_ms,
        )
        return record

    async def checkpoint_recovery_sweep(
        self,
        world_id: str,
        kind: str,
        claimant: str,
        fence_epoch: int,
        *,
        cursor: str,
        active_subject_key: str = "",
    ) -> RecoverySweepRecord:
        kind = _require_recovery_kind(kind)
        claimant = _require_bounded_text(claimant, field="recovery claimant", max_chars=1024)
        fence_epoch = _require_portable_counter(fence_epoch)
        if cursor != "":
            cursor = _require_sha256(cursor, field="recovery cursor")
        if active_subject_key != "":
            active_subject_key = _require_sha256(active_subject_key, field="active_subject_key")
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/sweeps/checkpoint-v1",
            {
                "kind": kind,
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "cursor": cursor,
                "active_subject_key": active_subject_key,
            },
        )
        return _require_remote_sweep_receipt(
            _require_remote_sweep_transition_target(
                _recovery_sweep_from_json(world_id, response.json()),
                "checkpoint",
                (RecoverySweepStatus.LEASED, RecoverySweepEvent.CHECKPOINT),
            ),
            "sweep checkpoint",
            kind=kind,
            claimant=claimant,
            fence_epoch=fence_epoch,
            cursor=cursor,
            active_subject_key=active_subject_key,
        )

    async def yield_recovery_sweep(
        self,
        world_id: str,
        kind: str,
        claimant: str,
        fence_epoch: int,
        *,
        next_delay_ms: int,
    ) -> RecoverySweepRecord:
        kind = _require_recovery_kind(kind)
        claimant = _require_bounded_text(claimant, field="recovery claimant", max_chars=1024)
        fence_epoch = _require_portable_counter(fence_epoch)
        next_delay_ms = _require_recovery_delay(next_delay_ms, field="next_delay_ms")
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/sweeps/yield-v1",
            {
                "kind": kind,
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "next_delay_ms": next_delay_ms,
            },
        )
        record = _require_remote_sweep_receipt(
            _require_remote_sweep_transition_target(
                _recovery_sweep_from_json(world_id, response.json()),
                "yield",
                (RecoverySweepStatus.LEASED, RecoverySweepEvent.YIELD),
            ),
            "sweep yield",
            kind=kind,
            claimant=claimant,
            fence_epoch=fence_epoch,
            lease_expires_at_ms=0,
            active_subject_key="",
            consecutive_failures=0,
            last_error_code="",
            last_error_detail="",
            paused_at_ms=None,
        )
        _require_remote_schedule_delta(
            operation="sweep yield",
            updated_at_ms=record.updated_at_ms,
            scheduled_at_ms=record.next_due_at_ms,
            delay_ms=next_delay_ms,
        )
        return record

    async def fail_recovery_sweep(
        self,
        world_id: str,
        kind: str,
        claimant: str,
        fence_epoch: int,
        *,
        error_code: str,
        error_detail: str,
        retry_delay_ms: int,
    ) -> RecoverySweepRecord:
        kind = _require_recovery_kind(kind)
        claimant = _require_bounded_text(claimant, field="recovery claimant", max_chars=1024)
        fence_epoch = _require_portable_counter(fence_epoch)
        error_code, error_detail = _validate_recovery_error(error_code, error_detail)
        retry_delay_ms = _require_recovery_delay(retry_delay_ms, field="retry_delay_ms")
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/sweeps/fail-v1",
            {
                "kind": kind,
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "error_code": error_code,
                "error_detail": error_detail,
                "retry_delay_ms": retry_delay_ms,
            },
        )
        record = _require_remote_sweep_receipt(
            _require_remote_sweep_transition_target(
                _recovery_sweep_from_json(world_id, response.json()),
                "failure",
                (RecoverySweepStatus.LEASED, RecoverySweepEvent.FAIL),
                (RecoverySweepStatus.LEASED, RecoverySweepEvent.EXHAUST),
            ),
            "sweep failure",
            kind=kind,
            claimant=claimant,
            fence_epoch=fence_epoch,
            lease_expires_at_ms=0,
            last_error_code=error_code,
            last_error_detail=error_detail,
        )
        _require_remote_schedule_delta(
            operation="sweep failure",
            updated_at_ms=record.updated_at_ms,
            scheduled_at_ms=record.next_due_at_ms,
            delay_ms=retry_delay_ms,
        )
        return record

    async def pause_recovery_sweep(
        self,
        world_id: str,
        kind: str,
        claimant: str,
        fence_epoch: int,
        *,
        error_code: str,
        error_detail: str,
    ) -> RecoverySweepRecord:
        kind = _require_recovery_kind(kind)
        claimant = _require_bounded_text(claimant, field="recovery claimant", max_chars=1024)
        fence_epoch = _require_portable_counter(fence_epoch)
        error_code, error_detail = _validate_recovery_error(error_code, error_detail)
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/sweeps/pause-v1",
            {
                "kind": kind,
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "error_code": error_code,
                "error_detail": error_detail,
            },
        )
        return _require_remote_sweep_receipt(
            _require_remote_sweep_transition_target(
                _recovery_sweep_from_json(world_id, response.json()),
                "pause",
                (RecoverySweepStatus.LEASED, RecoverySweepEvent.PAUSE),
            ),
            "sweep pause",
            kind=kind,
            claimant=claimant,
            fence_epoch=fence_epoch,
            lease_expires_at_ms=0,
            last_error_code=error_code,
            last_error_detail=error_detail,
        )

    async def redrive_recovery_sweep(
        self,
        world_id: str,
        kind: str,
        *,
        expected_fence_epoch: int,
        delay_ms: int = 0,
    ) -> RecoverySweepRecord:
        kind = _require_recovery_kind(kind)
        expected_fence_epoch = _require_portable_counter(
            expected_fence_epoch, field="expected_fence_epoch"
        )
        if expected_fence_epoch == _MAX_PORTABLE_COUNTER:
            raise ValueError("expected_fence_epoch must leave room for the redrive fence")
        delay_ms = _require_recovery_delay(delay_ms, field="delay_ms")
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/sweeps/redrive-v1",
            {
                "kind": kind,
                "expected_fence_epoch": expected_fence_epoch,
                "delay_ms": delay_ms,
            },
        )
        record = _require_remote_sweep_receipt(
            _require_remote_sweep_transition_target(
                _recovery_sweep_from_json(world_id, response.json()),
                "redrive",
                (RecoverySweepStatus.PAUSED, RecoverySweepEvent.REDRIVE),
            ),
            "sweep redrive",
            kind=kind,
            claimant="",
            fence_epoch=expected_fence_epoch + 1,
            lease_expires_at_ms=0,
            consecutive_failures=0,
            last_error_code="",
            last_error_detail="",
            paused_at_ms=None,
        )
        _require_remote_schedule_delta(
            operation="sweep redrive",
            updated_at_ms=record.updated_at_ms,
            scheduled_at_ms=record.next_due_at_ms,
            delay_ms=delay_ms,
        )
        return record

    async def list_recovery_sweeps(
        self, world_id: str, *, status: str | None = None
    ) -> list[RecoverySweepRecord]:
        if status is not None and status not in _RECOVERY_SWEEP_STATUSES:
            raise ValueError(f"unsupported recovery sweep status {status!r}")
        await self._require_recovery_protocol(world_id)
        query = urlencode({"status": status}) if status is not None else ""
        suffix = f"?{query}" if query else ""
        response = await self._call("GET", f"/w/{world_id}/recovery/sweeps{suffix}")
        body = response.json()
        if not isinstance(body, list) or len(body) > len(_RECOVERY_KINDS):
            raise RuntimeError("remote recovery sweep list exceeds the closed kind set")
        records = [_recovery_sweep_from_json(world_id, row) for row in body]
        previous: tuple[str, str] | None = None
        seen_kinds: set[str] = set()
        for record in records:
            identity = (record.kind, record.sweep_key)
            if record.kind in seen_kinds:
                raise RuntimeError("remote recovery sweep list returned a duplicate kind")
            if previous is not None and identity <= previous:
                raise RuntimeError("remote recovery sweep list is not strictly ordered")
            if status is not None and record.status != status:
                raise RuntimeError("remote recovery sweep list violated its status filter")
            seen_kinds.add(record.kind)
            previous = identity
        return records

    async def retry_recovery_exception(
        self,
        world_id: str,
        kind: str,
        claimant: str,
        fence_epoch: int,
        *,
        subject_key: str,
        authority_key: str,
        expected_attempt_count: int,
        error_code: str,
        error_detail: str,
        retry_delay_ms: int,
        max_attempts: int,
        permanent: bool = False,
    ) -> RecoveryExceptionRecord:
        kind = _require_recovery_kind(kind)
        claimant = _require_bounded_text(claimant, field="recovery claimant", max_chars=1024)
        fence_epoch = _require_portable_counter(fence_epoch)
        if type(permanent) is not bool:
            raise TypeError("permanent must be a boolean")
        subject_key = _require_sha256(subject_key, field="subject_key")
        authority_key = _require_sha256(authority_key, field="authority_key")
        error_code, error_detail = _validate_recovery_error(error_code, error_detail)
        retry_delay_ms = _require_recovery_delay(retry_delay_ms, field="retry_delay_ms")
        if (
            isinstance(expected_attempt_count, bool)
            or not isinstance(expected_attempt_count, int)
            or expected_attempt_count < 0
            or expected_attempt_count >= _MAX_PORTABLE_COUNTER
        ):
            raise ValueError(
                "expected_attempt_count must be a non-negative portable incrementable integer"
            )
        if (
            isinstance(max_attempts, bool)
            or not isinstance(max_attempts, int)
            or max_attempts < 1
            or max_attempts > 1_000_000
        ):
            raise ValueError("max_attempts must be between 1 and 1000000")
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/exceptions/retry-v1",
            {
                "kind": kind,
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "subject_key": subject_key,
                "authority_key": authority_key,
                "expected_attempt_count": expected_attempt_count,
                "error_code": error_code,
                "error_detail": error_detail,
                "retry_delay_ms": retry_delay_ms,
                "max_attempts": max_attempts,
                "permanent": permanent,
            },
        )
        event = (
            RecoveryExceptionEvent.DEAD_LETTER
            if permanent or expected_attempt_count + 1 >= max_attempts
            else RecoveryExceptionEvent.RETRY
        )
        record = _require_remote_exception_receipt(
            _require_remote_exception_transition_target(
                _recovery_exception_from_json(world_id, response.json()),
                "exception retry",
                (None, event),
                (RecoveryExceptionStatus.RETRY_WAIT, event),
            ),
            "exception retry",
            kind=kind,
            subject_key=subject_key,
            authority_key=authority_key,
            attempt_count=expected_attempt_count + 1,
            max_attempts=max_attempts,
            last_error_code=error_code,
            last_error_detail=error_detail,
        )
        _require_remote_schedule_delta(
            operation="exception retry",
            updated_at_ms=record.updated_at_ms,
            scheduled_at_ms=record.retry_at_ms,
            delay_ms=retry_delay_ms,
        )
        return record

    async def resolve_recovery_exception(
        self,
        world_id: str,
        kind: str,
        claimant: str,
        fence_epoch: int,
        exception_key: str,
    ) -> RecoveryExceptionRecord:
        kind = _require_recovery_kind(kind)
        claimant = _require_bounded_text(claimant, field="recovery claimant", max_chars=1024)
        fence_epoch = _require_portable_counter(fence_epoch)
        exception_key = _require_sha256(exception_key, field="exception_key")
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/exceptions/resolve-v1",
            {
                "kind": kind,
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "exception_key": exception_key,
            },
        )
        return _require_remote_exception_receipt(
            _require_remote_exception_transition_target(
                _recovery_exception_from_json(world_id, response.json()),
                "exception resolution",
                (RecoveryExceptionStatus.RETRY_WAIT, RecoveryExceptionEvent.RESOLVE),
                (RecoveryExceptionStatus.DEAD_LETTER, RecoveryExceptionEvent.RESOLVE),
            ),
            "exception resolution",
            kind=kind,
            exception_key=exception_key,
        )

    async def redrive_recovery_exception(
        self,
        world_id: str,
        kind: str,
        claimant: str,
        fence_epoch: int,
        exception_key: str,
        *,
        expected_attempt_count: int,
        retry_delay_ms: int = 0,
    ) -> RecoveryExceptionRecord:
        kind = _require_recovery_kind(kind)
        claimant = _require_bounded_text(claimant, field="recovery claimant", max_chars=1024)
        fence_epoch = _require_portable_counter(fence_epoch)
        exception_key = _require_sha256(exception_key, field="exception_key")
        retry_delay_ms = _require_recovery_delay(retry_delay_ms, field="retry_delay_ms")
        if (
            isinstance(expected_attempt_count, bool)
            or not isinstance(expected_attempt_count, int)
            or expected_attempt_count < 0
            or expected_attempt_count > _MAX_PORTABLE_COUNTER
        ):
            raise ValueError("expected_attempt_count must be a portable non-negative integer")
        await self._require_recovery_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/recovery/exceptions/redrive-v1",
            {
                "kind": kind,
                "claimant": claimant,
                "fence_epoch": fence_epoch,
                "exception_key": exception_key,
                "expected_attempt_count": expected_attempt_count,
                "retry_delay_ms": retry_delay_ms,
            },
        )
        record = _require_remote_exception_receipt(
            _require_remote_exception_transition_target(
                _recovery_exception_from_json(world_id, response.json()),
                "exception redrive",
                (RecoveryExceptionStatus.DEAD_LETTER, RecoveryExceptionEvent.REDRIVE),
            ),
            "exception redrive",
            kind=kind,
            exception_key=exception_key,
            attempt_count=expected_attempt_count,
        )
        _require_remote_schedule_delta(
            operation="exception redrive",
            updated_at_ms=record.updated_at_ms,
            scheduled_at_ms=record.retry_at_ms,
            delay_ms=retry_delay_ms,
        )
        return record

    async def get_recovery_exception(
        self, world_id: str, kind: str, exception_key: str
    ) -> RecoveryExceptionRecord | None:
        kind = _require_recovery_kind(kind)
        exception_key = _require_sha256(exception_key, field="exception_key")
        await self._require_recovery_protocol(world_id)
        query = urlencode({"kind": kind})
        response = await self._call(
            "GET",
            f"/w/{world_id}/recovery/exceptions/{exception_key}?{query}",
            ignore_status=(404,),
        )
        if response.status_code == 404:
            return None
        return _require_remote_exception_receipt(
            _recovery_exception_from_json(world_id, response.json()),
            "exception lookup",
            kind=kind,
            exception_key=exception_key,
        )

    async def list_recovery_exceptions(
        self,
        world_id: str,
        *,
        kind: str | None = None,
        status: str | None = None,
        due_only: bool = False,
        limit: int = 100,
    ) -> list[RecoveryExceptionRecord]:
        if kind is not None:
            kind = _require_recovery_kind(kind)
        if type(due_only) is not bool:
            raise TypeError("due_only must be a boolean")
        if status is not None and status not in _RECOVERY_EXCEPTION_STATUSES:
            raise ValueError(f"unsupported recovery exception status {status!r}")
        if type(limit) is not int or limit < 1 or limit > 10_000:
            raise ValueError("recovery exception limit must be between 1 and 10000")
        if due_only and status not in {None, "retry_wait"}:
            raise ValueError("due_only recovery exceptions must have retry_wait status")
        await self._require_recovery_protocol(world_id)
        query_values: dict[str, str | int] = {"limit": limit}
        if kind is not None:
            query_values["kind"] = kind
        if status is not None:
            query_values["status"] = status
        if due_only:
            query_values["due_only"] = "1"
        response = await self._call(
            "GET",
            f"/w/{world_id}/recovery/exceptions?{urlencode(query_values)}",
        )
        body = response.json()
        if not isinstance(body, list) or len(body) > limit:
            raise RuntimeError("remote recovery exception list returned an invalid page size")
        records = [_recovery_exception_from_json(world_id, row) for row in body]
        previous: tuple[int, str] | None = None
        for record in records:
            identity = (record.retry_at_ms, record.exception_key)
            if previous is not None and identity <= previous:
                raise RuntimeError("remote recovery exception list is not strictly ordered")
            if kind is not None and record.kind != kind:
                raise RuntimeError("remote recovery exception list violated its kind filter")
            if status is not None and record.status != status:
                raise RuntimeError("remote recovery exception list violated its status filter")
            if due_only and record.status != "retry_wait":
                raise RuntimeError("remote due recovery exception list returned a non-retry row")
            previous = identity
        return records


def _recovery_world_from_json(row: object) -> WorldRecord:
    if not isinstance(row, dict):
        raise RuntimeError("remote world discovery row is not an object")
    row_dict = cast(dict[str, object], row)
    _require_exact_recovery_response_fields(
        row_dict,
        _RECOVERY_WORLD_RESPONSE_FIELDS,
        record="world discovery row",
    )
    world_id = row_dict.get("world_id")
    if not isinstance(world_id, str):
        raise RuntimeError("remote world discovery returned an invalid world identity")
    for field in ("name", "run_id", "parent_world_id"):
        value = row_dict.get(field)
        if value is not None and not isinstance(value, str):
            raise RuntimeError(f"remote world discovery returned a non-string {field}")
    status = row_dict.get("status")
    if status not in {"active", "destroyed"}:
        raise RuntimeError("remote world discovery returned an invalid status")
    tick_head = row_dict.get("tick_head")
    if type(tick_head) is not int or tick_head < 0 or tick_head > _MAX_PORTABLE_COUNTER:
        raise RuntimeError("remote world discovery returned a lossy tick_head")
    return WorldRecord(
        world_id=world_id,
        name=cast(str | None, row_dict["name"]),
        run_id=cast(str | None, row_dict["run_id"]),
        parent_world_id=cast(str | None, row_dict["parent_world_id"]),
        status=cast(str, status),
        tick_head=tick_head,
    )


def _world_from_json(row: dict) -> WorldRecord:
    return WorldRecord(
        world_id=row["world_id"],
        name=row.get("name"),
        run_id=row.get("run_id"),
        parent_world_id=row.get("parent_world_id"),
        status=row["status"],
        tick_head=int(row.get("tick_head", 0)),
    )


def _claim_from_json(world_id: str, row: dict) -> ClaimRecord:
    return ClaimRecord(
        scope_key=row["scope_key"],
        world_id=world_id,
        run_id=row["run_id"],
        producer=row["producer"],
        external_id=row["external_id"],
        payload_digest=row["payload_digest"],
        status=row["status"],
        commit_token=row["commit_token"],
        tick=int(row["tick"]),
        artifact_entity_id=int(row.get("artifact_entity_id", 0)),
        table_id=row.get("table_id"),
        claimant=row["claimant"],
        lease_expires_at=float(row["lease_expires_at"]),
        fence_epoch=int(row["fence_epoch"]),
    )


def _attempt_claim_from_json(world_id: str, row: dict) -> AttemptClaimRecord:
    return AttemptClaimRecord(
        claim_key=row["claim_key"],
        world_id=world_id,
        run_id=row["run_id"],
        mission_id=row["mission_id"],
        task_id=row["task_id"],
        attempt_id=row["attempt_id"],
        idempotency_key=row["idempotency_key"],
        request_fingerprint=row["request_fingerprint"],
        request_json=row["request_json"],
        redaction_policy_id=row.get("redaction_policy_id", ""),
        redaction_evidence_json=row.get("redaction_evidence_json", ""),
        status=row["status"],
        provider=row["provider"],
        provider_request_fingerprint=row["provider_request_fingerprint"],
        supports_idempotent_replay=bool(row["supports_idempotent_replay"]),
        supports_session_resume=bool(row["supports_session_resume"]),
        provider_idempotency_key=row.get("provider_idempotency_key", ""),
        claimant=row["claimant"],
        lease_expires_at=float(row["lease_expires_at"]),
        fence_epoch=int(row["fence_epoch"]),
        execution_nonce=row.get("execution_nonce", ""),
        execution_consumed_at=row.get("execution_consumed_at"),
        provider_session_id=row.get("provider_session_id", ""),
        provider_request_id=row.get("provider_request_id", ""),
        settlement_status=row.get("settlement_status", ""),
        outcome_digest=row.get("outcome_digest", ""),
        outcome_json=row.get("outcome_json", ""),
        artifact_request_json=row.get("artifact_request_json", ""),
        artifact_request_digest=row.get("artifact_request_digest", ""),
        artifact_publication_key=row.get("artifact_publication_key", ""),
        legacy_unbound_eligible=bool(row.get("legacy_unbound_eligible", False)),
        last_error=row.get("last_error", ""),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        possibly_submitted_at=row.get("possibly_submitted_at"),
        acknowledged_at=row.get("acknowledged_at"),
        finalizing_at=row.get("finalizing_at"),
        settled_at=row.get("settled_at"),
    )


def _artifact_candidate_from_json(row: object) -> ArtifactPublicationCandidate:
    if not isinstance(row, dict) or set(row) != {"publication_key"}:
        raise RuntimeError("remote artifact due candidate is not digest-only")
    candidate = cast(dict[str, object], row)
    publication_key = candidate["publication_key"]
    if not isinstance(publication_key, str):
        raise RuntimeError("remote artifact due candidate has an invalid publication_key")
    try:
        publication_key = _require_sha256(publication_key, field="publication_key")
    except (TypeError, ValueError) as exc:
        raise RuntimeError("remote artifact due candidate has an invalid publication_key") from exc
    return ArtifactPublicationCandidate(publication_key=publication_key)


def _artifact_acquisition_from_json(
    world_id: str,
    body: object,
    *,
    allow_obsolete: bool = False,
) -> tuple[str, ArtifactPublicationRecord | None]:
    if not isinstance(body, dict):
        raise RuntimeError("remote artifact acquisition returned a non-object response")
    outcome = body.get("outcome")
    allowed = (
        {"owned", "recovered", "duplicate", "expired", "obsolete"}
        if allow_obsolete
        else {"acquired", "owned", "recovered", "duplicate", "expired"}
    )
    if not isinstance(outcome, str) or outcome not in allowed:
        raise RuntimeError("remote artifact acquisition returned an invalid outcome")
    publication = body.get("publication")
    if outcome == "obsolete":
        if publication is not None:
            raise RuntimeError("obsolete artifact acquisition returned a publication")
        return outcome, None
    if not isinstance(publication, dict):
        raise RuntimeError("remote artifact acquisition returned no publication")
    record = _artifact_publication_from_json(world_id, publication)
    expected_statuses = {
        "acquired": {"PENDING"},
        "owned": {"PENDING", "UPLOADED"},
        "recovered": {"PENDING", "UPLOADED"},
        "duplicate": {"INDEXED"},
        "expired": {"EXPIRED"},
    }
    if record.status not in expected_statuses[outcome]:
        raise RuntimeError("remote artifact acquisition outcome contradicts its status")
    return outcome, record


def _artifact_publication_from_json(world_id: str, row: dict) -> ArtifactPublicationRecord:
    if not isinstance(row, dict):
        raise RuntimeError("remote artifact publication is not an object")
    status = row.get("status")
    if not isinstance(status, str) or status not in {
        "PENDING",
        "UPLOADED",
        "INDEXED",
        "EXPIRED",
    }:
        raise RuntimeError(f"remote artifact publication has invalid status {status!r}")
    publication_key = row.get("publication_key")
    if not isinstance(publication_key, str):
        raise RuntimeError("remote artifact publication has a non-string publication_key")
    try:
        _require_sha256(publication_key, field="publication_key")
    except (TypeError, ValueError) as exc:
        raise RuntimeError("remote artifact publication has an invalid publication_key") from exc
    retry_until_ms = row.get("retry_until_ms")
    attempt_count = row.get("attempt_count")
    lease_expires_at = row.get("lease_expires_at")
    if (
        type(retry_until_ms) is not int
        or retry_until_ms < 0
        or retry_until_ms > _MAX_PORTABLE_COUNTER
    ):
        raise RuntimeError("remote artifact publication has invalid retry_until_ms")
    if type(attempt_count) is not int or attempt_count < 1 or attempt_count > _MAX_PORTABLE_COUNTER:
        raise RuntimeError("remote artifact publication has invalid attempt_count")
    if isinstance(lease_expires_at, bool) or not isinstance(lease_expires_at, int | float):
        raise RuntimeError("remote artifact publication has invalid lease_expires_at")
    lease_expires_at_value = float(lease_expires_at)
    if (
        not math.isfinite(lease_expires_at_value)
        or lease_expires_at_value < 0
        or lease_expires_at_value > _MAX_PORTABLE_COUNTER
    ):
        raise RuntimeError("remote artifact publication has invalid lease_expires_at")
    index_snapshot_id = _remote_index_snapshot_id(row)
    return ArtifactPublicationRecord(
        publication_key=publication_key,
        world_id=world_id,
        run_id=row["run_id"],
        attempt_id=row["attempt_id"],
        idempotency_key=row["idempotency_key"],
        request_digest=row["request_digest"],
        status=status,
        request_json=row["request_json"],
        records_json=row.get("records_json", "[]"),
        claimant=row["claimant"],
        lease_expires_at=lease_expires_at_value,
        retry_until_ms=retry_until_ms,
        attempt_count=attempt_count,
        index_snapshot_id=index_snapshot_id,
        manifest_uri=row.get("manifest_uri", ""),
        last_error=row.get("last_error", ""),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row.get("completed_at"),
    )


def _remote_recovery_int(
    row: dict,
    field: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    value = row.get(field)
    if type(value) is not int:
        raise RuntimeError(f"remote recovery record has non-integer {field}")
    if minimum is not None and value < minimum:
        raise RuntimeError(f"remote recovery record has out-of-range {field}")
    if maximum is not None and value > maximum:
        raise RuntimeError(f"remote recovery record has out-of-range {field}")
    return value


def _remote_optional_recovery_int(
    row: dict,
    field: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    value = row.get(field)
    if value is None:
        return None
    return _remote_recovery_int(row, field, minimum=minimum, maximum=maximum)


def _remote_recovery_text(row: dict, field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str):
        raise RuntimeError(f"remote recovery record has non-string {field}")
    return value


def _remote_recovery_error_code(row: dict) -> str:
    value = _remote_recovery_text(row, "last_error_code")
    if value and value not in _RECOVERY_ERROR_CODES:
        raise RuntimeError(f"remote recovery record has invalid last_error_code {value!r}")
    return value


def _remote_recovery_digest(row: dict, field: str, *, allow_empty: bool = False) -> str:
    value = _remote_recovery_text(row, field)
    if allow_empty and value == "":
        return value
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise RuntimeError(f"remote recovery record has invalid {field}")
    return value


def _require_exact_recovery_response_fields(
    row: dict, expected: frozenset[str], *, record: str
) -> None:
    actual = set(row)
    if actual == expected:
        return
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected, key=repr)
    raise RuntimeError(
        f"remote recovery {record} has invalid field set: "
        f"missing={missing!r}, unexpected={unexpected!r}"
    )


def _require_remote_sweep_timestamp_semantics(record: RecoverySweepRecord) -> None:
    if record.created_at_ms > record.updated_at_ms:
        raise RuntimeError("remote recovery sweep timestamps are not monotonic")
    if record.status == RecoverySweepStatus.PAUSED.value:
        if (
            record.paused_at_ms is None
            or record.paused_at_ms < record.created_at_ms
            or record.paused_at_ms > record.updated_at_ms
        ):
            raise RuntimeError("remote recovery paused sweep has invalid paused_at_ms")
    elif record.paused_at_ms is not None:
        raise RuntimeError("remote recovery non-paused sweep has paused_at_ms")


def _require_remote_exception_timestamp_semantics(record: RecoveryExceptionRecord) -> None:
    if record.created_at_ms > record.updated_at_ms:
        raise RuntimeError("remote recovery exception timestamps are not monotonic")
    for field in ("resolved_at_ms", "dead_lettered_at_ms"):
        value = getattr(record, field)
        if value is not None and not (record.created_at_ms <= value <= record.updated_at_ms):
            raise RuntimeError(f"remote recovery exception has invalid {field}")
    if record.status == RecoveryExceptionStatus.RETRY_WAIT.value:
        if record.resolved_at_ms is not None or record.dead_lettered_at_ms is not None:
            raise RuntimeError("remote recovery retry exception has terminal timestamps")
    elif record.status == RecoveryExceptionStatus.DEAD_LETTER.value:
        if record.resolved_at_ms is not None or record.dead_lettered_at_ms is None:
            raise RuntimeError("remote recovery dead-letter exception has invalid timestamps")
    elif record.resolved_at_ms is None:
        raise RuntimeError("remote recovery resolved exception has no resolved_at_ms")


def _recovery_sweep_from_json(world_id: str, row: dict) -> RecoverySweepRecord:
    if not isinstance(row, dict):
        raise RuntimeError("remote recovery sweep is not an object")
    _require_exact_recovery_response_fields(
        row,
        _RECOVERY_SWEEP_RESPONSE_FIELDS,
        record="sweep",
    )
    if row.get("world_id") != world_id:
        raise RuntimeError("remote recovery sweep belongs to a different world")
    status = row.get("status")
    if not isinstance(status, str) or status not in _RECOVERY_SWEEP_STATUSES:
        raise RuntimeError(f"remote recovery sweep has invalid status {status!r}")
    kind = row.get("kind")
    if not isinstance(kind, str) or kind not in _RECOVERY_KINDS:
        raise RuntimeError(f"remote recovery sweep has invalid kind {kind!r}")
    sweep_key = _remote_recovery_digest(row, "sweep_key")
    storage_fingerprint = _remote_recovery_digest(row, "storage_fingerprint")
    if sweep_key != recovery_sweep_key(storage_fingerprint, world_id, kind):
        raise RuntimeError("remote recovery sweep has a non-deterministic sweep_key")
    record = RecoverySweepRecord(
        sweep_key=sweep_key,
        storage_fingerprint=storage_fingerprint,
        world_id=world_id,
        kind=kind,
        status=status,
        cursor=_remote_recovery_digest(row, "cursor", allow_empty=True),
        cycle=_remote_recovery_int(row, "cycle", minimum=0, maximum=_MAX_PORTABLE_COUNTER),
        claimant=_remote_recovery_text(row, "claimant"),
        lease_expires_at_ms=_remote_recovery_int(
            row, "lease_expires_at_ms", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        fence_epoch=_remote_recovery_int(
            row, "fence_epoch", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        active_subject_key=_remote_recovery_digest(row, "active_subject_key", allow_empty=True),
        consecutive_failures=_remote_recovery_int(
            row, "consecutive_failures", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        max_consecutive_failures=_remote_recovery_int(
            row, "max_consecutive_failures", minimum=1, maximum=1_000_000
        ),
        next_due_at_ms=_remote_recovery_int(
            row, "next_due_at_ms", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        last_error_code=_remote_recovery_error_code(row),
        last_error_detail=_remote_recovery_text(row, "last_error_detail"),
        created_at_ms=_remote_recovery_int(
            row, "created_at_ms", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        updated_at_ms=_remote_recovery_int(
            row, "updated_at_ms", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        paused_at_ms=_remote_optional_recovery_int(
            row, "paused_at_ms", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
    )
    _require_remote_sweep_timestamp_semantics(record)
    return record


def _recovery_exception_from_json(world_id: str, row: dict) -> RecoveryExceptionRecord:
    if not isinstance(row, dict):
        raise RuntimeError("remote recovery exception is not an object")
    _require_exact_recovery_response_fields(
        row,
        _RECOVERY_EXCEPTION_RESPONSE_FIELDS,
        record="exception",
    )
    if row.get("world_id") != world_id:
        raise RuntimeError("remote recovery exception belongs to a different world")
    status = row.get("status")
    if not isinstance(status, str) or status not in _RECOVERY_EXCEPTION_STATUSES:
        raise RuntimeError(f"remote recovery exception has invalid status {status!r}")
    kind = row.get("kind")
    if not isinstance(kind, str) or kind not in _RECOVERY_KINDS:
        raise RuntimeError(f"remote recovery exception has invalid kind {kind!r}")
    storage_fingerprint = _remote_recovery_digest(row, "storage_fingerprint")
    sweep_key = _remote_recovery_digest(row, "sweep_key")
    if sweep_key != recovery_sweep_key(storage_fingerprint, world_id, kind):
        raise RuntimeError("remote recovery exception has a non-deterministic sweep_key")
    subject_key = _remote_recovery_digest(row, "subject_key")
    exception_key = _remote_recovery_digest(row, "exception_key")
    if exception_key != recovery_exception_key(sweep_key, subject_key):
        raise RuntimeError("remote recovery exception has a non-deterministic exception_key")
    record = RecoveryExceptionRecord(
        exception_key=exception_key,
        sweep_key=sweep_key,
        storage_fingerprint=storage_fingerprint,
        world_id=world_id,
        kind=kind,
        subject_key=subject_key,
        authority_key=_remote_recovery_digest(row, "authority_key"),
        status=status,
        attempt_count=_remote_recovery_int(
            row, "attempt_count", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        max_attempts=_remote_recovery_int(row, "max_attempts", minimum=1, maximum=1_000_000),
        retry_at_ms=_remote_recovery_int(
            row, "retry_at_ms", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        last_error_code=_remote_recovery_error_code(row),
        last_error_detail=_remote_recovery_text(row, "last_error_detail"),
        created_at_ms=_remote_recovery_int(
            row, "created_at_ms", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        updated_at_ms=_remote_recovery_int(
            row, "updated_at_ms", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        resolved_at_ms=_remote_optional_recovery_int(
            row, "resolved_at_ms", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
        dead_lettered_at_ms=_remote_optional_recovery_int(
            row, "dead_lettered_at_ms", minimum=0, maximum=_MAX_PORTABLE_COUNTER
        ),
    )
    _require_remote_exception_timestamp_semantics(record)
    return record


def _remote_index_snapshot_id(row: dict) -> int:
    """Parse only lossless Worker snapshot receipts."""

    raw = row.get("index_snapshot_id", "0")
    if row.get("status") != "INDEXED":
        if raw == "0" or (type(raw) is int and raw == 0):
            return 0
        raise RuntimeError("remote artifact publication has an invalid unindexed snapshot ID")
    if not (isinstance(raw, str) and raw.isascii() and raw.isdecimal() and not raw.startswith("0")):
        raise RuntimeError("remote INDEXED artifact publication has a lossy snapshot ID")
    parsed = int(raw)
    if parsed <= 0 or parsed > MAX_ICEBERG_SNAPSHOT_ID:
        raise RuntimeError("remote INDEXED artifact publication snapshot ID is out of range")
    return parsed


def _command_from_json(world_id: str, row: dict) -> CommandRecord:
    return CommandRecord(
        command_id=row["command_id"],
        world_id=world_id,
        sequence=int(row["sequence"]),
        scheduled_tick=int(row["scheduled_tick"]),
        priority=int(row["priority"]),
        command_type=row["command_type"],
        payload_json=row["payload_json"],
        payload_digest=row["payload_digest"],
        version=int(row["version"]),
        principal_id=row.get("principal_id"),
        origin=row["origin"],
        reserved_entity_id=(
            int(row["reserved_entity_id"]) if row.get("reserved_entity_id") is not None else None
        ),
        status=row["status"],
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        lease_owner=row.get("lease_owner"),
        lease_expires_at=(
            float(row["lease_expires_at"]) if row.get("lease_expires_at") is not None else None
        ),
        last_error_code=row.get("last_error_code"),
        last_error_detail=row.get("last_error_detail"),
        accepted_at=row["accepted_at"],
        updated_at=row["updated_at"],
        applied_tick=int(row["applied_tick"]) if row.get("applied_tick") is not None else None,
        commit_token=row.get("commit_token"),
    )


def _outbox_from_json(world_id: str, row: dict) -> OutboxRecord:
    return OutboxRecord(
        sequence=int(row["sequence"]),
        event_id=row["event_id"],
        world_id=world_id,
        aggregate_type=row["aggregate_type"],
        aggregate_id=row["aggregate_id"],
        event_type=row["event_type"],
        command_type=row["command_type"],
        status=row["status"],
        actor_id=row.get("actor_id"),
        payload_json=row["payload_json"],
        occurred_at=row["occurred_at"],
        projected_at=row.get("projected_at"),
    )
