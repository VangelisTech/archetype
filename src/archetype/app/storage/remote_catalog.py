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

import httpx

from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.app.storage.catalog import (
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
    SignatureRecord,
    WorldRecord,
    _validate_attempt_claim_transition,
    artifact_publication_key,
    claim_scope_key,
)
from archetype.core.interfaces import StaleWriterError

logger = logging.getLogger(__name__)

_ATTEMPT_CLAIM_PROTOCOL_VERSION = 4
_ATTEMPT_CLAIM_CAPABILITY = "attempt_claim_execution_v2"
_ARTIFACT_SNAPSHOT_PROTOCOL_VERSION = 3
_ARTIFACT_SNAPSHOT_CAPABILITY = "artifact_snapshot_decimal_v1"

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
    "stale_writer": StaleWriterError,
}


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
        capability: str,
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
        if (
            not isinstance(protocol_version, int)
            or protocol_version < minimum_version
            or not isinstance(capabilities, list)
            or capability not in capabilities
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
        retry_until_ms: int,
        lease_seconds: float = 900.0,
    ) -> tuple[str, ArtifactPublicationRecord]:
        publication_key = artifact_publication_key(world_id, run_id, idempotency_key)
        await self._require_artifact_snapshot_protocol(world_id)
        response = await self._call(
            "POST",
            f"/w/{world_id}/artifact-publications/acquire-v2",
            {
                "publication_key": publication_key,
                "run_id": run_id,
                "attempt_id": attempt_id,
                "idempotency_key": idempotency_key,
                "request_digest": request_digest,
                "request_json": request_json,
                "claimant": claimant,
                "retry_until_ms": retry_until_ms,
                "lease_seconds": lease_seconds,
            },
        )
        body = response.json()
        return body["outcome"], _artifact_publication_from_json(world_id, body["publication"])

    async def renew_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        *,
        lease_seconds: float,
    ) -> ArtifactPublicationRecord:
        await self._require_artifact_snapshot_protocol(world_id)
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
        await self._require_artifact_snapshot_protocol(world_id)
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
        await self._require_artifact_snapshot_protocol(world_id)
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
        retry_at: float,
    ) -> None:
        await self._require_artifact_snapshot_protocol(world_id)
        await self._call(
            "POST",
            f"/w/{world_id}/artifact-publications/{publication_key}/fail-v2",
            {"claimant": claimant, "error": error, "retry_at": retry_at},
        )

    async def expire_artifact_publication(
        self,
        world_id: str,
        publication_key: str,
        claimant: str,
        error: str,
    ) -> None:
        await self._require_artifact_snapshot_protocol(world_id)
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
        self, world_id: str, *, now: float, limit: int = 100
    ) -> list[ArtifactPublicationRecord]:
        response = await self._call(
            "GET",
            f"/w/{world_id}/artifact-publications?due={now}&limit={limit}",
        )
        return [_artifact_publication_from_json(world_id, row) for row in response.json()]


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


def _artifact_publication_from_json(world_id: str, row: dict) -> ArtifactPublicationRecord:
    index_snapshot_id = _remote_index_snapshot_id(row)
    return ArtifactPublicationRecord(
        publication_key=row["publication_key"],
        world_id=world_id,
        run_id=row["run_id"],
        attempt_id=row["attempt_id"],
        idempotency_key=row["idempotency_key"],
        request_digest=row["request_digest"],
        status=row["status"],
        request_json=row["request_json"],
        records_json=row.get("records_json", "[]"),
        claimant=row["claimant"],
        lease_expires_at=float(row["lease_expires_at"]),
        retry_until_ms=int(row["retry_until_ms"]),
        attempt_count=int(row.get("attempt_count", 1)),
        index_snapshot_id=index_snapshot_id,
        manifest_uri=row.get("manifest_uri", ""),
        last_error=row.get("last_error", ""),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row.get("completed_at"),
    )


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
