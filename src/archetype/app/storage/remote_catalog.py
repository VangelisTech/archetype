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
    ArtifactPublicationCandidate,
    ArtifactPublicationConflictError,
    ArtifactPublicationExpiredError,
    ArtifactPublicationPendingError,
    ArtifactPublicationRecord,
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
    _require_artifact_lease_ms,
    _require_artifact_lease_seconds,
    _require_artifact_milliseconds,
    _require_bounded_text,
    _require_portable_counter,
    _require_sha256,
    artifact_publication_key,
    claim_scope_key,
)
from archetype.core.interfaces import StaleWriterError

logger = logging.getLogger(__name__)

_ARTIFACT_SNAPSHOT_PROTOCOL_VERSION = 3
_ARTIFACT_SNAPSHOT_CAPABILITY = "artifact_snapshot_decimal_v1"
_ARTIFACT_SERVER_CLOCK_PROTOCOL_VERSION = 6
_ARTIFACT_SERVER_CLOCK_CAPABILITY = "artifact_publication_server_clock_v1"

_ERROR_MAP: dict[str, type[Exception]] = {
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
