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

from archetype.app.storage.catalog import (
    CatalogConflictError,
    CommandAdmission,
    CommandConflictError,
    CommandRecord,
    EvaluationLease,
    ManifestRecord,
    OutboxRecord,
    SignatureRecord,
    WorldRecord,
)
from archetype.core.interfaces import StaleWriterError

logger = logging.getLogger(__name__)

_ERROR_MAP: dict[str, type[Exception]] = {
    "catalog_conflict": CatalogConflictError,
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

    # ── evaluation execution serialization ────────────────────────────────

    async def lease_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        subject_digest: str,
        contract_digest: str,
        owner: str,
        *,
        lease_seconds: float = 300.0,
    ) -> EvaluationLease:
        response = await self._call(
            "POST",
            f"/w/{world_id}/evaluations/lease",
            {
                "run_id": run_id,
                "evaluation_id": evaluation_id,
                "subject_digest": subject_digest,
                "contract_digest": contract_digest,
                "owner": owner,
                "lease_seconds": lease_seconds,
            },
        )
        return _evaluation_lease_from_json(world_id, response.json())

    async def complete_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        owner: str,
    ) -> None:
        await self._call(
            "POST",
            f"/w/{world_id}/evaluations/complete",
            {
                "run_id": run_id,
                "evaluation_id": evaluation_id,
                "owner": owner,
            },
        )

    async def release_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        owner: str,
    ) -> None:
        await self._call(
            "POST",
            f"/w/{world_id}/evaluations/release",
            {
                "run_id": run_id,
                "evaluation_id": evaluation_id,
                "owner": owner,
            },
        )

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


def _world_from_json(row: dict) -> WorldRecord:
    return WorldRecord(
        world_id=row["world_id"],
        name=row.get("name"),
        run_id=row.get("run_id"),
        parent_world_id=row.get("parent_world_id"),
        status=row["status"],
        tick_head=int(row.get("tick_head", 0)),
    )


def _evaluation_lease_from_json(world_id: str, row: dict) -> EvaluationLease:
    return EvaluationLease(
        world_id=world_id,
        run_id=row["run_id"],
        evaluation_id=row["evaluation_id"],
        subject_digest=row["subject_digest"],
        contract_digest=row["contract_digest"],
        status=row["status"],
        owner=row.get("owner"),
        lease_expires_at=(
            float(row["lease_expires_at"]) if row.get("lease_expires_at") is not None else None
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        acquired=bool(row["acquired"]),
    )


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
