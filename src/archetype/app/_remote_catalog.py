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

from archetype.app._catalog import (
    CatalogConflictError,
    ClaimConflictError,
    ClaimPendingError,
    ClaimRecord,
    ManifestRecord,
    SignatureRecord,
    WorldRecord,
    claim_scope_key,
)
from archetype.core.interfaces import StaleWriterError

logger = logging.getLogger(__name__)

_ERROR_MAP: dict[str, type[Exception]] = {
    "catalog_conflict": CatalogConflictError,
    "claim_conflict": ClaimConflictError,
    "claim_pending": ClaimPendingError,
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

    async def publish_manifest(
        self,
        world_id: str,
        run_id: str,
        tick: int,
        commit_token: str,
        writer_epoch: int,
        table_ids: list[str],
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
            },
        )

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

    # ── claims ───────────────────────────────────────────────────────────────

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
        fact_entity_id=int(row.get("fact_entity_id", 0)),
        table_id=row.get("table_id"),
        claimant=row["claimant"],
        lease_expires_at=float(row["lease_expires_at"]),
        fence_epoch=int(row["fence_epoch"]),
    )
