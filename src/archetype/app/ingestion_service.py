# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ingestion Service (issue #274): replay-safe external facts.

Deliberately small and NOT MutationService: mutations stage RAM state that
persists on a later simulation step, so either crash outcome for a fact
would be wrong (a completed claim over RAM-only rows, or ingestion driving
a tick through every processor). Facts write durable rows directly, under
the claim's own commit identity, and become visible in the same catalog
transaction that completes the claim.

Exactly-once means exactly one logically VISIBLE fact per
(storage, world, run, producer, external_id). Physical appends may retry —
duplicates stay invisible because their tokens never enter the visible set.

Facts are non-processable by construction: they use catalog-allocated
entity ids in the negative metadata band, so they never enter entity2sig,
never join active simulation, and are excluded from resume's entity
directory. They are ordinary queryable rows in every other respect.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time

import daft
import pyarrow as pa
from uuid_utils import uuid7

from archetype.app._catalog import (
    ClaimPendingError,
    ClaimRecord,
    SignatureRecord,
    arrow_schema_descriptor,
    claim_scope_key,
    schema_fingerprint,
)
from archetype.app.facts import FactMeta, FactReceipt, fact_payload_digest
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig

logger = logging.getLogger(__name__)


class IngestionService:
    """Owns the fact-claim lifecycle: PENDING → append → flush → COMPLETE."""

    def __init__(self, storage_service: StorageService, world_service: WorldService) -> None:
        self._storage_service = storage_service
        self._world_service = world_service

    async def ingest_fact(
        self,
        world_id: str,
        components: list[Component],
        *,
        external_id: str,
        producer: str = "default",
        storage_config: StorageConfig | None = None,
        lease_seconds: float = 30.0,
    ) -> FactReceipt:
        """Ingest one external fact, exactly-once-visible.

        Works against live worlds (storage resolved from the registry) and
        cold ones (explicit ``storage_config``; the world must be recorded
        in the catalog). Concurrent identical submissions converge: one
        caller appends, the rest receive the original receipt.
        """
        if not components:
            raise ValueError("a fact needs at least one component")

        async def _ready(_claim: ClaimRecord) -> list[Component]:
            return components

        return await self._ingest(
            world_id,
            external_id=external_id,
            producer=producer,
            payload_digest=fact_payload_digest(components),
            build_components=_ready,
            storage_config=storage_config,
            lease_seconds=lease_seconds,
        )

    async def ingest_evaluated(
        self,
        world_id: str,
        *,
        evaluation_id: str,
        producer: str,
        identity_digest: str,
        build_components,
        storage_config: StorageConfig | None = None,
        lease_seconds: float = 30.0,
    ) -> FactReceipt:
        """Claim-BEFORE-grade (issue #275): the payload is built only after
        this claimant owns the claim.

        ``identity_digest`` names what the evaluation is OF (subject +
        contract), never the graded outcome — trials of nondeterministic
        graders share it while concluding differently. ``build_components``
        (the grader, composed by the gate layer) runs at most once per
        completed claim: a duplicate returns the persisted receipt without
        re-grading, and a lease takeover that finds the orphan rows
        completes without re-running.
        """
        return await self._ingest(
            world_id,
            external_id=evaluation_id,
            producer=producer,
            payload_digest=identity_digest,
            build_components=build_components,
            storage_config=storage_config,
            lease_seconds=lease_seconds,
        )

    async def _ingest(
        self,
        world_id: str,
        *,
        external_id: str,
        producer: str,
        payload_digest: str,
        build_components,
        storage_config: StorageConfig | None,
        lease_seconds: float,
    ) -> FactReceipt:
        if not external_id.strip():
            raise ValueError("external_id must be a non-empty producer-scoped identity")

        wid = str(world_id)
        effective = self._resolve_storage(wid, storage_config)
        catalog = self._storage_service.get_control_catalog(effective)
        record = await catalog.get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {effective.uri}")
        run_id = record.run_id
        if not run_id:
            raise RuntimeError(f"world {wid} has no recorded run; nothing to attach facts to")

        claimant = f"{socket.gethostname()}:{os.getpid()}:{uuid7().hex[:8]}"
        deadline = time.monotonic() + max(lease_seconds, 1.0) * 2

        while True:
            try:
                outcome, claim = await catalog.acquire_claim(
                    world_id=wid,
                    run_id=str(run_id),
                    producer=producer,
                    external_id=external_id,
                    payload_digest=payload_digest,
                    claimant=claimant,
                    tick=record.tick_head,
                    lease_seconds=lease_seconds,
                )
            except ClaimPendingError:
                # Another claimant is mid-flight on the identical fact.
                # Converge on its outcome instead of erroring: exactly one
                # visible fact either way.
                claim = await self._await_settled(catalog, wid, str(run_id), producer, external_id)
                if claim is not None:
                    return self._receipt(claim, duplicate=True)
                if time.monotonic() > deadline:
                    raise
                continue
            break

        if outcome == "duplicate":
            return self._receipt(claim, duplicate=True)

        store = await self._storage_service.get_or_create_store(effective, None)

        if outcome == "recovered" and claim.table_id:
            # Crash recovery: the original claimant may have appended before
            # dying. The claim's token finds the orphan ON the data plane —
            # complete without re-appending (and without re-grading), never
            # duplicate. table_id on the claim names where to look; absent
            # table_id (or a recorded table that was never materialized)
            # means the crash preceded any append.
            try:
                existing = await store.get_existing_table_df(claim.table_id, wid, str(run_id))
                orphaned = existing.where(
                    existing["commit_token"]  # ty: ignore[invalid-argument-type]
                    == claim.commit_token
                )
                found = orphaned.count_rows() > 0
            except KeyError:
                found = False
            if found:
                await store.flush()
                await catalog.complete_claim(claim.scope_key, claimant, claim.table_id)
                settled = await catalog.get_claim(claim.scope_key)
                return self._receipt(settled if settled is not None else claim, duplicate=False)

        components = await build_components(claim)
        if not components:
            raise ValueError("a fact needs at least one component")
        sig = tuple(sorted({*(type(c) for c in components), FactMeta}, key=lambda t: t.__name__))
        table_id = Archetype.get_name(sig)
        await catalog.record_claim_table(claim.scope_key, table_id)

        await self._append_fact(store, sig, claim, components, wid, str(run_id))

        # Facts are discoverable like everything else.
        schema = Archetype.get_archetype_schema(sig)
        await catalog.register_signature(
            SignatureRecord(
                table_id=table_id,
                component_names=tuple(c.__name__ for c in sig),
                schema_json=json.dumps(arrow_schema_descriptor(schema)),
                fingerprint=schema_fingerprint(schema),
            )
        )

        # Visibility must never outrun durability (same rule as ticks).
        await store.flush()
        await catalog.complete_claim(claim.scope_key, claimant, table_id)
        settled = await catalog.get_claim(claim.scope_key)
        return self._receipt(settled if settled is not None else claim, duplicate=False)

    async def snapshot_ref(
        self, world_id: str, storage_config: StorageConfig | None = None
    ) -> tuple[str, int, list[str], StorageConfig]:
        """The world's pinned snapshot reference: (run_id, head tick, tokens
        at that tick, effective storage).

        Persisted receipts require a pinned subject (issue #275); a world
        with no published visibility has nothing immutable to pin — fail
        closed rather than hash a moving target.
        """
        wid = str(world_id)
        effective = self._resolve_storage(wid, storage_config)
        catalog = self._storage_service.get_control_catalog(effective)
        record = await catalog.get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {effective.uri}")
        if not record.run_id:
            raise RuntimeError(f"world {wid} has no recorded run; nothing to pin")
        # Manifests ONLY: the subject is the simulation snapshot. Fact and
        # receipt tokens are evidence ATTACHED to that snapshot — including
        # them would let every completed receipt perturb the identity of the
        # subject it was graded against.
        manifests = await catalog.list_manifests(wid, str(record.run_id))
        if not manifests:
            raise RuntimeError(
                f"world {wid} has no published visibility to pin a subject against "
                "(step it at least once before evaluating)"
            )
        head = max(m.tick for m in manifests)
        tokens = sorted(m.commit_token for m in manifests if m.tick == head)
        return str(record.run_id), head, tokens, effective

    # ── internals ────────────────────────────────────────────────────────────

    def _resolve_storage(
        self, world_id: str, storage_config: StorageConfig | None
    ) -> StorageConfig:
        if storage_config is not None:
            return storage_config
        live = self._world_service.storage_record(world_id)
        if live is not None:
            return live[0]
        return StorageConfig()

    async def _append_fact(
        self,
        store,
        sig: tuple,
        claim: ClaimRecord,
        components: list[Component],
        world_id: str,
        run_id: str,
    ) -> None:
        meta = FactMeta(
            producer=claim.producer,
            external_id=claim.external_id,
            payload_digest=claim.payload_digest,
            commit_id=claim.commit_token,
        )
        row = Archetype.to_row_dict(
            entity_id=claim.fact_entity_id,
            tick=claim.tick,
            components=[*components, meta],
            world_id=world_id,
            run_id=run_id,
        )
        row["commit_token"] = claim.commit_token
        row["writer_epoch"] = claim.fence_epoch
        schema = Archetype.get_archetype_schema(sig)
        df = daft.from_arrow(pa.Table.from_pylist([row], schema=schema))
        await store.append(sig, df)

    async def _await_settled(
        self, catalog, world_id: str, run_id: str, producer: str, external_id: str
    ) -> ClaimRecord | None:
        """Poll briefly for a racing claimant's completion.

        Returns the COMPLETE claim, or None when the lease looks abandoned
        (the caller then retries acquisition and takes the lease over).
        """
        scope = claim_scope_key(world_id, run_id, producer, external_id)
        for _ in range(100):
            claim = await catalog.get_claim(scope)
            if claim is None:
                return None
            if claim.status == "COMPLETE":
                return claim
            if claim.lease_expires_at <= time.time():
                return None
            await asyncio.sleep(0.05)
        return None

    def _receipt(self, claim: ClaimRecord, *, duplicate: bool) -> FactReceipt:
        return FactReceipt(
            world_id=claim.world_id,
            run_id=claim.run_id,
            producer=claim.producer,
            external_id=claim.external_id,
            payload_digest=claim.payload_digest,
            commit_token=claim.commit_token,
            fact_entity_id=claim.fact_entity_id,
            tick=claim.tick,
            table_id=claim.table_id or "",
            duplicate=duplicate,
        )
