# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Publication Service (issue #274): replay-safe external artifacts.

Deliberately small and NOT MutationService: mutations stage RAM state that
persists on a later simulation step, so either crash outcome for an artifact
would be wrong (a completed claim over RAM-only rows, or publication driving
a tick through every processor). Artifacts write durable rows directly, under
the claim's own commit identity, and become visible in the same catalog
transaction that completes the claim.

Exactly-once means exactly one logically VISIBLE artifact per
(storage, world, run, producer, external_id). Physical appends may retry —
duplicates stay invisible because their tokens never enter the visible set.

Artifacts are non-processable by construction: they use catalog-allocated
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
from dataclasses import dataclass

import daft
import pyarrow as pa
from uuid_utils import uuid7

from archetype.app.storage.catalog import (
    CatalogSchemaMismatchError,
    ClaimPendingError,
    ClaimRecord,
    SignatureRecord,
    arrow_schema_descriptor,
    claim_scope_key,
    schema_fingerprint,
)
from archetype.app.storage.interfaces import iStorageService
from archetype.app.world.interfaces import iWorldService
from archetype.artifacts.components import ArtifactMeta
from archetype.artifacts.contracts import ArtifactReceipt, artifact_payload_digest
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PinnedSnapshot:
    """Immutable simulation visibility captured for one evaluation."""

    run_id: str
    tick: int
    head_tokens: tuple[str, ...]
    visibility_tokens: tuple[str, ...]
    storage_config: StorageConfig


class ArtifactService:
    """Own the artifact-publication lifecycle: PENDING → append → flush → COMPLETE."""

    def __init__(self, storage_service: iStorageService, world_service: iWorldService) -> None:
        self._storage_service = storage_service
        self._world_service = world_service

    async def publish(
        self,
        world_id: str,
        components: list[Component],
        *,
        external_id: str,
        producer: str = "default",
        storage_config: StorageConfig | None = None,
        lease_seconds: float = 30.0,
    ) -> ArtifactReceipt:
        """Publish one external artifact, exactly-once-visible.

        Works against live worlds (storage resolved from the registry) and
        cold ones (explicit ``storage_config``; the world must be recorded
        in the catalog). Concurrent identical submissions converge: one
        caller appends, the rest receive the original receipt.
        """
        if not components:
            raise ValueError("an artifact needs at least one component")

        async def _ready(_claim: ClaimRecord) -> list[Component]:
            return components

        return await self._publish(
            world_id,
            external_id=external_id,
            producer=producer,
            payload_digest=artifact_payload_digest(components),
            component_types=[type(component) for component in components],
            build_components=_ready,
            storage_config=storage_config,
            lease_seconds=lease_seconds,
        )

    async def publish_evaluation(
        self,
        world_id: str,
        *,
        evaluation_id: str,
        producer: str,
        identity_digest: str,
        component_types: list[type[Component]],
        build_components,
        storage_config: StorageConfig | None = None,
        lease_seconds: float = 30.0,
    ) -> ArtifactReceipt:
        """Claim-BEFORE-grade (issue #275): the payload is built only after
        this claimant owns the claim.

        ``identity_digest`` names what the evaluation is OF (subject +
        contract), never the graded outcome — trials of nondeterministic
        graders share it while concluding differently. ``build_components``
        (the grader, composed by the gate layer) runs at most once per
        completed claim: a duplicate returns the persisted receipt without
        re-grading, and a lease takeover that finds the orphan rows
        completes without re-running. ``component_types`` declares the
        persisted shape so takeover can restore a missing signature record
        without rebuilding the graded payload.
        """
        return await self._publish(
            world_id,
            external_id=evaluation_id,
            producer=producer,
            payload_digest=identity_digest,
            component_types=component_types,
            build_components=build_components,
            storage_config=storage_config,
            lease_seconds=lease_seconds,
        )

    async def _publish(
        self,
        world_id: str,
        *,
        external_id: str,
        producer: str,
        payload_digest: str,
        component_types: list[type[Component]],
        build_components,
        storage_config: StorageConfig | None,
        lease_seconds: float,
    ) -> ArtifactReceipt:
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
            raise RuntimeError(f"world {wid} has no recorded run; nothing to attach artifacts to")
        manifest_tick = await catalog.max_manifest_tick(wid, str(run_id))
        claim_tick = record.tick_head if manifest_tick is None else manifest_tick
        sig, table_id, signature_record = self._signature(component_types)

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
                    tick=claim_tick,
                    lease_seconds=lease_seconds,
                )
            except ClaimPendingError:
                # Another claimant is mid-flight on the identical artifact.
                # Converge on its outcome instead of erroring: exactly one
                # visible artifact either way.
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

        if outcome == "recovered":
            # First probe the expired writer's token. If its append is durable,
            # publish that orphan without rebuilding or re-appending.
            found = False
            if claim.table_id:
                if claim.table_id != table_id:
                    raise RuntimeError(
                        f"claim {claim.scope_key} records table {claim.table_id}, but the "
                        f"declared component shape resolves to {table_id}"
                    )
                try:
                    existing = await store.get_existing_table_df(claim.table_id, wid, str(run_id))
                    orphaned = existing.where(existing["commit_token"] == claim.commit_token)
                    found = orphaned.count_rows() > 0
                except KeyError:
                    pass
            if found:
                assert claim.table_id is not None
                await store.flush()
                physical = await store.get_existing_table_schema(claim.table_id)
                if not signature_record.matches(physical):
                    raise CatalogSchemaMismatchError(
                        f"recovered table {claim.table_id} does not match its declared "
                        "component schema; refusing to publish the claim"
                    )
                # The original claimant may have died after append but before
                # signature registration. Restore discovery BEFORE making its
                # token visible by completing the claim.
                await catalog.register_signature(signature_record)
                await catalog.complete_claim(wid, claim.scope_key, claimant, claim.table_id)
                settled = await catalog.get_claim(wid, claim.scope_key)
                return self._receipt(settled if settled is not None else claim, duplicate=False)

            # No orphan exists yet. Rotate before rebuilding so an expired,
            # slow writer that appends later can never share the published
            # token with this recovery attempt.
            claim = await catalog.rearm_claim(
                wid,
                claim.scope_key,
                claimant,
                f"artifact-{claim.scope_key[:16]}-{uuid7().hex}",
            )

        components = await build_components(claim)
        if not components:
            raise ValueError("an artifact needs at least one component")
        actual_types = {type(component) for component in components}
        declared_types = set(component_types)
        if actual_types != declared_types:
            raise ValueError(
                "built artifact components do not match their declared types: "
                f"declared={sorted(t.__name__ for t in declared_types)}, "
                f"actual={sorted(t.__name__ for t in actual_types)}"
            )
        await catalog.record_claim_table(wid, claim.scope_key, table_id)

        await self._append_artifact(store, sig, claim, components, wid, str(run_id))

        # Artifacts are discoverable like everything else.
        await catalog.register_signature(signature_record)

        # Visibility must never outrun durability (same rule as ticks).
        await store.flush()
        await catalog.complete_claim(wid, claim.scope_key, claimant, table_id)
        settled = await catalog.get_claim(wid, claim.scope_key)
        return self._receipt(settled if settled is not None else claim, duplicate=False)

    async def snapshot_ref(
        self, world_id: str, storage_config: StorageConfig | None = None
    ) -> PinnedSnapshot:
        """Capture the world's immutable simulation visibility.

        Persisted receipts require a pinned subject (issue #275); a world
        with no published visibility has nothing immutable to pin — fail
        closed rather than hash a moving target. ``head_tokens`` identify
        the snapshot for the subject digest; ``visibility_tokens`` pin the
        full manifest prefix that the grader may read.
        """
        wid = str(world_id)
        effective = self._resolve_storage(wid, storage_config)
        catalog = self._storage_service.get_control_catalog(effective)
        record = await catalog.get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {effective.uri}")
        if not record.run_id:
            raise RuntimeError(f"world {wid} has no recorded run; nothing to pin")
        # Manifests ONLY: the subject is the simulation snapshot. Artifact and
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
        # Keep the subject identity manifest-only, but pin every row that is
        # visible at capture time. Completed artifact claims publish their own
        # commit tokens and may share a tick with a manifest; omitting them
        # would make durable artifacts disappear from the grader's exact-token
        # read even though an ordinary query can see them.
        visible = await catalog.visible_tokens(wid, str(record.run_id))
        visibility_tokens = {
            token for tick, tokens in (visible or {}).items() if tick <= head for token in tokens
        }
        return PinnedSnapshot(
            run_id=str(record.run_id),
            tick=head,
            head_tokens=tuple(sorted(m.commit_token for m in manifests if m.tick == head)),
            visibility_tokens=tuple(sorted(visibility_tokens)),
            storage_config=effective,
        )

    # ── internals ────────────────────────────────────────────────────────────

    @staticmethod
    def _signature(
        component_types: list[type[Component]],
    ) -> tuple[tuple[type[Component], ...], str, SignatureRecord]:
        if not component_types:
            raise ValueError("an artifact needs at least one declared component type")
        if any(not isinstance(component_type, type) for component_type in component_types):
            raise TypeError("component_types must contain Component classes")
        if any(not issubclass(component_type, Component) for component_type in component_types):
            raise TypeError("component_types must contain Component classes")

        sig = tuple(
            sorted({*component_types, ArtifactMeta}, key=lambda component: component.__name__)
        )
        table_id = Archetype.get_name(sig)
        schema = Archetype.get_archetype_schema(sig)
        return (
            sig,
            table_id,
            SignatureRecord(
                table_id=table_id,
                component_names=tuple(component.__name__ for component in sig),
                schema_json=json.dumps(arrow_schema_descriptor(schema)),
                fingerprint=schema_fingerprint(schema),
            ),
        )

    def _resolve_storage(
        self, world_id: str, storage_config: StorageConfig | None
    ) -> StorageConfig:
        if storage_config is not None:
            return storage_config
        live = self._world_service.storage_record(world_id)
        if live is not None:
            return live[0]
        return StorageConfig()

    async def _append_artifact(
        self,
        store,
        sig: tuple,
        claim: ClaimRecord,
        components: list[Component],
        world_id: str,
        run_id: str,
    ) -> None:
        meta = ArtifactMeta(
            producer=claim.producer,
            external_id=claim.external_id,
            payload_digest=claim.payload_digest,
            commit_id=claim.commit_token,
        )
        row = Archetype.to_row_dict(
            entity_id=claim.artifact_entity_id,
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
            claim = await catalog.get_claim(world_id, scope)
            if claim is None:
                return None
            if claim.status == "COMPLETE":
                return claim
            if claim.lease_expires_at <= time.time():
                return None
            await asyncio.sleep(0.05)
        return None

    def _receipt(self, claim: ClaimRecord, *, duplicate: bool) -> ArtifactReceipt:
        return ArtifactReceipt(
            world_id=claim.world_id,
            run_id=claim.run_id,
            producer=claim.producer,
            external_id=claim.external_id,
            payload_digest=claim.payload_digest,
            commit_token=claim.commit_token,
            artifact_entity_id=claim.artifact_entity_id,
            tick=claim.tick,
            table_id=claim.table_id or "",
            duplicate=duplicate,
        )
