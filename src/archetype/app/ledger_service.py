# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable ledger discovery over the storage control catalog.

A1 deliberately creates only generation-zero ledgers.  Transactional world
commits, batch visibility, and writer fencing activate in A2; this service does
not register or mutate live worlds.
"""

from __future__ import annotations

import json
import time
from typing import Any

import uuid_utils as uuid
from pydantic import ValidationError
from uuid_utils import UUID

from archetype.app.storage_service import StorageService
from archetype.core.config import StorageConfig
from archetype.ledger.canonical import canonical_json, internal_digest
from archetype.ledger.errors import (
    DurableRecordConflictError,
    LedgerNotFoundError,
    ManifestConflictError,
    ManifestCorruptionError,
)
from archetype.ledger.models import (
    LedgerIdentity,
    LedgerInfo,
    LedgerManifest,
    LedgerRef,
    StorageRef,
)
from archetype.ledger.records import DurableRecord, iAsyncAtomicRecordStore

_MANIFEST_KIND = "ledger-manifest"


class LedgerService:
    """Stateless façade for immutable manifests and recoverable ledger heads."""

    def __init__(self, storage_service: StorageService) -> None:
        self._storage_service = storage_service

    @staticmethod
    def _identity_key(identity: LedgerIdentity) -> str:
        return internal_digest(
            "archetype-ledger-identity-v1",
            identity.model_dump(mode="json", exclude_none=False),
        )

    @staticmethod
    def _manifest_payload(manifest: LedgerManifest) -> dict[str, Any]:
        # committed_at_ms is diagnostic.  Keeping it in the SQLite record
        # column (rather than its semantic payload) makes concurrent identical
        # creation requests true replays while preserving the winner's time.
        return manifest.model_dump(
            mode="json",
            exclude_none=False,
            exclude={"committed_at_ms"},
        )

    @staticmethod
    def _manifest_from_record(record: DurableRecord) -> LedgerManifest:
        try:
            payload = json.loads(record.payload_json)
            return LedgerManifest.model_validate_json(
                canonical_json({**payload, "committed_at_ms": record.committed_at_ms})
            )
        except (ValidationError, ValueError, TypeError) as exc:
            raise ManifestCorruptionError(
                f"invalid ledger manifest record {record.key!r} revision {record.revision}"
            ) from exc

    @staticmethod
    def _ref(manifest: LedgerManifest) -> LedgerRef:
        return LedgerRef(
            identity=manifest.identity,
            manifest_digest=manifest.manifest_digest,
            manifest_generation=manifest.generation,
            committed_through_tick=manifest.committed_through_tick,
            next_tick=manifest.next_tick,
        )

    @classmethod
    def _info(cls, manifest: LedgerManifest) -> LedgerInfo:
        return LedgerInfo(
            ref=cls._ref(manifest),
            name=manifest.name,
            next_entity_id=manifest.next_entity_id,
            signatures=manifest.signatures,
            lineage=manifest.lineage,
        )

    async def _validate_manifest_chain(
        self,
        catalog: iAsyncAtomicRecordStore,
        selected_record: DurableRecord,
    ) -> LedgerManifest:
        """Validate the complete record and manifest predecessor chain."""
        selected_manifest: LedgerManifest | None = None
        current_record = selected_record
        while True:
            current_manifest = self._manifest_from_record(current_record)
            if selected_manifest is None:
                selected_manifest = current_manifest
            identity = current_manifest.identity
            if current_record.kind != _MANIFEST_KIND:
                raise ManifestCorruptionError("manifest record has the wrong kind")
            if current_record.scope != identity.storage.storage_id:
                raise ManifestCorruptionError("manifest record has the wrong storage scope")
            if current_record.key != self._identity_key(identity):
                raise ManifestCorruptionError("manifest record has the wrong identity key")
            if current_record.revision != current_manifest.generation:
                raise ManifestCorruptionError("manifest record generation mismatch")
            if current_record.revision == 0:
                return selected_manifest

            try:
                previous_record = await catalog.get(
                    kind=_MANIFEST_KIND,
                    scope=current_record.scope,
                    key=current_record.key,
                    revision=current_record.revision - 1,
                )
            except ValidationError as exc:
                raise ManifestCorruptionError("manifest predecessor record is corrupt") from exc
            if previous_record is None:
                raise ManifestCorruptionError("manifest predecessor record is missing")
            if current_record.previous_digest != previous_record.content_digest:
                raise ManifestCorruptionError("durable record predecessor digest mismatch")
            previous_manifest = self._manifest_from_record(previous_record)
            if previous_manifest.identity != identity:
                raise ManifestCorruptionError("manifest predecessor identity mismatch")
            if current_manifest.previous_manifest_digest != previous_manifest.manifest_digest:
                raise ManifestCorruptionError("manifest predecessor digest mismatch")
            current_record = previous_record

    async def create_ledger(
        self,
        *,
        name: str | None,
        storage_config: StorageConfig,
        world_id: str | UUID | None = None,
        run_id: str | UUID | None = None,
    ) -> LedgerRef:
        """Atomically create or replay one empty, generation-zero ledger."""
        storage = self._storage_service.storage_ref(storage_config)
        identity = LedgerIdentity(
            storage=storage,
            world_id=str(world_id) if world_id is not None else str(uuid.uuid7()),
            run_id=str(run_id) if run_id is not None else str(uuid.uuid7()),
        )
        commit_id = internal_digest(
            "archetype-ledger-genesis-commit-v1",
            {
                "identity": identity.model_dump(mode="json", exclude_none=False),
                "name": name,
            },
        )
        manifest = LedgerManifest.create(
            identity=identity,
            name=name,
            generation=0,
            previous_manifest_digest=None,
            commit_id=commit_id,
            committed_through_tick=None,
            next_tick=0,
            next_entity_id=1,
            signatures=(),
            entity_directory=(),
            lineage=(),
            batches=(),
            writer_epoch=0,
            execution_contract_digest=None,
            committed_at_ms=int(time.time() * 1_000),
        )
        record = DurableRecord.create(
            kind=_MANIFEST_KIND,
            scope=storage.storage_id,
            key=self._identity_key(identity),
            revision=0,
            payload=self._manifest_payload(manifest),
            committed_at_ms=manifest.committed_at_ms,
        )
        catalog = await self._storage_service.get_or_create_atomic_record_store(storage_config)
        try:
            result = await catalog.compare_and_swap(
                record,
                expected_revision=None,
                expected_digest=None,
            )
        except ValidationError as exc:
            raise ManifestCorruptionError("existing ledger creation record is corrupt") from exc
        except DurableRecordConflictError as exc:
            if exc.latest_record is not None:
                await self._validate_manifest_chain(catalog, exc.latest_record)
            raise ManifestConflictError(
                "ledger identity already exists with different generation-zero content"
            ) from exc
        persisted = await self._validate_manifest_chain(catalog, result.record)
        return self._ref(persisted)

    async def get_head(
        self,
        identity: LedgerIdentity,
        *,
        storage_config: StorageConfig,
    ) -> LedgerRef:
        """Recover the latest immutable ledger reference by durable identity."""
        self._storage_service.verify_storage_ref(identity.storage, storage_config)
        catalog = await self._storage_service.get_or_create_atomic_record_store(storage_config)
        try:
            record = await catalog.get_latest(
                kind=_MANIFEST_KIND,
                scope=identity.storage.storage_id,
                key=self._identity_key(identity),
            )
        except ValidationError as exc:
            raise ManifestCorruptionError("durable ledger head record is corrupt") from exc
        if record is None:
            raise LedgerNotFoundError(f"ledger {identity.world_id}/{identity.run_id} was not found")
        manifest = await self._validate_manifest_chain(catalog, record)
        if manifest.identity != identity or manifest.generation != record.revision:
            raise ManifestCorruptionError("manifest record identity or generation mismatch")
        return self._ref(manifest)

    async def list_ledgers(
        self,
        storage: StorageRef,
        *,
        storage_config: StorageConfig,
        name: str | None = None,
    ) -> list[LedgerInfo]:
        """List the latest committed head for every ledger in one store."""
        self._storage_service.verify_storage_ref(storage, storage_config)
        catalog = await self._storage_service.get_or_create_atomic_record_store(storage_config)
        rows = (
            (await catalog.scan_latest(kind=_MANIFEST_KIND, scope=storage.storage_id))
            .collect()
            .to_pylist()
        )
        latest: dict[str, DurableRecord] = {}
        for row in rows:
            try:
                record = DurableRecord.model_validate(row)
            except ValidationError as exc:
                raise ManifestCorruptionError("durable ledger catalog row is corrupt") from exc
            if record.key in latest:
                raise ManifestCorruptionError("catalog returned duplicate ledger heads")
            latest[record.key] = record

        infos: list[LedgerInfo] = []
        for key in sorted(latest):
            record = latest[key]
            manifest = await self._validate_manifest_chain(catalog, record)
            if manifest.identity.storage != storage or manifest.generation != record.revision:
                raise ManifestCorruptionError("catalog scan returned a mismatched manifest")
            if name is None or manifest.name == name:
                infos.append(self._info(manifest))
        return infos

    async def get_manifest(
        self,
        ref: LedgerRef,
        *,
        storage_config: StorageConfig,
    ) -> LedgerManifest:
        """Load and verify the exact manifest pinned by *ref*."""
        self._storage_service.verify_storage_ref(ref.identity.storage, storage_config)
        catalog = await self._storage_service.get_or_create_atomic_record_store(storage_config)
        try:
            record = await catalog.get(
                kind=_MANIFEST_KIND,
                scope=ref.identity.storage.storage_id,
                key=self._identity_key(ref.identity),
                revision=ref.manifest_generation,
            )
        except ValidationError as exc:
            raise ManifestCorruptionError("durable ledger manifest record is corrupt") from exc
        if record is None:
            raise LedgerNotFoundError(
                f"manifest generation {ref.manifest_generation} was not found"
            )
        manifest = await self._validate_manifest_chain(catalog, record)
        if manifest.identity != ref.identity or self._ref(manifest) != ref:
            raise ManifestCorruptionError("manifest content does not match the pinned ledger ref")
        return manifest

    async def describe_ledger(
        self,
        ref: LedgerRef,
        *,
        storage_config: StorageConfig,
    ) -> LedgerInfo:
        """Return durable metadata for one exact generation."""
        return self._info(await self.get_manifest(ref, storage_config=storage_config))
