# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The first fleet-recovery vertical slice: exact artifact publications."""

import hashlib

import pytest

from archetype.app.artifacts.bundle_models import (
    ArtifactReconcileCandidate,
    ArtifactReconcileDisposition,
    ArtifactReconcileItemResult,
)
from archetype.app.recovery import (
    ArtifactPublicationRecovery,
    RecoveryItemDisposition,
    RecoveryKind,
    iMaintenanceRecoveryHandler,
    iModelRecoveryHandler,
    recovery_subject_key,
)
from archetype.core.config import StorageConfig

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("recovery.artifact.item_scoped"),
]


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class _BundleStub:
    def __init__(self) -> None:
        self.keys = tuple(sorted((_digest("one"), _digest("two"))))
        self.list_calls: list[tuple[str, StorageConfig, int, str]] = []
        self.reconcile_calls: list[tuple[str, str, StorageConfig]] = []
        self.disposition = ArtifactReconcileDisposition.INDEXED

    async def list_due_publications(
        self,
        world_id: str,
        *,
        storage_config: StorageConfig,
        limit: int,
        after_publication_key: str = "",
    ) -> tuple[ArtifactReconcileCandidate, ...]:
        self.list_calls.append((world_id, storage_config, limit, after_publication_key))
        return tuple(
            ArtifactReconcileCandidate(publication_key=key)
            for key in sorted(key for key in self.keys if key > after_publication_key)[:limit]
        )

    async def reconcile_publication(
        self,
        world_id: str,
        publication_key: str,
        *,
        storage_config: StorageConfig,
    ) -> ArtifactReconcileItemResult:
        self.reconcile_calls.append((world_id, publication_key, storage_config))
        return ArtifactReconcileItemResult(
            publication_key=publication_key,
            disposition=self.disposition,
        )


async def test_artifact_adapter_is_maintenance_only_and_digest_scoped(tmp_path) -> None:
    bundle = _BundleStub()
    storage = StorageConfig(uri=tmp_path / "worlds", namespace="fleet")
    adapter = ArtifactPublicationRecovery(bundle, storage_config=storage)

    assert isinstance(adapter, iMaintenanceRecoveryHandler)
    assert not isinstance(adapter, iModelRecoveryHandler)
    assert not hasattr(adapter, "recover_model")

    page = await adapter.discover("world-1", "", limit=1)
    assert not page.exhausted and page.next_cursor == page.subjects[-1].cursor_after
    assert len(page.subjects) == 1
    subject = page.subjects[0]
    assert subject.kind is RecoveryKind.ARTIFACT_PUBLICATION
    assert subject.authority_key == subject.cursor_after == bundle.keys[0]
    assert subject.subject_key == recovery_subject_key(
        RecoveryKind.ARTIFACT_PUBLICATION,
        "world-1",
        bundle.keys[0],
    )
    assert bundle.list_calls == [("world-1", storage, 1, "")]

    outcome = await adapter.recover(subject)
    assert outcome.disposition is RecoveryItemDisposition.COMPLETED
    assert bundle.reconcile_calls == [("world-1", bundle.keys[0], storage)]

    bundle.disposition = ArtifactReconcileDisposition.OBSOLETE
    assert (await adapter.recover(subject)).disposition is RecoveryItemDisposition.OBSOLETE


async def test_artifact_adapter_reconstructs_only_safe_exact_references(tmp_path) -> None:
    bundle = _BundleStub()
    adapter = ArtifactPublicationRecovery(
        bundle,
        storage_config=StorageConfig(uri=tmp_path / "worlds"),
    )
    authority_key = _digest("crash-local")
    subject = await adapter.resolve("world-1", authority_key)
    assert subject.authority_key == subject.cursor_after == authority_key
    assert subject.subject_key == recovery_subject_key(
        RecoveryKind.ARTIFACT_PUBLICATION,
        "world-1",
        authority_key,
    )

    with pytest.raises(ValueError, match="lowercase SHA-256"):
        await adapter.discover("world-1", "not-a-digest", limit=1)
    with pytest.raises(ValueError, match="SHA-256"):
        await adapter.resolve("world-1", "../../unsafe")

    substituted = subject.model_copy(update={"subject_key": _digest("another-subject")})
    with pytest.raises(ValueError, match="must be derived"):
        await adapter.recover(substituted)
    assert bundle.reconcile_calls == []


async def test_artifact_adapter_revalidates_structural_bundle_results(tmp_path) -> None:
    class _ConstructedResultBundle(_BundleStub):
        async def reconcile_publication(
            self,
            world_id: str,
            publication_key: str,
            *,
            storage_config: StorageConfig,
        ) -> ArtifactReconcileItemResult:
            self.reconcile_calls.append((world_id, publication_key, storage_config))
            return ArtifactReconcileItemResult.model_construct(
                publication_key=publication_key,
                disposition="invented",
            )

    bundle = _ConstructedResultBundle()
    adapter = ArtifactPublicationRecovery(
        bundle,
        storage_config=StorageConfig(uri=tmp_path / "worlds"),
    )
    subject = await adapter.resolve("world-1", _digest("publication"))

    with pytest.raises(ValueError):
        await adapter.recover(subject)
    assert len(bundle.reconcile_calls) == 1
