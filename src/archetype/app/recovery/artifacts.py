# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Item-scoped artifact publication recovery adapter."""

from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable

from archetype.app.artifacts.bundle_models import (
    ArtifactReconcileCandidate,
    ArtifactReconcileDisposition,
    ArtifactReconcileItemResult,
)
from archetype.app.recovery.models import (
    RecoveryItemDisposition,
    RecoveryItemResult,
    RecoveryKind,
    RecoveryPage,
    RecoverySubject,
    recovery_subject_key,
)
from archetype.core.config import StorageConfig


@runtime_checkable
class _ArtifactPublicationRecoveryPort(Protocol):
    """Only the two artifact authorities available to maintenance recovery."""

    async def list_due_publications(
        self,
        world_id: str,
        *,
        storage_config: StorageConfig,
        limit: int,
        after_publication_key: str = "",
    ) -> tuple[ArtifactReconcileCandidate, ...]: ...

    async def reconcile_publication(
        self,
        world_id: str,
        publication_key: str,
        *,
        storage_config: StorageConfig,
    ) -> ArtifactReconcileItemResult: ...


class ArtifactPublicationRecovery:
    """Discover and reconcile artifacts without any model-execution capability."""

    kind: Literal[RecoveryKind.ARTIFACT_PUBLICATION] = RecoveryKind.ARTIFACT_PUBLICATION

    def __init__(
        self,
        bundle_service: _ArtifactPublicationRecoveryPort,
        *,
        storage_config: StorageConfig,
    ) -> None:
        if not isinstance(bundle_service, _ArtifactPublicationRecoveryPort):
            raise ValueError("artifact publication recovery capability is unavailable")
        self._bundle_service = bundle_service
        self._storage_config = StorageConfig.model_validate(
            storage_config.model_dump(mode="python")
        )

    async def discover(
        self,
        world_id: str,
        cursor: str,
        *,
        limit: int,
    ) -> RecoveryPage:
        """Return one bounded due batch.

        The cursor is the last publication digest durably scheduled by this
        sweep. Lexicographic paging lets a pass move beyond a delayed or
        dead-letter subject without copying artifact authority into the fleet
        ledger.
        """
        if cursor:
            cursor = ArtifactReconcileCandidate(publication_key=cursor).publication_key
        candidates = tuple(
            ArtifactReconcileCandidate.model_validate(
                dict(candidate.__dict__)
                if isinstance(candidate, ArtifactReconcileCandidate)
                else candidate,
                from_attributes=True,
            )
            for candidate in await self._bundle_service.list_due_publications(
                world_id,
                storage_config=self._storage_config,
                limit=limit,
                after_publication_key=cursor,
            )
        )
        full_page = len(candidates) == limit
        return RecoveryPage(
            subjects=tuple(
                RecoverySubject(
                    world_id=str(world_id),
                    kind=self.kind,
                    subject_key=recovery_subject_key(
                        self.kind,
                        str(world_id),
                        candidate.publication_key,
                    ),
                    authority_key=candidate.publication_key,
                    cursor_after=candidate.publication_key,
                )
                for candidate in candidates
            ),
            next_cursor=candidates[-1].publication_key if full_page else "",
            exhausted=not full_page,
        )

    async def resolve(
        self,
        world_id: str,
        authority_key: str,
    ) -> RecoverySubject:
        """Reconstruct a crash-local exact reference; recovery rereads authority."""

        return RecoverySubject(
            world_id=str(world_id),
            kind=self.kind,
            subject_key=recovery_subject_key(self.kind, str(world_id), authority_key),
            authority_key=authority_key,
            cursor_after=authority_key,
        )

    async def recover(self, subject: RecoverySubject) -> RecoveryItemResult:
        """Reconcile only the exact publication named by ``subject``."""

        subject = RecoverySubject.model_validate(dict(subject.__dict__))
        if subject.kind is not self.kind:
            raise ValueError("artifact recovery received a subject from another lane")
        if subject.subject_key != recovery_subject_key(
            self.kind,
            subject.world_id,
            subject.authority_key,
        ):
            raise ValueError("artifact recovery subject is not bound to its authority")
        raw_result = await self._bundle_service.reconcile_publication(
            subject.world_id,
            subject.authority_key,
            storage_config=self._storage_config,
        )
        result = ArtifactReconcileItemResult.model_validate(
            dict(raw_result.__dict__)
            if isinstance(raw_result, ArtifactReconcileItemResult)
            else raw_result,
            from_attributes=True,
        )
        if result.publication_key != subject.authority_key:
            raise RuntimeError("artifact recovery result does not match its authority key")
        disposition = (
            RecoveryItemDisposition.OBSOLETE
            if result.disposition is ArtifactReconcileDisposition.OBSOLETE
            else RecoveryItemDisposition.COMPLETED
        )
        return RecoveryItemResult(
            subject_key=subject.subject_key,
            disposition=disposition,
        )
