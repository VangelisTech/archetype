# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the artifact family."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from daft import DataFrame

from archetype.app.artifacts.bundle_models import (
    ArtifactBundleRequest,
    ArtifactPublishReceipt,
    ArtifactReconcileCandidate,
    ArtifactReconcileItemResult,
    ArtifactReconcileResult,
    PreparedArtifactBundleRequest,
)
from archetype.app.artifacts.models import ArtifactProcessor, ArtifactReceipt, ArtifactWriteReceipt
from archetype.core.component import Component
from archetype.core.config import StorageConfig


@runtime_checkable
class iArtifactService(Protocol):
    """Publish claim-backed component artifacts and pin evaluation snapshots."""

    async def publish(
        self,
        world_id: str,
        components: list[Component],
        *,
        external_id: str,
        producer: str = "default",
        storage_config: StorageConfig | None = None,
        lease_seconds: float = 30.0,
    ) -> ArtifactReceipt: ...

    async def publish_evaluation(
        self,
        world_id: str,
        *,
        evaluation_id: str,
        producer: str,
        identity_digest: str,
        component_types: list[type[Component]],
        build_components: Any,
        storage_config: StorageConfig | None = None,
        lease_seconds: float = 30.0,
    ) -> ArtifactReceipt: ...
    async def snapshot_ref(
        self, world_id: str, storage_config: StorageConfig | None = None
    ) -> Any: ...


@runtime_checkable
class iArtifactTableService(Protocol):
    """Persist typed, world/run-scoped artifact tables."""

    async def ingest_files(
        self,
        world_id: str,
        paths: str | Path | list[str | Path],
        processor: ArtifactProcessor,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactWriteReceipt: ...

    async def write_artifacts(
        self,
        world_id: str,
        table_name: str,
        artifacts: DataFrame,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactWriteReceipt: ...

    async def read_artifacts(
        self,
        world_id: str,
        table_name: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame: ...


@runtime_checkable
class iArtifactBundleService(Protocol):
    """Publish portable blobs and provider checkpoints for one episode attempt."""

    @property
    def enabled(self) -> bool: ...

    def prepare(self, request: ArtifactBundleRequest) -> PreparedArtifactBundleRequest: ...

    async def publish(
        self,
        request: ArtifactBundleRequest,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactPublishReceipt: ...

    async def publish_prepared(
        self,
        prepared: PreparedArtifactBundleRequest,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactPublishReceipt: ...

    async def query(
        self,
        world_id: str,
        run_id: str,
        *,
        attempt_id: str | None = None,
        kinds: list[str] | None = None,
    ) -> DataFrame: ...

    async def reconcile(
        self,
        world_id: str,
        *,
        storage_config: StorageConfig | None = None,
        limit: int = 100,
    ) -> ArtifactReconcileResult: ...

    async def list_due_publications(
        self,
        world_id: str,
        *,
        storage_config: StorageConfig | None = None,
        limit: int = 100,
        after_publication_key: str = "",
    ) -> tuple[ArtifactReconcileCandidate, ...]: ...

    async def reconcile_publication(
        self,
        world_id: str,
        publication_key: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactReconcileItemResult: ...
