# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The single application port owned by the artifact family."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from daft import DataFrame

from archetype.artifacts.contracts import ArtifactRef, ArtifactSource
from archetype.core.config import StorageConfig


@runtime_checkable
class iArtifactService(Protocol):
    """Copy and index file artifacts through the common ingestion authority."""

    async def ingest(
        self,
        world_id: str,
        sources: ArtifactSource | Sequence[ArtifactSource],
        *,
        storage_config: StorageConfig | None = None,
    ) -> tuple[ArtifactRef, ...]: ...

    async def index(
        self,
        world_id: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame: ...
