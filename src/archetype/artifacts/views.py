# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Lazy storage-backed reads for common and typed artifact indexes."""

from __future__ import annotations

from daft import DataFrame

from archetype.artifacts.pipeline import (
    ARTIFACT_AUDIO,
    ARTIFACT_DIFF,
    ARTIFACT_FILES,
    ARTIFACT_IMAGES,
    ARTIFACT_PDF,
    ARTIFACT_TEXT,
    ARTIFACT_VIDEO,
)
from archetype.core.config import StorageConfig
from archetype.storage.interfaces import iStorageService

_ARTIFACT_INDEXES = frozenset(
    {
        ARTIFACT_AUDIO,
        ARTIFACT_DIFF,
        ARTIFACT_FILES,
        ARTIFACT_IMAGES,
        ARTIFACT_PDF,
        ARTIFACT_TEXT,
        ARTIFACT_VIDEO,
    }
)


async def read_artifact_index(
    storage_service: iStorageService,
    world_id: str,
    table_name: str,
    *,
    storage_config: StorageConfig,
) -> DataFrame:
    """Return one lazy artifact index scoped by storage-owned world/run rows."""

    if table_name not in _ARTIFACT_INDEXES:
        raise ValueError(f"{table_name!r} is not an artifact index")
    return await storage_service.read_world_rows(
        storage_config,
        str(world_id),
        table_name,
    )


async def read_artifacts(
    storage_service: iStorageService,
    world_id: str,
    *,
    storage_config: StorageConfig,
) -> DataFrame:
    """Return the common artifact visibility root for one durable world."""

    return await read_artifact_index(
        storage_service,
        world_id,
        ARTIFACT_FILES,
        storage_config=storage_config,
    )


__all__ = ["read_artifact_index", "read_artifacts"]
