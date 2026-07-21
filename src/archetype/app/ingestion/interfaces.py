# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the ingestion application family."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from daft import DataFrame

from archetype.core.config import StorageConfig


@runtime_checkable
class iIngestionService(Protocol):
    """Envelope typed rows and select a StorageService-owned append.

    This port knows world/run identity and caller-declared logical keys. Table
    registration, schema checks, terminal Daft execution, and Iceberg retry
    remain storage responsibilities.
    """

    async def append(
        self,
        world_id: str,
        table_name: str,
        rows: DataFrame,
        *,
        key_columns: tuple[str, ...] = (),
        storage_config: StorageConfig | None = None,
    ) -> int:
        """Add the world/run envelope and append all rows or only absent keys."""
        ...

    async def read(
        self,
        world_id: str,
        table_name: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame:
        """Return a lazy table read scoped to the world's current run."""
        ...
