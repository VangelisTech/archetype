# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the ingestion application family."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from daft import DataFrame

from archetype.core.config import StorageConfig
from archetype.ingestion.contracts import IngestionTable, TableVersion


@runtime_checkable
class iIngestionService(Protocol):
    """Register, append, and read world-scoped tables through Daft Catalog."""

    async def append(
        self,
        world_id: str,
        table: IngestionTable,
        rows: DataFrame,
        *,
        storage_config: StorageConfig | None = None,
    ) -> TableVersion: ...

    async def read(
        self,
        world_id: str,
        table: IngestionTable,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame: ...
