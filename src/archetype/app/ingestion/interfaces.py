# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the ingestion application family."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from daft import DataFrame

from archetype.core.config import StorageConfig


@runtime_checkable
class iIngestionService(Protocol):
    """Register, append, and read world-scoped tables through Daft Catalog."""

    async def append(
        self,
        world_id: str,
        table_name: str,
        rows: DataFrame,
        *,
        key_columns: tuple[str, ...] = (),
        storage_config: StorageConfig | None = None,
    ) -> int: ...

    async def read(
        self,
        world_id: str,
        table_name: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame: ...
