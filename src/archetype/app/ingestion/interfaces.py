# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the ingestion application family."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from daft import DataFrame

from archetype.core.config import StorageConfig


@runtime_checkable
class iIngestionService(Protocol):
    """Select live storage and delegate typed world-row publication.

    This port knows the target world and caller-declared logical keys. Durable
    world/run lookup and envelope stamping, table registration, schema checks,
    terminal Daft execution, and Iceberg retry remain storage responsibilities.
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
        """Delegate a plain or logical-key-conditional append to storage."""
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
