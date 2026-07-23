# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the query family."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from daft import DataFrame

from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.interfaces import ArchetypeSignature


@runtime_checkable
class iQueryService(Protocol):
    """Read persisted component state without owning world lifecycle."""

    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type[Component]] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame: ...

    async def query_components(
        self,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
        visibility_tokens: list[str] | None = None,
    ) -> DataFrame: ...

    async def get_lineage(
        self,
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
    ) -> list[tuple[str, str, int]] | None: ...

    async def list_signatures(
        self, storage_config: StorageConfig | None = None
    ) -> list[ArchetypeSignature]: ...
