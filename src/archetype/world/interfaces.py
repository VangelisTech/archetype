# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Family-owned ports for managed world authority."""

from __future__ import annotations

from collections.abc import Iterable
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from uuid_utils import UUID

from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import CommittedTickReceipt, iAsyncSystem

if TYPE_CHECKING:
    from archetype.world.registry import WorldBindingRef, WorldCleanupLease


@runtime_checkable
class iWorldRegistry(Protocol):
    """Own live-world identities and serialize exact-world operations."""

    async def insert(
        self,
        world: Any,
        *,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        required_projector: Any | None = None,
        closing: bool = False,
    ) -> WorldCleanupLease | None: ...

    def activation(
        self,
        world_id: str | UUID,
    ) -> AbstractAsyncContextManager[None]: ...

    async def remember_storage_identity(
        self,
        world_id: str | UUID,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> None: ...

    async def storage_record(
        self,
        world_id: str | UUID,
    ) -> tuple[StorageConfig, CacheConfig | None] | None: ...

    async def catalog_world_ids(self) -> list[str]: ...

    async def contains(self, world_id: str | UUID) -> bool: ...

    async def live_world(self, world_id: str | UUID) -> Any | None: ...

    async def contains_name(self, name: str) -> bool: ...

    async def world_id_for_name(self, name: str) -> str: ...

    async def list_worlds(self) -> list[Any]: ...

    async def snapshot_world_bindings(self) -> list[WorldBindingRef]: ...

    def is_public_binding(
        self,
        binding: WorldBindingRef,
        world: object,
    ) -> bool: ...

    def target_tick(self, world_id: str | UUID) -> int: ...

    def operation(
        self,
        world_id: str | UUID,
    ) -> AbstractAsyncContextManager[Any]: ...

    def operations(
        self,
        world_ids: Iterable[str | UUID],
    ) -> AbstractAsyncContextManager[dict[str, Any]]: ...

    async def begin_close(self, world_id: str | UUID) -> WorldCleanupLease: ...

    def cleanup_operation(
        self,
        lease: WorldCleanupLease,
    ) -> AbstractAsyncContextManager[Any]: ...

    async def finish_close(self, lease: WorldCleanupLease) -> None: ...

    def required_projector(self, world_id: str | UUID) -> Any | None: ...

    def retain_receipt(
        self,
        world_id: str | UUID,
        receipt: CommittedTickReceipt,
    ) -> None: ...

    def pending_receipt(
        self,
        world_id: str | UUID,
    ) -> CommittedTickReceipt | None: ...

    def acknowledge_receipt(
        self,
        world_id: str | UUID,
        *,
        consumer_name: str,
        receipt_identity: object,
    ) -> None: ...

    def validate_cleanup_lease(
        self,
        lease: WorldCleanupLease,
        *,
        world_id: str | UUID,
    ) -> None: ...


@runtime_checkable
class iWorldLifecycle(Protocol):
    """Construct, discover, resume, fork, and close managed worlds."""

    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> AsyncWorld: ...

    async def create_closing_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> tuple[AsyncWorld, WorldCleanupLease]: ...

    async def fork_world(
        self,
        source_world_id: str | UUID,
        name: str | None = None,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> AsyncWorld: ...

    async def discover_worlds(
        self,
        storage_config: StorageConfig,
    ) -> list[Any]: ...

    async def open_world_readonly(
        self,
        storage_config: StorageConfig,
        world_id: str | UUID,
    ) -> Any: ...

    async def open_world_mutable(
        self,
        storage_config: StorageConfig,
        world_id: str | UUID,
    ) -> AsyncWorld: ...

    async def destroy_world(
        self,
        world_id: str | UUID,
        *,
        lease: WorldCleanupLease | None = None,
    ) -> None: ...


@runtime_checkable
class iWorldCleanup(Protocol):
    """Non-ambient cleanup authority for one exact closing world."""

    @property
    def world_id(self) -> str: ...

    async def stage_teardown(self, components: list[Component]) -> int: ...

    async def update_retained(
        self,
        entity_id: int,
        components: list[Component],
    ) -> None: ...

    async def commit(
        self,
        run_config: RunConfig,
        **input_kwargs: Any,
    ) -> int: ...

    async def finish(self) -> None: ...


__all__ = ["iWorldCleanup", "iWorldLifecycle", "iWorldRegistry"]
