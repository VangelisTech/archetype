# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Managed world construction, durable lifecycle, and fenced resume."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections.abc import Callable
from typing import Any, Protocol

from uuid_utils import UUID, uuid7

from archetype.core.aio import (
    AsyncQueryManager,
    AsyncSystem,
    AsyncUpdateManager,
    AsyncWorld,
)
from archetype.core.aio.async_world import CommandMaterializer
from archetype.core.config import CacheConfig, StorageConfig, WorldConfig
from archetype.core.hooks import HookRegistry, OnDestroy
from archetype.core.interfaces import iAsyncStore, iAsyncSystem, iCommitCoordinator
from archetype.core.lineage import load_lineage, persist_lineage
from archetype.core.resources import Resources
from archetype.storage.catalog import ControlCatalog, WorldRecord, storage_fingerprint
from archetype.storage.service import PinnedVisibility
from archetype.world.interfaces import iWorldRegistry
from archetype.world.registry import WorldCleanupLease
from archetype.world.resume import (
    ResumeStorage,
    reconstruct_resume_snapshot,
    resolve_live_signatures,
)
from archetype.world.simulation import retry_required_projection

logger = logging.getLogger(__name__)

RequiredProjectorFactory = Callable[[str], Any | None]


class LifecycleStorage(ResumeStorage, Protocol):
    """Storage substrate required by managed world lifecycle."""

    def get_control_catalog(self, storage_config: StorageConfig) -> ControlCatalog: ...

    async def get_or_create_store(
        self,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iAsyncStore: ...

    def bind_commit_coordinator(
        self,
        storage_config: StorageConfig,
        *,
        world_id: str,
        run_id: str,
        writer_epoch: int,
    ) -> iCommitCoordinator: ...


def _writer_holder() -> str:
    """Return a diagnostic label; the acquired epoch remains the authority."""

    return f"{socket.gethostname()}:{os.getpid()}"


def _require_uuid7(value: UUID | str, *, field: str) -> UUID:
    try:
        parsed = value if isinstance(value, UUID) else UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a valid UUIDv7") from exc
    if parsed.version != 7:
        raise ValueError(f"{field} must be a UUIDv7")
    return parsed


def build_world(
    store: iAsyncStore,
    config: WorldConfig,
    *,
    restored_run_id: UUID | None = None,
    commit_coordinator: iCommitCoordinator | None = None,
    materialize_commands: CommandMaterializer | None = None,
    system: iAsyncSystem | None = None,
) -> AsyncWorld:
    """Construct one world with fresh runtime-owned dependencies.

    Managed lifecycle pre-mints and durably registers the run identity before
    passing it through ``restored_run_id``. Bare callers may omit it and let
    :class:`AsyncWorld` mint its own UUIDv7.
    """

    if config.world_id is None:
        raise ValueError("build_world requires an assigned world_id")
    run_id = (
        _require_uuid7(restored_run_id, field="restored_run_id")
        if restored_run_id is not None
        else None
    )
    return AsyncWorld(
        world_id=str(config.world_id),
        name=config.name,
        querier=AsyncQueryManager(store=store),
        updater=AsyncUpdateManager(store=store),
        system=system or AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
        run_id=run_id,
        tick=config.tick,
        next_entity_id=config.next_entity_id,
        entity2sig=dict(config.entity2sig) if config.entity2sig else None,
        spawn_cache=dict(config.spawn_cache) if config.spawn_cache else None,
        despawn_cache=dict(config.despawn_cache) if config.despawn_cache else None,
        lineage=list(config.lineage) if config.lineage else None,
        commit_coordinator=commit_coordinator,
        materialize_commands=materialize_commands,
    )


def _world_info_from_record(record: WorldRecord) -> Any:
    """Convert a storage record without creating a world-to-application edge."""

    from archetype.world.models import WorldInfo

    if not record.run_id:
        raise RuntimeError(f"world {record.world_id} has no recorded immutable run identity")
    return WorldInfo(
        world_id=record.world_id,
        name=record.name,
        tick=record.tick_head,
        run_id=_require_uuid7(record.run_id, field="recorded run identity"),
    )


class WorldLifecycle:
    """Own managed create, fork, discovery, resume, and exact-world close."""

    def __init__(
        self,
        storage: LifecycleStorage,
        registry: iWorldRegistry,
        *,
        materialize_commands: CommandMaterializer | None = None,
        required_projector_factory: RequiredProjectorFactory | None = None,
    ) -> None:
        self._storage = storage
        self._registry = registry
        self._materialize_commands = materialize_commands
        self._required_projector_factory = required_projector_factory

    def _required_projector(self, world_id: str) -> Any | None:
        factory = self._required_projector_factory
        return factory(world_id) if factory is not None else None

    async def _mark_failed_activation(
        self,
        catalog: ControlCatalog,
        world_id: str,
    ) -> None:
        try:
            await asyncio.shield(catalog.set_world_status(world_id, "destroyed"))
        except BaseException:
            logger.exception(
                "failed to mark incompletely activated world %s non-active",
                world_id,
            )

    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> AsyncWorld:
        """Create one managed world with immutable durable writer identity."""

        if config.world_id is None:
            config = config.model_copy(update={"world_id": uuid7()})
        world_id = str(config.world_id)
        storage_config = storage_config or StorageConfig()

        async with self._registry.activation(world_id):
            existing = await self._registry.live_world(world_id)
            if existing is not None:
                return existing
            if config.name and await self._registry.contains_name(config.name):
                raise ValueError(f"World with name '{config.name}' already exists.")

            store = await self._storage.get_or_create_store(
                storage_config,
                cache_config,
            )
            catalog = self._storage.get_control_catalog(storage_config)
            run_id = uuid7()
            registered = False
            try:
                await catalog.register_world(
                    WorldRecord(
                        world_id=world_id,
                        name=config.name,
                        run_id=str(run_id),
                        parent_world_id=None,
                        status="active",
                        tick_head=config.tick,
                    )
                )
                registered = True
                epoch = await catalog.acquire_fence(world_id, _writer_holder())
                coordinator = self._storage.bind_commit_coordinator(
                    storage_config,
                    world_id=world_id,
                    run_id=str(run_id),
                    writer_epoch=epoch,
                )
                world = build_world(
                    store,
                    config,
                    restored_run_id=run_id,
                    commit_coordinator=coordinator,
                    materialize_commands=self._materialize_commands,
                    system=system,
                )
                projector = self._required_projector(world_id)
                await self._registry.insert(
                    world,
                    storage_config=storage_config,
                    cache_config=cache_config,
                    required_projector=projector,
                )
            except BaseException:
                if registered:
                    await self._mark_failed_activation(catalog, world_id)
                raise
            return world

    async def fork_world(
        self,
        source_world_id: str | UUID,
        name: str | None = None,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> AsyncWorld:
        """Fork one locked source snapshot with fresh managed bindings."""

        fork_world_id = str(uuid7())
        async with self._registry.operation(str(source_world_id)) as source:
            if not isinstance(source, AsyncWorld):
                raise TypeError("Can only fork AsyncWorld instances")
            if name and await self._registry.contains_name(name):
                raise ValueError(f"World with name '{name}' already exists.")

            source_storage = await self._registry.storage_record(str(source_world_id))
            source_storage_config = (
                source_storage[0] if source_storage is not None else StorageConfig()
            )
            if storage_config is None:
                if source_storage is not None:
                    storage_config, source_cache = source_storage
                    if cache_config is None:
                        cache_config = source_cache
                else:
                    storage_config = StorageConfig()

            retains_lineage = storage_fingerprint(storage_config) == storage_fingerprint(
                source_storage_config
            )
            spawn_cache = {signature: list(rows) for signature, rows in source.spawn_cache.items()}
            despawn_cache = {
                signature: list(entity_ids)
                for signature, entity_ids in source.despawn_cache.items()
            }
            pending_entity_ids = {
                int(row["entity_id"]) for rows in spawn_cache.values() for row in rows
            }
            entity2sig = {
                entity_id: signature
                for entity_id, signature in source.entity2sig.items()
                if retains_lineage or entity_id in pending_entity_ids
            }
            store = await self._storage.get_or_create_store(
                storage_config,
                cache_config,
            )
            lineage: list[tuple[str, str, int]] = []
            if retains_lineage:
                lineage = list(source.lineage)
                if source.tick > 0:
                    lineage.append(
                        (
                            str(source.world_id),
                            str(source.run_id),
                            source.tick - 1,
                        )
                    )
            elif source.tick > 0:
                logger.warning(
                    "fork_world: destination storage differs from source's; "
                    "the fork will not see the source's persisted history "
                    "(world %s, tick %d)",
                    source_world_id,
                    source.tick,
                )
            run_id = uuid7()
            fork_config = WorldConfig(
                world_id=fork_world_id,
                name=name,
                tick=source.tick,
                next_entity_id=source.next_entity_id,
                entity2sig=entity2sig,
                spawn_cache=spawn_cache,
                despawn_cache=despawn_cache,
                lineage=lineage,
            )
            catalog = self._storage.get_control_catalog(storage_config)
            registered = False
            try:
                await catalog.register_world(
                    WorldRecord(
                        world_id=fork_world_id,
                        name=name,
                        run_id=str(run_id),
                        parent_world_id=str(source_world_id),
                        status="active",
                        tick_head=source.tick,
                    )
                )
                registered = True
                epoch = await catalog.acquire_fence(
                    fork_world_id,
                    _writer_holder(),
                )
                coordinator = self._storage.bind_commit_coordinator(
                    storage_config,
                    world_id=fork_world_id,
                    run_id=str(run_id),
                    writer_epoch=epoch,
                )
                fork = build_world(
                    store,
                    fork_config,
                    restored_run_id=run_id,
                    commit_coordinator=coordinator,
                    materialize_commands=self._materialize_commands,
                )
                fork.resources = source.resources
                fork.system.processors = list(source.system.processors)
                for event_type, _handle, fn, mode in source.hooks.items():
                    fork.hooks.add(event_type, fn, mode=mode)

                await persist_lineage(
                    store,
                    world_id=fork_world_id,
                    run_id=str(run_id),
                    tick=fork.tick,
                    lineage=lineage,
                )
                await self._registry.insert(
                    fork,
                    storage_config=storage_config,
                    cache_config=cache_config,
                    required_projector=self._required_projector(fork_world_id),
                )
            except BaseException:
                if registered:
                    await self._mark_failed_activation(catalog, fork_world_id)
                raise
            return fork

    async def discover_worlds(self, storage_config: StorageConfig) -> list[Any]:
        """List durable worlds and retain their physical storage coordinates."""

        catalog = self._storage.get_control_catalog(storage_config)
        records = await catalog.list_worlds()
        for record in records:
            await self._registry.remember_storage_identity(
                record.world_id,
                storage_config,
            )
        return [_world_info_from_record(record) for record in records]

    async def open_world_readonly(
        self,
        storage_config: StorageConfig,
        world_id: str | UUID,
    ) -> Any:
        """Cold-open one durable descriptor without acquiring a writer fence."""

        key = str(world_id)
        catalog = self._storage.get_control_catalog(storage_config)
        record = await catalog.get_world(key)
        if record is None:
            raise KeyError(f"world {key} is not recorded in catalog for {storage_config.uri}")
        await self._registry.remember_storage_identity(key, storage_config)
        return _world_info_from_record(record)

    async def open_world_mutable(
        self,
        storage_config: StorageConfig,
        world_id: str | UUID,
    ) -> AsyncWorld:
        """Preflight, fence, then authoritatively reconstruct a cold world."""

        key = str(world_id)
        async with self._registry.activation(key):
            if await self._registry.live_world(key) is not None:
                raise RuntimeError(
                    f"world {key} is already live in this process; resume is for "
                    "cold worlds (re-fencing it would stale its own writer)"
                )

            catalog = self._storage.get_control_catalog(storage_config)
            record = await catalog.get_world(key)
            if record is None:
                raise KeyError(f"world {key} is not recorded in catalog for {storage_config.uri}")
            if record.status == "destroyed":
                raise RuntimeError(
                    f"world {key} is destroyed: queryable through read paths, not resumable"
                )
            if record.name and await self._registry.contains_name(record.name):
                raise RuntimeError(
                    f"a live world already holds the name {record.name!r}; cannot resume {key}"
                )
            if not record.run_id:
                raise RuntimeError(f"world {key} has no recorded run; nothing to resume")
            run_id = _require_uuid7(record.run_id, field="recorded run identity")

            store = await self._storage.get_or_create_store(storage_config, None)
            lineage = await load_lineage(
                store,
                world_id=key,
                run_id=str(run_id),
            )
            if record.parent_world_id and not lineage:
                raise RuntimeError(
                    f"world {key} records parent {record.parent_world_id} but has "
                    "no persisted lineage rows; refusing to resume (corruption)"
                )
            durable_lineage = lineage or []

            preflight = await reconstruct_resume_snapshot(
                self._storage,
                catalog,
                storage_config,
                record,
                run_id=str(run_id),
                lineage=durable_lineage,
            )
            resolve_live_signatures(preflight.directory)

            epoch = await catalog.acquire_fence(key, _writer_holder())
            try:
                authoritative = await reconstruct_resume_snapshot(
                    self._storage,
                    catalog,
                    storage_config,
                    record,
                    run_id=str(run_id),
                    lineage=durable_lineage,
                )
                entity2sig, _live_tables = resolve_live_signatures(authoritative.directory)
                reserved_entity_id = await catalog.max_reserved_entity_id(key)
                next_entity_id = max(
                    authoritative.next_entity_id,
                    (reserved_entity_id + 1 if reserved_entity_id is not None else 1),
                )
                coordinator = self._storage.bind_commit_coordinator(
                    storage_config,
                    world_id=key,
                    run_id=str(run_id),
                    writer_epoch=epoch,
                )
                world = build_world(
                    store,
                    WorldConfig(
                        world_id=key,
                        name=record.name,
                        tick=authoritative.resume_tick,
                        next_entity_id=next_entity_id,
                        entity2sig=entity2sig,
                        lineage=durable_lineage,
                    ),
                    restored_run_id=run_id,
                    commit_coordinator=coordinator,
                    materialize_commands=self._materialize_commands,
                )
                projector = self._required_projector(key)
                pending_receipt = self._head_receipt(
                    key,
                    run_id,
                    authoritative.visibility,
                    required=projector is not None,
                )
                await self._registry.insert(
                    world,
                    storage_config=storage_config,
                    cache_config=None,
                    required_projector=projector,
                )
                if pending_receipt is not None:
                    self._registry.retain_receipt(key, pending_receipt)
            except BaseException:
                logger.exception(
                    "resume for world %s acquired fence epoch %d but failed "
                    "before installing its replacement writer",
                    key,
                    epoch,
                )
                raise
            return world

    @staticmethod
    def _head_receipt(
        world_id: str,
        run_id: UUID,
        visibility: PinnedVisibility,
        *,
        required: bool,
    ) -> Any | None:
        if not required or visibility.head_tick is None:
            return None
        if len(visibility.head_tokens) != 1:
            raise RuntimeError(
                f"world {world_id} has an ambiguous manifest head at tick {visibility.head_tick}"
            )
        from archetype.core.interfaces import CommittedTickReceipt

        return CommittedTickReceipt(
            world_id=world_id,
            run_id=str(run_id),
            committed_tick=int(visibility.head_tick),
            visibility_token=visibility.head_tokens[0],
            commands_applied=0,
        )

    async def destroy_world(
        self,
        world_id: str | UUID,
        *,
        lease: WorldCleanupLease | None = None,
    ) -> None:
        """Close one world; failures retain exact ownership for retry."""

        key = str(world_id)
        if lease is None:
            try:
                lease = await self._registry.begin_close(key)
            except KeyError:
                return
        else:
            self._registry.validate_cleanup_lease(lease, world_id=key)

        # Close is sticky, so the retained cleanup lease is the only authority
        # that may reconcile an unacknowledged post-commit receipt.  Projection
        # must finish before durable destruction: on failure the catalog stays
        # active and the exact world, projector, receipt, and lease remain
        # strongly owned for the next public destroy retry.
        await retry_required_projection(self._registry, key, lease=lease)

        async with self._registry.cleanup_operation(lease) as world:
            if bool(getattr(world, "has_prepared_tick_commit", False)):
                raise RuntimeError(
                    f"world {key} has a prepared tick commit awaiting exact publication; "
                    "refusing durable destruction"
                )
            if isinstance(world, AsyncWorld):
                await world.hooks.fire(OnDestroy(world_id=world.world_id))
            storage_record = await self._registry.storage_record(key)
            if storage_record is not None:
                catalog = self._storage.get_control_catalog(storage_record[0])
                await catalog.set_world_status(key, "destroyed")
        await self._registry.finish_close(lease)
