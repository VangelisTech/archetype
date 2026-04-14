# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
World Service

Manages the lifecycle of multiple worlds. Renamed from WorldOrchestrator for v0.1.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

from uuid_utils import UUID, uuid7

from archetype.app.broker import CommandBroker
from archetype.app.factory import WorldFactory
from archetype.app.models import WorldInfo
from archetype.app.registry import WorldRegistry
from archetype.app.storage_service import StorageService
from archetype.core.aio import AsyncSystem, AsyncWorld
from archetype.core.config import CacheConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import iAsyncSystem, iWorld


def _world_entity_count(world: iWorld) -> int:
    """Return the number of tracked entities on a world.

    ``iWorld`` does not define ``entity_count`` directly, but both
    ``AsyncWorld`` and ``SyncWorld`` track entities in ``_entity2sig``.
    Falls back to ``0`` if neither attribute is present.
    """
    count = getattr(world, "entity_count", None)
    if isinstance(count, int):
        return count
    return len(getattr(world, "_entity2sig", {}))


class WorldService:
    """
    Manages the lifecycle of multiple worlds.

    Provides:
    - World creation with automatic resource sharing
    - World lookup by ID or name
    - World forking
    - Clean shutdown of all managed resources
    - Optional persistent discovery via ``WorldRegistry``
    """

    def __init__(
        self,
        storage_service: StorageService,
        broker: CommandBroker | None = None,
        registry: WorldRegistry | None = None,
    ):
        self.storage_service = storage_service
        self.factory = WorldFactory(storage_service)
        self._broker = broker
        self._registry = registry
        self._worlds: dict[UUID, iWorld] = {}
        self._world_names: dict[str, UUID] = {}

    async def shutdown(self):
        """Gracefully shuts down all managed resources."""
        await self.storage_service.shutdown()
        self._worlds.clear()
        self._world_names.clear()

    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld:
        """
        Creates or retrieves a world based on the provided configuration.
        Idempotent: if a world_id already exists, returns the existing instance.
        Injects CommandBroker into world resources if available.

        If ``storage_config`` is omitted, a default :class:`StorageConfig` is
        applied (LanceDB backend at ``./archetype_data``) so every world
        created through the service layer is durable out of the box. If the
        configured URI is a local path that cannot be created or written to,
        a :class:`PermissionError` is raised with a clear message.
        """
        if storage_config is None:
            storage_config = StorageConfig()

        _ensure_storage_uri_writable(storage_config)

        # ``WorldConfig.world_id`` is typed ``UUID | None`` and has a
        # ``default_factory=uuid7``. The factory only fires when the field is
        # omitted; callers passing ``world_id=None`` explicitly produce a
        # ``WorldConfig`` with ``world_id is None``. Resolve that here and
        # thread the concrete UUID back into the config that the factory
        # sees, so ``AsyncWorld.world_id`` (which reads from the config) is
        # never ``None`` and ``_worlds`` is never keyed by ``None``.
        world_id = config.world_id or uuid7()
        if config.world_id is None:
            config = config.model_copy(update={"world_id": world_id})

        if world_id in self._worlds:
            return self._worlds[world_id]

        # Validate name uniqueness BEFORE allocating a storage backend or
        # constructing a world. Otherwise, a duplicate-name attempt leaks a
        # half-built world into _worlds while the validity check raises.
        if config.name and config.name in self._world_names:
            raise ValueError(f"World with name '{config.name}' already exists.")

        world = await self.factory.create_world(
            world_config=config,
            storage_config=storage_config,
            cache_config=cache_config,
            system=system or AsyncSystem(),
        )

        # Inject broker into world resources for processor access
        if self._broker and isinstance(world, AsyncWorld) and hasattr(world, "resources"):
            world.resources.insert(self._broker)

        self._worlds[world.world_id] = world

        if config.name:
            self._world_names[config.name] = world.world_id

        self._persist_entry(world, storage_config)
        self._attach_registry_sync(world)

        return world

    def get_world(self, world_id: UUID) -> iWorld:
        """Retrieves a managed world instance by its ID."""
        if world_id not in self._worlds:
            raise KeyError(f"World with ID '{world_id}' not found.")
        return self._worlds[world_id]

    def get_world_by_name(self, name: str) -> iWorld:
        """Retrieves a managed world instance by its human-readable name."""
        if name not in self._world_names:
            raise KeyError(f"World with name '{name}' not found.")
        return self.get_world(self._world_names[name])

    def list_worlds(self) -> list[WorldInfo]:
        """Returns info for all managed worlds."""
        result = []
        for wid, world in self._worlds.items():
            info = WorldInfo(
                world_id=wid,
                name=getattr(world, "name", None),
                tick=getattr(world, "tick", 0),
                entity_count=_world_entity_count(world),
                archetype_signatures=[],
            )
            result.append(info)
        return result

    async def remove_world(self, world_id: UUID) -> None:
        """Removes a world from management by its ID.

        Also evicts the world's broker state (pending queue, in-flight
        command ids, and audit history) so destroying a world does not leak
        broker bookkeeping for the lifetime of the process. Without this,
        ``broker.get_pending_count()`` over-reports indefinitely and
        ``broker.get_history(world_id)`` returns orphaned commands for a
        world that no longer exists.
        """
        if world_id in self._worlds:
            for name, uid in list(self._world_names.items()):
                if uid == world_id:
                    del self._world_names[name]
            del self._worlds[world_id]
        if self._broker is not None:
            await self._broker.clear(world_id)
        if self._registry is not None:
            self._registry.delete(world_id)

    async def fork_world(
        self,
        source_world_id: UUID,
        name: str | None,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iWorld:
        """
        Fork a world: create a new world that clones the current state of the source.

        The fork always receives a system-generated ``world_id``. Callers control
        only the human-readable ``name`` and storage placement.

        The fork starts from an identical entity/component snapshot at the source's
        current tick. Source and fork then diverge independently.

        Inheritance policy (see archetype#61):
          * Copied: ``tick``, ``run_id``, ``_entity2sig``, ``_next_entity_id``,
            live archetype snapshots (re-stamped with the new ``world_id``),
            processors (shared instances), and non-broker resources.
          * Persisted: the live snapshots are written to the store under the new
            ``world_id`` at tick ``(source.tick - 1)`` so store-backed reads
            see the forked state.
          * Not copied: pending spawn/despawn caches (already reflected in
            ``_live`` once materialized), lifecycle hooks (fork-specific
            observers), and the ``CommandBroker`` reference (re-injected by
            ``create_world`` via the service's own broker).

        Raises:
            KeyError: If ``source_world_id`` is not managed.
            TypeError: If the source is not an ``AsyncWorld``.
            ValueError: If the source has pending mutations (un-materialized
                spawn/despawn caches).
        """
        from daft import lit

        from archetype.app.broker import CommandBroker
        from archetype.core.aio.async_system import AsyncSystem

        source = self.get_world(source_world_id)
        if not isinstance(source, AsyncWorld):
            raise TypeError("Can only fork AsyncWorld instances")

        # Guard: reject if the source has un-materialized mutations; callers
        # must step() first so _live reflects the intended snapshot.
        has_pending_spawns = any(v for v in source._spawn_cache.values())
        has_pending_despawns = any(v for v in source._despawn_cache.values())
        if has_pending_spawns or has_pending_despawns:
            raise ValueError(
                "Cannot fork a world with pending mutations. "
                "Call step() to materialize spawn/despawn caches first."
            )

        fork_config = WorldConfig(world_id=uuid7(), name=name)

        # Build a fresh system that shares processor instances with the source.
        # Processors are stateless DataFrame transforms, so sharing is safe.
        new_system = AsyncSystem()
        new_system.processors = list(source.system.processors)

        new_world = await self.create_world(
            config=fork_config,
            storage_config=storage_config,
            cache_config=cache_config,
            system=new_system,
        )

        if not isinstance(new_world, AsyncWorld):
            return new_world

        # --- Clone in-memory state ---
        new_world.tick = source.tick
        new_world.run_id = source.run_id
        new_world._entity2sig = dict(source._entity2sig)
        new_world._next_entity_id = source._next_entity_id

        # Re-stamp live snapshots with the new world_id so they stay consistent
        # with the fork's identity (used by prefer_live_reads).
        new_world_id_str = str(new_world.world_id)
        new_live: dict = {}
        for sig, df in source._live.items():
            new_live[sig] = df.with_column("world_id", lit(new_world_id_str))
        new_world._live = new_live

        # --- Copy non-broker resources (selective policy) ---
        # The broker is world-scoped governance; create_world already injected
        # the service's broker into new_world.resources.
        for resource_type, resource in source.resources.items():
            if resource_type is CommandBroker or isinstance(resource, CommandBroker):
                continue
            if resource_type in new_world.resources:
                continue
            new_world.resources.insert(resource)

        # --- Persist snapshot under the new world_id ---
        # After step(), source.tick is the NEXT tick to process and _live holds
        # rows stamped with (source.tick - 1). Replaying that under the new
        # world_id lets the default store-backed reads find the forked state.
        if source.tick > 0 and new_live:
            persist_tick = source.tick - 1
            persist_run_id = new_world.run_id or ""
            for sig, df in new_live.items():
                await new_world.updater.update(
                    df, sig, persist_tick, new_world.world_id, persist_run_id
                )

        return new_world

    # ------------------------------------------------------------------
    # Registry-backed discovery
    # ------------------------------------------------------------------

    async def discover_worlds(self) -> list[UUID]:
        """Rehydrate worlds listed in the registry into the in-memory cache.

        Returns the list of newly loaded world IDs. If no registry is
        configured, returns an empty list.
        """
        if self._registry is None:
            return []

        loaded: list[UUID] = []
        for entry in self._registry.list_entries():
            try:
                wid = UUID(entry["world_id"])
            except (KeyError, ValueError):
                continue
            if wid in self._worlds:
                continue

            name = entry.get("name")
            storage_config = StorageConfig(
                uri=entry.get("storage_uri", "./archetype_data"),
                namespace=entry.get("namespace", "archetypes"),
            )
            config = WorldConfig(world_id=wid, name=name)

            # Reuse create_world so broker injection, registry hooks, and
            # name tracking all go through a single code path.
            world = await self.create_world(config, storage_config)

            # Restore tick from registry so step/run continues where left off.
            if isinstance(world, AsyncWorld):
                world.tick = int(entry.get("tick", 0) or 0)

            loaded.append(wid)

        return loaded

    def _persist_entry(self, world: iWorld, storage_config: StorageConfig) -> None:
        if self._registry is None:
            return
        self._registry.upsert(
            world.world_id,
            {
                "world_id": str(world.world_id),
                "name": getattr(world, "name", None),
                "storage_uri": str(storage_config.uri),
                "namespace": storage_config.namespace,
                "tick": int(getattr(world, "tick", 0)),
            },
        )

    def _attach_registry_sync(self, world: iWorld) -> None:
        """Attach a post_tick hook that keeps the registry tick in sync.

        Captures the full registry entry in the closure so each tick only
        writes (no read-modify-write) and never drops metadata fields.
        """
        if self._registry is None or not isinstance(world, AsyncWorld):
            return
        registry = self._registry
        world_id = world.world_id
        # Snapshot the current entry so the hook never re-reads from disk.
        cached_entry = dict(registry.get(world_id) or {})
        cached_entry.setdefault("world_id", str(world_id))
        cached_entry.setdefault("name", getattr(world, "name", None))

        async def _sync_tick(world, tick, **_kw):  # noqa: ARG001 — hook signature
            cached_entry["tick"] = int(tick)
            registry.upsert(world_id, cached_entry)

        world.add_hook("post_tick", _sync_tick)


def _ensure_storage_uri_writable(storage_config: StorageConfig) -> None:
    """Validate that a local storage URI is writable before initializing a backend.

    Remote URIs (``s3://``, ``gs://``, etc.) are skipped — credentials and
    permissions for those are surfaced by the remote client at first I/O.
    For local paths, this creates the directory if needed and verifies that
    the current process can write to it. Raises :class:`PermissionError`
    with a clear message otherwise.
    """
    uri = str(storage_config.uri)
    scheme = urlparse(uri).scheme.lower()
    if scheme not in ("", "file"):
        return

    # Strip a ``file://`` prefix if present.
    path_str = uri[len("file://") :] if scheme == "file" else uri
    base_path = Path(path_str)
    if not base_path.is_absolute():
        base_path = Path.cwd() / base_path

    try:
        base_path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise PermissionError(
            f"Storage URI {base_path!s} is not writable: {exc}. "
            "Configure a writable path via StorageConfig(uri=...)."
        ) from exc

    if not os.access(base_path, os.W_OK):
        raise PermissionError(
            f"Storage URI {base_path!s} exists but is not writable by the current user. "
            "Configure a writable path via StorageConfig(uri=...)."
        )
