# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Live-world ownership, synchronization, and retryable cleanup."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from uuid_utils import UUID

from archetype.core.config import CacheConfig, StorageConfig
from archetype.world.errors import WorldClosingError


@dataclass(slots=True, eq=False)
class _RegistryEntry:
    """One strongly owned live world and its process-local managed state."""

    world_id: str
    name: str | None
    world: Any
    lock: asyncio.Lock
    required_projector: Any | None = None
    pending_receipt: Any | None = None
    cleanup_lease: WorldCleanupLease | None = None


class WorldBindingRef:
    """Opaque point-in-time reference to one exact live-world binding.

    The reference retains registry-entry identity rather than only the Python
    world object. Rebinding that same object under a new entry therefore
    creates a replacement that this reference cannot match.
    """

    __slots__ = ("_entry", "_registry")

    def __init__(
        self,
        registry: WorldRegistry,
        entry: _RegistryEntry,
        *,
        _key: object,
    ) -> None:
        if _key is not _BINDING_REF_KEY:
            raise TypeError("binding references are created by WorldRegistry")
        self._registry = registry
        self._entry = entry

    @property
    def world_id(self) -> str:
        """Return the durable ID captured for this exact registry entry."""

        return self._entry.world_id


class WorldCleanupLease:
    """Opaque authority to reconcile one exact world after close starts.

    Instances are created only by :meth:`WorldRegistry.begin_close` or an
    atomic closing-world insertion. The registry validates object identity,
    registry identity, and entry identity on every use, so a lease cannot
    authorize a replacement or sibling world.
    """

    __slots__ = (
        "_entry",
        "_on_destroy_complete",
        "_registry",
        "_reopenable_activity_gate",
    )

    def __init__(
        self,
        registry: WorldRegistry,
        entry: _RegistryEntry,
        *,
        _key: object,
        _reopenable_activity_gate: bool = False,
    ) -> None:
        if _key is not _LEASE_KEY:
            raise TypeError("cleanup leases are created by WorldRegistry")
        self._registry = registry
        self._entry = entry
        self._on_destroy_complete = False
        self._reopenable_activity_gate = _reopenable_activity_gate

    @property
    def on_destroy_complete(self) -> bool:
        """Whether this exact cleanup lease completed advisory destroy dispatch."""

        return self._on_destroy_complete

    def _complete_on_destroy(self) -> None:
        """Checkpoint successful advisory destroy dispatch for later retries."""

        self._on_destroy_complete = True

    @property
    def reopenable_activity_gate(self) -> bool:
        """Whether a public pre-destroy gate may still reopen this world."""

        return self._reopenable_activity_gate

    def _complete_activity_gate(self) -> None:
        """Make close permanently sticky after its public pre-destroy gate."""

        self._reopenable_activity_gate = False


_BINDING_REF_KEY = object()
_LEASE_KEY = object()


class WorldRegistry:
    """Strongly own live worlds and serialize exact-world operations.

    The registry lock protects entry, alias, storage-identity, and lock-map
    changes.  It is never held while waiting for a per-world lock or while
    caller behavior executes.
    """

    def __init__(self) -> None:
        self._registry_lock = asyncio.Lock()
        self._worlds: dict[str, _RegistryEntry] = {}
        self._names: dict[str, str] = {}
        self._world_locks: dict[str, asyncio.Lock] = {}
        self._activation_refs: dict[str, int] = {}
        self._storage_records: dict[
            str,
            tuple[StorageConfig, CacheConfig | None],
        ] = {}

    async def insert(
        self,
        world: Any,
        *,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        required_projector: Any | None = None,
        closing: bool = False,
    ) -> WorldCleanupLease | None:
        """Insert one world, optionally already under sticky cleanup authority."""

        world_id = str(world.world_id)
        name = str(world.name) if getattr(world, "name", None) else None
        async with self._registry_lock:
            existing = self._worlds.get(world_id)
            if existing is not None:
                if closing:
                    raise ValueError(
                        f"World with ID '{world_id}' already exists and cannot "
                        "be converted to closing ownership."
                    )
                if existing.world is not world:
                    raise ValueError(f"World with ID '{world_id}' already exists.")
                if (
                    required_projector is not None
                    and existing.required_projector is not required_projector
                ):
                    raise ValueError(
                        f"World with ID '{world_id}' already has a different required projector."
                    )
                self._remember_storage_identity_locked(
                    world_id,
                    storage_config,
                    cache_config,
                )
                return None

            if name is not None:
                named_world_id = self._names.get(name)
                if named_world_id is not None and named_world_id != world_id:
                    raise ValueError(f"World with name '{name}' already exists.")

            lock = self._world_locks.setdefault(world_id, asyncio.Lock())
            entry = _RegistryEntry(
                world_id=world_id,
                name=name,
                world=world,
                lock=lock,
                required_projector=required_projector,
            )
            if closing:
                entry.cleanup_lease = WorldCleanupLease(self, entry, _key=_LEASE_KEY)
            self._worlds[world_id] = entry
            if name is not None:
                self._names[name] = world_id
            try:
                self._remember_storage_identity_locked(
                    world_id,
                    storage_config,
                    cache_config,
                )
            except Exception:
                self._worlds.pop(world_id, None)
                if world_id not in self._activation_refs:
                    self._world_locks.pop(world_id, None)
                if name is not None:
                    self._names.pop(name, None)
                raise
            return entry.cleanup_lease

    @asynccontextmanager
    async def activation(self, world_id: str | UUID) -> AsyncIterator[None]:
        """Serialize first-use construction for one not-yet-live world ID."""

        key = str(world_id)
        async with self._registry_lock:
            lock = self._world_locks.setdefault(key, asyncio.Lock())
            self._activation_refs[key] = self._activation_refs.get(key, 0) + 1

        acquired = False
        try:
            await lock.acquire()
            acquired = True
            yield
        finally:
            if acquired:
                lock.release()
            async with self._registry_lock:
                remaining = self._activation_refs[key] - 1
                if remaining:
                    self._activation_refs[key] = remaining
                else:
                    self._activation_refs.pop(key)
                    if key not in self._worlds:
                        self._world_locks.pop(key, None)

    async def remember_storage_identity(
        self,
        world_id: str | UUID,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> None:
        """Retain durable coordinates independently of live-world lifetime."""

        async with self._registry_lock:
            self._remember_storage_identity_locked(
                str(world_id),
                storage_config,
                cache_config,
            )

    def _remember_storage_identity_locked(
        self,
        world_id: str,
        storage_config: StorageConfig | None,
        cache_config: CacheConfig | None,
    ) -> None:
        if storage_config is None:
            return
        existing = self._storage_records.get(world_id)
        if existing is not None and existing[0] != storage_config:
            raise RuntimeError(f"world {world_id} is already bound to a different storage identity")
        if existing is None:
            self._storage_records[world_id] = (storage_config, cache_config)

    async def storage_record(
        self,
        world_id: str | UUID,
    ) -> tuple[StorageConfig, CacheConfig | None] | None:
        """Return the durable coordinates known for ``world_id``."""

        async with self._registry_lock:
            return self._storage_records.get(str(world_id))

    async def catalog_world_ids(self) -> list[str]:
        """Return known durable world IDs in deterministic order."""

        async with self._registry_lock:
            return sorted(self._storage_records)

    async def contains(self, world_id: str | UUID) -> bool:
        """Whether an exact live world is currently strongly owned."""

        async with self._registry_lock:
            return str(world_id) in self._worlds

    async def live_world(self, world_id: str | UUID) -> Any | None:
        """Return an existing live binding for idempotent activation."""

        async with self._registry_lock:
            entry = self._worlds.get(str(world_id))
            return entry.world if entry is not None else None

    async def contains_name(self, name: str) -> bool:
        """Whether a live-world alias is currently owned."""

        async with self._registry_lock:
            return name in self._names

    async def world_id_for_name(self, name: str) -> str:
        """Resolve a live-world alias without admitting world behavior."""

        async with self._registry_lock:
            try:
                return self._names[name]
            except KeyError:
                raise KeyError(f"World with name '{name}' not found.") from None

    async def list_worlds(self) -> list[Any]:
        """Return a deterministic snapshot of strongly owned live worlds."""

        async with self._registry_lock:
            return [self._worlds[world_id].world for world_id in sorted(self._worlds)]

    async def snapshot_world_bindings(self) -> list[WorldBindingRef]:
        """Return deterministic point-in-time references to exact live bindings."""

        async with self._registry_lock:
            return [
                WorldBindingRef(
                    self,
                    self._worlds[world_id],
                    _key=_BINDING_REF_KEY,
                )
                for world_id in sorted(self._worlds)
            ]

    def is_public_binding(
        self,
        binding: WorldBindingRef,
        world: object,
    ) -> bool:
        """Linearize whether one captured binding remains live and non-closing."""

        if not isinstance(binding, WorldBindingRef) or binding._registry is not self:
            raise ValueError("binding reference does not belong to this registry")
        entry = binding._entry
        return (
            self._worlds.get(entry.world_id) is entry
            and entry.world is world
            and entry.cleanup_lease is None
        )

    def target_tick(self, world_id: str | UUID) -> int:
        """Return a point-in-time target-tick key for synchronous quota scope.

        PR-3 Policy owns counters; this exposes neither the world nor ambient
        operation authority.
        """

        entry = self._entry_unlocked(str(world_id))
        if entry.cleanup_lease is not None:
            raise WorldClosingError(entry.world_id)
        return int(entry.world.tick)

    @asynccontextmanager
    async def operation(self, world_id: str | UUID) -> AsyncIterator[Any]:
        """Admit public work for one live, non-closing world."""

        key = str(world_id)
        async with self._registry_lock:
            entry = self._entry_locked(key)
            if entry.cleanup_lease is not None:
                raise WorldClosingError(key)

        await entry.lock.acquire()
        try:
            async with self._registry_lock:
                self._validate_current_entry_locked(entry)
                if entry.cleanup_lease is not None:
                    raise WorldClosingError(key)
            yield entry.world
        finally:
            entry.lock.release()

    @asynccontextmanager
    async def operations(
        self,
        world_ids: Iterable[str | UUID],
    ) -> AsyncIterator[dict[str, Any]]:
        """Admit multiple worlds after acquiring their locks in sorted order."""

        ordered_ids = sorted({str(world_id) for world_id in world_ids})
        async with self._registry_lock:
            entries = [self._entry_locked(world_id) for world_id in ordered_ids]
            closing = [entry.world_id for entry in entries if entry.cleanup_lease is not None]
            if closing:
                raise WorldClosingError(closing[0])

        acquired: list[_RegistryEntry] = []
        try:
            for entry in entries:
                await entry.lock.acquire()
                acquired.append(entry)

            async with self._registry_lock:
                for entry in entries:
                    self._validate_current_entry_locked(entry)
                    if entry.cleanup_lease is not None:
                        raise WorldClosingError(entry.world_id)
            yield {entry.world_id: entry.world for entry in entries}
        finally:
            for entry in reversed(acquired):
                entry.lock.release()

    async def begin_close(self, world_id: str | UUID) -> WorldCleanupLease:
        """Make close sticky and return its retained exact-world authority."""

        lease, _started = await self.begin_close_attempt(world_id)
        return lease

    async def begin_close_attempt(
        self,
        world_id: str | UUID,
        *,
        reopenable_activity_gate: bool = False,
    ) -> tuple[WorldCleanupLease, bool]:
        """Make close sticky and report whether this call began the transition."""

        key = str(world_id)
        async with self._registry_lock:
            entry = self._entry_locked(key)
            started = entry.cleanup_lease is None
            if entry.cleanup_lease is None:
                entry.cleanup_lease = WorldCleanupLease(
                    self,
                    entry,
                    _key=_LEASE_KEY,
                    _reopenable_activity_gate=reopenable_activity_gate,
                )
            return entry.cleanup_lease, started

    async def abort_close(self, lease: WorldCleanupLease) -> None:
        """Reopen a newly gated world while its cleanup operation is held."""

        async with self._registry_lock:
            entry = self._validate_lease_locked(lease)
            if not lease.reopenable_activity_gate:
                raise RuntimeError("cleanup lease is not reopenable")
            if lease.on_destroy_complete:
                raise RuntimeError("cannot abort close after OnDestroy completed")
            entry.cleanup_lease = None

    @asynccontextmanager
    async def cleanup_operation(
        self,
        lease: WorldCleanupLease,
    ) -> AsyncIterator[Any]:
        """Serialize explicit cleanup for the exact world bound to ``lease``."""

        async with self._registry_lock:
            entry = self._validate_lease_locked(lease)

        await entry.lock.acquire()
        try:
            async with self._registry_lock:
                self._validate_lease_locked(lease)
            yield entry.world
        finally:
            entry.lock.release()

    async def finish_close(self, lease: WorldCleanupLease) -> None:
        """Release live ownership only after exact-world cleanup succeeds."""

        async with self._registry_lock:
            entry = self._validate_lease_locked(lease)
            if entry.pending_receipt is not None:
                raise RuntimeError(
                    f"world {entry.world_id} has a pending required-projection receipt"
                )

        await entry.lock.acquire()
        try:
            async with self._registry_lock:
                entry = self._validate_lease_locked(lease)
                if entry.pending_receipt is not None:
                    raise RuntimeError(
                        f"world {entry.world_id} has a pending required-projection receipt"
                    )
                self._worlds.pop(entry.world_id)
                self._world_locks.pop(entry.world_id)
                if entry.name is not None:
                    self._names.pop(entry.name, None)
        finally:
            entry.lock.release()

    def required_projector(self, world_id: str | UUID) -> Any | None:
        """Return the strongly retained required-projector binding, if any."""

        entry = self._worlds.get(str(world_id))
        return entry.required_projector if entry is not None else None

    async def bind_required_projector(
        self,
        world_id: str | UUID,
        projector: Any,
    ) -> None:
        """Attach one projector before the world's first managed tick.

        Some application workflows learn their durable world identity only
        during lazy activation. They may bind their already-constructed
        projector immediately afterwards, while the exact-world lock proves
        that no tick or cleanup can race the transition.
        """

        if projector is None:
            raise TypeError("required projector cannot be None")
        key = str(world_id)
        async with self._registry_lock:
            entry = self._entry_locked(key)
        await entry.lock.acquire()
        try:
            async with self._registry_lock:
                self._validate_current_entry_locked(entry)
                if entry.cleanup_lease is not None:
                    raise WorldClosingError(key)
                if entry.pending_receipt is not None:
                    raise RuntimeError(
                        f"world {key} already has a pending required-projection receipt"
                    )
                existing = entry.required_projector
                if existing is not None and existing is not projector:
                    raise ValueError(
                        f"World with ID '{key}' already has a different required projector."
                    )
                entry.required_projector = projector
        finally:
            entry.lock.release()

    def retain_receipt(self, world_id: str | UUID, receipt: Any) -> None:
        """Retain one exact manifest-bound receipt until acknowledgment."""

        entry = self._entry_unlocked(str(world_id))
        if entry.required_projector is None:
            raise RuntimeError(f"world {entry.world_id} has no required projector")
        if getattr(receipt, "visibility_token", None) is None:
            raise ValueError("managed required projection requires a manifest-bound receipt")
        if str(getattr(receipt, "world_id", "")) != entry.world_id:
            raise ValueError("receipt world identity does not match registry world")
        world_run_id = getattr(entry.world, "run_id", None)
        if world_run_id is not None and str(getattr(receipt, "run_id", "")) != str(world_run_id):
            raise ValueError("receipt run identity does not match registry world")

        pending = entry.pending_receipt
        if pending is None:
            entry.pending_receipt = receipt
            return
        if getattr(pending, "identity", None) != getattr(receipt, "identity", None):
            raise RuntimeError(
                f"world {entry.world_id} already retains a different pending receipt"
            )

    def pending_receipt(self, world_id: str | UUID) -> Any | None:
        """Return the exact retained receipt, including while closing."""

        entry = self._worlds.get(str(world_id))
        return entry.pending_receipt if entry is not None else None

    def acknowledge_receipt(
        self,
        world_id: str | UUID,
        *,
        consumer_name: str,
        receipt_identity: object,
    ) -> None:
        """Clear only the exact receipt acknowledged by its bound consumer."""

        entry = self._entry_unlocked(str(world_id))
        projector = entry.required_projector
        if projector is None:
            raise RuntimeError(f"world {entry.world_id} has no required projector")
        if str(getattr(projector, "consumer_name", "")) != consumer_name:
            raise ValueError("required-projector consumer does not match receipt acknowledgment")
        pending = entry.pending_receipt
        if pending is None:
            raise RuntimeError(f"world {entry.world_id} has no pending receipt")
        if getattr(pending, "identity", None) != receipt_identity:
            raise ValueError("receipt acknowledgment does not match the retained identity")
        entry.pending_receipt = None

    def validate_cleanup_lease(
        self,
        lease: WorldCleanupLease,
        *,
        world_id: str | UUID,
    ) -> None:
        """Fail unless ``lease`` authorizes the current exact world entry."""

        entry = self._validate_lease_unlocked(lease)
        if entry.world_id != str(world_id):
            raise ValueError(
                f"cleanup lease is bound to world {entry.world_id}, not world {world_id}"
            )

    def _entry_locked(self, world_id: str) -> _RegistryEntry:
        try:
            return self._worlds[world_id]
        except KeyError:
            raise KeyError(f"World with ID '{world_id}' not found.") from None

    def _entry_unlocked(self, world_id: str) -> _RegistryEntry:
        return self._entry_locked(world_id)

    def _validate_current_entry_locked(self, entry: _RegistryEntry) -> None:
        current = self._worlds.get(entry.world_id)
        if current is not entry or self._world_locks.get(entry.world_id) is not entry.lock:
            raise KeyError(f"World with ID '{entry.world_id}' not found.")

    def _validate_lease_locked(self, lease: WorldCleanupLease) -> _RegistryEntry:
        return self._validate_lease_unlocked(lease)

    def _validate_lease_unlocked(self, lease: WorldCleanupLease) -> _RegistryEntry:
        if not isinstance(lease, WorldCleanupLease) or lease._registry is not self:
            raise ValueError("cleanup lease does not belong to this registry")
        entry = lease._entry
        if self._worlds.get(entry.world_id) is not entry or entry.cleanup_lease is not lease:
            raise ValueError("cleanup lease no longer owns its exact world")
        return entry
