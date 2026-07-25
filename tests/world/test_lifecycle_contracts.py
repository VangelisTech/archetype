# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for managed world construction and activation."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from importlib import import_module
from typing import Any

import pytest
from uuid_utils import UUID, uuid7

from archetype.core.config import StorageConfig, WorldConfig
from archetype.storage.catalog import WorldRecord
from archetype.world.errors import WorldClosingError

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("world.tick.atomic_visibility"),
]


async def test_build_world_restores_exact_uuid7_and_bare_build_mints() -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    restored = uuid7()
    coordinator = object()

    async def materialize_commands(world: object, target_tick: int) -> int:
        del world, target_tick
        return 0

    managed = lifecycle_module.build_world(
        object(),
        WorldConfig(
            world_id="00000000-0000-7000-8000-000000000070",
            name="managed-build",
        ),
        restored_run_id=restored,
        commit_coordinator=coordinator,
        materialize_commands=materialize_commands,
    )
    bare = lifecycle_module.build_world(
        object(),
        WorldConfig(
            world_id="00000000-0000-7000-8000-000000000069",
            name="bare-build",
        ),
    )

    assert managed.run_id == restored
    assert managed.commit_coordinator is coordinator
    assert managed._materialize_commands is materialize_commands
    assert bare.run_id.version == 7
    with pytest.raises(ValueError, match="UUIDv7"):
        lifecycle_module.build_world(
            object(),
            WorldConfig(
                world_id="00000000-0000-7000-8000-000000000068",
                name="invalid-build",
            ),
            restored_run_id=UUID(int=1),
        )


@dataclass(slots=True)
class _World:
    world_id: str
    run_id: UUID
    name: str | None


class _Catalog:
    def __init__(self, events: list[tuple[Any, ...]]) -> None:
        self.events = events
        self.records: dict[str, Any] = {}

    async def register_world(self, record: Any) -> None:
        self.events.append(("register", record.world_id, record.run_id))
        self.records[record.world_id] = record

    async def acquire_fence(self, world_id: str, holder: str) -> int:
        assert holder
        self.events.append(("fence", world_id))
        return 13

    async def set_world_status(self, world_id: str, status: str) -> None:
        self.events.append(("status", world_id, status))

    async def get_world(self, world_id: str) -> Any | None:
        return self.records.get(world_id)

    async def list_worlds(self) -> list[Any]:
        return [self.records[world_id] for world_id in sorted(self.records)]


class _Storage:
    def __init__(self, catalog: _Catalog, events: list[tuple[Any, ...]]) -> None:
        self.catalog = catalog
        self.events = events
        self.store = object()
        self.coordinator = object()

    async def get_or_create_store(
        self,
        storage_config: StorageConfig,
        cache_config: object | None = None,
    ) -> object:
        del storage_config, cache_config
        self.events.append(("store",))
        return self.store

    def get_control_catalog(self, storage_config: StorageConfig) -> _Catalog:
        del storage_config
        return self.catalog

    def bind_commit_coordinator(
        self,
        storage_config: StorageConfig,
        *,
        world_id: str,
        run_id: str,
        writer_epoch: int,
    ) -> object:
        del storage_config
        self.events.append(("bind", world_id, run_id, writer_epoch))
        return self.coordinator


class _Registry:
    def __init__(self, events: list[tuple[Any, ...]]) -> None:
        self.events = events
        self.world: _World | None = None
        self.projector: object | None = None

    @asynccontextmanager
    async def activation(self, world_id: str):
        self.events.append(("activation-enter", world_id))
        try:
            yield
        finally:
            self.events.append(("activation-exit", world_id))

    async def live_world(self, world_id: str) -> _World | None:
        del world_id
        return self.world

    async def contains_name(self, name: str) -> bool:
        del name
        return False

    async def insert(
        self,
        world: _World,
        *,
        storage_config: StorageConfig,
        cache_config: object | None,
        required_projector: object | None,
        closing: bool = False,
    ) -> object | None:
        del storage_config, cache_config
        assert not closing
        self.events.append(("insert", world.world_id, str(world.run_id)))
        self.world = world
        self.projector = required_projector
        return None


async def test_create_registers_final_uuid7_before_bound_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    events: list[tuple[Any, ...]] = []
    catalog = _Catalog(events)
    storage = _Storage(catalog, events)
    registry = _Registry(events)
    materializer = object()
    projector = object()

    def projector_factory(world_id: str) -> object:
        events.append(("projector", world_id))
        return projector

    def fake_build(
        store: object,
        config: WorldConfig,
        *,
        restored_run_id: UUID | None,
        commit_coordinator: object,
        materialize_commands: object,
        system: object | None = None,
    ) -> _World:
        del system
        assert store is storage.store
        assert commit_coordinator is storage.coordinator
        assert materialize_commands is materializer
        assert restored_run_id is not None
        events.append(("build", str(config.world_id), str(restored_run_id)))
        return _World(
            world_id=str(config.world_id),
            run_id=restored_run_id,
            name=config.name,
        )

    monkeypatch.setattr(lifecycle_module, "build_world", fake_build)
    lifecycle = lifecycle_module.WorldLifecycle(
        storage,
        registry,
        materialize_commands=materializer,
        required_projector_factory=projector_factory,
    )
    world = await lifecycle.create_world(
        WorldConfig(
            world_id="00000000-0000-7000-8000-000000000071",
            name="managed",
        ),
        StorageConfig(),
    )

    assert world is registry.world
    assert world.run_id.version == 7
    assert registry.projector is projector
    world_id = str(world.world_id)
    run_id = str(world.run_id)
    assert catalog.records[world_id].writer_mode == "resumable"
    assert events == [
        ("activation-enter", world_id),
        ("store",),
        ("register", world_id, run_id),
        ("fence", world_id),
        ("bind", world_id, run_id, 13),
        ("build", world_id, run_id),
        ("projector", world_id),
        ("insert", world_id, run_id),
        ("activation-exit", world_id),
    ]


async def test_create_closing_world_is_atomically_non_public_and_exactly_cleanable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module = import_module("archetype.world.registry")
    events: list[tuple[Any, ...]] = []
    catalog = _Catalog(events)
    storage = _Storage(catalog, events)
    registry = registry_module.WorldRegistry()

    def fake_build(
        store: object,
        config: WorldConfig,
        *,
        restored_run_id: UUID | None,
        commit_coordinator: object,
        materialize_commands: object | None,
        system: object | None = None,
    ) -> _World:
        del store, commit_coordinator, materialize_commands, system
        assert restored_run_id is not None
        return _World(
            world_id=str(config.world_id),
            run_id=restored_run_id,
            name=config.name,
        )

    monkeypatch.setattr(lifecycle_module, "build_world", fake_build)
    lifecycle = lifecycle_module.WorldLifecycle(storage, registry)
    world, lease = await lifecycle.create_closing_world(
        WorldConfig(
            world_id="00000000-0000-7000-8000-000000000075",
            name="private-workflow",
        ),
        StorageConfig(),
    )

    assert await registry.begin_close(world.world_id) is lease
    with pytest.raises(WorldClosingError, match=str(world.world_id)):
        async with registry.operation(world.world_id):
            pytest.fail("closing world admitted a public operation")
    async with registry.cleanup_operation(lease) as exact_world:
        assert exact_world is world

    assert catalog.records[str(world.world_id)].status == "active"
    assert catalog.records[str(world.world_id)].writer_mode == "cleanup_only"
    await lifecycle.destroy_world(world.world_id, lease=lease)
    assert not await registry.contains(world.world_id)
    assert ("status", str(world.world_id), "destroyed") in events


async def test_create_failure_after_registration_is_not_activated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    events: list[tuple[Any, ...]] = []
    catalog = _Catalog(events)
    storage = _Storage(catalog, events)
    registry = _Registry(events)

    def fail_build(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("construction failed")

    monkeypatch.setattr(lifecycle_module, "build_world", fail_build)
    lifecycle = lifecycle_module.WorldLifecycle(storage, registry)
    world_id = "00000000-0000-7000-8000-000000000072"

    with pytest.raises(RuntimeError, match="construction failed"):
        await lifecycle.create_world(
            WorldConfig(world_id=world_id, name="failed"),
            StorageConfig(),
        )

    assert registry.world is None
    assert ("status", world_id, "destroyed") in events
    assert not any(event[0] == "insert" for event in events)


async def test_concurrent_same_name_create_never_leaves_an_orphan_active_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module = import_module("archetype.world.registry")
    first_registered = asyncio.Event()
    release_first = asyncio.Event()

    class _RacingCatalog(_Catalog):
        async def register_world(self, record: Any) -> None:
            self.events.append(("register", record.world_id, record.name))
            self.records[record.world_id] = record
            if len(self.records) == 1:
                first_registered.set()
                await release_first.wait()

        async def set_world_status(self, world_id: str, status: str) -> None:
            del world_id, status
            raise RuntimeError("cleanup authority unavailable")

    def fake_build(
        store: object,
        config: WorldConfig,
        *,
        restored_run_id: UUID | None,
        commit_coordinator: object,
        materialize_commands: object | None,
        system: object | None = None,
    ) -> _World:
        del store, commit_coordinator, materialize_commands, system
        assert restored_run_id is not None
        return _World(
            world_id=str(config.world_id),
            run_id=restored_run_id,
            name=config.name,
        )

    monkeypatch.setattr(lifecycle_module, "build_world", fake_build)
    events: list[tuple[Any, ...]] = []
    catalog = _RacingCatalog(events)
    lifecycle = lifecycle_module.WorldLifecycle(
        _Storage(catalog, events),
        registry_module.WorldRegistry(),
    )
    configs = (
        WorldConfig(world_id=str(uuid7()), name="shared-name"),
        WorldConfig(world_id=str(uuid7()), name="shared-name"),
    )

    first = asyncio.create_task(lifecycle.create_world(configs[0], StorageConfig()))
    await first_registered.wait()
    second = asyncio.create_task(lifecycle.create_world(configs[1], StorageConfig()))
    for _ in range(10):
        await asyncio.sleep(0)
    release_first.set()
    results = await asyncio.gather(first, second, return_exceptions=True)

    assert sum(isinstance(result, _World) for result in results) == 1
    assert sum(isinstance(result, ValueError) for result in results) == 1
    assert len(catalog.records) == 1
    assert all(record.status == "active" for record in catalog.records.values())


async def test_fork_mints_identity_and_wires_fresh_projector_binding() -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module = import_module("archetype.world.registry")
    events: list[tuple[Any, ...]] = []
    catalog = _Catalog(events)
    storage = _Storage(catalog, events)
    projectors: list[object] = []

    async def materialize_commands(world: object, target_tick: int) -> int:
        del world, target_tick
        return 0

    def projector_factory(world_id: str) -> object:
        del world_id
        projector = object()
        projectors.append(projector)
        return projector

    registry = registry_module.WorldRegistry()
    lifecycle = lifecycle_module.WorldLifecycle(
        storage,
        registry,
        materialize_commands=materialize_commands,
        required_projector_factory=projector_factory,
    )
    source = await lifecycle.create_world(
        WorldConfig(
            world_id="00000000-0000-7000-8000-000000000073",
            name="source",
        ),
        StorageConfig(),
    )
    fork = await lifecycle.fork_world(source.world_id, name="fork")

    assert source.run_id.version == 7
    assert fork.run_id.version == 7
    assert fork.run_id != source.run_id
    assert fork.resources is source.resources
    assert fork._materialize_commands is materialize_commands
    assert len(projectors) == 2
    assert projectors[0] is not projectors[1]
    assert catalog.records[str(source.world_id)].writer_mode == "resumable"
    assert catalog.records[str(fork.world_id)].writer_mode == "resumable"
    assert registry.required_projector(str(source.world_id)) is projectors[0]
    assert registry.required_projector(str(fork.world_id)) is projectors[1]


async def test_discovery_and_readonly_open_retain_storage_identity() -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module = import_module("archetype.world.registry")
    events: list[tuple[Any, ...]] = []
    run_id = uuid7()
    record = WorldRecord(
        world_id="00000000-0000-7000-8000-000000000074",
        name="cold",
        run_id=str(run_id),
        parent_world_id=None,
        status="active",
        tick_head=12,
    )
    catalog = _Catalog(events)
    catalog.records[record.world_id] = record
    storage = _Storage(catalog, events)
    registry = registry_module.WorldRegistry()
    lifecycle = lifecycle_module.WorldLifecycle(storage, registry)
    storage_config = StorageConfig(namespace="durable-discovery")

    discovered = await lifecycle.discover_worlds(storage_config)
    readonly = await lifecycle.open_world_readonly(storage_config, record.world_id)

    assert [str(info.world_id) for info in discovered] == [record.world_id]
    assert readonly.name == "cold"
    assert readonly.tick == 12
    assert str(readonly.run_id) == str(run_id)
    assert await registry.storage_record(record.world_id) == (storage_config, None)
