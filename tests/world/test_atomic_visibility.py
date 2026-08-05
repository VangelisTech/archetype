# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Resume fencing contracts at the world/storage boundary."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from importlib import import_module
from typing import Any

import pytest
from uuid_utils import UUID, uuid7

from archetype.core.config import StorageConfig
from archetype.storage.catalog import WorldRecord
from archetype.storage.service import PinnedVisibility

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("world.tick.atomic_visibility"),
]


@dataclass(slots=True)
class _World:
    world_id: str
    run_id: UUID
    name: str | None
    tick: int


class _Catalog:
    def __init__(
        self,
        record: WorldRecord,
        events: list[tuple[Any, ...]],
    ) -> None:
        self.record = record
        self.events = events

    async def get_world(self, world_id: str) -> WorldRecord | None:
        self.events.append(("get-record", world_id))
        return self.record if world_id == self.record.world_id else None

    async def acquire_fence(self, world_id: str, holder: str) -> int:
        assert holder
        self.events.append(("fence", world_id))
        return 29

    async def max_reserved_entity_id(self, world_id: str) -> int | None:
        self.events.append(("reserved", world_id))
        return 40


class _Storage:
    def __init__(self, catalog: _Catalog, events: list[tuple[Any, ...]]) -> None:
        self.catalog = catalog
        self.events = events
        self.store = object()
        self.coordinator = object()

    def get_control_catalog(self, storage_config: StorageConfig) -> _Catalog:
        del storage_config
        return self.catalog

    async def get_or_create_store(
        self,
        storage_config: StorageConfig,
        cache_config: object | None = None,
    ) -> object:
        del storage_config, cache_config
        self.events.append(("store",))
        return self.store

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
        del storage_config, cache_config, required_projector
        assert not closing
        self.events.append(("insert", world.world_id))
        self.world = world
        return None


async def test_mutable_resume_preflights_then_fences_then_rescans_authoritatively(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    resume_module = import_module("archetype.world.resume")
    events: list[tuple[Any, ...]] = []
    world_id = "00000000-0000-7000-8000-000000000081"
    run_id = uuid7()
    record = WorldRecord(
        world_id=world_id,
        name="resumed",
        run_id=str(run_id),
        parent_world_id=None,
        status="active",
        tick_head=3,
    )
    catalog = _Catalog(record, events)
    storage = _Storage(catalog, events)
    registry = _Registry(events)
    visibility = PinnedVisibility(
        world_id=world_id,
        run_id=str(run_id),
        head_tick=3,
        head_tokens=("manifest-3",),
        visibility_tokens=("manifest-3",),
        max_tick=None,
    )
    snapshots = [
        resume_module.ResumeSnapshot({}, 5, 4, visibility),
        resume_module.ResumeSnapshot({}, 7, 4, visibility),
    ]

    async def fake_load_lineage(
        store: object,
        *,
        world_id: str,
        run_id: str,
    ) -> None:
        del store
        events.append(("lineage", world_id, run_id))
        return None

    async def fake_reconstruct(
        storage_arg: object,
        catalog_arg: object,
        storage_config: StorageConfig,
        world_record: WorldRecord,
        *,
        run_id: str,
        lineage: list[tuple[str, str, int]],
    ) -> Any:
        del storage_config, lineage
        assert storage_arg is storage
        assert catalog_arg is catalog
        assert world_record is record
        snapshot = snapshots.pop(0)
        events.append(("snapshot", run_id, snapshot.next_entity_id))
        return snapshot

    def fake_resolve(directory: object) -> tuple[dict[int, tuple], set[str]]:
        del directory
        events.append(("resolve",))
        return {}, set()

    def fake_build(
        store: object,
        config: Any,
        *,
        restored_run_id: UUID,
        commit_coordinator: object,
        materialize_commands: object | None,
        system: object | None = None,
    ) -> _World:
        del materialize_commands, system
        assert store is storage.store
        assert commit_coordinator is storage.coordinator
        events.append(
            (
                "build",
                str(config.world_id),
                str(restored_run_id),
                config.tick,
                config.next_entity_id,
            )
        )
        return _World(
            world_id=str(config.world_id),
            run_id=restored_run_id,
            name=config.name,
            tick=config.tick,
        )

    monkeypatch.setattr(lifecycle_module, "load_lineage", fake_load_lineage)
    monkeypatch.setattr(lifecycle_module, "reconstruct_resume_snapshot", fake_reconstruct)
    monkeypatch.setattr(lifecycle_module, "resolve_live_signatures", fake_resolve)
    monkeypatch.setattr(lifecycle_module, "build_world", fake_build)

    lifecycle = lifecycle_module.WorldLifecycle(storage, registry)
    world = await lifecycle.open_world_mutable(StorageConfig(), world_id)

    assert world is registry.world
    assert world.run_id == run_id
    assert events.index(("snapshot", str(run_id), 5)) < events.index(("fence", world_id))
    assert events.index(("fence", world_id)) < events.index(("snapshot", str(run_id), 7))
    assert events.index(("snapshot", str(run_id), 7)) < events.index(("insert", world_id))
    assert (
        "build",
        world_id,
        str(run_id),
        4,
        41,
    ) in events


async def test_cold_resume_retains_exact_manifest_head_for_fresh_projector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module = import_module("archetype.world.registry")
    resume_module = import_module("archetype.world.resume")
    simulation_module = import_module("archetype.world.simulation")
    events: list[tuple[Any, ...]] = []
    world_id = "00000000-0000-7000-8000-000000000083"
    run_id = uuid7()
    record = WorldRecord(
        world_id=world_id,
        name="projected-resume",
        run_id=str(run_id),
        parent_world_id=None,
        status="active",
        tick_head=8,
    )
    catalog = _Catalog(record, events)
    storage = _Storage(catalog, events)
    visibility = PinnedVisibility(
        world_id=world_id,
        run_id=str(run_id),
        head_tick=8,
        head_tokens=("manifest-8",),
        visibility_tokens=("manifest-8",),
        max_tick=None,
    )
    snapshot = resume_module.ResumeSnapshot({}, 1, 9, visibility)

    async def fake_load_lineage(
        store: object,
        *,
        world_id: str,
        run_id: str,
    ) -> None:
        del store, world_id, run_id
        return None

    async def fake_reconstruct(*args: object, **kwargs: object) -> Any:
        del args, kwargs
        return snapshot

    def fake_resolve(directory: object) -> tuple[dict[int, tuple], set[str]]:
        del directory
        return {}, set()

    def fake_build(
        store: object,
        config: Any,
        *,
        restored_run_id: UUID,
        commit_coordinator: object,
        materialize_commands: object | None,
        system: object | None = None,
    ) -> _World:
        del store, commit_coordinator, materialize_commands, system
        return _World(
            world_id=str(config.world_id),
            run_id=restored_run_id,
            name=config.name,
            tick=config.tick,
        )

    async def project(receipt: object) -> None:
        del receipt

    projector = simulation_module.RequiredProjector(
        consumer_name="test.cold-resume",
        project=project,
    )
    monkeypatch.setattr(lifecycle_module, "load_lineage", fake_load_lineage)
    monkeypatch.setattr(lifecycle_module, "reconstruct_resume_snapshot", fake_reconstruct)
    monkeypatch.setattr(lifecycle_module, "resolve_live_signatures", fake_resolve)
    monkeypatch.setattr(lifecycle_module, "build_world", fake_build)

    registry = registry_module.WorldRegistry()
    lifecycle = lifecycle_module.WorldLifecycle(
        storage,
        registry,
        required_projector_factory=lambda _world_id: projector,
    )
    await lifecycle.open_world_mutable(StorageConfig(), world_id)

    pending = registry.pending_receipt(world_id)
    assert pending is not None
    assert pending.identity == (
        world_id,
        str(run_id),
        8,
        "manifest-8",
    )
    assert registry.required_projector(world_id) is projector


@pytest.mark.parametrize("writer_mode", ["cleanup_only", "future_mode"])
async def test_mutable_resume_rejects_every_nonresumable_writer_mode_before_fencing(
    writer_mode: str,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    events: list[tuple[Any, ...]] = []
    world_id = "00000000-0000-7000-8000-000000000084"
    record = WorldRecord(
        world_id=world_id,
        name="private-evidence",
        run_id=str(uuid7()),
        parent_world_id=None,
        status="active",
        tick_head=1,
        writer_mode=writer_mode,
    )
    lifecycle = lifecycle_module.WorldLifecycle(
        _Storage(_Catalog(record, events), events),
        _Registry(events),
    )

    with pytest.raises(RuntimeError, match=rf"{writer_mode}.*not resumable"):
        await lifecycle.open_world_mutable(StorageConfig(), world_id)

    assert not any(event[0] == "fence" for event in events)
    assert not any(event[0] == "store" for event in events)


@pytest.mark.parametrize("recorded_run_id", [None, "not-a-uuid", str(UUID(int=1))])
async def test_resume_rejects_missing_or_non_uuid7_identity_before_fencing(
    recorded_run_id: str | None,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    events: list[tuple[Any, ...]] = []
    world_id = "00000000-0000-7000-8000-000000000082"
    record = WorldRecord(
        world_id=world_id,
        name="invalid",
        run_id=recorded_run_id,
        parent_world_id=None,
        status="active",
        tick_head=0,
    )
    catalog = _Catalog(record, events)
    lifecycle = lifecycle_module.WorldLifecycle(
        _Storage(catalog, events),
        _Registry(events),
    )

    with pytest.raises((RuntimeError, ValueError), match="run|UUIDv7"):
        await lifecycle.open_world_mutable(StorageConfig(), world_id)

    assert not any(event[0] == "fence" for event in events)
