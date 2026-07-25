# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real-port contracts for frozen world-operation handlers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.world import handlers, query
from archetype.world.models import (
    ComponentTypeRef,
    GetWorldInfo,
    ListWorlds,
    ListWorldSignatures,
    QueryArchetype,
    QueryComponents,
    Run,
    RunEpisode,
    RunRollout,
    Step,
)
from archetype.world.registry import WorldRegistry

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("world.tick.atomic_visibility"),
]


class _Marker(Component):
    value: int = 0


@dataclass(slots=True)
class _World:
    world_id: str
    name: str
    tick: int
    run_id: str
    lineage: list[tuple[str, str, int]]


async def test_direct_simulation_handlers_preserve_live_input_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capability = object()
    coordinates = ("outer", ("inner", 3))
    input_kwargs = {
        "capability": capability,
        "coordinates": coordinates,
    }
    operations = (
        Step(world_id="world", input_kwargs=input_kwargs),
        Run(world_id="world", input_kwargs=input_kwargs),
        RunEpisode(world_id="world", input_kwargs=input_kwargs),
        RunRollout(world_id="world", input_kwargs=input_kwargs),
    )
    marker = object()

    def capture(target: list[tuple[tuple[Any, ...], dict[str, Any]]]):
        async def execute(*args: Any, **kwargs: Any) -> object:
            target.append((args, kwargs))
            return marker

        return execute

    for operation in operations:
        calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        monkeypatch.setattr(
            handlers.simulation,
            operation.operation,
            capture(calls),
        )
        handler = getattr(handlers, operation.operation)
        if type(operation) in {Step, Run}:
            result = await handler(object(), operation)
        elif type(operation) is RunEpisode:
            result = await handler(object(), object(), operation)
        else:
            result = await handler(
                object(),
                object(),
                object(),
                object(),
                operation,
            )

        assert result is marker
        assert len(calls) == 1
        forwarded = calls[0][1]
        assert forwarded["capability"] is capability
        assert forwarded["coordinates"] is coordinates
        assert forwarded["coordinates"] == ("outer", ("inner", 3))


async def test_get_world_info_uses_registry_lock_and_reconciles_before_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = WorldRegistry()
    world = _World("world-info", "info", 3, "run-info", [])
    await registry.insert(world)
    events: list[tuple[str, str]] = []

    async def reconcile(actual_registry: object, world_id: str, actual_world: _World) -> None:
        assert actual_registry is registry
        assert actual_world is world
        events.append(("reconcile", world_id))
        actual_world.tick = 4

    monkeypatch.setattr(handlers.simulation, "reconcile_committed_work_locked", reconcile)

    info = await handlers.get_world_info(
        registry,
        GetWorldInfo(world_id=world.world_id),
    )

    assert events == [("reconcile", world.world_id)]
    assert info.tick == 4


async def test_list_worlds_awaits_registry_and_reconciles_sequentially(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = WorldRegistry()
    worlds = (
        _World("world-b", "second", 2, "run-b", []),
        _World("world-a", "first", 1, "run-a", []),
    )
    for world in worlds:
        await registry.insert(world)

    reconciled: list[str] = []

    async def reconcile(
        actual_registry: WorldRegistry,
        world_id: str,
        actual_world: _World,
    ) -> None:
        assert actual_registry is registry
        assert actual_world.world_id == world_id
        sibling_id = "world-b" if world_id == "world-a" else "world-a"
        # Recovery can call into a sibling because listing never holds both
        # world locks at once.
        async with registry.operation(sibling_id):
            reconciled.append(world_id)

    monkeypatch.setattr(handlers.simulation, "reconcile_committed_work_locked", reconcile)

    infos = await handlers.list_worlds(registry, ListWorlds())

    assert [str(info.world_id) for info in infos] == ["world-a", "world-b"]
    assert reconciled == ["world-a", "world-b"]


async def test_list_worlds_omits_closing_entries_without_poisoning_siblings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = WorldRegistry()
    available = _World("world-a", "available", 1, "run-a", [])
    closing = _World("world-b", "closing", 2, "run-b", [])
    await registry.insert(available)
    await registry.insert(closing)
    await registry.begin_close(closing.world_id)

    async def reconcile(
        actual_registry: WorldRegistry,
        world_id: str,
        actual_world: _World,
    ) -> None:
        assert actual_registry is registry
        assert actual_world is available
        assert world_id == available.world_id

    monkeypatch.setattr(handlers.simulation, "reconcile_committed_work_locked", reconcile)

    infos = await handlers.list_worlds(registry, ListWorlds())

    assert [str(info.world_id) for info in infos] == [available.world_id]


async def test_list_worlds_omits_world_that_begins_closing_during_reconcile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = WorldRegistry()
    world = _World("world-race", "racing", 1, "run-race", [])
    await registry.insert(world)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def reconcile(
        actual_registry: WorldRegistry,
        world_id: str,
        actual_world: _World,
    ) -> None:
        assert actual_registry is registry
        assert actual_world is world
        assert world_id == world.world_id
        entered.set()
        await release.wait()

    monkeypatch.setattr(handlers.simulation, "reconcile_committed_work_locked", reconcile)
    listing = asyncio.create_task(handlers.list_worlds(registry, ListWorlds()))
    await asyncio.wait_for(entered.wait(), timeout=1.0)

    lease = await registry.begin_close(world.world_id)
    release.set()

    assert await asyncio.wait_for(listing, timeout=1.0) == []
    await registry.finish_close(lease)


async def test_list_worlds_omits_same_id_replacement_created_after_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = WorldRegistry()
    original = _World("world-replaced", "original", 1, "run-original", [])
    replacement = _World("world-replaced", "replacement", 2, "run-replacement", [])
    await registry.insert(original)
    original_list_worlds = registry.list_worlds

    async def snapshot_then_replace() -> list[Any]:
        snapshot = await original_list_worlds()
        lease = await registry.begin_close(original.world_id)
        await registry.finish_close(lease)
        await registry.insert(replacement)
        return snapshot

    async def reconcile(
        _registry: WorldRegistry,
        _world_id: str,
        _world: _World,
    ) -> None:
        pytest.fail("a same-ID replacement is outside the captured snapshot")

    monkeypatch.setattr(registry, "list_worlds", snapshot_then_replace)
    monkeypatch.setattr(handlers.simulation, "reconcile_committed_work_locked", reconcile)

    assert await handlers.list_worlds(registry, ListWorlds()) == []


async def test_world_signature_handler_resolves_registered_storage_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = WorldRegistry()
    storage_config = StorageConfig(namespace="world-signatures")
    await registry.remember_storage_identity("world-signatures", storage_config)
    storage = object()
    marker = object()
    calls: list[tuple[object, StorageConfig | None]] = []

    async def list_signatures(
        actual_storage: object,
        actual_config: StorageConfig | None,
    ) -> object:
        calls.append((actual_storage, actual_config))
        return marker

    monkeypatch.setattr(query, "list_signatures", list_signatures)

    result = await handlers.list_world_signatures(
        registry,
        storage,
        ListWorldSignatures(world_id="world-signatures"),
    )

    assert result is marker
    assert calls == [(storage, storage_config)]


async def test_query_components_resolves_live_storage_and_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = WorldRegistry()
    storage_config = StorageConfig(namespace="live-query")
    lineage = [("ancestor", "ancestor-run", 5)]
    world = _World("world-live", "live", 6, "run-live", lineage)
    await registry.insert(world, storage_config=storage_config)
    storage = object()
    marker = object()
    calls: list[dict[str, Any]] = []

    async def query_components(
        actual_storage: object,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        actual_config: StorageConfig | None,
        **kwargs: Any,
    ) -> object:
        calls.append(
            {
                "storage": actual_storage,
                "components": components,
                "world_id": world_id,
                "run_id": run_id,
                "storage_config": actual_config,
                **kwargs,
            }
        )
        return marker

    monkeypatch.setattr(query, "query_components", query_components)
    operation = QueryComponents(
        components=(ComponentTypeRef.from_type(_Marker),),
        world_id=world.world_id,
        run_id=world.run_id,
    )

    result = await handlers.query_components(registry, storage, operation)

    assert result is marker
    assert calls == [
        {
            "storage": storage,
            "components": [_Marker],
            "world_id": world.world_id,
            "run_id": world.run_id,
            "storage_config": storage_config,
            "ticks": None,
            "entity_ids": None,
            "lineage": lineage,
            "visibility_tokens": None,
        }
    ]


async def test_query_archetype_recovers_cold_lineage_from_resolved_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = WorldRegistry()
    storage_config = StorageConfig(namespace="cold-query")
    await registry.remember_storage_identity("world-cold", storage_config)
    storage = object()
    marker = object()
    lineage = [("root", "root-run", 2)]
    lineage_calls: list[tuple[object, str, str, StorageConfig | None]] = []
    query_calls: list[dict[str, Any]] = []

    async def get_lineage(
        actual_storage: object,
        world_id: str,
        run_id: str,
        actual_config: StorageConfig | None,
    ) -> list[tuple[str, str, int]]:
        lineage_calls.append((actual_storage, world_id, run_id, actual_config))
        return lineage

    async def query_archetype(
        actual_storage: object,
        signature: tuple[type[Component], ...],
        world_id: str,
        run_id: str,
        actual_config: StorageConfig | None,
        **kwargs: Any,
    ) -> object:
        query_calls.append(
            {
                "storage": actual_storage,
                "signature": signature,
                "world_id": world_id,
                "run_id": run_id,
                "storage_config": actual_config,
                **kwargs,
            }
        )
        return marker

    monkeypatch.setattr(query, "get_lineage", get_lineage)
    monkeypatch.setattr(query, "query_archetype", query_archetype)
    operation = QueryArchetype(
        signature=(ComponentTypeRef.from_type(_Marker),),
        world_id="world-cold",
        run_id="run-cold",
    )

    result = await handlers.query_archetype(registry, storage, operation)

    assert result is marker
    assert lineage_calls == [(storage, "world-cold", "run-cold", storage_config)]
    assert query_calls == [
        {
            "storage": storage,
            "signature": (_Marker,),
            "world_id": "world-cold",
            "run_id": "run-cold",
            "storage_config": storage_config,
            "ticks": None,
            "entity_ids": None,
            "components": None,
            "lineage": lineage,
        }
    ]


async def test_live_but_closing_query_is_not_reclassified_as_cold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = WorldRegistry()
    storage_config = StorageConfig(namespace="closing-query")
    world = _World("world-closing", "closing", 3, "run-closing", [])
    await registry.insert(world, storage_config=storage_config)
    await registry.begin_close(world.world_id)

    async def unexpected(*args: object, **kwargs: object) -> object:
        del args, kwargs
        pytest.fail("a closing live world must not enter cold query resolution")

    monkeypatch.setattr(query, "get_lineage", unexpected)
    monkeypatch.setattr(query, "query_archetype", unexpected)
    operation = QueryArchetype(
        signature=(ComponentTypeRef.from_type(_Marker),),
        world_id=world.world_id,
        run_id=world.run_id,
    )

    with pytest.raises(RuntimeError, match="closing"):
        await handlers.query_archetype(registry, object(), operation)
