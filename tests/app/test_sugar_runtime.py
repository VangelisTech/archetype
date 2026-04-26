# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import pytest
from daft import DataFrame, col
from uuid_utils import uuid7

from archetype import (
    ArchetypeRuntime,
    AsyncProcessor,
    Component,
    Processor,
    RuntimeWorld,
    StorageConfig,
    SyncArchetypeRuntime,
    SyncProcessor,
    SyncWorld,
    World,
    run_sync,
)
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Health(Component):
    hp: int = 100
    max_hp: int = 100


class Move(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__vx"),
                "position__y": col("position__y") + col("velocity__vy"),
            }
        )


class Delta:
    def __init__(self, value: float) -> None:
        self.value = value


class AddDelta(AsyncProcessor):
    components = (Position,)
    priority = 5

    async def process(self, df: DataFrame, resources=None, **kwargs) -> DataFrame:
        delta = resources.require(Delta)
        return df.with_columns({"position__x": col("position__x") + delta.value})


class Noop(AsyncProcessor):
    components = ()
    priority = 0

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df


class SlowPass(AsyncProcessor):
    components = (Position,)
    priority = 1

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        await asyncio.sleep(0.05)
        return df


def test_sugar_exports_are_additive():
    assert World is SyncWorld
    assert Processor is SyncProcessor
    assert ArchetypeRuntime.__module__ == "archetype.runtime.runtime"
    assert SyncArchetypeRuntime.__module__ == "archetype.runtime.runtime"
    assert RuntimeWorld.__module__ == "archetype.runtime.world"


@pytest.mark.asyncio
async def test_runtime_world_construction_is_pure(tmp_path, monkeypatch):
    async with ArchetypeRuntime() as app:
        create_calls = 0
        backend_calls = 0

        original_create_world = app._container.world_service.create_world
        original_create_store = app._container.storage_service.create_store

        async def tracked_create_world(*args, **kwargs):
            nonlocal create_calls
            create_calls += 1
            return await original_create_world(*args, **kwargs)

        async def tracked_create_store(*args, **kwargs):
            nonlocal backend_calls
            backend_calls += 1
            return await original_create_store(*args, **kwargs)

        monkeypatch.setattr(app._container.world_service, "create_world", tracked_create_world)
        monkeypatch.setattr(app._container.storage_service, "create_store", tracked_create_store)

        world = app.world(
            "pure-construction",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_pure"),
        )

        assert isinstance(world, RuntimeWorld)
        assert create_calls == 0
        assert backend_calls == 0
        assert app._container.world_service.list_worlds() == []


@pytest.mark.asyncio
async def test_runtime_world_as_actor_is_pure_before_activation(tmp_path, monkeypatch):
    async with ArchetypeRuntime() as app:
        create_calls = 0
        backend_calls = 0

        original_create_world = app._container.world_service.create_world
        original_create_store = app._container.storage_service.create_store

        async def tracked_create_world(*args, **kwargs):
            nonlocal create_calls
            create_calls += 1
            return await original_create_world(*args, **kwargs)

        async def tracked_create_store(*args, **kwargs):
            nonlocal backend_calls
            backend_calls += 1
            return await original_create_store(*args, **kwargs)

        monkeypatch.setattr(app._container.world_service, "create_world", tracked_create_world)
        monkeypatch.setattr(app._container.storage_service, "create_store", tracked_create_store)

        world = app.world(
            "alias-pure",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_alias_pure"),
        )
        viewer = world.as_actor(ActorCtx(id=uuid7(), roles={"viewer"}))

        assert isinstance(viewer, RuntimeWorld)
        assert create_calls == 0
        assert backend_calls == 0
        assert app._container.world_service.list_worlds() == []


@pytest.mark.asyncio
async def test_runtime_world_properties_raise_before_activation(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "pre-activation",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_props"),
        )

        with pytest.raises(RuntimeError, match="not been activated"):
            _ = world.world_id

        with pytest.raises(RuntimeError, match="not been activated"):
            _ = world.tick

        with pytest.raises(RuntimeError, match="not been activated"):
            _ = world.resources

        assert world.name == "pre-activation"


@pytest.mark.asyncio
async def test_runtime_world_activation_applies_staged_processors_resources_and_hooks(tmp_path):
    hook_ticks: list[int] = []

    async with ArchetypeRuntime() as app:
        world = app.world(
            "staged-init",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_staged"),
            processors=[AddDelta()],
            resources=[Delta(3.0)],
        )

        from archetype.core.hooks import PostTick

        async def on_post_tick(event: PostTick) -> None:
            hook_ticks.append(event.tick)

        world.add_hook(PostTick, on_post_tick)

        entity_id = await world.spawn(Position(x=2.0, y=0.0))
        await world.step()

        rows = (await world.query(Position, entity_ids=[entity_id])).collect().to_pylist()
        assert rows[0]["position__x"] == 5.0
        assert hook_ticks == [1]


@pytest.mark.asyncio
async def test_runtime_world_single_flight_init_under_concurrent_spawns(tmp_path, monkeypatch):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "concurrent",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_single_flight"),
        )

        create_calls = 0
        original_create_world = app._container.world_service.create_world

        async def tracked_create_world(*args, **kwargs):
            nonlocal create_calls
            create_calls += 1
            await asyncio.sleep(0.05)
            return await original_create_world(*args, **kwargs)

        monkeypatch.setattr(app._container.world_service, "create_world", tracked_create_world)

        entity_ids = await asyncio.gather(
            world.spawn(Position(x=1.0)),
            world.spawn(Position(x=2.0)),
        )

        assert create_calls == 1
        assert entity_ids == [1, 2]
        assert len(app._container.world_service.list_worlds()) == 1

        await world.step()
        rows = sorted(
            (await world.query(Position)).collect().to_pylist(), key=lambda row: row["entity_id"]
        )
        assert [row["entity_id"] for row in rows] == [1, 2]
        assert [row["position__x"] for row in rows] == [1.0, 2.0]


@pytest.mark.asyncio
async def test_runtime_world_single_flight_across_query_and_run(tmp_path, monkeypatch):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "mixed-first-use",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_mixed_first_use"),
        )

        create_calls = 0
        original_create_world = app._container.world_service.create_world

        async def tracked_create_world(*args, **kwargs):
            nonlocal create_calls
            create_calls += 1
            await asyncio.sleep(0.05)
            return await original_create_world(*args, **kwargs)

        monkeypatch.setattr(app._container.world_service, "create_world", tracked_create_world)

        df, result = await asyncio.gather(world.query(Position), world.run(steps=1))

        assert create_calls == 1
        assert df.collect().to_pylist() == []
        assert result.ticks_completed == 1
        assert len(app._container.world_service.list_worlds()) == 1


@pytest.mark.asyncio
async def test_runtime_world_actor_aliases_share_single_flight_activation_and_world_id(
    tmp_path, monkeypatch
):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "actor-aliases",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_actor_aliases"),
        )
        player = world.as_actor(ActorCtx(id=uuid7(), roles={"player"}))
        maintainer = world.as_actor(ActorCtx(id=uuid7(), roles={"maintainer"}))

        create_calls = 0
        original_create_world = app._container.world_service.create_world

        async def tracked_create_world(*args, **kwargs):
            nonlocal create_calls
            create_calls += 1
            await asyncio.sleep(0.05)
            return await original_create_world(*args, **kwargs)

        monkeypatch.setattr(app._container.world_service, "create_world", tracked_create_world)

        entity_id, df = await asyncio.gather(
            player.spawn(Position(x=1.0)), maintainer.query(Position)
        )

        assert create_calls == 1
        assert entity_id == 1
        assert df.collect().to_pylist() == []
        assert world.world_id == player.world_id == maintainer.world_id
        assert len(app._container.world_service.list_worlds()) == 1


@pytest.mark.asyncio
async def test_runtime_world_shutdown_waits_for_first_activation(tmp_path, monkeypatch):
    started = asyncio.Event()
    release = asyncio.Event()

    async with ArchetypeRuntime() as app:
        world = app.world(
            "shutdown-race",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_shutdown_race"),
        )

        original_create_world = app._container.world_service.create_world

        async def delayed_create_world(*args, **kwargs):
            started.set()
            await release.wait()
            return await original_create_world(*args, **kwargs)

        monkeypatch.setattr(app._container.world_service, "create_world", delayed_create_world)

        spawn_task = asyncio.create_task(world.spawn(Position(x=1.0)))
        await started.wait()

        shutdown_task = asyncio.create_task(world.shutdown())
        await asyncio.sleep(0)
        release.set()

        entity_id = await spawn_task
        await shutdown_task

        assert entity_id == 1
        assert app._container.world_service.list_worlds() == []
        with pytest.raises(RuntimeError, match="closed"):
            await world.query(Position)


@pytest.mark.asyncio
async def test_runtime_world_fork_waits_for_first_activation(tmp_path, monkeypatch):
    started = asyncio.Event()
    release = asyncio.Event()

    async with ArchetypeRuntime() as app:
        world = app.world(
            "fork-race",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_fork_race"),
        )

        create_calls: dict[str | None, int] = {}
        original_create_world = app._container.world_service.create_world

        async def delayed_create_world(*args, **kwargs):
            config = kwargs.get("config", args[0] if args else None)
            create_calls[config.name] = create_calls.get(config.name, 0) + 1
            started.set()
            await release.wait()
            return await original_create_world(*args, **kwargs)

        monkeypatch.setattr(app._container.world_service, "create_world", delayed_create_world)

        query_task = asyncio.create_task(world.query(Position))
        await started.wait()
        fork_task = asyncio.create_task(world.fork("branch"))
        await asyncio.sleep(0)
        release.set()

        df, fork = await asyncio.gather(query_task, fork_task)

        assert create_calls["fork-race"] == 1
        assert create_calls["branch"] == 1
        assert df.collect().to_pylist() == []
        assert fork.world_id != world.world_id


@pytest.mark.asyncio
async def test_world_shutdown_isolated_from_sibling_world(tmp_path):
    async with ArchetypeRuntime() as app:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_multi_world")
        left = app.world("left", storage=storage)
        right = app.world("right", storage=storage)

        await left.spawn(Position(x=1.0))
        await right.spawn(Position(x=9.0))
        await left.step()
        await right.step()

        await left.shutdown()

        assert len(app._container.world_service.list_worlds()) == 1

        result = await right.run(steps=1)
        assert result.ticks_completed == 1
        rows = (await right.query(Position)).collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["position__x"] == 9.0

        with pytest.raises(RuntimeError, match="closed"):
            _ = left.world_id


@pytest.mark.asyncio
async def test_world_shutdown_clears_only_its_pending_commands(tmp_path):
    async with ArchetypeRuntime() as app:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_pending")
        left = app.world("left", storage=storage)
        right = app.world("right", storage=storage)

        await left.spawn(Position(x=1.0))
        await right.spawn(Position(x=2.0))
        left_id = left.world_id
        right_id = right.world_id

        assert await app._container.broker.get_pending_count(left_id) == 1
        assert await app._container.broker.get_pending_count(right_id) == 1

        await left.shutdown()

        assert await app._container.broker.get_pending_count(left_id) == 0
        assert await app._container.broker.get_pending_count(right_id) == 1

        await right.step()
        rows = (await right.query(Position)).collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["position__x"] == 2.0


@pytest.mark.asyncio
async def test_world_shutdown_removes_only_its_registry_entry(tmp_path):
    async with ArchetypeRuntime() as app:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_registry")
        left = app.world("left", storage=storage)
        right = app.world("right", storage=storage)

        await left.query(Position)
        await right.query(Position)
        await left.shutdown()

        remaining = app._container.world_service.list_worlds()
        names = {w.name for w in remaining}
        assert names == {"right"}


@pytest.mark.asyncio
async def test_fork_survives_source_shutdown(tmp_path):
    async with ArchetypeRuntime() as app:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_fork")
        source = app.world("source", storage=storage)

        entity_id = await source.spawn(Position(x=4.0, y=5.0))
        await source.step()

        fork = await source.fork("branch", storage=storage)
        await source.shutdown()

        result = await fork.run(steps=1)
        assert result.ticks_completed == 1
        rows = (await fork.query(Position, entity_ids=[entity_id])).collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["entity_id"] == entity_id
        assert rows[0]["position__x"] == 4.0
        assert len(app._container.world_service.list_worlds()) == 1


@pytest.mark.asyncio
async def test_fork_shutdown_does_not_invalidate_source(tmp_path):
    async with ArchetypeRuntime() as app:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_fork_shutdown")
        source = app.world("source", storage=storage)

        await source.spawn(Position(x=8.0))
        await source.step()

        fork = await source.fork("fork", storage=storage)
        await fork.shutdown()

        result = await source.run(steps=1)
        assert result.ticks_completed == 1
        rows = (await source.query(Position)).collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["position__x"] == 8.0


@pytest.mark.asyncio
async def test_runtime_shutdown_is_explicit_idempotent_and_invalidates_all_handles(tmp_path):
    async with ArchetypeRuntime() as app:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_shutdown")
        left = app.world("left", storage=storage)
        right = app.world("right", storage=storage)

        await left.query(Position)
        await right.query(Position)

        await app.shutdown()
        await app.shutdown()

        with pytest.raises(RuntimeError, match="closed"):
            await left.query(Position)

        with pytest.raises(RuntimeError, match="closed"):
            await right.query(Position)


@pytest.mark.asyncio
async def test_runtime_shutdown_waits_for_in_flight_run(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "shutdown-in-flight",
            storage=StorageConfig(
                uri=str(tmp_path / "store"), namespace="runtime_shutdown_in_flight"
            ),
            processors=[SlowPass()],
        )

        await world.spawn(Position(x=1.0))
        await world.step()

        run_task = asyncio.create_task(world.run(steps=1))
        await asyncio.sleep(0.01)
        await app.shutdown()
        result = await run_task

        assert result.ticks_completed == 1
        with pytest.raises(RuntimeError, match="closed"):
            await world.query(Position)


@pytest.mark.asyncio
async def test_runtime_context_exit_rejects_future_use(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "ctx-exit",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_ctx_exit"),
        )
        await world.query(Position)

    with pytest.raises(RuntimeError, match="closed"):
        await world.query(Position)


@pytest.mark.asyncio
async def test_multiple_runtimes_are_isolated_and_allow_same_world_name(tmp_path):
    storage_one = StorageConfig(uri=str(tmp_path / "store1"), namespace="runtime_iso_1")
    storage_two = StorageConfig(uri=str(tmp_path / "store2"), namespace="runtime_iso_2")

    async with ArchetypeRuntime() as app1, ArchetypeRuntime() as app2:
        world1 = app1.world("same-name", storage=storage_one)
        world2 = app2.world("same-name", storage=storage_two)

        id1 = await world1.spawn(Position(x=1.0))
        id2 = await world2.spawn(Position(x=9.0))
        await world1.step()
        await world2.step()

        rows1 = (await world1.query(Position, entity_ids=[id1])).collect().to_pylist()
        rows2 = (await world2.query(Position, entity_ids=[id2])).collect().to_pylist()

        assert world1.world_id != world2.world_id
        assert rows1[0]["position__x"] == 1.0
        assert rows2[0]["position__x"] == 9.0
        assert (
            app1._container.world_service.get_world_by_name("same-name").world_id == world1.world_id
        )
        assert (
            app2._container.world_service.get_world_by_name("same-name").world_id == world2.world_id
        )


@pytest.mark.asyncio
async def test_runtime_world_spawn_not_query_visible_until_step(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "visibility",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_visibility"),
        )

        entity_id = await world.spawn(Position(x=6.0))

        assert (await world.query(Position, entity_ids=[entity_id])).collect().to_pylist() == []

        await world.step()
        rows = (await world.query(Position, entity_ids=[entity_id])).collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["position__x"] == 6.0


@pytest.mark.asyncio
async def test_runtime_world_update_is_not_query_visible_until_step(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "update-boundary",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_update_boundary"),
        )

        entity_id = await world.spawn(Position(x=1.0, y=2.0))
        await world.step()

        await world.update(entity_id, Position(x=9.0, y=4.0))
        before = (await world.query(Position, entity_ids=[entity_id])).collect().to_pylist()
        assert before[0]["position__x"] == 1.0
        assert before[0]["position__y"] == 2.0

        await world.step()
        after = (await world.query(Position, entity_ids=[entity_id])).collect().to_pylist()
        assert after[0]["position__x"] == 9.0
        assert after[0]["position__y"] == 4.0


@pytest.mark.asyncio
async def test_runtime_world_add_components_migrates_at_tick_boundary(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "add-components-boundary",
            storage=StorageConfig(
                uri=str(tmp_path / "store"), namespace="runtime_add_components_boundary"
            ),
        )

        entity_id = await world.spawn(Position(x=4.0, y=5.0))
        await world.step()

        await world.add_components(entity_id, Velocity(vx=2.0, vy=3.0))

        before = (
            (await world.query(Position, Velocity, entity_ids=[entity_id])).collect().to_pylist()
        )
        assert before == []

        await world.step()
        after = (
            (await world.query(Position, Velocity, entity_ids=[entity_id])).collect().to_pylist()
        )
        assert len(after) == 1
        assert after[0]["position__x"] == 4.0
        assert after[0]["velocity__vx"] == 2.0
        assert after[0]["velocity__vy"] == 3.0


@pytest.mark.asyncio
async def test_runtime_world_remove_components_applies_at_tick_boundary_and_preserves_remaining_data(
    tmp_path,
):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "remove-components-boundary",
            storage=StorageConfig(
                uri=str(tmp_path / "store"), namespace="runtime_remove_components_boundary"
            ),
        )

        entity_id = await world.spawn(
            Position(x=7.0, y=8.0),
            Velocity(vx=1.0, vy=2.0),
            Health(hp=80, max_hp=100),
        )
        await world.step()

        await world.remove_components(entity_id, Velocity)

        before = (
            (await world.query(Position, Velocity, entity_ids=[entity_id])).collect().to_pylist()
        )
        assert len(before) == 1

        await world.step()
        after = (
            (await world.query(Position, Velocity, entity_ids=[entity_id])).collect().to_pylist()
        )
        assert after == []

        remaining = (
            (await world.query(Position, Health, entity_ids=[entity_id])).collect().to_pylist()
        )
        assert len(remaining) == 1
        assert remaining[0]["position__x"] == 7.0
        assert remaining[0]["position__y"] == 8.0
        assert remaining[0]["health__hp"] == 80


@pytest.mark.asyncio
async def test_runtime_world_add_processor_applies_at_tick_boundary(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "processor-boundary",
            storage=StorageConfig(
                uri=str(tmp_path / "store"), namespace="runtime_processor_boundary"
            ),
        )

        entity_id = await world.spawn(Position(x=1.0, y=1.0), Velocity(vx=2.0, vy=0.0))
        await world.step()
        before = (await world.query(Position, entity_ids=[entity_id])).collect().to_pylist()
        assert before[0]["position__x"] == 1.0

        await world.add_processor(Move())
        mid = (await world.query(Position, entity_ids=[entity_id])).collect().to_pylist()
        assert mid[0]["position__x"] == 1.0

        await world.step()
        after = (await world.query(Position, entity_ids=[entity_id])).collect().to_pylist()
        assert after[0]["position__x"] == 3.0


@pytest.mark.asyncio
async def test_runtime_world_commands_are_audited_in_broker_history_in_order(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "audit",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_audit"),
        )

        entity_id = await world.spawn(Position(x=1.0))
        await world.add_processor(Noop())
        await world.step()
        await world.despawn(entity_id)

        history = await world.command_history()
        assert [cmd.type.value for cmd in history] == ["spawn", "add_processor", "despawn"]


@pytest.mark.asyncio
async def test_runtime_world_component_mutations_are_audited_in_broker_history_in_order(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "component-audit",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_component_audit"),
        )

        entity_id = await world.spawn(Position(x=1.0, y=1.0))
        await world.step()
        await world.update(entity_id, Position(x=5.0, y=6.0))
        await world.add_components(entity_id, Health(hp=90, max_hp=100))
        await world.remove_components(entity_id, Health)

        history = await world.command_history()
        assert [cmd.type.value for cmd in history] == [
            "spawn",
            "update",
            "add_component",
            "remove_component",
        ]


@pytest.mark.asyncio
async def test_runtime_world_spawn_respects_actor_permissions(tmp_path):
    async with ArchetypeRuntime(actor_ctx=ActorCtx(id=uuid7(), roles={"viewer"})) as app:
        world = app.world(
            "rbac",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_rbac"),
        )

        with pytest.raises(PermissionError):
            await world.spawn(Position(x=1.0))


@pytest.mark.asyncio
async def test_runtime_world_resource_mutation_does_not_generate_broker_history(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "resources",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_resources"),
        )

        await world.query(Position)
        before = await world.command_history()
        world.resources.insert(Delta(2.0))
        after = await world.command_history()

        assert before == []
        assert after == []


@pytest.mark.asyncio
async def test_runtime_world_hook_mutation_does_not_generate_broker_history(tmp_path):
    from archetype.core.hooks import PostTick

    hook_ticks: list[int] = []

    async def on_post_tick(event: PostTick) -> None:
        hook_ticks.append(event.tick)

    async with ArchetypeRuntime() as app:
        world = app.world(
            "hooks",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_hooks"),
        )

        await world.query(Position)
        before = await world.command_history()
        handle = world.add_hook(PostTick, on_post_tick)
        world.remove_hook(handle)
        after = await world.command_history()

        assert before == []
        assert after == []
        assert hook_ticks == []


@pytest.mark.asyncio
async def test_runtime_world_actor_aliases_enforce_rbac_by_role(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "rbac-aliases",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_rbac_aliases"),
        )
        viewer = world.as_actor(ActorCtx(id=uuid7(), roles={"viewer"}))
        player = world.as_actor(ActorCtx(id=uuid7(), roles={"player"}))
        coder = world.as_actor(ActorCtx(id=uuid7(), roles={"coder"}))
        maintainer = world.as_actor(ActorCtx(id=uuid7(), roles={"maintainer"}))
        admin = world.as_actor(ActorCtx(id=uuid7(), roles={"admin"}))

        with pytest.raises(PermissionError):
            await viewer.spawn(Position(x=0.0))

        entity_id = await player.spawn(Position(x=1.0, y=2.0))
        await admin.step()

        await player.update(entity_id, Position(x=4.0, y=5.0))
        await admin.step()

        with pytest.raises(PermissionError):
            await player.add_components(entity_id, Velocity(vx=1.0, vy=0.0))

        await coder.add_components(entity_id, Velocity(vx=1.0, vy=0.0))
        await admin.step()

        with pytest.raises(PermissionError):
            await coder.add_processor(Noop())

        await maintainer.add_processor(Noop())
        await admin.remove_processor(Noop)

        rows = (await admin.query(Position, Velocity, entity_ids=[entity_id])).collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["position__x"] == 4.0
        assert rows[0]["velocity__vx"] == 1.0


@pytest.mark.asyncio
async def test_world_shutdown_from_actor_alias_invalidates_all_aliases_but_not_siblings(tmp_path):
    async with ArchetypeRuntime() as app:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_alias_shutdown")
        left = app.world("left", storage=storage)
        right = app.world("right", storage=storage)
        player = left.as_actor(ActorCtx(id=uuid7(), roles={"player"}))
        admin = left.as_actor(ActorCtx(id=uuid7(), roles={"admin"}))

        await player.spawn(Position(x=1.0))
        await admin.step()
        await right.spawn(Position(x=9.0))
        await right.step()

        await player.shutdown()

        with pytest.raises(RuntimeError, match="closed"):
            await left.query(Position)

        with pytest.raises(RuntimeError, match="closed"):
            await admin.query(Position)

        rows = (await right.query(Position)).collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["position__x"] == 9.0


@pytest.mark.asyncio
async def test_runtime_world_fork_from_actor_alias_preserves_actor_binding(tmp_path):
    async with ArchetypeRuntime() as app:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_actor_fork")
        world = app.world("source", storage=storage)
        player = world.as_actor(ActorCtx(id=uuid7(), roles={"player"}))
        admin = world.as_actor(ActorCtx(id=uuid7(), roles={"admin"}))

        await player.spawn(Position(x=2.0))
        await admin.step()

        fork = await player.fork("branch", storage=storage)

        await fork.spawn(Position(x=3.0))
        with pytest.raises(PermissionError):
            await fork.add_processor(Noop())

        await admin.step()
        await fork.step()

        rows = sorted(
            (await fork.query(Position)).collect().to_pylist(), key=lambda row: row["entity_id"]
        )
        assert [row["position__x"] for row in rows] == [2.0, 3.0]


@pytest.mark.asyncio
async def test_archetype_runtime_async_smoke(tmp_path):
    async with ArchetypeRuntime() as app:
        world = app.world(
            "physics",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_async_smoke"),
            processors=[Move()],
        )

        await world.spawn(Position(x=0.0, y=0.0), Velocity(vx=1.0, vy=2.0))
        await world.spawn(Position(x=10.0, y=0.0), Velocity(vx=-1.0, vy=1.0))

        result = await world.run(steps=3)
        assert result.ticks_completed == 3

        rows = sorted(
            (await world.query(Position)).collect().to_pylist(), key=lambda row: row["entity_id"]
        )
        assert [(row["position__x"], row["position__y"]) for row in rows] == [
            (3.0, 6.0),
            (7.0, 3.0),
        ]


def test_archetype_runtime_sync_smoke(tmp_path):
    with ArchetypeRuntime.sync() as app:
        world = app.world(
            "physics-sync",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_sync_smoke"),
            processors=[Move()],
        )

        entity_id = world.spawn(Position(x=1.0, y=1.0), Velocity(vx=2.0, vy=0.5))
        result = world.run(steps=2)

        assert entity_id == 1
        assert result.ticks_completed == 2

        rows = world.query(Position, entity_ids=[entity_id]).collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["position__x"] == 5.0
        assert rows[0]["position__y"] == 2.0


def test_sync_runtime_preserves_state_across_multiple_calls(tmp_path):
    with ArchetypeRuntime.sync() as app:
        world = app.world(
            "sync-multi-call",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_sync_multi"),
        )

        entity_id = world.spawn(Position(x=3.0))
        assert world.query(Position, entity_ids=[entity_id]).collect().to_pylist() == []

        world.step()
        rows = world.query(Position, entity_ids=[entity_id]).collect().to_pylist()
        assert rows[0]["position__x"] == 3.0

        world.run(steps=2)
        rows = world.query(Position, entity_ids=[entity_id]).collect().to_pylist()
        assert rows[0]["position__x"] == 3.0


def test_sync_runtime_world_supports_mutation_parity_and_actor_aliases(tmp_path):
    with ArchetypeRuntime.sync() as app:
        world = app.world(
            "sync-parity",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_sync_parity"),
        )
        viewer = world.as_actor(ActorCtx(id=uuid7(), roles={"viewer"}))
        player = world.as_actor(ActorCtx(id=uuid7(), roles={"player"}))
        coder = world.as_actor(ActorCtx(id=uuid7(), roles={"coder"}))

        with pytest.raises(PermissionError):
            viewer.spawn(Position(x=0.0))

        entity_id = player.spawn(Position(x=1.0, y=2.0))
        world.step()

        player.update(entity_id, Position(x=4.0, y=6.0))
        world.step()

        coder.add_components(entity_id, Velocity(vx=2.0, vy=1.0))
        world.step()

        coder.remove_components(entity_id, Velocity)
        world.step()

        rows = world.query(Position, entity_ids=[entity_id]).collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["position__x"] == 4.0
        assert rows[0]["position__y"] == 6.0

        history = world.command_history()
        assert [cmd.type.value for cmd in history] == [
            "spawn",
            "update",
            "add_component",
            "remove_component",
        ]


def test_sync_world_rejects_use_after_runtime_context_exit(tmp_path):
    with ArchetypeRuntime.sync() as app:
        world = app.world(
            "sync-exit",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="runtime_sync_exit"),
        )
        world.query(Position)

    with pytest.raises(RuntimeError):
        world.query(Position)


def test_run_sync_executes_coroutines():
    assert run_sync(asyncio.sleep(0, result=5)) == 5


@pytest.mark.asyncio
async def test_run_sync_raises_inside_running_loop():
    coro = asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="running event loop"):
        run_sync(coro)
    coro.close()
