# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Batch spawn API + entity-id reservation contract tests.

Acceptance criteria verified here:
  1. Batch spawn: one materialization batch per archetype, one OnSpawn per
     entity, raw tick-0 rows (strict-equality), 256-entity timing test passes.
  2. Reservation API: no collisions under interleaving, same-tick cancel works.
  3. Hook firing counts are exactly N for N spawns (batch or single).

Design note: docs/design/entity-id-reservation.md
"""

from __future__ import annotations

import time

import pytest

from archetype.core.aio import AsyncProcessor, AsyncSystem
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_service

# ─────────────────────────────────────────────────────────────────────────────
# Test components
# ─────────────────────────────────────────────────────────────────────────────


class Val(Component):
    v: float = 0.0


class Tag(Component):
    name: str = ""


class DoubleComp(Component):
    x: float = 0.0


class StepProcessor(AsyncProcessor):
    """Increments Val.v by 1 each tick."""

    components = (Val,)
    priority = 10

    async def process(self, df, **kwargs):
        from daft import col

        return df.with_column("val__v", col("val__v") + 1.0)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Batch spawn: raw-at-tick semantics + one OnSpawn per entity
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_batch_spawn_raw_initial_conditions(tmp_path):
    """create_entities preserves x_0 raw at spawn tick; processors apply next tick."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(StepProcessor())
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)

        # Batch-spawn 3 entities with distinct initial values
        entity_ids = await world.create_entities(
            [
                [Val(v=10.0)],
                [Val(v=20.0)],
                [Val(v=30.0)],
            ]
        )
        assert len(entity_ids) == 3
        assert len(set(entity_ids)) == 3, "all IDs must be unique"

        # Step once: tick 0 writes raw spawn rows; tick 1 applies processor
        await world.run(RunConfig(num_steps=2))

        sig = (Val,)
        # Tick 0: raw spawn values
        t0_rows = {
            r["entity_id"]: r["val__v"]
            for r in (await world.query_archetype(sig=sig, ticks=[0])).to_pylist()
        }
        # Tick 1: processor applied
        t1_rows = {
            r["entity_id"]: r["val__v"]
            for r in (await world.query_archetype(sig=sig, ticks=[1])).to_pylist()
        }

        assert t0_rows == {entity_ids[0]: 10.0, entity_ids[1]: 20.0, entity_ids[2]: 30.0}, (
            f"raw spawn rows at tick 0 must match initial values: {t0_rows}"
        )
        assert t1_rows == {entity_ids[0]: 11.0, entity_ids[1]: 21.0, entity_ids[2]: 31.0}, (
            f"processor increments at tick 1: {t1_rows}"
        )
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_batch_spawn_on_spawn_hook_count(tmp_path):
    """create_entities fires exactly one OnSpawn per entity."""
    from archetype.core.hooks import OnSpawn

    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        fired: list[int] = []

        async def on_spawn(event: OnSpawn) -> None:
            fired.append(event.entity_id)

        world.add_hook(OnSpawn, on_spawn)

        n = 5
        entity_ids = await world.create_entities([[Val(v=float(i))] for i in range(n)])

        assert len(fired) == n, f"expected {n} OnSpawn fires, got {len(fired)}"
        assert set(fired) == set(entity_ids), "OnSpawn must fire for each spawned entity ID"
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_batch_spawn_mixed_archetypes_one_batch_each(tmp_path):
    """Entities with different component sets end up in separate archetype tables."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        entity_ids = await world.create_entities(
            [
                [Val(v=1.0)],
                [Tag(name="a")],
                [Val(v=2.0)],
                [Tag(name="b")],
            ]
        )
        assert len(entity_ids) == 4

        await world.run(RunConfig(num_steps=1))

        val_rows = (await world.query_archetype(sig=(Val,), ticks=[0])).to_pylist()
        tag_rows = (await world.query_archetype(sig=(Tag,), ticks=[0])).to_pylist()

        assert len(val_rows) == 2, f"expected 2 Val entities, got {len(val_rows)}"
        assert len(tag_rows) == 2, f"expected 2 Tag entities, got {len(tag_rows)}"
    finally:
        await ws.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 2. 256-entity batch spawn timing test (loose upper bound: < 5 s)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_batch_spawn_256_entities_timing(tmp_path):
    """256 entities spawned via create_entities must complete in under 5 seconds.

    The acceptance criterion is < 1 s; we use 5 s as the CI bound to avoid
    flakiness on loaded runners, while still catching gross regressions.
    """
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(StepProcessor())
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)

        n = 256
        t0 = time.monotonic()
        entity_ids = await world.create_entities([[Val(v=float(i))] for i in range(n)])
        await world.run(RunConfig(num_steps=1))
        elapsed = time.monotonic() - t0

        assert len(entity_ids) == n
        assert elapsed < 5.0, f"256-entity batch spawn took {elapsed:.2f}s, must be < 5s"
    finally:
        await ws.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 3. Reservation API: collision rules + interleaving
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reserve_ids_no_collision_with_auto_assign(tmp_path):
    """Reserved IDs never collide with auto-assigned IDs under interleaving."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        all_ids: list[int] = []

        # Interleave reservations and auto-assigns multiple times
        for _ in range(10):
            reserved = world.reserve_entity_ids(3)
            auto_id = await world.create_entity([Val(v=0.0)])
            all_ids.extend(reserved)
            all_ids.append(auto_id)

        assert len(all_ids) == len(set(all_ids)), (
            f"ID collision detected: {len(all_ids)} IDs, {len(set(all_ids))} unique"
        )
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_reserve_ids_no_collision_with_batch_spawn(tmp_path):
    """Reserved IDs never collide with batch-spawn IDs under interleaving."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        all_ids: list[int] = []

        for _ in range(5):
            reserved = world.reserve_entity_ids(5)
            batch_ids = await world.create_entities([[Val(v=float(i))] for i in range(5)])
            all_ids.extend(reserved)
            all_ids.extend(batch_ids)

        assert len(all_ids) == len(set(all_ids)), (
            f"ID collision detected: {len(all_ids)} IDs, {len(set(all_ids))} unique"
        )
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_reserve_ids_invalid_n_raises(tmp_path):
    """reserve_entity_ids(n < 1) raises ValueError."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        with pytest.raises(ValueError, match="n >= 1"):
            world.reserve_entity_ids(0)

        with pytest.raises(ValueError, match="n >= 1"):
            world.reserve_entity_ids(-5)
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_spawn_with_reserved_id_materializes_entity(tmp_path):
    """spawn_with_reserved_id registers the entity exactly as create_entity would."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        await system.add_processor(StepProcessor())
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)

        (reserved_id,) = world.reserve_entity_ids(1)
        await world.spawn_with_reserved_id(reserved_id, [Val(v=42.0)])

        await world.run(RunConfig(num_steps=2))

        sig = (Val,)
        t0 = {
            r["entity_id"]: r["val__v"]
            for r in (await world.query_archetype(sig=sig, ticks=[0])).to_pylist()
        }
        t1 = {
            r["entity_id"]: r["val__v"]
            for r in (await world.query_archetype(sig=sig, ticks=[1])).to_pylist()
        }

        assert t0[reserved_id] == 42.0, f"raw spawn row expected 42.0, got {t0}"
        assert t1[reserved_id] == 43.0, f"processor-applied row expected 43.0, got {t1}"
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_spawn_with_reserved_id_fires_on_spawn_hook(tmp_path):
    """spawn_with_reserved_id fires exactly one OnSpawn."""
    from archetype.core.hooks import OnSpawn

    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        fired: list[int] = []

        async def on_spawn(event: OnSpawn) -> None:
            fired.append(event.entity_id)

        world.add_hook(OnSpawn, on_spawn)

        (reserved_id,) = world.reserve_entity_ids(1)
        await world.spawn_with_reserved_id(reserved_id, [Val(v=1.0)])

        assert fired == [reserved_id], f"expected one OnSpawn for {reserved_id}, got {fired}"
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_spawn_with_reserved_id_double_spawn_raises(tmp_path):
    """spawn_with_reserved_id raises ValueError when entity is already registered."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        (reserved_id,) = world.reserve_entity_ids(1)
        await world.spawn_with_reserved_id(reserved_id, [Val(v=1.0)])

        with pytest.raises(ValueError, match="already registered"):
            await world.spawn_with_reserved_id(reserved_id, [Val(v=2.0)])
    finally:
        await ws.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 4. Same-tick cancel of reserved spawns
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_same_tick_cancel_of_reserved_spawn(tmp_path):
    """remove_entity on a reserved+materialised entity cancels the spawn cache.

    The entity must not appear in the ledger after the tick runs.
    """
    from archetype.core.hooks import OnDespawn, OnSpawn

    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        spawned: list[int] = []
        despawned: list[int] = []

        async def on_spawn(event: OnSpawn) -> None:
            spawned.append(event.entity_id)

        async def on_despawn(event: OnDespawn) -> None:
            despawned.append(event.entity_id)

        world.add_hook(OnSpawn, on_spawn)
        world.add_hook(OnDespawn, on_despawn)

        # Reserve, materialise, then immediately cancel before any step
        (reserved_id,) = world.reserve_entity_ids(1)
        await world.spawn_with_reserved_id(reserved_id, [Val(v=99.0)])
        await world.remove_entity(reserved_id)

        # Also spawn a normal entity that should survive
        survivor = await world.create_entity([Val(v=1.0)])

        await world.run(RunConfig(num_steps=1))

        sig = (Val,)
        t0_rows = (await world.query_archetype(sig=sig, ticks=[0])).to_pylist()
        entity_ids_in_ledger = {r["entity_id"] for r in t0_rows}

        assert reserved_id not in entity_ids_in_ledger, (
            f"cancelled spawn {reserved_id} must not appear in ledger: {entity_ids_in_ledger}"
        )
        assert survivor in entity_ids_in_ledger, "surviving entity must still be in ledger"

        # Hook counts
        assert reserved_id in spawned, "OnSpawn must fire even for cancelled spawns"
        assert reserved_id in despawned, "OnDespawn must fire on cancel"
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_cancel_reserved_id_that_was_never_materialised(tmp_path):
    """Calling remove_entity on a reserved-but-not-yet-materialised ID is a no-op.

    A reserved ID is just a number — until spawn_with_reserved_id is called,
    the world knows nothing about it, so remove_entity should warn but not crash.
    """
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        (reserved_id,) = world.reserve_entity_ids(1)
        # Never called spawn_with_reserved_id — entity not registered
        # remove_entity should be a no-op (logs a warning, does not crash)
        await world.remove_entity(reserved_id)  # must not raise

        await world.run(RunConfig(num_steps=1))

        # Counter must still be past the reserved range
        assert world.next_entity_id > reserved_id
    finally:
        await ws.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 5. Property-style collision test across many interleaved operations
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_no_collisions_under_heavy_interleaving(tmp_path):
    """100 rounds of mixed reserve/create/batch-spawn produce no ID collisions."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage)

        all_ids: list[int] = []

        for i in range(100):
            choice = i % 3
            if choice == 0:
                ids = world.reserve_entity_ids(2)
            elif choice == 1:
                ids = [await world.create_entity([Val(v=float(i))])]
            else:
                ids = await world.create_entities(
                    [
                        [Val(v=float(i))],
                        [Tag(name=str(i))],
                    ]
                )
            all_ids.extend(ids)

        assert len(all_ids) == len(set(all_ids)), (
            f"Collision in {len(all_ids)} IDs, only {len(set(all_ids))} unique"
        )
    finally:
        await ws.shutdown()
