# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Same-tick spawn → migrate contracts.

Regression tests: migrating an entity spawned in the same tick used to
enqueue an empty row into the spawn cache, which raised KeyError inside
materialize_mutations on every subsequent step — permanently breaking the
world. The migration must instead consume the pending spawn row so the
entity materializes under exactly one signature.
"""

import pytest
import pytest_asyncio

from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_store import AsyncStore
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_updater import AsyncUpdateManager
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig
from archetype.core.hooks import HookRegistry
from archetype.core.resources import Resources
from archetype.runtime.session import configure_session


class Position(Component):
    x: int
    y: int


class Velocity(Component):
    dx: int
    dy: int


@pytest_asyncio.fixture()
async def world(tmp_path):
    storage = StorageConfig(uri=str(tmp_path), namespace="test")
    session = configure_session(storage)
    store = AsyncStore(session, io_config=storage.io_config)
    world = AsyncWorld(
        world_id="test",
        name="w",
        querier=AsyncQueryManager(store=store),
        updater=AsyncUpdateManager(store=store),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
    )
    try:
        yield world
    finally:
        await store.shutdown()


@pytest.mark.asyncio
async def test_same_tick_spawn_then_add_components(world):
    """add_components on an entity spawned this tick keeps its values and
    materializes it under the new signature only."""
    sig_pos = Archetype.sig_from_components([Position(x=0, y=0)])
    sig_pos_vel = Archetype.add_components(sig_pos, [Velocity])

    e1 = await world.create_entity([Position(x=5, y=6)])
    await world.add_components(e1, [Velocity(dx=1, dy=1)])

    assert world.entity2sig[e1] == sig_pos_vel
    # The pending spawn under the old signature must be consumed
    assert not world.spawn_cache.get(sig_pos)

    await world.step(RunConfig())

    df_new = await world.query_archetype(sig_pos_vel, ticks=[0])
    rows = df_new.to_pylist()
    assert len(rows) == 1
    assert rows[0]["entity_id"] == e1
    assert rows[0]["position__x"] == 5
    assert rows[0]["velocity__dx"] == 1
    assert rows[0]["is_active"] is True

    # No active row under the old signature
    df_old = await world.query_archetype(sig_pos, ticks=[0])
    assert all(not r["is_active"] for r in df_old.to_pylist() if r["entity_id"] == e1)

    # The world keeps stepping (used to raise KeyError forever)
    await world.step(RunConfig())
    await world.step(RunConfig())


@pytest.mark.asyncio
async def test_migrate_unknown_prior_row_does_not_brick_world(world):
    """If the prior row cannot be found, the migration is dropped with a
    warning and the world keeps stepping."""
    sig_pos = Archetype.sig_from_components([Position(x=0, y=0)])

    e1 = await world.create_entity([Position(x=1, y=1)])
    await world.step(RunConfig())  # persist spawn at t=0

    # Force a vanished prior row: lie about the entity's signature so the
    # store query for the previous tick finds nothing.
    world.entity2sig[e1] = Archetype.sig_from_components([Velocity(dx=0, dy=0)])
    await world.add_components(e1, [Position(x=2, y=2)])

    # No empty rows in any spawn cache
    for rows in world.spawn_cache.values():
        assert all(row for row in rows)

    # Stepping continues to work
    await world.step(RunConfig())
    await world.step(RunConfig())

    # And the original data is still queryable
    df = await world.query_archetype(sig_pos, ticks=[0])
    assert any(r["entity_id"] == e1 for r in df.to_pylist())


@pytest.mark.asyncio
async def test_emptied_signatures_are_not_reprocessed_forever(world):
    """Signature keys are dropped from the caches once consumed so dead
    archetypes do not stay in active_signatures for every future tick."""
    sig_pos = Archetype.sig_from_components([Position(x=0, y=0)])

    e1 = await world.create_entity([Position(x=1, y=1)])
    await world.step(RunConfig())
    await world.add_components(e1, [Velocity(dx=1, dy=1)])
    await world.step(RunConfig())

    # e1 now lives under (Position, Velocity); the bare Position archetype
    # has no entities and must not be active anymore.
    assert sig_pos not in world.active_signatures
