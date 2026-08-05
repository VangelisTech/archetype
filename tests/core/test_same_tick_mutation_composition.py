# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Same-tick mutation composition (spec C7) and migration no-op contracts.

Issue #193: a later mutation for the same entity in one drain cycle must
observe the earlier staged mutation — `update_entity` then `add_components`
composes; the second command bases on the first's staged row, not on the
last persisted tick, and the staged row must not also materialize under the
old signature.

Issue #367: migrating an entity with no visible state at all (neither a
staged row nor a persisted row) must be an observable no-op — no empty row
staged, no bookkeeping moved.
"""

import pytest

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_harness


class ComposePos(Component):
    x: float = 0.0


class ComposeVel(Component):
    vx: float = 0.0


async def _make_world(ws, tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    return await ws.lifecycle.create_world(WorldConfig(name="compose"), storage_config=storage)


@pytest.mark.asyncio
async def test_update_then_add_component_composes(tmp_path):
    """spec C7: ADD_COMPONENT after a same-tick UPDATE keeps the update."""
    ws = make_world_harness()
    try:
        world = await _make_world(ws, tmp_path)
        eid = await world.create_entity([ComposePos(x=1.0)])
        await world.step(RunConfig())  # x_0 = 1.0 persists @ t0

        await world.update_entity(eid, [ComposePos(x=5.0)])
        await world.add_components(eid, [ComposeVel(vx=2.0)])
        await world.step(RunConfig())  # both mutations materialize @ t1

        df = await world.query_archetype(sig=(ComposePos, ComposeVel), ticks=[1])
        rows = df.to_pylist()
        assert len(rows) == 1
        assert rows[0]["composepos__x"] == 5.0, (
            "add_components based its row on the persisted tick, losing the "
            "same-tick update (x=5.0) staged before it"
        )
        assert rows[0]["composevel__vx"] == 2.0

        live = await world.get_components([ComposePos])
        live_rows = live.to_pylist()
        assert len(live_rows) == 1, (
            f"entity {eid} is active in {len(live_rows)} rows; the update's "
            "staged row must not also materialize under the old signature"
        )
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_two_partial_updates_compose(tmp_path):
    """A second update touching different components keeps the first."""
    ws = make_world_harness()
    try:
        world = await _make_world(ws, tmp_path)
        eid = await world.create_entity([ComposePos(x=1.0), ComposeVel(vx=1.0)])
        await world.step(RunConfig())

        await world.update_entity(eid, [ComposePos(x=5.0)])
        await world.update_entity(eid, [ComposeVel(vx=9.0)])
        await world.step(RunConfig())

        df = await world.query_archetype(sig=(ComposePos, ComposeVel), ticks=[1])
        rows = df.to_pylist()
        assert len(rows) == 1
        assert rows[0]["composepos__x"] == 5.0, "first partial update was lost"
        assert rows[0]["composevel__vx"] == 9.0
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_spawn_then_add_component_before_first_step(tmp_path):
    """Migrating a spawned-but-unmaterialized entity uses its staged row."""
    ws = make_world_harness()
    try:
        world = await _make_world(ws, tmp_path)
        eid = await world.create_entity([ComposePos(x=3.0)])
        await world.add_components(eid, [ComposeVel(vx=4.0)])
        await world.step(RunConfig())

        df = await world.query_archetype(sig=(ComposePos, ComposeVel), ticks=[0])
        rows = df.to_pylist()
        assert len(rows) == 1, (
            "spawn-then-migrate before the first step must land exactly one "
            "row under the new signature"
        )
        assert rows[0]["entity_id"] == eid
        assert rows[0]["composepos__x"] == 3.0
        assert rows[0]["composevel__vx"] == 4.0
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_empty_spawn_then_add_component_before_first_step(tmp_path):
    ws = make_world_harness()
    try:
        world = await _make_world(ws, tmp_path)
        eid = await world.create_entity([])

        await world.add_components(eid, [ComposeVel(vx=4.0)])

        assert () not in world.spawn_cache
        await world.step(RunConfig())

        rows = (await world.query_archetype(sig=(ComposeVel,), ticks=[0])).to_pylist()
        assert len(rows) == 1
        assert rows[0]["entity_id"] == eid
        assert rows[0]["composevel__vx"] == 4.0
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_add_components_without_prior_row_is_noop(tmp_path):
    """Issue #367: no staged row + no persisted row -> logged no-op."""
    ws = make_world_harness()
    try:
        world = await _make_world(ws, tmp_path)
        # Poison: the registry claims the entity exists, but no row was ever
        # staged or persisted (state divergence, e.g. a bad replay).
        sig = Archetype.sig_from_components([ComposePos()])
        world.entity2sig[77] = sig

        await world.add_components(77, [ComposeVel(vx=1.0)])

        assert world.entity2sig[77] == sig, "no-op must not move the signature"
        assert not world.spawn_cache, "no-op must not stage a spawn row"
        assert not world.despawn_cache, "no-op must not stage a despawn"
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_remove_components_without_prior_row_is_noop(tmp_path):
    """Issue #367: remove_components mirrors the add_components no-op."""
    ws = make_world_harness()
    try:
        world = await _make_world(ws, tmp_path)
        sig = Archetype.sig_from_components([ComposePos(), ComposeVel()])
        world.entity2sig[77] = sig

        await world.remove_components(77, [ComposeVel])

        assert world.entity2sig[77] == sig, "no-op must not move the signature"
        assert not world.spawn_cache, "no-op must not stage a spawn row"
        assert not world.despawn_cache, "no-op must not stage a despawn"
    finally:
        await ws.close()
