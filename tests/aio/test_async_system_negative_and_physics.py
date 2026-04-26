import math

import pytest
import pytest_asyncio
from daft import col, lit

from archetype.core.aio import (
    AsyncQueryManager,
    AsyncStore,
    AsyncSystem,
    AsyncUpdateManager,
    AsyncWorld,
)
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig
from archetype.core.hooks import HookRegistry
from archetype.core.resources import Resources
from archetype.runtime.session import configure_session


class Position(Component):
    x: float
    y: float


class Velocity(Component):
    dx: float
    dy: float


class Accel(Component):
    ax: float
    ay: float


@pytest_asyncio.fixture
async def world(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    session = configure_session(storage)
    store = AsyncStore(session, io_config=storage.io_config)
    w = AsyncWorld(
        world_id="test",
        name="w",
        querier=AsyncQueryManager(store=store),
        updater=AsyncUpdateManager(store=store),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
    )
    try:
        yield w
    finally:
        await store.shutdown()


@pytest.mark.asyncio
async def test_processor_skipped_when_components_do_not_match(world):
    class MismatchProc(AsyncProcessor):
        components = (Velocity,)  # requires velocity; our entity will not have it
        priority = 1

        async def process(self, df, **kwargs):
            # If incorrectly invoked, overwrite position__x to a sentinel value
            return df.with_column("position__x", lit(999.0))

    class IncX(AsyncProcessor):
        components = (Position,)
        priority = 2

        async def process(self, df, **kwargs):
            return df.with_column("position__x", col("position__x") + lit(1.0))

    await world.add_processor(MismatchProc())
    await world.add_processor(IncX())

    eid = await world.create_entity([Position(x=3.0, y=0.0)])
    rc = RunConfig()
    await world.step(rc)

    df = await world.get_components([Position], entity_ids=[eid])
    rows = df.collect().to_pylist()
    assert len(rows) == 1
    # Should only be incremented by IncX, not overwritten by MismatchProc
    assert math.isclose(rows[0]["position__x"], 4.0)


@pytest.mark.asyncio
async def test_priority_and_removal_affect_results(world):
    class TimesTwo(AsyncProcessor):
        components = (Position,)
        priority = 5

        async def process(self, df, **kwargs):
            return df.with_column("position__x", col("position__x") * lit(2))

    class PlusOne(AsyncProcessor):
        components = (Position,)
        priority = 6

        async def process(self, df, **kwargs):
            return df.with_column("position__x", col("position__x") + lit(1))

    # Register such that TimesTwo runs before PlusOne: x -> (x*2)+1
    await world.add_processor(TimesTwo())
    await world.add_processor(PlusOne())

    eid = await world.create_entity([Position(x=2.0, y=0.0)])
    rc = RunConfig()
    await world.step(rc)
    df = await world.get_components([Position], entity_ids=[eid])
    val1 = df.collect().to_pylist()[0]["position__x"]
    assert math.isclose(val1, (2.0 * 2) + 1)

    # Remove PlusOne, run again: now only TimesTwo applies
    await world.remove_processor(PlusOne)
    await world.step(rc)
    # Query from the store to avoid transient schema mismatches in live concat
    from archetype.core.archetype import Archetype

    sig = Archetype.sig_from_components([Position(x=0.0, y=0.0)])
    df2 = await world.query_archetype(sig, rc, ticks=[world.tick - 1])
    rows2 = [r for r in df2.collect().to_pylist() if r["entity_id"] == eid]
    assert rows2, "Expected entity row after second step"
    val2 = sorted(rows2, key=lambda r: r["tick"])[-1]["position__x"]
    # Prior result was 5; now only multiply-by-2 applied to that row: 10
    assert math.isclose(val2, val1 * 2)


@pytest.mark.asyncio
async def test_physics_dag_position_velocity_accel(world):
    # Physics processors: gravity, integrate velocity, integrate position, drag
    class Gravity(AsyncProcessor):
        components = (Accel,)
        priority = 1

        async def process(self, df, **kwargs):
            g = kwargs.get("g", 9.8)
            return df.with_column("accel__ay", col("accel__ay") - lit(g))

    class IntegrateV(AsyncProcessor):
        components = (Velocity, Accel)
        priority = 2

        async def process(self, df, **kwargs):
            dt = kwargs.get("dt", 1.0)
            return df.with_column(
                "velocity__dx", col("velocity__dx") + col("accel__ax") * lit(dt)
            ).with_column("velocity__dy", col("velocity__dy") + col("accel__ay") * lit(dt))

    class IntegrateP(AsyncProcessor):
        components = (Position, Velocity)
        priority = 3

        async def process(self, df, **kwargs):
            dt = kwargs.get("dt", 1.0)
            return df.with_column(
                "position__x", col("position__x") + col("velocity__dx") * lit(dt)
            ).with_column("position__y", col("position__y") + col("velocity__dy") * lit(dt))

    class Drag(AsyncProcessor):
        components = (Velocity,)
        priority = 4

        async def process(self, df, **kwargs):
            dt = kwargs.get("dt", 1.0)
            k = kwargs.get("k", 0.1)
            factor = 1.0 - (k * dt)
            return df.with_column("velocity__dx", col("velocity__dx") * lit(factor)).with_column(
                "velocity__dy", col("velocity__dy") * lit(factor)
            )

    await world.add_processor(Gravity())
    await world.add_processor(IntegrateV())
    await world.add_processor(IntegrateP())
    await world.add_processor(Drag())

    # Entity 1: has full P,V,A
    e1 = await world.create_entity(
        [Position(x=0.0, y=10.0), Velocity(dx=0.0, dy=0.0), Accel(ax=0.0, ay=0.0)]
    )
    # Entity 2: has only P,V
    e2 = await world.create_entity([Position(x=0.0, y=0.0), Velocity(dx=1.0, dy=0.0)])

    rc = RunConfig()
    await world.step(rc, dt=1.0, g=9.8, k=0.1)

    # Check E1 under P,V,A
    df1 = await world.get_components([Position, Velocity, Accel], entity_ids=[e1])
    rows1 = df1.collect().to_pylist()
    assert len(rows1) == 1
    pos_y = rows1[0]["position__y"]
    vel_y = rows1[0]["velocity__dy"]
    # After one step (dt=1):
    # vel_y = 0 + (0 - g)*1 = -9.8, then drag => -9.8 * 0.9 = -8.82
    # pos_y = 10 + (-9.8) = 0.2 (uses pre-drag velocity in this simple integrator)
    assert math.isclose(pos_y, 0.2, rel_tol=1e-6, abs_tol=1e-6)
    assert math.isclose(vel_y, -8.82, rel_tol=1e-6, abs_tol=1e-6)

    # Check E2 under P,V
    df2 = await world.get_components([Position, Velocity], entity_ids=[e2])
    rows2 = df2.collect().to_pylist()
    assert len(rows2) == 1
    pos_x2 = rows2[0]["position__x"]
    vel_x2 = rows2[0]["velocity__dx"]
    # vel_x unchanged by gravity (no accel): vel_x = 1.0, then drag => 0.9
    # pos_x = 0 + 1.0 = 1.0
    assert math.isclose(pos_x2, 1.0, rel_tol=1e-6, abs_tol=1e-6)
    assert math.isclose(vel_x2, 0.9, rel_tol=1e-6, abs_tol=1e-6)
