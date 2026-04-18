# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""QueryService contract tests.

One test per MUST clause in ``docs/guide/specification.md#queryservice``
(Q1-Q12). These tests define the objective bar the service implementation
must clear. Tests that cannot pass against the current stub implementation
are marked xfail with the Q-number they guard; removing the xfail markers
tracks implementation progress.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from daft import DataFrame
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class FieldWithUnderscores(Component):
    """Guards Q5: field names may legitimately contain '__'."""

    nested__value: int = 0


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest_asyncio.fixture
async def container(tmp_path):
    c = ServiceContainer()
    yield c
    await c.shutdown()


@pytest_asyncio.fixture
async def world_with_entities(container, tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await container.world_service.create_world(WorldConfig(name="qs"), storage)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    for (x, y, vx, vy) in [(1.0, 2.0, 3.0, 4.0), (10.0, 20.0, 30.0, 40.0)]:
        await container.command_service.submit(
            str(world.world_id),
            Command(
                type=CommandType.SPAWN,
                tick=0,
                payload={
                    "components": [
                        Position(x=x, y=y).to_payload(),
                        Velocity(vx=vx, vy=vy).to_payload(),
                    ]
                },
            ),
            ctx,
        )
    await container.simulation_service.step(world.world_id)
    return container, world


# ---------------------------------------------------------------------------
# Q2: world existence — every read raises KeyError on unknown world
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_q2_get_world_state_raises_on_unknown_world(container):
    with pytest.raises(KeyError):
        await container.query_service.get_world_state(uuid7())


@pytest.mark.asyncio
async def test_q2_get_entity_raises_on_unknown_world(container):
    with pytest.raises(KeyError):
        await container.query_service.get_entity(uuid7(), entity_id=1)


@pytest.mark.asyncio
async def test_q2_get_components_raises_on_unknown_world(container):
    with pytest.raises(KeyError):
        await container.query_service.get_components(uuid7(), ["Position"])


@pytest.mark.asyncio
async def test_q2_get_command_history_raises_on_unknown_world(container):
    with pytest.raises(KeyError):
        await container.query_service.get_command_history(uuid7())


# ---------------------------------------------------------------------------
# Q3: entity existence — missing entity raises KeyError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_q3_missing_entity_raises(world_with_entities):
    container, world = world_with_entities
    with pytest.raises(KeyError):
        await container.query_service.get_entity(world.world_id, entity_id=9999)


# ---------------------------------------------------------------------------
# Q4: unknown component type raises ValueError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_q4_unknown_component_type_raises_valueerror(world_with_entities):
    container, world = world_with_entities
    with pytest.raises(ValueError):
        await container.query_service.get_components(world.world_id, ["NoSuchComponent"])


# ---------------------------------------------------------------------------
# Q5: response encoding — class-name keys, get_prefix-based unprefixing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_q5_component_keys_use_class_name(world_with_entities):
    container, world = world_with_entities
    result = await container.query_service.get_entity(world.world_id, entity_id=1)
    assert "Position" in result["components"]
    assert "position" not in result["components"]


@pytest.mark.asyncio
async def test_q5_field_names_handle_double_underscore(container, tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await container.world_service.create_world(WorldConfig(name="us"), storage)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    await container.command_service.submit(
        str(world.world_id),
        Command(
            type=CommandType.SPAWN,
            tick=0,
            payload={"components": [FieldWithUnderscores(nested__value=7).to_payload()]},
        ),
        ctx,
    )
    await container.simulation_service.step(world.world_id)
    result = await container.query_service.get_entity(world.world_id, entity_id=1)
    assert result["components"]["FieldWithUnderscores"]["nested__value"] == 7


# ---------------------------------------------------------------------------
# Q6: base columns are stripped from component payloads
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_q6_base_columns_not_in_component_payload(world_with_entities):
    container, world = world_with_entities
    result = await container.query_service.get_entity(world.world_id, entity_id=1)
    for name, payload in result["components"].items():
        for forbidden in ("world_id", "run_id", "entity_id", "tick", "is_active"):
            assert forbidden not in payload, f"{name} leaked base column {forbidden}"


# ---------------------------------------------------------------------------
# Q10: filter expressions are Daft Expressions, not strings
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="Q10 / Q12 — compilable filter surface not yet implemented; see specification.md",
    strict=True,
)
@pytest.mark.asyncio
async def test_q10_where_accepts_daft_expression(world_with_entities):
    from daft import col

    container, world = world_with_entities
    df = await container.query_service.get_components(
        world.world_id,
        [Position],  # class, not string — see Q12
        where=col("position__x") > 5.0,
        as_dataframe=True,
    )
    assert isinstance(df, DataFrame)
    assert df.count_rows() == 1


# ---------------------------------------------------------------------------
# Q11: count_only + limit is mutually exclusive
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="Q11 — count_only/limit combo validation not yet implemented",
    strict=True,
)
@pytest.mark.asyncio
async def test_q11_count_only_with_limit_raises(world_with_entities):
    container, world = world_with_entities
    with pytest.raises(ValueError):
        await container.query_service.get_components(
            world.world_id, ["Position"], count_only=True, limit=5
        )


# ---------------------------------------------------------------------------
# Q12: compilable client surface — Component classes + lazy DataFrame
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="Q12 — compilable query surface (world.query(Component)) not yet implemented",
    strict=True,
)
@pytest.mark.asyncio
async def test_q12_query_accepts_component_classes_returns_dataframe(world_with_entities):
    container, world = world_with_entities
    df = await container.query_service.get_components(
        world.world_id,
        [Position, Velocity],  # classes, not strings
        as_dataframe=True,
    )
    assert isinstance(df, DataFrame), "Q12 requires lazy DataFrame for in-process consumers"
    # No materialization by the service.
    rows = df.collect().to_pylist()
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# Q7 + Q10 core prerequisite: AsyncWorld.get_components must accept tick
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="Q7 prerequisite — AsyncWorld.get_components does not yet accept tick=; see CURRENT GAP",
    strict=True,
)
@pytest.mark.asyncio
async def test_q7_historical_multi_archetype_without_service_duplication(world_with_entities):
    container, world = world_with_entities
    # Historical read must route through a single core primitive, not be
    # reconstructed in the service layer.
    result = await container.query_service.get_components(
        world.world_id, ["Position"], tick=0
    )
    assert "rows" in result
