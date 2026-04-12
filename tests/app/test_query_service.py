# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for QueryService.query_dataframe()."""

from __future__ import annotations

import pytest
import pytest_asyncio

from archetype.app.query_service import QueryService
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

# ── Test components ──


class Agent(Component):
    name: str = ""


class Score(Component):
    val: float = 0.0


# ── Fixtures ──


@pytest_asyncio.fixture
async def world_with_data(tmp_path):
    """Create a world with Agent+Score entities, step once, return (world_service, world_id)."""
    ws = WorldService(StorageService())
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    world = await ws.create_world(WorldConfig(name="q-test"), storage_config=storage)

    # Spawn entities with Agent + Score
    await world.create_entity([Agent(name="alice"), Score(val=0.9)])
    await world.create_entity([Agent(name="bob"), Score(val=0.3)])
    await world.create_entity([Agent(name="carol"), Score(val=0.7)])

    # Step to materialize into _live
    await world.step(RunConfig())

    return ws, world.world_id


# ── Tests ──


@pytest.mark.asyncio
async def test_query_basic(world_with_data):
    """query_dataframe returns rows for matching components."""
    ws, wid = world_with_data
    qs = QueryService(ws)

    result = await qs.query_dataframe(wid, ["Agent", "Score"])

    assert "rows" in result
    assert "columns" in result
    assert len(result["rows"]) == 3
    # Check that component-prefixed columns are present
    col_names = result["columns"]
    assert "agent__name" in col_names
    assert "score__val" in col_names


@pytest.mark.asyncio
async def test_query_count(world_with_data):
    """count=True returns {count: N} without rows."""
    ws, wid = world_with_data
    qs = QueryService(ws)

    result = await qs.query_dataframe(wid, ["Agent", "Score"], count=True)

    assert result == {"count": 3}
    assert "rows" not in result


@pytest.mark.asyncio
async def test_query_where_filter(world_with_data):
    """where clause filters rows via SQL expression."""
    ws, wid = world_with_data
    qs = QueryService(ws)

    result = await qs.query_dataframe(wid, ["Agent", "Score"], where="score__val > 0.5")

    assert len(result["rows"]) == 2
    names = {r["agent__name"] for r in result["rows"]}
    assert names == {"alice", "carol"}


@pytest.mark.asyncio
async def test_query_limit(world_with_data):
    """limit restricts the number of rows returned."""
    ws, wid = world_with_data
    qs = QueryService(ws)

    result = await qs.query_dataframe(wid, ["Agent", "Score"], limit=2)

    assert len(result["rows"]) == 2


@pytest.mark.asyncio
async def test_query_sort(world_with_data):
    """sort orders rows by the specified column."""
    ws, wid = world_with_data
    qs = QueryService(ws)

    result = await qs.query_dataframe(wid, ["Agent", "Score"], sort="score__val", desc=True)

    vals = [r["score__val"] for r in result["rows"]]
    assert vals == sorted(vals, reverse=True)


@pytest.mark.asyncio
async def test_query_select(world_with_data):
    """select restricts returned columns."""
    ws, wid = world_with_data
    qs = QueryService(ws)

    result = await qs.query_dataframe(wid, ["Agent", "Score"], select=["entity_id", "agent__name"])

    assert set(result["columns"]) == {"entity_id", "agent__name"}
    for row in result["rows"]:
        assert "score__val" not in row


@pytest.mark.asyncio
async def test_query_offset(world_with_data):
    """offset skips the first N rows."""
    ws, wid = world_with_data
    qs = QueryService(ws)

    all_result = await qs.query_dataframe(wid, ["Agent", "Score"], sort="score__val")
    offset_result = await qs.query_dataframe(wid, ["Agent", "Score"], sort="score__val", offset=1)

    assert len(offset_result["rows"]) == 2
    assert offset_result["rows"][0] == all_result["rows"][1]


@pytest.mark.asyncio
async def test_query_unknown_component(world_with_data):
    """Unknown component name raises ValueError."""
    ws, wid = world_with_data
    qs = QueryService(ws)

    with pytest.raises(ValueError, match="not found"):
        await qs.query_dataframe(wid, ["NoSuchComponent"])
