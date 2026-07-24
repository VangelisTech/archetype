# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable fork-lineage storage contracts."""

import lancedb
import pyarrow as pa
import pytest

from archetype.core.aio import AsyncCachedStore, AsyncLancedbStore
from archetype.core.archetype import Archetype
from archetype.core.config import CacheConfig
from archetype.core.lineage import LINEAGE_SIG, load_lineage, persist_lineage


@pytest.mark.asyncio
async def test_legacy_only_lineage_is_read_without_creating_current_table(tmp_path) -> None:
    legacy_name = Archetype.get_legacy_name(LINEAGE_SIG)
    current_name = Archetype.get_name(LINEAGE_SIG)
    legacy_schema = Archetype.get_legacy_schema(LINEAGE_SIG)
    assert legacy_name != current_name

    db = await lancedb.connect_async(str(tmp_path / "ns" / "lance"))
    table = await db.create_table(legacy_name, schema=legacy_schema)
    await table.add(
        pa.Table.from_pylist(
            [
                {
                    "world_id": "child",
                    "run_id": "child-run",
                    "entity_id": -1,
                    "tick": 3,
                    "is_active": True,
                    "worldlineage__parent_world_id": "parent",
                    "worldlineage__parent_run_id": "parent-run",
                    "worldlineage__up_to_tick": 2,
                    "worldlineage__position": 0,
                }
            ],
            schema=legacy_schema,
        )
    )

    store = AsyncLancedbStore(str(tmp_path), "ns")
    try:
        assert await load_lineage(
            store,
            world_id="child",
            run_id="child-run",
        ) == [("parent", "parent-run", 2)]
        with pytest.raises(KeyError):
            await store.get_existing_table_schema(current_name)
    finally:
        await store.shutdown()


@pytest.mark.asyncio
async def test_cached_lineage_append_is_durable_before_persist_returns(tmp_path) -> None:
    inner = AsyncLancedbStore(str(tmp_path), "ns")
    cached = AsyncCachedStore(
        inner,
        CacheConfig(
            flush_rows=1_000_000,
            flush_mb=10_000,
            global_mb=10_000,
            idle_sec=3600,
        ),
    )
    cold = AsyncLancedbStore(str(tmp_path), "ns")
    try:
        await persist_lineage(
            cached,
            world_id="child",
            run_id="child-run",
            tick=3,
            lineage=[("parent", "parent-run", 2)],
        )

        assert cached.total_cached_bytes == 0
        assert await load_lineage(
            cold,
            world_id="child",
            run_id="child-run",
        ) == [("parent", "parent-run", 2)]
    finally:
        await cached.shutdown()
        await cold.shutdown()
