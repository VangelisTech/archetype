# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Read-only physical-table access for durable ledger discovery."""

import daft
import pyarrow as pa
import pytest

from archetype.core.aio import AsyncLancedbStore
from archetype.core.archetype import Archetype
from archetype.core.component import Component


class _ColdReadValue(Component):
    value: int


def _rows(*, world_id: str = "world", run_id: str = "run"):
    sig = (_ColdReadValue,)
    schema = Archetype.get_archetype_schema(sig)
    table = pa.Table.from_pylist(
        [
            {
                "world_id": world_id,
                "run_id": run_id,
                "entity_id": 1,
                "tick": 0,
                "is_active": True,
                "_coldreadvalue__value": 7,
            }
        ],
        schema=schema,
    )
    return sig, daft.from_arrow(table)


@pytest.mark.asyncio
async def test_fresh_store_reads_existing_table_without_signature_priming(tmp_path):
    sig, frame = _rows()
    writer = AsyncLancedbStore(str(tmp_path), "ns")
    await writer.append(sig, frame)
    await writer.shutdown()

    reader = AsyncLancedbStore(str(tmp_path), "ns")
    table_id = Archetype.get_name(sig)
    result = await reader.get_table_df(table_id, "world", "run")

    assert result.to_pylist() == frame.to_pylist()
    assert await reader.list_signatures() == []
    assert await reader.list_committed_signatures() == []
    assert await reader.get_table_schema(table_id) == Archetype.get_archetype_schema(sig)
    await reader.shutdown()


@pytest.mark.asyncio
async def test_missing_table_read_does_not_create_table(tmp_path):
    reader = AsyncLancedbStore(str(tmp_path), "ns")

    with pytest.raises(KeyError, match="does not exist"):
        await reader.get_table_df("missing-table", "world", "run")

    assert "missing-table" not in await reader._list_table_names()
    assert await reader.list_signatures() == []
    await reader.shutdown()


@pytest.mark.asyncio
async def test_empty_selectors_return_empty_existing_schema(tmp_path):
    sig, frame = _rows()
    store = AsyncLancedbStore(str(tmp_path), "ns")
    await store.append(sig, frame)

    by_tick = await store.get_table_df(
        Archetype.get_name(sig),
        "world",
        "run",
        ticks=[],
    )
    by_entity = await store.get_table_df(
        Archetype.get_name(sig),
        "world",
        "run",
        entity_ids=[],
    )

    assert by_tick.count_rows() == 0
    assert by_entity.count_rows() == 0
    assert by_tick.column_names == list(Archetype.get_archetype_schema(sig).names)
    await store.shutdown()


@pytest.mark.parametrize("subdir", ["", ".", "..", "../escape", "/tmp/escape", "a/b"])
def test_lancedb_subdir_rejects_empty_absolute_and_traversal(tmp_path, subdir):
    with pytest.raises(ValueError, match="safe relative path segment"):
        AsyncLancedbStore(str(tmp_path), "ns", subdir=subdir)
