import daft
import pyarrow as pa
import pytest

from archetype.app.storage_factory import StorageFactory
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.storage.lancedb import AsyncLancedbStore


class Demo(Component):
    v: int


class FakeAsyncTable:
    def __init__(self, name, schema):
        self.name = name
        self._schema = schema
        self._rows = []

    async def create_index(self, column, config, replace=True):
        return None

    class _Query:
        def __init__(self, rows, schema):
            self._rows = rows
            self._schema = schema

        def where(self, expr: str):
            # Extremely naive filter: if no rows, return self; used only in test
            return self

        async def to_arrow(self):
            return pa.Table.from_pylist(self._rows, schema=self._schema)

    def query(self):
        return FakeAsyncTable._Query(self._rows, self._schema)

    async def add(self, arrow_table, mode="append"):
        self._rows.extend(arrow_table.to_pylist())

    async def optimize(self, retrain=False):
        return None


class FakeClient:
    def __init__(self):
        self._tables = {}

    async def list_tables(self):
        return list(self._tables.keys())

    async def table_names(self):
        return list(self._tables.keys())

    async def open_table(self, name):
        return self._tables[name]

    async def create_table(self, name, schema, storage_options=None, exist_ok=True):
        t = FakeAsyncTable(name, schema)
        self._tables[name] = t
        return t

    def close(self):
        pass


@pytest.mark.asyncio
async def test_lancedb_store_creates_opens_and_appends(monkeypatch, tmp_path):
    """AsyncLancedbStore should create/open tables, append arrow rows, and query filtered frames via fake client."""
    fake = FakeClient()

    async def fake_connect_async(path):
        return fake

    monkeypatch.setattr("archetype.core.storage.lancedb.lancedb.connect_async", fake_connect_async)

    uri, namespace = StorageFactory.resolve_location(
        StorageConfig(uri=str(tmp_path / "warehouse"), namespace="ns")
    )
    store = AsyncLancedbStore(uri, namespace)

    sig = Archetype.sig_from_components([Demo(v=1)])

    # 1) get df on empty table should yield empty daft frame
    df = await store.get_archetype_df(sig, world_id="w", run_id="r")
    assert df.count_rows() == 0

    # 2) append a row and then query
    schema = Archetype.get_archetype_schema(sig)
    arrow = pa.Table.from_pylist(
        [
            {
                "world_id": "w",
                "run_id": "r",
                "entity_id": 1,
                "tick": 0,
                "is_active": True,
                "demo__v": 7,
            }
        ],
        schema=schema,
    )
    await store.append(sig, daft.from_arrow(arrow))

    df2 = await store.get_archetype_df(sig, world_id="w", run_id="r")
    assert df2.count_rows() == 1

    # 3) optimizeTables traverses
    await fake.open_table(Archetype.get_name(sig))  # ensure exists
    await store.optimize_tables()
