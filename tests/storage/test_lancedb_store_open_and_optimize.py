import daft
import pyarrow as pa
import pytest

from archetype.app.storage.service import AsyncLancedbStore, _resolve_uri
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig


class Demo(Component):
    v: int


class OpenOnlyTable:
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema
        self._rows = []
        self.optimized = False

    class _Query:
        def __init__(self, rows, schema):
            self._rows = rows
            self._schema = schema

        def where(self, expr):
            return self

        async def to_arrow(self):
            return pa.Table.from_pylist(self._rows, schema=self._schema)

    def query(self):
        return OpenOnlyTable._Query(self._rows, self.schema)

    async def add(self, arrow_table, mode="append"):
        self._rows.extend(arrow_table.to_pylist())

    async def optimize(self, retrain=False):
        self.optimized = True

    async def create_index(self, column, config, replace=True):
        return None


class OpenPathClient:
    def __init__(self, name_to_table):
        self._tables = name_to_table

    async def list_tables(self):
        return list(self._tables.keys())

    async def table_names(self):
        return list(self._tables.keys())

    async def open_table(self, name):
        return self._tables[name]

    async def create_table(self, *args, **kwargs):
        raise AssertionError("create_table should not be called when table exists")

    def close(self):
        pass


@pytest.mark.asyncio
async def test_lancedb_open_path_no_create_called(monkeypatch, tmp_path):
    """When table exists, store should open table and avoid create/index calls."""
    config = StorageConfig(uri=str(tmp_path / "wh"), namespace="ns")
    uri = _resolve_uri(str(config.uri))
    store = AsyncLancedbStore(uri, config.namespace)

    sig = Archetype.sig_from_components([Demo(v=1)])
    schema = Archetype.get_archetype_schema(sig)
    table_name = Archetype.get_name(sig)
    fake_table = OpenOnlyTable(table_name, schema)
    client = OpenPathClient({table_name: fake_table})

    async def fake_connect_async(path):
        return client

    monkeypatch.setattr(
        "archetype.core.aio.async_lancedb_store.lancedb.connect_async", fake_connect_async
    )

    # Append should open existing table and not attempt create
    arrow = pa.Table.from_pylist(
        [
            {
                "world_id": "w",
                "run_id": "r",
                "entity_id": 1,
                "tick": 0,
                "is_active": True,
                "commit_token": "",
                "writer_epoch": 0,
                "demo__v": 5,
            }
        ],
        schema=schema,
    )
    await store.append(sig, daft.from_arrow(arrow))
    df = await store.get_archetype_df(sig, world_id="w", run_id="r")
    assert df.count_rows() == 1


class MultiTableClient:
    def __init__(self):
        self._tables = {
            "t1": OpenOnlyTable("t1", pa.schema([pa.field("id", pa.int32())])),
            "t2": OpenOnlyTable("t2", pa.schema([pa.field("id", pa.int32())])),
        }

    async def list_tables(self):
        return list(self._tables.keys())

    async def table_names(self):
        return list(self._tables.keys())

    async def open_table(self, name):
        return self._tables[name]

    async def create_table(self, name, schema, storage_options=None, exist_ok=True):
        t = OpenOnlyTable(name, schema)
        self._tables[name] = t
        return t

    def close(self):
        pass


@pytest.mark.asyncio
async def test_lancedb_optimize_multiple_tables(monkeypatch, tmp_path):
    """optimize_tables should iterate all table names and invoke optimize(retrain=False)."""
    client = MultiTableClient()

    async def fake_connect_async(path):
        return client

    monkeypatch.setattr(
        "archetype.core.aio.async_lancedb_store.lancedb.connect_async", fake_connect_async
    )

    config = StorageConfig(uri=str(tmp_path / "wh2"), namespace="ns")
    uri = _resolve_uri(str(config.uri))
    store = AsyncLancedbStore(uri, config.namespace)

    # trigger connection init by a read OR create a table if needed
    # Create a dummy signature that will exercise _ensure_table create path with our client
    class T(Component):
        a: int

    await store.get_archetype_df((T,), world_id="w", run_id="r")
    await store.optimize_tables()
    assert getattr(client._tables["t1"], "optimized", False) is True
    assert getattr(client._tables["t2"], "optimized", False) is True
