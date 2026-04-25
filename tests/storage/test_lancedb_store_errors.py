import daft
import pyarrow as pa
import pytest

from archetype.app.storage_service import StorageService
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.storage.lancedb import AsyncLancedbStore


class Demo(Component):
    v: int


class FailingOpenClient:
    def __init__(self):
        self._tables = {"t": object()}

    async def list_tables(self):
        return ["t"]

    async def table_names(self):
        return ["t"]

    async def open_table(self, name):
        raise RuntimeError("open failed")

    def close(self):
        pass


class FailingAddTable:
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    async def create_index(self, *args, **kwargs):
        return None

    async def add(self, arrow_table, mode="append"):
        raise RuntimeError("add failed")


class FailingAddClient:
    def __init__(self):
        self._tables = {}

    async def list_tables(self):
        return []

    async def table_names(self):
        return []

    async def create_table(self, name, schema, storage_options=None, exist_ok=True):
        return FailingAddTable(name, schema)

    def close(self):
        pass


@pytest.mark.asyncio
async def test_lancedb_store_open_table_failure_raises(monkeypatch, tmp_path):
    client = FailingOpenClient()

    async def fake_connect_async(path):
        return client

    monkeypatch.setattr("archetype.core.storage.lancedb.lancedb.connect_async", fake_connect_async)
    uri, namespace = StorageService.resolve_location(
        StorageConfig(uri=str(tmp_path / "wh"), namespace="ns")
    )
    store = AsyncLancedbStore(uri, namespace)

    # Craft sig that maps to existing table name to hit open path
    class T(Component):
        a: int

    sig = (T,)
    # Monkeypatch Archetype.get_name to return 't'
    monkeypatch.setattr("archetype.core.storage.lancedb.Archetype.get_name", lambda s: "t")

    with pytest.raises(RuntimeError, match="open failed"):
        await store.get_archetype_df(sig, world_id="w", run_id="r")


@pytest.mark.asyncio
async def test_lancedb_store_append_failure_raises(monkeypatch, tmp_path):
    client = FailingAddClient()

    async def fake_connect_async(path):
        return client

    monkeypatch.setattr("archetype.core.storage.lancedb.lancedb.connect_async", fake_connect_async)
    uri, namespace = StorageService.resolve_location(
        StorageConfig(uri=str(tmp_path / "wh2"), namespace="ns")
    )
    store = AsyncLancedbStore(uri, namespace)

    sig = Archetype.sig_from_components([Demo(v=1)])
    schema = Archetype.get_archetype_schema(sig)
    arrow = pa.Table.from_pylist(
        [
            {
                "world_id": "w",
                "run_id": "r",
                "entity_id": 1,
                "tick": 0,
                "is_active": True,
                "demo__v": 1,
            }
        ],
        schema=schema,
    )

    with pytest.raises(RuntimeError, match="add failed"):
        await store.append(sig, daft.from_arrow(arrow))
