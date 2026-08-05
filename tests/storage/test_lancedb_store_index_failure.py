import pytest

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.storage.service import AsyncLancedbStore, _resolve_uri


class Demo(Component):
    v: int


class BadIndexTable:
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    async def create_index(self, column, config, replace=True):
        raise RuntimeError("index failed")


class RetryableIndexTable(BadIndexTable):
    def __init__(self, name, schema):
        super().__init__(name, schema)
        self.calls = 0

    async def create_index(self, column, config, replace=True):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("retryable conflict")


class IndexFailClient:
    def __init__(self, table_type=BadIndexTable):
        self._tables = {}
        self._table_type = table_type

    async def list_tables(self):
        return []

    async def table_names(self):
        return []

    async def create_table(self, name, schema, storage_options=None, exist_ok=True):
        table = self._table_type(name, schema)
        self._tables[name] = table
        return table

    async def open_table(self, name):
        return self._tables[name]

    def close(self):
        pass


@pytest.mark.asyncio
async def test_lancedb_create_index_failure_propagates(monkeypatch, tmp_path):
    async def fake_connect_async(path):
        return IndexFailClient()

    monkeypatch.setattr(
        "archetype.core.aio.async_lancedb_store.lancedb.connect_async", fake_connect_async
    )
    config = StorageConfig(uri=str(tmp_path / "wh"), namespace="ns")
    uri = _resolve_uri(str(config.uri))
    store = AsyncLancedbStore(uri, config.namespace)

    sig = Archetype.sig_from_components([Demo(v=1)])
    # First operation that ensures the table should attempt index creation and fail
    with pytest.raises(RuntimeError, match="index failed"):
        await store.get_archetype_df(sig, world_id="w", run_id="r")


@pytest.mark.asyncio
async def test_lancedb_create_index_retries_runtime_conflict(monkeypatch, tmp_path):
    client = IndexFailClient(RetryableIndexTable)

    async def fake_connect_async(path):
        return client

    monkeypatch.setattr(
        "archetype.core.aio.async_lancedb_store.lancedb.connect_async", fake_connect_async
    )
    config = StorageConfig(uri=str(tmp_path / "wh"), namespace="ns")
    store = AsyncLancedbStore(_resolve_uri(str(config.uri)), config.namespace)
    sig = Archetype.sig_from_components([Demo(v=1)])

    await store._ensure_table(sig)

    table = client._tables[Archetype.get_name(sig)]
    assert table.calls == 5
