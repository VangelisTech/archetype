import pytest

from archetype.app.services.storage_service import AsyncLancedbStore, _resolve_uri
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig


class Demo(Component):
    v: int


class BadIndexTable:
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    async def create_index(self, column, config, replace=True):
        raise RuntimeError("index failed")


class IndexFailClient:
    def __init__(self):
        self._tables = {}

    async def list_tables(self):
        return []

    async def table_names(self):
        return []

    async def create_table(self, name, schema, storage_options=None, exist_ok=True):
        return BadIndexTable(name, schema)

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
