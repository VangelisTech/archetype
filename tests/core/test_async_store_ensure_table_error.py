import pytest
import pytest_asyncio

from archetype.core.aio.async_store import AsyncStore
from archetype.core.config import StorageConfig
from archetype.core.runtime.storage import StorageContextFactory
from archetype.core.archetype import Archetype
from archetype.core.component import Component


class Demo(Component):
    v: int


@pytest.mark.asyncio
async def test_async_store_ensure_table_create_failure(monkeypatch, tmp_path):
    """_ensure_table should wrap and raise an exception if table creation fails."""
    ctx = StorageContextFactory.build(StorageConfig(uri=str(tmp_path / "wh"), namespace="ns"))
    store = AsyncStore(ctx)

    def fail_create_table(name, source):
        raise RuntimeError("create table failed")

    monkeypatch.setattr(store.session, "create_table_if_not_exists", fail_create_table)
    sig = Archetype.sig_from_components([Demo(v=1)])
    with pytest.raises(Exception):
        await store.get_archetype_df(sig, world_id="w", run_id="r")


