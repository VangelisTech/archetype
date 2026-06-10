import daft
import pytest

from archetype.core.aio.async_store import AsyncStore
from archetype.core.aio.async_updater import AsyncUpdateManager
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig


class Demo(Component):
    v: int


class FailingStore(AsyncStore):
    async def append(self, sig, df):  # type: ignore[override]
        raise RuntimeError("append failed")


def _build_session_and_config(tmp_path):
    from archetype.runtime.session import configure_session

    cfg = StorageConfig(uri=str(tmp_path / "store_fail"), namespace="ns")
    session = configure_session(cfg)
    return session, cfg


@pytest.mark.asyncio
async def test_async_store_append_skips_on_empty_df(tmp_path):
    """AsyncStore.append should no-op for empty dataframes without raising errors."""
    session, cfg = _build_session_and_config(tmp_path)
    store = AsyncStore(session, io_config=cfg.io_config)
    sig = Archetype.sig_from_components([Demo(v=1)])
    # empty df for schema
    a = Archetype.get_archetype_schema(sig)
    df = daft.from_arrow(__import__("pyarrow").Table.from_batches([], schema=a))
    await store.append(sig, df)  # should not raise


@pytest.mark.asyncio
async def test_async_store_append_raises_on_collect_failure(tmp_path, caplog):
    """A frame that cannot materialize must fail the append, not no-op.

    Persistence failure is observable to callers (specification.md, updater
    contracts): a swallowed collect failure would let a tick 'succeed' while
    persisting nothing.
    """
    session, cfg = _build_session_and_config(tmp_path)
    store = AsyncStore(session, io_config=cfg.io_config)
    sig = Archetype.sig_from_components([Demo(v=1)])

    class BadDf:
        def collect(self):
            raise RuntimeError("boom")

    with caplog.at_level("ERROR"):
        with pytest.raises(RuntimeError, match="boom"):
            await store.append(sig, BadDf())
    assert any("Append collect failed" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_async_updater_raises_on_bad_schema(tmp_path, caplog):
    """An append with an incompatible schema must raise out of the updater."""
    from daft.exceptions import DaftCoreException

    session, cfg = _build_session_and_config(tmp_path)
    store = AsyncStore(session, io_config=cfg.io_config)
    updater = AsyncUpdateManager(store)
    sig = Archetype.sig_from_components([Demo(v=1)])

    # Build wrong schema table missing required columns
    import pyarrow as pa

    bad = daft.from_arrow(pa.Table.from_pylist([{"not_entity_id": 1}]))

    with caplog.at_level("ERROR"):
        with pytest.raises(DaftCoreException):
            await updater.update(bad, sig, tick=0, world_id="w", run_id="r")
    assert any("Error updating table" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_async_updater_raises_on_store_failure(tmp_path, caplog):
    """AsyncUpdateManager must surface store.append failures to its caller.

    The old contract (log and return a stamped frame) made durability
    unobservable — a world could advance past a hole in its own history.
    """
    session, cfg = _build_session_and_config(tmp_path)
    store = FailingStore(session, io_config=cfg.io_config)
    updater = AsyncUpdateManager(store)
    sig = Archetype.sig_from_components([Demo(v=1)])
    schema = Archetype.get_archetype_schema(sig)
    df = daft.from_arrow(
        __import__("pyarrow").Table.from_pylist(
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
    )
    with caplog.at_level("ERROR"):
        with pytest.raises(RuntimeError, match="append failed"):
            await updater.update(df, sig, tick=1, world_id="w", run_id="r")
    assert any("Error updating table" in rec.message for rec in caplog.records)
