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


def build_context(tmp_path):
    # Minimal StorageContext with dummy catalog/session via factory path
    from archetype.core.runtime.storage import StorageContextFactory

    cfg = StorageConfig(uri=str(tmp_path / "store_fail"), namespace="ns")
    return StorageContextFactory.build(cfg)


@pytest.mark.asyncio
async def test_async_store_append_skips_on_empty_df(tmp_path):
    """AsyncStore.append should no-op for empty dataframes without raising errors."""
    ctx = build_context(tmp_path)
    store = AsyncStore(ctx)
    sig = Archetype.sig_from_components([Demo(v=1)])
    # empty df for schema
    a = Archetype.get_archetype_schema(sig)
    df = daft.from_arrow(__import__("pyarrow").Table.from_batches([], schema=a))
    await store.append(sig, df)  # should not raise


@pytest.mark.asyncio
async def test_async_store_append_handles_bad_df_gracefully(tmp_path, caplog):
    """AsyncStore.append should catch backend failures and log without raising."""
    ctx = build_context(tmp_path)
    store = AsyncStore(ctx)
    sig = Archetype.sig_from_components([Demo(v=1)])

    class BadDf:
        """A non-DataFrame object that will fail when table.append tries to use it."""

        pass

    with caplog.at_level("ERROR"):
        await store.append(sig, BadDf())
    assert any("Append failed" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_async_store_append_bad_schema_raises_and_is_caught(tmp_path, caplog):
    """AsyncStore.get_archetype_df/_ensure_table should create schema, but appending with incompatible schema should be handled by update path. Here we simulate a mismatch by passing wrong columns and verify updater logs error."""
    ctx = build_context(tmp_path)
    store = AsyncStore(ctx)
    updater = AsyncUpdateManager(store)
    sig = Archetype.sig_from_components([Demo(v=1)])

    # Build wrong schema table missing required columns
    import pyarrow as pa

    bad = daft.from_arrow(pa.Table.from_pylist([{"not_entity_id": 1}]))

    with caplog.at_level("ERROR"):
        # Should log an error from updater when append fails in backend
        await updater.update(bad, sig, tick=0, world_id="w", run_id="r")
    assert any("Error updating table" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_async_updater_logs_error_on_store_failure(tmp_path, caplog):
    """AsyncUpdateManager should catch store.append exceptions and log them, returning the original df."""
    ctx = build_context(tmp_path)
    store = FailingStore(ctx)
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
    # Force a failure path by raising in store.append
    with caplog.at_level("ERROR"):
        out = await updater.update(df, sig, tick=1, world_id="w", run_id="r")
    assert out.count_rows() == df.count_rows()
    assert any("Error updating table" in rec.message for rec in caplog.records)
