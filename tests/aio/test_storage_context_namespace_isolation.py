import daft
import pytest

from archetype.core.aio.async_store import AsyncStore
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.runtime.storage import StorageContextFactory


class Position(Component):
    x: int
    y: int


@pytest.mark.asyncio
async def test_contexts_are_isolated_by_namespace(tmp_path):
    uri = str(tmp_path)

    # Same URI, different namespaces
    ctx_a = StorageContextFactory.build(StorageConfig(uri=uri, namespace="nsA"))
    ctx_b = StorageContextFactory.build(StorageConfig(uri=uri, namespace="nsB"))

    store_a = AsyncStore(ctx_a)
    store_b = AsyncStore(ctx_b)

    try:
        # Prepare signature and a single row
        sig = Archetype.sig_from_components([Position(x=1, y=2)])
        world_id = "w"
        run_id_a = "rA"
        run_id_b = "rB"

        # Touch tables to ensure creation under each namespace
        _ = await store_a.get_archetype_df(sig, world_id=world_id, run_id=run_id_a)
        _ = await store_b.get_archetype_df(sig, world_id=world_id, run_id=run_id_b)

        # Append one row only to namespace A
        row = Archetype.to_row_dict(
            entity_id=1, tick=0, components=[Position(x=1, y=2)], world_id=world_id, run_id=run_id_a
        )
        df = daft.from_pylist([row]).collect()
        await store_a.append(sig, df)

        # Query from A sees 1 row
        out_a = await store_a.get_archetype_df(sig, world_id=world_id, run_id=run_id_a)
        assert out_a.collect().count_rows() == 1

        # Query from B with same world/run sees 0 rows
        out_b = await store_b.get_archetype_df(sig, world_id=world_id, run_id=run_id_a)
        assert out_b.collect().count_rows() == 0

    finally:
        await store_a.shutdown()
        await store_b.shutdown()
