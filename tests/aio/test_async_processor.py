import pytest
import pytest_asyncio

import daft
from daft import col, lit

from archetype.core.aio.async_processor import AsyncProcessor


@pytest.mark.asyncio
async def test_async_processor_default_process_identity():
    df = daft.from_pylist([
        {"x": 1, "y": 2},
        {"x": 3, "y": 4},
    ]).collect()

    p = AsyncProcessor()
    out = await p.process(df, ignored_kw=123)

    # Default implementation returns the same DataFrame unchanged
    assert out is df
    assert p.components == ()
    assert p.priority == 10


@pytest.mark.asyncio
async def test_async_processor_subclass_overrides_behavior():
    class ScaleX(AsyncProcessor):
        components = ()
        priority = 5

        async def process(self, df, **kwargs):
            return df.with_column("x", col("x") * lit(2))

    df = daft.from_pylist([
        {"x": 2},
        {"x": 5},
    ]).collect()

    p = ScaleX()
    out = await p.process(df)
    vals = [r["x"] for r in out.collect().to_pylist()]
    assert vals == [4, 10]
    assert p.priority == 5

