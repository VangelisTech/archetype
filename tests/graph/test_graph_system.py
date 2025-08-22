import pytest
import pytest_asyncio

import daft
from daft import col, lit

from archetype.core.graph.graph_system import GraphSystem
from archetype.core.graph.graph_processor import GraphProcessor, UpdateStage


class P1(GraphProcessor):
    stage = UpdateStage.PreUpdate
    reads = []
    writes = [int]

    async def process(self, df, **kwargs):
        return df.with_column("v", (col("v") if "v" in df.column_names else lit(0)) + 1)


class P2(GraphProcessor):
    stage = UpdateStage.Update
    reads = [int]
    writes = [str]

    async def process(self, df, **kwargs):
        return df.with_column("s", (col("s") if "s" in df.column_names else lit("")) + lit("x"))


class P3(GraphProcessor):
    stage = UpdateStage.PostUpdate
    reads = [str]
    writes = []

    async def process(self, df, **kwargs):
        # No-op, but validates stage ordering reaches here
        return df


@pytest.mark.asyncio
async def test_graph_system_executes_stages_in_order_and_resolves_dependencies():
    """GraphSystem should execute processors by stages, infer dependencies, and produce expected columns/values."""
    sys = GraphSystem()
    sys.add_processor(P1())
    sys.add_processor(P2())
    sys.add_processor(P3())

    # Start with a single-row frame so column additions materialize values
    df = daft.from_pydict({"seed": [0]})
    out = await sys.execute(df)
    assert set(out.column_names) >= {"v", "s"}
    assert out.select("v").to_pylist()[0]["v"] == 1
    assert out.select("s").to_pylist()[0]["s"] == "x"


class C1(GraphProcessor):
    stage = UpdateStage.Update
    reads = []
    writes = [int]

    async def process(self, df, **kwargs):
        return df


class C2(GraphProcessor):
    stage = UpdateStage.Update
    reads = [int]
    writes = []

    async def process(self, df, **kwargs):
        return df


@pytest.mark.asyncio
async def test_graph_system_detects_circular_dependency():
    """A circular dependency in writes/reads should raise a RuntimeError."""
    sys = GraphSystem()
    # Force a cycle: C1 writes int; add another that writes int and reads it, creating mutual dependency via scan
    sys.add_processor(C1())

    class C3(GraphProcessor):
        stage = UpdateStage.Update
        reads = [int]
        writes = [int]

        async def process(self, df, **kwargs):
            return df

    sys.add_processor(C3())

    # The graph built for Update stage should be cyclic: C1 -> C3 and C3 -> C3 (self write/read implies edge); detect as non-DAG
    df = daft.from_pydict({})
    with pytest.raises(RuntimeError):
        await sys.execute(df)


