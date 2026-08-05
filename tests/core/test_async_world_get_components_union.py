import pytest

from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_harness


class A(Component):
    x: int


class B(Component):
    y: int


@pytest.mark.asyncio
async def test_get_components_unions_across_signatures_and_projects_schema(tmp_path):
    """get_components([A]) should union rows from all active archetypes that include A and project to A's schema; get_components([A,B]) should only return rows that include both."""
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.lifecycle.create_world(WorldConfig(name="w"), storage_config=storage)

        # Spawn across multiple signatures: (A), (A,B), (B), (A)
        await world.create_entity([A(x=1)])
        await world.create_entity([A(x=2), B(y=20)])
        await world.create_entity([B(y=3)])
        await world.create_entity([A(x=4)])

        # Materialize spawns
        await world.run(RunConfig(num_steps=1))

        # 1) Union of all archetypes containing A, projected to A's schema
        df_a = await world.get_components([A])
        # Only A's prefixed field should appear among component fields
        assert "a__x" in df_a.column_names
        assert "b__y" not in df_a.column_names
        # Expect entities that have A: three rows (1,2,4)
        vals = [r["a__x"] for r in df_a.select("a__x").to_pylist()]
        assert set(vals) == {1, 2, 4}

        # 2) Intersection: only rows that include both A and B
        df_ab = await world.get_components([A, B])
        assert set(["a__x", "b__y"]).issubset(set(df_ab.column_names))
        # Only the (A,B) entity should be present
        assert df_ab.count_rows() == 1
        row = df_ab.select("a__x", "b__y").to_pylist()[0]
        assert row["a__x"] == 2 and row["b__y"] == 20
    finally:
        await ws.close()
