import pytest
import pytest_asyncio

from archetype.core.config import StorageConfig, WorldConfig, RunConfig
from archetype.core.aio import AsyncSystem
from archetype.core.component import Component
from archetype.core.orchestrator import WorldOrchestrator


class C1(Component):
    a: int
    s: str


class C2(Component):
    f: float
    b: bool


class C3(Component):
    by: bytes
    lst: list[int]


class Sub(Component):
    z: int


class C4(Component):
    nested: Sub
    tag: str


class C5(Component):
    note: str
    ok: bool


@pytest.mark.asyncio
async def test_get_components_matrix_union_and_projection(tmp_path):
    """Matrix test: 5 components, 5 archetypes, 5 entities each.

    Archetypes:
      - A1: (C1)
      - A2: (C2)
      - A3: (C1, C2)
      - A4: (C3, C4)
      - A5: (C1, C2, C3, C4, C5)

    Validates union and projection semantics of get_components across signatures, including nested and list/bytes types.
    """
    orch = WorldOrchestrator()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await orch.create_world(WorldConfig(name="w"), system=AsyncSystem(), storage_config=storage)

        a1_ids = []
        a2_ids = []
        a3_ids = []
        a4_ids = []
        a5_ids = []

        # A1: (C1)
        for i in range(5):
            eid = await world.create_entity([C1(a=i, s=f"c1-{i}")])
            a1_ids.append(eid)

        # A2: (C2)
        for i in range(5):
            eid = await world.create_entity([C2(f=float(i) + 0.5, b=(i % 2 == 0))])
            a2_ids.append(eid)

        # A3: (C1, C2)
        for i in range(5):
            eid = await world.create_entity([
                C1(a=100 + i, s=f"c1b-{i}"),
                C2(f=float(100 + i) + 0.25, b=(i % 2 == 1)),
            ])
            a3_ids.append(eid)

        # A4: (C3, C4)
        for i in range(5):
            eid = await world.create_entity([
                C3(by=bytes([i]), lst=[i, i + 1]),
                C4(nested=Sub(z=i), tag=f"tag-{i}"),
            ])
            a4_ids.append(eid)

        # A5: (C1, C2, C3, C4, C5)
        for i in range(5):
            eid = await world.create_entity([
                C1(a=1000 + i, s=f"c1c-{i}"),
                C2(f=float(2000 + i) + 0.75, b=(i % 2 == 0)),
                C3(by=b"x" + bytes([i]), lst=[10 + i]),
                C4(nested=Sub(z=10 + i), tag=f"A5-{i}"),
                C5(note=f"n-{i}", ok=True),
            ])
            a5_ids.append(eid)

        # Materialize spawns
        await world.run(RunConfig(num_steps=1))

        # 1) get_components([C1]) unions A1 + A3 + A5 => 15 rows; projects to C1 only
        df_c1 = await world.get_components([C1])
        assert set(["c1__a", "c1__s"]).issubset(set(df_c1.column_names))
        assert "c2__f" not in df_c1.column_names and "c3__by" not in df_c1.column_names
        assert df_c1.count_rows() == 15

        # 2) get_components([C1, C2]) => A3 + A5 => 10 rows; projects to C1 + C2
        df_c1c2 = await world.get_components([C1, C2])
        assert set(["c1__a", "c1__s", "c2__f", "c2__b"]).issubset(set(df_c1c2.column_names))
        assert df_c1c2.count_rows() == 10

        # 3) get_components([C4]) => A4 + A5 => 10 rows; includes nested struct
        df_c4 = await world.get_components([C4])
        assert set(["c4__nested", "c4__tag"]).issubset(set(df_c4.column_names))
        assert df_c4.count_rows() == 10

        # 4) get_components([C5]) => A5 => 5 rows
        df_c5 = await world.get_components([C5])
        assert set(["c5__note", "c5__ok"]).issubset(set(df_c5.column_names))
        assert df_c5.count_rows() == 5

        # 5) get_components([C2, C4]) => A5 => 5 rows; intersection of two different components
        df_c2c4 = await world.get_components([C2, C4])
        assert set(["c2__f", "c2__b", "c4__nested", "c4__tag"]).issubset(set(df_c2c4.column_names))
        assert df_c2c4.count_rows() == 5

        # 6) entity_ids filter: pick one from A1 and one from A5 for C1 query => 2 rows
        sample_ids = [a1_ids[0], a5_ids[0]]
        df_filtered = await world.get_components([C1], entity_ids=sample_ids)
        assert df_filtered.count_rows() == 2
        vals = [r["c1__a"] for r in df_filtered.select("c1__a").to_pylist()]
        assert set(vals).issubset({0, 1000})

    finally:
        await orch.shutdown()


