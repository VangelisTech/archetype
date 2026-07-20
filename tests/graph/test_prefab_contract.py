# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for PreFabs (stage 7, design D5).

Each test pins a D5 claim: instantiation copies values with overrides and
records IsA lineage, the Prefab marker never copies, subtrees rebuild with
new ids, editing a prefab does not mutate existing instances, and the sync
flavor mirrors the async one.
"""

from __future__ import annotations

import asyncio
import os

import pytest

os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("DO_NOT_TRACK", "1")

from daft import col  # noqa: E402

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.component import Component  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.core.hooks import PostTick  # noqa: E402
from archetype.graph import (  # noqa: E402
    ChildOf,
    GraphView,
    IsA,
    Prefab,
    Relation,
    edges,
    instantiate,
    instantiate_sync,
    link,
)


class Chassis(Component):
    armor: int = 10
    color: str = "grey"


class Turret(Component):
    caliber: int = 30


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "prefab_data"), namespace="prefab_tests")


def _run(coro):
    return asyncio.run(coro)


def _world(runtime, name, tmp_path, view):
    return runtime.world(
        name,
        storage=_storage(tmp_path),
        resources=[view],
        hooks=[(PostTick, view.on_post_tick)],
    )


def test_instantiate_copies_overrides_and_records_lineage(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "copy", tmp_path, view)
            template = await world.spawn(Prefab(name="tank"), Chassis(armor=42))
            await world.step()

            inst = await instantiate(
                world, view, template, overrides=[Chassis(armor=42, color="red")]
            )
            await world.step()

            latest = (await world.info()).tick - 1
            rows = (await world.query(Chassis)).where(col("tick") == latest).to_pylist()
            by_id = {row["entity_id"]: row for row in rows}
            assert by_id[inst]["chassis__armor"] == 42
            assert by_id[inst]["chassis__color"] == "red"  # override applied

            # The Prefab marker was not copied: the instance is not a template.
            prefab_rows = (await world.query(Prefab)).where(col("tick") == latest).to_pylist()
            assert {row["entity_id"] for row in prefab_rows} == {template}

            lineage = (await edges(world, IsA, at=latest)).to_pylist()
            assert len(lineage) == 1
            assert lineage[0]["isa__source"] == inst
            assert lineage[0]["isa__target"] == template
            return True

    assert _run(go())


def test_subtree_instantiates_with_new_ids(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "subtree", tmp_path, view)
            body = await world.spawn(Prefab(name="body"), Chassis(armor=5))
            gun = await world.spawn(Prefab(name="gun"), Turret(caliber=88))
            await link(world, ChildOf(source=gun, target=body))
            await world.step()

            inst = await instantiate(world, view, body)
            await world.step()

            latest = (await world.info()).tick - 1
            child_edges = (await edges(world, ChildOf, at=latest)).to_pylist()
            new_wiring = [row for row in child_edges if row["childof__target"] == inst]
            assert len(new_wiring) == 1
            new_gun = new_wiring[0]["childof__source"]
            assert new_gun not in {body, gun, inst}

            turrets = (await world.query(Turret)).where(col("tick") == latest).to_pylist()
            by_id = {row["entity_id"]: row for row in turrets}
            assert by_id[new_gun]["turret__caliber"] == 88

            lineage = (await edges(world, IsA, at=latest)).to_pylist()
            assert {(row["isa__source"], row["isa__target"]) for row in lineage} == {
                (inst, body),
                (new_gun, gun),
            }
            return True

    assert _run(go())


def test_editing_prefab_does_not_mutate_instances(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "upgrade", tmp_path, view)
            template = await world.spawn(Prefab(name="mk"), Chassis(armor=1))
            await world.step()

            first = await instantiate(world, view, template)
            await world.step()

            await world.update(template, Chassis(armor=99))
            await world.step()  # one step: the edit persists and the view captures it

            second = await instantiate(world, view, template)
            await world.step()

            latest = (await world.info()).tick - 1
            rows = (await world.query(Chassis)).where(col("tick") == latest).to_pylist()
            by_id = {row["entity_id"]: row["chassis__armor"] for row in rows}
            assert by_id[first] == 1  # copy-on-instantiate: old instance untouched
            assert by_id[second] == 99  # re-instantiation is the upgrade path
            return True

    assert _run(go())


def test_missing_prefab_raises(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "missing", tmp_path, view)
            await world.spawn(Prefab(name="x"), Chassis())
            await world.step()
            with pytest.raises(LookupError):
                await instantiate(world, view, 424242)
            return True

    assert _run(go())


def test_sync_instantiate_parity(tmp_path):
    view = GraphView()
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world(
            "sync-prefab",
            storage=_storage(tmp_path),
            resources=[view],
            hooks=[(PostTick, view.on_post_tick_sync)],
        )
        template = world.spawn(Prefab(name="t"), Chassis(armor=7))
        world.step()

        inst = instantiate_sync(world, view, template)
        world.step()

        latest = world.info().tick - 1
        rows = world.query(Chassis).where(col("tick") == latest).to_pylist()
        by_id = {row["entity_id"]: row["chassis__armor"] for row in rows}
        assert by_id[inst] == 7


def _make_chassis_twin() -> type[Component]:
    class Chassis(Component):
        armor: int = 10
        color: str = "grey"

    return Chassis


def test_override_matches_schema_identical_twin(tmp_path):
    """An override from a schema-identical twin class replaces the copied
    component instead of colliding with it — the cold-resume rule."""

    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "twin-override", tmp_path, view)
            template = await world.spawn(Prefab(name="t"), Chassis(armor=3))
            await world.step()

            twin = _make_chassis_twin()
            inst = await instantiate(world, view, template, overrides=[twin(armor=77)])
            await world.step()

            latest = (await world.info()).tick - 1
            rows = (await world.query(Chassis)).where(col("tick") == latest).to_pylist()
            by_id = {row["entity_id"]: row["chassis__armor"] for row in rows}
            assert by_id[inst] == 77
            return True

    assert _run(go())


def _make_prefab_twin() -> type[Component]:
    class Prefab(Component):
        name: str = ""

    return Prefab


def test_twin_prefab_marker_is_not_copied():
    """A schema-identical twin Prefab marker (cold resume) is excluded from
    the copy exactly like the real one — instances never become templates."""
    import daft

    from archetype.graph.prefab import _entity_components

    twin_marker = _make_prefab_twin()
    signature = tuple(sorted((twin_marker, Chassis), key=lambda t: t.__name__))
    frame = daft.from_pydict(
        {
            "entity_id": [7],
            "tick": [0],
            "prefab__name": ["ghost"],
            "chassis__armor": [12],
            "chassis__color": ["blue"],
        }
    )
    view = GraphView()
    view._frames = {signature: frame}
    view.tick = 0

    components = _entity_components(view, 7)
    assert [type(c).__name__ for c in components] == ["Chassis"]
    assert components[0].armor == 12


def test_marker_and_relation_overrides_are_rejected(tmp_path):
    """An override that could smuggle a marker or relation onto an instance
    fails loudly instead of silently minting a second template."""

    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "smuggle", tmp_path, view)
            template = await world.spawn(Prefab(name="t"), Chassis(armor=1))
            await world.step()

            with pytest.raises(ValueError, match="not copyable"):
                await instantiate(world, view, template, overrides=[Prefab(name="oops")])
            with pytest.raises(ValueError, match="not copyable"):
                await instantiate(world, view, template, overrides=[IsA(source=1, target=2)])
            twin_marker = _make_prefab_twin()
            with pytest.raises(ValueError, match="not copyable"):
                await instantiate(world, view, template, overrides=[twin_marker(name="ghost")])
            return True

    assert _run(go())


def test_lineage_is_version_pinned_and_same_world_is_empty(tmp_path):
    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "pinned", tmp_path, view)
            template = await world.spawn(Prefab(name="t"), Chassis(armor=4))
            await world.step()
            pinned_tick = view.tick

            inst = await instantiate(world, view, template)
            await world.step()

            latest = (await world.info()).tick - 1
            lineage = (await edges(world, IsA, at=latest)).to_pylist()
            assert len(lineage) == 1
            assert lineage[0]["isa__source"] == inst
            assert lineage[0]["isa__at_tick"] == pinned_tick
            assert lineage[0]["isa__world"] == ""  # same-world lineage
            return True

    assert _run(go())


def test_cross_world_instantiation_records_source_world(tmp_path):
    """R1's seam plus R2's payload: import from a library world and keep
    globally unambiguous lineage."""

    async def go():
        library_view = GraphView()
        async with ArchetypeRuntime() as runtime:
            library = runtime.world(
                "library",
                storage=_storage(tmp_path),
                resources=[library_view],
                hooks=[(PostTick, library_view.on_post_tick)],
            )
            consumer = runtime.world("consumer", storage=_storage(tmp_path))

            template = await library.spawn(Prefab(name="unit"), Chassis(armor=13))
            await library.step()

            inst = await instantiate(consumer, library_view, template)
            await consumer.step()

            latest = (await consumer.info()).tick - 1
            rows = (await consumer.query(Chassis)).where(col("tick") == latest).to_pylist()
            assert {r["entity_id"]: r["chassis__armor"] for r in rows}[inst] == 13

            lineage = (await edges(consumer, IsA, at=latest)).to_pylist()
            assert len(lineage) == 1
            library_id = str((await library.info()).world_id)
            assert lineage[0]["isa__world"] == library_id
            assert lineage[0]["isa__target"] == template  # id valid IN that world
            return True

    assert _run(go())


class Wired(Relation):
    """Arbitrary domain relation for the negative contract."""


def test_arbitrary_relations_are_not_copied(tmp_path):
    """The relation-copy boundary (R7): only ChildOf wiring is rebuilt; any
    other relation between prefab entities stays on the originals."""

    async def go():
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            world = _world(runtime, "boundary", tmp_path, view)
            body = await world.spawn(Prefab(name="body"), Chassis(armor=1))
            gun = await world.spawn(Prefab(name="gun"), Turret(caliber=20))
            await link(world, ChildOf(source=gun, target=body))
            await link(world, Wired(source=body, target=gun))
            await world.step()

            inst = await instantiate(world, view, body)
            await world.step()

            latest = (await world.info()).tick - 1
            new_ids = {inst} | {
                row["childof__source"]
                for row in (await edges(world, ChildOf, at=latest)).to_pylist()
                if row["childof__target"] == inst
            }
            wired = (await edges(world, Wired, at=latest)).to_pylist()
            assert len(wired) == 1  # exactly the original edge
            touched = {wired[0]["wired__source"], wired[0]["wired__target"]}
            assert touched == {body, gun}
            assert not (touched & new_ids)
            return True

    assert _run(go())


def test_instantiation_is_pinned_against_a_ticking_source(tmp_path):
    """A source world capturing new ticks mid-instantiation must not split
    the copy across versions: every lineage row carries the snapshot's tick
    and children resolve from the snapshot's frames."""
    import daft

    from archetype.core.hooks import PostTick
    from archetype.graph import ChildOf, link  # noqa: F401  (link exercised via instantiate)

    prefab_sig = tuple(sorted((Prefab, Chassis), key=lambda t: t.__name__))
    prefab_frame = daft.from_pydict(
        {
            "entity_id": [1, 2],
            "tick": [0, 0],
            "is_active": [True, True],
            "prefab__name": ["body", "gun"],
            "chassis__armor": [5, 7],
            "chassis__color": ["grey", "grey"],
        }
    )
    edge_frame = daft.from_pydict(
        {
            "entity_id": [5],
            "tick": [0],
            "is_active": [True],
            "childof__source": [2],
            "childof__target": [1],
        }
    )
    view = GraphView()
    view._frames = {prefab_sig: prefab_frame, (ChildOf,): edge_frame}
    view.tick = 0
    view.world_id = "library-world"

    class _TickingWorld:
        """Every spawn advances the live view — the mid-instantiation race."""

        def __init__(self):
            self.spawned = []
            self._next = 100

        async def info(self):
            class _Info:
                tick = 1
                world_id = "consumer-world"

            return _Info()

        async def query(self, *_components):
            # Exclusive-link replacement legitimately queries the TARGET
            # world; a fresh consumer has no edge tables yet.
            raise KeyError("no table in consumer world")

        async def spawn(self, *components) -> int:
            self.spawned.append(components)
            view._capture(PostTick(world_id="library-world", tick=view.tick + 2, results={}))
            self._next += 1
            return self._next

        async def despawn(self, entity_id: int) -> None:
            raise AssertionError("no despawns during instantiation")

    fake = _TickingWorld()
    _run(instantiate(fake, view, 1))

    lineage = [c for batch in fake.spawned for c in batch if isinstance(c, IsA)]
    assert len(lineage) == 2  # root and child both copied despite the racing view
    assert {edge.at_tick for edge in lineage} == {0}  # one pinned version
    assert {edge.world for edge in lineage} == {"library-world"}


def test_cascade_never_reaps_cross_world_lineage(tmp_path):
    """A cross-world IsA target is out of local-liveness jurisdiction: the
    generic cascade removes genuinely dangling LOCAL lineage while foreign
    lineage survives untouched."""
    from archetype.graph import cascade

    async def go():
        library_view = GraphView()
        view = GraphView()
        async with ArchetypeRuntime() as runtime:
            library = runtime.world(
                "reap-library",
                storage=_storage(tmp_path),
                resources=[library_view],
                hooks=[(PostTick, library_view.on_post_tick)],
            )
            consumer = runtime.world(
                "reap-consumer",
                storage=_storage(tmp_path),
                resources=[view],
                hooks=[(PostTick, view.on_post_tick)],
            )
            lib_template = await library.spawn(Prefab(name="imported"), Chassis(armor=9))
            await library.step()

            local_template = await consumer.spawn(Prefab(name="local"), Chassis(armor=2))
            await consumer.step()

            imported = await instantiate(consumer, library_view, lib_template)
            local_inst = await instantiate(consumer, view, local_template)
            await consumer.step()

            # Kill the LOCAL template: its lineage edge genuinely dangles.
            await consumer.despawn(local_template)
            await consumer.step()

            result = await cascade(consumer, IsA, view)
            await consumer.step()

            latest = (await consumer.info()).tick - 1
            lineage = (await edges(consumer, IsA, at=latest)).to_pylist()
            survivors = {(r["isa__source"], r["isa__world"]) for r in lineage}
            # The cross-world edge survives; the dangling local one is gone.
            library_id = str((await library.info()).world_id)
            assert (imported, library_id) in survivors
            assert all(src != local_inst for src, _ in survivors)
            assert result.total >= 1
            return True

    assert _run(go())
