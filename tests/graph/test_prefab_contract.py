# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the prefab family (stage 7).

Each test pins a claim from docs/design/prefab-library.md: instantiation is a
graph operation that remaps internal edges, preserves shared-asset edges,
applies per-component InstantiationPolicy, records IsA lineage, and never
mutates the prefab. Assets are excluded from live queries (PD1).
"""

from __future__ import annotations

import asyncio
import os
from typing import ClassVar

import pytest
from daft import col

os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("DO_NOT_TRACK", "1")

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.component import Component  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.graph import (  # noqa: E402
    AssetRef,
    InstantiationPolicy,
    IsA,
    NodeRef,
    Prefab,
    PrefabEdge,
    PrefabNode,
    PrefabTemplate,
    Relation,
    define,
    edges,
    instantiate,
    prefab_frame,
    without_prefabs,
)


class Role(Component):
    name: str = ""


class Budget(Component):
    tokens: int = 0


class RetryCounter(Component):
    on_instantiate: ClassVar[InstantiationPolicy] = InstantiationPolicy.RESET
    attempts: int = 0


class WorkingDirectory(Component):
    on_instantiate: ClassVar[InstantiationPolicy] = InstantiationPolicy.OMIT
    path: str = ""


class Observes(Relation):
    pass


class Uses(Relation):
    pass


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "prefab_data"), namespace="prefab_tests")


def _run(coro):
    return asyncio.run(coro)


def _template(shared_asset: int) -> PrefabTemplate:
    return PrefabTemplate(
        key="CodingMission",
        components=[
            Budget(tokens=100),
            RetryCounter(attempts=5),
            WorkingDirectory(path="/asset"),
        ],
        children=[
            PrefabNode("test_runner", [Role(name="test_runner")]),
            PrefabNode(
                "completion_gate",
                [Role(name="completion_gate")],
                edges=[PrefabEdge(Observes, NodeRef("test_runner"))],
            ),
            PrefabNode(
                "reviewer",
                [Role(name="reviewer")],
                edges=[PrefabEdge(Uses, AssetRef(shared_asset))],
            ),
        ],
    )


async def _library(world):
    shared = await world.spawn(Prefab(), Role(name="CodeReviewPolicy"))
    defined = await define(world, _template(shared))
    await world.step()
    return shared, defined


def test_instance_records_isa_lineage(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("lineage", storage=_storage(tmp_path))
            _shared, defined = await _library(world)
            instance = await instantiate(world, defined)
            await world.step()

            latest = (await world.info()).tick - 1
            lineage = {
                (row["isa__source"], row["isa__target"])
                for row in (await edges(world, IsA, at=latest)).to_pylist()
            }
            # Root and every child node point back to their authored node.
            assert (instance.root_id, defined.root_id) in lineage
            for key, node_id in instance.node_ids.items():
                assert (node_id, defined.asset_ids[key]) in lineage
            return True

    assert _run(go())


def test_internal_edge_is_remapped(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("remap", storage=_storage(tmp_path))
            _shared, defined = await _library(world)
            instance = await instantiate(world, defined)
            await world.step()

            latest = (await world.info()).tick - 1
            observes = {
                (row["observes__source"], row["observes__target"])
                for row in (await edges(world, Observes, at=latest)).to_pylist()
            }
            gate = instance.node_ids["completion_gate"]
            runner = instance.node_ids["test_runner"]
            # The instance gate observes the instance runner, not the asset's.
            assert (gate, runner) in observes
            assert (gate, defined.asset_ids["test_runner"]) not in observes
            return True

    assert _run(go())


def test_shared_edge_is_preserved(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("shared", storage=_storage(tmp_path))
            shared, defined = await _library(world)
            instance = await instantiate(world, defined)
            await world.step()

            latest = (await world.info()).tick - 1
            uses = {
                (row["uses__source"], row["uses__target"])
                for row in (await edges(world, Uses, at=latest)).to_pylist()
            }
            # The instance reviewer keeps the shared library target.
            assert (instance.node_ids["reviewer"], shared) in uses
            assert (defined.asset_ids["reviewer"], shared) in uses
            return True

    assert _run(go())


def test_instantiation_policy_reset_and_omit(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("policy", storage=_storage(tmp_path))
            _shared, defined = await _library(world)
            instance = await instantiate(world, defined)
            await world.step()

            latest = (await world.info()).tick - 1
            retry = (
                (await world.query(RetryCounter))
                .where(col("tick") == latest)
                .where(col("entity_id") == instance.root_id)
                .to_pylist()
            )
            assert retry[0]["retrycounter__attempts"] == 0  # RESET, asset had 5

            wd_ids = {row["entity_id"] for row in (await world.query(WorkingDirectory)).to_pylist()}
            assert instance.root_id not in wd_ids  # OMIT never lands on the instance
            assert defined.root_id in wd_ids  # but stays on the asset
            return True

    assert _run(go())


def test_override_wins_on_root(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("override", storage=_storage(tmp_path))
            _shared, defined = await _library(world)
            instance = await instantiate(world, defined, overrides={Budget: Budget(tokens=500_000)})
            await world.step()

            latest = (await world.info()).tick - 1
            budget = (
                (await world.query(Budget))
                .where(col("tick") == latest)
                .where(col("entity_id") == instance.root_id)
                .to_pylist()
            )
            assert budget[0]["budget__tokens"] == 500_000
            return True

    assert _run(go())


def test_assets_excluded_from_live_queries(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("exclude", storage=_storage(tmp_path))
            _shared, defined = await _library(world)
            await instantiate(world, defined)
            await world.step()

            latest = (await world.info()).tick - 1
            roles = (await world.query(Role)).where(col("tick") == latest)
            live = without_prefabs(roles, await prefab_frame(world)).to_pylist()
            names = sorted(row["role__name"] for row in live)
            # Only the instance's three role children; the asset roles are dropped.
            assert names == ["completion_gate", "reviewer", "test_runner"]
            return True

    assert _run(go())


def test_prefab_is_not_mutated_by_instantiation(tmp_path):
    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("immutable", storage=_storage(tmp_path))
            _shared, defined = await _library(world)
            await instantiate(world, defined, overrides={Budget: Budget(tokens=999)})
            await world.step()

            latest = (await world.info()).tick - 1
            budget = (
                (await world.query(Budget))
                .where(col("tick") == latest)
                .where(col("entity_id") == defined.root_id)
                .to_pylist()
            )
            # The asset still carries its authored value, not the override.
            assert budget[0]["budget__tokens"] == 100
            return True

    assert _run(go())


def test_instantiate_does_not_reject_missing_policy(tmp_path):
    """A component with no on_instantiate class var defaults to COPY."""

    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("default", storage=_storage(tmp_path))
            shared = await world.spawn(Prefab(), Role(name="shared"))
            template = PrefabTemplate(
                key="Thing",
                components=[Budget(tokens=42)],
                children=[PrefabNode("part", [Role(name="part")])],
            )
            _ = shared
            defined = await define(world, template)
            await world.step()
            instance = await instantiate(world, defined)
            await world.step()

            latest = (await world.info()).tick - 1
            budget = (
                (await world.query(Budget))
                .where(col("tick") == latest)
                .where(col("entity_id") == instance.root_id)
                .to_pylist()
            )
            assert budget[0]["budget__tokens"] == 42  # COPY default
            return True

    assert _run(go())


def test_duplicate_node_key_is_rejected(tmp_path):
    """Colliding node keys would corrupt edge remapping, so define fails loud (PD4)."""

    async def go():
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("dup", storage=_storage(tmp_path))
            template = PrefabTemplate(
                key="Root",
                children=[PrefabNode("Root", [Role(name="dup")])],
            )
            with pytest.raises(ValueError, match="duplicate node keys"):
                await define(world, template)
            return True

    assert _run(go())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
