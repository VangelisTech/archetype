# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Prefab libraries: author agent missions as assets, instantiate them as runs.

A factory run is not assembled procedurally from Python constructors — it is
*instantiated* from an explicit, inspectable asset graph. This example builds a
small software-factory library, then instantiates two concrete coding missions
from the one ``CodingMission`` prefab.

It demonstrates the whole of ``docs/design/prefab-library.md`` end to end:

- A prefab is a non-executing ``Prefab``-tagged entity graph (PD1); shared
  library assets (agents, validators, policies) are referenced, not copied.
- ``CodingMission`` owns role children (planner, implementer, reviewer,
  test_runner, completion_gate) with a stable ``PrefabNodeKey`` each (PD4).
- ``instantiate`` reserves the instance graph, remaps the *internal* edge
  (``completion_gate Observes test_runner``) onto the instance's own nodes,
  and *preserves* the shared edge (``reviewer Uses CodeReviewPolicy``) (PD7/PD8).
- Per-component ``InstantiationPolicy`` (PD6): ``MissionPolicy`` inherits,
  ``RetryCounter``/``MissionStatus`` reset, ``WorkingDirectory`` is omitted
  (factory-bound), and root overrides carry the instance-owned operational
  state (objective, repository, budget).
- ``IsA`` records lineage so the population stays queryable and gradeable, and
  live queries exclude the assets.

No LLM credentials are needed: this is pure ECS structure, so it runs in CI.
"""

import asyncio
from typing import ClassVar

from daft import col

from archetype import ArchetypeRuntime, Component, StorageConfig
from archetype.graph import (
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

# --- domain components -------------------------------------------------------


class Role(Component):
    """What a mission node is for."""

    name: str = ""


class MissionPolicy(Component):
    """Shared definition state: carried from the prefab to every instance."""

    on_instantiate: ClassVar[InstantiationPolicy] = InstantiationPolicy.INHERIT
    max_parallel_agents: int = 1
    require_tests: bool = True


class RetryCounter(Component):
    """Operational state: every instance starts fresh."""

    on_instantiate: ClassVar[InstantiationPolicy] = InstantiationPolicy.RESET
    attempts: int = 0


class MissionStatus(Component):
    """Lifecycle state: reset on instantiation."""

    on_instantiate: ClassVar[InstantiationPolicy] = InstantiationPolicy.RESET
    phase: str = "pending"


class WorkingDirectory(Component):
    """Factory-bound: never copied off the asset; a runtime processor binds it."""

    on_instantiate: ClassVar[InstantiationPolicy] = InstantiationPolicy.OMIT
    path: str = ""


class Budget(Component):
    """Instance-owned; supplied per run as an override."""

    tokens: int = 0


class Objective(Component):
    text: str = ""


class TargetRepository(Component):
    url: str = ""


# --- domain relations --------------------------------------------------------


class Uses(Relation):
    pass


class Requires(Relation):
    pass


class Observes(Relation):
    pass


async def main() -> None:
    storage = StorageConfig(uri="./archetype_data", namespace="prefabs")
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("prefabs", storage=storage)

        # 1. Spawn the shared library assets (tagged Prefab; referenced, not copied).
        implementer_agent = await world.spawn(Prefab(), Role(name="ImplementerAgent"))
        reviewer_policy = await world.spawn(Prefab(), Role(name="CodeReviewPolicy"))
        code_sandbox = await world.spawn(Prefab(), Role(name="CodeSandbox"))

        # 2. Author the CodingMission prefab: root state + role children + edges.
        template = PrefabTemplate(
            key="CodingMission",
            components=[
                MissionPolicy(max_parallel_agents=1, require_tests=True),
                MissionStatus(),
                RetryCounter(attempts=0),
                WorkingDirectory(path="<asset-placeholder>"),
            ],
            edges=[PrefabEdge(Requires, AssetRef(code_sandbox))],
            children=[
                PrefabNode("planner", [Role(name="planner")]),
                PrefabNode(
                    "implementer",
                    [Role(name="implementer")],
                    edges=[PrefabEdge(Uses, AssetRef(implementer_agent))],  # shared
                ),
                PrefabNode(
                    "reviewer",
                    [Role(name="reviewer")],
                    edges=[PrefabEdge(Uses, AssetRef(reviewer_policy))],  # shared
                ),
                PrefabNode("test_runner", [Role(name="test_runner")]),
                PrefabNode(
                    "completion_gate",
                    [Role(name="completion_gate")],
                    edges=[PrefabEdge(Observes, NodeRef("test_runner"))],  # internal
                ),
            ],
        )
        coding_mission = await define(world, template)
        await world.step()  # persist the library

        # 3. Instantiate two concrete missions from the one prefab.
        mission_a = await instantiate(
            world,
            coding_mission,
            overrides={
                Objective: Objective(text="Add retry to the client"),
                TargetRepository: TargetRepository(url="framework-zero/archetype"),
                Budget: Budget(tokens=500_000),
            },
        )
        mission_b = await instantiate(
            world,
            coding_mission,
            overrides={
                Objective: Objective(text="Fix flaky pagination test"),
                TargetRepository: TargetRepository(url="framework-zero/world-examples"),
                Budget: Budget(tokens=200_000),
            },
        )
        await world.step()  # persist the instances

        latest = (await world.info()).tick - 1

        # 4. Report. Lineage: every instance node IsA its authored node.
        isa = {
            (row["isa__source"], row["isa__target"])
            for row in (await edges(world, IsA, at=latest)).to_pylist()
        }
        print(f"IsA lineage edges: {len(isa)} (2 instances x 6 nodes)")
        print(
            f"  mission_a root {mission_a.root_id} IsA CodingMission {coding_mission.root_id}: "
            f"{(mission_a.root_id, coding_mission.root_id) in isa}"
        )

        # Internal edge remapped: each mission's completion_gate observes *its own*
        # test_runner, never the asset's.
        observes = {
            (row["observes__source"], row["observes__target"])
            for row in (await edges(world, Observes, at=latest)).to_pylist()
        }
        for tag, mission in (("A", mission_a), ("B", mission_b)):
            gate, runner = mission.node_ids["completion_gate"], mission.node_ids["test_runner"]
            print(
                f"  mission_{tag}: completion_gate {gate} Observes test_runner {runner}: "
                f"{(gate, runner) in observes}"
            )

        # Shared edge preserved: both instances' reviewers Use the one policy asset.
        uses = {
            (row["uses__source"], row["uses__target"])
            for row in (await edges(world, Uses, at=latest)).to_pylist()
        }
        shared_ok = all(
            (m.node_ids["reviewer"], reviewer_policy) in uses for m in (mission_a, mission_b)
        )
        print(f"  both reviewers Use shared CodeReviewPolicy {reviewer_policy}: {shared_ok}")

        # Policy: RESET gives fresh counters; OMIT keeps WorkingDirectory off instances.
        retry = await world.query(RetryCounter)
        retry_live = without_prefabs(retry.where(col("tick") == latest), await prefab_frame(world))
        attempts = {row["retrycounter__attempts"] for row in retry_live.to_pylist()}
        print(f"  instance RetryCounter.attempts (RESET): {attempts} (expect {{0}})")

        try:
            wd_ids = {row["entity_id"] for row in (await world.query(WorkingDirectory)).to_pylist()}
        except KeyError:
            wd_ids = set()
        instance_roots = {mission_a.root_id, mission_b.root_id}
        print(
            f"  WorkingDirectory (OMIT) never lands on an instance root: "
            f"{instance_roots.isdisjoint(wd_ids)}"
        )

        # Live query excludes the assets.
        roles = await world.query(Role)
        live = without_prefabs(roles.where(col("tick") == latest), await prefab_frame(world))
        live_roles = [row["role__name"] for row in live.to_pylist()]
        print(f"  live role entities: {len(live_roles)} (2 missions x 5 children, assets excluded)")


if __name__ == "__main__":
    asyncio.run(main())
