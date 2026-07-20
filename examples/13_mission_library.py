# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dogfood: author the software factory's own missions as a prefab library.

``examples/12_prefabs.py`` shows the prefab mechanics on toy components. This
one dogfoods them on the *real* product domain — ``archetype.missions`` — to
make the point that agent missions are authored, not constructed:

- A ``CodingMission`` prefab carries a real ``Mission`` + ``TaskGate`` (the same
  components the mission state machine drives) plus its role topology and the
  shared agent / validator / sandbox assets it references.
- ``instantiate`` produces genuine, validator-passing mission rows — today's
  representation, unchanged — from per-run overrides (repo, branch, plan). The
  runtime is untouched: this is the additive authoring layer of the trade study
  (``docs/design/agent-missions-as-product-trade-study.md``, alternative B).
- The canonical ``CodingMission`` asset is ``Prefab``-tagged, so a live mission
  query excludes it: the factory never runs its own template as a mission.

No credentials are needed — pure ECS authoring — so it runs in CI.
"""

import asyncio
import json
from typing import Any

from daft import col

from archetype import ArchetypeRuntime, Component, StorageConfig
from archetype.graph import (
    AssetRef,
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
from archetype.missions import Mission, MissionStatus, TaskGate, TaskStatus


class Role(Component):
    """The role an authored mission node plays (planner, reviewer, ...)."""

    name: str = ""


class Uses(Relation):
    pass


class Requires(Relation):
    pass


class Observes(Relation):
    pass


def mission_overrides(
    *, name: str, repo: str, branch: str, plan: list[dict[str, Any]]
) -> dict[type[Component], Component]:
    """Per-run overrides that yield a valid READY mission whose gate mirrors step 0.

    This is the whole contract the runtime cares about: a ``Mission`` row and a
    ``TaskGate`` row consistent with ``plan[0]``. The prefab supplies topology
    and policy around it; the override supplies the operational state.
    """
    first = plan[0]
    return {
        Mission: Mission(
            name=name,
            repo=repo,
            branch=branch,
            plan_json=json.dumps(plan),
            status=MissionStatus.READY.value,
        ),
        TaskGate: TaskGate(
            step_index=0,
            step_name=first["name"],
            prompt=first["prompt"],
            validators_json=json.dumps(first["validators"]),
            max_attempts=first.get("max_attempts", 5),
            status=TaskStatus.READY.value,
        ),
    }


async def main() -> None:
    storage = StorageConfig(uri="./archetype_data", namespace="mission_library")
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("mission_library", storage=storage)

        # 1. Shared library assets — one copy each, referenced by every mission.
        implementer_agent = await world.spawn(Prefab(), Role(name="ImplementerAgent"))
        reviewer_agent = await world.spawn(Prefab(), Role(name="ReviewerAgent"))
        test_validator = await world.spawn(Prefab(), Role(name="UnitTestValidator"))
        code_sandbox = await world.spawn(Prefab(), Role(name="IsolatedContainerSandbox"))

        # 2. The CodingMission prefab: a real (placeholder) Mission + TaskGate at
        #    the root, the role topology as children, shared assets referenced.
        template = PrefabTemplate(
            key="CodingMission",
            components=[Mission(), TaskGate()],  # overridden per run; asset value is a placeholder
            edges=[
                PrefabEdge(Requires, AssetRef(code_sandbox))
            ],  # shared: mission requires a sandbox
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
                    edges=[PrefabEdge(Uses, AssetRef(reviewer_agent))],  # shared
                ),
                PrefabNode(
                    "test_runner",
                    [Role(name="test_runner")],
                    edges=[PrefabEdge(Uses, AssetRef(test_validator))],  # shared
                ),
                PrefabNode(
                    "completion_gate",
                    [Role(name="completion_gate")],
                    edges=[PrefabEdge(Observes, NodeRef("test_runner"))],  # internal
                ),
            ],
        )
        coding_mission = await define(world, template)
        await world.step()

        # 3. Instantiate two real missions from the one prefab.
        add_retry = await instantiate(
            world,
            coding_mission,
            overrides=mission_overrides(
                name="add-retry-to-client",
                repo="framework-zero/archetype",
                branch="agent/add-retry",
                plan=[
                    {
                        "name": "implement",
                        "prompt": "Add retry with backoff to the API client.",
                        "validators": [{"name": "tests", "command": ["pytest"]}],
                    },
                    {
                        "name": "review",
                        "prompt": "Review the retry implementation.",
                        "validators": [{"name": "lint", "command": ["ruff"]}],
                    },
                ],
            ),
        )
        fix_flaky = await instantiate(
            world,
            coding_mission,
            overrides=mission_overrides(
                name="fix-flaky-pagination",
                repo="framework-zero/world-examples",
                branch="agent/fix-flaky",
                plan=[
                    {
                        "name": "reproduce",
                        "prompt": "Reproduce the flaky pagination test.",
                        "validators": [
                            {"name": "tests", "command": ["pytest", "-k", "pagination"]}
                        ],
                    }
                ],
            ),
        )
        await world.step()

        latest = (await world.info()).tick - 1

        # 4a. The instantiated rows are genuine, live mission-domain entities — and
        #     the canonical CodingMission asset is NOT among them.
        missions = await world.query(Mission)
        live = without_prefabs(missions.where(col("tick") == latest), await prefab_frame(world))
        rows = {row["entity_id"]: row for row in live.to_pylist()}
        print(f"live missions: {len(rows)} (2 instances; the CodingMission asset is excluded)")
        for tag, inst in (("add_retry", add_retry), ("fix_flaky", fix_flaky)):
            row = rows[inst.root_id]
            plan = json.loads(row["mission__plan_json"])
            print(
                f"  {tag}: entity {inst.root_id} name={row['mission__name']!r} "
                f"repo={row['mission__repo']!r} status={row['mission__status']} "
                f"steps={len(plan)}"
            )

        # 4b. Each mission's TaskGate mirrors its plan's first step (valid to run).
        gates = await world.query(TaskGate)
        gate_rows = {
            r["entity_id"]: r
            for r in without_prefabs(
                gates.where(col("tick") == latest), await prefab_frame(world)
            ).to_pylist()
        }
        for tag, inst in (("add_retry", add_retry), ("fix_flaky", fix_flaky)):
            gate = gate_rows[inst.root_id]
            plan = json.loads(rows[inst.root_id]["mission__plan_json"])
            ok = (
                gate["taskgate__step_name"] == plan[0]["name"]
                and gate["taskgate__status"] == "ready"
            )
            print(f"  {tag}: TaskGate step={gate['taskgate__step_name']!r} mirrors plan[0]: {ok}")

        # 4c. Topology came along: the completion gate observes its own test_runner,
        #     and both missions' test_runners share the one UnitTestValidator asset.
        observes = {
            (r["observes__source"], r["observes__target"])
            for r in (await edges(world, Observes, at=latest)).to_pylist()
        }
        uses = {
            (r["uses__source"], r["uses__target"])
            for r in (await edges(world, Uses, at=latest)).to_pylist()
        }
        remapped = all(
            (inst.node_ids["completion_gate"], inst.node_ids["test_runner"]) in observes
            for inst in (add_retry, fix_flaky)
        )
        shared = all(
            (inst.node_ids["test_runner"], test_validator) in uses
            for inst in (add_retry, fix_flaky)
        )
        print(f"  internal completion_gate->test_runner remapped per mission: {remapped}")
        print(f"  both test_runners share UnitTestValidator {test_validator}: {shared}")

        # These invariants are the contract; fail the example in CI if they regress.
        assert len(rows) == 2
        assert remapped and shared
        assert all(
            gate_rows[inst.root_id]["taskgate__status"] == "ready"
            for inst in (add_retry, fix_flaky)
        )


if __name__ == "__main__":
    asyncio.run(main())
