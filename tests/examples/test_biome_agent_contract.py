# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the live upstream-Biome agent bridge."""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest

_EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
if str(_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES))

from biome_agent import (  # noqa: E402
    BiomeClient,
    BiomeObservation,
    DepositObservation,
    DrillObservation,
    ExtractionGoal,
    GoalDirectedDrillPolicy,
    MissionPlan,
    PlaceExtractorAction,
    TerrainCell,
    monitor_mission,
)
from biome_agent.bootstrap import (  # noqa: E402
    BIOME_REVISION,
    FLECS_REF,
    FLECS_REVISION,
    MISSION_SCENE,
)


def _deposit(
    path: str,
    resource: str,
    amount: int,
    x: int,
    y: int,
    entity_id: int = 1,
) -> DepositObservation:
    return DepositObservation(
        entity_id=entity_id,
        entity_path=path,
        resource=f"resources.{resource}",
        amount=amount,
        terrain="biome.terrain.Terrain",
        cell=TerrainCell(x, y),
    )


def test_client_reads_reflected_deposits_and_occupancy() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        expression = request.url.params["expr"]
        if expression.startswith("biome.miner.Deposit"):
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "parent": "mission",
                            "name": "copper_site",
                            "id": 42,
                            "fields": {
                                "values": [
                                    {"resource": "resources.Copper", "amount": 100000},
                                    {
                                        "terrain": "biome.terrain.Terrain",
                                        "x": 30,
                                        "y": 24,
                                    },
                                ]
                            },
                        }
                    ]
                },
            )
        assert expression.startswith("biome.buildings.Building")
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": 42,
                        "fields": {
                            "values": [
                                {"footprint": {"x": 0, "y": 0}},
                                {
                                    "terrain": "biome.terrain.Terrain",
                                    "x": 30,
                                    "y": 24,
                                },
                            ]
                        },
                    }
                ]
            },
        )

    http = httpx.Client(
        base_url="http://biome.test",
        transport=httpx.MockTransport(handler),
    )
    client = BiomeClient("http://biome.test", client=http, allow_remote=True)

    observation = client.observe()

    assert observation.deposits == (
        _deposit("mission.copper_site", "Copper", 100000, 30, 24, entity_id=42),
    )
    assert observation.occupied_cells == frozenset({TerrainCell(30, 24)})


def test_policy_selects_goal_resource_and_a_free_power_cell() -> None:
    observation = BiomeObservation(
        deposits=(
            _deposit("mission.iron", "Iron", 500000, 10, 10),
            _deposit("mission.copper", "Copper", 100000, 20, 20),
        ),
        occupied_cells=frozenset({TerrainCell(10, 10), TerrainCell(20, 20), TerrainCell(21, 20)}),
    )

    action = GoalDirectedDrillPolicy().choose(
        ExtractionGoal(resource="copper", amount=10),
        observation,
    )

    assert action.target_path == "mission.copper"
    assert action.drill_cell == TerrainCell(20, 20)
    assert action.power_cell == TerrainCell(20, 21)
    assert action.drill_path == "mission.agent_drill"


def test_deploy_composes_upstream_prefabs_instead_of_writing_native_state() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={})

    http = httpx.Client(
        base_url="http://biome.test",
        transport=httpx.MockTransport(handler),
    )
    client = BiomeClient("http://biome.test", client=http, allow_remote=True)
    action = PlaceExtractorAction(
        target_path="mission.copper_site",
        resource="resources.Copper",
        terrain="biome.terrain.Terrain",
        drill_cell=TerrainCell(30, 24),
        power_cell=TerrainCell(31, 24),
    )

    client.deploy(action)

    assert [(request.method, request.url.path) for request in requests] == [
        ("PUT", "/entity/archetype_agent_action"),
        ("PUT", "/script/archetype_agent_action"),
    ]
    code = requests[1].content.decode()
    assert "agent_drill : buildings.Drill" in code
    assert "agent_solar : buildings.Solar" in code
    assert "TerrainPosition" in code
    assert "PowerConsumer" not in code
    assert "biome.miner.Miner" not in code
    assert "biome.resources.Storage" not in code


def test_monitor_requires_native_power_targeting_and_deposit_delta() -> None:
    target = _deposit("mission.copper_site", "Copper", 100, 30, 24)
    action = PlaceExtractorAction(
        target_path=target.entity_path,
        resource=target.resource,
        terrain=target.terrain,
        drill_cell=target.cell,
        power_cell=TerrainCell(31, 24),
    )
    plan = MissionPlan(
        goal=ExtractionGoal("Copper", 3),
        observation=BiomeObservation((target,), frozenset({target.cell})),
        action=action,
    )

    class NativeState:
        def __init__(self) -> None:
            self.sample = -1

        def get_deposit(self, _path: str) -> DepositObservation:
            self.sample += 1
            amount = (100, 96)[self.sample]
            return _deposit("mission.copper_site", "Copper", amount, 30, 24)

        def get_drill(self, _path: str, resource: str) -> DrillObservation:
            powered = self.sample == 1
            return DrillObservation(
                entity_id=7,
                entity_path="mission.agent_drill",
                powered=powered,
                deposit_path="mission.copper_site",
                stored_resource=resource,
                stored_amount=4 if powered else 0,
            )

    clock_values = iter((0.0, 0.0, 0.1))
    trace = monitor_mission(
        NativeState(),  # type: ignore[arg-type]
        plan,
        timeout=1,
        poll_interval=0,
        clock=lambda: next(clock_values),
        sleep=lambda _seconds: None,
    )

    assert trace.success
    assert trace.extracted == 4
    assert len(trace.samples) == 2
    assert trace.final_sample is not None
    assert trace.final_sample.drill is not None
    assert trace.final_sample.drill.powered
    assert trace.final_sample.drill.stored_amount == 4


def test_bootstrap_pins_the_compatible_public_flecs_branch_without_vendoring() -> None:
    assert len(BIOME_REVISION) == 40
    assert FLECS_REF == "script_await"
    assert FLECS_REVISION == "fd137d63deccded67aba4a0dd8a8a4231d24e897"
    scene = MISSION_SCENE.read_text()
    assert "include config/buildings" in scene
    assert "environment.CopperOre" in scene
    assert "buildings.Drill" not in scene


def test_goal_and_action_reject_ambiguous_or_injected_values() -> None:
    with pytest.raises(ValueError, match="amount"):
        ExtractionGoal("Copper", 0)
    with pytest.raises(ValueError, match="script_name"):
        PlaceExtractorAction(
            target_path="mission.copper",
            resource="resources.Copper",
            terrain="biome.terrain.Terrain",
            drill_cell=TerrainCell(1, 1),
            power_cell=TerrainCell(2, 1),
            script_name="bad/name",
        )
    with pytest.raises(ValueError, match="terrain"):
        PlaceExtractorAction(
            target_path="mission.copper",
            resource="resources.Copper",
            terrain="terrain.Terrain\nInjected {}",
            drill_cell=TerrainCell(1, 1),
            power_cell=TerrainCell(2, 1),
        )
    with pytest.raises(ValueError, match="non-loopback"):
        BiomeClient("http://biome.example")
