# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the live upstream-Biome agent bridge."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from uuid import UUID

import httpx
import pytest

from archetype import ArchetypeRuntime, StorageConfig

_EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
if str(_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES))

from biome_agent import (  # noqa: E402
    BiomeClient,
    BiomeEpisodeState,
    BiomeMission,
    BiomeMissionOutcome,
    BiomeObservation,
    DepositObservation,
    DrillObservation,
    ExtractionGoal,
    GoalDirectedDrillPolicy,
    MissionPlan,
    PlaceExtractorAction,
    TerrainCell,
    monitor_mission,
    run_durable_episode,
)
from biome_agent import bootstrap as biome_bootstrap  # noqa: E402
from biome_agent.bootstrap import (  # noqa: E402
    BIOME_HOST,
    BIOME_REVISION,
    FLECS_REF,
    FLECS_REVISION,
    MISSION_SCENE,
    BiomeCheckout,
    is_port_open,
    is_process_group_alive,
    launch,
    terminate,
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
    assert action.drill_path == "scene.buildings.agent_drill"


def test_deploy_purchases_upstream_prefabs_through_native_placement() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=1000 + len(requests))

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
        ("GET", "/call/archetype/biome/placeBuilding"),
        ("GET", "/call/archetype/biome/placeBuilding"),
    ]
    assert [request.url.params["prefab"] for request in requests] == [
        "buildings.Solar",
        "buildings.Drill",
    ]
    assert [request.url.params["name"] for request in requests] == [
        "agent_solar",
        "agent_drill",
    ]


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
            amount = (96, 96)[self.sample]
            return _deposit("mission.copper_site", "Copper", amount, 30, 24)

        def get_drill(self, _path: str, resource: str) -> DrillObservation:
            return DrillObservation(
                entity_id=7,
                entity_path="scene.buildings.agent_drill",
                powered=True,
                deposit_path="mission.copper_site",
                stored_resource=resource,
                stored_amount=4 if self.sample == 1 else 0,
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


def test_durable_episode_returns_and_reopens_native_and_tick_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _deposit("mission.copper_site", "Copper", 100, 30, 24)

    class NativeEpisodeClient:
        base_url = "http://127.0.0.1:27750"

        def __init__(self) -> None:
            self.deployed = False

        def observe(self) -> BiomeObservation:
            return BiomeObservation((target,), frozenset({target.cell}))

        def deploy(self, action: PlaceExtractorAction) -> None:
            assert action.target_path == target.entity_path
            self.deployed = True

        def get_deposit(self, entity_path: str) -> DepositObservation:
            assert self.deployed
            assert entity_path == target.entity_path
            return _deposit(entity_path, "Copper", 96, 30, 24)

        def get_drill(self, entity_path: str, resource: str) -> DrillObservation:
            assert self.deployed
            return DrillObservation(
                entity_id=7,
                entity_path=entity_path,
                powered=True,
                deposit_path=target.entity_path,
                stored_resource=resource,
                stored_amount=4,
            )

    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "catalog"))
    monkeypatch.delenv("ARCHETYPE_CONTROL_CATALOG_URL", raising=False)
    monkeypatch.delenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", raising=False)
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="biome_episode")
    result = run_durable_episode(
        NativeEpisodeClient(),  # type: ignore[arg-type]
        ExtractionGoal("Copper", 3),
        storage=storage,
        biome_revision=BIOME_REVISION,
        flecs_revision=FLECS_REVISION,
        timeout=1,
        poll_interval=0,
        world_name="biome-contract",
    )

    assert result.trace.success
    assert result.trace.extracted == 4
    assert result.biome_revision == BIOME_REVISION
    assert result.flecs_revision == FLECS_REVISION
    assert result.committed_tick == 3
    UUID(result.world_id)
    UUID(result.run_id)

    # A fresh runtime performs a cold read from the same physical storage. The
    # returned coordinates are useful release evidence only if this succeeds.
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.attach(result.world_id, storage=storage)
        info = world.info()
        rows = world.query(
            BiomeMission,
            BiomeEpisodeState,
            BiomeMissionOutcome,
            entity_ids=[result.episode_entity_id],
        ).to_pylist()

    assert str(info.run_id) == result.run_id
    assert info.tick == result.committed_tick
    assert len(rows) == 1
    row = rows[0]
    assert row["tick"] == result.committed_tick
    assert row["biomemission__biome_revision"] == BIOME_REVISION
    assert row["biomemission__flecs_revision"] == FLECS_REVISION
    assert row["biomeepisodestate__phase"] == "succeeded"
    assert row["biomeepisodestate__target_entity"] == target.entity_path
    assert row["biomeepisodestate__deposit_amount"] == 96
    assert row["biomeepisodestate__powered"] is True
    assert row["biomeepisodestate__stored_amount"] == 4
    assert row["biomemissionoutcome__success"] is True
    assert row["biomemissionoutcome__extracted"] == 4


def test_bootstrap_pins_the_compatible_public_flecs_branch_without_vendoring() -> None:
    assert len(BIOME_REVISION) == 40
    assert FLECS_REF == "script_await"
    assert FLECS_REVISION == "fd137d63deccded67aba4a0dd8a8a4231d24e897"
    scene = MISSION_SCENE.read_text()
    assert "include config/buildings" in scene
    assert "environment.CopperOre" in scene
    assert "mission_base : buildings.Base" in scene
    assert "agent_drill : buildings.Drill" not in scene


def test_checkout_fetches_the_exact_revision_without_a_mutable_provenance_ref(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout = tmp_path / "checkout"
    (checkout / ".git").mkdir(parents=True)
    repository = "https://example.invalid/upstream.git"
    revision = "1" * 40
    commands: list[list[str]] = []

    def record_run(command: list[str], *, cwd: Path | None = None) -> None:
        assert cwd == checkout
        if command[:3] == ["git", "fetch", "origin"]:
            raise AssertionError("mutable provenance refs must never be fetched")
        commands.append(command)

    def git_output(command: list[str], *, cwd: Path | None = None) -> str:
        assert cwd == checkout
        if command[1:4] == ["config", "--get", "remote.origin.url"]:
            return repository
        assert command[1:3] == ["rev-parse", "HEAD"]
        return revision

    monkeypatch.setattr(biome_bootstrap, "_run", record_run)
    monkeypatch.setattr(biome_bootstrap, "_output", git_output)

    biome_bootstrap._ensure_checkout(
        checkout,
        repository,
        revision,
    )

    assert commands == [
        ["git", "fetch", "--no-tags", "--depth=1", "origin", revision],
        ["git", "checkout", "--detach", revision],
    ]


def test_git_output_preserves_porcelain_status_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def porcelain_output(*_args, **_kwargs) -> str:
        return " M src/main.c\n"

    monkeypatch.setattr(biome_bootstrap.subprocess, "check_output", porcelain_output)

    assert biome_bootstrap._output(["git", "status"]) == " M src/main.c"


def test_launch_revalidates_exact_upstream_heads_and_starts_an_owned_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    biome = tmp_path / "biome"
    flecs = tmp_path / "flecs"
    build = biome / "build-agent"
    scene = biome / "etc" / "scenes" / MISSION_SCENE.name
    executable = build / "biome"
    main = biome / "src" / "main.c"
    native = biome / "src" / "modules" / biome_bootstrap.NATIVE_MODULE.name
    flecs.mkdir(parents=True)
    scene.parent.mkdir(parents=True)
    build.mkdir(parents=True)
    native.parent.mkdir(parents=True)
    scene.write_bytes(MISSION_SCENE.read_bytes())
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o755)
    pristine_main = '#include "biome.h"\n\nint main(void) {\n    ECS_IMPORT(world, biomeUi);\n}\n'
    main.write_text(biome_bootstrap._patch_main(pristine_main))
    native.write_bytes(biome_bootstrap.NATIVE_MODULE.read_bytes())
    checkout = BiomeCheckout(tmp_path, biome, flecs, build, executable, scene)

    revisions = {biome: BIOME_REVISION, flecs: FLECS_REVISION}
    statuses = {
        biome: "\n".join(
            (
                "?? build-agent/biome",
                "?? etc/scenes/archetype_agent.flecs",
                " M src/main.c",
                "?? src/modules/archetype_biome.c",
            )
        ),
        flecs: "",
    }

    def git_output(command, *, cwd=None):
        if command[1:3] == ["rev-parse", "HEAD"]:
            assert cwd is not None
            return revisions[cwd]
        assert command[1:3] == ["status", "--porcelain"]
        assert cwd is not None
        return statuses[cwd]

    monkeypatch.setattr(
        biome_bootstrap,
        "_output",
        git_output,
    )
    monkeypatch.setattr(
        biome_bootstrap,
        "_revision_file",
        lambda _checkout, _revision, _path: pristine_main,
    )
    monkeypatch.setattr(biome_bootstrap, "is_port_open", lambda *args, **kwargs: False)
    launched: list[tuple[list[str], dict[str, object]]] = []
    sentinel = object()

    def popen(command, **kwargs):
        launched.append((command, kwargs))
        return sentinel

    monkeypatch.setattr(biome_bootstrap.subprocess, "Popen", popen)

    assert launch(checkout) is sentinel
    assert launched == [
        (
            [str(executable), "--scene", "etc/scenes/archetype_agent.flecs"],
            {"cwd": biome, "start_new_session": True},
        )
    ]

    revisions[biome] = "0" * 40
    with pytest.raises(RuntimeError, match="expected exact pin"):
        launch(checkout)
    assert len(launched) == 1

    revisions[biome] = BIOME_REVISION
    statuses[biome] = " M src/unrelated.c"
    with pytest.raises(RuntimeError, match="unrelated source changes"):
        launch(checkout)
    assert len(launched) == 1


def test_terminate_closes_owned_descendants_and_listener(tmp_path: Path) -> None:
    with socket.socket() as reservation:
        reservation.bind((BIOME_HOST, 0))
        port = reservation.getsockname()[1]

    marker = tmp_path / "ready"
    child_source = """
import socket
import sys
import time
from pathlib import Path

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind((sys.argv[1], int(sys.argv[2])))
listener.listen()
Path(sys.argv[3]).write_text("ready")
time.sleep(60)
"""
    leader_source = """
import subprocess
import sys
import time

subprocess.Popen([sys.executable, "-c", sys.argv[1], *sys.argv[2:]])
time.sleep(60)
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            leader_source,
            child_source,
            BIOME_HOST,
            str(port),
            str(marker),
        ],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not marker.exists():
            time.sleep(0.05)
        assert marker.is_file()
        assert is_port_open(BIOME_HOST, port)

        terminate(
            process,
            host=BIOME_HOST,
            port=port,
            term_timeout=1,
            kill_timeout=2,
            port_timeout=2,
        )

        assert not is_process_group_alive(process.pid)
        assert not is_port_open(BIOME_HOST, port)
    finally:
        if is_process_group_alive(process.pid):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


def test_goal_and_action_reject_ambiguous_or_injected_values() -> None:
    with pytest.raises(ValueError, match="amount"):
        ExtractionGoal("Copper", 0)
    with pytest.raises(ValueError, match="drill_name"):
        PlaceExtractorAction(
            target_path="mission.copper",
            resource="resources.Copper",
            terrain="biome.terrain.Terrain",
            drill_cell=TerrainCell(1, 1),
            power_cell=TerrainCell(2, 1),
            drill_name="bad/name",
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
