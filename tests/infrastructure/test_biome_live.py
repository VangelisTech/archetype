# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Release-only evidence against exact upstream Biome and Flecs revisions."""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path
from uuid import UUID

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
    ExtractionGoal,
    run_durable_episode,
    wait_until_ready,
)
from biome_agent.bootstrap import (  # noqa: E402
    BIOME_HOST,
    BIOME_PORT,
    BIOME_REVISION,
    FLECS_REVISION,
    is_port_open,
    is_process_group_alive,
    launch,
    prepare,
    terminate,
)

_LIVE = os.environ.get("ARCHETYPE_BIOME_LIVE") == "1"

pytestmark = [
    pytest.mark.contract("missions.environment.pinned"),
    pytest.mark.integration,
    pytest.mark.external,
    pytest.mark.slow,
    pytest.mark.skipif(
        not _LIVE,
        reason="set ARCHETYPE_BIOME_LIVE=1 for pinned macOS Biome release evidence",
    ),
]


def _require_live_macos_host() -> None:
    assert platform.system() == "Darwin", "live Biome evidence requires macOS"
    missing = [name for name in ("git", "cmake", "cargo", "pkg-config") if not shutil.which(name)]
    assert not missing, f"live Biome evidence is missing required tools: {missing}"

    process_uid = os.geteuid()
    console_uid = os.stat("/dev/console").st_uid
    assert process_uid != 0 and console_uid == process_uid, (
        "live Biome evidence must run as the user who owns the active GUI session"
    )
    window_server = subprocess.run(
        ["/usr/bin/pgrep", "-x", "WindowServer"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert window_server.returncode == 0, "live Biome evidence requires WindowServer"

    profile = subprocess.run(
        ["/usr/sbin/system_profiler", "SPDisplaysDataType", "-json"],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    displays = json.loads(profile.stdout).get("SPDisplaysDataType", [])
    metal_values = [
        value
        for display in displays
        if isinstance(display, dict)
        for key, value in display.items()
        if key == "spdisplays_mtlgpufamilysupport" or key.startswith("spdisplays_metal")
    ]
    assert metal_values and all(
        "unsupported" not in str(value).casefold() for value in metal_values
    ), "live Biome evidence requires a Metal-capable GPU"
    online_displays = [
        driver
        for display in displays
        if isinstance(display, dict)
        for driver in display.get("spdisplays_ndrvs", [])
        if isinstance(driver, dict) and driver.get("spdisplays_online") == "spdisplays_yes"
    ]
    assert online_displays, "live Biome evidence requires an online display in the GUI session"


def test_pinned_biome_episode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Prove native control, cold durable evidence, and owned cleanup together."""

    _require_live_macos_host()
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "catalog"))
    monkeypatch.delenv("ARCHETYPE_CONTROL_CATALOG_URL", raising=False)
    monkeypatch.delenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", raising=False)

    checkout = prepare(tmp_path / "upstream")
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="biome_release")
    goal = ExtractionGoal(resource="Copper", amount=10)
    process = None
    process_group: int | None = None
    try:
        assert not is_port_open(), f"release host already has a listener on {BIOME_PORT}"
        process = launch(checkout)
        process_group = process.pid
        with BiomeClient() as client:
            assert wait_until_ready(client, process, timeout=60), (
                "pinned Biome exited or did not expose Flecs REST"
            )
            result = run_durable_episode(
                client,
                goal,
                storage=storage,
                biome_revision=BIOME_REVISION,
                flecs_revision=FLECS_REVISION,
                timeout=30,
                poll_interval=0.25,
                world_name="biome-release-evidence",
            )

        trace = result.trace
        sample = trace.final_sample
        assert trace.success, trace.reason
        assert sample is not None
        assert sample.drill is not None
        drill = sample.drill
        assert trace.extracted >= goal.amount
        assert trace.plan.target.amount - sample.deposit_amount == trace.extracted
        assert sample.deposit_amount < trace.plan.target.amount
        assert drill.powered is True
        assert drill.deposit_path == trace.plan.action.target_path
        assert drill.stored_resource == trace.plan.action.resource
        assert drill.stored_amount >= goal.amount
        assert result.biome_revision == BIOME_REVISION
        assert result.flecs_revision == FLECS_REVISION
        assert result.committed_tick == 3
        UUID(result.world_id)
        UUID(result.run_id)

        # Recompose the runtime after the writer is gone and read the exact
        # component row from durable storage. This prevents info-only evidence.
        with ArchetypeRuntime.sync() as runtime:
            world = runtime.attach(result.world_id, storage=storage)
            info = world.info()
            rows = world.query(
                BiomeMission,
                BiomeEpisodeState,
                BiomeMissionOutcome,
                entity_ids=[result.episode_entity_id],
            ).to_pylist()

        assert str(info.world_id) == result.world_id
        assert str(info.run_id) == result.run_id
        assert info.tick == result.committed_tick
        assert len(rows) == 1
        row = rows[0]
        assert row["tick"] == result.committed_tick
        assert row["biomemission__biome_revision"] == BIOME_REVISION
        assert row["biomemission__flecs_revision"] == FLECS_REVISION
        assert row["biomeepisodestate__phase"] == "succeeded"
        assert row["biomeepisodestate__target_entity"] == trace.plan.action.target_path
        assert row["biomeepisodestate__deposit_amount"] == sample.deposit_amount
        assert row["biomeepisodestate__extracted"] == trace.extracted
        assert row["biomeepisodestate__drill_entity"] == trace.plan.action.drill_path
        assert row["biomeepisodestate__powered"] is True
        assert row["biomeepisodestate__stored_amount"] == drill.stored_amount
        assert row["biomemissionoutcome__success"] is True
        assert row["biomemissionoutcome__extracted"] == trace.extracted
    finally:
        if process is not None:
            terminate(process)
        if process_group is not None:
            assert not is_process_group_alive(process_group), (
                f"Biome process group {process_group} survived live evidence"
            )
        assert not is_port_open(BIOME_HOST, BIOME_PORT), "Biome REST port survived live evidence"
