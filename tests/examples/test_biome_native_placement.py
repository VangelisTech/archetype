# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the example-local native Biome placement bridge."""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest

_EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
if str(_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES))

from biome_agent import BiomeClient, FlecsRemoteError, TerrainCell  # noqa: E402
from biome_agent.bootstrap import (  # noqa: E402
    NATIVE_MODULE,
    _patch_main,
)
from biome_agent.client import NATIVE_PLACE_BUILDING  # noqa: E402


def test_bootstrap_registers_native_module_once() -> None:
    source = '#include "biome.h"\n\nint main(void) {\n    ECS_IMPORT(world, biomeUi);\n}\n'

    patched = _patch_main(source)

    assert patched.count("void archetypeBiomeImport(ecs_world_t *world);") == 1
    assert patched.count("ECS_IMPORT(world, archetypeBiome);") == 1
    assert _patch_main(patched) == patched


def test_native_module_delegates_purchase_placement_and_refund() -> None:
    source = NATIVE_MODULE.read_text()

    assert "biome_factory_purchase(world, prefab, 1)" in source
    assert "biomePlaceBuilding(" in source
    assert "biome_factory_refund(world, prefab, 1)" in source
    assert "ecs_lookup_child(world, parent, name)" in source
    assert "ecs_set_name(world, placed, name)" in source
    assert '.name = "placeBuilding"' in source
    assert ".return_type = ecs_id(ecs_i64_t)" in source


@pytest.mark.parametrize("entity_id", [1080, "1080"])
def test_client_calls_native_placement_with_typed_arguments(entity_id: int | str) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=entity_id)

    http = httpx.Client(
        base_url="http://biome.test",
        transport=httpx.MockTransport(handler),
    )
    client = BiomeClient("http://biome.test", client=http, allow_remote=True)

    placement = client.place_building(
        "buildings.DronePad",
        "biome.terrain.Terrain",
        TerrainCell(25, 24),
        name="agent_drone_pad",
    )

    assert placement.entity_id == 1080
    assert placement.entity_path == "scene.buildings.agent_drone_pad"
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET"
    assert request.url.path == NATIVE_PLACE_BUILDING
    assert dict(request.url.params) == {
        "prefab": "buildings.DronePad",
        "terrain": "biome.terrain.Terrain",
        "x": "25",
        "y": "24",
        "name": "agent_drone_pad",
    }


def test_client_rejects_injected_identifiers_before_rest_call() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("invalid identifiers must not cross the REST boundary")

    http = httpx.Client(
        base_url="http://biome.test",
        transport=httpx.MockTransport(handler),
    )
    client = BiomeClient("http://biome.test", client=http, allow_remote=True)

    with pytest.raises(ValueError, match="prefab"):
        client.place_building(
            "buildings.DronePad) || delete(*)",
            "biome.terrain.Terrain",
            TerrainCell(25, 24),
            name="agent_drone_pad",
        )

    with pytest.raises(ValueError, match="name"):
        client.place_building(
            "buildings.DronePad",
            "biome.terrain.Terrain",
            TerrainCell(25, 24),
            name="bad/name",
        )


def test_client_turns_native_zero_result_into_rejected_action() -> None:
    http = httpx.Client(
        base_url="http://biome.test",
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=0)),
    )
    client = BiomeClient("http://biome.test", client=http, allow_remote=True)

    with pytest.raises(FlecsRemoteError, match="rejected placement"):
        client.place_building(
            "buildings.DronePad",
            "biome.terrain.Terrain",
            TerrainCell(25, 24),
            name="agent_drone_pad",
        )
