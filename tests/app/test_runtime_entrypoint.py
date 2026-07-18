# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""@archetype.entrypoint — the script boundary as a decorator (R18)."""

from __future__ import annotations

from archetype import ArchetypeRuntime, SyncArchetypeRuntime, entrypoint, public_api
from archetype._api import is_public_api


def test_top_level_excludes_internal_service_wiring():
    import archetype

    internal = {
        "ServiceContainer",
        "CommandGateway",
        "WorldService",
        "SimulationService",
        "QueryService",
        "ArtifactTableService",
        "StorageService",
    }
    assert internal.isdisjoint(archetype.__all__)
    for name in internal:
        assert not hasattr(archetype, name)


def test_entrypoint_injects_sync_runtime_and_tears_down():
    seen: dict = {}

    @entrypoint()
    def main(runtime: SyncArchetypeRuntime, x: int) -> int:
        seen["runtime_type"] = type(runtime).__name__
        return x * 2

    assert main(21) == 42
    assert seen["runtime_type"] == "SyncArchetypeRuntime"


def test_entrypoint_injects_async_runtime():
    seen: dict = {}

    @entrypoint()
    async def amain(runtime: ArchetypeRuntime, x: int) -> int:
        seen["runtime_type"] = type(runtime).__name__
        seen["open"] = not runtime._closed
        return x + 1

    assert amain(41) == 42
    assert seen == {"runtime_type": "ArchetypeRuntime", "open": True}


def test_public_api_marker_registers():
    @public_api
    def sample() -> None: ...

    assert is_public_api(sample)
    assert not is_public_api(test_public_api_marker_registers)
