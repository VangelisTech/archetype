# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Route-drift enforcement and the live loopback proof (issues #809/#833).

The MissionRun REST surface shipped on ``main`` (issue #821), so the
route-set agreement test runs unconditionally: the adapter's six fixed
paths must exist in ``archetype.missions.api.create_router``. Body-level
schema agreement lives in ``test_mission_mcp_rest_contract.py``, which
validates the adapter's serialized submit payload directly against the
real ``MissionRunSubmitRequest`` model.

``test_live_loopback_roundtrip`` is the live Archetype proof (repo
convention: freeze dogfood runs as skipif tests). It stays gated on the
loopback environment variables because it needs a served host with a real
execution-profile catalog and principals file; the v0.6.3 dogfood receipt
records the composition recipe.
"""

from __future__ import annotations

import io
import os
import uuid

import pytest

from archetype.missions.mcp.config import McpHostConfig
from archetype.missions.mcp.server import MissionMcpServer
from tests.missions.mcp.conftest import call_tool

LOOPBACK_URL_ENV = "ARCHETYPE_MISSIONS_MCP_LOOPBACK_URL"
LOOPBACK_CREDENTIAL_ENV = "ARCHETYPE_MISSIONS_MCP_LOOPBACK_CREDENTIAL"
LOOPBACK_PROFILE_ENV = "ARCHETYPE_MISSIONS_MCP_LOOPBACK_PROFILE"
LOOPBACK_REPOSITORY_ENV = "ARCHETYPE_MISSIONS_MCP_LOOPBACK_REPOSITORY"
LOOPBACK_BRANCH_PREFIX_ENV = "ARCHETYPE_MISSIONS_MCP_LOOPBACK_BRANCH_PREFIX"

# The adapter's REST surface: the six fixed paths in
# archetype/missions/mcp/client.py, all shipped by issue #821.
ADAPTER_ROUTES = {
    ("POST", "/v1/mission-runs"),
    ("GET", "/v1/mission-runs"),
    ("GET", "/v1/mission-runs/{run_id}"),
    ("GET", "/v1/mission-runs/{run_id}/events"),
    ("GET", "/v1/mission-runs/{run_id}/result"),
    ("POST", "/v1/mission-runs/{run_id}/cancel"),
}


def _installed_mission_run_routes() -> set[tuple[str, str]]:
    from archetype.missions.api import create_router

    routes: set[tuple[str, str]] = set()
    for route in create_router().routes:
        path = getattr(route, "path", "")
        for method in getattr(route, "methods", ()) or ():
            if "/mission-runs" in path:
                routes.add((method, path))
    return routes


def test_installed_rest_routes_match_the_adapter_contract():
    installed = _installed_mission_run_routes()
    assert installed, (
        "archetype.missions.api no longer ships /v1/mission-runs routes; the "
        "MCP adapter in archetype/missions/mcp/client.py targets that "
        "surface (issues #809/#821/#833)."
    )
    missing = {
        (method, path)
        for method, path in ADAPTER_ROUTES
        if not any(
            method == installed_method and path == installed_path
            for installed_method, installed_path in installed
        )
    }
    assert not missing, (
        "The installed MissionRun route set drifted from the paths the MCP "
        f"adapter targets; missing {sorted(missing)}. Update "
        "archetype/missions/mcp/client.py and rebind "
        "tests/missions/mcp/conftest.py to the real models."
    )


@pytest.mark.external
@pytest.mark.skipif(
    LOOPBACK_URL_ENV not in os.environ,
    reason=(
        "Live loopback Archetype proof (frozen from the issue #833 fix "
        f"evidence): export {LOOPBACK_URL_ENV}, {LOOPBACK_CREDENTIAL_ENV}, "
        f"{LOOPBACK_PROFILE_ENV}, and {LOOPBACK_REPOSITORY_ENV} (optionally "
        f"{LOOPBACK_BRANCH_PREFIX_ENV}, default 'agent/') against a served "
        "host with a real execution-profile catalog to run the submit -> "
        "duplicate-submit -> get -> events -> cancel roundtrip through the "
        "MCP tools."
    ),
)
def test_live_loopback_roundtrip():
    config = McpHostConfig.from_env(
        {
            "ARCHETYPE_MISSIONS_MCP_URL": os.environ[LOOPBACK_URL_ENV],
            "ARCHETYPE_MISSIONS_MCP_CREDENTIAL": os.environ.get(LOOPBACK_CREDENTIAL_ENV, ""),
        }
    )
    server = MissionMcpServer(config, stderr=io.StringIO())

    def tool(name: str, arguments: dict) -> tuple[bool, dict]:
        return call_tool(server, name, arguments)

    try:
        unique = uuid.uuid4().hex[:12]
        prefix = os.environ.get(LOOPBACK_BRANCH_PREFIX_ENV, "agent/")
        submit = {
            "profile_id": os.environ[LOOPBACK_PROFILE_ENV],
            "repository": os.environ[LOOPBACK_REPOSITORY_ENV],
            "branch": f"{prefix}mcp-loopback-proof-{unique}",
            "base_ref": "main",
            "name": "mcp-loopback-proof",
            "tasks": [
                {
                    "name": "noop-proof",
                    "prompt": "Report the repository name and stop.",
                    "validators": [{"name": "true", "command": ["true"]}],
                }
            ],
            "idempotency_key": f"loopback-{unique}",
        }
        is_error, accepted = tool("mission_submit", submit)
        assert is_error is False, accepted
        run_id = accepted["run_id"]
        assert accepted["state"] == "accepted"
        assert accepted["profile"]["profile_id"] == submit["profile_id"]

        is_error, duplicate = tool("mission_submit", submit)
        assert is_error is False, duplicate
        assert duplicate["run_id"] == run_id

        is_error, status = tool("mission_get", {"run_id": run_id})
        assert is_error is False and status["run_id"] == run_id

        is_error, events = tool("mission_events", {"run_id": run_id, "limit": 10})
        assert is_error is False and "events" in events

        # Cancel immediately after acceptance to bound provider spend; the
        # committed-fact race semantics are the server's to resolve.
        is_error, cancelled = tool("mission_cancel", {"run_id": run_id})
        assert is_error is False, cancelled
    finally:
        server.close()
