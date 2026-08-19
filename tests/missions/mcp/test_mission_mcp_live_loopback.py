# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Binding seams left for issue #809 (recorded, not faked).

Two explicitly gated tests own the gap between the offline fake contract
in ``conftest.py`` and the real MissionRun REST surface:

* ``test_installed_rest_routes_match_the_adapter_contract`` skips while
  ``archetype.missions.api`` has no ``/v1/mission-runs`` routes and
  activates automatically once they land, so route drift fails CI.
* ``test_live_loopback_roundtrip`` is the live Archetype proof, skipped
  until a served loopback host with the issue #809 surface exists
  (repo convention: freeze dogfood runs as skipif tests).
"""

from __future__ import annotations

import io
import json
import os
import uuid

import pytest

from archetype.missions.mcp.config import McpHostConfig
from archetype.missions.mcp.server import MissionMcpServer

LOOPBACK_URL_ENV = "ARCHETYPE_MISSIONS_MCP_LOOPBACK_URL"
LOOPBACK_CREDENTIAL_ENV = "ARCHETYPE_MISSIONS_MCP_LOOPBACK_CREDENTIAL"
LOOPBACK_PROFILE_ENV = "ARCHETYPE_MISSIONS_MCP_LOOPBACK_PROFILE"
LOOPBACK_REPOSITORY_ENV = "ARCHETYPE_MISSIONS_MCP_LOOPBACK_REPOSITORY"

# The adapter's REST surface, exactly as documented in issue #809 plus the
# principal-scoped list route required by issue #810 (OPEN Q: #809 does not
# document a list route; the PR body records the GET-collection assumption).
ADAPTER_ROUTES = {
    ("POST", "/v1/mission-runs"),
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
    if not installed:
        pytest.skip(
            "Recorded seam (issue #810): archetype.missions.api does not ship "
            "/v1/mission-runs yet (issue #809 in flight). The offline fake in "
            "tests/missions/mcp/conftest.py carries the documented contract; "
            "when the routes land this test activates and any drift from the "
            "adapter's route set fails CI. Then bind FakeMissionControl's "
            "payloads to the real response models."
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
        "Issue #809 landed with a different MissionRun route set than the "
        f"MCP adapter targets; missing {sorted(missing)}. Update "
        "archetype/missions/mcp/client.py and rebind "
        "tests/missions/mcp/conftest.py to the real models."
    )


@pytest.mark.external
@pytest.mark.skipif(
    LOOPBACK_URL_ENV not in os.environ,
    reason=(
        "Live loopback Archetype proof pending issue #809: export "
        f"{LOOPBACK_URL_ENV}, {LOOPBACK_CREDENTIAL_ENV}, "
        f"{LOOPBACK_PROFILE_ENV}, and {LOOPBACK_REPOSITORY_ENV} against a "
        "served host exposing the MissionRun REST surface to run the "
        "submit -> get -> events -> cancel roundtrip through the MCP tools."
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
        frame = server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
        )
        result = frame["result"]
        return result["isError"], json.loads(result["content"][0]["text"])

    try:
        submit = {
            "profile_id": os.environ[LOOPBACK_PROFILE_ENV],
            "repository": os.environ[LOOPBACK_REPOSITORY_ENV],
            "ref": "main",
            "mission": "mcp-loopback-proof",
            "tasks": [
                {
                    "name": "noop-proof",
                    "prompt": "Report the repository name and stop.",
                    "validators": [{"name": "true", "argv": ["true"]}],
                }
            ],
            "idempotency_key": f"loopback-{uuid.uuid4().hex}",
        }
        is_error, accepted = tool("mission_submit", submit)
        assert is_error is False, accepted
        run_id = accepted["run_id"]

        is_error, duplicate = tool("mission_submit", submit)
        assert is_error is False and duplicate["run_id"] == run_id

        is_error, status = tool("mission_get", {"run_id": run_id})
        assert is_error is False and status["run_id"] == run_id

        is_error, events = tool("mission_events", {"run_id": run_id, "limit": 10})
        assert is_error is False and "events" in events

        is_error, cancelled = tool("mission_cancel", {"run_id": run_id})
        assert is_error is False
    finally:
        server.close()
