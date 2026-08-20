# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""MCP protocol conformance for the Mission MCP server (issue #810)."""

from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys

import pytest

from archetype.missions.mcp.server import (
    SUPPORTED_PROTOCOL_VERSIONS,
    TOOLS,
    MissionMcpServer,
)
from tests.missions.mcp.conftest import host_environ

EXPECTED_TOOL_NAMES = [
    "mission_submit",
    "mission_get",
    "mission_events",
    "mission_result",
    "mission_cancel",
    "mission_list",
]


def _request(method: str, request_id: int = 1, params: dict | None = None) -> dict:
    frame: dict = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        frame["params"] = params
    return frame


def test_initialize_echoes_a_supported_protocol_version(mcp_server):
    frame = mcp_server.handle_message(
        _request("initialize", params={"protocolVersion": "2025-03-26"})
    )
    result = frame["result"]
    assert result["protocolVersion"] == "2025-03-26"
    assert result["serverInfo"]["name"] == "archetype-missions-mcp"
    assert "tools" in result["capabilities"]


def test_initialize_answers_latest_supported_for_unknown_version(mcp_server):
    frame = mcp_server.handle_message(
        _request("initialize", params={"protocolVersion": "1999-01-01"})
    )
    assert frame["result"]["protocolVersion"] == SUPPORTED_PROTOCOL_VERSIONS[0]


def test_initialized_notification_produces_no_frame(mcp_server):
    assert (
        mcp_server.handle_message({"jsonrpc": "2.0", "method": "notifications/initialized"}) is None
    )


def test_unknown_notification_is_ignored_without_frame(mcp_server):
    assert (
        mcp_server.handle_message({"jsonrpc": "2.0", "method": "notifications/unknown-extension"})
        is None
    )


def test_ping_returns_empty_result(mcp_server):
    frame = mcp_server.handle_message(_request("ping"))
    assert frame == {"jsonrpc": "2.0", "id": 1, "result": {}}


def test_unsupported_method_returns_method_not_found(mcp_server):
    for method in ("resources/list", "prompts/list", "completion/complete"):
        frame = mcp_server.handle_message(_request(method))
        assert frame["error"]["code"] == -32601, method


def test_parse_error_returns_32700(mcp_server):
    frame = mcp_server.handle_line("{not json")
    assert frame["error"]["code"] == -32700
    assert frame["id"] is None


def test_invalid_request_returns_32600(mcp_server):
    frame = mcp_server.handle_message({"id": 5, "method": "ping"})
    assert frame["error"]["code"] == -32600


def test_unknown_tool_returns_invalid_params(mcp_server):
    frame = mcp_server.handle_message(
        _request("tools/call", params={"name": "mission_attach", "arguments": {}})
    )
    assert frame["error"]["code"] == -32602


def test_tools_list_is_exactly_the_six_mission_tools(mcp_server):
    frame = mcp_server.handle_message(_request("tools/list"))
    names = [tool["name"] for tool in frame["result"]["tools"]]
    assert names == EXPECTED_TOOL_NAMES


def test_no_interactive_attachment_tools_are_stubbed():
    """Issue #811 tools must be absent entirely, not stubbed as successful."""

    names = {tool["name"] for tool in TOOLS}
    for forbidden in ("attach", "steer", "takeover", "terminal", "viewport"):
        assert not any(forbidden in name for name in names), names


def test_every_tool_schema_rejects_unknown_properties():
    def closed(schema: dict) -> None:
        assert schema.get("additionalProperties") is False, schema
        for nested in schema.get("properties", {}).values():
            if nested.get("type") == "object":
                closed(nested)
            if nested.get("type") == "array" and isinstance(nested.get("items"), dict):
                if nested["items"].get("type") == "object":
                    closed(nested["items"])

    for tool in TOOLS:
        closed(tool["inputSchema"])


def test_serve_writes_protocol_frames_only_to_stdout(fake_control, host_config):
    lines = [
        json.dumps(_request("initialize", 1, {"protocolVersion": "2025-06-18"})),
        json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        json.dumps(_request("tools/list", 2)),
        "this line is not json",
        json.dumps(_request("ping", 3)),
    ]
    stdin = io.StringIO("\n".join(lines) + "\n")
    stdout = io.StringIO()
    server = MissionMcpServer(host_config, stderr=io.StringIO())
    assert server.serve(stdin, stdout) == 0
    frames = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [frame.get("id") for frame in frames] == [1, 2, None, 3]
    assert frames[2]["error"]["code"] == -32700
    assert all(frame["jsonrpc"] == "2.0" for frame in frames)


@pytest.mark.process
def test_stdio_subprocess_end_to_end(fake_control):
    """A real ``python -m archetype.missions.mcp`` process speaks MCP over stdio."""

    run = fake_control.seed_run(state="running", events=2)
    requests = [
        _request("initialize", 1, {"protocolVersion": "2025-06-18"}),
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        _request("tools/list", 2),
        _request(
            "tools/call",
            3,
            {"name": "mission_get", "arguments": {"run_id": run.run_id}},
        ),
        _request("ping", 4),
    ]
    env = dict(os.environ)
    env.update(host_environ(fake_control))
    process = subprocess.run(
        [sys.executable, "-m", "archetype.missions.mcp"],
        input="".join(json.dumps(frame) + "\n" for frame in requests),
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
    )
    assert process.returncode == 0, process.stderr
    frames = [json.loads(line) for line in process.stdout.splitlines()]
    assert [frame["id"] for frame in frames] == [1, 2, 3, 4]
    tool_result = frames[2]["result"]
    assert tool_result["isError"] is False
    payload = json.loads(tool_result["content"][0]["text"])
    assert payload["run_id"] == run.run_id
    assert fake_control.credential not in process.stdout
    assert fake_control.credential not in process.stderr


def test_declared_schemas_agree_with_runtime_validation():
    """Advertised inputSchema fragments and runtime validators render from one table."""

    from archetype.missions.mcp import server as server_module
    from archetype.missions.mcp.client import MissionToolError

    samples = {
        "opaque_id": ("run-1", "a/b"),
        "line": ("main", "bad\nline"),
        "limit": (5, 0),
        "tasks": (
            [{"name": "t", "prompt": "p"}],
            [{"name": "t", "prompt": "p", "sandbox": {}}],
        ),
    }
    for spec in server_module._TOOL_SPECS:
        schema = next(t for t in TOOLS if t["name"] == spec.name)["inputSchema"]
        for argument, kind, required in spec.arguments:
            fragment, validator = server_module._ARGUMENT_KINDS[kind]
            assert schema["properties"][argument] == fragment
            assert (argument in schema.get("required", [])) is required
            good, bad = samples[kind]
            validator(good, argument)  # schema-conforming values pass
            with pytest.raises(MissionToolError):
                validator(bad, argument)  # schema-violating values fail closed
            if "pattern" in fragment:
                assert re.fullmatch(fragment["pattern"], good)
                assert not re.fullmatch(fragment["pattern"], bad)


def test_oversized_frame_is_rejected_boundedly(host_config, monkeypatch):
    """A newline-less flood is discarded in bounded chunks and answered with
    a parse error; the loop keeps serving later frames."""

    from archetype.missions.mcp import server as server_module

    monkeypatch.setattr(server_module, "_MAX_FRAME_CHARS", 128)
    stdin = io.StringIO("x" * 300 + "\n" + json.dumps(_request("ping", 9)) + "\n")
    stdout = io.StringIO()
    server = MissionMcpServer(host_config, stderr=io.StringIO())
    assert server.serve(stdin, stdout) == 0
    frames = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert frames[0]["error"] == {"code": -32700, "message": "Frame too large"}
    assert frames[1] == {"jsonrpc": "2.0", "id": 9, "result": {}}
