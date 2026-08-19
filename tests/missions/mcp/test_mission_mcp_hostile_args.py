# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Malicious-argument corpus: hostile tool arguments cannot escape the adapter.

Issue #810 acceptance: arguments cannot redirect the HTTP client, widen the
profile, traverse paths, inject headers, or expose credentials. Every
rejected argument must fail before a single request reaches the fake
server.
"""

from __future__ import annotations

import io
import json

import pytest

from archetype.missions.mcp.config import McpHostConfig
from archetype.missions.mcp.server import MissionMcpServer
from tests.missions.mcp.conftest import CREDENTIAL

HOSTILE_OPAQUE_IDS = [
    "../admin",
    "..%2F..%2Fsecrets",
    "runs/../../v1/principals",
    "a/b",
    "a?admin=true",
    "a#fragment",
    "a b",
    "a\r\nX-Injected: 1",
    "a\nAuthorization: Bearer stolen",
    "http://evil.example/v1/mission-runs",
    "",
    ".hidden",
    "-leading-dash",
    "a" * 300,
    "run\x00id",
    "run🚀id",
]

CONFIG_OVERRIDE_ARGUMENTS = [
    {"base_url": "http://evil.example"},
    {"url": "http://evil.example"},
    {"token": "attacker-token"},
    {"authorization": "Bearer stolen"},
    {"headers": {"Authorization": "Bearer stolen"}},
    {"path": "/v1/principals"},
    {"method": "DELETE"},
    {"backend": "modal"},
    {"profile": "admin"},
    {"credential_file": "/etc/passwd"},
]


@pytest.mark.parametrize("hostile", HOSTILE_OPAQUE_IDS)
def test_hostile_run_id_never_reaches_the_wire(call, fake_control, hostile):
    is_error, payload = call("mission_get", {"run_id": hostile})
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert fake_control.requests == []


@pytest.mark.parametrize("hostile", HOSTILE_OPAQUE_IDS)
def test_hostile_cursor_never_reaches_the_wire(call, fake_control, hostile):
    run = fake_control.seed_run(state="running", events=1)
    fake_control.requests.clear()
    is_error, payload = call("mission_events", {"run_id": run.run_id, "after": hostile})
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert fake_control.requests == []


@pytest.mark.parametrize(
    "hostile",
    ["../key", "key with space", "key\r\nX-Evil: 1", "", "k" * 300],
)
def test_hostile_idempotency_key_cannot_inject_headers(call, fake_control, hostile, submit_args):
    is_error, payload = call("mission_submit", submit_args(idempotency_key=hostile))
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert fake_control.requests == []


@pytest.mark.parametrize("limit", [0, -5, "10", 2.5, True, False, None])
def test_invalid_limit_is_rejected_before_the_wire(call, fake_control, limit):
    run = fake_control.seed_run(state="running")
    fake_control.requests.clear()
    is_error, payload = call("mission_events", {"run_id": run.run_id, "limit": limit})
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert fake_control.requests == []


@pytest.mark.parametrize("override", CONFIG_OVERRIDE_ARGUMENTS)
@pytest.mark.parametrize(
    "tool,base",
    [
        ("mission_get", {"run_id": "run-1"}),
        ("mission_events", {"run_id": "run-1"}),
        ("mission_result", {"run_id": "run-1"}),
        ("mission_cancel", {"run_id": "run-1"}),
        ("mission_list", {}),
    ],
)
def test_configuration_overrides_are_unknown_arguments(call, fake_control, tool, base, override):
    """A model can never supply a URL, credential, path, method, or backend."""

    is_error, payload = call(tool, {**base, **override})
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert "unknown argument" in payload["error"]["message"]
    assert fake_control.requests == []


@pytest.mark.parametrize("override", CONFIG_OVERRIDE_ARGUMENTS)
def test_submit_configuration_overrides_are_rejected(call, fake_control, override, submit_args):
    is_error, payload = call("mission_submit", {**submit_args(), **override})
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert fake_control.requests == []


@pytest.mark.parametrize(
    "task_widening",
    [
        {"sandbox": {"image": "attacker/image"}},
        {"env": {"AWS_SECRET_ACCESS_KEY": "x"}},
        {"model": "expensive-model"},
        {"driver": "raw-shell"},
        {"budget": 10**9},
    ],
)
def test_profile_widening_through_task_payload_is_rejected(
    call, fake_control, task_widening, submit_args
):
    """Task items carry name/prompt/validators/depends_on only (issue #808)."""

    tasks = [
        {
            "name": "fix-bug",
            "prompt": "Fix the reported bug.",
            **task_widening,
        }
    ]
    is_error, payload = call("mission_submit", submit_args(tasks=tasks))
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert fake_control.requests == []


def test_task_count_and_prompt_bytes_are_bounded(call, fake_control, submit_args):
    many = [{"name": f"task-{index}", "prompt": "p"} for index in range(33)]
    is_error, payload = call("mission_submit", submit_args(tasks=many))
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"

    huge_prompt = [{"name": "task", "prompt": "x" * 70000}]
    is_error, payload = call("mission_submit", submit_args(tasks=huge_prompt))
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert fake_control.requests == []


def test_a_redirect_response_is_an_error_and_is_never_followed(call, fake_control):
    run = fake_control.seed_run(state="running")
    fake_control.requests.clear()
    fake_control.redirect_next = True
    is_error, payload = call("mission_get", {"run_id": run.run_id})
    assert is_error is True
    assert payload["error"]["code"] == "protocol_error"
    assert "redirect" in payload["error"]["message"]
    assert len(fake_control.requests) == 1
    assert "evil-target" not in fake_control.requests[0].path


def test_upstream_failure_text_is_bounded_and_credential_free(call, fake_control):
    run = fake_control.seed_run(state="running")
    fake_control.requests.clear()
    fake_control.fail_next_status = 500
    fake_control.fail_next_detail = "boom " * 2000
    is_error, payload = call("mission_get", {"run_id": run.run_id})
    assert is_error is True
    assert payload["error"]["code"] == "upstream_error"
    assert len(payload["error"]["message"]) < 300
    assert CREDENTIAL not in json.dumps(payload)


def test_transport_failure_is_bounded_and_credential_free():
    config = McpHostConfig.from_env(
        {
            "ARCHETYPE_MISSIONS_MCP_URL": "http://127.0.0.1:9",
            "ARCHETYPE_MISSIONS_MCP_CREDENTIAL": CREDENTIAL,
            "ARCHETYPE_MISSIONS_MCP_TIMEOUT_SECONDS": "0.2",
        }
    )
    server = MissionMcpServer(config, stderr=io.StringIO())
    try:
        frame = server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "mission_get",
                    "arguments": {"run_id": "run-1"},
                },
            }
        )
    finally:
        server.close()
    result = frame["result"]
    assert result["isError"] is True
    text = result["content"][0]["text"]
    assert CREDENTIAL not in text
    assert json.loads(text)["error"]["code"] == "unavailable"


def test_stderr_diagnostics_redact_the_credential_and_stay_bounded(mcp_server, stderr_sink):
    mcp_server._diagnostics.emit(f"Authorization: Bearer {CREDENTIAL} " + "overflow " * 500)
    written = stderr_sink.getvalue()
    assert CREDENTIAL not in written
    assert "[redacted]" in written
    longest = max(len(line) for line in written.splitlines())
    assert longest <= 600


def test_upstream_body_echoing_the_credential_is_redacted(call, fake_control):
    run = fake_control.seed_run(state="running")
    fake_control.fail_next_status = 500
    fake_control.fail_next_detail = f"leaked Bearer {CREDENTIAL} in detail"
    is_error, payload = call("mission_get", {"run_id": run.run_id})
    assert is_error is True
    assert CREDENTIAL not in json.dumps(payload)
    assert "[redacted]" in payload["error"]["message"]


def test_deeply_nested_hostile_frame_is_a_parse_error(mcp_server):
    frame = mcp_server.handle_line("[" * 20000)
    assert frame["error"]["code"] == -32700
