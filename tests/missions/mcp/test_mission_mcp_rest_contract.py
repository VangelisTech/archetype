# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Offline contract tests: MCP tools over the documented issue #809 REST surface."""

from __future__ import annotations

import io
import json

from archetype.missions.mcp.config import McpHostConfig
from archetype.missions.mcp.server import MissionMcpServer
from tests.missions.mcp.conftest import host_environ, submit_arguments


def _tool(server: MissionMcpServer, name: str, arguments: dict | None = None):
    frame = server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments or {}},
        }
    )
    result = frame["result"]
    return result["isError"], json.loads(result["content"][0]["text"])


def test_submit_returns_run_id_before_completion(call, fake_control, submit_args):
    is_error, payload = call("mission_submit", submit_args())
    assert is_error is False
    assert payload["run_id"]
    assert payload["state"] == "queued"
    assert payload["request_digest"]
    assert payload["status_url"].endswith(payload["run_id"])
    # The fake never completes work: acceptance is decoupled from completion.
    assert fake_control.runs[payload["run_id"]].result is None


def test_duplicate_submit_after_process_death_returns_original_run(fake_control, submit_args):
    """A fresh MCP process reusing the idempotency key recovers the same run."""

    config = McpHostConfig.from_env(host_environ(fake_control))
    first = MissionMcpServer(config, stderr=io.StringIO())
    try:
        _, original = _tool(first, "mission_submit", submit_args())
    finally:
        first.close()

    second = MissionMcpServer(config, stderr=io.StringIO())
    try:
        is_error, recovered = _tool(second, "mission_submit", submit_args())
    finally:
        second.close()
    assert is_error is False
    assert recovered["run_id"] == original["run_id"]
    assert recovered["request_digest"] == original["request_digest"]


def test_submit_with_changed_content_conflicts(call, submit_args):
    call("mission_submit", submit_args())
    is_error, payload = call("mission_submit", submit_args(mission="different-mission"))
    assert is_error is True
    assert payload["error"]["code"] == "conflict"


def test_missing_credential_fails_closed(fake_control, submit_args):
    config = McpHostConfig.from_env({"ARCHETYPE_MISSIONS_MCP_URL": fake_control.base_url})
    server = MissionMcpServer(config, stderr=io.StringIO())
    try:
        is_error, payload = _tool(server, "mission_submit", submit_arguments())
    finally:
        server.close()
    assert is_error is True
    assert payload["error"]["code"] == "unauthenticated"


def test_get_returns_bounded_projection(call, fake_control):
    run = fake_control.seed_run(state="running")
    is_error, payload = call("mission_get", {"run_id": run.run_id})
    assert is_error is False
    assert payload == {
        "run_id": run.run_id,
        "profile_id": run.profile_id,
        "state": "running",
        "request_digest": "",
    }


def test_get_unknown_run_is_not_found(call):
    is_error, payload = call("mission_get", {"run_id": "run-does-not-exist"})
    assert is_error is True
    assert payload["error"]["code"] == "not_found"


def test_events_cursor_replay_has_no_gaps_or_duplicates(call, fake_control):
    run = fake_control.seed_run(state="running", events=7)
    _, first = call("mission_events", {"run_id": run.run_id, "limit": 3})
    assert [event["cursor"] for event in first["events"]] == ["1", "2", "3"]

    _, replay = call("mission_events", {"run_id": run.run_id, "limit": 3})
    assert replay == first

    cursor = first["next_cursor"]
    _, second = call("mission_events", {"run_id": run.run_id, "after": cursor, "limit": 3})
    assert [event["cursor"] for event in second["events"]] == ["4", "5", "6"]
    _, tail = call(
        "mission_events",
        {"run_id": run.run_id, "after": second["next_cursor"], "limit": 3},
    )
    assert [event["cursor"] for event in tail["events"]] == ["7"]
    seen = [event["event_id"] for page in (first, second, tail) for event in page["events"]]
    assert len(seen) == len(set(seen)) == 7


def test_events_past_the_end_returns_stable_cursor(call, fake_control):
    run = fake_control.seed_run(state="running", events=2)
    _, page = call("mission_events", {"run_id": run.run_id, "after": "2"})
    assert page["events"] == []
    assert page["next_cursor"] == "2"


def test_events_limit_is_clamped_to_the_host_page_bound(call, fake_control):
    run = fake_control.seed_run(state="running", events=3)
    is_error, _ = call("mission_events", {"run_id": run.run_id, "limit": 999999})
    assert is_error is False
    sent = fake_control.requests[-1].query["limit"]
    assert sent == ["100"]  # the McpHostConfig default page bound


def test_result_nonterminal_is_not_ready(call, fake_control):
    run = fake_control.seed_run(state="running")
    is_error, payload = call("mission_result", {"run_id": run.run_id})
    assert is_error is True
    assert payload["error"]["code"] == "not_ready"


def test_result_terminal_is_immutable(call, fake_control):
    run = fake_control.seed_run(
        state="completed", result={"status": "green", "commits": ["abc123"]}
    )
    _, first = call("mission_result", {"run_id": run.run_id})
    _, second = call("mission_result", {"run_id": run.run_id})
    assert first == second
    assert first["result"] == {"status": "green", "commits": ["abc123"]}


def test_cancel_is_idempotent(call, fake_control):
    run = fake_control.seed_run(state="running")
    is_error, first = call("mission_cancel", {"run_id": run.run_id})
    assert is_error is False
    assert first["state"] == "cancelling"
    is_error, second = call("mission_cancel", {"run_id": run.run_id})
    assert is_error is False
    assert second["state"] == "cancelling"


def test_cancel_completion_race_reports_the_committed_fact(call, fake_control):
    run = fake_control.seed_run(state="completed", result={"status": "green"})
    is_error, payload = call("mission_cancel", {"run_id": run.run_id})
    assert is_error is False
    assert payload["state"] == "completed"


def test_cancel_cannot_target_an_unauthorized_run(call, fake_control):
    foreign = fake_control.seed_run(state="running", foreign=True)
    is_error, payload = call("mission_cancel", {"run_id": foreign.run_id})
    assert is_error is True
    assert payload["error"]["code"] == "not_found"


def test_list_is_scoped_to_the_authenticated_principal(call, fake_control):
    own_one = fake_control.seed_run(state="running")
    own_two = fake_control.seed_run(state="queued")
    fake_control.seed_run(state="running", foreign=True)
    is_error, payload = call("mission_list", {})
    assert is_error is False
    listed = {run["run_id"] for run in payload["runs"]}
    assert listed == {own_one.run_id, own_two.run_id}


def test_oversized_payload_is_truncated_with_an_explicit_marker(fake_control):
    run = fake_control.seed_run(state="completed", result={"log": "x" * 5000})
    config = McpHostConfig.from_env(
        host_environ(fake_control, ARCHETYPE_MISSIONS_MCP_MAX_RESULT_BYTES="256")
    )
    server = MissionMcpServer(config, stderr=io.StringIO())
    try:
        is_error, payload = _tool(server, "mission_result", {"run_id": run.run_id})
    finally:
        server.close()
    assert is_error is False
    assert payload["truncated"] is True
    assert payload["limit_bytes"] == 256
    assert len(payload["content_prefix"].encode("utf-8")) <= 256
