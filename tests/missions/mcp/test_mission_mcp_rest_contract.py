# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Offline contract tests: MCP tools over the shipped MissionRun REST surface."""

from __future__ import annotations

import io
import json

import pytest

from archetype.missions.api import (
    MissionRunSubmitRequest,
    MissionRunTaskRequest,
    MissionRunValidatorRequest,
)
from archetype.missions.mcp.config import McpHostConfig, McpHostConfigError
from archetype.missions.mcp.server import MissionMcpServer
from tests.missions.mcp.conftest import call_tool, host_environ, submit_arguments


def test_submit_returns_run_id_before_completion(call, fake_control):
    is_error, payload = call("mission_submit", submit_arguments())
    assert is_error is False
    assert payload["run_id"]
    assert payload["state"] == "accepted"
    assert payload["request_digest"]
    assert payload["profile"]["profile_id"] == "profile-default"
    assert payload["status_url"].endswith(payload["run_id"])
    # The fake never completes work: acceptance is decoupled from completion.
    assert fake_control.runs[payload["run_id"]].result is None


def test_submit_body_is_the_shipped_rest_submit_schema(call, fake_control):
    """The serialized wire body must satisfy the real request model directly.

    Issue #833 regression gate: the shipped adapter sealed the draft
    issue #809 field names while the REST route landed different ones, and
    the offline fake mirrored the adapter's own mistake, so every real
    ``mission_submit`` 422ed. This test validates the exact bytes the
    adapter posts against ``archetype.missions.api.MissionRunSubmitRequest``
    (``extra="forbid"``) with no fake in the loop, so any future rename on
    either side fails CI instead of production.
    """

    is_error, _ = call("mission_submit", submit_arguments())
    assert is_error is False
    recorded = fake_control.requests[-1]
    assert (recorded.method, recorded.path) == ("POST", "/v1/mission-runs")
    assert recorded.headers["Idempotency-Key"] == "key-0001"
    request = MissionRunSubmitRequest.model_validate(json.loads(recorded.body))
    assert request.profile_id == "profile-default"
    assert request.repository == "vangelis/archetype"
    assert request.branch == "agent/demo-mission"
    assert request.base_ref == "main"
    assert request.name == "demo-mission"
    task = request.tasks[0]
    assert task.name == "fix-bug"
    assert task.prompt == "Fix the reported bug."
    assert task.validators[0].name == "pytest"
    assert task.validators[0].command == ["pytest", "-q"]


def test_submit_omitted_optionals_defer_to_the_rest_defaults(call, fake_control):
    """Absent optional arguments stay off the wire; the server owns defaults."""

    arguments = submit_arguments()
    del arguments["base_ref"]
    del arguments["name"]
    is_error, _ = call("mission_submit", arguments)
    assert is_error is False
    body = json.loads(fake_control.requests[-1].body)
    assert "base_ref" not in body and "name" not in body
    assert "max_dispatches" not in body["tasks"][0]
    assert "expected_returncode" not in body["tasks"][0]["validators"][0]
    request = MissionRunSubmitRequest.model_validate(body)
    assert request.base_ref == "main"
    assert request.name == "agent-mission"
    assert request.tasks[0].max_dispatches == 3
    assert request.tasks[0].validators[0].expected_returncode == 0
    assert request.tasks[0].validators[0].timeout_seconds == 300


def test_submit_can_express_every_client_writable_rest_field(call, fake_control):
    """A maximal tool call covers the full field set of every request model,
    so a new client-writable REST field makes this test fail until the
    adapter grows it."""

    arguments = submit_arguments(
        tasks=[
            {
                "name": "fix-bug",
                "prompt": "Fix the reported bug.",
                "validators": [
                    {
                        "name": "pytest",
                        "command": ["pytest", "-q"],
                        "expected_returncode": 0,
                        "timeout_seconds": 120,
                    }
                ],
                "depends_on": [],
                "max_dispatches": 2,
            }
        ]
    )
    is_error, _ = call("mission_submit", arguments)
    assert is_error is False
    body = json.loads(fake_control.requests[-1].body)
    request = MissionRunSubmitRequest.model_validate(body)
    assert request.tasks[0].validators[0].timeout_seconds == 120
    assert request.tasks[0].max_dispatches == 2
    assert set(body) == set(MissionRunSubmitRequest.model_fields)
    assert set(body["tasks"][0]) == set(MissionRunTaskRequest.model_fields)
    assert set(body["tasks"][0]["validators"][0]) == set(MissionRunValidatorRequest.model_fields)


def test_draft_schema_field_names_fail_before_the_wire(call, fake_control):
    """The pre-#821 draft names (`ref`, `mission`, `validators[].argv`) that
    shipped the issue #833 bug are now rejected by the tool itself."""

    renamed = submit_arguments()
    del renamed["branch"]
    renamed["ref"] = "main"
    renamed["mission"] = renamed.pop("name")
    is_error, payload = call("mission_submit", renamed)
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert fake_control.requests == []

    draft_validator = [
        {
            "name": "t",
            "prompt": "p",
            "validators": [{"name": "pytest", "argv": ["pytest", "-q"]}],
        }
    ]
    is_error, payload = call("mission_submit", submit_arguments(tasks=draft_validator))
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert fake_control.requests == []


def test_task_without_validators_fails_before_the_wire(call, fake_control):
    """The REST task model requires a non-empty validator list."""

    is_error, payload = call(
        "mission_submit", submit_arguments(tasks=[{"name": "t", "prompt": "p"}])
    )
    assert is_error is True
    assert payload["error"]["code"] == "invalid_argument"
    assert "validators" in payload["error"]["message"]
    assert fake_control.requests == []


def test_duplicate_submit_after_process_death_returns_original_run(fake_control):
    """A fresh MCP process reusing the idempotency key recovers the same run."""

    config = McpHostConfig.from_env(host_environ(fake_control))
    first = MissionMcpServer(config, stderr=io.StringIO())
    try:
        _, original = call_tool(first, "mission_submit", submit_arguments())
    finally:
        first.close()

    second = MissionMcpServer(config, stderr=io.StringIO())
    try:
        is_error, recovered = call_tool(second, "mission_submit", submit_arguments())
    finally:
        second.close()
    assert is_error is False
    assert recovered["run_id"] == original["run_id"]
    assert recovered["request_digest"] == original["request_digest"]


def test_submit_with_changed_content_conflicts(call):
    call("mission_submit", submit_arguments())
    is_error, payload = call("mission_submit", submit_arguments(name="different-mission"))
    assert is_error is True
    assert payload["error"]["code"] == "conflict"


def test_missing_credential_fails_closed(fake_control):
    config = McpHostConfig.from_env({"ARCHETYPE_MISSIONS_MCP_URL": fake_control.base_url})
    server = MissionMcpServer(config, stderr=io.StringIO())
    try:
        is_error, payload = call_tool(server, "mission_submit", submit_arguments())
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
        state="succeeded", result={"status": "green", "commits": ["abc123"]}
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
    run = fake_control.seed_run(state="succeeded", result={"status": "green"})
    is_error, payload = call("mission_cancel", {"run_id": run.run_id})
    assert is_error is False
    assert payload["state"] == "succeeded"


def test_cancel_cannot_target_an_unauthorized_run(call, fake_control):
    foreign = fake_control.seed_run(state="running", foreign=True)
    is_error, payload = call("mission_cancel", {"run_id": foreign.run_id})
    assert is_error is True
    assert payload["error"]["code"] == "not_found"


def test_list_is_scoped_to_the_authenticated_principal(call, fake_control):
    own_one = fake_control.seed_run(state="running")
    own_two = fake_control.seed_run(state="accepted")
    fake_control.seed_run(state="running", foreign=True)
    is_error, payload = call("mission_list", {})
    assert is_error is False
    listed = {run["run_id"] for run in payload["runs"]}
    assert listed == {own_one.run_id, own_two.run_id}


def test_oversized_payload_is_truncated_with_an_explicit_marker(fake_control):
    """The FULL rendered tool text obeys max_result_bytes even for content
    whose JSON escaping expands aggressively (control chars, quotes)."""

    run = fake_control.seed_run(
        state="succeeded",
        result={"log": ('"' + "\x01\x02" + "\\") * 900},
    )
    config = McpHostConfig.from_env(
        host_environ(fake_control, ARCHETYPE_MISSIONS_MCP_MAX_RESULT_BYTES="256")
    )
    server = MissionMcpServer(config, stderr=io.StringIO())
    try:
        frame = server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "mission_result",
                    "arguments": {"run_id": run.run_id},
                },
            }
        )
    finally:
        server.close()
    result = frame["result"]
    assert result["isError"] is False
    rendered = result["content"][0]["text"]
    assert len(rendered.encode("utf-8")) <= 256
    payload = json.loads(rendered)
    assert payload["truncated"] is True
    assert payload["limit_bytes"] == 256
    assert payload["content_prefix"]


def test_result_bytes_floor_fails_closed(fake_control):
    with pytest.raises(McpHostConfigError):
        McpHostConfig.from_env(
            host_environ(fake_control, ARCHETYPE_MISSIONS_MCP_MAX_RESULT_BYTES="40")
        )
