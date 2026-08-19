# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""HTTP contracts for the durable MissionRun REST control surface (#809).

The route handlers authenticate verified principals and dispatch the exact
registered MissionRun operations. Provider execution is out of scope here:
the governed submit/run executor is replaced with a deterministic gated fake
so admission, idempotency, events, result, cancel, and restart semantics can
be asserted without sandbox or model credentials.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import archetype.missions._extension as missions_extension
from archetype.api.app import create_app
from archetype.missions.contracts import MissionResult, SubmittedMission, TaskResult
from quality.secret_corpus import SECRET_LEAK_CORPUS

_AGENT_TOKEN = "mission-agent-credential-aaaa-0001"
_READER_TOKEN = "mission-reader-credential-bbbb-0002"
_STRANGER_TOKEN = "mission-stranger-credential-cccc-0003"
_PROMPT_MARKER = "PROMPT-MARKER-XYZZY"
_DEADLINE_SECONDS = 15.0


def _write_principals(path) -> None:
    path.write_text(
        """
[[principal]]
id = "agent"
token_env = "ARCHETYPE_MISSION_PRINCIPAL_AGENT_TOKEN"
capabilities = ["mission:submit", "mission:read", "mission:cancel"]
allowed_profile_ids = ["coding-default", "no-cancel"]

[[principal]]
id = "reader"
token_env = "ARCHETYPE_MISSION_PRINCIPAL_READER_TOKEN"
capabilities = ["mission:read"]
allowed_profile_ids = ["coding-default"]

[[principal]]
id = "stranger"
token_env = "ARCHETYPE_MISSION_PRINCIPAL_STRANGER_TOKEN"
capabilities = ["mission:submit", "mission:read", "mission:cancel"]
allowed_profile_ids = ["coding-default"]
""",
        encoding="utf-8",
    )


_PROFILE_TEMPLATE = """
[[profile]]
profile_id = "{profile_id}"
version = "1"
allowed_repositories = ["VangelisTech/archetype"]
allowed_base_refs = ["main"]
branch_namespace = "agent/"
sandbox_backend = "modal"
sandbox_environment = "coding-agent:v1"
agent_driver = "codex-app-server"
critic_driver = "codex-app-server"
model = "gpt-5"
timeout_seconds = 3600
max_ticks = 100
max_retries = 3
max_concurrency = 1
cost_ceiling_usd_cents = 5000
max_validators_per_task = 8
max_validator_timeout_seconds = 900
publication_policy = "commit_and_push"
checkpoint_after_dispatch = true
secret_names = ["codex-auth"]
provider_credential_names = ["modal"]
allow_cancel = {allow_cancel}
allow_attach = false
allow_steer = false
allow_takeover = false
"""


def _write_profiles(path) -> None:
    path.write_text(
        _PROFILE_TEMPLATE.format(profile_id="coding-default", allow_cancel="true")
        + _PROFILE_TEMPLATE.format(profile_id="no-cancel", allow_cancel="false"),
        encoding="utf-8",
    )


class _ExecutorGate:
    """Deterministic control over the governed submit/run executor."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.submitted = threading.Event()
        self.result_status = "succeeded"


@pytest.fixture
def mission_api(tmp_path, monkeypatch) -> Iterator[SimpleNamespace]:
    principals = tmp_path / "principals.toml"
    profiles = tmp_path / "profiles.toml"
    _write_principals(principals)
    _write_profiles(profiles)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "catalogs"))
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPALS_PATH", str(principals))
    monkeypatch.setenv("ARCHETYPE_MISSION_PROFILES_PATH", str(profiles))
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPAL_AGENT_TOKEN", _AGENT_TOKEN)
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPAL_READER_TOKEN", _READER_TOKEN)
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPAL_STRANGER_TOKEN", _STRANGER_TOKEN)
    monkeypatch.setenv("ARCHETYPE_BIND_HOST", "127.0.0.1")

    gate = _ExecutorGate()

    async def fake_submit(self, run):
        del self
        gate.submitted.set()
        return SubmittedMission(
            mission_id=1,
            task_ids=tuple(
                (task.name, index + 100) for index, task in enumerate(run.submission.tasks)
            ),
            episode_id=f"episode-{run.run_id}",
            repository=run.submission.repository,
            branch=run.submission.branch,
            world_id=run.world_id,
        )

    async def fake_load_existing(self, run):
        del self, run
        return None

    async def fake_run(self, run, mission):
        del self, run
        import asyncio

        while not gate.release.is_set():
            await asyncio.sleep(0.005)
        return MissionResult(
            mission_id=mission.mission_id,
            episode_id=mission.episode_id,
            status=gate.result_status,
            repository=mission.repository,
            branch=mission.branch,
            ticks_completed=3,
            tasks=tuple(
                TaskResult(
                    task_id=task_id,
                    name=name,
                    status="accepted" if gate.result_status == "succeeded" else "failed",
                    dispatches=1,
                    commit_shas=("d" * 40,),
                )
                for name, task_id in mission.task_ids
            ),
        )

    executor = missions_extension._DispatchedMissionRunExecutor
    monkeypatch.setattr(executor, "submit", fake_submit)
    monkeypatch.setattr(executor, "load_existing", fake_load_existing)
    monkeypatch.setattr(executor, "run", fake_run)

    def make_client() -> TestClient:
        return TestClient(create_app())

    yield SimpleNamespace(make_client=make_client, gate=gate)


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _submit_headers(key: str = "issue-809-key-1") -> dict[str, str]:
    return {**_auth(_AGENT_TOKEN), "Idempotency-Key": key}


def _submit_body(**overrides) -> dict:
    body = {
        "profile_id": "coding-default",
        "repository": "VangelisTech/archetype",
        "branch": "agent/issue-809",
        "base_ref": "main",
        "name": "issue-809-mission",
        "tasks": [
            {
                "name": "implementation",
                "prompt": f"Implement the requested change. {_PROMPT_MARKER}",
                "validators": [
                    {
                        "name": "tests",
                        "command": ["pytest", "-q"],
                        "expected_returncode": 0,
                        "timeout_seconds": 300,
                    }
                ],
            }
        ],
    }
    body.update(overrides)
    return body


def _assert_sanitized(payload: str) -> None:
    for token in (_AGENT_TOKEN, _READER_TOKEN, _STRANGER_TOKEN):
        assert token not in payload
    for case in SECRET_LEAK_CORPUS:
        assert case.payload not in payload
    for host_internal in ("gpt-5", "codex-auth", "coding-agent:v1", "codex-app-server"):
        assert host_internal not in payload


def _poll(
    client: TestClient,
    run_id: str,
    predicate: Callable[[dict], bool],
    *,
    token: str = _AGENT_TOKEN,
) -> dict:
    deadline = time.monotonic() + _DEADLINE_SECONDS
    while time.monotonic() < deadline:
        response = client.get(f"/v1/mission-runs/{run_id}", headers=_auth(token))
        assert response.status_code == 200, response.text
        body = response.json()
        if predicate(body):
            return body
        time.sleep(0.02)
    raise AssertionError(f"run {run_id} never satisfied the polled predicate")


def test_all_run_routes_fail_closed_without_a_verified_principal(
    mission_api: SimpleNamespace,
) -> None:
    with mission_api.make_client() as client:
        requests = (
            ("POST", "/v1/mission-runs"),
            ("GET", "/v1/mission-runs/run-1"),
            ("GET", "/v1/mission-runs/run-1/events"),
            ("GET", "/v1/mission-runs/run-1/result"),
            ("POST", "/v1/mission-runs/run-1/cancel"),
        )
        credentials = (
            {},
            _auth("mission-unknown-credential-zzzz"),
            {"Authorization": "Basic abcdefghijklmnopqrstuvwxyz"},
            _auth("admin"),
        )
        for method, path in requests:
            for headers in credentials:
                response = client.request(
                    method,
                    path,
                    headers={**headers, "Idempotency-Key": "k"},
                    json=_submit_body() if method == "POST" else None,
                )
                assert response.status_code == 401, (method, path, response.text)
                _assert_sanitized(response.text)


def test_capabilities_and_ownership_fail_closed(mission_api: SimpleNamespace) -> None:
    with mission_api.make_client() as client:
        created = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_submit_headers(),
        )
        assert created.status_code == 202, created.text
        run_id = created.json()["run_id"]

        no_submit = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers={**_auth(_READER_TOKEN), "Idempotency-Key": "reader-key"},
        )
        assert no_submit.status_code == 403

        no_cancel = client.post(
            f"/v1/mission-runs/{run_id}/cancel",
            headers=_auth(_READER_TOKEN),
        )
        assert no_cancel.status_code == 403

        for path in (
            f"/v1/mission-runs/{run_id}",
            f"/v1/mission-runs/{run_id}/events",
            f"/v1/mission-runs/{run_id}/result",
        ):
            foreign = client.get(path, headers=_auth(_STRANGER_TOKEN))
            assert foreign.status_code == 403, path
            _assert_sanitized(foreign.text)
        foreign_cancel = client.post(
            f"/v1/mission-runs/{run_id}/cancel",
            headers=_auth(_STRANGER_TOKEN),
        )
        assert foreign_cancel.status_code == 403

        missing = client.get(
            "/v1/mission-runs/does-not-exist",
            headers=_auth(_AGENT_TOKEN),
        )
        assert missing.status_code == 404


def test_submit_returns_202_with_stable_identity_and_no_host_internals(
    mission_api: SimpleNamespace,
) -> None:
    with mission_api.make_client() as client:
        response = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_submit_headers(),
        )
        assert response.status_code == 202, response.text
        body = response.json()
        assert body["run_id"]
        assert len(body["request_digest"]) == 64
        assert body["state"] in {"accepted", "running"}
        assert body["profile"] == {
            "profile_id": "coding-default",
            "version": "1",
            "digest": body["profile"]["digest"],
        }
        assert len(body["profile"]["digest"]) == 64
        assert body["status_url"] == f"/v1/mission-runs/{body['run_id']}"
        _assert_sanitized(response.text)
        assert _PROMPT_MARKER not in response.text


def test_submit_requires_idempotency_key_and_rejects_host_execution_fields(
    mission_api: SimpleNamespace,
) -> None:
    with mission_api.make_client() as client:
        keyless = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_auth(_AGENT_TOKEN),
        )
        assert keyless.status_code == 422

        for extra in (
            {"model": "gpt-5"},
            {"sandbox_backend": "modal"},
            {"sandbox_environment": "custom"},
            {"driver": "codex"},
            {"critic": "codex"},
            {"secret": "codex-auth"},
            {"timeout_seconds": 5},
            {"publication_policy": "commit_and_push"},
        ):
            response = client.post(
                "/v1/mission-runs",
                json={**_submit_body(), **extra},
                headers=_submit_headers(),
            )
            assert response.status_code == 422, extra
            # A validation error may echo the client's own rejected input;
            # host credentials must still never appear.
            for token in (_AGENT_TOKEN, _READER_TOKEN, _STRANGER_TOKEN):
                assert token not in response.text
            for case in SECRET_LEAK_CORPUS:
                assert case.payload not in response.text


def test_submit_limits_bound_tasks_prompts_and_validators(
    mission_api: SimpleNamespace,
) -> None:
    task = _submit_body()["tasks"][0]
    with mission_api.make_client() as client:
        over_tasks = _submit_body(tasks=[{**task, "name": f"task-{index}"} for index in range(33)])
        over_prompt = _submit_body(tasks=[{**task, "prompt": "p" * 65_537}])
        over_argv = _submit_body(
            tasks=[
                {
                    **task,
                    "validators": [{"name": "tests", "command": ["a"] * 65, "timeout_seconds": 60}],
                }
            ]
        )
        over_validator_count = _submit_body(
            tasks=[
                {
                    **task,
                    "validators": [
                        {"name": f"v-{index}", "command": ["true"], "timeout_seconds": 60}
                        for index in range(9)
                    ],
                }
            ]
        )
        over_validator_timeout = _submit_body(
            tasks=[
                {
                    **task,
                    "validators": [{"name": "tests", "command": ["true"], "timeout_seconds": 901}],
                }
            ]
        )
        for body in (
            over_tasks,
            over_prompt,
            over_argv,
            over_validator_count,
            over_validator_timeout,
        ):
            response = client.post(
                "/v1/mission-runs",
                json=body,
                headers=_submit_headers(),
            )
            assert response.status_code == 422, response.text

        outside_repo = client.post(
            "/v1/mission-runs",
            json=_submit_body(repository="other/repo"),
            headers=_submit_headers(),
        )
        outside_branch = client.post(
            "/v1/mission-runs",
            json=_submit_body(branch="main"),
            headers=_submit_headers(),
        )
        assert outside_repo.status_code == 403
        assert outside_branch.status_code == 403


def test_duplicate_and_conflict_semantics_survive_api_restart(
    mission_api: SimpleNamespace,
) -> None:
    with mission_api.make_client() as client:
        first = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_submit_headers("restart-key"),
        )
        assert first.status_code == 202, first.text
        run_id = first.json()["run_id"]
        digest = first.json()["request_digest"]

        replay = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_submit_headers("restart-key"),
        )
        assert replay.status_code == 202
        assert replay.json()["run_id"] == run_id
        assert replay.json()["request_digest"] == digest

        conflict = client.post(
            "/v1/mission-runs",
            json=_submit_body(name="different-mission"),
            headers=_submit_headers("restart-key"),
        )
        assert conflict.status_code == 409

    with mission_api.make_client() as restarted:
        replay = restarted.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_submit_headers("restart-key"),
        )
        assert replay.status_code == 202, replay.text
        assert replay.json()["run_id"] == run_id
        assert replay.json()["request_digest"] == digest

        conflict = restarted.post(
            "/v1/mission-runs",
            json=_submit_body(name="different-mission"),
            headers=_submit_headers("restart-key"),
        )
        assert conflict.status_code == 409
        _assert_sanitized(conflict.text)


def test_event_cursor_replay_has_no_gaps_or_duplicates(
    mission_api: SimpleNamespace,
) -> None:
    gate = mission_api.gate
    with mission_api.make_client() as client:
        created = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_submit_headers("events-key"),
        )
        run_id = created.json()["run_id"]
        assert gate.submitted.wait(_DEADLINE_SECONDS)
        gate.release.set()
        _poll(client, run_id, lambda body: body["state"] == "succeeded")

        full = client.get(
            f"/v1/mission-runs/{run_id}/events",
            headers=_auth(_AGENT_TOKEN),
        )
        assert full.status_code == 200, full.text
        page = full.json()
        cursors = [event["cursor"] for event in page["events"]]
        assert cursors == list(range(1, len(cursors) + 1))
        assert [event["event_type"] for event in page["events"]] == [
            "accepted",
            "running",
            "mission_bound",
            "succeeded",
        ]
        for event in page["events"]:
            assert event["schema_version"] == 1
            assert event["event_id"] == f"{run_id}/{event['cursor']}"
            assert event["created_at_ms"] > 0
            assert event["phase"]
        assert page["next_after"] == cursors[-1]
        _assert_sanitized(full.text)
        assert _PROMPT_MARKER not in full.text

        first_page = client.get(
            f"/v1/mission-runs/{run_id}/events?after=0&limit=2",
            headers=_auth(_AGENT_TOKEN),
        ).json()
        second_page = client.get(
            f"/v1/mission-runs/{run_id}/events?after={first_page['next_after']}&limit=500",
            headers=_auth(_AGENT_TOKEN),
        ).json()
        replayed = [*first_page["events"], *second_page["events"]]
        assert [event["event_id"] for event in replayed] == [
            event["event_id"] for event in page["events"]
        ]
        beyond = client.get(
            f"/v1/mission-runs/{run_id}/events?after={page['next_after']}",
            headers=_auth(_AGENT_TOKEN),
        ).json()
        assert beyond["events"] == []
        assert beyond["next_after"] == page["next_after"]

        for query in ("limit=0", "limit=501", "after=-1"):
            bounded = client.get(
                f"/v1/mission-runs/{run_id}/events?{query}",
                headers=_auth(_AGENT_TOKEN),
            )
            assert bounded.status_code == 422, query


def test_result_is_425_until_terminal_then_immutable(
    mission_api: SimpleNamespace,
) -> None:
    gate = mission_api.gate
    with mission_api.make_client() as client:
        created = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_submit_headers("result-key"),
        )
        run_id = created.json()["run_id"]
        assert gate.submitted.wait(_DEADLINE_SECONDS)

        early = client.get(
            f"/v1/mission-runs/{run_id}/result",
            headers=_auth(_AGENT_TOKEN),
        )
        assert early.status_code == 425

        gate.release.set()
        _poll(client, run_id, lambda body: body["state"] == "succeeded")
        first = client.get(
            f"/v1/mission-runs/{run_id}/result",
            headers=_auth(_AGENT_TOKEN),
        )
        assert first.status_code == 200, first.text
        body = first.json()
        assert body["state"] == "succeeded"
        assert body["result"]["status"] == "succeeded"
        assert body["result"]["tasks"][0]["name"] == "implementation"
        _assert_sanitized(first.text)
        assert _PROMPT_MARKER not in first.text

        second = client.get(
            f"/v1/mission-runs/{run_id}/result",
            headers=_auth(_AGENT_TOKEN),
        )
        assert second.json() == body

    with mission_api.make_client() as restarted:
        replayed = restarted.get(
            f"/v1/mission-runs/{run_id}/result",
            headers=_auth(_AGENT_TOKEN),
        )
        assert replayed.status_code == 200
        assert replayed.json() == body


def test_cancel_records_durable_intent_idempotently_and_converges(
    mission_api: SimpleNamespace,
) -> None:
    gate = mission_api.gate
    with mission_api.make_client() as client:
        created = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_submit_headers("cancel-key"),
        )
        run_id = created.json()["run_id"]
        assert gate.submitted.wait(_DEADLINE_SECONDS)

        first = client.post(
            f"/v1/mission-runs/{run_id}/cancel",
            json={"reason": "operator stop"},
            headers=_auth(_AGENT_TOKEN),
        )
        assert first.status_code == 202, first.text
        assert first.json()["state"] == "cancelling"
        assert first.json()["cancellation_requested"] is True
        assert first.json()["cancellation_reason"] == "operator stop"

        repeat = client.post(
            f"/v1/mission-runs/{run_id}/cancel",
            json={"reason": "operator stop"},
            headers=_auth(_AGENT_TOKEN),
        )
        assert repeat.status_code == 202
        assert repeat.json()["state"] == "cancelling"

        gate.release.set()
        final = _poll(client, run_id, lambda body: body["state"] == "cancelled")
        assert final["cancellation_requested"] is True

        events = client.get(
            f"/v1/mission-runs/{run_id}/events",
            headers=_auth(_AGENT_TOKEN),
        ).json()["events"]
        types = [event["event_type"] for event in events]
        assert types.count("cancel_requested") == 1
        assert types.count("cancelling") == 1
        assert types.count("cancelled") == 1
        assert types[-1] == "cancelled"

        result = client.get(
            f"/v1/mission-runs/{run_id}/result",
            headers=_auth(_AGENT_TOKEN),
        )
        assert result.status_code == 200
        assert result.json()["state"] == "cancelled"
        assert result.json()["result"]["status"] == "succeeded"


def test_cancel_fails_closed_when_the_pinned_profile_forbids_it(
    mission_api: SimpleNamespace,
) -> None:
    with mission_api.make_client() as client:
        created = client.post(
            "/v1/mission-runs",
            json=_submit_body(profile_id="no-cancel"),
            headers=_submit_headers("no-cancel-key"),
        )
        assert created.status_code == 202, created.text
        run_id = created.json()["run_id"]
        denied = client.post(
            f"/v1/mission-runs/{run_id}/cancel",
            headers=_auth(_AGENT_TOKEN),
        )
        assert denied.status_code == 403


def test_api_restart_during_run_resumes_one_run_with_a_truthful_outcome(
    mission_api: SimpleNamespace,
) -> None:
    gate = mission_api.gate
    with mission_api.make_client() as client:
        created = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_submit_headers("crash-key"),
        )
        run_id = created.json()["run_id"]
        assert gate.submitted.wait(_DEADLINE_SECONDS)
        # The client (and its supervised execution) disappears mid-run here.

    with mission_api.make_client() as restarted:
        status = restarted.get(
            f"/v1/mission-runs/{run_id}",
            headers=_auth(_AGENT_TOKEN),
        )
        assert status.status_code == 200, status.text
        assert status.json()["state"] == "running"

        gate.release.set()
        final = _poll(restarted, run_id, lambda body: body["state"] == "succeeded")
        assert final["run_id"] == run_id
        assert final["mission_id"] == 1

        events = restarted.get(
            f"/v1/mission-runs/{run_id}/events",
            headers=_auth(_AGENT_TOKEN),
        ).json()["events"]
        types = [event["event_type"] for event in events]
        assert types.count("mission_bound") == 1
        assert types.count("running") == 1
        assert types.count("succeeded") == 1


def test_api_restart_during_cancel_converges_durably(
    mission_api: SimpleNamespace,
) -> None:
    gate = mission_api.gate
    with mission_api.make_client() as client:
        created = client.post(
            "/v1/mission-runs",
            json=_submit_body(),
            headers=_submit_headers("cancel-crash-key"),
        )
        run_id = created.json()["run_id"]
        assert gate.submitted.wait(_DEADLINE_SECONDS)
        cancelling = client.post(
            f"/v1/mission-runs/{run_id}/cancel",
            json={"reason": "operator stop"},
            headers=_auth(_AGENT_TOKEN),
        )
        assert cancelling.status_code == 202, cancelling.text
        assert cancelling.json()["state"] == "cancelling"
        # The API process disappears with the intent durable and the run open.

    with mission_api.make_client() as restarted:
        status = restarted.get(
            f"/v1/mission-runs/{run_id}",
            headers=_auth(_AGENT_TOKEN),
        )
        assert status.status_code == 200, status.text
        assert status.json()["cancellation_requested"] is True

        final = _poll(restarted, run_id, lambda body: body["state"] == "cancelled")
        assert final["cancellation_requested"] is True
        assert final["cancellation_reason"] == "operator stop"

        events = restarted.get(
            f"/v1/mission-runs/{run_id}/events",
            headers=_auth(_AGENT_TOKEN),
        ).json()["events"]
        cursors = [event["cursor"] for event in events]
        assert cursors == list(range(1, len(cursors) + 1))
        types = [event["event_type"] for event in events]
        assert types.count("cancel_requested") == 1
        assert types.count("cancelling") == 1
        assert types.count("cancelled") == 1

        result = restarted.get(
            f"/v1/mission-runs/{run_id}/result",
            headers=_auth(_AGENT_TOKEN),
        )
        assert result.status_code == 200, result.text
        assert result.json()["state"] == "cancelled"


def test_result_and_status_projections_truncate_oversized_text() -> None:
    from archetype.missions.api import (
        _MAX_CANCEL_REASON_CHARS,
        _MAX_RESULT_REASON_CHARS,
        _run_result_response,
        _run_status_response,
    )
    from archetype.missions.contracts import (
        AgentTask,
        CommandValidator,
        MissionSubmission,
    )
    from archetype.missions.run_contracts import (
        ExecutionProfileIdentity,
        MissionRun,
        MissionRunStatus,
    )

    long_reason = "r" * (_MAX_RESULT_REASON_CHARS + 500)
    run = MissionRun(
        run_id="run-truncation",
        principal="agent",
        idempotency_key="truncation-key",
        request_digest="0" * 64,
        profile=ExecutionProfileIdentity(profile_id="p", version="1", digest="0" * 64),
        status=MissionRunStatus.FAILED,
        submission=MissionSubmission(
            repository="VangelisTech/archetype",
            branch="agent/truncation",
            tasks=(
                AgentTask(
                    name="implementation",
                    prompt="Implement the requested change.",
                    validators=(CommandValidator(name="tests", command=("true",)),),
                ),
            ),
        ),
        cancellation_intent=True,
        cancellation_reason="c" * 4096,
        result=MissionResult(
            mission_id=1,
            episode_id="episode-truncation",
            status="failed",
            repository="VangelisTech/archetype",
            branch="agent/truncation",
            ticks_completed=1,
            reason=long_reason,
            tasks=(
                TaskResult(
                    task_id=2,
                    name="implementation",
                    status="failed",
                    dispatches=1,
                    commit_shas=(),
                    reason=long_reason,
                ),
            ),
        ),
        accepted_at_ms=1,
        terminal_at_ms=2,
    )

    result_view = _run_result_response(run)
    assert result_view.result is not None
    assert len(result_view.result.reason) == _MAX_RESULT_REASON_CHARS
    assert len(result_view.result.tasks[0].reason) == _MAX_RESULT_REASON_CHARS

    status_view = _run_status_response(run)
    assert len(status_view.cancellation_reason) == _MAX_CANCEL_REASON_CHARS


def test_openapi_documents_every_run_route_state_and_error(
    mission_api: SimpleNamespace,
) -> None:
    with mission_api.make_client() as client:
        schema = client.get("/openapi.json").json()
    paths = schema["paths"]
    expectations = {
        ("/v1/mission-runs", "post"): {"202", "401", "403", "404", "409", "422"},
        ("/v1/mission-runs/{run_id}", "get"): {"200", "401", "403", "404"},
        ("/v1/mission-runs/{run_id}/events", "get"): {"200", "401", "403", "404"},
        ("/v1/mission-runs/{run_id}/result", "get"): {"200", "401", "403", "404", "425"},
        ("/v1/mission-runs/{run_id}/cancel", "post"): {"202", "401", "403", "404"},
    }
    for (path, method), statuses in expectations.items():
        declared = set(paths[path][method]["responses"])
        assert statuses <= declared, (path, method, declared)

    components = schema["components"]["schemas"]
    for model in (
        "MissionRunSubmitRequest",
        "MissionRunAcceptedResponse",
        "MissionRunStatusResponse",
        "MissionRunEventPageResponse",
        "MissionRunResultResponse",
    ):
        assert model in components, model
    import json

    serialized = json.dumps(components["MissionRunStatusResponse"]) + json.dumps(components)
    for state in (
        "accepted",
        "running",
        "succeeded",
        "failed",
        "cancelling",
        "cancelled",
        "interrupted",
    ):
        assert state in serialized, state


def test_existing_mission_and_mission_control_routes_remain_compatible(
    mission_api: SimpleNamespace,
) -> None:
    with mission_api.make_client() as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/worlds", headers=_auth("player")).status_code == 200
        pin = client.post(
            "/v1/mission-control/runs",
            json={
                "profile_id": "coding-default",
                "repository": "VangelisTech/archetype",
                "branch": "agent/issue-809",
                "base_ref": "main",
            },
            headers=_auth(_AGENT_TOKEN),
        )
        assert pin.status_code == 202, pin.text
