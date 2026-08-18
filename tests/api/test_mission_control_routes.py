# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""HTTP contracts for authenticated mission-control routes."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from archetype.api.app import create_app
from quality.secret_corpus import SECRET_LEAK_CORPUS

_AGENT_TOKEN = "K" * 40
_READER_TOKEN = "mission-reader-credential-bbbb"
_STRANGER_TOKEN = "mission-stranger-credential-cccc"
_SUBMIT_BODY = {
    "profile_id": "coding-default",
    "repository": "VangelisTech/archetype",
    "branch": "agent/issue-808",
    "base_ref": "main",
}


def _write_principals(path) -> None:
    path.write_text(
        """
[[principal]]
id = "agent"
token_env = "ARCHETYPE_MISSION_PRINCIPAL_AGENT_TOKEN"
capabilities = [
  "mission:submit",
  "mission:read",
  "mission:cancel",
  "mission:attach",
  "mission:steer",
  "mission:takeover",
]
allowed_profile_ids = ["coding-default"]

[[principal]]
id = "reader"
token_env = "ARCHETYPE_MISSION_PRINCIPAL_READER_TOKEN"
capabilities = ["mission:read"]
allowed_profile_ids = ["coding-default"]

[[principal]]
id = "stranger"
token_env = "ARCHETYPE_MISSION_PRINCIPAL_STRANGER_TOKEN"
capabilities = ["mission:read", "mission:cancel"]
allowed_profile_ids = ["coding-default"]
""",
        encoding="utf-8",
    )


def _write_profiles(path) -> None:
    path.write_text(
        """
[[profile]]
profile_id = "coding-default"
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
allow_cancel = true
allow_attach = true
allow_steer = true
allow_takeover = true
""",
        encoding="utf-8",
    )


@pytest.fixture
def client(tmp_path, monkeypatch) -> Iterator[TestClient]:
    principals = tmp_path / "principals.toml"
    profiles = tmp_path / "profiles.toml"
    _write_principals(principals)
    _write_profiles(profiles)
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "catalogs"))
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPALS_PATH", str(principals))
    monkeypatch.setenv("ARCHETYPE_MISSION_PROFILES_PATH", str(profiles))
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPAL_AGENT_TOKEN", _AGENT_TOKEN)
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPAL_READER_TOKEN", _READER_TOKEN)
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPAL_STRANGER_TOKEN", _STRANGER_TOKEN)
    monkeypatch.setenv("ARCHETYPE_BIND_HOST", "127.0.0.1")
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _assert_no_credentials(payload: str) -> None:
    assert _AGENT_TOKEN not in payload
    assert _READER_TOKEN not in payload
    assert _STRANGER_TOKEN not in payload
    for case in SECRET_LEAK_CORPUS:
        assert case.payload not in payload


def test_developer_routes_remain_on_loopback_developer_auth(client: TestClient) -> None:
    anonymous = client.get("/healthz")
    assert anonymous.status_code == 200
    role = client.get("/worlds", headers=_auth("player"))
    assert role.status_code == 200


def test_mission_control_fails_closed_without_a_verified_principal(
    client: TestClient,
) -> None:
    missing = client.post("/v1/mission-control/runs", json=_SUBMIT_BODY)
    unknown = client.post(
        "/v1/mission-control/runs",
        json=_SUBMIT_BODY,
        headers=_auth("mission-unknown-credential-zzzz"),
    )
    malformed = client.post(
        "/v1/mission-control/runs",
        json=_SUBMIT_BODY,
        headers={"Authorization": "Basic abcdefghijklmnopqrstuvwxyz"},
    )
    role = client.post(
        "/v1/mission-control/runs",
        json=_SUBMIT_BODY,
        headers=_auth("admin"),
    )
    for response in (missing, unknown, malformed, role):
        assert response.status_code == 401
        _assert_no_credentials(response.text)


def test_submit_pins_the_selected_profile(client: TestClient) -> None:
    response = client.post(
        "/v1/mission-control/runs",
        json=_SUBMIT_BODY,
        headers=_auth(_AGENT_TOKEN),
    )
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["owner_principal_id"] == "agent"
    assert body["profile_id"] == "coding-default"
    assert body["profile_version"] == "1"
    assert len(body["profile_digest"]) == 64
    assert body["state"] == "accepted"
    _assert_no_credentials(response.text)
    assert "modal" not in body
    assert "gpt-5" not in response.text
    assert "codex-auth" not in response.text


def test_request_cannot_choose_host_execution_fields(client: TestClient) -> None:
    for extra in (
        {"model": "gpt-5"},
        {"sandbox_backend": "modal"},
        {"timeout_seconds": 12},
        {"driver": "codex"},
        {"secret": "codex-auth"},
    ):
        response = client.post(
            "/v1/mission-control/runs",
            json={**_SUBMIT_BODY, **extra},
            headers=_auth(_AGENT_TOKEN),
        )
        assert response.status_code == 422, response.text
        _assert_no_credentials(response.text)


def test_request_cannot_leave_profile_allowlists(client: TestClient) -> None:
    response = client.post(
        "/v1/mission-control/runs",
        json={**_SUBMIT_BODY, "repository": "other/repo"},
        headers=_auth(_AGENT_TOKEN),
    )
    assert response.status_code == 403
    _assert_no_credentials(response.text)


def test_capabilities_are_enforced_per_route(client: TestClient) -> None:
    created = client.post(
        "/v1/mission-control/runs",
        json=_SUBMIT_BODY,
        headers=_auth(_AGENT_TOKEN),
    )
    run_id = created.json()["run_id"]
    owner = client.app.state.mission_principals.authenticate(_AGENT_TOKEN)
    catalog = client.app.state.resources.host_capability("missions:control")
    catalog.grant(owner, run_id, "reader")
    reader = _auth(_READER_TOKEN)

    readable = client.get(f"/v1/mission-control/runs/{run_id}", headers=reader)
    assert readable.status_code == 200, readable.text
    for path in (
        f"/v1/mission-control/runs/{run_id}/cancel",
        f"/v1/mission-control/runs/{run_id}/attach",
        f"/v1/mission-control/runs/{run_id}/steer",
        f"/v1/mission-control/runs/{run_id}/takeover",
    ):
        denied = client.post(path, headers=reader)
        assert denied.status_code == 403
        _assert_no_credentials(denied.text)

    submit_denied = client.post(
        "/v1/mission-control/runs",
        json=_SUBMIT_BODY,
        headers=reader,
    )
    assert submit_denied.status_code == 403


def test_foreign_principal_cannot_read_without_a_grant(client: TestClient) -> None:
    created = client.post(
        "/v1/mission-control/runs",
        json=_SUBMIT_BODY,
        headers=_auth(_AGENT_TOKEN),
    )
    run_id = created.json()["run_id"]
    denied = client.get(
        f"/v1/mission-control/runs/{run_id}",
        headers=_auth(_STRANGER_TOKEN),
    )
    assert denied.status_code == 403
    _assert_no_credentials(denied.text)

    catalog = client.app.state.resources.host_capability("missions:control")
    owner = client.app.state.mission_principals.authenticate(_AGENT_TOKEN)
    catalog.grant(owner, run_id, "stranger")
    allowed = client.get(
        f"/v1/mission-control/runs/{run_id}",
        headers=_auth(_STRANGER_TOKEN),
    )
    assert allowed.status_code == 200, allowed.text
    assert allowed.json()["run_id"] == run_id


def test_same_profile_version_digest_is_stable_across_submits(client: TestClient) -> None:
    first = client.post(
        "/v1/mission-control/runs",
        json=_SUBMIT_BODY,
        headers=_auth(_AGENT_TOKEN),
    )
    second = client.post(
        "/v1/mission-control/runs",
        json=_SUBMIT_BODY,
        headers=_auth(_AGENT_TOKEN),
    )
    assert first.json()["profile_digest"] == second.json()["profile_digest"]
    assert first.json()["run_id"] != second.json()["run_id"]


def test_non_loopback_without_principals_cannot_run_missions(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "catalogs"))
    monkeypatch.delenv("ARCHETYPE_MISSION_PRINCIPALS_PATH", raising=False)
    monkeypatch.setenv("ARCHETYPE_BIND_HOST", "0.0.0.0")
    app = create_app()
    with TestClient(app) as test_client:
        assert test_client.get("/healthz").status_code == 200
        denied = test_client.post(
            "/v1/mission-control/runs",
            json=_SUBMIT_BODY,
            headers=_auth(_AGENT_TOKEN),
        )
        assert denied.status_code == 401
        _assert_no_credentials(denied.text)
