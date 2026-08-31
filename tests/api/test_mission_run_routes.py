# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""HTTP boundary contracts for the Temporal-backed Mission control facade."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

import archetype.missions._extension as missions_extension
from archetype.api.app import create_app
from archetype.missions.config import MissionsExtensionConfig
from archetype.missions.execution_profiles import (
    ExecutionProfile,
    ExecutionProfileBinding,
    ExecutionProfileCatalog,
)
from archetype.missions.models import (
    AcceptMissionRun,
    CancelMissionRun,
    GetMissionRun,
    GetMissionRunEvents,
    ListMissionRuns,
)
from archetype.missions.run_contracts import (
    ExecutionProfileIdentity,
    MissionRun,
    MissionRunConflictError,
    MissionRunEvent,
    MissionRunNotFoundError,
    MissionRunStatus,
    mission_request_digest,
)

_AGENT_TOKEN = "mission-agent-credential-aaaa-0001"
_READER_TOKEN = "mission-reader-credential-bbbb-0002"
_STRANGER_TOKEN = "mission-stranger-credential-cccc-0003"


def _write_principals(path) -> None:
    path.write_text(
        """
[[principal]]
id = "agent"
token_env = "ARCHETYPE_MISSION_PRINCIPAL_AGENT_TOKEN"
capabilities = ["mission:submit", "mission:read", "mission:cancel"]
allowed_profile_ids = ["coding-default"]

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


def _profiles() -> ExecutionProfileCatalog:
    profile = ExecutionProfile.model_validate(
        {
            "profile_id": "coding-default",
            "version": "1",
            "allowed_repositories": ("VangelisTech/archetype",),
            "allowed_base_refs": ("main",),
            "branch_namespace": "agent/",
            "sandbox_backend": "modal",
            "sandbox_environment": "coding-agent:v1",
            "agent_driver": "codex-app-server",
            "critic_driver": "codex-app-server",
            "model": "gpt-5",
            "timeout_seconds": 3600,
            "max_ticks": 100,
            "max_retries": 3,
            "max_concurrency": 1,
            "cost_ceiling_usd_cents": 5000,
            "max_validators_per_task": 8,
            "max_validator_timeout_seconds": 900,
            "publication_policy": "commit_and_push",
            "checkpoint_after_dispatch": True,
            "allow_cancel": True,
        }
    )
    binding = ExecutionProfileBinding(profile=profile, config_factory=lambda _profile: None)
    return ExecutionProfileCatalog((binding,), current_versions={"coding-default": "1"})


def _headers(token: str, *, key: str | None = None) -> dict[str, str]:
    values = {"Authorization": f"Bearer {token}"}
    if key is not None:
        values["Idempotency-Key"] = key
    return values


def _body(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "profile_id": "coding-default",
        "repository": "VangelisTech/archetype",
        "branch": "agent/mermaid-docs",
        "base_ref": "main",
        "name": "mermaid-docs",
        "tasks": [
            {
                "name": "repair",
                "prompt": "Repair Mermaid documentation rendering.",
                "validators": [
                    {"name": "docs", "command": ["make", "docs"], "timeout_seconds": 300}
                ],
            }
        ],
    }
    value.update(changes)
    return value


@pytest.fixture
def mission_api(tmp_path, monkeypatch) -> SimpleNamespace:
    principals = tmp_path / "principals.toml"
    _write_principals(principals)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "catalogs"))
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPALS_PATH", str(principals))
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPAL_AGENT_TOKEN", _AGENT_TOKEN)
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPAL_READER_TOKEN", _READER_TOKEN)
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPAL_STRANGER_TOKEN", _STRANGER_TOKEN)
    monkeypatch.setenv("ARCHETYPE_BIND_HOST", "127.0.0.1")
    runs: dict[str, MissionRun] = {}
    events: dict[str, tuple[MissionRunEvent, ...]] = {}

    async def accept(operation: AcceptMissionRun) -> MissionRun:
        profile = ExecutionProfileIdentity(
            profile_id=operation.profile_id,
            version=operation.profile_version,
            digest=operation.profile_digest,
        )
        request = operation.request
        run_id = f"mission-{request.principal}-{request.idempotency_key}"
        candidate = MissionRun(
            run_id=run_id,
            principal=request.principal,
            idempotency_key=request.idempotency_key,
            request_digest=mission_request_digest(request.submission, profile),
            profile=profile,
            status=MissionRunStatus.ACCEPTED,
            submission=request.submission,
            world_id=f"world-{run_id}",
            accepted_at_ms=1,
            updated_at_ms=1,
        )
        existing = runs.get(run_id)
        if existing is not None:
            if existing.request_digest != candidate.request_digest:
                raise MissionRunConflictError("idempotency key reused with a different request")
            return existing
        runs[run_id] = candidate
        events[run_id] = (
            MissionRunEvent(
                run_id=run_id,
                cursor=1,
                event_type="accepted",
                phase="admission",
                payload_json="{}",
                created_at_ms=1,
            ),
        )
        return candidate

    async def get(operation: GetMissionRun) -> MissionRun:
        try:
            return runs[operation.run_id]
        except KeyError:
            raise MissionRunNotFoundError(operation.run_id) from None

    async def cancel(operation: CancelMissionRun) -> MissionRun:
        current = await get(
            GetMissionRun(owner_id=operation.owner_id, name=operation.name, run_id=operation.run_id)
        )
        updated = replace(
            current,
            status=MissionRunStatus.CANCELLING,
            cancellation_intent=True,
            cancellation_reason=operation.reason,
            updated_at_ms=2,
        )
        runs[operation.run_id] = updated
        events[operation.run_id] = (
            *events[operation.run_id],
            MissionRunEvent(
                run_id=operation.run_id,
                cursor=2,
                event_type="cancel_requested",
                phase="cancellation",
                payload_json="{}",
                created_at_ms=2,
            ),
        )
        return updated

    async def get_events(operation: GetMissionRunEvents) -> tuple[MissionRunEvent, ...]:
        await get(
            GetMissionRun(owner_id=operation.owner_id, name=operation.name, run_id=operation.run_id)
        )
        return tuple(event for event in events[operation.run_id] if event.cursor > operation.after)[
            : operation.limit
        ]

    async def list_runs(operation: ListMissionRuns) -> tuple[MissionRun, ...]:
        return tuple(run for run in runs.values() if run.principal == operation.owner_principal)[
            : operation.limit
        ]

    original = missions_extension._operation_handlers

    def handlers(*args: Any, **kwargs: Any):
        result = original(*args, **kwargs)
        result.update(
            {
                AcceptMissionRun: accept,
                GetMissionRun: get,
                CancelMissionRun: cancel,
                GetMissionRunEvents: get_events,
                ListMissionRuns: list_runs,
            }
        )
        return result

    monkeypatch.setattr(missions_extension, "_operation_handlers", handlers)
    return SimpleNamespace(
        make_client=lambda: TestClient(
            create_app(
                world_library_configs={
                    "missions": MissionsExtensionConfig(execution_profiles=_profiles())
                }
            )
        )
    )


def test_mission_http_requires_identity_and_authorizes_profile(mission_api) -> None:
    with mission_api.make_client() as client:
        assert client.post("/v1/mission-runs", json=_body()).status_code == 401
        assert (
            client.post(
                "/v1/mission-runs",
                json=_body(repository="other/repo"),
                headers=_headers(_AGENT_TOKEN, key="bad-repository"),
            ).status_code
            == 403
        )


def test_mission_http_projects_run_events_result_and_cancellation(mission_api) -> None:
    with mission_api.make_client() as client:
        created = client.post(
            "/v1/mission-runs", json=_body(), headers=_headers(_AGENT_TOKEN, key="mermaid")
        )
        assert created.status_code == 202, created.text
        run_id = created.json()["run_id"]
        assert (
            client.get(f"/v1/mission-runs/{run_id}", headers=_headers(_AGENT_TOKEN)).status_code
            == 200
        )
        assert (
            client.get(
                f"/v1/mission-runs/{run_id}/result", headers=_headers(_AGENT_TOKEN)
            ).status_code
            == 425
        )
        assert (
            client.get(f"/v1/mission-runs/{run_id}", headers=_headers(_STRANGER_TOKEN)).status_code
            == 403
        )
        events = client.get(f"/v1/mission-runs/{run_id}/events", headers=_headers(_AGENT_TOKEN))
        assert [event["event_type"] for event in events.json()["events"]] == ["accepted"]
        cancelled = client.post(
            f"/v1/mission-runs/{run_id}/cancel",
            json={"reason": "operator stop"},
            headers=_headers(_AGENT_TOKEN),
        )
        assert cancelled.status_code == 202
        assert cancelled.json()["cancellation_requested"] is True


def test_mission_http_list_is_principal_scoped_and_idempotent(mission_api) -> None:
    with mission_api.make_client() as client:
        first = client.post(
            "/v1/mission-runs", json=_body(), headers=_headers(_AGENT_TOKEN, key="same")
        )
        replay = client.post(
            "/v1/mission-runs", json=_body(), headers=_headers(_AGENT_TOKEN, key="same")
        )
        assert replay.json()["run_id"] == first.json()["run_id"]
        assert client.get("/v1/mission-runs", headers=_headers(_AGENT_TOKEN)).json()["runs"]
        assert client.get("/v1/mission-runs", headers=_headers(_READER_TOKEN)).json()["runs"] == []
