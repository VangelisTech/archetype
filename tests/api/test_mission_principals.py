# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verified mission-principal transport contracts."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from archetype.api.app import create_app
from archetype.api.principals import (
    AuthenticationError,
    MissionPrincipalDirectory,
    is_loopback_host,
    parse_bearer_credential,
)
from archetype.missions._extension import get_manifest

_TOKEN = "mission-agent-credential-" + "A" * 32


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": "agent",
        "token_env": "MISSION_AGENT_TOKEN",
        "capabilities": ["mission:submit", "mission:read"],
        "allowed_profile_ids": ["coding-default"],
    }
    row.update(overrides)
    return row


def test_opaque_credential_resolves_to_stable_explicit_claims() -> None:
    directory = MissionPrincipalDirectory.from_provisioning(
        (_row(),),
        {"MISSION_AGENT_TOKEN": _TOKEN},
    )

    principal = directory.authenticate(_TOKEN)

    assert principal.principal_id == "agent"
    assert principal.capabilities == {"mission:submit", "mission:read"}
    assert principal.allowed_profile_ids == {"coding-default"}
    assert _TOKEN not in repr(directory)


def test_provisioning_accepts_a_verifier_but_not_a_stored_plaintext_credential() -> None:
    verifier = hashlib.sha256(_TOKEN.encode()).hexdigest()
    directory = MissionPrincipalDirectory.from_provisioning(
        (_row(token_env=None, credential_sha256=verifier),),
        {},
    )
    assert directory.authenticate(_TOKEN).principal_id == "agent"

    plaintext = _row(token_env=None, credential=_TOKEN)
    with pytest.raises(ValueError, match="exactly one"):
        MissionPrincipalDirectory.from_provisioning((plaintext,), {})


@pytest.mark.parametrize(
    "authorization",
    [None, "", "Basic abc", "Bearer admin", "Bearer short", "Bearer token with-space"],
)
def test_malformed_and_role_label_credentials_fail_closed(authorization: str | None) -> None:
    with pytest.raises(AuthenticationError):
        parse_bearer_credential(authorization)


def test_unknown_expired_and_revoked_credentials_fail_closed() -> None:
    now = datetime(2026, 8, 19, tzinfo=UTC)
    expired = MissionPrincipalDirectory.from_provisioning(
        (_row(expires_at="2026-08-18T00:00:00Z"),),
        {"MISSION_AGENT_TOKEN": _TOKEN},
    )
    revoked = MissionPrincipalDirectory.from_provisioning(
        (_row(revoked=True),),
        {"MISSION_AGENT_TOKEN": _TOKEN},
    )
    active = MissionPrincipalDirectory.from_provisioning(
        (_row(),),
        {"MISSION_AGENT_TOKEN": _TOKEN},
    )

    with pytest.raises(AuthenticationError):
        expired.authenticate(_TOKEN, now=now)
    with pytest.raises(AuthenticationError):
        revoked.authenticate(_TOKEN, now=now)
    with pytest.raises(AuthenticationError):
        active.authenticate("unknown-credential-" + "Z" * 32, now=now)


def test_non_loopback_missions_host_requires_a_configured_directory() -> None:
    assert is_loopback_host("127.0.0.1")
    assert is_loopback_host("::1")
    assert not is_loopback_host("0.0.0.0")
    assert not is_loopback_host("")
    with pytest.raises(RuntimeError, match="verified principals"):
        MissionPrincipalDirectory.empty().require_non_loopback_configuration("0.0.0.0")


def test_base_only_host_does_not_acquire_missions_auth_configuration(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "catalogs"))
    monkeypatch.setenv("ARCHETYPE_BIND_HOST", "0.0.0.0")
    monkeypatch.setenv("ARCHETYPE_MISSION_PRINCIPALS_PATH", str(tmp_path / "missing.toml"))

    with TestClient(create_app(world_libraries=())) as client:
        assert client.get("/healthz").status_code == 200


def test_non_loopback_missions_host_fails_startup_without_principals(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "catalogs"))
    monkeypatch.setenv("ARCHETYPE_BIND_HOST", "0.0.0.0")
    monkeypatch.delenv("ARCHETYPE_MISSION_PRINCIPALS_PATH", raising=False)
    app = create_app(world_libraries=(get_manifest(),))

    with pytest.raises(RuntimeError, match="verified principals"), TestClient(app):
        pass


def test_auth_and_profile_contract_does_not_publish_a_shadow_run_router() -> None:
    app = create_app(world_libraries=(get_manifest(),))
    paths = {route.path for route in app.routes}
    assert not any(path.startswith("/v1/mission") for path in paths)
