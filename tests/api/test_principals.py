# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verified service-principal transport contracts."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from archetype.api.app import create_app
from archetype.api.principals import (
    AuthenticationError,
    PrincipalDirectory,
    bind_host_from_env,
    is_loopback_host,
    parse_bearer_credential,
)

_TOKEN = "service-credential-" + "A" * 32


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": "agent",
        "token_env": "SERVICE_TOKEN",
        "capabilities": ["simulation:submit", "simulation:read"],
    }
    row.update(overrides)
    return row


def test_opaque_credential_resolves_to_stable_explicit_claims() -> None:
    directory = PrincipalDirectory.from_provisioning(
        (_row(),),
        {"SERVICE_TOKEN": _TOKEN},
    )

    principal = directory.authenticate(_TOKEN)

    assert principal.principal_id == "agent"
    assert principal.capabilities == {"simulation:submit", "simulation:read"}
    assert _TOKEN not in repr(directory)


def test_provisioning_accepts_a_verifier_but_not_a_stored_plaintext_credential() -> None:
    verifier = hashlib.sha256(_TOKEN.encode()).hexdigest()
    directory = PrincipalDirectory.from_provisioning(
        (_row(token_env=None, credential_sha256=verifier),),
        {},
    )
    assert directory.authenticate(_TOKEN).principal_id == "agent"

    plaintext = _row(token_env=None, credential=_TOKEN)
    with pytest.raises(ValueError, match="exactly one"):
        PrincipalDirectory.from_provisioning((plaintext,), {})


@pytest.mark.parametrize(
    "authorization",
    [None, "", "Basic abc", "Bearer admin", "Bearer short", "Bearer token with-space"],
)
def test_malformed_and_role_label_credentials_fail_closed(authorization: str | None) -> None:
    with pytest.raises(AuthenticationError):
        parse_bearer_credential(authorization)


def test_unknown_expired_and_revoked_credentials_fail_closed() -> None:
    now = datetime(2026, 8, 19, tzinfo=UTC)
    expired = PrincipalDirectory.from_provisioning(
        (_row(expires_at="2026-08-18T00:00:00Z"),),
        {"SERVICE_TOKEN": _TOKEN},
    )
    revoked = PrincipalDirectory.from_provisioning(
        (_row(revoked=True),),
        {"SERVICE_TOKEN": _TOKEN},
    )
    active = PrincipalDirectory.from_provisioning(
        (_row(),),
        {"SERVICE_TOKEN": _TOKEN},
    )

    with pytest.raises(AuthenticationError):
        expired.authenticate(_TOKEN, now=now)
    with pytest.raises(AuthenticationError):
        revoked.authenticate(_TOKEN, now=now)
    with pytest.raises(AuthenticationError):
        active.authenticate("unknown-credential-" + "Z" * 32, now=now)


def test_expiry_accepts_native_toml_datetimes_and_requires_a_timezone(tmp_path) -> None:
    document = tmp_path / "principals.toml"
    document.write_text(
        "\n".join(
            (
                "[[principal]]",
                'id = "agent"',
                'token_env = "SERVICE_TOKEN"',
                'capabilities = ["simulation:submit"]',
                "expires_at = 2026-08-18T00:00:00Z",
            )
        ),
        encoding="utf-8",
    )

    directory = PrincipalDirectory.from_env(
        {
            "ARCHETYPE_PRINCIPALS_PATH": str(document),
            "SERVICE_TOKEN": _TOKEN,
        }
    )

    active = directory.authenticate(_TOKEN, now=datetime(2026, 8, 17, tzinfo=UTC))
    assert active.principal_id == "agent"
    with pytest.raises(AuthenticationError):
        directory.authenticate(_TOKEN, now=datetime(2026, 8, 19, tzinfo=UTC))

    with pytest.raises(ValueError, match="timezone"):
        PrincipalDirectory.from_provisioning(
            (_row(expires_at=datetime(2026, 8, 18)),),
            {"SERVICE_TOKEN": _TOKEN},
        )


def test_undeclared_bind_host_stays_fail_closed_and_loopback_is_explicit() -> None:
    assert bind_host_from_env({}) == ""
    assert not is_loopback_host(bind_host_from_env({}))
    assert bind_host_from_env({"ARCHETYPE_BIND_HOST": " 127.0.0.1 "}) == "127.0.0.1"

    with pytest.raises(RuntimeError, match="verified principals"):
        PrincipalDirectory.empty().require_non_loopback_configuration(bind_host_from_env({}))
    PrincipalDirectory.empty().require_non_loopback_configuration(
        bind_host_from_env({"ARCHETYPE_BIND_HOST": "127.0.0.1"})
    )


def test_non_loopback_host_requires_a_configured_directory() -> None:
    assert is_loopback_host("127.0.0.1")
    assert is_loopback_host("::1")
    assert not is_loopback_host("0.0.0.0")
    assert not is_loopback_host("")
    with pytest.raises(RuntimeError, match="verified principals"):
        PrincipalDirectory.empty().require_non_loopback_configuration("0.0.0.0")


def test_developer_host_does_not_implicitly_acquire_service_auth_configuration(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "catalogs"))
    monkeypatch.setenv("ARCHETYPE_BIND_HOST", "0.0.0.0")
    monkeypatch.setenv("ARCHETYPE_PRINCIPALS_PATH", str(tmp_path / "missing.toml"))

    with TestClient(create_app(world_libraries=())) as client:
        assert client.get("/healthz").status_code == 200
