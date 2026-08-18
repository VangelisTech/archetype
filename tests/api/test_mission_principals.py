# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verified mission service-principal contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from archetype.api.principals import (
    AuthenticationError,
    MissionPrincipal,
    MissionPrincipalDirectory,
    is_loopback_host,
    parse_bearer_credential,
)

_TOKEN = "mission-agent-credential-aaaa"
_OTHER = "mission-reader-credential-bbbb"


def _directory(**overrides: object) -> MissionPrincipalDirectory:
    row = {
        "id": "agent",
        "token_env": "ARCHETYPE_MISSION_PRINCIPAL_AGENT_TOKEN",
        "capabilities": ["mission:submit", "mission:read"],
        "allowed_profile_ids": ["coding-default"],
        **overrides,
    }
    return MissionPrincipalDirectory.from_provisioning(
        (row,),
        {"ARCHETYPE_MISSION_PRINCIPAL_AGENT_TOKEN": _TOKEN},
    )


def test_missing_and_malformed_credentials_fail_closed() -> None:
    with pytest.raises(AuthenticationError, match="authentication required"):
        parse_bearer_credential(None)
    with pytest.raises(AuthenticationError, match="authentication required"):
        parse_bearer_credential("   ")
    with pytest.raises(AuthenticationError, match="invalid credentials"):
        parse_bearer_credential("Basic abc")
    with pytest.raises(AuthenticationError, match="invalid credentials"):
        parse_bearer_credential("Bearer")
    with pytest.raises(AuthenticationError, match="invalid credentials"):
        parse_bearer_credential("Bearer admin")
    with pytest.raises(AuthenticationError, match="invalid credentials"):
        parse_bearer_credential("Bearer operator")


def test_role_labels_are_never_accepted_as_credentials() -> None:
    directory = _directory()
    for role in ("admin", "operator", "player", "viewer"):
        with pytest.raises(AuthenticationError, match="invalid credentials"):
            directory.authenticate(role)
        with pytest.raises(AuthenticationError):
            parse_bearer_credential(f"Bearer {role}")


def test_unknown_expired_and_revoked_credentials_fail_closed() -> None:
    directory = _directory()
    with pytest.raises(AuthenticationError, match="invalid credentials"):
        directory.authenticate(_OTHER)

    expired = _directory(expires_at=(datetime.now(UTC) - timedelta(seconds=1)).isoformat())
    with pytest.raises(AuthenticationError, match="invalid credentials"):
        expired.authenticate(_TOKEN)

    revoked = _directory(revoked=True)
    with pytest.raises(AuthenticationError, match="invalid credentials"):
        revoked.authenticate(_TOKEN)


def test_empty_directory_fails_closed() -> None:
    with pytest.raises(AuthenticationError, match="invalid credentials"):
        MissionPrincipalDirectory.empty().authenticate(_TOKEN)


def test_valid_principal_is_stable_and_does_not_echo_the_credential() -> None:
    directory = _directory()
    principal = directory.authenticate(_TOKEN)
    again = directory.authenticate(_TOKEN)

    assert principal == again
    assert principal.principal_id == "agent"
    assert principal.capabilities == frozenset({"mission:submit", "mission:read"})
    assert _TOKEN not in repr(directory)
    assert _TOKEN not in repr(principal)
    assert _TOKEN not in str(directory)


def test_principal_id_cannot_be_a_developer_role() -> None:
    with pytest.raises(ValueError, match="developer role"):
        MissionPrincipal(
            principal_id="admin",
            capabilities=frozenset({"mission:read"}),
            allowed_profile_ids=frozenset({"coding-default"}),
        )


def test_loopback_detection_and_non_loopback_fail_closed() -> None:
    assert is_loopback_host("127.0.0.1")
    assert is_loopback_host("localhost")
    assert is_loopback_host("::1")
    assert not is_loopback_host("0.0.0.0")
    assert not is_loopback_host("192.168.1.9")

    empty = MissionPrincipalDirectory.empty()
    empty.require_non_loopback_configuration("127.0.0.1")
    with pytest.raises(RuntimeError, match="non-loopback mission hosting"):
        empty.require_non_loopback_configuration("0.0.0.0")
    _directory().require_non_loopback_configuration("0.0.0.0")


def test_errors_never_include_credential_material() -> None:
    directory = _directory()
    with pytest.raises(AuthenticationError) as caught:
        directory.authenticate(_TOKEN + "-forged")
    assert _TOKEN not in str(caught.value)
    assert _TOKEN not in repr(caught.value)
