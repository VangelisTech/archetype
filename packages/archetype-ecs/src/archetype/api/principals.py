# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verified service principals for future mission-control transports.

Existing ECS routes intentionally retain their loopback developer identity.
Mission-control transports use this separate verifier-backed directory and
never interpret a role label as proof of identity.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import os
import string
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEVELOPER_ROLE_LABELS = frozenset({"admin", "operator", "player", "viewer"})
MISSION_CAPABILITIES = frozenset(
    {
        "mission:submit",
        "mission:read",
        "mission:cancel",
        "mission:attach",
        "mission:steer",
        "mission:takeover",
    }
)
_MIN_CREDENTIAL_LENGTH = 24
_PRINCIPALS_PATH_ENV = "ARCHETYPE_MISSION_PRINCIPALS_PATH"
_BIND_HOST_ENV = "ARCHETYPE_BIND_HOST"
_SHA256_HEX_LENGTH = 64


class AuthenticationError(Exception):
    """Closed authentication failure that never carries credential material."""


def is_loopback_host(host: str) -> bool:
    """Return whether ``host`` names a loopback bind."""

    normalized = host.strip().lower().strip("[]")
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def parse_bearer_credential(authorization: str | None) -> str:
    """Extract one opaque bearer credential or fail closed."""

    if authorization is None:
        raise AuthenticationError("authentication required")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise AuthenticationError("invalid credentials")
    credential = parts[1]
    if len(credential) < _MIN_CREDENTIAL_LENGTH:
        raise AuthenticationError("invalid credentials")
    if credential.lower() in DEVELOPER_ROLE_LABELS:
        raise AuthenticationError("invalid credentials")
    return credential


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _require_identifier(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value.strip() != value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _frozen_strings(values: object, *, label: str) -> frozenset[str]:
    if values is None:
        return frozenset()
    if not isinstance(values, list | tuple | set | frozenset):
        raise ValueError(f"{label} must be a list of strings")
    items = tuple(_require_identifier(item, label=label) for item in values)
    if len(set(items)) != len(items):
        raise ValueError(f"{label} must not contain duplicates")
    return frozenset(items)


def _require_bool(value: object, *, label: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{label} must be a boolean")
    return value


def _parse_expiry(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("expires_at must be an RFC 3339 timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("expires_at must include a timezone")
    return parsed.astimezone(UTC)


def _credential_verifier(credential: str) -> bytes:
    return hashlib.sha256(credential.encode("utf-8")).digest()


def _verifier_from_mapping(raw: Mapping[str, Any], environ: Mapping[str, str]) -> bytes:
    token_env = raw.get("token_env")
    verifier_hex = raw.get("credential_sha256")
    if (token_env is None) == (verifier_hex is None):
        raise ValueError("principal requires exactly one of token_env or credential_sha256")
    if token_env is not None:
        name = _require_identifier(token_env, label="token_env")
        credential = environ.get(name, "")
        if (
            len(credential) < _MIN_CREDENTIAL_LENGTH
            or credential.strip() != credential
            or any(character.isspace() for character in credential)
        ):
            raise RuntimeError(f"{name} must contain a strong opaque mission credential")
        if credential.lower() in DEVELOPER_ROLE_LABELS:
            raise ValueError("mission credential must not be a developer role label")
        return _credential_verifier(credential)
    if not isinstance(verifier_hex, str):
        raise ValueError("credential_sha256 must be a lowercase SHA-256 verifier")
    if (
        len(verifier_hex) != _SHA256_HEX_LENGTH
        or verifier_hex.lower() != verifier_hex
        or any(character not in string.hexdigits for character in verifier_hex)
    ):
        raise ValueError("credential_sha256 must be a lowercase SHA-256 verifier")
    return bytes.fromhex(verifier_hex)


@dataclass(frozen=True, slots=True)
class MissionPrincipal:
    """Stable authenticated identity and its explicit mission grants."""

    principal_id: str
    capabilities: frozenset[str]
    allowed_profile_ids: frozenset[str]

    def __post_init__(self) -> None:
        principal_id = _require_identifier(self.principal_id, label="principal_id")
        if principal_id.lower() in DEVELOPER_ROLE_LABELS:
            raise ValueError("principal_id must not be a developer role label")
        capabilities = frozenset(self.capabilities)
        unknown = sorted(capabilities - MISSION_CAPABILITIES)
        if unknown:
            raise ValueError(f"unsupported mission capabilities: {', '.join(unknown)}")
        object.__setattr__(self, "principal_id", principal_id)
        object.__setattr__(self, "capabilities", capabilities)
        object.__setattr__(self, "allowed_profile_ids", frozenset(self.allowed_profile_ids))


@dataclass(frozen=True, slots=True)
class _PrincipalRecord:
    principal: MissionPrincipal
    verifier: bytes = field(repr=False)
    expires_at: datetime | None = None
    revoked: bool = False


def _record_from_mapping(
    raw: Mapping[str, Any],
    environ: Mapping[str, str],
) -> _PrincipalRecord:
    return _PrincipalRecord(
        principal=MissionPrincipal(
            principal_id=_require_identifier(raw.get("id"), label="principal id"),
            capabilities=_frozen_strings(raw.get("capabilities"), label="capabilities"),
            allowed_profile_ids=_frozen_strings(
                raw.get("allowed_profile_ids"),
                label="allowed_profile_ids",
            ),
        ),
        verifier=_verifier_from_mapping(raw, environ),
        expires_at=_parse_expiry(raw.get("expires_at")),
        revoked=_require_bool(raw.get("revoked", False), label="revoked"),
    )


@dataclass(frozen=True, slots=True)
class MissionPrincipalDirectory:
    """Immutable process-owned verifier set for mission service principals."""

    _records: tuple[_PrincipalRecord, ...] = ()

    def __post_init__(self) -> None:
        ids = tuple(record.principal.principal_id for record in self._records)
        verifiers = tuple(record.verifier for record in self._records)
        if len(set(ids)) != len(ids):
            raise ValueError("mission principal ids must be unique")
        if len(set(verifiers)) != len(verifiers):
            raise ValueError("mission principal credentials must be unique")

    @classmethod
    def empty(cls) -> MissionPrincipalDirectory:
        return cls()

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> MissionPrincipalDirectory:
        """Load verifier records from the configured provisioning document."""

        source = os.environ if environ is None else environ
        configured = source.get(_PRINCIPALS_PATH_ENV, "").strip()
        if not configured:
            return cls.empty()
        document = tomllib.loads(Path(configured).expanduser().read_text(encoding="utf-8"))
        rows = document.get("principal", [])
        if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
            raise ValueError("mission principal document must contain [[principal]] tables")
        return cls(tuple(_record_from_mapping(row, source) for row in rows))

    @classmethod
    def from_provisioning(
        cls,
        rows: tuple[Mapping[str, Any], ...],
        environ: Mapping[str, str] | None = None,
    ) -> MissionPrincipalDirectory:
        source = os.environ if environ is None else environ
        return cls(tuple(_record_from_mapping(row, source) for row in rows))

    @property
    def configured(self) -> bool:
        return bool(self._records)

    def authenticate(
        self,
        credential: str,
        *,
        now: datetime | None = None,
    ) -> MissionPrincipal:
        """Resolve a credential without retaining or reflecting it."""

        offered = _credential_verifier(credential)
        matched: _PrincipalRecord | None = None
        for record in self._records:
            if hmac.compare_digest(offered, record.verifier):
                matched = record
        if matched is None or matched.revoked:
            raise AuthenticationError("invalid credentials")
        if matched.expires_at is not None and (now or _utc_now()) >= matched.expires_at:
            raise AuthenticationError("invalid credentials")
        return matched.principal

    def require_non_loopback_configuration(self, bind_host: str) -> None:
        """Fail process startup when a Missions host is remotely exposed without auth."""

        if not is_loopback_host(bind_host) and not self.configured:
            raise RuntimeError("non-loopback mission hosting requires verified principals")


def bind_host_from_env(environ: Mapping[str, str] | None = None) -> str:
    source = os.environ if environ is None else environ
    return source.get(_BIND_HOST_ENV, "127.0.0.1").strip() or "127.0.0.1"


__all__ = [
    "DEVELOPER_ROLE_LABELS",
    "MISSION_CAPABILITIES",
    "AuthenticationError",
    "MissionPrincipal",
    "MissionPrincipalDirectory",
    "bind_host_from_env",
    "is_loopback_host",
    "parse_bearer_credential",
]
