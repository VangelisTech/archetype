# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verified mission service principals for the API transport boundary.

Developer-mode ``Bearer <role>`` identity remains on existing ECS routes.
Mission-control authenticates an opaque credential to a stable principal and
never treats a role label as proof of identity.
"""

from __future__ import annotations

import hmac
import ipaddress
import os
import secrets
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


class AuthenticationError(Exception):
    """Closed authentication failure that must not carry credential material."""


def is_loopback_host(host: str) -> bool:
    """Return whether ``host`` is a loopback bind or address."""

    normalized = host.strip().lower().strip("[]")
    if not normalized or normalized == "localhost":
        return True
    if normalized in {"::1", "0:0:0:0:0:0:0:1"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def parse_bearer_credential(authorization: str | None) -> str:
    """Return the bearer credential or raise a closed authentication error."""

    if authorization is None or not authorization.strip():
        raise AuthenticationError("authentication required")
    scheme, separator, token = authorization.partition(" ")
    if separator == "" or scheme.lower() != "bearer" or not token.strip():
        raise AuthenticationError("invalid credentials")
    credential = token.strip()
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
    items: list[str] = []
    for item in values:
        if not isinstance(item, str) or not item.strip() or item.strip() != item:
            raise ValueError(f"{label} must contain non-empty strings")
        items.append(item)
    if len(set(items)) != len(items):
        raise ValueError(f"{label} must not contain duplicates")
    return frozenset(items)


def _verifier(credential: str, salt: bytes) -> bytes:
    return hmac.new(salt, credential.encode("utf-8"), "sha256").digest()


@dataclass(frozen=True, slots=True)
class MissionPrincipal:
    """Stable authenticated identity for mission-control and interactive routes."""

    principal_id: str
    capabilities: frozenset[str]
    allowed_profile_ids: frozenset[str]
    granted_run_ids: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        principal_id = _require_identifier(self.principal_id, label="principal_id")
        if principal_id.lower() in DEVELOPER_ROLE_LABELS:
            raise ValueError("principal_id must not be a developer role label")
        unknown = sorted(self.capabilities - MISSION_CAPABILITIES)
        if unknown:
            raise ValueError(f"unsupported mission capabilities: {', '.join(unknown)}")
        object.__setattr__(self, "principal_id", principal_id)
        object.__setattr__(self, "capabilities", frozenset(self.capabilities))
        object.__setattr__(
            self,
            "allowed_profile_ids",
            frozenset(self.allowed_profile_ids),
        )
        object.__setattr__(self, "granted_run_ids", frozenset(self.granted_run_ids))

    def has_capability(self, capability: str) -> bool:
        """Return whether this principal was granted ``capability``."""

        return capability in self.capabilities


@dataclass(frozen=True, slots=True)
class _PrincipalRecord:
    principal: MissionPrincipal
    salt: bytes = field(repr=False)
    verifier: bytes = field(repr=False)
    expires_at: datetime | None = None
    revoked: bool = False


def _parse_expiry(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("expires_at must be an RFC 3339 timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("expires_at must include a timezone")
    return parsed.astimezone(UTC)


def _credential_from_mapping(
    raw: Mapping[str, Any],
    environ: Mapping[str, str],
) -> str:
    token_env = raw.get("token_env")
    if token_env is not None:
        name = _require_identifier(token_env, label="token_env")
        credential = environ.get(name, "")
        if not credential.strip():
            raise RuntimeError(f"{name} is required for mission principal provisioning")
        return credential.strip()
    credential = raw.get("credential")
    if isinstance(credential, str) and credential.strip():
        return credential.strip()
    raise ValueError("principal requires token_env or credential at provisioning time")


def _record_from_mapping(
    raw: Mapping[str, Any],
    environ: Mapping[str, str],
) -> _PrincipalRecord:
    principal = MissionPrincipal(
        principal_id=_require_identifier(raw.get("id"), label="principal id"),
        capabilities=_frozen_strings(raw.get("capabilities"), label="capabilities"),
        allowed_profile_ids=_frozen_strings(
            raw.get("allowed_profile_ids"),
            label="allowed_profile_ids",
        ),
        granted_run_ids=_frozen_strings(raw.get("granted_run_ids"), label="granted_run_ids"),
    )
    credential = _credential_from_mapping(raw, environ)
    if len(credential) < _MIN_CREDENTIAL_LENGTH:
        raise ValueError("mission principal credential is too short")
    if credential.lower() in DEVELOPER_ROLE_LABELS:
        raise ValueError("mission principal credential must not be a role label")
    salt = secrets.token_bytes(16)
    return _PrincipalRecord(
        principal=principal,
        salt=salt,
        verifier=_verifier(credential, salt),
        expires_at=_parse_expiry(raw.get("expires_at")),
        revoked=bool(raw.get("revoked", False)),
    )


@dataclass(frozen=True, slots=True)
class MissionPrincipalDirectory:
    """Process-owned verifier set for mission service principals."""

    _records: tuple[_PrincipalRecord, ...] = ()

    def __post_init__(self) -> None:
        ids = [record.principal.principal_id for record in self._records]
        if len(set(ids)) != len(ids):
            raise ValueError("mission principal ids must be unique")

    @classmethod
    def empty(cls) -> MissionPrincipalDirectory:
        """Return a fail-closed directory with no verifiers."""

        return cls()

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> MissionPrincipalDirectory:
        """Load principals from ``ARCHETYPE_MISSION_PRINCIPALS_PATH`` if set."""

        source = os.environ if environ is None else environ
        configured = source.get(_PRINCIPALS_PATH_ENV, "").strip()
        if not configured:
            return cls.empty()
        path = Path(configured).expanduser()
        document = tomllib.loads(path.read_text(encoding="utf-8"))
        rows = document.get("principal", [])
        if not isinstance(rows, list):
            raise ValueError("mission principal document must contain [[principal]] tables")
        records = tuple(_record_from_mapping(row, source) for row in rows)
        return cls(records)

    @classmethod
    def from_provisioning(
        cls,
        rows: tuple[Mapping[str, Any], ...],
        environ: Mapping[str, str] | None = None,
    ) -> MissionPrincipalDirectory:
        """Build a directory from provisioning rows, discarding raw credentials."""

        source = os.environ if environ is None else environ
        return cls(tuple(_record_from_mapping(row, source) for row in rows))

    @property
    def configured(self) -> bool:
        """Whether at least one verifier is installed."""

        return bool(self._records)

    def authenticate(
        self,
        credential: str,
        *,
        now: datetime | None = None,
    ) -> MissionPrincipal:
        """Resolve a credential to a principal or fail closed."""

        if not self._records:
            raise AuthenticationError("invalid credentials")
        offered = credential.encode("utf-8")
        matched: _PrincipalRecord | None = None
        for record in self._records:
            expected = hmac.new(record.salt, offered, "sha256").digest()
            if hmac.compare_digest(expected, record.verifier):
                matched = record
                break
        if matched is None:
            raise AuthenticationError("invalid credentials")
        if matched.revoked:
            raise AuthenticationError("invalid credentials")
        expires_at = matched.expires_at
        if expires_at is not None and (now or _utc_now()) >= expires_at:
            raise AuthenticationError("invalid credentials")
        return matched.principal

    def require_non_loopback_configuration(self, bind_host: str) -> None:
        """Refuse unauthenticated mission hosting on a non-loopback bind."""

        if not is_loopback_host(bind_host) and not self.configured:
            raise RuntimeError("non-loopback mission hosting requires verified mission principals")


def bind_host_from_env(environ: Mapping[str, str] | None = None) -> str:
    """Return the process bind host captured at server start."""

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
