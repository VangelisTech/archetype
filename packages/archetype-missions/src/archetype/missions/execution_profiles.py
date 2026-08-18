# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Server-owned, versioned execution profiles for Agent Missions.

Clients supply a ``profile_id``. The host owns provider, sandbox, model,
budget, secret, and interactive-capability choices. An accepted run pins
profile id, version, and canonical digest so later configuration changes
cannot reinterpret that run.
"""

from __future__ import annotations

import hashlib
import json
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archetype.missions.contracts import RepositoryPublicationPolicy

_PROFILES_PATH_ENV = "ARCHETYPE_MISSION_PROFILES_PATH"
_DIGEST_FIELDS = (
    "profile_id",
    "version",
    "allowed_repositories",
    "allowed_base_refs",
    "branch_namespace",
    "sandbox_backend",
    "sandbox_environment",
    "agent_driver",
    "critic_driver",
    "model",
    "timeout_seconds",
    "max_ticks",
    "max_retries",
    "max_concurrency",
    "cost_ceiling_usd_cents",
    "max_validators_per_task",
    "max_validator_timeout_seconds",
    "publication_policy",
    "checkpoint_after_dispatch",
    "secret_names",
    "provider_credential_names",
    "allow_cancel",
    "allow_attach",
    "allow_steer",
    "allow_takeover",
)


def _require_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value.strip() != value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _require_tuple(value: object, *, label: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list | tuple):
        raise ValueError(f"{label} must be a list of strings")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip() or item.strip() != item:
            raise ValueError(f"{label} must contain non-empty strings")
        items.append(item)
    if len(set(items)) != len(items):
        raise ValueError(f"{label} must not contain duplicates")
    return tuple(items)


def _require_positive_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _require_bool(value: object, *, label: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{label} must be a boolean")
    return value


@dataclass(frozen=True, slots=True)
class ExecutionProfile:
    """Immutable host-owned execution authority for one profile version."""

    profile_id: str
    version: str
    allowed_repositories: tuple[str, ...]
    allowed_base_refs: tuple[str, ...]
    branch_namespace: str
    sandbox_backend: str
    sandbox_environment: str
    agent_driver: str
    critic_driver: str
    model: str
    timeout_seconds: int
    max_ticks: int
    max_retries: int
    max_concurrency: int
    cost_ceiling_usd_cents: int
    max_validators_per_task: int
    max_validator_timeout_seconds: int
    publication_policy: RepositoryPublicationPolicy
    checkpoint_after_dispatch: bool
    secret_names: tuple[str, ...]
    provider_credential_names: tuple[str, ...]
    allow_cancel: bool
    allow_attach: bool
    allow_steer: bool
    allow_takeover: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile_id", _require_text(self.profile_id, label="profile_id"))
        object.__setattr__(self, "version", _require_text(self.version, label="version"))
        repositories = _require_tuple(
            self.allowed_repositories,
            label="allowed_repositories",
        )
        if not repositories:
            raise ValueError("allowed_repositories must not be empty")
        object.__setattr__(self, "allowed_repositories", repositories)
        refs = _require_tuple(self.allowed_base_refs, label="allowed_base_refs")
        if not refs:
            raise ValueError("allowed_base_refs must not be empty")
        object.__setattr__(self, "allowed_base_refs", refs)
        object.__setattr__(
            self,
            "branch_namespace",
            _require_text(self.branch_namespace, label="branch_namespace"),
        )
        object.__setattr__(
            self,
            "sandbox_backend",
            _require_text(self.sandbox_backend, label="sandbox_backend"),
        )
        object.__setattr__(
            self,
            "sandbox_environment",
            _require_text(self.sandbox_environment, label="sandbox_environment"),
        )
        object.__setattr__(
            self,
            "agent_driver",
            _require_text(self.agent_driver, label="agent_driver"),
        )
        object.__setattr__(
            self,
            "critic_driver",
            _require_text(self.critic_driver, label="critic_driver"),
        )
        object.__setattr__(self, "model", _require_text(self.model, label="model"))
        object.__setattr__(
            self,
            "timeout_seconds",
            _require_positive_int(self.timeout_seconds, label="timeout_seconds"),
        )
        object.__setattr__(
            self,
            "max_ticks",
            _require_positive_int(self.max_ticks, label="max_ticks"),
        )
        object.__setattr__(
            self,
            "max_retries",
            _require_positive_int(self.max_retries, label="max_retries"),
        )
        object.__setattr__(
            self,
            "max_concurrency",
            _require_positive_int(self.max_concurrency, label="max_concurrency"),
        )
        object.__setattr__(
            self,
            "cost_ceiling_usd_cents",
            _require_positive_int(
                self.cost_ceiling_usd_cents,
                label="cost_ceiling_usd_cents",
            ),
        )
        object.__setattr__(
            self,
            "max_validators_per_task",
            _require_positive_int(
                self.max_validators_per_task,
                label="max_validators_per_task",
            ),
        )
        object.__setattr__(
            self,
            "max_validator_timeout_seconds",
            _require_positive_int(
                self.max_validator_timeout_seconds,
                label="max_validator_timeout_seconds",
            ),
        )
        try:
            policy = RepositoryPublicationPolicy(self.publication_policy)
        except ValueError as exc:
            raise ValueError("unsupported publication_policy") from exc
        object.__setattr__(self, "publication_policy", policy)
        object.__setattr__(
            self,
            "checkpoint_after_dispatch",
            _require_bool(
                self.checkpoint_after_dispatch,
                label="checkpoint_after_dispatch",
            ),
        )
        object.__setattr__(
            self,
            "secret_names",
            _require_tuple(self.secret_names, label="secret_names"),
        )
        object.__setattr__(
            self,
            "provider_credential_names",
            _require_tuple(
                self.provider_credential_names,
                label="provider_credential_names",
            ),
        )
        for flag in ("allow_cancel", "allow_attach", "allow_steer", "allow_takeover"):
            object.__setattr__(self, flag, _require_bool(getattr(self, flag), label=flag))

    @property
    def canonical_payload(self) -> dict[str, Any]:
        """Return the digestible profile document."""

        payload: dict[str, Any] = {}
        for field_name in _DIGEST_FIELDS:
            value = getattr(self, field_name)
            if isinstance(value, tuple):
                payload[field_name] = list(value)
            elif isinstance(value, RepositoryPublicationPolicy):
                payload[field_name] = str(value)
            else:
                payload[field_name] = value
        return payload

    @property
    def digest(self) -> str:
        """Return the canonical identity of this profile version."""

        encoded = json.dumps(
            self.canonical_payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class PinnedExecutionProfile:
    """Accepted profile identity that cannot be reinterpreted later."""

    profile_id: str
    version: str
    digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile_id", _require_text(self.profile_id, label="profile_id"))
        object.__setattr__(self, "version", _require_text(self.version, label="version"))
        object.__setattr__(self, "digest", _require_text(self.digest, label="digest"))


@dataclass(frozen=True, slots=True)
class MissionProfileRequest:
    """Client-supplied coordinates that a profile may admit."""

    profile_id: str
    repository: str
    branch: str
    base_ref: str = "main"

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile_id", _require_text(self.profile_id, label="profile_id"))
        object.__setattr__(self, "repository", _require_text(self.repository, label="repository"))
        object.__setattr__(self, "branch", _require_text(self.branch, label="branch"))
        object.__setattr__(self, "base_ref", _require_text(self.base_ref, label="base_ref"))


def pin_profile(profile: ExecutionProfile) -> PinnedExecutionProfile:
    """Pin the exact id, version, and digest of one profile."""

    return PinnedExecutionProfile(
        profile_id=profile.profile_id,
        version=profile.version,
        digest=profile.digest,
    )


def authorize_profile_request(
    profile: ExecutionProfile,
    request: MissionProfileRequest,
) -> PinnedExecutionProfile:
    """Admit client coordinates that stay inside the selected profile."""

    if request.profile_id != profile.profile_id:
        raise PermissionError("Permission denied")
    if request.repository not in profile.allowed_repositories:
        raise PermissionError("Permission denied")
    if request.base_ref not in profile.allowed_base_refs:
        raise PermissionError("Permission denied")
    if not request.branch.startswith(profile.branch_namespace):
        raise PermissionError("Permission denied")
    return pin_profile(profile)


def _profile_from_mapping(raw: Mapping[str, Any]) -> ExecutionProfile:
    return ExecutionProfile(
        profile_id=_require_text(raw.get("profile_id"), label="profile_id"),
        version=_require_text(raw.get("version"), label="version"),
        allowed_repositories=_require_tuple(
            raw.get("allowed_repositories"),
            label="allowed_repositories",
        ),
        allowed_base_refs=_require_tuple(raw.get("allowed_base_refs"), label="allowed_base_refs"),
        branch_namespace=_require_text(raw.get("branch_namespace"), label="branch_namespace"),
        sandbox_backend=_require_text(raw.get("sandbox_backend"), label="sandbox_backend"),
        sandbox_environment=_require_text(
            raw.get("sandbox_environment"),
            label="sandbox_environment",
        ),
        agent_driver=_require_text(raw.get("agent_driver"), label="agent_driver"),
        critic_driver=_require_text(raw.get("critic_driver"), label="critic_driver"),
        model=_require_text(raw.get("model"), label="model"),
        timeout_seconds=_require_positive_int(raw.get("timeout_seconds"), label="timeout_seconds"),
        max_ticks=_require_positive_int(raw.get("max_ticks"), label="max_ticks"),
        max_retries=_require_positive_int(raw.get("max_retries"), label="max_retries"),
        max_concurrency=_require_positive_int(raw.get("max_concurrency"), label="max_concurrency"),
        cost_ceiling_usd_cents=_require_positive_int(
            raw.get("cost_ceiling_usd_cents"),
            label="cost_ceiling_usd_cents",
        ),
        max_validators_per_task=_require_positive_int(
            raw.get("max_validators_per_task"),
            label="max_validators_per_task",
        ),
        max_validator_timeout_seconds=_require_positive_int(
            raw.get("max_validator_timeout_seconds"),
            label="max_validator_timeout_seconds",
        ),
        publication_policy=raw.get("publication_policy", "commit_and_push"),
        checkpoint_after_dispatch=_require_bool(
            raw.get("checkpoint_after_dispatch", True),
            label="checkpoint_after_dispatch",
        ),
        secret_names=_require_tuple(raw.get("secret_names"), label="secret_names"),
        provider_credential_names=_require_tuple(
            raw.get("provider_credential_names"),
            label="provider_credential_names",
        ),
        allow_cancel=_require_bool(raw.get("allow_cancel", False), label="allow_cancel"),
        allow_attach=_require_bool(raw.get("allow_attach", False), label="allow_attach"),
        allow_steer=_require_bool(raw.get("allow_steer", False), label="allow_steer"),
        allow_takeover=_require_bool(raw.get("allow_takeover", False), label="allow_takeover"),
    )


@dataclass(frozen=True, slots=True)
class ExecutionProfileCatalog:
    """Immutable (id, version) catalog with a current version per profile id."""

    _profiles: dict[tuple[str, str], ExecutionProfile]
    _current: dict[str, str]

    def __init__(
        self,
        profiles: tuple[ExecutionProfile, ...] = (),
    ) -> None:
        by_key: dict[tuple[str, str], ExecutionProfile] = {}
        current: dict[str, str] = {}
        for profile in profiles:
            key = (profile.profile_id, profile.version)
            existing = by_key.get(key)
            if existing is not None and existing.digest != profile.digest:
                raise ValueError(
                    f"profile {profile.profile_id!r} version {profile.version!r} "
                    "changed its canonical digest"
                )
            by_key[key] = profile
            current[profile.profile_id] = profile.version
        object.__setattr__(self, "_profiles", by_key)
        object.__setattr__(self, "_current", current)

    @classmethod
    def empty(cls) -> ExecutionProfileCatalog:
        """Return a fail-closed catalog with no profiles."""

        return cls()

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> ExecutionProfileCatalog:
        """Load profiles from ``ARCHETYPE_MISSION_PROFILES_PATH`` if set."""

        source = os.environ if environ is None else environ
        configured = source.get(_PROFILES_PATH_ENV, "").strip()
        if not configured:
            return cls.empty()
        path = Path(configured).expanduser()
        document = tomllib.loads(path.read_text(encoding="utf-8"))
        rows = document.get("profile", [])
        if not isinstance(rows, list):
            raise ValueError("mission profile document must contain [[profile]] tables")
        return cls(tuple(_profile_from_mapping(row) for row in rows))

    def resolve(
        self,
        profile_id: str,
        *,
        version: str | None = None,
        digest: str | None = None,
    ) -> ExecutionProfile:
        """Return one immutable profile version, optionally checking its digest."""

        resolved_version = version or self._current.get(profile_id)
        if resolved_version is None:
            raise KeyError(f"execution profile {profile_id!r} is not configured")
        try:
            profile = self._profiles[(profile_id, resolved_version)]
        except KeyError:
            raise KeyError(
                f"execution profile {profile_id!r} version {resolved_version!r} is not configured"
            ) from None
        if digest is not None and profile.digest != digest:
            raise ValueError("pinned execution profile digest does not match")
        return profile

    def pin(
        self,
        profile_id: str,
        *,
        version: str | None = None,
    ) -> PinnedExecutionProfile:
        """Resolve and pin one profile version."""

        return pin_profile(self.resolve(profile_id, version=version))


__all__ = [
    "ExecutionProfile",
    "ExecutionProfileCatalog",
    "MissionProfileRequest",
    "PinnedExecutionProfile",
    "authorize_profile_request",
    "pin_profile",
]
