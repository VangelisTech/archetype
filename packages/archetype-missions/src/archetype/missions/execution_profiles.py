# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Versioned, server-owned execution authority for Agent Missions."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from archetype.missions.contracts import AgentMissionConfig, RepositoryPublicationPolicy

_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_REF_FORBIDDEN = re.compile(r"[\x00-\x20\x7f~^:?*\\\[]")

type PositiveInt = Annotated[int, Field(strict=True, ge=1)]
type NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
type MissionConfigFactory = Callable[["ExecutionProfile"], AgentMissionConfig]


def _valid_git_ref(value: str) -> bool:
    if not value or value != value.strip() or _REF_FORBIDDEN.search(value):
        return False
    if value.startswith(("/", ".")) or value.endswith(("/", ".", ".lock")):
        return False
    return not any(fragment in value for fragment in ("..", "//", "@{"))


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ExecutionProfile(_FrozenModel):
    """Canonical policy and provider identity for one immutable profile version."""

    schema_version: Annotated[int, Field(strict=True, ge=1)] = 1
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
    timeout_seconds: PositiveInt
    max_ticks: PositiveInt
    max_retries: NonNegativeInt
    max_concurrency: PositiveInt
    cost_ceiling_usd_cents: NonNegativeInt
    max_validators_per_task: PositiveInt
    max_validator_timeout_seconds: PositiveInt
    publication_policy: RepositoryPublicationPolicy
    checkpoint_after_dispatch: bool
    secret_names: tuple[str, ...] = ()
    provider_credential_names: tuple[str, ...] = ()
    allow_cancel: bool = False
    allow_attach: bool = False
    allow_steer: bool = False
    allow_takeover: bool = False

    @field_validator(
        "profile_id",
        "version",
        "sandbox_backend",
        "sandbox_environment",
        "agent_driver",
        "critic_driver",
        "model",
    )
    @classmethod
    def _non_empty_text(cls, value: str) -> str:
        if not value or value != value.strip():
            raise ValueError("value must be a non-empty, unpadded string")
        return value

    @field_validator(
        "allowed_repositories",
        "allowed_base_refs",
        "secret_names",
        "provider_credential_names",
    )
    @classmethod
    def _canonical_string_set(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value or value != value.strip() for value in values):
            raise ValueError("values must be non-empty, unpadded strings")
        if len(set(values)) != len(values):
            raise ValueError("values must not contain duplicates")
        return tuple(sorted(values))

    @model_validator(mode="after")
    def _validate_authority(self) -> ExecutionProfile:
        if not self.allowed_repositories:
            raise ValueError("allowed_repositories must not be empty")
        if not self.allowed_base_refs:
            raise ValueError("allowed_base_refs must not be empty")
        if not self.branch_namespace.endswith("/") or not _valid_git_ref(
            self.branch_namespace[:-1]
        ):
            raise ValueError("branch_namespace must be a valid non-root Git ref prefix")
        if any(not _valid_git_ref(base_ref) for base_ref in self.allowed_base_refs):
            raise ValueError("allowed_base_refs must contain valid Git refs")
        return self

    @property
    def canonical_payload(self) -> dict[str, Any]:
        """Return the complete secret-free authority document."""

        return self.model_dump(mode="json")

    @property
    def digest(self) -> str:
        encoded = json.dumps(
            self.canonical_payload,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


class ExecutionProfileIdentity(_FrozenModel):
    """Durable identity copied into the authoritative MissionRun record."""

    profile_id: str
    version: str
    digest: str

    @field_validator("profile_id", "version")
    @classmethod
    def _identity_text(cls, value: str) -> str:
        if not value or value != value.strip():
            raise ValueError("profile identity values must be non-empty and unpadded")
        return value

    @field_validator("digest")
    @classmethod
    def _digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None:
            raise ValueError("profile digest must be a lowercase SHA-256 digest")
        return value


class MissionProfileRequest(_FrozenModel):
    """Client-owned coordinates evaluated against a selected server profile."""

    profile_id: str
    repository: str
    branch: str
    base_ref: str = "main"

    @field_validator("profile_id", "repository", "branch", "base_ref")
    @classmethod
    def _request_text(cls, value: str) -> str:
        if not value or value != value.strip():
            raise ValueError("mission coordinates must be non-empty and unpadded")
        return value

    @model_validator(mode="after")
    def _valid_refs(self) -> MissionProfileRequest:
        if not _valid_git_ref(self.branch) or not _valid_git_ref(self.base_ref):
            raise ValueError("mission branch and base_ref must be valid Git refs")
        return self


@dataclass(frozen=True, slots=True)
class ExecutionProfileBinding:
    """Trusted process binding from canonical authority to live provider config."""

    profile: ExecutionProfile
    config_factory: MissionConfigFactory = field(repr=False)

    def __post_init__(self) -> None:
        if not callable(self.config_factory):
            raise TypeError("config_factory must be callable")

    @property
    def identity(self) -> ExecutionProfileIdentity:
        return ExecutionProfileIdentity(
            profile_id=self.profile.profile_id,
            version=self.profile.version,
            digest=self.profile.digest,
        )

    def build_config(self) -> AgentMissionConfig:
        """Materialize live execution config and prove shared fields did not drift."""

        config = self.config_factory(self.profile)
        if not isinstance(config, AgentMissionConfig):
            raise TypeError("execution profile factory must return AgentMissionConfig")
        actual_backend = getattr(config.sandbox_backend, "name", None)
        expected = (
            (actual_backend, self.profile.sandbox_backend, "sandbox backend"),
            (config.sandbox_environment, self.profile.sandbox_environment, "environment"),
            (config.model, self.profile.model, "model"),
            (config.max_ticks, self.profile.max_ticks, "max_ticks"),
            (
                config.checkpoint_after_dispatch,
                self.profile.checkpoint_after_dispatch,
                "checkpoint policy",
            ),
        )
        for actual, declared, label in expected:
            if actual != declared:
                raise ValueError(f"execution profile factory changed declared {label}")
        # Driver comparisons stay unconditional: a factory that drops a declared
        # driver (None) erases declared authority exactly like a mismatched one.
        if getattr(config.driver, "driver_id", None) != self.profile.agent_driver:
            raise ValueError("execution profile factory changed declared agent driver")
        if getattr(config.critic_driver, "driver_id", None) != self.profile.critic_driver:
            raise ValueError("execution profile factory changed declared critic driver")
        return config


@dataclass(frozen=True, slots=True, init=False)
class ExecutionProfileCatalog:
    """Immutable explicit-current catalog retaining every admitted profile version."""

    _bindings: Mapping[tuple[str, str], ExecutionProfileBinding]
    _current: Mapping[str, str]

    def __init__(
        self,
        bindings: tuple[ExecutionProfileBinding, ...] = (),
        *,
        current_versions: Mapping[str, str] | None = None,
    ) -> None:
        by_key: dict[tuple[str, str], ExecutionProfileBinding] = {}
        versions_by_id: dict[str, set[str]] = {}
        for binding in bindings:
            if not isinstance(binding, ExecutionProfileBinding):
                raise TypeError("profile catalog entries must be ExecutionProfileBinding values")
            key = (binding.profile.profile_id, binding.profile.version)
            if key in by_key:
                raise ValueError(f"duplicate execution profile version {key!r}")
            by_key[key] = binding
            versions_by_id.setdefault(key[0], set()).add(key[1])
        current = dict(current_versions or {})
        if set(current) != set(versions_by_id):
            raise ValueError("current_versions must select exactly one version per profile id")
        for profile_id, version in current.items():
            if (profile_id, version) not in by_key:
                raise ValueError(f"current execution profile {(profile_id, version)!r} is missing")
        object.__setattr__(self, "_bindings", MappingProxyType(by_key))
        object.__setattr__(self, "_current", MappingProxyType(current))

    @classmethod
    def empty(cls) -> ExecutionProfileCatalog:
        return cls()

    def resolve(
        self,
        profile_id: str,
        *,
        version: str | None = None,
        digest: str | None = None,
    ) -> ExecutionProfileBinding:
        resolved_version = version if version is not None else self._current.get(profile_id)
        if resolved_version is None:
            raise KeyError(f"execution profile {profile_id!r} is not configured")
        try:
            binding = self._bindings[(profile_id, resolved_version)]
        except KeyError:
            raise KeyError(
                f"execution profile {profile_id!r} version {resolved_version!r} is not configured"
            ) from None
        if digest is not None and not hmac.compare_digest(binding.profile.digest, digest):
            raise ValueError("pinned execution profile digest does not match")
        return binding


def authorize_profile_request(
    binding: ExecutionProfileBinding,
    request: MissionProfileRequest,
) -> ExecutionProfileIdentity:
    """Authorize client coordinates without minting or persisting a run identity."""

    profile = binding.profile
    if request.profile_id != profile.profile_id:
        raise PermissionError("Permission denied")
    if request.repository not in profile.allowed_repositories:
        raise PermissionError("Permission denied")
    if request.base_ref not in profile.allowed_base_refs:
        raise PermissionError("Permission denied")
    if not request.branch.startswith(profile.branch_namespace):
        raise PermissionError("Permission denied")
    return binding.identity


__all__ = [
    "ExecutionProfile",
    "ExecutionProfileBinding",
    "ExecutionProfileCatalog",
    "ExecutionProfileIdentity",
    "MissionConfigFactory",
    "MissionProfileRequest",
    "authorize_profile_request",
]
