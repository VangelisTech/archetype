# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Server-owned execution profile and Missions authorization contracts."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

import pytest

from archetype.api.principals import MissionPrincipal
from archetype.missions.authorization import MISSION_CAPABILITY, MissionAuthorizer
from archetype.missions.contracts import AgentMissionConfig
from archetype.missions.execution_profiles import (
    ExecutionProfile,
    ExecutionProfileBinding,
    ExecutionProfileCatalog,
    MissionProfileRequest,
)


def _profile(**overrides: object) -> ExecutionProfile:
    values: dict[str, object] = {
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
        "secret_names": ("codex-auth",),
        "provider_credential_names": ("modal",),
        "allow_cancel": True,
        "allow_attach": True,
        "allow_steer": True,
        "allow_takeover": True,
    }
    values.update(overrides)
    return ExecutionProfile.model_validate(values)


def _config(profile: ExecutionProfile, **overrides: Any) -> AgentMissionConfig:
    values: dict[str, Any] = {
        "sandbox_backend": cast(Any, SimpleNamespace(name=profile.sandbox_backend)),
        "sandbox_environment": profile.sandbox_environment,
        "driver": cast(Any, SimpleNamespace(driver_id=profile.agent_driver)),
        "critic_driver": cast(Any, SimpleNamespace(driver_id=profile.critic_driver)),
        "model": profile.model,
        "max_ticks": profile.max_ticks,
        "checkpoint_after_dispatch": profile.checkpoint_after_dispatch,
    }
    values.update(overrides)
    return AgentMissionConfig(**values)


def _binding(profile: ExecutionProfile) -> ExecutionProfileBinding:
    return ExecutionProfileBinding(profile=profile, config_factory=_config)


def _catalog(*profiles: ExecutionProfile, current: str = "1") -> ExecutionProfileCatalog:
    return ExecutionProfileCatalog(
        tuple(_binding(profile) for profile in profiles),
        current_versions={"coding-default": current},
    )


def _actor(
    *capabilities: str,
    principal_id: str = "agent",
) -> MissionPrincipal:
    return MissionPrincipal(
        principal_id=principal_id,
        capabilities=frozenset(capabilities),
        allowed_profile_ids=frozenset({"coding-default"}),
    )


def _request(**overrides: str) -> MissionProfileRequest:
    values = {
        "profile_id": "coding-default",
        "repository": "VangelisTech/archetype",
        "branch": "agent/issue-808",
        "base_ref": "main",
    }
    values.update(overrides)
    return MissionProfileRequest.model_validate(values)


@dataclass(frozen=True)
class _Run:
    owner_principal_id: str
    granted_principal_ids: frozenset[str]
    profile_id: str
    profile_version: str
    profile_digest: str


def test_profile_digest_is_canonical_and_current_version_is_explicit() -> None:
    first = _profile(
        allowed_repositories=("VangelisTech/other", "VangelisTech/archetype"),
        secret_names=("provider", "codex-auth"),
    )
    same = _profile(
        allowed_repositories=("VangelisTech/archetype", "VangelisTech/other"),
        secret_names=("codex-auth", "provider"),
    )
    version_two = _profile(version="2", model="gpt-5.1")
    catalog = _catalog(first, version_two, current="2")

    assert first.digest == same.digest
    assert catalog.resolve("coding-default").profile.version == "2"
    assert catalog.resolve("coding-default", version="1").profile.digest == first.digest


def test_catalog_rejects_implicit_current_or_mutated_duplicate_versions() -> None:
    profile = _profile()
    with pytest.raises(ValueError, match="current_versions"):
        ExecutionProfileCatalog((_binding(profile),))
    with pytest.raises(ValueError, match="duplicate"):
        ExecutionProfileCatalog(
            (_binding(profile), _binding(_profile(model="other"))),
            current_versions={"coding-default": "1"},
        )


def test_profile_binding_builds_live_config_and_detects_drift() -> None:
    profile = _profile()
    config = _binding(profile).build_config()
    assert config.sandbox_environment == profile.sandbox_environment
    assert config.model == profile.model
    assert config.driver is not None and config.driver.driver_id == profile.agent_driver
    assert (
        config.critic_driver is not None and config.critic_driver.driver_id == profile.critic_driver
    )

    drifted = ExecutionProfileBinding(
        profile=profile,
        config_factory=lambda selected: _config(
            selected,
            sandbox_environment="other-environment",
        ),
    )
    with pytest.raises(ValueError, match="environment"):
        drifted.build_config()


@pytest.mark.parametrize(
    ("field_name", "label"),
    [("driver", "agent driver"), ("critic_driver", "critic driver")],
)
def test_factory_dropping_or_renaming_a_declared_driver_fails_closed(
    field_name: str,
    label: str,
) -> None:
    profile = _profile()

    dropped = ExecutionProfileBinding(
        profile=profile,
        config_factory=lambda selected: _config(selected, **{field_name: None}),
    )
    with pytest.raises(ValueError, match=label):
        dropped.build_config()

    renamed = ExecutionProfileBinding(
        profile=profile,
        config_factory=lambda selected: _config(
            selected,
            **{field_name: cast(Any, SimpleNamespace(driver_id="other-driver"))},
        ),
    )
    with pytest.raises(ValueError, match=label):
        renamed.build_config()


def test_shipped_codex_agent_driver_declares_its_protocol_identity() -> None:
    from archetype.missions.coding_agents.app_server import CodexAppServerDriver
    from archetype.missions.coding_agents.contracts import CodingAgentDriver

    assert "driver_id" in CodingAgentDriver.__annotations__
    assert CodexAppServerDriver.driver_id == "codex"


def test_submit_authorizes_only_profile_owned_coordinates_without_minting_a_run() -> None:
    authorizer = MissionAuthorizer(_catalog(_profile()))
    binding = authorizer.submit(_actor("mission:submit"), _request())
    assert binding.identity.profile_id == "coding-default"
    assert not hasattr(binding.identity, "run_id")

    for request in (
        _request(repository="other/repo"),
        _request(base_ref="release"),
        _request(branch="main"),
    ):
        with pytest.raises(PermissionError, match="Permission denied"):
            authorizer.submit(_actor("mission:submit"), request)


def test_run_authorization_consumes_durable_ownership_and_pinned_profile_facts() -> None:
    profile = _profile()
    authorizer = MissionAuthorizer(_catalog(profile))
    run = _Run(
        owner_principal_id="owner",
        granted_principal_ids=frozenset({"reader"}),
        profile_id=profile.profile_id,
        profile_version=profile.version,
        profile_digest=profile.digest,
    )

    authorizer.run(_actor("mission:read", principal_id="reader"), run, "mission:read")
    with pytest.raises(PermissionError, match="Permission denied"):
        authorizer.run(_actor("mission:read", principal_id="stranger"), run, "mission:read")
    with pytest.raises(PermissionError, match="Permission denied"):
        authorizer.run(_actor("mission:submit", principal_id="owner"), run, "mission:read")


def test_profile_flags_bound_each_interactive_capability() -> None:
    profile = _profile(
        allow_cancel=False,
        allow_attach=False,
        allow_steer=False,
        allow_takeover=False,
    )
    authorizer = MissionAuthorizer(_catalog(profile))
    run = _Run(
        owner_principal_id="agent",
        granted_principal_ids=frozenset(),
        profile_id=profile.profile_id,
        profile_version=profile.version,
        profile_digest=profile.digest,
    )
    for capability in (
        MISSION_CAPABILITY["cancel"],
        MISSION_CAPABILITY["attach"],
        MISSION_CAPABILITY["steer"],
        MISSION_CAPABILITY["takeover"],
    ):
        with pytest.raises(PermissionError, match="Permission denied"):
            authorizer.run(_actor(capability), run, capability)


@pytest.mark.parametrize(
    "branch",
    ["agent/../main", "agent//escape", "agent/topic.lock", "agent/topic~1"],
)
def test_malformed_git_refs_fail_before_profile_authorization(branch: str) -> None:
    with pytest.raises(ValueError, match="valid Git refs"):
        _request(branch=branch)
