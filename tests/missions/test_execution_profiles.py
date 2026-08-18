# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Server-owned execution profile and mission-control policy contracts."""

from __future__ import annotations

import pytest

from archetype.api.principals import MissionPrincipal
from archetype.missions.control import MissionControlCatalog, MissionRunPin
from archetype.missions.execution_profiles import (
    ExecutionProfile,
    ExecutionProfileCatalog,
    MissionProfileRequest,
    authorize_profile_request,
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
    return ExecutionProfile(**values)  # type: ignore[arg-type]


def _actor(*, capabilities: set[str], principal_id: str = "agent") -> MissionPrincipal:
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
    return MissionProfileRequest(**values)


def test_same_profile_id_and_version_always_resolve_to_the_same_digest() -> None:
    first = _profile()
    second = _profile()
    catalog = ExecutionProfileCatalog((first, second))

    assert first.digest == second.digest
    assert catalog.resolve("coding-default").digest == first.digest
    assert catalog.pin("coding-default").digest == first.digest


def test_mutating_a_version_in_place_fails_closed() -> None:
    original = _profile()
    changed = _profile(model="other-model")
    with pytest.raises(ValueError, match="canonical digest"):
        ExecutionProfileCatalog((original, changed))


def test_existing_pins_retain_meaning_after_a_new_current_version() -> None:
    version_one = _profile()
    control = MissionControlCatalog(ExecutionProfileCatalog((version_one,)))
    actor = _actor(capabilities={"mission:submit", "mission:read"})
    pin = control.submit(actor, _request())
    original_digest = pin.profile_digest
    assert pin.profile_version == "1"

    version_two = _profile(version="2", model="gpt-5.1", max_ticks=50)
    control.replace_profiles(ExecutionProfileCatalog((version_one, version_two)))
    reread = control.pin_for(actor, pin.run_id)

    assert reread.profile_digest == original_digest
    assert control.profiles.resolve("coding-default").digest == version_two.digest
    assert control.profiles.resolve("coding-default").digest != original_digest
    assert control.profiles.resolve("coding-default", version="1").digest == original_digest


def test_request_cannot_leave_the_selected_profile() -> None:
    profile = _profile()
    authorize_profile_request(profile, _request())
    with pytest.raises(PermissionError, match="Permission denied"):
        authorize_profile_request(profile, _request(repository="other/repo"))
    with pytest.raises(PermissionError, match="Permission denied"):
        authorize_profile_request(profile, _request(base_ref="release"))
    with pytest.raises(PermissionError, match="Permission denied"):
        authorize_profile_request(profile, _request(branch="main"))


def test_capabilities_are_enforced_independently() -> None:
    catalog = MissionControlCatalog(ExecutionProfileCatalog((_profile(),)))
    owner = _actor(
        capabilities={
            "mission:submit",
            "mission:read",
            "mission:cancel",
            "mission:attach",
            "mission:steer",
            "mission:takeover",
        }
    )
    pin = catalog.submit(owner, _request())

    for capability, method in (
        ("mission:read", catalog.pin_for),
        ("mission:cancel", catalog.cancel),
        ("mission:attach", catalog.attach),
        ("mission:steer", catalog.steer),
        ("mission:takeover", catalog.takeover),
    ):
        allowed = _actor(capabilities={capability})
        method(allowed, pin.run_id)
        denied = _actor(capabilities={"mission:submit"})
        with pytest.raises(PermissionError, match="Permission denied"):
            method(denied, pin.run_id)


def test_profile_flags_can_forbid_interactive_capabilities() -> None:
    profile = _profile(
        allow_cancel=False,
        allow_attach=False,
        allow_steer=False,
        allow_takeover=False,
    )
    catalog = MissionControlCatalog(ExecutionProfileCatalog((profile,)))
    owner = _actor(
        capabilities={
            "mission:submit",
            "mission:cancel",
            "mission:attach",
            "mission:steer",
            "mission:takeover",
        }
    )
    pin = catalog.submit(owner, _request())
    for method in (catalog.cancel, catalog.attach, catalog.steer, catalog.takeover):
        with pytest.raises(PermissionError, match="Permission denied"):
            method(owner, pin.run_id)


def test_foreign_principal_requires_an_explicit_grant() -> None:
    catalog = MissionControlCatalog(ExecutionProfileCatalog((_profile(),)))
    owner = _actor(capabilities={"mission:submit", "mission:read"})
    stranger = _actor(
        principal_id="stranger",
        capabilities={"mission:read"},
    )
    pin = catalog.submit(owner, _request())
    with pytest.raises(PermissionError, match="Permission denied"):
        catalog.pin_for(stranger, pin.run_id)

    catalog.grant(owner, pin.run_id, "stranger")
    reread = catalog.pin_for(stranger, pin.run_id)
    assert reread.run_id == pin.run_id
    assert reread.profile_digest == pin.profile_digest


def test_pins_and_profiles_do_not_carry_credential_values() -> None:
    secret = "sk-proj-" + "A" * 32
    profile = _profile(secret_names=("codex-auth",))
    pin = MissionRunPin(
        run_id="run-1",
        owner_principal_id="agent",
        profile_id=profile.profile_id,
        profile_version=profile.version,
        profile_digest=profile.digest,
    )
    assert secret not in repr(profile)
    assert secret not in repr(pin)
    assert "codex-auth" in profile.secret_names
