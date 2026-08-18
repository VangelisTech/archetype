# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Mission-control policy: capabilities, ownership, and profile pins.

API transport authenticates a principal. This module authorizes the resulting
operation against missions-owned profile and run-pin state. It does not
construct Mission ECS entities or start provider work.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from uuid_utils import uuid7

from archetype.missions.execution_profiles import (
    ExecutionProfile,
    ExecutionProfileCatalog,
    MissionProfileRequest,
    PinnedExecutionProfile,
    authorize_profile_request,
)

MISSION_CONTROL_CAPABILITY = {
    "submit": "mission:submit",
    "read": "mission:read",
    "cancel": "mission:cancel",
    "attach": "mission:attach",
    "steer": "mission:steer",
    "takeover": "mission:takeover",
}
_PROFILE_FLAGS = {
    "mission:cancel": "allow_cancel",
    "mission:attach": "allow_attach",
    "mission:steer": "allow_steer",
    "mission:takeover": "allow_takeover",
}


@runtime_checkable
class MissionActor(Protocol):
    """Authenticated mission caller consumed by missions-owned policy."""

    principal_id: str
    capabilities: frozenset[str]
    allowed_profile_ids: frozenset[str]
    granted_run_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class MissionRunPin:
    """Accepted run identity bound to one pinned execution profile.

    Durable asynchronous execution belongs to MissionRun. This pin is the
    server-owned profile/ownership fact required before that lifecycle.
    """

    run_id: str
    owner_principal_id: str
    profile_id: str
    profile_version: str
    profile_digest: str
    granted_principal_ids: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        for label in (
            "run_id",
            "owner_principal_id",
            "profile_id",
            "profile_version",
            "profile_digest",
        ):
            value = getattr(self, label)
            if not isinstance(value, str) or not value.strip() or value.strip() != value:
                raise ValueError(f"{label} must be a non-empty string")
        object.__setattr__(
            self,
            "granted_principal_ids",
            frozenset(self.granted_principal_ids),
        )


def require_capability(actor: MissionActor, capability: str) -> None:
    """Fail closed when the principal lacks an explicit capability."""

    if capability not in actor.capabilities:
        raise PermissionError("Permission denied")


def require_profile_access(actor: MissionActor, profile_id: str) -> None:
    """Fail closed when the principal may not select this profile."""

    if profile_id not in actor.allowed_profile_ids:
        raise PermissionError("Permission denied")


def require_profile_capability(profile: ExecutionProfile, capability: str) -> None:
    """Fail closed when the pinned profile forbids an interactive capability."""

    flag = _PROFILE_FLAGS.get(capability)
    if flag is not None and not bool(getattr(profile, flag)):
        raise PermissionError("Permission denied")


def require_run_access(actor: MissionActor, pin: MissionRunPin) -> None:
    """Fail closed unless the caller owns the run or holds an explicit grant."""

    if pin.owner_principal_id == actor.principal_id:
        return
    if pin.run_id in actor.granted_run_ids:
        return
    if actor.principal_id in pin.granted_principal_ids:
        return
    raise PermissionError("Permission denied")


class MissionControlCatalog:
    """Process-local accepted-run pins over a versioned profile catalog."""

    def __init__(self, profiles: ExecutionProfileCatalog) -> None:
        self._profiles = profiles
        self._pins: dict[str, MissionRunPin] = {}

    @property
    def profiles(self) -> ExecutionProfileCatalog:
        """Return the immutable execution-profile catalog."""

        return self._profiles

    def submit(self, actor: MissionActor, request: MissionProfileRequest) -> MissionRunPin:
        """Authorize submit, bind the current profile, and record the pin."""

        require_capability(actor, MISSION_CONTROL_CAPABILITY["submit"])
        require_profile_access(actor, request.profile_id)
        profile = self._profiles.resolve(request.profile_id)
        pinned = authorize_profile_request(profile, request)
        pin = MissionRunPin(
            run_id=str(uuid7()),
            owner_principal_id=actor.principal_id,
            profile_id=pinned.profile_id,
            profile_version=pinned.version,
            profile_digest=pinned.digest,
        )
        self._pins[pin.run_id] = pin
        return pin

    def pin_for(self, actor: MissionActor, run_id: str) -> MissionRunPin:
        """Return one accepted pin after ownership and read authorization."""

        return self._authorize_run(actor, run_id, MISSION_CONTROL_CAPABILITY["read"])

    def cancel(self, actor: MissionActor, run_id: str) -> MissionRunPin:
        """Authorize cancellation against ownership and profile policy."""

        return self._authorize_run(actor, run_id, MISSION_CONTROL_CAPABILITY["cancel"])

    def attach(self, actor: MissionActor, run_id: str) -> MissionRunPin:
        """Authorize attachment against ownership and profile policy."""

        return self._authorize_run(actor, run_id, MISSION_CONTROL_CAPABILITY["attach"])

    def steer(self, actor: MissionActor, run_id: str) -> MissionRunPin:
        """Authorize steering against ownership and profile policy."""

        return self._authorize_run(actor, run_id, MISSION_CONTROL_CAPABILITY["steer"])

    def takeover(self, actor: MissionActor, run_id: str) -> MissionRunPin:
        """Authorize takeover against ownership and profile policy."""

        return self._authorize_run(actor, run_id, MISSION_CONTROL_CAPABILITY["takeover"])

    def grant(self, actor: MissionActor, run_id: str, grantee_principal_id: str) -> MissionRunPin:
        """Grant another principal access to a run the actor owns."""

        pin = self._require_pin(run_id)
        if pin.owner_principal_id != actor.principal_id:
            raise PermissionError("Permission denied")
        if not isinstance(grantee_principal_id, str) or not grantee_principal_id.strip():
            raise ValueError("grantee_principal_id must be a non-empty string")
        updated = MissionRunPin(
            run_id=pin.run_id,
            owner_principal_id=pin.owner_principal_id,
            profile_id=pin.profile_id,
            profile_version=pin.profile_version,
            profile_digest=pin.profile_digest,
            granted_principal_ids=pin.granted_principal_ids | {grantee_principal_id.strip()},
        )
        self._pins[run_id] = updated
        return updated

    def replace_profiles(self, profiles: ExecutionProfileCatalog) -> None:
        """Install a new catalog without rewriting existing pins."""

        self._profiles = profiles

    def _require_pin(self, run_id: str) -> MissionRunPin:
        try:
            return self._pins[run_id]
        except KeyError:
            raise KeyError(f"mission run {run_id!r} is not accepted") from None

    def _authorize_run(self, actor: MissionActor, run_id: str, capability: str) -> MissionRunPin:
        require_capability(actor, capability)
        pin = self._require_pin(run_id)
        require_run_access(actor, pin)
        profile = self._profiles.resolve(
            pin.profile_id,
            version=pin.profile_version,
            digest=pin.profile_digest,
        )
        require_profile_capability(profile, capability)
        return pin


__all__ = [
    "MISSION_CONTROL_CAPABILITY",
    "MissionActor",
    "MissionControlCatalog",
    "MissionRunPin",
    "PinnedExecutionProfile",
    "require_capability",
    "require_profile_access",
    "require_profile_capability",
    "require_run_access",
]
