# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Missions-owned capability, ownership, and execution-profile policy."""

from __future__ import annotations

from typing import Protocol

from archetype.missions.execution_profiles import (
    ExecutionProfileBinding,
    ExecutionProfileCatalog,
    MissionProfileRequest,
    authorize_profile_request,
)

MISSION_CAPABILITY = {
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


class MissionActor(Protocol):
    principal_id: str
    capabilities: frozenset[str]
    allowed_profile_ids: frozenset[str]


class MissionRunAccess(Protocol):
    """The authorization projection supplied by the durable MissionRun owner."""

    owner_principal_id: str
    granted_principal_ids: frozenset[str]
    profile_id: str
    profile_version: str
    profile_digest: str


def require_capability(actor: MissionActor, capability: str) -> None:
    if capability not in actor.capabilities:
        raise PermissionError("Permission denied")


def require_run_access(actor: MissionActor, run: MissionRunAccess) -> None:
    if actor.principal_id == run.owner_principal_id:
        return
    if actor.principal_id in run.granted_principal_ids:
        return
    raise PermissionError("Permission denied")


class MissionAuthorizer:
    """Pure policy over host profiles and an externally supplied durable run fact."""

    def __init__(self, profiles: ExecutionProfileCatalog) -> None:
        if not isinstance(profiles, ExecutionProfileCatalog):
            raise TypeError("profiles must be an ExecutionProfileCatalog")
        self._profiles = profiles

    def submit(
        self,
        actor: MissionActor,
        request: MissionProfileRequest,
    ) -> ExecutionProfileBinding:
        require_capability(actor, MISSION_CAPABILITY["submit"])
        if request.profile_id not in actor.allowed_profile_ids:
            raise PermissionError("Permission denied")
        binding = self._profiles.resolve(request.profile_id)
        authorize_profile_request(binding, request)
        return binding

    def run(
        self,
        actor: MissionActor,
        run: MissionRunAccess,
        capability: str,
    ) -> ExecutionProfileBinding:
        require_capability(actor, capability)
        require_run_access(actor, run)
        binding = self._profiles.resolve(
            run.profile_id,
            version=run.profile_version,
            digest=run.profile_digest,
        )
        flag = _PROFILE_FLAGS.get(capability)
        if flag is not None and not getattr(binding.profile, flag):
            raise PermissionError("Permission denied")
        return binding


__all__ = [
    "MISSION_CAPABILITY",
    "MissionActor",
    "MissionAuthorizer",
    "MissionRunAccess",
    "require_capability",
    "require_run_access",
]
