# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Process-host configuration for the Missions world-library extension."""

from __future__ import annotations

from dataclasses import dataclass, field

from archetype.missions.execution_profiles import ExecutionProfileCatalog


@dataclass(frozen=True, slots=True, kw_only=True)
class MissionTemporalActivityConfig:
    """Host-owned route from committed Mission Activities into Temporal."""

    workflows: object
    namespace_digest: str

    def __post_init__(self) -> None:
        if not callable(getattr(self.workflows, "start", None)):
            raise TypeError("Mission Temporal Activity config requires a Workflow launcher")
        if len(self.namespace_digest) != 64 or any(
            character not in "0123456789abcdef" for character in self.namespace_digest
        ):
            raise ValueError("Mission Temporal Activity namespace digest is invalid")


@dataclass(frozen=True, slots=True, kw_only=True)
class MissionsExtensionConfig:
    """Bind server-owned mission profiles during the wiring transaction."""

    execution_profiles: ExecutionProfileCatalog = field(
        default_factory=ExecutionProfileCatalog.empty
    )
    temporal_activities: MissionTemporalActivityConfig | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.execution_profiles, ExecutionProfileCatalog):
            raise TypeError("execution_profiles must be an ExecutionProfileCatalog")
        if self.temporal_activities is not None and not isinstance(
            self.temporal_activities,
            MissionTemporalActivityConfig,
        ):
            raise TypeError("temporal_activities must be a MissionTemporalActivityConfig")


def installed_execution_profiles(installed: object) -> ExecutionProfileCatalog:
    """Return the execution-profile catalog retained by the installed library.

    ``installed`` is the value ``RuntimeResources.world_library("missions")``
    resolves after composition: the Missions installer retains the validated
    ``MissionsExtensionConfig`` (or its typed empty default) on
    ``InstalledWorldLibrary.config``, so profile authority flows through the
    existing world-library seam rather than a parallel service locator.
    """

    config = getattr(installed, "config", None)
    if not isinstance(config, MissionsExtensionConfig):
        raise TypeError("installed missions library did not retain MissionsExtensionConfig")
    return config.execution_profiles


__all__ = [
    "MissionTemporalActivityConfig",
    "MissionsExtensionConfig",
    "installed_execution_profiles",
]
