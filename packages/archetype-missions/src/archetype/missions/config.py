# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Process-host configuration for the Missions world-library extension."""

from __future__ import annotations

from dataclasses import dataclass, field

from archetype.missions.execution_profiles import ExecutionProfileCatalog


@dataclass(frozen=True, slots=True, kw_only=True)
class MissionsExtensionConfig:
    """Bind server-owned mission profiles during the wiring transaction."""

    execution_profiles: ExecutionProfileCatalog = field(
        default_factory=ExecutionProfileCatalog.empty
    )

    def __post_init__(self) -> None:
        if not isinstance(self.execution_profiles, ExecutionProfileCatalog):
            raise TypeError("execution_profiles must be an ExecutionProfileCatalog")


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


__all__ = ["MissionsExtensionConfig", "installed_execution_profiles"]
