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


__all__ = ["MissionsExtensionConfig"]
