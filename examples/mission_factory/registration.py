# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Process-local graph capture for mission-factory prefab instantiation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from archetype.core.hooks import PostTick
from archetype.graph import GraphView


@dataclass(frozen=True, slots=True)
class MissionFactoryRegistration:
    """Fresh world-local code wiring for one asset-library world."""

    view: GraphView

    def world_options(self) -> dict[str, list[Any]]:
        """Return the options required by ``ArchetypeRuntime.world``."""

        return {
            "resources": [self.view],
            "hooks": [(PostTick, self.view.on_post_tick)],
        }


def register_mission_factory() -> MissionFactoryRegistration:
    """Install graph capture only; Agent Missions owns executable behavior."""

    return MissionFactoryRegistration(view=GraphView())
