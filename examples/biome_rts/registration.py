# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Example-local process wiring for the Biome-inspired RTS world.

This is the Archetype counterpart of importing a Biome C module.  It wires
processors, the graph resource, and its capture hook into one world.  Durable
prefab content is authored separately by :func:`author_prefab_library`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.hooks import PostTick
from archetype.graph import GraphView

from .processors import biome_rts_processors


@dataclass(frozen=True, slots=True)
class BiomeRTSRegistration:
    """World-local example wiring created by :func:`register_biome_rts`.

    Create one registration per world.  The contained :class:`GraphView` is
    mutable previous-tick state and must not be shared between worlds.
    """

    view: GraphView
    processors: tuple[AsyncProcessor, ...]

    def world_options(self) -> dict[str, list[Any]]:
        """Return keyword arguments accepted by ``ArchetypeRuntime.world``."""

        return {
            "processors": list(self.processors),
            "resources": [self.view],
            "hooks": [(PostTick, self.view.on_post_tick)],
        }


def register_biome_rts() -> BiomeRTSRegistration:
    """Build fresh process-local wiring for one Biome RTS example world."""

    return BiomeRTSRegistration(
        view=GraphView(),
        processors=tuple(biome_rts_processors()),
    )
