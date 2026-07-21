# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Example-local ledger components for a live Biome control episode."""

from archetype import Component


class BiomeMission(Component):
    environment_uri: str
    resource: str
    target_amount: int
    biome_revision: str
    flecs_revision: str


class BiomeEpisodeState(Component):
    phase: str = "admitted"
    target_entity: str = ""
    deposit_amount: int = 0
    extracted: int = 0
    drill_entity: str = ""
    powered: bool = False
    stored_amount: int = 0


class BiomeAgentDecision(Component):
    action: str = "place_extractor"
    target_entity: str
    drill_prefab: str = "buildings.Drill"
    power_prefab: str = "buildings.Solar"
    drill_x: int
    drill_y: int
    power_x: int
    power_y: int


class BiomeMissionOutcome(Component):
    success: bool
    extracted: int
    reason: str
    elapsed_seconds: float
