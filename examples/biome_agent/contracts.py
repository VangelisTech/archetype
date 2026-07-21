# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed values shared by the live Biome environment, policy, and ledger."""

from __future__ import annotations

import re
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUALIFIED_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+$")


def _require_identifier(value: str, field: str) -> None:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{field} must be a Flecs identifier, got {value!r}")


def _require_qualified_identifier(value: str, field: str) -> None:
    if not _QUALIFIED_IDENTIFIER.fullmatch(value):
        raise ValueError(f"{field} must be a qualified Flecs identifier, got {value!r}")


@dataclass(frozen=True, order=True)
class TerrainCell:
    """One addressable cell in Biome's terrain grid."""

    x: int
    y: int


@dataclass(frozen=True)
class DepositObservation:
    """Reflected state for one real ``biome.miner.Deposit`` entity."""

    entity_id: int
    entity_path: str
    resource: str
    amount: int
    terrain: str
    cell: TerrainCell

    @property
    def resource_name(self) -> str:
        return self.resource.rsplit(".", 1)[-1]


@dataclass(frozen=True)
class BiomeObservation:
    """The policy-visible slice of a live Biome ECS world."""

    deposits: tuple[DepositObservation, ...]
    occupied_cells: frozenset[TerrainCell]

    def deposit(self, entity_path: str) -> DepositObservation:
        for candidate in self.deposits:
            if candidate.entity_path == entity_path:
                return candidate
        raise LookupError(f"deposit {entity_path!r} is no longer observable")


@dataclass(frozen=True)
class ExtractionGoal:
    """Mine at least ``amount`` units of one named Biome resource."""

    resource: str
    amount: int

    def __post_init__(self) -> None:
        if not self.resource.strip():
            raise ValueError("resource must not be empty")
        if self.amount < 1:
            raise ValueError("amount must be at least 1")


@dataclass(frozen=True)
class PlaceExtractorAction:
    """Instantiate an upstream Drill and its adjacent power source."""

    target_path: str
    resource: str
    terrain: str
    drill_cell: TerrainCell
    power_cell: TerrainCell
    namespace: str = "mission"
    drill_name: str = "agent_drill"
    power_name: str = "agent_solar"
    script_name: str = "archetype_agent_action"

    def __post_init__(self) -> None:
        for field in ("namespace", "drill_name", "power_name", "script_name"):
            _require_identifier(getattr(self, field), field)
        for field in ("target_path", "resource", "terrain"):
            _require_qualified_identifier(getattr(self, field), field)

    @property
    def drill_path(self) -> str:
        return f"{self.namespace}.{self.drill_name}"

    @property
    def power_path(self) -> str:
        return f"{self.namespace}.{self.power_name}"


@dataclass(frozen=True)
class DrillObservation:
    """Native power, target, and storage state for a Biome Drill."""

    entity_id: int
    entity_path: str
    powered: bool
    deposit_path: str
    stored_resource: str
    stored_amount: int


@dataclass(frozen=True)
class MissionPlan:
    """Policy decision paired with the observation that produced it."""

    goal: ExtractionGoal
    observation: BiomeObservation
    action: PlaceExtractorAction

    @property
    def target(self) -> DepositObservation:
        return self.observation.deposit(self.action.target_path)


@dataclass(frozen=True)
class MissionSample:
    """One closed-loop sample after the action crossed the REST boundary."""

    elapsed_seconds: float
    deposit_amount: int
    extracted: int
    drill: DrillObservation | None


@dataclass(frozen=True)
class MissionTrace:
    """Terminal result and all sampled native state used to establish it."""

    plan: MissionPlan
    samples: tuple[MissionSample, ...]
    success: bool
    reason: str

    @property
    def final_sample(self) -> MissionSample | None:
        return self.samples[-1] if self.samples else None

    @property
    def extracted(self) -> int:
        sample = self.final_sample
        return sample.extracted if sample else 0
