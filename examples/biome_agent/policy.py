# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""A small but real baseline policy for the live Biome mission."""

from __future__ import annotations

from .contracts import (
    BiomeObservation,
    ExtractionGoal,
    PlaceExtractorAction,
    TerrainCell,
)


class GoalDirectedDrillPolicy:
    """Select a matching deposit and a free adjacent power cell.

    The baseline is deliberately inspectable. It is a policy, not environment
    logic: Biome still owns prefab instantiation, placement observers, power
    distribution, miner targeting, storage, and resource depletion.
    """

    _NEIGHBORS = ((1, 0), (0, 1), (-1, 0), (0, -1))

    def choose(
        self,
        goal: ExtractionGoal,
        observation: BiomeObservation,
    ) -> PlaceExtractorAction:
        requested = goal.resource.casefold()
        candidates = [
            deposit
            for deposit in observation.deposits
            if deposit.resource_name.casefold() == requested and deposit.amount >= goal.amount
        ]
        if not candidates:
            raise LookupError(
                f"no {goal.resource} deposit can satisfy an extraction goal of {goal.amount}"
            )

        target = min(candidates, key=lambda item: (-item.amount, item.entity_path))
        power_cell = next(
            (
                TerrainCell(target.cell.x + dx, target.cell.y + dy)
                for dx, dy in self._NEIGHBORS
                if TerrainCell(target.cell.x + dx, target.cell.y + dy)
                not in observation.occupied_cells
            ),
            None,
        )
        if power_cell is None:
            raise LookupError(f"no free power cell is adjacent to {target.entity_path}")

        return PlaceExtractorAction(
            target_path=target.entity_path,
            resource=target.resource,
            terrain=target.terrain,
            drill_cell=target.cell,
            power_cell=power_cell,
        )
