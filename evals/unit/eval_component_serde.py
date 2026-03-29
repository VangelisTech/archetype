# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit evals: Component serialization and schema generation.

Tests that components serialize to prefixed row dicts correctly,
schemas are generated with proper field names, and from_dict
round-trips produce identical instances.
"""

from __future__ import annotations

from archetype.core.component import Component
from evals.types import EvalResult, Tier, binary


class Position(Component):
    x: int
    y: int


class Velocity(Component):
    dx: int
    dy: int


class Stats(Component):
    hp: int
    name: str


def run() -> list[EvalResult]:
    results = []

    # --- prefix generation ---
    results.append(
        binary(
            Position.get_prefix(),
            "position__",
            case_name="component_prefix_position",
            tier=Tier.UNIT,
        )
    )
    results.append(
        binary(
            Velocity.get_prefix(),
            "velocity__",
            case_name="component_prefix_velocity",
            tier=Tier.UNIT,
        )
    )

    # --- to_row_dict produces prefixed keys ---
    pos = Position(x=10, y=20)
    row = pos.to_row_dict()
    results.append(
        binary(
            row,
            {"position__x": 10, "position__y": 20},
            case_name="component_row_dict_position",
            tier=Tier.UNIT,
        )
    )

    vel = Velocity(dx=-1, dy=3)
    row = vel.to_row_dict()
    results.append(
        binary(
            row,
            {"velocity__dx": -1, "velocity__dy": 3},
            case_name="component_row_dict_velocity",
            tier=Tier.UNIT,
        )
    )

    # --- mixed field types ---
    stats = Stats(hp=100, name="hero")
    row = stats.to_row_dict()
    results.append(
        binary(
            row,
            {"stats__hp": 100, "stats__name": "hero"},
            case_name="component_row_dict_mixed_types",
            tier=Tier.UNIT,
        )
    )

    # --- prefixed schema field names ---
    schema = Position.get_prefixed_schema()
    field_names = [f.name for f in schema]
    results.append(
        binary(
            set(field_names),
            {"position__x", "position__y"},
            case_name="component_prefixed_schema_fields",
            tier=Tier.UNIT,
        )
    )

    # --- from_dict type dispatch ---
    restored = Component.from_dict({"type": "Position", "x": 5, "y": 15})
    results.append(
        binary(
            type(restored).__name__,
            "Position",
            case_name="component_from_dict_type_dispatch",
            tier=Tier.UNIT,
        )
    )
    results.append(
        binary(
            restored.to_row_dict(),
            {"position__x": 5, "position__y": 15},
            case_name="component_from_dict_roundtrip",
            tier=Tier.UNIT,
        )
    )

    return results
