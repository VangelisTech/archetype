# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Example read models composed from generic projections and graph edges."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import daft
from daft import DataFrame, Expression, col

from archetype.graph import Relation
from archetype.projections import latest, overview, possession

from .relations import VisibleTo


def minimap(units: DataFrame) -> DataFrame:
    """Latest spatial unit view from a ``UnitSpec, Position, Health`` frame."""

    return latest(units).select(
        col("entity_id"),
        col("tick"),
        col("unitspec__role").alias("role"),
        col("position__x").alias("x"),
        col("position__y").alias("y"),
        col("health__current").alias("health"),
    )


def minimap_overview(**layers: DataFrame) -> DataFrame:
    """Per-tick population series for the strategic minimap layers."""

    return overview(**layers)


def fog_of_war(map_frame: DataFrame, visibility: DataFrame, observer: int) -> DataFrame:
    """Restrict a minimap to ``observer`` and its outgoing visibility edges."""

    prefix = VisibleTo.get_prefix()
    visible = visibility.where(cast(Expression, col(f"{prefix}source") == observer)).select(
        col(f"{prefix}target").alias("entity_id")
    )
    known = visible.concat(daft.from_pydict({"entity_id": [observer]})).distinct()
    return map_frame.join(known, on="entity_id")


def unit_view(
    edges_by_relation: Sequence[tuple[type[Relation], DataFrame]],
    entity: int,
    depth: int = 1,
) -> DataFrame:
    """The relational field of view used when a player possesses one unit."""

    return possession(edges_by_relation, entity, depth)
