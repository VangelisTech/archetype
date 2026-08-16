# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Storage-backed physical-AI report projections."""

from __future__ import annotations

from typing import Any

from daft import DataFrame, col

from archetype.storage.interfaces import iStorageService


async def latest_rows(
    frame: DataFrame,
    storage: iStorageService,
) -> dict[int, dict[str, Any]]:
    """Materialize one latest terminal row per entity for report production."""

    heads = frame.groupby("entity_id").agg(col("tick").max().alias("latest_tick"))
    terminal = frame.join(
        heads,
        left_on=["entity_id", "tick"],
        right_on=["entity_id", "latest_tick"],
    ).select(*frame.column_names)
    materialized = await storage.materialize(terminal)
    return {int(row["entity_id"]): row for row in materialized.to_pylist()}


__all__ = ["latest_rows"]
