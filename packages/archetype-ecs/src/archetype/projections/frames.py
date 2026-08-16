# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Frame-pure world read models (design D1, docs/design/graph-system.md, stage 0).

Projections are read models over the append-only ledger: functions from lazy
frames to lazy frames, importing nothing above core. They operate on the base
columns every archetype table carries (``entity_id``, ``tick``), so they work
on any queried frame — component tables and EdgeTables alike. Nothing here
materializes; callers own the terminal collect at their boundary.
"""

from __future__ import annotations

from daft import DataFrame, col, lit


def activity(frame: DataFrame, *, table: str = "entities") -> DataFrame:
    """Per-tick population of a queried frame: one row per tick.

    Every live entity persists one row per tick, so the row count per tick is
    the population at that tick. Output columns: ``table``, ``tick``,
    ``population``.
    """
    per_tick = frame.groupby("tick").agg(col("entity_id").count().alias("population"))
    return per_tick.with_column("table", lit(table)).select(
        col("table"), col("tick"), col("population")
    )


def overview(**frames: DataFrame) -> DataFrame:
    """Union of :func:`activity` across named frames.

    ``overview(nodes=node_frame, edges=edge_frame)`` yields one population
    series per name — the minimap primitive: what lives where, over time.
    """
    if not frames:
        raise ValueError("overview requires at least one named frame")
    parts = [activity(frame, table=name) for name, frame in frames.items()]
    out = parts[0]
    for part in parts[1:]:
        out = out.concat(part)
    return out.sort(["table", "tick"])


def latest(frame: DataFrame) -> DataFrame:
    """Rows at the frame's maximum tick.

    The maximum is found with a one-row aggregate joined back on ``tick``, so
    the whole read stays in the lazy plan.
    """
    max_tick = frame.agg(col("tick").max().alias("tick"))
    return frame.join(max_tick, on="tick")
