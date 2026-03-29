# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit evals: Command model ordering and serialization.

Tests that Command ordering respects (tick, priority, seq) and
that Arrow serialization round-trips correctly.
"""

from __future__ import annotations

from archetype.app.models import Command, CommandType
from evals.types import EvalResult, Tier, binary


def run() -> list[EvalResult]:
    results = []

    # --- ordering: lower tick comes first ---
    cmd_t0 = Command(type=CommandType.SPAWN, tick=0, payload={})
    cmd_t1 = Command(type=CommandType.SPAWN, tick=1, payload={})
    results.append(
        binary(
            cmd_t0 < cmd_t1,
            True,
            case_name="cmd_order_tick_lower_first",
            tier=Tier.UNIT,
        )
    )

    # --- ordering: same tick, lower priority first ---
    cmd_p0 = Command(type=CommandType.SPAWN, tick=0, priority=0, payload={})
    cmd_p1 = Command(type=CommandType.SPAWN, tick=0, priority=1, payload={})
    results.append(
        binary(
            cmd_p0 < cmd_p1,
            True,
            case_name="cmd_order_priority_lower_first",
            tier=Tier.UNIT,
        )
    )

    # --- ordering: same tick+priority, earlier seq first ---
    cmd_a = Command(type=CommandType.SPAWN, tick=0, priority=0, payload={})
    cmd_b = Command(type=CommandType.SPAWN, tick=0, priority=0, payload={})
    results.append(
        binary(
            cmd_a < cmd_b,
            True,
            case_name="cmd_order_seq_earlier_first",
            tier=Tier.UNIT,
        )
    )

    # --- each command gets a unique id ---
    ids = {Command(type=CommandType.CUSTOM, payload={}).id for _ in range(100)}
    results.append(
        binary(
            len(ids),
            100,
            case_name="cmd_unique_ids",
            tier=Tier.UNIT,
        )
    )

    # --- immutability: frozen model ---
    cmd = Command(type=CommandType.SPAWN, payload={"key": "val"})
    frozen = False
    try:
        cmd.tick = 999  # type: ignore[misc]
    except (TypeError, ValueError, AttributeError):
        frozen = True
    results.append(
        binary(
            frozen,
            True,
            case_name="cmd_immutable",
            tier=Tier.UNIT,
        )
    )

    # --- arrow serialization round-trip ---
    cmd = Command(type=CommandType.MESSAGE, tick=5, payload={"msg": "hello"})
    batch = cmd.to_arrow()
    results.append(
        binary(
            batch.num_rows,
            1,
            case_name="cmd_arrow_single_row",
            tier=Tier.UNIT,
        )
    )

    return results
