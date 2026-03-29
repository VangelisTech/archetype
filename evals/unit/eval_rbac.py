# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit evals: RBAC guard, permissions, and quotas.

Tests that role-based access control correctly allows/denies commands,
enforces per-tick and daily token quotas, and token cost estimation
is consistent.
"""

from __future__ import annotations

from uuid_utils import uuid7

from archetype.app.auth.guard import (
    MAX_CMDS_PER_TICK,
    estimate_token_cost,
    guardrail_allow,
    reset_daily_tokens,
    reset_tick_counters,
)
from archetype.app.auth.models import ActorCtx
from archetype.app.models import Command, CommandType
from evals.types import EvalResult, Tier, binary


def _check_denied(cmd: Command, ctx: ActorCtx) -> bool:
    """Return True if guardrail_allow raises PermissionError."""
    try:
        guardrail_allow(cmd, ctx)
        return False
    except PermissionError:
        return True


def run() -> list[EvalResult]:
    results = []
    reset_tick_counters()
    reset_daily_tokens()

    admin = ActorCtx(id=uuid7(), roles={"admin"})
    viewer = ActorCtx(id=uuid7(), roles={"viewer"})
    player = ActorCtx(id=uuid7(), roles={"player"})

    # --- admin can do anything ---
    for cmd_type in [CommandType.SPAWN, CommandType.DESPAWN, CommandType.ADD_PROCESSOR]:
        cmd = Command(type=cmd_type, payload={})
        results.append(
            binary(
                _check_denied(cmd, admin),
                False,
                case_name=f"rbac_admin_allowed_{cmd_type.value}",
                tier=Tier.UNIT,
            )
        )

    reset_tick_counters()
    reset_daily_tokens()

    # --- viewer denied mutation commands ---
    for cmd_type in [CommandType.SPAWN, CommandType.DESPAWN, CommandType.UPDATE]:
        cmd = Command(type=cmd_type, payload={})
        results.append(
            binary(
                _check_denied(cmd, viewer),
                True,
                case_name=f"rbac_viewer_denied_{cmd_type.value}",
                tier=Tier.UNIT,
            )
        )

    reset_tick_counters()
    reset_daily_tokens()

    # --- player can spawn but not add_processor ---
    results.append(
        binary(
            _check_denied(Command(type=CommandType.SPAWN, payload={}), player),
            False,
            case_name="rbac_player_allowed_spawn",
            tier=Tier.UNIT,
        )
    )

    reset_tick_counters()
    reset_daily_tokens()

    results.append(
        binary(
            _check_denied(Command(type=CommandType.ADD_PROCESSOR, payload={}), player),
            True,
            case_name="rbac_player_denied_add_processor",
            tier=Tier.UNIT,
        )
    )

    reset_tick_counters()
    reset_daily_tokens()

    # --- token cost estimation returns positive ints ---
    for cmd_type in CommandType:
        cost = estimate_token_cost(Command(type=cmd_type, payload={}))
        results.append(
            binary(
                cost > 0,
                True,
                case_name=f"token_cost_positive_{cmd_type.value}",
                tier=Tier.UNIT,
            )
        )

    # --- per-tick quota enforcement ---
    reset_tick_counters()
    reset_daily_tokens()
    quota_actor = ActorCtx(id=uuid7(), roles={"admin"})

    # Exhaust the per-tick quota
    for _ in range(MAX_CMDS_PER_TICK):
        guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), quota_actor)

    # Next one should be denied
    results.append(
        binary(
            _check_denied(Command(type=CommandType.CUSTOM, payload={}), quota_actor),
            True,
            case_name="rbac_per_tick_quota_enforced",
            tier=Tier.UNIT,
        )
    )

    # Reset clears the quota
    reset_tick_counters()
    results.append(
        binary(
            _check_denied(Command(type=CommandType.CUSTOM, payload={}), quota_actor),
            False,
            case_name="rbac_per_tick_quota_reset",
            tier=Tier.UNIT,
        )
    )

    # --- default role is viewer ---
    default_actor = ActorCtx(id=uuid7())
    results.append(
        binary(
            default_actor.roles,
            {"viewer"},
            case_name="actor_default_role_viewer",
            tier=Tier.UNIT,
        )
    )

    reset_tick_counters()
    reset_daily_tokens()
    return results
