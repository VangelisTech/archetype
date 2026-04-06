# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RBAC guardrails, per-tick quotas, and token budget enforcement."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from archetype.app.auth.models import ActorCtx
    from archetype.app.models import Command

# ── Role → permitted command types ──

ROLE_PERMS: dict[str, set[str]] = {
    "viewer": {"get_state", "get_world", "get_run", "query_world"},
    "coder": {"add_component", "remove_component", "update"},
    "operator": {
        "spawn",
        "despawn",
        "update",
        "get_state",
        "get_world",
        "get_run",
        "query_world",
    },
    "maintainer": {
        "spawn",
        "despawn",
        "add_component",
        "remove_component",
        "add_processor",
        "remove_processor",
        "update",
    },
    "admin": {"*"},
    "player": {"spawn", "despawn", "update", "message", "custom"},
}

# ── Quotas ──

MAX_CMDS_PER_TICK: int = 500
MAX_TOKENS_PER_DAY: int = 200_000

# Token cost estimates per command type
_TOKEN_COSTS: dict[str, int] = {
    "spawn": 10,
    "despawn": 5,
    "update": 8,
    "add_component": 8,
    "remove_component": 5,
    "add_processor": 15,
    "remove_processor": 5,
    "create_world": 50,
    "destroy_world": 10,
    "fork_world": 100,
    "run_rollout": 200,
    "run_episode": 500,
    "query_world": 5,
    "message": 3,
    "custom": 10,
}

# Per-actor tick counters: actor_id → count this tick
_tick_counters: dict[UUID, int] = {}
# Per-actor daily token usage: actor_id → tokens used today
_daily_tokens: dict[UUID, int] = {}


def estimate_token_cost(cmd: Command) -> int:
    """Estimate token cost for a command."""
    return _TOKEN_COSTS.get(cmd.type.value, 10)


def guardrail_allow(cmd: Command, ctx: ActorCtx) -> None:
    """
    Check RBAC permissions and quotas. Raises PermissionError if denied.

    Checks:
    1. Role permissions — does any of the actor's roles grant the command type?
    2. Per-tick quota — has the actor exceeded MAX_CMDS_PER_TICK?
    3. Daily token budget — has the actor exceeded MAX_TOKENS_PER_DAY?
    """
    # 1. Permission check
    cmd_type = cmd.type.value
    allowed = False
    for role in ctx.roles:
        perms = ROLE_PERMS.get(role, set())
        if "*" in perms or cmd_type in perms:
            allowed = True
            break

    if not allowed:
        raise PermissionError(f"Actor {ctx.id} with roles {ctx.roles} cannot execute '{cmd_type}'")

    # 2. Per-tick quota
    current_count = _tick_counters.get(ctx.id, 0)
    if current_count >= MAX_CMDS_PER_TICK:
        raise PermissionError(
            f"Actor {ctx.id} exceeded per-tick quota ({MAX_CMDS_PER_TICK} commands)"
        )
    _tick_counters[ctx.id] = current_count + 1

    # 3. Daily token budget
    cost = estimate_token_cost(cmd)
    current_tokens = _daily_tokens.get(ctx.id, 0)
    if current_tokens + cost > MAX_TOKENS_PER_DAY:
        raise PermissionError(
            f"Actor {ctx.id} exceeded daily token budget ({MAX_TOKENS_PER_DAY} tokens)"
        )
    _daily_tokens[ctx.id] = current_tokens + cost


def reset_tick_counters() -> None:
    """Reset per-tick command counters. Called at the start of each tick."""
    _tick_counters.clear()


def reset_daily_tokens() -> None:
    """Reset daily token budgets. Called at day boundary."""
    _daily_tokens.clear()
