# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RBAC guardrails, per-tick quotas, and token budget enforcement."""

from __future__ import annotations

from datetime import UTC, date, datetime
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
        "run_rollout",
        "run_episode",
    },
    "maintainer": {
        "spawn",
        "despawn",
        "add_component",
        "remove_component",
        "add_processor",
        "remove_processor",
        "update",
        "create_world",
        "destroy_world",
        "fork_world",
        "run_rollout",
        "run_episode",
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
# UTC date of the last daily-token reset.
_last_reset_date: date = datetime.now(UTC).date()


def estimate_token_cost(cmd: Command) -> int:
    """Estimate token cost for a command."""
    return _TOKEN_COSTS.get(cmd.type.value, 10)


def guardrail_check(
    cmd: Command,
    ctx: ActorCtx,
    projected_count: int = 0,
    projected_tokens: int = 0,
) -> int:
    """Pure RBAC + quota check.

    Returns the token cost of ``cmd`` if allowed; raises ``PermissionError``
    otherwise. Does NOT mutate ``_tick_counters`` or ``_daily_tokens`` — use
    :func:`guardrail_commit` to apply the debit after the caller is certain
    the command will actually be enqueued.

    ``projected_count`` / ``projected_tokens`` let bulk callers stack
    pre-check quota usage across a bulk without committing anything to the
    global state until every command in the bulk passes.

    The daily-token budget is rolled over lazily: before running the quota
    check, :func:`maybe_reset_daily_tokens` clears ``_daily_tokens`` if the
    UTC date has advanced since the last reset.
    """
    # Roll the daily budget forward if the UTC date has changed.
    maybe_reset_daily_tokens()

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
    if current_count + projected_count >= MAX_CMDS_PER_TICK:
        raise PermissionError(
            f"Actor {ctx.id} exceeded per-tick quota ({MAX_CMDS_PER_TICK} commands)"
        )

    # 3. Daily token budget
    cost = estimate_token_cost(cmd)
    current_tokens = _daily_tokens.get(ctx.id, 0)
    if current_tokens + projected_tokens + cost > MAX_TOKENS_PER_DAY:
        raise PermissionError(
            f"Actor {ctx.id} exceeded daily token budget ({MAX_TOKENS_PER_DAY} tokens)"
        )

    return cost


def guardrail_commit(ctx: ActorCtx, count: int, tokens: int) -> None:
    """Apply the quota debit computed by previous :func:`guardrail_check` calls.

    Callers MUST only invoke this after they have ensured the associated
    commands will actually be enqueued. Bulk callers should compute the
    projected totals via :func:`guardrail_check` and commit once at the end,
    so a partial validation failure leaves counters untouched.
    """
    if count:
        _tick_counters[ctx.id] = _tick_counters.get(ctx.id, 0) + count
    if tokens:
        _daily_tokens[ctx.id] = _daily_tokens.get(ctx.id, 0) + tokens


def guardrail_allow(cmd: Command, ctx: ActorCtx) -> None:
    """
    Check RBAC permissions and quotas and debit the actor's counters.

    Thin wrapper over :func:`guardrail_check` + :func:`guardrail_commit`
    for single-command callers. Raises ``PermissionError`` if denied (in
    which case no counters are mutated).
    """
    cost = guardrail_check(cmd, ctx)
    guardrail_commit(ctx, count=1, tokens=cost)


def reset_tick_counters() -> None:
    """Reset per-tick command counters. Called at the start of each tick."""
    _tick_counters.clear()


def reset_daily_tokens() -> None:
    """Reset daily token budgets unconditionally.

    Used by tests to clear state between cases, and by
    :func:`maybe_reset_daily_tokens` once a UTC day boundary is crossed.
    """
    global _last_reset_date
    _daily_tokens.clear()
    _last_reset_date = datetime.now(UTC).date()


def maybe_reset_daily_tokens(now: datetime | None = None) -> bool:
    """Clear daily token budgets iff the UTC date has advanced.

    ``guardrail_allow`` calls this on every command so the "daily" in
    ``MAX_TOKENS_PER_DAY`` actually rolls over at midnight UTC rather
    than accumulating for the entire process lifetime. Tests can pass
    an explicit ``now`` to exercise the rollover without touching the
    wall clock.

    Returns ``True`` if a reset was performed, ``False`` otherwise.
    """
    global _last_reset_date
    current_date = (now or datetime.now(UTC)).date()
    if current_date != _last_reset_date:
        _daily_tokens.clear()
        _last_reset_date = current_date
        return True
    return False
