# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""RBAC guardrails, per-tick quotas, and token budget enforcement.

The four-role model (viewer, player, operator, admin) is defined in
``permissions.py``. This module enforces it.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from uuid_utils import UUID

from archetype.app.gateway.auth.errors import GuardrailError
from archetype.app.gateway.auth.permissions import COMMANDS_BY_ROLE

if TYPE_CHECKING:
    from archetype.app.gateway.auth.models import ActorCtx
    from archetype.app.models import Command

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
    "autoresearch": 200,  # per iteration; see estimate_token_cost
    "run_episode": 500,
    "run": 50,
    "step": 10,
    "query_world": 5,
    "get_world_info": 2,
    "get_audit_history": 5,
    "list_signatures": 2,
    "list_worlds": 2,
    "list_processors": 2,
    "list_hooks": 2,
    "list_resources": 2,
    "add_resource": 10,
    "add_hook": 10,
    "remove_hook": 5,
    "message": 3,
    "custom": 10,
}

# PR-2's temporary process-local quota generation. PR-3 moves this state into
# Policy, but the key already carries the full durable admission scope so no
# simulation callback or process-wide tick reset is required.
type TickQuotaScope = tuple[str, int]
type TickQuotaKey = tuple[UUID, str, int]

# Per-actor/world/target-tick counters.
_tick_counters: dict[TickQuotaKey, int] = {}
# Per-actor daily token usage: actor_id → tokens used today
_daily_tokens: dict[UUID, int] = {}
# UTC date of the last daily-token reset.
_last_reset_date: date = datetime.now(UTC).date()


def estimate_token_cost(cmd: Command) -> int:
    """Estimate token cost for a command.

    ``autoresearch`` is one command that performs up to ``max_iterations``
    rollouts (the loop's internal rollouts are not gated individually), so
    it is charged at the rollout rate per iteration rather than as one
    flat command.
    """
    cost = _TOKEN_COSTS.get(cmd.type.value, 10)
    if cmd.type.value == "autoresearch":
        iterations = cmd.payload.get("max_iterations", 1)
        cost *= max(int(iterations), 1)
    return cost


def guardrail_check(
    cmd: Command,
    ctx: ActorCtx,
    *,
    world_id: object,
    target_tick: int,
    projected_count: int = 0,
    projected_tokens: int = 0,
    now: datetime | None = None,
) -> int:
    """Pure RBAC + quota check.

    Returns the token cost of ``cmd`` if allowed; raises ``GuardrailError``
    otherwise. Does NOT mutate counters.
    """
    maybe_reset_daily_tokens(now)

    # 1. Permission check via the four-role matrix
    allowed = any(cmd.type in COMMANDS_BY_ROLE.get(r, frozenset()) for r in ctx.roles)

    if not allowed:
        raise GuardrailError(
            f"Actor {ctx.id} with roles {sorted(ctx.roles)} cannot execute '{cmd.type.value}'"
        )

    # 2. Per-world, per-target-tick quota
    key = _tick_quota_key(ctx, world_id, target_tick)
    current_count = _tick_counters.get(key, 0)
    if current_count + projected_count >= MAX_CMDS_PER_TICK:
        raise GuardrailError(
            f"Actor {ctx.id} exceeded per-tick quota ({MAX_CMDS_PER_TICK} commands)"
        )

    # 3. Daily token budget
    cost = estimate_token_cost(cmd)
    current_tokens = _daily_tokens.get(ctx.id, 0)
    if current_tokens + projected_tokens + cost > MAX_TOKENS_PER_DAY:
        raise GuardrailError(
            f"Actor {ctx.id} exceeded daily token budget ({MAX_TOKENS_PER_DAY} tokens)"
        )

    return cost


def guardrail_commit(
    ctx: ActorCtx,
    *,
    tick_counts: Mapping[TickQuotaScope, int],
    tokens: int,
) -> None:
    """Apply one already-validated quota debit without a partial batch state."""
    if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens < 0:
        raise ValueError("tokens must be a non-negative integer")
    debits: list[tuple[TickQuotaKey, int]] = []
    for (world_id, target_tick), count in tick_counts.items():
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("tick quota count must be a non-negative integer")
        normalized_world_id, normalized_target_tick = _tick_quota_scope(world_id, target_tick)
        key = (ctx.id, normalized_world_id, normalized_target_tick)
        debits.append((key, _tick_counters.get(key, 0) + count))

    for key, new_count in debits:
        if new_count:
            _tick_counters[key] = new_count
    if tokens:
        _daily_tokens[ctx.id] = _daily_tokens.get(ctx.id, 0) + tokens


def guardrail_allow(
    cmd: Command,
    ctx: ActorCtx,
    *,
    world_id: object,
    target_tick: int,
    now: datetime | None = None,
) -> None:
    """Check RBAC + quotas and debit counters. Raises GuardrailError if denied."""
    normalized_world_id, normalized_target_tick = _tick_quota_scope(world_id, target_tick)
    cost = guardrail_check(
        cmd,
        ctx,
        world_id=normalized_world_id,
        target_tick=normalized_target_tick,
        now=now,
    )
    guardrail_commit(
        ctx,
        tick_counts={(normalized_world_id, normalized_target_tick): 1},
        tokens=cost,
    )


def reset_daily_tokens() -> None:
    """Reset daily token budgets unconditionally."""
    global _last_reset_date
    _daily_tokens.clear()
    _last_reset_date = datetime.now(UTC).date()


def maybe_reset_daily_tokens(now: datetime | None = None) -> bool:
    """Clear daily token budgets iff the UTC date has advanced."""
    global _last_reset_date
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    current_date = current.astimezone(UTC).date()
    if current_date != _last_reset_date:
        _daily_tokens.clear()
        _last_reset_date = current_date
        return True
    return False


def _tick_quota_scope(world_id: object, target_tick: int) -> TickQuotaScope:
    """Normalize and validate the non-ambient coordinates of one quota debit."""
    normalized_world_id = str(world_id)
    if not normalized_world_id:
        raise ValueError("world_id must not be empty")
    if isinstance(target_tick, bool) or not isinstance(target_tick, int) or target_tick < 0:
        raise ValueError("target_tick must be a non-negative integer")
    return normalized_world_id, target_tick


def _tick_quota_key(ctx: ActorCtx, world_id: object, target_tick: int) -> TickQuotaKey:
    normalized_world_id, normalized_target_tick = _tick_quota_scope(world_id, target_tick)
    return ctx.id, normalized_world_id, normalized_target_tick
