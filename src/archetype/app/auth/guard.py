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

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from uuid_utils import UUID

from archetype.app.auth.errors import GuardrailError
from archetype.app.auth.permissions import COMMANDS_BY_ROLE

if TYPE_CHECKING:
    from archetype.app.auth.models import ActorCtx
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

    Returns the token cost of ``cmd`` if allowed; raises ``GuardrailError``
    otherwise. Does NOT mutate counters.
    """
    maybe_reset_daily_tokens()

    # 1. Permission check via the four-role matrix
    allowed = any(cmd.type in COMMANDS_BY_ROLE.get(r, frozenset()) for r in ctx.roles)

    if not allowed:
        raise GuardrailError(
            f"Actor {ctx.id} with roles {sorted(ctx.roles)} cannot execute '{cmd.type.value}'"
        )

    # 2. Per-tick quota
    current_count = _tick_counters.get(ctx.id, 0)
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


def guardrail_commit(ctx: ActorCtx, count: int, tokens: int) -> None:
    """Apply the quota debit after commands are confirmed enqueued."""
    if count:
        _tick_counters[ctx.id] = _tick_counters.get(ctx.id, 0) + count
    if tokens:
        _daily_tokens[ctx.id] = _daily_tokens.get(ctx.id, 0) + tokens


def guardrail_allow(cmd: Command, ctx: ActorCtx) -> None:
    """Check RBAC + quotas and debit counters. Raises GuardrailError if denied."""
    cost = guardrail_check(cmd, ctx)
    guardrail_commit(ctx, count=1, tokens=cost)


def reset_tick_counters() -> None:
    """Reset per-tick command counters for ALL actors.

    Test/maintenance helper. The live tick boundary uses
    ``reset_tick_counter_for`` so one actor's tick does not clear another
    actor's budget.
    """
    _tick_counters.clear()


def reset_tick_counter_for(actor_id: UUID) -> None:
    """Reset one actor's per-tick command counter at a tick boundary.

    Called by ``CommandService.step`` once a tick's commands have drained, so
    the documented per-tick quota actually resets each tick instead of
    accumulating across an entire episode/run.
    """
    _tick_counters.pop(actor_id, None)


def reset_daily_tokens() -> None:
    """Reset daily token budgets unconditionally."""
    global _last_reset_date
    _daily_tokens.clear()
    _last_reset_date = datetime.now(UTC).date()


def maybe_reset_daily_tokens(now: datetime | None = None) -> bool:
    """Clear daily token budgets iff the UTC date has advanced."""
    global _last_reset_date
    current_date = (now or datetime.now(UTC)).date()
    if current_date != _last_reset_date:
        _daily_tokens.clear()
        _last_reset_date = current_date
        return True
    return False
