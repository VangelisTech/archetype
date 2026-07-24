# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Instance-owned authorization and quota policy for governed commands."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

from archetype.commands.models import PolicyRequest

DEFAULT_MAX_COMMANDS_PER_TICK = 500
DEFAULT_MAX_TOKENS_PER_DAY = 200_000

_VIEWER_PERMISSIONS = frozenset(
    {
        "get_world_info",
        "list_worlds",
        "discover_worlds",
        "open_world_readonly",
        "query_components",
        "query_archetype",
        "list_signatures",
        "get_audit_history",
        "list_processors",
        "list_hooks",
        "list_resources",
        "query_artifacts",
    }
)
_PLAYER_PERMISSIONS = _VIEWER_PERMISSIONS | {
    "spawn",
    "create_entities",
    "despawn",
    "update",
}
_OPERATOR_PERMISSIONS = _PLAYER_PERMISSIONS | {
    "add_components",
    "remove_components",
    "add_processor",
    "remove_processor",
    "fork_world",
    "destroy_world",
    "step",
    "run",
    "run_episode",
    "run_rollout",
    "add_resource",
    "add_hook",
    "remove_hook",
    "autoresearch",
    "ingest_artifacts",
    "evaluate",
}
_ADMIN_PERMISSIONS = _OPERATOR_PERMISSIONS | {
    "create_world",
    "resume_world",
}

PERMISSIONS_BY_ROLE = {
    "viewer": _VIEWER_PERMISSIONS,
    "player": _PLAYER_PERMISSIONS,
    "operator": _OPERATOR_PERMISSIONS,
    "admin": _ADMIN_PERMISSIONS,
}

type TickQuotaKey = tuple[str, str, int]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _non_negative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _positive_integer(value: object, *, field: str) -> int:
    normalized = _non_negative_integer(value, field=field)
    if normalized == 0:
        raise ValueError(f"{field} must be positive")
    return normalized


def _world_coordinate(world_id: object, target_tick: object) -> tuple[str, int]:
    if world_id is None:
        raise ValueError("world_id must not be empty")
    normalized_world_id = str(world_id)
    if not normalized_world_id:
        raise ValueError("world_id must not be empty")
    return normalized_world_id, _non_negative_integer(target_tick, field="target_tick")


class Policy:
    """Apply flat-role authorization and instance-local quota debits."""

    def __init__(
        self,
        *,
        max_commands_per_tick: int = DEFAULT_MAX_COMMANDS_PER_TICK,
        max_tokens_per_day: int = DEFAULT_MAX_TOKENS_PER_DAY,
        utcnow: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._max_commands_per_tick = _positive_integer(
            max_commands_per_tick,
            field="max_commands_per_tick",
        )
        self._max_tokens_per_day = _non_negative_integer(
            max_tokens_per_day,
            field="max_tokens_per_day",
        )
        self._utcnow = utcnow
        self._tick_debits: dict[TickQuotaKey, int] = {}
        self._daily_token_debits: dict[str, int] = {}
        self._daily_generation: date | None = None

    def preauthorize(
        self,
        actor: Any,
        *,
        permission: str,
    ) -> None:
        """Check only role membership, without reading a clock or quota state."""
        roles = getattr(actor, "roles", ())
        allowed = any(
            permission in PERMISSIONS_BY_ROLE.get(str(role), frozenset()) for role in roles
        )
        if not allowed:
            actor_id = getattr(actor, "id", "<unknown>")
            raise PermissionError(
                f"actor {actor_id} with roles {sorted(map(str, roles))} "
                f"cannot execute permission {permission!r}"
            )

    def authorize(
        self,
        actor: Any,
        *,
        permission: str,
        world_id: object,
        target_tick: int,
        token_cost: int = 0,
    ) -> None:
        """Authorize and atomically debit one world/tick command."""
        # Keep the public full-policy method role-first even if the bounded
        # request value grows stricter construction-time validation.
        self.preauthorize(actor, permission=permission)
        request = PolicyRequest(
            permission=permission,
            world_id=world_id,
            target_tick=target_tick,
            token_cost=token_cost,
        )
        self.authorize_batch(actor, requests=(request,))

    def authorize_application(
        self,
        actor: Any,
        *,
        permission: str,
        token_cost: int = 0,
    ) -> None:
        """Authorize an application operation without a pseudo tick bucket."""
        self.preauthorize(actor, permission=permission)
        normalized_cost = _non_negative_integer(token_cost, field="token_cost")
        actor_id = str(actor.id)
        self._roll_daily_generation()
        new_total = self._daily_token_debits.get(actor_id, 0) + normalized_cost
        self._require_daily_budget(new_total)
        if normalized_cost:
            self._daily_token_debits[actor_id] = new_total

    def authorize_batch(
        self,
        actor: Any,
        *,
        requests: tuple[PolicyRequest, ...],
    ) -> None:
        """Validate every member, then apply one all-or-nothing quota debit."""
        if not requests:
            raise ValueError("policy batch must not be empty")

        normalized: list[tuple[TickQuotaKey, int]] = []
        actor_id = str(actor.id)
        for request in requests:
            self.preauthorize(actor, permission=request.permission)

        for request in requests:
            world_id, target_tick = _world_coordinate(
                request.world_id,
                request.target_tick,
            )
            token_cost = _non_negative_integer(
                request.token_cost,
                field="token_cost",
            )
            normalized.append(((actor_id, world_id, target_tick), token_cost))

        projected_counts = Counter(key for key, _cost in normalized)
        projected_tokens = sum(cost for _key, cost in normalized)

        self._roll_daily_generation()
        for key, count in projected_counts.items():
            if self._tick_debits.get(key, 0) + count > self._max_commands_per_tick:
                raise PermissionError(
                    f"actor {actor.id} exceeded per-tick quota "
                    f"({self._max_commands_per_tick} commands)"
                )

        new_daily_total = self._daily_token_debits.get(actor_id, 0) + projected_tokens
        self._require_daily_budget(new_daily_total)

        for key, count in projected_counts.items():
            self._tick_debits[key] = self._tick_debits.get(key, 0) + count
        if projected_tokens:
            self._daily_token_debits[actor_id] = new_daily_total

    def _roll_daily_generation(self) -> None:
        current = self._utcnow()
        if current.tzinfo is None:
            current = current.replace(tzinfo=UTC)
        current_date = current.astimezone(UTC).date()
        if self._daily_generation != current_date:
            self._daily_token_debits.clear()
            self._daily_generation = current_date

    def _require_daily_budget(self, projected_total: int) -> None:
        if projected_total > self._max_tokens_per_day:
            raise PermissionError(
                f"actor exceeded daily token budget ({self._max_tokens_per_day} tokens)"
            )


__all__ = [
    "DEFAULT_MAX_COMMANDS_PER_TICK",
    "DEFAULT_MAX_TOKENS_PER_DAY",
    "PERMISSIONS_BY_ROLE",
    "Policy",
]
