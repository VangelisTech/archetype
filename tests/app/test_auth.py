# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Compatibility and instance-owned commands policy contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from types import ModuleType

import pytest
from pydantic import ValidationError
from uuid_utils import uuid7

import archetype.commands.policy as policy_module
from archetype.app.gateway.auth import ActorCtx as CompatibilityActorCtx
from archetype.commands.models import ActorCtx, PolicyRequest
from archetype.commands.policy import (
    DEFAULT_MAX_COMMANDS_PER_TICK,
    DEFAULT_MAX_TOKENS_PER_DAY,
    Policy,
)


@dataclass(slots=True)
class _UtcClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value


def _authorize(
    policy: Policy,
    actor: ActorCtx,
    *,
    world_id: object = "world-a",
    target_tick: int = 7,
    token_cost: int = 0,
) -> None:
    policy.authorize(
        actor,
        permission="spawn",
        world_id=world_id,
        target_tick=target_tick,
        token_cost=token_cost,
    )


def test_compatibility_actor_is_the_commands_owned_frozen_value() -> None:
    assert CompatibilityActorCtx is ActorCtx
    actor = CompatibilityActorCtx(id=uuid7(), roles={"admin"})

    with pytest.raises(ValidationError):
        actor.roles = {"viewer"}

    assert ActorCtx(id=uuid7()).roles == {"viewer"}


def test_policy_defaults_preserve_the_ratified_quota_limits() -> None:
    assert DEFAULT_MAX_COMMANDS_PER_TICK == 500
    assert DEFAULT_MAX_TOKENS_PER_DAY == 200_000


def test_tick_quota_generation_is_actor_world_and_target_tick_scoped() -> None:
    policy = Policy(max_commands_per_tick=1, max_tokens_per_day=1_000)
    actor_a = ActorCtx(id=uuid7(), roles={"player"})
    actor_b = ActorCtx(id=uuid7(), roles={"player"})

    _authorize(policy, actor_a, world_id="world-a", target_tick=7)
    with pytest.raises(PermissionError, match="per-tick quota"):
        _authorize(policy, actor_a, world_id="world-a", target_tick=7)

    _authorize(policy, actor_b, world_id="world-a", target_tick=7)
    _authorize(policy, actor_a, world_id="world-b", target_tick=7)
    _authorize(policy, actor_a, world_id="world-a", target_tick=8)


def test_policy_instances_never_share_quota_state() -> None:
    actor = ActorCtx(id=uuid7(), roles={"player"})
    first = Policy(max_commands_per_tick=1, max_tokens_per_day=1)
    second = Policy(max_commands_per_tick=1, max_tokens_per_day=1)

    _authorize(first, actor, token_cost=1)
    with pytest.raises(PermissionError, match="per-tick quota"):
        _authorize(first, actor)

    _authorize(second, actor, token_cost=1)


def test_batch_rejection_is_atomic() -> None:
    actor = ActorCtx(id=uuid7(), roles={"player"})
    policy = Policy(max_commands_per_tick=1, max_tokens_per_day=1_000)
    request = PolicyRequest(
        permission="spawn",
        world_id="world-a",
        target_tick=9,
        token_cost=1,
    )

    with pytest.raises(PermissionError, match="per-tick quota"):
        policy.authorize_batch(actor, requests=(request, request))

    policy.authorize_batch(actor, requests=(request,))
    with pytest.raises(PermissionError, match="per-tick quota"):
        policy.authorize_batch(actor, requests=(request,))


def test_role_denial_is_pure_and_precedes_clock_or_quota_work() -> None:
    clock_calls = 0

    def forbidden_clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        raise AssertionError("denied authorization must not read the quota clock")

    policy = Policy(utcnow=forbidden_clock)
    viewer = ActorCtx(id=uuid7(), roles={"viewer"})

    with pytest.raises(PermissionError, match="cannot execute permission 'spawn'"):
        _authorize(policy, viewer, token_cost=100)

    assert clock_calls == 0


def test_daily_generation_uses_utc_and_rolls_at_the_next_utc_date() -> None:
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    clock = _UtcClock(datetime(2026, 7, 23, 23, 30, tzinfo=UTC))
    policy = Policy(max_tokens_per_day=5, utcnow=clock)

    policy.authorize_application(actor, permission="create_world", token_cost=5)

    clock.value = datetime(
        2026,
        7,
        23,
        18,
        45,
        tzinfo=timezone(-timedelta(hours=5)),
    )
    with pytest.raises(PermissionError, match="daily token budget"):
        policy.authorize_application(actor, permission="create_world", token_cost=1)

    clock.value = datetime(
        2026,
        7,
        23,
        19,
        15,
        tzinfo=timezone(-timedelta(hours=5)),
    )
    policy.authorize_application(actor, permission="create_world", token_cost=5)


def test_application_scope_has_no_synthetic_tick_bucket() -> None:
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    policy = Policy(max_commands_per_tick=1, max_tokens_per_day=10)

    for _ in range(3):
        policy.authorize_application(actor, permission="create_world", token_cost=0)

    _authorize(policy, actor, token_cost=0)
    with pytest.raises(PermissionError, match="per-tick quota"):
        _authorize(policy, actor, token_cost=0)


@pytest.mark.parametrize(
    ("world_id", "target_tick", "error"),
    [
        (None, 0, ValueError),
        ("", 0, ValueError),
        ("world-a", -1, ValueError),
        ("world-a", True, TypeError),
    ],
)
def test_policy_coordinates_fail_closed(
    world_id: object,
    target_tick: int,
    error: type[Exception],
) -> None:
    actor = ActorCtx(id=uuid7(), roles={"player"})
    policy = Policy()

    with pytest.raises(error):
        _authorize(
            policy,
            actor,
            world_id=world_id,
            target_tick=target_tick,
        )


def test_policy_module_exposes_no_legacy_process_global_authority() -> None:
    assert isinstance(policy_module, ModuleType)
    forbidden = {
        "_daily_tokens",
        "_last_reset_date",
        "_tick_counters",
        "guardrail_allow",
        "maybe_reset_daily_tokens",
        "reset_daily_tokens",
        "reset_tick_quotas",
        "set_quota_reset",
    }

    assert forbidden.isdisjoint(vars(policy_module))
