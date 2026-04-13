# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for auth model: RBAC, quotas, token budgets."""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError
from uuid_utils import uuid7

from archetype.app.auth import guard as _guard
from archetype.app.auth.guard import (
    MAX_CMDS_PER_TICK,
    MAX_TOKENS_PER_DAY,
    ROLE_PERMS,
    guardrail_allow,
    maybe_reset_daily_tokens,
    reset_daily_tokens,
    reset_tick_counters,
)
from archetype.app.auth.models import ActorCtx
from archetype.app.models import Command, CommandType


@pytest.fixture(autouse=True)
def _reset_quotas():
    """Reset quotas before each test."""
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


class TestRBAC:
    def test_admin_can_do_anything(self):
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        cmd = Command(type=CommandType.SPAWN, payload={})
        guardrail_allow(cmd, ctx)  # should not raise

    def test_viewer_cannot_spawn(self):
        ctx = ActorCtx(id=uuid7(), roles={"viewer"})
        cmd = Command(type=CommandType.SPAWN, payload={})
        with pytest.raises(PermissionError, match="cannot execute 'spawn'"):
            guardrail_allow(cmd, ctx)

    def test_player_can_spawn(self):
        ctx = ActorCtx(id=uuid7(), roles={"player"})
        cmd = Command(type=CommandType.SPAWN, payload={})
        guardrail_allow(cmd, ctx)  # should not raise

    def test_player_cannot_add_processor(self):
        ctx = ActorCtx(id=uuid7(), roles={"player"})
        cmd = Command(type=CommandType.ADD_PROCESSOR, payload={})
        with pytest.raises(PermissionError, match="cannot execute 'add_processor'"):
            guardrail_allow(cmd, ctx)

    def test_coder_can_update(self):
        ctx = ActorCtx(id=uuid7(), roles={"coder"})
        cmd = Command(type=CommandType.UPDATE, payload={})
        guardrail_allow(cmd, ctx)  # should not raise

    def test_maintainer_can_spawn_and_add_processor(self):
        ctx = ActorCtx(id=uuid7(), roles={"maintainer"})
        guardrail_allow(Command(type=CommandType.SPAWN, payload={}), ctx)
        guardrail_allow(Command(type=CommandType.ADD_PROCESSOR, payload={}), ctx)

    def test_multiple_roles_grant_union(self):
        ctx = ActorCtx(id=uuid7(), roles={"viewer", "player"})
        # Player can spawn
        guardrail_allow(Command(type=CommandType.SPAWN, payload={}), ctx)

    def test_no_roles_denies_everything(self):
        ctx = ActorCtx(id=uuid7(), roles=set())
        cmd = Command(type=CommandType.SPAWN, payload={})
        with pytest.raises(PermissionError):
            guardrail_allow(cmd, ctx)

    def test_role_perms_keys(self):
        assert "viewer" in ROLE_PERMS
        assert "coder" in ROLE_PERMS
        assert "maintainer" in ROLE_PERMS
        assert "admin" in ROLE_PERMS
        assert "player" in ROLE_PERMS


class TestQuotas:
    def test_per_tick_quota_enforced(self):
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        # Exhaust per-tick quota
        for _ in range(MAX_CMDS_PER_TICK):
            guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx)

        # Next should fail
        with pytest.raises(PermissionError, match="per-tick quota"):
            guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx)

    def test_reset_tick_counters_clears_quota(self):
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        for _ in range(MAX_CMDS_PER_TICK):
            guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx)

        reset_tick_counters()

        # Should succeed again after reset
        guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx)

    def test_different_actors_have_separate_quotas(self):
        ctx1 = ActorCtx(id=uuid7(), roles={"admin"})
        ctx2 = ActorCtx(id=uuid7(), roles={"admin"})

        for _ in range(MAX_CMDS_PER_TICK):
            guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx1)

        # ctx2 should still have quota
        guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx2)


class TestDailyTokenReset:
    """Regression tests for the ``daily-tokens-never-reset`` bug.

    ``MAX_TOKENS_PER_DAY`` was historically enforced without any code
    path calling ``reset_daily_tokens`` — so once an actor crossed the
    budget they were locked out for the lifetime of the process.
    ``guardrail_allow`` now calls :func:`maybe_reset_daily_tokens`
    on every command, which rolls the budget forward at UTC midnight.
    """

    def test_maybe_reset_no_op_within_same_day(self):
        ctx_id = uuid7()
        _guard._daily_tokens[ctx_id] = 123
        # Advance ``now`` within the same UTC day — must NOT clear.
        same_day = datetime.combine(
            _guard._last_reset_date, datetime.min.time(), tzinfo=UTC
        ) + timedelta(hours=1)
        did_reset = maybe_reset_daily_tokens(now=same_day)
        assert did_reset is False
        assert _guard._daily_tokens[ctx_id] == 123

    def test_maybe_reset_clears_on_day_rollover(self):
        ctx_id = uuid7()
        _guard._daily_tokens[ctx_id] = MAX_TOKENS_PER_DAY + 1
        # Advance to the next UTC day.
        next_day = datetime.combine(
            _guard._last_reset_date + timedelta(days=1),
            datetime.min.time(),
            tzinfo=UTC,
        )
        did_reset = maybe_reset_daily_tokens(now=next_day)
        assert did_reset is True
        assert _guard._daily_tokens == {}
        assert _guard._last_reset_date == next_day.date()

    def test_over_budget_actor_recovers_after_day_rollover(self):
        """The core regression: an over-budget actor must regain access
        once the UTC date advances, without any server restart."""
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        _guard._daily_tokens[ctx.id] = MAX_TOKENS_PER_DAY + 1

        cmd = Command(type=CommandType.SPAWN, payload={})
        with pytest.raises(PermissionError, match="daily token budget"):
            guardrail_allow(cmd, ctx)

        # Simulate the UTC date rolling over: rewind ``_last_reset_date``
        # so the next ``guardrail_allow`` call sees a fresh day. In
        # production, wall-clock time advances and this happens on its
        # own.
        _guard._last_reset_date = _guard._last_reset_date - timedelta(days=1)

        # After the rollover the same actor can submit again; the lazy
        # reset inside ``guardrail_allow`` clears the stale budget.
        guardrail_allow(cmd, ctx)
        assert _guard._daily_tokens[ctx.id] == _guard.estimate_token_cost(cmd)


class TestActorCtx:
    def test_frozen(self):
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        with pytest.raises(ValidationError):
            ctx.roles = {"viewer"}

    def test_default_roles(self):
        ctx = ActorCtx(id=uuid7())
        assert "viewer" in ctx.roles
