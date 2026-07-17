# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for auth: quotas, token budgets, daily reset."""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError
from uuid_utils import uuid7

from archetype.app.auth import guard as _guard
from archetype.app.auth.errors import GuardrailError
from archetype.app.auth.guard import (
    MAX_CMDS_PER_TICK,
    MAX_TOKENS_PER_DAY,
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


class TestQuotas:
    def test_per_tick_quota_enforced(self):
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        for _ in range(MAX_CMDS_PER_TICK):
            guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx)

        with pytest.raises(GuardrailError, match="per-tick quota"):
            guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx)

    def test_reset_tick_counters_clears_quota(self):
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        for _ in range(MAX_CMDS_PER_TICK):
            guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx)

        reset_tick_counters()
        guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx)

    def test_different_actors_have_separate_quotas(self):
        ctx1 = ActorCtx(id=uuid7(), roles={"admin"})
        ctx2 = ActorCtx(id=uuid7(), roles={"admin"})

        for _ in range(MAX_CMDS_PER_TICK):
            guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx1)

        # ctx2 should still have quota
        guardrail_allow(Command(type=CommandType.CUSTOM, payload={}), ctx2)


class TestDailyTokenReset:
    def test_maybe_reset_no_op_within_same_day(self):
        ctx_id = uuid7()
        _guard._daily_tokens[ctx_id] = 123
        same_day = datetime.combine(
            _guard._last_reset_date, datetime.min.time(), tzinfo=UTC
        ) + timedelta(hours=1)
        did_reset = maybe_reset_daily_tokens(now=same_day)
        assert did_reset is False
        assert _guard._daily_tokens[ctx_id] == 123

    def test_maybe_reset_clears_on_day_rollover(self):
        ctx_id = uuid7()
        _guard._daily_tokens[ctx_id] = MAX_TOKENS_PER_DAY + 1
        next_day = datetime.combine(
            _guard._last_reset_date + timedelta(days=1),
            datetime.min.time(),
            tzinfo=UTC,
        )
        did_reset = maybe_reset_daily_tokens(now=next_day)
        assert did_reset is True
        assert _guard._daily_tokens == {}
        assert _guard._last_reset_date == next_day.date()

    def test_maybe_reset_uses_utc_date_for_offset_datetime(self):
        ctx_id = uuid7()
        _guard._daily_tokens[ctx_id] = 123
        _guard._last_reset_date = datetime(2026, 1, 1, tzinfo=UTC).date()

        utc_next_day = datetime(2026, 1, 1, 20, tzinfo=timezone(timedelta(hours=-5)))
        assert maybe_reset_daily_tokens(now=utc_next_day) is True
        assert _guard._last_reset_date == datetime(2026, 1, 2, tzinfo=UTC).date()

    def test_over_budget_actor_recovers_after_day_rollover(self):
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        _guard._daily_tokens[ctx.id] = MAX_TOKENS_PER_DAY + 1
        today = datetime.combine(_guard._last_reset_date, datetime.min.time(), tzinfo=UTC)

        cmd = Command(type=CommandType.SPAWN, payload={})
        with pytest.raises(GuardrailError, match="daily token budget"):
            guardrail_allow(cmd, ctx, now=today)

        guardrail_allow(cmd, ctx, now=today + timedelta(days=1))
        assert _guard._daily_tokens[ctx.id] == _guard.estimate_token_cost(cmd)


class TestActorCtx:
    def test_frozen(self):
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        with pytest.raises(ValidationError):
            ctx.roles = {"viewer"}

    def test_default_roles(self):
        ctx = ActorCtx(id=uuid7())
        assert "viewer" in ctx.roles


class TestAutoresearchTokenCost:
    """One autoresearch command performs max_iterations rollouts; the budget
    must charge it accordingly, not as one flat command."""

    def test_cost_scales_with_iterations(self):
        flat = Command(type=CommandType.AUTORESEARCH, payload={})
        assert _guard.estimate_token_cost(flat) == _guard._TOKEN_COSTS["autoresearch"]

        loop = Command(type=CommandType.AUTORESEARCH, payload={"max_iterations": 100})
        assert _guard.estimate_token_cost(loop) == _guard._TOKEN_COSTS["run_rollout"] * 100, (
            "the loop is charged at the rollout rate per iteration"
        )

    def test_large_loop_exceeds_budget_where_one_rollout_would_not(self):
        ctx = ActorCtx(id=uuid7(), roles={"operator"})
        _guard._daily_tokens[ctx.id] = MAX_TOKENS_PER_DAY - 300

        guardrail_allow(Command(type=CommandType.RUN_ROLLOUT, payload={}), ctx)

        with pytest.raises(GuardrailError, match="daily token budget"):
            guardrail_allow(
                Command(type=CommandType.AUTORESEARCH, payload={"max_iterations": 100}),
                ctx,
            )
