# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for auth model: RBAC, quotas, token budgets."""

import pytest
from pydantic import ValidationError
from uuid_utils import uuid7

from archetype.app.auth.guard import (
    MAX_CMDS_PER_TICK,
    ROLE_PERMS,
    guardrail_allow,
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


class TestActorCtx:
    def test_frozen(self):
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        with pytest.raises(ValidationError):
            ctx.roles = {"viewer"}

    def test_default_roles(self):
        ctx = ActorCtx(id=uuid7())
        assert "viewer" in ctx.roles
