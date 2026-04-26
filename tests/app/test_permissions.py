# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the four-role permissions matrix.

Generates one test case per (role, command_type) pair from COMMANDS_BY_ROLE.
Adding a new CommandType automatically expands coverage.
"""

import pytest
from uuid_utils import uuid7

from archetype.app.auth.errors import GuardrailError
from archetype.app.auth.guard import guardrail_allow, reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.auth.permissions import COMMANDS_BY_ROLE
from archetype.app.models import Command, CommandType

ALL_ROLES = frozenset(COMMANDS_BY_ROLE.keys())


def _matrix_cases() -> list[tuple[str, CommandType, bool]]:
    return [
        (role, cmd, cmd in COMMANDS_BY_ROLE[role])
        for role in sorted(ALL_ROLES)
        for cmd in CommandType
    ]


@pytest.fixture(autouse=True)
def _reset_counters():
    """Reset quota counters between tests."""
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest.mark.parametrize("role,cmd_type,allowed", _matrix_cases(), ids=lambda x: str(x))
def test_role_command_matrix(role, cmd_type, allowed):
    """Each (role, command) pair is either permitted or denied per the matrix."""
    ctx = ActorCtx(id=uuid7(), roles={role})
    cmd = Command(type=cmd_type, payload={})

    if allowed:
        guardrail_allow(cmd, ctx)  # should not raise
    else:
        with pytest.raises(GuardrailError):
            guardrail_allow(cmd, ctx)


def test_admin_allows_everything():
    """Admin role permits every command type."""
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    for cmd_type in CommandType:
        guardrail_allow(Command(type=cmd_type), ctx)


def test_viewer_cannot_mutate():
    """Viewer can only read."""
    ctx = ActorCtx(id=uuid7(), roles={"viewer"})

    # Reads succeed
    guardrail_allow(Command(type=CommandType.QUERY_WORLD), ctx)
    guardrail_allow(Command(type=CommandType.GET_WORLD_INFO), ctx)

    # Mutations fail
    with pytest.raises(GuardrailError):
        guardrail_allow(Command(type=CommandType.SPAWN), ctx)
    with pytest.raises(GuardrailError):
        guardrail_allow(Command(type=CommandType.CREATE_WORLD), ctx)


def test_player_can_spawn_but_not_add_component():
    """Player can mutate entity values but not schema."""
    ctx = ActorCtx(id=uuid7(), roles={"player"})

    guardrail_allow(Command(type=CommandType.SPAWN), ctx)
    guardrail_allow(Command(type=CommandType.DESPAWN), ctx)
    guardrail_allow(Command(type=CommandType.UPDATE), ctx)

    with pytest.raises(GuardrailError):
        guardrail_allow(Command(type=CommandType.ADD_COMPONENT), ctx)
    with pytest.raises(GuardrailError):
        guardrail_allow(Command(type=CommandType.STEP), ctx)


def test_operator_can_run_and_fork():
    """Operator has simulation control and fork/destroy."""
    ctx = ActorCtx(id=uuid7(), roles={"operator"})

    guardrail_allow(Command(type=CommandType.STEP), ctx)
    guardrail_allow(Command(type=CommandType.RUN), ctx)
    guardrail_allow(Command(type=CommandType.RUN_EPISODE), ctx)
    guardrail_allow(Command(type=CommandType.RUN_ROLLOUT), ctx)
    guardrail_allow(Command(type=CommandType.FORK_WORLD), ctx)
    guardrail_allow(Command(type=CommandType.DESTROY_WORLD), ctx)

    # But cannot create worlds from scratch
    with pytest.raises(GuardrailError):
        guardrail_allow(Command(type=CommandType.CREATE_WORLD), ctx)


def test_multi_role_union():
    """Multiple roles compose via union — both sets of permissions apply."""
    ctx = ActorCtx(id=uuid7(), roles={"viewer", "player"})

    # Gets viewer reads + player mutations
    guardrail_allow(Command(type=CommandType.QUERY_WORLD), ctx)
    guardrail_allow(Command(type=CommandType.SPAWN), ctx)

    # Still can't do operator things
    with pytest.raises(GuardrailError):
        guardrail_allow(Command(type=CommandType.STEP), ctx)
