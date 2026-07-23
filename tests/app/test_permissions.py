# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the four-role permissions matrix.

Generates one test case per (role, command_type) pair from COMMANDS_BY_ROLE.
Adding a new CommandType automatically expands coverage.
"""

import pytest
from uuid_utils import uuid7

from archetype.app.gateway.auth import guard as _guard
from archetype.app.gateway.auth.errors import GuardrailError
from archetype.app.gateway.auth.guard import guardrail_allow, reset_daily_tokens
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.gateway.auth.permissions import COMMANDS_BY_ROLE
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
    _guard._tick_counters.clear()
    reset_daily_tokens()
    yield
    _guard._tick_counters.clear()
    reset_daily_tokens()


def _allow(command: Command, ctx: ActorCtx) -> None:
    guardrail_allow(command, ctx, world_id="world-1", target_tick=0)


@pytest.mark.parametrize("role,cmd_type,allowed", _matrix_cases(), ids=lambda x: str(x))
def test_role_command_matrix(role, cmd_type, allowed):
    """Each (role, command) pair is either permitted or denied per the matrix."""
    ctx = ActorCtx(id=uuid7(), roles={role})
    cmd = Command(type=cmd_type, payload={})

    if allowed:
        _allow(cmd, ctx)  # should not raise
    else:
        with pytest.raises(GuardrailError):
            _allow(cmd, ctx)


def test_admin_allows_everything():
    """Admin role permits every command type."""
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    for cmd_type in CommandType:
        _allow(Command(type=cmd_type), ctx)


def test_viewer_cannot_mutate():
    """Viewer can only read."""
    ctx = ActorCtx(id=uuid7(), roles={"viewer"})

    # Reads succeed
    _allow(Command(type=CommandType.QUERY_WORLD), ctx)
    _allow(Command(type=CommandType.GET_WORLD_INFO), ctx)

    # Mutations fail
    with pytest.raises(GuardrailError):
        _allow(Command(type=CommandType.SPAWN), ctx)
    with pytest.raises(GuardrailError):
        _allow(Command(type=CommandType.CREATE_WORLD), ctx)


def test_player_can_spawn_but_not_add_component():
    """Player can mutate entity values but not schema."""
    ctx = ActorCtx(id=uuid7(), roles={"player"})

    _allow(Command(type=CommandType.SPAWN), ctx)
    _allow(Command(type=CommandType.DESPAWN), ctx)
    _allow(Command(type=CommandType.UPDATE), ctx)

    with pytest.raises(GuardrailError):
        _allow(Command(type=CommandType.ADD_COMPONENT), ctx)
    with pytest.raises(GuardrailError):
        _allow(Command(type=CommandType.STEP), ctx)


def test_operator_can_run_and_fork():
    """Operator has simulation control and fork/destroy."""
    ctx = ActorCtx(id=uuid7(), roles={"operator"})

    _allow(Command(type=CommandType.STEP), ctx)
    _allow(Command(type=CommandType.RUN), ctx)
    _allow(Command(type=CommandType.RUN_EPISODE), ctx)
    _allow(Command(type=CommandType.RUN_ROLLOUT), ctx)
    _allow(Command(type=CommandType.FORK_WORLD), ctx)
    _allow(Command(type=CommandType.DESTROY_WORLD), ctx)

    # But cannot create worlds from scratch
    with pytest.raises(GuardrailError):
        _allow(Command(type=CommandType.CREATE_WORLD), ctx)


def test_multi_role_union():
    """Multiple roles compose via union — both sets of permissions apply."""
    ctx = ActorCtx(id=uuid7(), roles={"viewer", "player"})

    # Gets viewer reads + player mutations
    _allow(Command(type=CommandType.QUERY_WORLD), ctx)
    _allow(Command(type=CommandType.SPAWN), ctx)

    # Still can't do operator things
    with pytest.raises(GuardrailError):
        _allow(Command(type=CommandType.STEP), ctx)
