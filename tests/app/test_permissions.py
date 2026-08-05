# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the commands-owned flat role matrix."""

from __future__ import annotations

import pytest
from uuid_utils import uuid7

from archetype.commands.models import ActorCtx
from archetype.commands.policy import PERMISSIONS_BY_ROLE, Policy

_VIEWER_PERMISSIONS = frozenset(
    {
        "discover_worlds",
        "get_audit_history",
        "get_world_info",
        "list_hooks",
        "list_processors",
        "list_resources",
        "list_signatures",
        "list_worlds",
        "open_world_readonly",
        "query_archetype",
        "query_artifacts",
        "query_components",
    }
)
_PLAYER_PERMISSIONS = _VIEWER_PERMISSIONS | {
    "create_entities",
    "despawn",
    "spawn",
    "update",
}
_OPERATOR_PERMISSIONS = _PLAYER_PERMISSIONS | {
    "add_components",
    "add_hook",
    "add_processor",
    "add_resource",
    "autoresearch",
    "destroy_world",
    "evaluate",
    "fork_world",
    "ingest_artifacts",
    "remove_components",
    "remove_hook",
    "remove_processor",
    "run",
    "run_episode",
    "run_rollout",
    "step",
}
_ADMIN_PERMISSIONS = _OPERATOR_PERMISSIONS | {
    "create_world",
    "resume_world",
}
_EXPECTED_PERMISSIONS_BY_ROLE = {
    "viewer": _VIEWER_PERMISSIONS,
    "player": _PLAYER_PERMISSIONS,
    "operator": _OPERATOR_PERMISSIONS,
    "admin": _ADMIN_PERMISSIONS,
}
_ALL_PERMISSIONS = frozenset().union(*_EXPECTED_PERMISSIONS_BY_ROLE.values())


def _matrix_cases() -> list[tuple[str, str, bool]]:
    return [
        (role, permission, permission in allowed)
        for role, allowed in sorted(_EXPECTED_PERMISSIONS_BY_ROLE.items())
        for permission in sorted(_ALL_PERMISSIONS)
    ]


def test_canonical_role_matrix_is_exact() -> None:
    assert PERMISSIONS_BY_ROLE == _EXPECTED_PERMISSIONS_BY_ROLE


@pytest.mark.parametrize(
    ("role", "permission", "allowed"),
    _matrix_cases(),
)
def test_every_role_permission_pair_is_explicit(
    role: str,
    permission: str,
    allowed: bool,
) -> None:
    policy = Policy()
    actor = ActorCtx(id=uuid7(), roles={role})

    if allowed:
        policy.preauthorize(actor, permission=permission)
    else:
        with pytest.raises(PermissionError, match="cannot execute permission"):
            policy.preauthorize(actor, permission=permission)


def test_admin_is_finite_and_unknown_permissions_fail_closed() -> None:
    admin = ActorCtx(id=uuid7(), roles={"admin"})

    with pytest.raises(PermissionError, match="cannot execute permission"):
        Policy().preauthorize(admin, permission="future_unregistered_operation")


def test_player_can_mutate_entity_values_but_not_schema() -> None:
    player = ActorCtx(id=uuid7(), roles={"player"})
    policy = Policy()

    for permission in ("spawn", "create_entities", "despawn", "update"):
        policy.preauthorize(player, permission=permission)

    for permission in ("add_components", "remove_components", "step"):
        with pytest.raises(PermissionError, match="cannot execute permission"):
            policy.preauthorize(player, permission=permission)


def test_operator_controls_simulation_but_not_application_world_creation() -> None:
    operator = ActorCtx(id=uuid7(), roles={"operator"})
    policy = Policy()

    for permission in (
        "step",
        "run",
        "run_episode",
        "run_rollout",
        "fork_world",
        "destroy_world",
    ):
        policy.preauthorize(operator, permission=permission)

    for permission in ("create_world", "resume_world"):
        with pytest.raises(PermissionError, match="cannot execute permission"):
            policy.preauthorize(operator, permission=permission)


def test_multi_role_grants_are_a_flat_union() -> None:
    actor = ActorCtx(id=uuid7(), roles={"viewer", "player"})
    policy = Policy()

    policy.preauthorize(actor, permission="query_components")
    policy.preauthorize(actor, permission="spawn")
    with pytest.raises(PermissionError, match="cannot execute permission"):
        policy.preauthorize(actor, permission="step")
