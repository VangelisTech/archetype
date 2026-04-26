# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Roles and permissions matrix.

The four-role model: viewer, player, operator, admin.
Each role's allowed commands are defined here. `guardrail_allow` uses
this mapping to decide whether to permit or deny.

Authoring is role-by-role; each tier adds to its predecessor with `|`.
Admin gets everything automatically via `frozenset(CommandType)`.
"""

from archetype.app.models import CommandType

# ── Reads: universally allowed ────────────────────────────────────────────

_READS = frozenset({
    CommandType.QUERY_WORLD,
    CommandType.GET_WORLD_INFO,
    CommandType.GET_AUDIT_HISTORY,
    CommandType.LIST_SIGNATURES,
    CommandType.LIST_PROCESSORS,
    CommandType.LIST_HOOKS,
    CommandType.LIST_RESOURCES,
})

# ── Player adds: entity-value mutation + participation ────────────────────

_PLAYER_ADDS = frozenset({
    CommandType.SPAWN,
    CommandType.DESPAWN,
    CommandType.UPDATE,
    CommandType.MESSAGE,
    CommandType.CUSTOM,
})

# ── Operator adds: schema mutation, system management, simulation,
#    fork + destroy ────────────────────────────────────────────────────────

_OPERATOR_ADDS = frozenset({
    CommandType.ADD_COMPONENT,
    CommandType.REMOVE_COMPONENT,
    CommandType.ADD_PROCESSOR,
    CommandType.REMOVE_PROCESSOR,
    CommandType.ADD_HOOK,
    CommandType.REMOVE_HOOK,
    CommandType.ADD_RESOURCE,
    CommandType.STEP,
    CommandType.RUN,
    CommandType.RUN_EPISODE,
    CommandType.RUN_ROLLOUT,
    CommandType.FORK_WORLD,
    CommandType.DESTROY_WORLD,
})

# ── The mapping ───────────────────────────────────────────────────────────

COMMANDS_BY_ROLE: dict[str, frozenset[CommandType]] = {
    "viewer": _READS,
    "player": _READS | _PLAYER_ADDS,
    "operator": _READS | _PLAYER_ADDS | _OPERATOR_ADDS,
    "admin": frozenset(CommandType),  # everything; auto-includes new types
}

# ── Derived inverse (command → roles that can execute it) ─────────────────

ROLES_BY_COMMAND: dict[CommandType, frozenset[str]] = {
    cmd: frozenset(role for role, cmds in COMMANDS_BY_ROLE.items() if cmd in cmds)
    for cmd in CommandType
}
