# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Audit Section C.1 — COMMANDS_BY_ROLE.

The role-permission matrix is the authoritative source for what each role
can do. Tests downstream of the gate (`tests/api/`, `tests/cli/`) are
*supposed* to parametrize from this dict, so any structural drift here
silently re-shapes the security model.

These tests assert structure only. Behavioral tests for `guardrail_allow`
itself live in the next round of the audit suite.
"""

from __future__ import annotations

import pytest

from archetype.app.auth.permissions import COMMANDS_BY_ROLE
from archetype.app.models import CommandType

pytestmark = [pytest.mark.audit, pytest.mark.audit_critical]


EXPECTED_ROLES = {"viewer", "player", "operator", "admin"}


# ── C.1.1 — module exports ──────────────────────────────────────────────


def test_c_1_1_module_exports_mapping() -> None:
    """app/auth/permissions.py exports COMMANDS_BY_ROLE."""
    assert isinstance(COMMANDS_BY_ROLE, dict)
    assert COMMANDS_BY_ROLE, "COMMANDS_BY_ROLE must not be empty"


# ── C.1.2 — type shape ──────────────────────────────────────────────────


def test_c_1_2_dict_str_to_frozenset_command_type() -> None:
    """COMMANDS_BY_ROLE is dict[str, frozenset[CommandType]]."""
    for role, cmds in COMMANDS_BY_ROLE.items():
        assert isinstance(role, str), f"Role key {role!r} must be a str"
        assert isinstance(cmds, frozenset), (
            f"Role {role!r}: value must be frozenset, got {type(cmds).__name__}"
        )
        for cmd in cmds:
            assert isinstance(cmd, CommandType), (
                f"Role {role!r}: non-CommandType entry {cmd!r}"
            )


# ── C.1.3 — exactly the four roles ──────────────────────────────────────


def test_c_1_3_four_role_keys_only() -> None:
    """Roles are exactly {viewer, player, operator, admin}."""
    actual = set(COMMANDS_BY_ROLE.keys())
    extra = actual - EXPECTED_ROLES
    missing = EXPECTED_ROLES - actual
    assert not extra, f"Unexpected roles: {sorted(extra)}"
    assert not missing, f"Missing roles: {sorted(missing)}"


# ── C.1.4 — admin is the full enum ──────────────────────────────────────


def test_c_1_4_admin_is_full_command_type_set() -> None:
    """admin's set is `frozenset(CommandType)` — auto-includes new types."""
    admin = COMMANDS_BY_ROLE["admin"]
    assert admin == frozenset(CommandType), (
        "admin must be frozenset(CommandType) so new CommandType members "
        "are admin-allowed by construction."
    )


# ── C.1.5 — operator ⊇ player ───────────────────────────────────────────


def test_c_1_5_operator_superset_of_player() -> None:
    """operator inherits player's permissions plus its own additions."""
    player = COMMANDS_BY_ROLE["player"]
    operator = COMMANDS_BY_ROLE["operator"]
    missing = player - operator
    assert not missing, (
        f"operator must be a superset of player; missing: {sorted(c.value for c in missing)}"
    )


# ── C.1.6 — player ⊇ viewer ─────────────────────────────────────────────


def test_c_1_6_player_superset_of_viewer() -> None:
    """player inherits viewer's permissions plus its own additions."""
    viewer = COMMANDS_BY_ROLE["viewer"]
    player = COMMANDS_BY_ROLE["player"]
    missing = viewer - player
    assert not missing, (
        f"player must be a superset of viewer; missing: {sorted(c.value for c in missing)}"
    )


# ── C.1.7 — admin contains every CommandType ────────────────────────────


def test_c_1_7_admin_contains_all_command_types() -> None:
    """Every CommandType member appears in admin."""
    admin = COMMANDS_BY_ROLE["admin"]
    for cmd in CommandType:
        assert cmd in admin, f"admin missing {cmd.name}"


# ── C.1.8 — read commands available to viewer ───────────────────────────


def test_c_1_8_reads_in_viewer() -> None:
    """Reads (QUERY_WORLD, GET_WORLD_INFO, GET_AUDIT_HISTORY, LIST_*) appear
    in viewer's set."""
    viewer = COMMANDS_BY_ROLE["viewer"]
    required_reads = {
        CommandType.QUERY_WORLD,
        CommandType.GET_WORLD_INFO,
        CommandType.GET_AUDIT_HISTORY,
        CommandType.LIST_PROCESSORS,
        CommandType.LIST_HOOKS,
        CommandType.LIST_RESOURCES,
    }
    missing = required_reads - viewer
    assert not missing, (
        f"viewer missing required reads: {sorted(c.name for c in missing)}"
    )


# ── C.1.9 — CREATE_WORLD admin-only ─────────────────────────────────────


def test_c_1_9_create_world_admin_only() -> None:
    """CREATE_WORLD appears ONLY in admin's set."""
    for role in ("viewer", "player", "operator"):
        assert CommandType.CREATE_WORLD not in COMMANDS_BY_ROLE[role], (
            f"CREATE_WORLD must not be in {role}'s set"
        )
    assert CommandType.CREATE_WORLD in COMMANDS_BY_ROLE["admin"]


# ── C.1.10 — FORK_WORLD / DESTROY_WORLD scope ───────────────────────────


def test_c_1_10_fork_destroy_operator_and_admin_only() -> None:
    """FORK_WORLD and DESTROY_WORLD are in operator and admin (NOT viewer or player)."""
    sensitive = (CommandType.FORK_WORLD, CommandType.DESTROY_WORLD)
    for cmd in sensitive:
        assert cmd not in COMMANDS_BY_ROLE["viewer"], f"{cmd.name} leaked into viewer"
        assert cmd not in COMMANDS_BY_ROLE["player"], f"{cmd.name} leaked into player"
        assert cmd in COMMANDS_BY_ROLE["operator"], f"{cmd.name} missing from operator"
        assert cmd in COMMANDS_BY_ROLE["admin"], f"{cmd.name} missing from admin"
