# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Audit Section A.1 — Foundational types and events.

Each test asserts the contract from the pre-merge checklist for one model
or enum. These are reflective: they import the live module and inspect
declared fields, frozen status, and enum membership.

When this file goes red, the model has drifted from the spec. The fix is
either to re-align the model or to amend the spec — both are atomic PRs.
"""

from __future__ import annotations

import dataclasses

import pytest
from pydantic import BaseModel

from archetype.app.models import (
    CommandType,
    EpisodeConfig,
    EpisodeResult,
    HookInfo,
    ProcessorInfo,
    ResourceInfo,
    RolloutConfig,
    RolloutResult,
    WorldInfo,
)
from archetype.core.hooks import HookEvent, OnDestroy

pytestmark = pytest.mark.audit


# ── A.1.1 — OnDestroy event ─────────────────────────────────────────────


def test_a_1_1_on_destroy_event() -> None:
    """OnDestroy is a HookEvent dataclass with `world_id`, frozen + slots."""
    assert issubclass(OnDestroy, HookEvent)
    assert dataclasses.is_dataclass(OnDestroy)

    params = OnDestroy.__dataclass_params__
    assert params.frozen, "OnDestroy must be frozen"
    # slots is a class-level configuration; verify the descriptor
    # is in __slots__ via the class definition.
    assert "__slots__" in OnDestroy.__dict__ or "__slots__" in HookEvent.__dict__, (
        "OnDestroy (or its base) must declare __slots__"
    )

    fields = {f.name: f for f in dataclasses.fields(OnDestroy)}
    assert "world_id" in fields, "OnDestroy must carry world_id"


def _model_fields(cls: type[BaseModel]) -> set[str]:
    return set(cls.model_fields.keys())


def _is_frozen(cls: type[BaseModel]) -> bool:
    return bool(cls.model_config.get("frozen"))


# ── A.1.2 — WorldInfo ───────────────────────────────────────────────────


def test_a_1_2_world_info() -> None:
    """WorldInfo is frozen and exposes (world_id, name, tick, run_id)."""
    assert _is_frozen(WorldInfo), "WorldInfo must be frozen"
    expected = {"world_id", "name", "tick", "run_id"}
    assert expected.issubset(_model_fields(WorldInfo)), (
        f"WorldInfo missing fields: {expected - _model_fields(WorldInfo)}"
    )


# ── A.1.3 — ProcessorInfo ───────────────────────────────────────────────


def test_a_1_3_processor_info() -> None:
    """ProcessorInfo: frozen, fields qualname/priority/components."""
    assert _is_frozen(ProcessorInfo)
    assert _model_fields(ProcessorInfo) == {"qualname", "priority", "components"}


# ── A.1.4 — HookInfo ────────────────────────────────────────────────────


def test_a_1_4_hook_info() -> None:
    """HookInfo: frozen, fields event_type/handler_qualname/mode/handle_id."""
    assert _is_frozen(HookInfo)
    assert _model_fields(HookInfo) == {
        "event_type",
        "handler_qualname",
        "mode",
        "handle_id",
    }


# ── A.1.5 — ResourceInfo (qualname-only) ────────────────────────────────


def test_a_1_5_resource_info_qualname_only() -> None:
    """ResourceInfo carries ONLY `qualname` — resources are type-keyed."""
    assert _is_frozen(ResourceInfo)
    assert _model_fields(ResourceInfo) == {"qualname"}, (
        "ResourceInfo must expose qualname only; identifier-style fields "
        "would imply non-type-keyed resources, violating the container "
        "invariant."
    )


# ── A.1.6 — EpisodeConfig ───────────────────────────────────────────────


def test_a_1_6_episode_config() -> None:
    """EpisodeConfig: episode_id, run_config, max_steps, terminal_component, termination."""
    expected = {
        "episode_id",
        "run_config",
        "max_steps",
        "terminal_component",
        "termination",
    }
    assert expected.issubset(_model_fields(EpisodeConfig)), (
        f"EpisodeConfig missing: {expected - _model_fields(EpisodeConfig)}"
    )


# ── A.1.7 — RolloutConfig ───────────────────────────────────────────────


def test_a_1_7_rollout_config() -> None:
    """RolloutConfig: rollout_id, episode_config, num_episodes, parallel,
    name_prefix, destroy_forks_on_complete."""
    expected = {
        "rollout_id",
        "episode_config",
        "num_episodes",
        "parallel",
        "name_prefix",
        "destroy_forks_on_complete",
    }
    actual = _model_fields(RolloutConfig)
    assert expected.issubset(actual), f"RolloutConfig missing: {expected - actual}"


# ── A.1.8 — EpisodeResult ───────────────────────────────────────────────


def test_a_1_8_episode_result() -> None:
    """EpisodeResult is frozen."""
    assert _is_frozen(EpisodeResult)
    # Core fields the spec implies for a per-episode summary.
    assert {"episode_id", "world_id", "terminated"}.issubset(
        _model_fields(EpisodeResult)
    )


# ── A.1.9 — RolloutResult ───────────────────────────────────────────────


def test_a_1_9_rollout_result() -> None:
    """RolloutResult is frozen and carries an `episodes` collection."""
    assert _is_frozen(RolloutResult)
    assert {"rollout_id", "episodes"}.issubset(_model_fields(RolloutResult))


# ── A.1.10 — CommandType enum entries ───────────────────────────────────


@pytest.mark.audit_critical
def test_a_1_10_command_type_entries() -> None:
    """CommandType must include the post-rewrite entries.

    Drift here breaks both the role-permission matrix (admin's
    `frozenset(CommandType)`) and the gate's audit-row taxonomy.
    """
    required = {
        "GET_WORLD_INFO",
        "GET_AUDIT_HISTORY",
        "LIST_PROCESSORS",
        "LIST_HOOKS",
        "LIST_RESOURCES",
        "ADD_RESOURCE",
        "RUN_EPISODE",
        "RUN_ROLLOUT",
    }
    members = {m.name for m in CommandType}
    missing = required - members
    assert not missing, f"CommandType missing entries: {sorted(missing)}"
