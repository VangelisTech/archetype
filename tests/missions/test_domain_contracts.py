# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the final Agent Missions family layout."""

from __future__ import annotations

from pathlib import Path

import pytest

import archetype
import archetype.app.missions as app_missions
import archetype.missions as missions
import archetype.missions.components as components
from archetype.core.component import Component
from archetype.missions import (
    MISSION_COMPONENTS,
    MISSION_TRANSITIONS,
    TASK_TRANSITIONS,
    MissionStatus,
    TaskStatus,
    TaskValidator,
    require_mission_transition,
    require_task_transition,
)

pytestmark = pytest.mark.contract("missions.agent_v1.validator_gated")


def _component_instance(component: type[Component]) -> Component:
    if component is TaskValidator:
        return TaskValidator(name="focused", command=["pytest", "-q"])
    return component()


def _component_matches(name: str) -> list[type[Component]]:
    matches: list[type[Component]] = []
    stack = [Component]
    seen: set[type[Component]] = set()
    while stack:
        current = stack.pop()
        for subclass in current.__subclasses__():
            if subclass in seen:
                continue
            seen.add(subclass)
            stack.append(subclass)
            if subclass.__name__ == name:
                matches.append(subclass)
    return matches


def test_components_have_one_family_owned_schema_identity() -> None:
    expected = (
        "Mission",
        "MissionState",
        "Task",
        "TaskWorkspace",
        "TaskPolicy",
        "TaskState",
        "TaskDispatch",
        "TaskValidator",
        "Sandbox",
        "AgentExecution",
        "ValidationResult",
        "Commit",
        "Checkpoint",
        "FilesystemManifest",
        "FrictionLog",
        "AgentArtifact",
    )
    assert tuple(component.__name__ for component in MISSION_COMPONENTS) == expected

    for component in MISSION_COMPONENTS:
        instance = _component_instance(component)
        restored = Component.from_dict(instance.to_payload())
        assert type(restored) is component
        assert restored.model_dump() == instance.model_dump()
        assert component.__module__ == "archetype.missions.components"
        assert getattr(missions, component.__name__) is component
        assert getattr(components, component.__name__) is component
        assert Component.get_type_by_name(component.__name__) is component
        assert _component_matches(component.__name__) == [component]
        assert component.__name__ not in app_missions.__all__
        assert component.__name__ not in archetype.__all__


def test_transition_tables_are_small_complete_and_terminal() -> None:
    assert TASK_TRANSITIONS == {
        TaskStatus.PENDING: frozenset({TaskStatus.READY}),
        TaskStatus.READY: frozenset({TaskStatus.DISPATCHED}),
        TaskStatus.DISPATCHED: frozenset(
            {TaskStatus.READY, TaskStatus.ACCEPTED, TaskStatus.FAILED}
        ),
        TaskStatus.ACCEPTED: frozenset(),
        TaskStatus.FAILED: frozenset(),
    }
    assert MISSION_TRANSITIONS == {
        MissionStatus.RUNNING: frozenset({MissionStatus.SUCCEEDED, MissionStatus.FAILED}),
        MissionStatus.SUCCEEDED: frozenset(),
        MissionStatus.FAILED: frozenset(),
    }
    for source, targets in TASK_TRANSITIONS.items():
        for target in targets:
            require_task_transition(source, target)
    for source, targets in MISSION_TRANSITIONS.items():
        for target in targets:
            require_mission_transition(source, target)
    with pytest.raises(ValueError, match="illegal task transition"):
        require_task_transition(TaskStatus.ACCEPTED, TaskStatus.READY)
    with pytest.raises(ValueError, match="illegal mission transition"):
        require_mission_transition(MissionStatus.SUCCEEDED, MissionStatus.RUNNING)


def test_legacy_kernel_is_not_a_compatibility_api() -> None:
    for name in (
        "Attempt",
        "AttemptStatus",
        "Evidence",
        "Finalization",
        "TaskGate",
        "MissionAttemptRequest",
        "FencedExecutionAuthorization",
    ):
        assert name not in missions.__all__
        assert not hasattr(missions, name)

    root = Path(__file__).resolve().parents[2] / "src" / "archetype" / "app"
    assert not any((root / "sandboxes").glob("*.py"))
    assert not any((root / "sandboxes").glob("*.toml"))
    for legacy in (
        "agent_service.py",
        "claim_service.py",
        "execution_service.py",
        "models.py",
        "outcomes.py",
        "transitions.py",
    ):
        assert not (root / "missions" / legacy).exists()
