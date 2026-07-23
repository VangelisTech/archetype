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
    Candidate,
    CriticFinding,
    CriticPolicy,
    CriticReceipt,
    MissionStatus,
    TaskCriticPolicy,
    TaskStatus,
    TaskValidator,
    require_mission_transition,
    require_task_transition,
)

pytestmark = pytest.mark.contract("missions.agent_v1.validator_gated")


def _component_instance(component: type[Component]) -> Component:
    if component is TaskValidator:
        return TaskValidator(name="focused", command=["pytest", "-q"])
    if component is TaskCriticPolicy:
        policy = CriticPolicy()
        return TaskCriticPolicy(
            policy_id=policy.policy_id,
            version=policy.version,
            digest=policy.digest,
            perspective=policy.perspective,
            information_view=policy.information_view,
            driver=policy.driver,
            sampling=policy.sampling,
        )
    if component is Candidate:
        return Candidate(
            candidate_id="candidate",
            dispatch_id="dispatch",
            dispatch_sequence=1,
            author_sandbox_id="author",
            repository="owner/repository",
            branch="agent/change",
            base_ref="main",
            base_revision="base",
            head_revision="head",
            diff_digest="diff",
            validator_bundle_digest="validators",
            policy_digest="policy",
            candidate_digest="candidate-subject",
        )
    if component is CriticFinding:
        return CriticFinding(
            finding_id="finding",
            severity="blocking",
            category="correctness",
            confidence=1.0,
            title="Defect",
            detail="Evidence",
        )
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
        "TaskCriticPolicy",
        "TaskState",
        "TaskDispatch",
        "TaskValidator",
        "Sandbox",
        "AgentExecution",
        "ValidationResult",
        "Commit",
        "Candidate",
        "CriticExecution",
        "CriticFinding",
        "CriticReceipt",
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


@pytest.mark.parametrize(
    "overrides",
    (
        {"information_view": "trajectory-and-diff"},
        {"sampling": "temperature=0.7"},
    ),
)
def test_critic_policy_rejects_unimplemented_behavior_axes(
    overrides: dict[str, str],
) -> None:
    with pytest.raises(ValueError, match="unsupported critic"):
        CriticPolicy(**overrides)


def test_critic_receipt_schema_contains_only_varying_evidence() -> None:
    assert "complete" not in CriticReceipt.model_fields
    assert "verifiable" not in CriticReceipt.model_fields


def test_transition_tables_are_small_complete_and_terminal() -> None:
    assert TASK_TRANSITIONS == {
        TaskStatus.PENDING: frozenset({TaskStatus.READY}),
        TaskStatus.READY: frozenset({TaskStatus.DISPATCHED}),
        TaskStatus.DISPATCHED: frozenset(
            {TaskStatus.READY, TaskStatus.CANDIDATE, TaskStatus.FAILED}
        ),
        TaskStatus.CANDIDATE: frozenset({TaskStatus.READY, TaskStatus.ACCEPTED, TaskStatus.FAILED}),
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
