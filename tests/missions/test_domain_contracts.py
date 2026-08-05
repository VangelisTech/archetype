# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the final Agent Missions family layout."""

from __future__ import annotations

from pathlib import Path

import pytest

import archetype
import archetype.missions as missions
import archetype.missions.components as components
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.missions import (
    MISSION_COMPONENTS,
    MISSION_TRANSITIONS,
    TASK_TRANSITIONS,
    AuthorActivityObservation,
    Candidate,
    CompleteAuthorActivityObservation,
    CompleteCriticActivityObservation,
    CriticFinding,
    CriticPolicy,
    CriticReceipt,
    MissionStatus,
    TaskCriticPolicy,
    TaskCriticSubjectPolicy,
    TaskStatus,
    TaskValidator,
    require_mission_transition,
    require_task_transition,
)
from archetype.storage.catalog import schema_fingerprint

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
    if component is AuthorActivityObservation:
        return AuthorActivityObservation(
            activity_id="dispatch",
            task_id=1,
            dispatch_sequence=1,
            result_ref="artifact://author-result",
            result_digest="digest",
            fact_bundle_digest="fact-bundle-digest",
            execution_id=1,
            redaction_policy_id="redaction-policy",
        )
    if component is CompleteAuthorActivityObservation:
        return CompleteAuthorActivityObservation(
            activity_id="dispatch",
            task_id=1,
            dispatch_sequence=1,
            result_ref="artifact://author-result",
            result_digest="digest",
            fact_bundle_digest="fact-bundle-digest",
            execution_id=1,
            sandbox_entity_id=2,
            relation_count=2,
            redaction_policy_id="redaction-policy",
        )
    if component is CompleteCriticActivityObservation:
        return CompleteCriticActivityObservation(
            activity_id="review",
            candidate_entity_id=1,
            domain_review_attempt=1,
            result_ref="mission-critic+json:sha256:result",
            result_digest="result",
            fact_bundle_digest="fact-bundle",
            execution_id=2,
            sandbox_entity_id=3,
            relation_count=2,
            author_sandbox_id="author",
            critic_sandbox_id="critic",
            redaction_policy_id="redaction-policy",
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
        "TaskCriticSubjectPolicy",
        "TaskState",
        "TaskDispatch",
        "TaskValidator",
        "Sandbox",
        "AgentExecution",
        "AuthorActivityObservation",
        "CompleteAuthorActivityObservation",
        "ValidationResult",
        "Commit",
        "Candidate",
        "CriticExecution",
        "CriticFinding",
        "CriticReceipt",
        "CompleteCriticActivityObservation",
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
        assert component.__name__ not in archetype.__all__


def test_v1_author_activity_observation_keeps_its_durable_schema_identity() -> None:
    assert tuple(AuthorActivityObservation.model_fields) == (
        "activity_id",
        "task_id",
        "dispatch_sequence",
        "result_ref",
        "result_digest",
        "fact_bundle_digest",
        "execution_id",
        "validation_count",
        "commit_count",
        "friction_count",
        "redaction_policy_id",
    )
    signature = (AuthorActivityObservation,)
    schema = Archetype.get_archetype_schema(signature)
    assert Archetype.get_name(signature) == "a_1c_sd88ad8b876ee47bc"
    assert (
        schema_fingerprint(schema)
        == "98d8764af332ccf6287a6fee2dee83704ae2bf0b5e10acc81b380e40be344bb7"
    )


def test_v1_critic_policy_keeps_its_durable_schema_identity() -> None:
    assert tuple(TaskCriticPolicy.model_fields) == (
        "policy_id",
        "version",
        "digest",
        "perspective",
        "information_view",
        "driver",
        "model",
        "sampling",
        "max_reviews",
        "timeout_seconds",
        "output_schema_version",
        "max_output_chars",
    )
    signature = (TaskCriticPolicy,)
    schema = Archetype.get_archetype_schema(signature)
    assert Archetype.get_name(signature) == "a_1c_sf8608970ea3994f5"
    assert (
        schema_fingerprint(schema)
        == "57b05a4c0718da90412693b74f60e9f549f42b8494d9571ea1492d40d0079d07"
    )


def test_critic_subject_budget_uses_a_companion_component() -> None:
    assert tuple(TaskCriticSubjectPolicy.model_fields) == ("max_subject_bytes",)
    assert TaskCriticSubjectPolicy().max_subject_bytes == CriticPolicy().max_subject_bytes


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


def test_critic_subject_budget_is_positive_and_identity_bound() -> None:
    baseline = CriticPolicy()
    bounded = CriticPolicy(max_subject_bytes=baseline.max_subject_bytes + 1)

    assert baseline.max_subject_bytes > 0
    assert bounded.digest != baseline.digest
    with pytest.raises(ValueError, match="max_subject_bytes"):
        CriticPolicy(max_subject_bytes=0)


def test_v1_critic_receipt_keeps_its_durable_schema_identity() -> None:
    assert tuple(CriticReceipt.model_fields) == (
        "candidate_entity_id",
        "critic_execution_id",
        "critic_sandbox_id",
        "review_id",
        "conclusion",
        "candidate_digest",
        "policy_digest",
        "evidence_digest",
        "reviewed_base_revision",
        "reviewed_head_revision",
        "reviewed_diff_digest",
        "validator_bundle_digest",
        "reviewed_scope",
        "finding_count",
        "blocking_count",
        "output_schema_version",
        "completed_at_ms",
    )
    signature = (CriticReceipt,)
    schema = Archetype.get_archetype_schema(signature)
    assert Archetype.get_name(signature) == "a_1c_sa0c654749eef2548"
    assert (
        schema_fingerprint(schema)
        == "7b3ebbb6636a667ca0e6d972f369511fa9c906e841e0290febcab9858f927c78"
    )


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
