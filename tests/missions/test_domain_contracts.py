# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Mechanical extraction contracts for the reusable mission domain (#559)."""

from __future__ import annotations

import hashlib
import json
import tomllib
from pathlib import Path

import pytest

import archetype
import archetype.app.application.mission_artifacts as mission_artifacts
import archetype.app.missions as app_missions
import archetype.app.missions.claim_service as claim_service
import archetype.app.missions.models as app_models
import archetype.app.missions.outcomes as outcomes
import archetype.app.missions.service as mission_service
import archetype.missions as missions
import archetype.missions.components as components
import archetype.missions.transitions as transitions
from archetype.app.missions.models import AttemptClaim, FencedExecutionAuthorization
from archetype.app.missions.transitions import AttemptRecoveryAction
from archetype.core.component import Component
from archetype.missions import (
    MISSION_COMPONENTS,
    MISSION_TRANSITION_GRAPH,
    AttemptStatus,
    MissionStatus,
    MissionTaskState,
    MissionTransitionEvent,
    MissionTransitionGraph,
    TaskStatus,
)

pytestmark = pytest.mark.contract("missions.transition.evidence_gated")

_COMPONENT_SCHEMA_FINGERPRINTS = {
    # (canonical Pydantic JSON schema, canonical Arrow schema). These were
    # captured from the app-owned definitions immediately before extraction.
    "Mission": (
        "3c182a1b8f981e50647e09d91dbaed0bad0b4e90515afd6ba5e70bc4928cf3fe",
        "10b4e6ef8a801ead2e4ff737074b9759ef0b9894f15694dc9adc4aa4ee36d652",
    ),
    "TaskGate": (
        "30662f37710bdb5a76c30ec2213c22c2075a653b952989afd2dcdd047c7cce45",
        "58f6509b55828022991e9af73264cb754f3dd49c81c4f11405c195864dfa8671",
    ),
    "Attempt": (
        "f2805ccfc195e6ae692ed3a6b805298287cec33d055509ae07fde818e7fc8b74",
        "5f7cda42242e2c60ecc8dfe451b97a2c449c07d160e5f8d9367c2ac0c692c28d",
    ),
    "Checkpoint": (
        "7afb98f3b4913ffab6c995c18e618f5576b73af95fdc77f36e82f3960ae9e108",
        "4e4402deb2977c347db36ac8021e9a4f50ca6988a4379d3ba7cb4f61d410d413",
    ),
    "Finalization": (
        "b454db14a7ade396cd2e58ed10f1c7fbcf2b12b0570528df3dfda193323745de",
        "79b336b9b786f771ba253c7a7df3015ceedcedd156523864571dc563ea7498a8",
    ),
    "Commit": (
        "15ad4ea9f2c58c573a35dd442e6635cfe29054337a46e4217355f235586ce027",
        "256c0114555bcca6946672dc6f13d53e40a269725973be48bfe0d34af8913db9",
    ),
    "Evidence": (
        "fb83ecc02c9eac7b6a28a5ca154d67ebc162c35f98af6e43aa60a5255f6b514b",
        "0f77b643f5e3f34b9998ce03ee0fe605d823a36c5941fe0e80d231ae6830d4c3",
    ),
    "FrictionLog": (
        "5f4e6c3260ff26412db2c2b3161e705e0e8e9f36293b4790c27132c41d8395d6",
        "7efba6f4c5937dcb6a816a86d3962900b772add1d3f80c2bd78ac148a859221d",
    ),
}

_ACTIVE_SOURCES = {
    MissionTaskState(MissionStatus.READY, TaskStatus.READY),
    MissionTaskState(MissionStatus.RUNNING, TaskStatus.READY),
    MissionTaskState(MissionStatus.RUNNING, TaskStatus.RETRYABLE),
}

_EXPECTED_EVENT_TARGETS = {
    MissionTransitionEvent.REJECTED_RETRY: (
        AttemptStatus.REJECTED,
        MissionTaskState(MissionStatus.RUNNING, TaskStatus.RETRYABLE),
    ),
    MissionTransitionEvent.INCOMPLETE_RETRY: (
        AttemptStatus.INCOMPLETE,
        MissionTaskState(MissionStatus.RUNNING, TaskStatus.RETRYABLE),
    ),
    MissionTransitionEvent.FAILED_RETRY: (
        AttemptStatus.FAILED,
        MissionTaskState(MissionStatus.RUNNING, TaskStatus.RETRYABLE),
    ),
    MissionTransitionEvent.REJECTED_EXHAUSTED: (
        AttemptStatus.REJECTED,
        MissionTaskState(MissionStatus.FAILED, TaskStatus.EXHAUSTED),
    ),
    MissionTransitionEvent.INCOMPLETE_EXHAUSTED: (
        AttemptStatus.INCOMPLETE,
        MissionTaskState(MissionStatus.FAILED, TaskStatus.EXHAUSTED),
    ),
    MissionTransitionEvent.FAILED_EXHAUSTED: (
        AttemptStatus.FAILED,
        MissionTaskState(MissionStatus.FAILED, TaskStatus.EXHAUSTED),
    ),
    MissionTransitionEvent.TASK_ADVANCED: (
        AttemptStatus.ACCEPTED,
        MissionTaskState(MissionStatus.RUNNING, TaskStatus.READY),
    ),
    MissionTransitionEvent.MISSION_SUCCEEDED: (
        AttemptStatus.ACCEPTED,
        MissionTaskState(MissionStatus.SUCCEEDED, TaskStatus.PASSED),
    ),
}


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


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


def test_component_schemas_defaults_and_serialization_are_unchanged() -> None:
    assert tuple(component.__name__ for component in MISSION_COMPONENTS) == tuple(
        _COMPONENT_SCHEMA_FINGERPRINTS
    )

    for component in MISSION_COMPONENTS:
        json_schema = json.dumps(
            component.model_json_schema(),
            sort_keys=True,
            separators=(",", ":"),
        )
        expected_json, expected_arrow = _COMPONENT_SCHEMA_FINGERPRINTS[component.__name__]
        assert _sha256(json_schema) == expected_json
        assert _sha256(str(component.to_pyarrow_schema())) == expected_arrow

        payload = component().to_payload()
        assert payload["type"] == component.__name__
        restored = Component.from_dict(payload)
        assert type(restored) is component
        assert restored.model_dump() == component().model_dump()


def test_family_exports_resolve_to_one_component_identity_without_legacy_copies() -> None:
    assert missions.MISSION_COMPONENTS is components.MISSION_COMPONENTS
    for component in MISSION_COMPONENTS:
        name = component.__name__
        assert component.__module__ == "archetype.missions.components"
        assert getattr(missions, name) is component
        assert getattr(components, name) is component
        assert Component.get_type_by_name(name) is component
        assert _component_matches(name) == [component]
        assert name not in app_missions.__all__
        assert not hasattr(app_models, name)
        assert name not in archetype.__all__


def test_transition_graph_preserves_the_full_legal_and_illegal_matrix() -> None:
    expected_keys = {
        (source, event) for source in _ACTIVE_SOURCES for event in MissionTransitionEvent
    }
    assert set(MISSION_TRANSITION_GRAPH) == expected_keys

    for mission in MissionStatus:
        for task in TaskStatus:
            source = MissionTaskState(mission, task)
            assert MissionTransitionGraph.state(mission.value, task.value) == source
            for event, (attempt, target) in _EXPECTED_EVENT_TARGETS.items():
                if source in _ACTIVE_SOURCES:
                    edge = MissionTransitionGraph.transition(source, event)
                    assert edge is MISSION_TRANSITION_GRAPH[(source, event)]
                    assert (edge.attempt, edge.target) == (attempt, target)
                else:
                    assert (source, event) not in MISSION_TRANSITION_GRAPH
                    with pytest.raises(ValueError, match="illegal mission transition"):
                        MissionTransitionGraph.transition(source, event)


def test_app_authority_stays_internal_while_consuming_the_top_level_domain() -> None:
    assert AttemptClaim.__module__ == "archetype.app.missions.models"
    assert FencedExecutionAuthorization.__module__ == "archetype.app.missions.models"
    assert AttemptRecoveryAction.__module__ == "archetype.app.missions.transitions"
    for name in ("AttemptClaim", "FencedExecutionAuthorization", "AttemptRecoveryAction"):
        assert name not in missions.__all__

    assert mission_service.MissionTransitionGraph is transitions.MissionTransitionGraph
    assert mission_service.MissionStatus is transitions.MissionStatus
    assert claim_service.MissionTaskState is transitions.MissionTaskState
    assert outcomes.CheckpointStatus is transitions.CheckpointStatus
    assert mission_artifacts.FinalizationPhase is transitions.FinalizationPhase


def test_architecture_manifest_registers_a_leaf_family_without_legacy_exceptions() -> None:
    root = Path(__file__).resolve().parents[2]
    manifest = tomllib.loads((root / "quality" / "architecture.toml").read_text(encoding="utf-8"))
    rules = {
        rule["consumer"]: rule["allowed_families"] for rule in manifest["top_level_family_rule"]
    }
    assert rules["archetype.missions"] == []
    assert all(exception.get("issue") != 559 for exception in manifest["exception"])
