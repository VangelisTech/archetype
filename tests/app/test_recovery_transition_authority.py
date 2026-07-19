# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract oracles for typed fleet-recovery state and capability lanes."""

import hashlib
import re
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path

import pytest
from pydantic import ValidationError

from archetype.app.recovery import (
    MAINTENANCE_RECOVERY_KINDS,
    RecoveryItemDisposition,
    RecoveryItemResult,
    RecoveryKind,
    RecoveryLimits,
    RecoveryPage,
    RecoveryPolicy,
    RecoverySubject,
    RecoverySweep,
    RecoverySweepStatus,
    iMaintenanceRecoveryHandler,
    iModelRecoveryHandler,
    recovery_backoff_ms,
    recovery_subject_key,
)
from archetype.app.recovery.models import RecoveryExceptionStatus
from archetype.app.recovery.transitions import (
    RECOVERY_EXCEPTION_TRANSITION_GRAPH,
    RECOVERY_SWEEP_TRANSITION_GRAPH,
    RecoveryExceptionEvent,
    RecoveryExceptionTransitionGraph,
    RecoverySweepEvent,
    RecoverySweepTransitionGraph,
)
from archetype.app.storage import recovery_transitions as storage_transitions

pytestmark = pytest.mark.contract("recovery.control.fenced")

_WORKER_SOURCE = (
    Path(__file__).resolve().parents[2] / "infra" / "control-catalog" / "src" / "index.ts"
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def test_maintenance_lane_structurally_excludes_model_recovery() -> None:
    assert RecoveryKind.MISSION_MODEL_RECOVERY not in MAINTENANCE_RECOVERY_KINDS
    assert MAINTENANCE_RECOVERY_KINDS == {
        RecoveryKind.MISSION_FINALIZATION,
        RecoveryKind.ARTIFACT_PUBLICATION,
        RecoveryKind.EVENT_PROJECTION,
        RecoveryKind.ARTIFACT_RETENTION,
        RecoveryKind.CHECKPOINT_RETENTION,
        RecoveryKind.LOCAL_STAGING_RETENTION,
    }
    # This equality deliberately fails when a future enum member is added
    # without an explicit capability-policy decision above.
    assert MAINTENANCE_RECOVERY_KINDS == set(RecoveryKind) - {RecoveryKind.MISSION_MODEL_RECOVERY}

    class _Maintenance:
        kind = RecoveryKind.ARTIFACT_PUBLICATION

        async def recover(self, subject: RecoverySubject) -> RecoveryItemResult:
            return RecoveryItemResult(
                subject_key=subject.subject_key,
                disposition=RecoveryItemDisposition.COMPLETED,
            )

    class _Model:
        kind = RecoveryKind.MISSION_MODEL_RECOVERY

        async def recover_model(self, subject: RecoverySubject) -> RecoveryItemResult:
            return RecoveryItemResult(
                subject_key=subject.subject_key,
                disposition=RecoveryItemDisposition.COMPLETED,
            )

    assert isinstance(_Maintenance(), iMaintenanceRecoveryHandler)
    assert not isinstance(_Maintenance(), iModelRecoveryHandler)
    assert isinstance(_Model(), iModelRecoveryHandler)
    assert not isinstance(_Model(), iMaintenanceRecoveryHandler)


def test_sweep_graph_is_exhaustive_and_fail_closed() -> None:
    for status in (None, *RecoverySweepStatus):
        for event in RecoverySweepEvent:
            edge = (status, event)
            if edge in RECOVERY_SWEEP_TRANSITION_GRAPH:
                assert (
                    RecoverySweepTransitionGraph.transition(*edge)
                    is (RECOVERY_SWEEP_TRANSITION_GRAPH[edge])
                )
            else:
                with pytest.raises(ValueError, match="illegal recovery sweep transition"):
                    RecoverySweepTransitionGraph.transition(*edge)

    with pytest.raises(ValueError, match="unknown recovery sweep state"):
        RecoverySweepTransitionGraph.state("invented")
    with pytest.raises(ValueError, match="unknown recovery sweep event"):
        RecoverySweepTransitionGraph.transition(RecoverySweepStatus.IDLE, "invented")


def test_exception_graph_is_exhaustive_and_fail_closed() -> None:
    for status in (None, *RecoveryExceptionStatus):
        for event in RecoveryExceptionEvent:
            edge = (status, event)
            if edge in RECOVERY_EXCEPTION_TRANSITION_GRAPH:
                assert (
                    RecoveryExceptionTransitionGraph.transition(*edge)
                    is (RECOVERY_EXCEPTION_TRANSITION_GRAPH[edge])
                )
            else:
                with pytest.raises(ValueError, match="illegal recovery exception transition"):
                    RecoveryExceptionTransitionGraph.transition(*edge)

    with pytest.raises(ValueError, match="unknown recovery exception state"):
        RecoveryExceptionTransitionGraph.state("invented")


def test_recovery_family_reexports_the_storage_owned_authority() -> None:
    assert RecoverySweepTransitionGraph is storage_transitions.RecoverySweepTransitionGraph
    assert RecoveryExceptionTransitionGraph is storage_transitions.RecoveryExceptionTransitionGraph
    assert RECOVERY_SWEEP_TRANSITION_GRAPH is storage_transitions.RECOVERY_SWEEP_TRANSITION_GRAPH
    assert (
        RECOVERY_EXCEPTION_TRANSITION_GRAPH
        is storage_transitions.RECOVERY_EXCEPTION_TRANSITION_GRAPH
    )
    assert RecoverySweepTransitionGraph.transition(None, RecoverySweepEvent.CREATE).value == "idle"
    assert (
        RecoveryExceptionTransitionGraph.transition(None, RecoveryExceptionEvent.RETRY).value
        == "retry_wait"
    )
    assert (
        RecoveryExceptionTransitionGraph.transition(None, RecoveryExceptionEvent.DEAD_LETTER).value
        == "dead_letter"
    )


def _worker_transition_graph(source: str, name: str) -> dict[str, str]:
    start = source.index(f"const {name}:")
    start = source.index("new Map([", start)
    end = source.index("]);", start)
    return dict(
        re.findall(
            r'\[\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\]',
            source[start:end],
        )
    )


def _python_transition_graph(
    graph: Mapping[tuple[StrEnum | None, StrEnum], StrEnum],
) -> dict[str, str]:
    return {
        f"{'<absent>' if source is None else source.value}|{event.value}": target.value
        for (source, event), target in graph.items()
    }


def test_worker_transition_graph_cannot_drift_from_storage_authority() -> None:
    source = _WORKER_SOURCE.read_text()
    assert _worker_transition_graph(source, "RECOVERY_SWEEP_TRANSITIONS") == (
        _python_transition_graph(RECOVERY_SWEEP_TRANSITION_GRAPH)
    )
    assert _worker_transition_graph(source, "RECOVERY_EXCEPTION_TRANSITIONS") == (
        _python_transition_graph(RECOVERY_EXCEPTION_TRANSITION_GRAPH)
    )

    # Each durable mutation path must invoke the mirrored oracle, including
    # creation and status-preserving renew/checkpoint writes.
    assert source.count("recoverySweepTransition(") == 9  # definition + eight mutation paths
    assert source.count("recoveryExceptionTransition(") == 5  # definition + four mutation paths


def test_recovery_models_reject_unsafe_keys_cursors_and_boolean_times() -> None:
    authority_key = _digest("authority")
    subject = RecoverySubject(
        world_id="world-1",
        kind=RecoveryKind.ARTIFACT_PUBLICATION,
        subject_key=recovery_subject_key(
            RecoveryKind.ARTIFACT_PUBLICATION,
            "world-1",
            authority_key,
        ),
        authority_key=authority_key,
        cursor_after=authority_key,
    )
    assert RecoveryPage(subjects=(subject,), exhausted=True).subjects == (subject,)

    with pytest.raises(ValidationError, match="lowercase SHA-256"):
        RecoverySubject(
            world_id="world-1",
            kind=RecoveryKind.ARTIFACT_PUBLICATION,
            subject_key="../../unsafe",
            authority_key=_digest("authority"),
        )
    with pytest.raises(ValidationError, match="final subject cursor"):
        RecoveryPage(subjects=(subject,), exhausted=False)
    with pytest.raises(ValidationError, match="requires cursor_after"):
        RecoveryPage(
            subjects=(subject.model_copy(update={"cursor_after": ""}),),
            exhausted=True,
        )
    with pytest.raises(ValidationError, match="increase strictly"):
        RecoveryPage(
            subjects=(
                RecoverySubject(
                    world_id="world-1",
                    kind=RecoveryKind.ARTIFACT_PUBLICATION,
                    subject_key=recovery_subject_key(
                        RecoveryKind.ARTIFACT_PUBLICATION,
                        "world-1",
                        "f" * 64,
                    ),
                    authority_key="f" * 64,
                    cursor_after="f" * 64,
                ),
                RecoverySubject(
                    world_id="world-1",
                    kind=RecoveryKind.ARTIFACT_PUBLICATION,
                    subject_key=recovery_subject_key(
                        RecoveryKind.ARTIFACT_PUBLICATION,
                        "world-1",
                        "e" * 64,
                    ),
                    authority_key="e" * 64,
                    cursor_after="e" * 64,
                ),
            ),
            exhausted=True,
        )
    with pytest.raises(ValidationError, match="final subject cursor"):
        RecoveryPage(
            subjects=(subject,),
            next_cursor=_digest("different-cursor"),
            exhausted=False,
        )
    with pytest.raises(ValidationError):
        RecoveryLimits(max_elapsed_ms=True)
    with pytest.raises(ValidationError, match="cannot exceed 24 hours"):
        RecoveryLimits(max_elapsed_ms=86_400_001)
    with pytest.raises(ValidationError):
        RecoveryPage(exhausted=1)
    with pytest.raises(ValidationError, match="cannot exceed 1000000"):
        RecoveryPolicy(maximum_exception_attempts=1_000_001)
    with pytest.raises(ValidationError):
        RecoverySweep(
            sweep_key=_digest("sweep"),
            storage_fingerprint=_digest("storage"),
            world_id="world-1",
            kind=RecoveryKind.ARTIFACT_PUBLICATION,
            status=RecoverySweepStatus.IDLE,
            lease_expires_at_ms=True,
            maximum_consecutive_failures=1,
        )
    with pytest.raises(ValidationError, match="less than or equal"):
        RecoverySweep(
            sweep_key=_digest("sweep"),
            storage_fingerprint=_digest("storage"),
            world_id="world-1",
            kind=RecoveryKind.ARTIFACT_PUBLICATION,
            status=RecoverySweepStatus.IDLE,
            fence_epoch=1 << 53,
            maximum_consecutive_failures=1,
        )
    with pytest.raises(ValidationError, match="SHA-256"):
        RecoverySweep(
            sweep_key=_digest("sweep"),
            storage_fingerprint=_digest("storage"),
            world_id="world-1",
            kind=RecoveryKind.ARTIFACT_PUBLICATION,
            status=RecoverySweepStatus.IDLE,
            cursor="page:7",
            maximum_consecutive_failures=1,
        )
    with pytest.raises(ValidationError):
        RecoverySweep(
            sweep_key=_digest("sweep"),
            storage_fingerprint=_digest("storage"),
            world_id="world-1",
            kind=RecoveryKind.ARTIFACT_PUBLICATION,
            status=RecoverySweepStatus.IDLE,
            last_error_code="credential-bearing-free-form-code",
            maximum_consecutive_failures=1,
        )


def test_recovery_backoff_is_deterministic_integer_bounded_and_keyed() -> None:
    policy = RecoveryPolicy(
        initial_retry_delay_ms=100,
        maximum_retry_delay_ms=1_000,
        jitter_basis_points=1_000,
    )
    key = _digest("item")
    values = [
        recovery_backoff_ms(
            key,
            attempt,
            initial_delay_ms=policy.initial_retry_delay_ms,
            maximum_delay_ms=policy.maximum_retry_delay_ms,
            jitter_basis_points=policy.jitter_basis_points,
        )
        for attempt in range(1, 20)
    ]
    assert values == [
        recovery_backoff_ms(
            key,
            attempt,
            initial_delay_ms=policy.initial_retry_delay_ms,
            maximum_delay_ms=policy.maximum_retry_delay_ms,
            jitter_basis_points=policy.jitter_basis_points,
        )
        for attempt in range(1, 20)
    ]
    assert all(type(value) is int and 0 <= value <= 1_000 for value in values)
    assert values != [
        recovery_backoff_ms(
            _digest("other-item"),
            attempt,
            initial_delay_ms=policy.initial_retry_delay_ms,
            maximum_delay_ms=policy.maximum_retry_delay_ms,
            jitter_basis_points=policy.jitter_basis_points,
        )
        for attempt in range(1, 20)
    ]
    with pytest.raises(TypeError, match="exact integers"):
        recovery_backoff_ms(
            key,
            True,
            initial_delay_ms=100,
            maximum_delay_ms=1_000,
            jitter_basis_points=0,
        )
