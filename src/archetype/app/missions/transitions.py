# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed mission, task, and attempt transition authority.

Arrow persists component states as strings. This module is the single parser
and graph for those strings; services never compare or invent state literals.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType


class MissionStatus(StrEnum):
    """Episode-level mission states."""

    READY = "ready"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class TaskStatus(StrEnum):
    """State of the task currently selected by ``step_index``."""

    READY = "ready"
    RETRYABLE = "retryable"
    PASSED = "passed"
    EXHAUSTED = "exhausted"


class AttemptStatus(StrEnum):
    """Authoritative result of one completed submission attempt."""

    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    INCOMPLETE = "incomplete"
    FAILED = "failed"


class AttemptClaimStatus(StrEnum):
    """Durable provider-submission state outside the world tick."""

    CLAIMED = "claimed"
    POSSIBLY_SUBMITTED = "possibly_submitted"
    PROVIDER_ACKNOWLEDGED = "provider_acknowledged"
    FINALIZING = "finalizing"
    SETTLED = "settled"


class AttemptClaimEvent(StrEnum):
    """Every legal durable claim transition."""

    ARM_SUBMISSION = "arm_submission"
    ACKNOWLEDGE_PROVIDER = "acknowledge_provider"
    STAGE_FINALIZATION = "stage_finalization"
    SETTLE_WITHOUT_SUBMISSION = "settle_without_submission"
    SETTLE_AFTER_RECONCILIATION = "settle_after_reconciliation"
    SETTLE_ACKNOWLEDGED = "settle_acknowledged"
    SETTLE_FINALIZED = "settle_finalized"


class AttemptClaimAcquireOutcome(StrEnum):
    """Result of acquiring or replaying one durable claim lease."""

    ACQUIRED = "acquired"
    OWNED = "owned"
    RECOVERED = "recovered"
    DUPLICATE = "duplicate"


class AttemptRecoveryAction(StrEnum):
    """Only actions a claim state and provider contract may authorize."""

    EXECUTE = "execute"
    RECONCILE = "reconcile"
    FINALIZE = "finalize"
    REPLAY_IDEMPOTENT = "replay_idempotent"
    RESUME_SESSION = "resume_session"
    SETTLED = "settled"


class CheckpointStatus(StrEnum):
    """Provider-checkpoint outcomes recorded with an attempt."""

    PENDING = "pending"
    CREATED = "created"
    FAILED = "failed"
    DISABLED = "disabled"


class FinalizationPhase(StrEnum):
    """Ordered evidence-finalization phases."""

    PENDING = "pending"
    CAPTURED = "captured"
    CHECKPOINTED = "checkpointed"
    UPLOADED = "uploaded"
    INDEXED = "indexed"
    # Compatibility value emitted before portable artifact publication had its
    # own PENDING -> UPLOADED -> INDEXED state machine. It remains parseable,
    # but indexed policy treats it explicitly rather than by rank.
    PUBLISHED = "published"

    @property
    def rank(self) -> int:
        return _FINALIZATION_RANK[self]


class MissionTransitionEvent(StrEnum):
    """Every legal graph edge produced by one attempt."""

    REJECTED_RETRY = "rejected_retry"
    INCOMPLETE_RETRY = "incomplete_retry"
    FAILED_RETRY = "failed_retry"
    REJECTED_EXHAUSTED = "rejected_exhausted"
    INCOMPLETE_EXHAUSTED = "incomplete_exhausted"
    FAILED_EXHAUSTED = "failed_exhausted"
    TASK_ADVANCED = "task_advanced"
    MISSION_SUCCEEDED = "mission_succeeded"


@dataclass(frozen=True)
class MissionTaskState:
    """Typed aggregate state persisted across mission and task components."""

    mission: MissionStatus
    task: TaskStatus


@dataclass(frozen=True)
class MissionTransition:
    """One validated edge that will be persisted with the attempt."""

    source: MissionTaskState
    event: MissionTransitionEvent
    attempt: AttemptStatus
    target: MissionTaskState

    @property
    def advances_task(self) -> bool:
        return self.event is MissionTransitionEvent.TASK_ADVANCED

    @property
    def terminal(self) -> bool:
        return self.target.mission in {MissionStatus.SUCCEEDED, MissionStatus.FAILED}


@dataclass(frozen=True)
class AttemptClaimTransition:
    """One validated provider-submission control-plane edge."""

    source: AttemptClaimStatus
    event: AttemptClaimEvent
    target: AttemptClaimStatus


_FINALIZATION_RANK = MappingProxyType(
    {
        FinalizationPhase.PENDING: 0,
        FinalizationPhase.CAPTURED: 1,
        FinalizationPhase.CHECKPOINTED: 2,
        FinalizationPhase.UPLOADED: 3,
        FinalizationPhase.INDEXED: 4,
        FinalizationPhase.PUBLISHED: 5,
    }
)

_EVENT_TARGETS = MappingProxyType(
    {
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
)

_ACTIVE_SOURCES = (
    MissionTaskState(MissionStatus.READY, TaskStatus.READY),
    MissionTaskState(MissionStatus.RUNNING, TaskStatus.READY),
    MissionTaskState(MissionStatus.RUNNING, TaskStatus.RETRYABLE),
)

# One immutable graph owns mission, task, and terminal attempt state together.
# Keeping the complete edge map public makes exhaustive contract tests possible.
MISSION_TRANSITION_GRAPH = MappingProxyType(
    {
        (source, event): MissionTransition(source, event, attempt, target)
        for source in _ACTIVE_SOURCES
        for event, (attempt, target) in _EVENT_TARGETS.items()
    }
)


ATTEMPT_CLAIM_TRANSITION_GRAPH = MappingProxyType(
    {
        (
            AttemptClaimStatus.CLAIMED,
            AttemptClaimEvent.ARM_SUBMISSION,
        ): AttemptClaimStatus.POSSIBLY_SUBMITTED,
        (
            AttemptClaimStatus.POSSIBLY_SUBMITTED,
            AttemptClaimEvent.ACKNOWLEDGE_PROVIDER,
        ): AttemptClaimStatus.PROVIDER_ACKNOWLEDGED,
        (
            AttemptClaimStatus.PROVIDER_ACKNOWLEDGED,
            AttemptClaimEvent.STAGE_FINALIZATION,
        ): AttemptClaimStatus.FINALIZING,
        (
            AttemptClaimStatus.CLAIMED,
            AttemptClaimEvent.SETTLE_WITHOUT_SUBMISSION,
        ): AttemptClaimStatus.SETTLED,
        (
            AttemptClaimStatus.POSSIBLY_SUBMITTED,
            AttemptClaimEvent.SETTLE_AFTER_RECONCILIATION,
        ): AttemptClaimStatus.SETTLED,
        (
            AttemptClaimStatus.PROVIDER_ACKNOWLEDGED,
            AttemptClaimEvent.SETTLE_ACKNOWLEDGED,
        ): AttemptClaimStatus.SETTLED,
        (
            AttemptClaimStatus.FINALIZING,
            AttemptClaimEvent.SETTLE_FINALIZED,
        ): AttemptClaimStatus.SETTLED,
    }
)


class MissionTransitionGraph:
    """Parse durable states and reject every edge absent from the graph."""

    @staticmethod
    def state(mission: object, task: object) -> MissionTaskState:
        try:
            return MissionTaskState(MissionStatus(str(mission)), TaskStatus(str(task)))
        except ValueError as exc:
            raise ValueError(f"invalid persisted mission/task state: {mission!r}/{task!r}") from exc

    @staticmethod
    def transition(
        source: MissionTaskState,
        event: MissionTransitionEvent | str,
    ) -> MissionTransition:
        try:
            parsed_event = MissionTransitionEvent(event)
        except ValueError as exc:
            raise ValueError(f"unknown mission transition event: {event!r}") from exc
        try:
            return MISSION_TRANSITION_GRAPH[(source, parsed_event)]
        except KeyError as exc:
            raise ValueError(
                "illegal mission transition: "
                f"{source.mission.value}/{source.task.value} via {parsed_event.value}"
            ) from exc

    @staticmethod
    def require_active(source: MissionTaskState) -> None:
        if source not in _ACTIVE_SOURCES:
            raise ValueError(
                f"mission is not attemptable from state {source.mission.value}/{source.task.value}"
            )


class AttemptClaimTransitionGraph:
    """Parse and validate every durable submission-claim edge."""

    @staticmethod
    def state(value: object) -> AttemptClaimStatus:
        try:
            return AttemptClaimStatus(str(value))
        except ValueError as exc:
            raise ValueError(f"invalid persisted attempt claim state: {value!r}") from exc

    @staticmethod
    def transition(
        source: AttemptClaimStatus | str,
        event: AttemptClaimEvent | str,
    ) -> AttemptClaimTransition:
        try:
            parsed_source = AttemptClaimStatus(source)
            parsed_event = AttemptClaimEvent(event)
        except ValueError as exc:
            raise ValueError(
                f"invalid attempt claim transition input: {source!r}/{event!r}"
            ) from exc
        try:
            target = ATTEMPT_CLAIM_TRANSITION_GRAPH[(parsed_source, parsed_event)]
        except KeyError as exc:
            raise ValueError(
                f"illegal attempt claim transition: {parsed_source.value} via {parsed_event.value}"
            ) from exc
        return AttemptClaimTransition(parsed_source, parsed_event, target)


def retry_event(attempt: AttemptStatus, *, exhausted: bool) -> MissionTransitionEvent:
    """Select the sole retry/exhaustion edge for a terminal attempt result."""

    suffix = "exhausted" if exhausted else "retry"
    try:
        return MissionTransitionEvent(f"{attempt.value}_{suffix}")
    except ValueError as exc:
        raise ValueError(f"attempt state {attempt.value!r} cannot retry or exhaust") from exc
