# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable attempt-claim and recovery transition authority."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType


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


@dataclass(frozen=True)
class AttemptClaimTransition:
    """One validated provider-submission control-plane edge."""

    source: AttemptClaimStatus
    event: AttemptClaimEvent
    target: AttemptClaimStatus


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
