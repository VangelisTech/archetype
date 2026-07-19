# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Storage-owned transition authority for durable fleet recovery records.

The control catalog is the writer of these states, so the closed graph lives
below the recovery workflow family.  Catalog implementations must ask this
oracle for every insert or update, including status-preserving lease renewals
and checkpoints.  An absent edge is a storage/programming error and therefore
fails closed before durable state changes.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType


class RecoverySweepStatus(StrEnum):
    """Durable recurring-sweep scheduling states."""

    IDLE = "idle"
    LEASED = "leased"
    RETRY_WAIT = "retry_wait"
    PAUSED = "paused"


class RecoveryExceptionStatus(StrEnum):
    """Durable sparse per-subject retry states."""

    RETRY_WAIT = "retry_wait"
    DEAD_LETTER = "dead_letter"
    RESOLVED = "resolved"


class RecoverySweepEvent(StrEnum):
    """Every mutation that may create or update a recurring sweep."""

    CREATE = "create"
    LEASE = "lease"
    TAKE_OVER = "take_over"
    RENEW = "renew"
    CHECKPOINT = "checkpoint"
    YIELD = "yield"
    FAIL = "fail"
    EXHAUST = "exhaust"
    PAUSE = "pause"
    REDRIVE = "redrive"


class RecoveryExceptionEvent(StrEnum):
    """Every mutation that may create or update a sparse exception."""

    RETRY = "retry"
    DEAD_LETTER = "dead_letter"
    RESOLVE = "resolve"
    REDRIVE = "redrive"


RECOVERY_SWEEP_TRANSITION_GRAPH: Mapping[
    tuple[RecoverySweepStatus | None, RecoverySweepEvent], RecoverySweepStatus
] = MappingProxyType(
    {
        (None, RecoverySweepEvent.CREATE): RecoverySweepStatus.IDLE,
        (RecoverySweepStatus.IDLE, RecoverySweepEvent.LEASE): RecoverySweepStatus.LEASED,
        (
            RecoverySweepStatus.RETRY_WAIT,
            RecoverySweepEvent.LEASE,
        ): RecoverySweepStatus.LEASED,
        (
            RecoverySweepStatus.LEASED,
            RecoverySweepEvent.TAKE_OVER,
        ): RecoverySweepStatus.LEASED,
        (
            RecoverySweepStatus.LEASED,
            RecoverySweepEvent.RENEW,
        ): RecoverySweepStatus.LEASED,
        (
            RecoverySweepStatus.LEASED,
            RecoverySweepEvent.CHECKPOINT,
        ): RecoverySweepStatus.LEASED,
        (RecoverySweepStatus.LEASED, RecoverySweepEvent.YIELD): RecoverySweepStatus.IDLE,
        (
            RecoverySweepStatus.LEASED,
            RecoverySweepEvent.FAIL,
        ): RecoverySweepStatus.RETRY_WAIT,
        (
            RecoverySweepStatus.LEASED,
            RecoverySweepEvent.EXHAUST,
        ): RecoverySweepStatus.PAUSED,
        (
            RecoverySweepStatus.LEASED,
            RecoverySweepEvent.PAUSE,
        ): RecoverySweepStatus.PAUSED,
        (
            RecoverySweepStatus.PAUSED,
            RecoverySweepEvent.REDRIVE,
        ): RecoverySweepStatus.IDLE,
    }
)

RECOVERY_EXCEPTION_TRANSITION_GRAPH: Mapping[
    tuple[RecoveryExceptionStatus | None, RecoveryExceptionEvent], RecoveryExceptionStatus
] = MappingProxyType(
    {
        (None, RecoveryExceptionEvent.RETRY): RecoveryExceptionStatus.RETRY_WAIT,
        (None, RecoveryExceptionEvent.DEAD_LETTER): RecoveryExceptionStatus.DEAD_LETTER,
        (
            RecoveryExceptionStatus.RETRY_WAIT,
            RecoveryExceptionEvent.RETRY,
        ): RecoveryExceptionStatus.RETRY_WAIT,
        (
            RecoveryExceptionStatus.RETRY_WAIT,
            RecoveryExceptionEvent.DEAD_LETTER,
        ): RecoveryExceptionStatus.DEAD_LETTER,
        (
            RecoveryExceptionStatus.RETRY_WAIT,
            RecoveryExceptionEvent.RESOLVE,
        ): RecoveryExceptionStatus.RESOLVED,
        (
            RecoveryExceptionStatus.DEAD_LETTER,
            RecoveryExceptionEvent.RESOLVE,
        ): RecoveryExceptionStatus.RESOLVED,
        (
            RecoveryExceptionStatus.DEAD_LETTER,
            RecoveryExceptionEvent.REDRIVE,
        ): RecoveryExceptionStatus.RETRY_WAIT,
    }
)


class RecoverySweepTransitionGraph:
    """Parse durable sweep values and reject every absent edge."""

    @staticmethod
    def state(value: object) -> RecoverySweepStatus:
        try:
            return RecoverySweepStatus(str(value))
        except ValueError as exc:
            raise ValueError(f"unknown recovery sweep state: {value!r}") from exc

    @staticmethod
    def transition(
        source: RecoverySweepStatus | str | None,
        event: RecoverySweepEvent | str,
    ) -> RecoverySweepStatus:
        parsed_source = None if source is None else RecoverySweepTransitionGraph.state(source)
        try:
            parsed_event = RecoverySweepEvent(event)
        except ValueError as exc:
            raise ValueError(f"unknown recovery sweep event: {event!r}") from exc
        try:
            return RECOVERY_SWEEP_TRANSITION_GRAPH[(parsed_source, parsed_event)]
        except KeyError as exc:
            source_label = "absent" if parsed_source is None else parsed_source.value
            raise ValueError(
                f"illegal recovery sweep transition: {source_label} via {parsed_event.value}"
            ) from exc


class RecoveryExceptionTransitionGraph:
    """Parse durable exception values and reject every absent edge."""

    @staticmethod
    def state(value: object) -> RecoveryExceptionStatus:
        try:
            return RecoveryExceptionStatus(str(value))
        except ValueError as exc:
            raise ValueError(f"unknown recovery exception state: {value!r}") from exc

    @staticmethod
    def transition(
        source: RecoveryExceptionStatus | str | None,
        event: RecoveryExceptionEvent | str,
    ) -> RecoveryExceptionStatus:
        parsed_source = None if source is None else RecoveryExceptionTransitionGraph.state(source)
        try:
            parsed_event = RecoveryExceptionEvent(event)
        except ValueError as exc:
            raise ValueError(f"unknown recovery exception event: {event!r}") from exc
        try:
            return RECOVERY_EXCEPTION_TRANSITION_GRAPH[(parsed_source, parsed_event)]
        except KeyError as exc:
            source_label = "absent" if parsed_source is None else parsed_source.value
            raise ValueError(
                f"illegal recovery exception transition: {source_label} via {parsed_event.value}"
            ) from exc


__all__ = [
    "RECOVERY_EXCEPTION_TRANSITION_GRAPH",
    "RECOVERY_SWEEP_TRANSITION_GRAPH",
    "RecoveryExceptionEvent",
    "RecoveryExceptionStatus",
    "RecoveryExceptionTransitionGraph",
    "RecoverySweepEvent",
    "RecoverySweepStatus",
    "RecoverySweepTransitionGraph",
]
