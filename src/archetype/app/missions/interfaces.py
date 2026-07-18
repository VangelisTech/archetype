# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the mission family."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from archetype.app.missions.models import (
    AttemptClaim,
    AttemptClaimAcquisition,
    AttemptRecoveryDecision,
    FencedAttemptRunner,
    FencedExecutionAuthorization,
    MissionAttemptExecution,
    MissionAttemptRequest,
    ProviderExecutionCapabilities,
)
from archetype.app.missions.transitions import AttemptStatus
from archetype.app.redaction.models import RedactedRecord


@runtime_checkable
class iMissionService(Protocol):
    """Prepare attempts and authorize task-state transitions."""

    def prepare_attempt(
        self, row: Mapping[str, Any], *, tick: int
    ) -> MissionAttemptRequest | None: ...

    def apply_attempt(
        self,
        row: Mapping[str, Any],
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
    ) -> dict[str, Any]: ...


@runtime_checkable
class iMissionAttemptClaimService(Protocol):
    """Persist, fence, recover, acknowledge, and settle provider submissions."""

    async def acquire(
        self,
        request: MissionAttemptRequest,
        capabilities: ProviderExecutionCapabilities,
        *,
        claimant: str,
        lease_seconds: float = 900.0,
    ) -> AttemptClaimAcquisition: ...

    async def decide_recovery(
        self,
        claim: AttemptClaim,
        *,
        lease_seconds: float = 900.0,
    ) -> AttemptRecoveryDecision: ...

    async def renew(
        self,
        claim: AttemptClaim,
        *,
        lease_seconds: float = 900.0,
    ) -> AttemptClaim: ...

    async def acknowledge_provider(
        self,
        claim: AttemptClaim,
        *,
        provider_session_id: str = "",
        provider_request_id: str = "",
    ) -> AttemptClaim: ...

    async def settle(
        self,
        claim: AttemptClaim,
        *,
        attempt_status: AttemptStatus | str,
        outcome: Mapping[str, Any] | RedactedRecord,
        last_error: str = "",
    ) -> AttemptClaim: ...

    async def consume_execution(
        self,
        authorization: FencedExecutionAuthorization,
    ) -> AttemptClaim: ...

    def prepare_durable_outcome(
        self,
        claim: AttemptClaim,
        outcome: Mapping[str, Any],
    ) -> RedactedRecord: ...

    def settled_outcome(self, claim: AttemptClaim) -> Any: ...


@runtime_checkable
class iMissionAttemptExecutionService(Protocol):
    """Orchestrate one claim-fenced sandbox attempt or its terminal replay."""

    async def run(
        self,
        row: Mapping[str, Any],
        *,
        tick: int,
        claimant: str,
        runner: FencedAttemptRunner,
        lease_seconds: float = 900.0,
    ) -> MissionAttemptExecution | None: ...
