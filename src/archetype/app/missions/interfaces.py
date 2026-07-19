# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the mission family."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from archetype.app.missions.models import (
    WORKTREE_ARCHIVE_OUTCOME_CONTRACT_VERSION,
    AttemptArtifactProjection,
    AttemptArtifactPublication,
    AttemptClaim,
    AttemptClaimAcquisition,
    AttemptRecoveryDecision,
    FencedAttemptRunner,
    FencedExecutionAuthorization,
    MissionAttemptExecution,
    MissionAttemptRequest,
    PreparedFinalizationSettlement,
    ProviderExecutionCapabilities,
)
from archetype.app.missions.outcomes import MissionAttemptAssessment
from archetype.app.redaction.models import RedactedRecord
from archetype.missions.transitions import AttemptStatus


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
class _iMissionExecutionProjection(iMissionService, Protocol):
    """Family-private row transformer reached only after durable authentication."""

    def _apply_claimed_attempt(
        self,
        row: Mapping[str, Any],
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
        claim: AttemptClaim,
    ) -> dict[str, Any]: ...

    def _apply_settled_attempt(
        self,
        row: Mapping[str, Any],
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
        claim: AttemptClaim,
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

    async def settle_finalized(
        self,
        claim: AttemptClaim,
        prepared: PreparedFinalizationSettlement,
        *,
        last_error: str = "",
    ) -> AttemptClaim: ...

    async def stage_finalization(
        self,
        claim: AttemptClaim,
        *,
        outcome: RedactedRecord,
        projection: AttemptArtifactProjection,
    ) -> AttemptClaim: ...

    async def consume_execution(
        self,
        authorization: FencedExecutionAuthorization,
    ) -> AttemptClaim: ...

    async def get(self, world_id: str, claim_key: str) -> AttemptClaim | None: ...

    async def require_settled(self, world_id: str, claim_key: str) -> AttemptClaim: ...

    async def list_due(
        self,
        world_id: str,
        *,
        now: float | None = None,
        limit: int = 100,
    ) -> list[AttemptClaim]: ...

    def prepare_durable_outcome(
        self,
        claim: AttemptClaim,
        outcome: Mapping[str, Any],
    ) -> RedactedRecord: ...

    def assess_outcome(
        self,
        claim: AttemptClaim,
        outcome: Mapping[str, Any],
    ) -> MissionAttemptAssessment: ...

    def settled_outcome(self, claim: AttemptClaim) -> Any: ...

    def staged_artifact_projection(self, claim: AttemptClaim) -> AttemptArtifactProjection: ...

    async def prepare_artifact_finalization_outcome(
        self,
        claim: AttemptClaim,
    ) -> PreparedFinalizationSettlement: ...

    def recover_request(self, claim: AttemptClaim) -> MissionAttemptRequest: ...

    def outcome_digest(self, outcome: Any) -> str: ...

    def claim_key(
        self,
        *,
        world_id: str,
        mission_id: str,
        task_id: str,
        attempt_id: str,
    ) -> str: ...


@runtime_checkable
class iMissionArtifactFinalizer(Protocol):
    """Prepare and publish one exact mission-owned artifact projection."""

    def prepare(
        self,
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
        *,
        redaction_policy_id: str,
        claim_contract_version: int = WORKTREE_ARCHIVE_OUTCOME_CONTRACT_VERSION,
    ) -> AttemptArtifactProjection: ...

    async def publish(
        self,
        projection: AttemptArtifactProjection,
    ) -> AttemptArtifactPublication: ...


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
