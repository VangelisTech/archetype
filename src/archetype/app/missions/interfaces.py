# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the mission family."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from daft import DataFrame

from archetype.app.evaluation.interfaces import GraderOutput, TrajectoryGrader
from archetype.app.missions.models import (
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
from archetype.app.redaction.models import RedactedRecord
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.missions.contracts import AgentTask, MissionResult, SubmittedMission
from archetype.missions.trajectories import TrajectorySelection
from archetype.missions.transitions import AttemptStatus


@runtime_checkable
class iAgentMissionService(Protocol):
    """Materialize and drive one persisted coding-agent task graph."""

    async def submit(
        self,
        *,
        repository: str,
        branch: str,
        tasks: Sequence[AgentTask],
        name: str = "agent-mission",
        base_ref: str = "main",
    ) -> SubmittedMission: ...

    async def run(
        self,
        mission: SubmittedMission,
        *,
        max_ticks: int | None = None,
    ) -> MissionResult: ...

    async def close(self) -> None: ...

    async def query(self, *components: type[Component]) -> DataFrame: ...

    @property
    def world_id(self) -> object: ...


@runtime_checkable
class iTrajectoryService(Protocol):
    """Query and optionally grade one persisted trajectory table."""

    async def query(
        self,
        component: type[Component],
        *,
        world_id: str,
        run_id: str,
        selection: TrajectorySelection | None = None,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame: ...

    async def grade(
        self,
        component: type[Component],
        *,
        world_id: str,
        run_id: str,
        graders: Sequence[TrajectoryGrader],
        selection: TrajectorySelection | None = None,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> list[GraderOutput]: ...


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
