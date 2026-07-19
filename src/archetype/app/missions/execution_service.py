# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Claim-fenced orchestration for one provider-neutral mission attempt."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import Any

from archetype.app.missions.interfaces import (
    _iMissionExecutionProjection,
    iMissionArtifactFinalizer,
    iMissionAttemptClaimService,
)
from archetype.app.missions.models import (
    AttemptClaim,
    AttemptClaimAcquisition,
    AttemptRecoveryDecision,
    FencedAttemptRunner,
    FencedExecutionAuthorization,
    MissionArtifactFinalizationExpiredError,
    MissionAttemptExecution,
    MissionAttemptRequest,
)
from archetype.app.missions.outcomes import assess_attempt_outcome
from archetype.app.missions.transitions import (
    AttemptClaimStatus,
    AttemptRecoveryAction,
)
from archetype.missions.transitions import (
    AttemptStatus,
    FinalizationPhase,
)


class MissionAttemptExecutionService:
    """Join mission transitions, durable claims, and a structural sandbox port."""

    def __init__(
        self,
        claims: iMissionAttemptClaimService,
        missions: _iMissionExecutionProjection,
        artifact_finalizer: iMissionArtifactFinalizer | None = None,
    ) -> None:
        self._claims = claims
        self._missions = missions
        self._artifact_finalizer = artifact_finalizer

    async def run(
        self,
        row: Mapping[str, Any],
        *,
        tick: int,
        claimant: str,
        runner: FencedAttemptRunner,
        lease_seconds: float = 900.0,
    ) -> MissionAttemptExecution | None:
        """Run or recover one attempt without converting reconciliation into inference."""

        if lease_seconds <= 0:
            raise ValueError("mission attempt execution lease_seconds must be positive")
        request = self._missions.prepare_attempt(row, tick=tick)
        if request is None:
            return None
        acquisition = await self._claims.acquire(
            request,
            runner.provider_execution_capabilities,
            claimant=claimant,
            lease_seconds=lease_seconds,
        )
        # Acquisition owns the durable request. A later reconciliation tick
        # must use its original observation tick and exact persisted evidence.
        request = self._claims.recover_request(acquisition.claim)
        decision = await self._claims.decide_recovery(
            acquisition.claim,
            lease_seconds=lease_seconds,
        )
        if decision.action is AttemptRecoveryAction.SETTLED:
            settled, outcome, updated = await self._project_settled(
                row,
                request,
                decision.claim,
            )
            return MissionAttemptExecution(
                request,
                acquisition,
                decision,
                settled,
                outcome,
                updated,
                True,
            )
        if decision.action is AttemptRecoveryAction.FINALIZE:
            return await self._finalize_artifacts(
                row,
                request=request,
                acquisition=acquisition,
                decision=decision,
                claim=decision.claim,
                lease_seconds=lease_seconds,
                replayed=True,
            )
        if decision.action not in {
            AttemptRecoveryAction.EXECUTE,
            AttemptRecoveryAction.RECONCILE,
        }:
            raise RuntimeError(
                f"attempt recovery action {decision.action.value!r} has no execution adapter"
            )

        active_claim = decision.claim

        async def authorize_execution(
            authorization: FencedExecutionAuthorization,
        ) -> None:
            nonlocal active_claim
            if authorization != decision.authorization:
                raise ValueError("sandbox requested consumption of a different execution grant")
            active_claim = await self._claims.consume_execution(authorization)

        async def acknowledge_provider(
            provider_session_id: str,
            provider_request_id: str,
        ) -> None:
            nonlocal active_claim
            if not provider_session_id.strip() and not provider_request_id.strip():
                return
            active_claim = await self._claims.acknowledge_provider(
                active_claim,
                provider_session_id=provider_session_id,
                provider_request_id=provider_request_id,
            )

        async def maintain_lease() -> None:
            while True:
                await asyncio.sleep(min(30.0, lease_seconds / 3))
                await self._claims.renew(active_claim, lease_seconds=lease_seconds)

        async def run_with_lease_heartbeat() -> dict[str, Any]:
            runner_task = asyncio.create_task(
                runner.run_attempt(
                    prompt=request.prompt,
                    validators=request.validators,
                    step_name=request.step_name,
                    attempt_index=request.attempt_index,
                    idempotency_key=request.idempotency_key,
                    authorization=decision.authorization,
                    authorize_execution=authorize_execution,
                    acknowledge_provider=acknowledge_provider,
                    previous_session_id=request.previous_session_id,
                    previous_validator_details=request.previous_validator_details,
                    correlation=request.correlation,
                )
            )
            heartbeat_task = asyncio.create_task(maintain_lease())
            try:
                done, _ = await asyncio.wait(
                    {runner_task, heartbeat_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if heartbeat_task in done:
                    error = heartbeat_task.exception()
                    if error is None:
                        raise RuntimeError("mission attempt lease heartbeat stopped unexpectedly")
                    raise error
                return await runner_task
            finally:
                for task in (runner_task, heartbeat_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(runner_task, heartbeat_task, return_exceptions=True)

        outcome = await run_with_lease_heartbeat()
        active_claim = await self._claims.renew(active_claim, lease_seconds=lease_seconds)
        if not isinstance(outcome, dict):
            raise TypeError("sandbox attempt outcome must be a JSON object")
        if (
            decision.action is AttemptRecoveryAction.EXECUTE
            and not active_claim.execution_consumed_at
        ):
            raise RuntimeError("attempt runner returned without consuming its execution grant")
        outcome_session_id = str(outcome.get("agent_session_id", ""))
        if active_claim.provider_session_id != outcome_session_id:
            raise RuntimeError(
                "attempt runner returned without durably acknowledging its provider session"
            )
        durable_outcome = self._claims.prepare_durable_outcome(active_claim, outcome)
        sanitized_outcome = dict(durable_outcome.value)
        assessment = assess_attempt_outcome(request, sanitized_outcome)
        if (
            request.required_finalization_phase is FinalizationPhase.INDEXED
            and assessment.provider_status in {AttemptStatus.ACCEPTED, AttemptStatus.REJECTED}
            and bool(sanitized_outcome["checkpoint_restorable"])
            and (
                assessment.provider_status is AttemptStatus.REJECTED
                or bool(str(sanitized_outcome["sha"]).strip())
            )
            and assessment.finalization_phase
            in {
                FinalizationPhase.CAPTURED,
                FinalizationPhase.CHECKPOINTED,
                FinalizationPhase.UPLOADED,
                FinalizationPhase.PUBLISHED,
            }
        ):
            finalizer = self._require_artifact_finalizer()
            projection = finalizer.prepare(
                request,
                sanitized_outcome,
                redaction_policy_id=active_claim.redaction_policy_id,
            )
            active_claim = await self._claims.stage_finalization(
                active_claim,
                outcome=durable_outcome,
                projection=projection,
            )
            return await self._finalize_artifacts(
                row,
                request=request,
                acquisition=acquisition,
                decision=decision,
                claim=active_claim,
                lease_seconds=lease_seconds,
                replayed=False,
            )
        updated = self._missions.apply_attempt(row, request, sanitized_outcome)
        settled = await self._claims.settle(
            active_claim,
            attempt_status=AttemptStatus(str(updated["attempt__status"])),
            outcome=durable_outcome,
            last_error=str(updated.get("mission__failure_reason") or ""),
        )
        # Re-read exactly what won the terminal CAS so first execution and
        # replay expose the same sanitized outcome and mission projection.
        settled, sanitized_outcome, updated = await self._project_settled(
            row,
            request,
            settled,
        )
        return MissionAttemptExecution(
            request,
            acquisition,
            decision,
            settled,
            sanitized_outcome,
            updated,
            False,
        )

    async def _finalize_artifacts(
        self,
        row: Mapping[str, Any],
        *,
        request: MissionAttemptRequest,
        acquisition: AttemptClaimAcquisition,
        decision: AttemptRecoveryDecision,
        claim: AttemptClaim,
        lease_seconds: float,
        replayed: bool,
    ) -> MissionAttemptExecution:
        finalizer = self._require_artifact_finalizer()

        def require_live_finalizing(renewed: AttemptClaim) -> AttemptClaim:
            if renewed.status is not AttemptClaimStatus.FINALIZING:
                raise RuntimeError("mission finalization lease no longer owns a finalizing claim")
            if renewed.lease_expires_at <= time.time():
                raise RuntimeError("mission finalization lease renewal returned an expired claim")
            return renewed

        # Staging and returning its projection are separate failure boundaries.
        # Revalidate the fence immediately before artifact I/O so an expired
        # worker cannot publish after another claimant has taken the claim over.
        active_claim = require_live_finalizing(
            await self._claims.renew(claim, lease_seconds=lease_seconds)
        )
        projection = self._claims.staged_artifact_projection(active_claim)

        async def maintain_lease() -> None:
            nonlocal active_claim
            while True:
                await asyncio.sleep(min(30.0, lease_seconds / 3))
                active_claim = require_live_finalizing(
                    await self._claims.renew(active_claim, lease_seconds=lease_seconds)
                )

        publication_task = asyncio.create_task(finalizer.publish(projection))
        heartbeat_task = asyncio.create_task(maintain_lease())
        try:
            done, _ = await asyncio.wait(
                {publication_task, heartbeat_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat_task in done:
                error = heartbeat_task.exception()
                if error is None:
                    raise RuntimeError("mission finalization lease heartbeat stopped unexpectedly")
                raise error
            try:
                await publication_task
            except MissionArtifactFinalizationExpiredError:
                # Expiry authority is the terminal catalog row, not this
                # process-local notification. Authenticate it below.
                pass
        finally:
            for task in (publication_task, heartbeat_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(publication_task, heartbeat_task, return_exceptions=True)

        active_claim = require_live_finalizing(
            await self._claims.renew(active_claim, lease_seconds=lease_seconds)
        )
        prepared_settlement = await self._claims.prepare_artifact_finalization_outcome(active_claim)
        settled = await self._claims.settle_finalized(
            active_claim,
            prepared_settlement,
        )
        settled, outcome, updated = await self._project_settled(
            row,
            request,
            settled,
        )
        return MissionAttemptExecution(
            request,
            acquisition,
            decision,
            settled,
            outcome,
            updated,
            replayed,
        )

    async def _project_settled(
        self,
        row: Mapping[str, Any],
        request: MissionAttemptRequest,
        claim: AttemptClaim,
    ) -> tuple[AttemptClaim, dict[str, Any], dict[str, Any]]:
        """Project only the canonical terminal row reread from durable authority."""

        settled = await self._claims.require_settled(claim.world_id, claim.claim_key)
        outcome = self._settled_mapping(settled)
        updated = self._missions._apply_settled_attempt(
            row,
            request,
            outcome,
            settled,
        )
        return settled, outcome, updated

    def _require_artifact_finalizer(self) -> iMissionArtifactFinalizer:
        if self._artifact_finalizer is None:
            raise RuntimeError("indexed mission finalization requires an artifact finalizer")
        return self._artifact_finalizer

    def _settled_mapping(self, claim: AttemptClaim) -> dict[str, Any]:
        outcome = self._claims.settled_outcome(claim)
        if not isinstance(outcome, dict):
            raise ValueError("settled attempt outcome must be a JSON object")
        return outcome
