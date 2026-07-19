# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Claim-fenced orchestration for one provider-neutral mission attempt."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

from archetype.app.missions.interfaces import iMissionAttemptClaimService, iMissionService
from archetype.app.missions.models import (
    AttemptClaim,
    FencedAttemptRunner,
    FencedExecutionAuthorization,
    MissionAttemptExecution,
)
from archetype.app.missions.transitions import AttemptRecoveryAction, AttemptStatus


class MissionAttemptExecutionService:
    """Join mission transitions, durable claims, and a structural sandbox port."""

    def __init__(
        self,
        claims: iMissionAttemptClaimService,
        missions: iMissionService,
    ) -> None:
        self._claims = claims
        self._missions = missions

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
        decision = await self._claims.decide_recovery(
            acquisition.claim,
            lease_seconds=lease_seconds,
        )
        if decision.action is AttemptRecoveryAction.SETTLED:
            outcome = self._settled_mapping(decision.claim)
            updated = self._missions.apply_attempt(row, request, outcome)
            return MissionAttemptExecution(
                request,
                acquisition,
                decision,
                decision.claim,
                outcome,
                updated,
                True,
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
        updated = self._missions.apply_attempt(row, request, sanitized_outcome)
        settled = await self._claims.settle(
            active_claim,
            attempt_status=AttemptStatus(str(updated["attempt__status"])),
            outcome=durable_outcome,
            last_error=str(updated.get("mission__failure_reason") or ""),
        )
        # Re-read exactly what won the terminal CAS so first execution and
        # replay expose the same sanitized outcome and mission projection.
        sanitized_outcome = self._settled_mapping(settled)
        updated = self._missions.apply_attempt(row, request, sanitized_outcome)
        return MissionAttemptExecution(
            request,
            acquisition,
            decision,
            settled,
            sanitized_outcome,
            updated,
            False,
        )

    def _settled_mapping(self, claim: AttemptClaim) -> dict[str, Any]:
        outcome = self._claims.settled_outcome(claim)
        if not isinstance(outcome, dict):
            raise ValueError("settled attempt outcome must be a JSON object")
        return outcome
