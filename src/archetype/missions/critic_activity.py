# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable exact-candidate critic Activity choreography owned by Missions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from archetype.core.interfaces import CommittedTickReceipt
from archetype.errors import AvailabilityError
from archetype.missions.components import CriticExecution
from archetype.missions.critics import (
    CandidateReviewRequest,
    CriticActivityRequest,
    CriticActivityRequestRef,
    CriticActivityResult,
    CriticActivityResultRef,
    CriticActivityRetryGuard,
    CriticConfirmedAbsent,
    CriticExecutionResult,
    CriticRecovered,
    CriticRecoveryUnknown,
    MissionCriticExecutor,
    complete_critic_activity_fact_bundle,
    critic_provider_operation_id,
)
from archetype.missions.projections import (
    project_complete_critic_activity_fact_bundles,
    project_critic_activity_requests,
)

from .author_activity import CommittedMissionSnapshot, MissionCommittedIntentReader


@dataclass(frozen=True, slots=True)
class CriticActivityClaim:
    """One fenced delivery of a logical exact-candidate critic Activity."""

    world_id: str
    activity_id: str
    attempt: int
    fence: int
    request: CriticActivityRequestRef
    provider: str = ""
    provider_operation_id: str = ""
    reconciliation_required: bool = False
    retry_guard: CriticActivityRetryGuard | None = None

    def __post_init__(self) -> None:
        if not self.world_id.strip() or not self.activity_id.strip():
            raise ValueError("critic Activity claim requires world and activity identities")
        if self.attempt < 1 or self.fence < 1:
            raise ValueError("critic Activity claim requires positive attempt and fence values")
        if bool(self.provider) != bool(self.provider_operation_id):
            raise ValueError(
                "critic Activity provider and operation identity must be present together"
            )
        if self.reconciliation_required and (not self.provider or not self.provider_operation_id):
            raise ValueError(
                "reconciliation-required critic Activity must retain provider identity"
            )


@dataclass(frozen=True, slots=True)
class CriticActivityResultDelivery:
    """One durable critic result not yet observed by a committed world tick."""

    world_id: str
    activity_id: str
    request: CriticActivityRequestRef
    result: CriticActivityResultRef

    def __post_init__(self) -> None:
        if not self.world_id.strip() or not self.activity_id.strip():
            raise ValueError("critic result delivery requires world and activity identities")


@runtime_checkable
class MissionCriticActivityCatalog(Protocol):
    """Mission view of generic critic Activity coordination mechanics."""

    async def admit_critic(
        self,
        *,
        world_id: str,
        receipt: CommittedTickReceipt,
        activity_id: str,
        request: CriticActivityRequestRef,
    ) -> None: ...

    async def claim_critic(
        self,
        *,
        world_id: str,
        owner: str,
    ) -> CriticActivityClaim | None: ...

    async def bind_provider_operation(
        self,
        claim: CriticActivityClaim,
        *,
        provider: str,
        operation_id: str,
    ) -> CriticActivityClaim: ...

    async def confirm_provider_operation_absent(
        self,
        claim: CriticActivityClaim,
        guard: CriticActivityRetryGuard,
    ) -> CriticActivityClaim: ...

    async def record_critic_result(
        self,
        claim: CriticActivityClaim,
        result: CriticActivityResultRef,
    ) -> None: ...

    async def pending_critic_results(
        self,
        *,
        world_id: str,
    ) -> tuple[CriticActivityResultDelivery, ...]: ...

    async def settle_critic_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        result_digest: str,
        receipt: CommittedTickReceipt,
    ) -> None: ...

    async def has_unsettled_work(self, world_id: str) -> bool: ...


@runtime_checkable
class MissionCriticValueStore(Protocol):
    """Durable critic codec/store outside the generic control catalog."""

    async def put_request(
        self,
        request: CandidateReviewRequest,
    ) -> CriticActivityRequestRef: ...

    async def get_request(
        self,
        value: CriticActivityRequestRef,
    ) -> CriticActivityRequest: ...

    async def put_result(
        self,
        result: CriticExecutionResult,
        request: CriticActivityRequest,
    ) -> CriticActivityResultRef: ...

    async def get_result(
        self,
        value: CriticActivityResultRef,
    ) -> CriticActivityResult: ...


@runtime_checkable
class MissionCriticObservationStager(Protocol):
    """Idempotently stage critic facts for one later world tick."""

    async def stage_critic_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        request: CriticActivityRequest,
        result: CriticActivityResultRef,
        observation: CriticActivityResult,
    ) -> None: ...


class CriticActivityReconciliationRequired(AvailabilityError):
    """A provider-bound critic execution cannot yet be reconciled safely."""

    public_detail = "Critic activity provider state is temporarily unavailable"

    def __init__(self, activity_id: str, operation_id: str) -> None:
        self.activity_id = activity_id
        self.operation_id = operation_id
        super().__init__(
            f"critic Activity {activity_id!r} requires reconciliation "
            f"for provider operation {operation_id!r}"
        )


class MissionCriticActivityProjector:
    """Admit exact-candidate reviews and settle complete committed observations."""

    consumer_name = "missions.critic-activities"

    def __init__(
        self,
        *,
        reader: MissionCommittedIntentReader,
        catalog: MissionCriticActivityCatalog,
        values: MissionCriticValueStore,
    ) -> None:
        self._reader = reader
        self._catalog = catalog
        self._values = values

    async def project(self, receipt: CommittedTickReceipt) -> None:
        if receipt.visibility_token is None:
            raise ValueError("mission critic projection requires a visibility token")
        snapshot = await self._reader.read(receipt)
        self._validate_snapshot(receipt, snapshot)
        event = snapshot.as_post_tick()

        for request in await project_critic_activity_requests(event):
            value = await self._values.put_request(request)
            durable = await self._values.get_request(value)
            await self._catalog.admit_critic(
                world_id=receipt.world_id,
                receipt=receipt,
                activity_id=durable.review_id,
                request=value,
            )

        pending = {
            delivery.activity_id: delivery
            for delivery in await self._catalog.pending_critic_results(
                world_id=receipt.world_id,
            )
        }
        for projected in project_complete_critic_activity_fact_bundles(event):
            marker = projected.marker
            delivery = pending.get(marker.activity_id)
            if delivery is None:
                continue
            request = await self._values.get_request(delivery.request)
            result = await self._values.get_result(delivery.result)
            executions = projected.bundle.components(CriticExecution)
            if len(executions) != 1:
                continue
            execution = executions[0].component
            assert isinstance(execution, CriticExecution)
            try:
                expected_bundle = complete_critic_activity_fact_bundle(
                    request,
                    result,
                    entity_ids=tuple(sorted(fact.entity_id for fact in projected.bundle.facts)),
                    receipt_staged_at_ms=execution.receipt_staged_at_ms,
                )
                expected_marker = expected_bundle.marker(
                    request=request,
                    result=result,
                    result_ref=delivery.result,
                )
            except ValueError:
                continue
            if marker != expected_marker or expected_bundle.digest != projected.bundle.digest:
                continue
            await self._catalog.settle_critic_observation(
                world_id=receipt.world_id,
                activity_id=marker.activity_id,
                result_digest=marker.result_digest,
                receipt=receipt,
            )

    @staticmethod
    def _validate_snapshot(
        receipt: CommittedTickReceipt,
        snapshot: CommittedMissionSnapshot,
    ) -> None:
        if snapshot.receipt_identity != receipt.identity:
            raise ValueError("mission intent snapshot does not match the exact committed receipt")


class MissionCriticActivityWorker:
    """Run one critic claim and redeliver its result until tick observation."""

    def __init__(
        self,
        *,
        world_id: str,
        owner: str,
        catalog: MissionCriticActivityCatalog,
        values: MissionCriticValueStore,
        executor: MissionCriticExecutor,
        stager: MissionCriticObservationStager,
    ) -> None:
        if not world_id.strip() or not owner.strip():
            raise ValueError("mission critic worker world and owner cannot be empty")
        if not executor.provider.strip():
            raise ValueError("mission critic executor provider cannot be empty")
        self._world_id = world_id
        self._owner = owner
        self._catalog = catalog
        self._values = values
        self._executor = executor
        self._stager = stager

    async def run_once(self) -> bool:
        """Make bounded progress without replaying provider-bound critic work."""

        progressed = await self._deliver_pending_results()
        return await self._run_claim_once() or progressed

    async def run_until_idle(self) -> bool:
        """Drain currently claimable critic work outside the tick lock."""

        progressed = await self._deliver_pending_results()
        while await self._run_claim_once():
            progressed = True
        return progressed

    async def _run_claim_once(self) -> bool:
        claim = await self._catalog.claim_critic(
            world_id=self._world_id,
            owner=self._owner,
        )
        if claim is None:
            return False
        if claim.world_id != self._world_id:
            raise ValueError("critic Activity catalog returned another world's claim")
        request = await self._values.get_request(claim.request)
        if request.review_id != claim.activity_id:
            raise ValueError("claimed critic Activity does not match its request")

        result_claim, raw_result = await self._execute_or_reconcile(claim, request)
        self._validate_provider_result(result_claim, request, raw_result)
        result_ref = await self._values.put_result(raw_result, request)
        await self._catalog.record_critic_result(result_claim, result_ref)
        await self._deliver_pending_results()
        return True

    async def _execute_or_reconcile(
        self,
        claim: CriticActivityClaim,
        request: CriticActivityRequest,
    ) -> tuple[CriticActivityClaim, CriticExecutionResult]:
        if claim.provider_operation_id:
            if claim.provider != self._executor.provider:
                raise ValueError("critic Activity claim belongs to another provider adapter")
            reconciliation = await self._executor.reconcile(
                operation_id=claim.provider_operation_id,
                request=request,
            )
            if isinstance(reconciliation, CriticRecovered):
                return claim, reconciliation.result
            if isinstance(reconciliation, CriticConfirmedAbsent):
                fresh = await self._catalog.confirm_provider_operation_absent(
                    claim,
                    reconciliation.guard,
                )
                if fresh.provider_operation_id or fresh.reconciliation_required:
                    raise RuntimeError(
                        "confirmed-absent critic reconciliation did not yield a fresh claim"
                    )
                if fresh.retry_guard != reconciliation.guard:
                    raise RuntimeError(
                        "confirmed-absent critic reconciliation returned another retry guard"
                    )
                return await self._bind_and_execute(
                    fresh,
                    request,
                    operation_id=claim.provider_operation_id,
                )
            if isinstance(reconciliation, CriticRecoveryUnknown):
                raise CriticActivityReconciliationRequired(
                    claim.activity_id,
                    claim.provider_operation_id,
                )
            raise TypeError("critic executor returned an invalid reconciliation result")

        if claim.reconciliation_required:
            raise AssertionError("critic reconciliation claim lacks provider identity")
        operation_id = critic_provider_operation_id(
            claim.world_id,
            claim.activity_id,
        )
        return await self._bind_and_execute(
            claim,
            request,
            operation_id=operation_id,
        )

    async def _bind_and_execute(
        self,
        claim: CriticActivityClaim,
        request: CriticActivityRequest,
        *,
        operation_id: str,
    ) -> tuple[CriticActivityClaim, CriticExecutionResult]:
        bound = await self._catalog.bind_provider_operation(
            claim,
            provider=self._executor.provider,
            operation_id=operation_id,
        )
        if bound.provider_operation_id != operation_id:
            raise RuntimeError("critic catalog returned another provider operation identity")
        if bound.provider != self._executor.provider:
            raise RuntimeError("critic catalog returned another provider adapter identity")
        return (
            bound,
            await self._executor.execute(
                operation_id=operation_id,
                request=request,
                attempt=bound.attempt,
                fence=bound.fence,
                retry_guard=bound.retry_guard,
            ),
        )

    async def _deliver_pending_results(self) -> bool:
        progressed = False
        for delivery in await self._catalog.pending_critic_results(
            world_id=self._world_id,
        ):
            if delivery.world_id != self._world_id:
                raise ValueError("critic catalog returned another world's result")
            request = await self._values.get_request(delivery.request)
            if request.review_id != delivery.activity_id:
                raise ValueError("critic result delivery does not match its request")
            result = await self._values.get_result(delivery.result)
            if result.review_id != delivery.activity_id:
                raise ValueError("critic Activity result does not match its activity")
            await self._stager.stage_critic_observation(
                world_id=delivery.world_id,
                activity_id=delivery.activity_id,
                request=request,
                result=delivery.result,
                observation=result,
            )
            progressed = True
        return progressed

    @staticmethod
    def _validate_provider_result(
        claim: CriticActivityClaim,
        request: CriticActivityRequest,
        result: CriticExecutionResult,
    ) -> None:
        if (
            result.request.review_id != request.review_id
            or result.request.candidate_digest != request.candidate_digest
            or request.review_id != claim.activity_id
        ):
            raise ValueError("provider critic result has another committed identity")


__all__ = [
    "CriticActivityClaim",
    "CriticActivityReconciliationRequired",
    "CriticActivityResultDelivery",
    "MissionCriticActivityCatalog",
    "MissionCriticActivityProjector",
    "MissionCriticActivityWorker",
    "MissionCriticObservationStager",
    "MissionCriticValueStore",
]
