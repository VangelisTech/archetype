# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable author-activity choreography owned by Agent Missions.

This module deliberately defines application ports instead of a second activity
catalog.  Concrete composition adapts these ports to the generic activity
coordinator and durable value storage.  The mission family retains the meaning
of author requests, provider reconciliation, and returned observations.

The concrete receipt-pinned reader, atomic complete fact-bundle stager, and
per-world binding live in :mod:`archetype.app.missions.activity_world`.
Concrete runtime composition installs that binding for the supported Modal
Mission author path; non-Modal backends retain the direct local path.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from daft import DataFrame

from archetype.core.hooks import PostTick
from archetype.core.interfaces import ArchetypeSignature, CommittedTickReceipt
from archetype.errors import AvailabilityError
from archetype.missions.activities import (
    AuthorActivityRequestRef,
    AuthorActivityResultRef,
    AuthorActivityRetryGuard,
    AuthorConfirmedAbsent,
    AuthorExecutionObservation,
    AuthorRecovered,
    AuthorRecoveryUnknown,
    DurableAuthorExecutionObservation,
    MissionAuthorExecutor,
    author_provider_operation_id,
    complete_author_activity_fact_bundle,
)
from archetype.missions.coding_agents.contracts import TaskDispatchRequest
from archetype.missions.components import Candidate
from archetype.missions.projections import (
    project_complete_author_activity_fact_bundles,
    project_task_dispatch_requests,
)
from archetype.redaction import RedactedText, RedactionReceipt


@dataclass(frozen=True, slots=True)
class AuthorActivityClaim:
    """One fenced delivery of a logical author activity."""

    world_id: str
    activity_id: str
    attempt: int
    fence: int
    request: AuthorActivityRequestRef
    provider: str = ""
    provider_operation_id: str = ""
    reconciliation_required: bool = False
    retry_guard: AuthorActivityRetryGuard | None = None

    def __post_init__(self) -> None:
        if not self.world_id.strip() or not self.activity_id.strip():
            raise ValueError("author activity claim requires world and activity identities")
        if self.attempt < 1 or self.fence < 1:
            raise ValueError("author activity claim requires positive attempt and fence values")
        if bool(self.provider) != bool(self.provider_operation_id):
            raise ValueError(
                "author activity provider and operation identity must be present together"
            )
        if self.reconciliation_required and (not self.provider or not self.provider_operation_id):
            raise ValueError(
                "reconciliation-required author activity must retain provider identity"
            )


@dataclass(frozen=True, slots=True)
class AuthorActivityResultDelivery:
    """One completed result that has not yet been observed by a committed tick."""

    world_id: str
    activity_id: str
    request: AuthorActivityRequestRef
    result: AuthorActivityResultRef

    def __post_init__(self) -> None:
        if not self.world_id.strip() or not self.activity_id.strip():
            raise ValueError("author result delivery requires world and activity identities")


@dataclass(frozen=True, slots=True)
class CommittedMissionSnapshot:
    """Mission-owned exact-state carrier returned for one committed receipt."""

    world_id: str
    run_id: str
    committed_tick: int
    visibility_token: str
    results: Mapping[ArchetypeSignature, DataFrame]

    def __post_init__(self) -> None:
        if not self.world_id.strip() or not self.run_id.strip():
            raise ValueError("committed mission snapshot requires world and run identities")
        if self.committed_tick < 0:
            raise ValueError("committed mission snapshot tick cannot be negative")
        if not self.visibility_token.strip():
            raise ValueError("committed mission snapshot requires a visibility token")

    @property
    def receipt_identity(self) -> tuple[str, str, int, str]:
        """Return the exact manifest-bound commit identity represented here."""

        return (
            self.world_id,
            self.run_id,
            self.committed_tick,
            self.visibility_token,
        )

    def as_post_tick(self) -> PostTick:
        """Adapt to the family projection carrier without exposing hooks as a port."""

        return PostTick(
            world_id=self.world_id,
            tick=self.committed_tick + 1,
            results=dict(self.results),
        )


@runtime_checkable
class MissionCommittedIntentReader(Protocol):
    """Read the exact committed mission snapshot authorized by one receipt."""

    async def read(self, receipt: CommittedTickReceipt) -> CommittedMissionSnapshot: ...


@runtime_checkable
class MissionAuthorActivityCatalog(Protocol):
    """Application view of generic activity coordination mechanics.

    ``admit_author`` is idempotent by ``(world_id, activity_id)`` across later
    full snapshots: an existing identical request retains its original source
    receipt. Conflicting request content must fail.
    """

    async def admit_author(
        self,
        *,
        world_id: str,
        receipt: CommittedTickReceipt,
        activity_id: str,
        request: AuthorActivityRequestRef,
    ) -> None: ...

    async def claim_author(
        self,
        *,
        world_id: str,
        owner: str,
    ) -> AuthorActivityClaim | None: ...

    async def bind_provider_operation(
        self,
        claim: AuthorActivityClaim,
        *,
        provider: str,
        operation_id: str,
    ) -> AuthorActivityClaim: ...

    async def confirm_provider_operation_absent(
        self,
        claim: AuthorActivityClaim,
        guard: AuthorActivityRetryGuard,
    ) -> AuthorActivityClaim: ...

    async def record_author_result(
        self,
        claim: AuthorActivityClaim,
        result: AuthorActivityResultRef,
    ) -> None: ...

    async def pending_author_results(
        self,
        *,
        world_id: str,
    ) -> tuple[AuthorActivityResultDelivery, ...]: ...

    async def settle_author_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        result_digest: str,
        receipt: CommittedTickReceipt,
    ) -> None: ...

    async def has_unsettled_work(self, world_id: str) -> bool: ...


@runtime_checkable
class MissionAuthorValueStore(Protocol):
    """Durable mission codec/store; values remain outside the control catalog."""

    async def put_request(self, request: TaskDispatchRequest) -> AuthorActivityRequestRef: ...

    async def get_request(self, value: AuthorActivityRequestRef) -> TaskDispatchRequest: ...

    async def put_result(
        self,
        observation: AuthorExecutionObservation,
    ) -> AuthorActivityResultRef: ...

    async def get_result(
        self,
        value: AuthorActivityResultRef,
    ) -> DurableAuthorExecutionObservation: ...


@runtime_checkable
class MissionAuthorRedactor(Protocol):
    """Canonical pre-durability redaction capability."""

    @property
    def policy_id(self) -> str: ...

    def redact_text(self, value: str, *, scope: str) -> RedactedText: ...

    def assert_safe_metadata(self, value: str, *, field: str) -> RedactionReceipt: ...


@runtime_checkable
class MissionAuthorObservationStager(Protocol):
    """Idempotently stage factual author evidence for a later world tick."""

    async def stage_author_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        request: TaskDispatchRequest,
        result: AuthorActivityResultRef,
        observation: DurableAuthorExecutionObservation,
    ) -> None: ...


class AuthorActivityReconciliationRequired(AvailabilityError):
    """A provider-bound execution cannot yet be reconciled safely."""

    public_detail = "Author activity provider state is temporarily unavailable"

    def __init__(self, activity_id: str, operation_id: str) -> None:
        self.activity_id = activity_id
        self.operation_id = operation_id
        super().__init__(
            f"author activity {activity_id!r} requires reconciliation "
            f"for provider operation {operation_id!r}"
        )


class MissionAuthorActivityProjector:
    """Admit committed dispatches and settle committed author observations."""

    consumer_name = "missions.author-activities"

    def __init__(
        self,
        *,
        reader: MissionCommittedIntentReader,
        catalog: MissionAuthorActivityCatalog,
        values: MissionAuthorValueStore,
    ) -> None:
        self._reader = reader
        self._catalog = catalog
        self._values = values

    async def project(self, receipt: CommittedTickReceipt) -> None:
        if receipt.visibility_token is None:
            raise ValueError("mission author projection requires a visibility token")
        snapshot = await self._reader.read(receipt)
        self._validate_snapshot(receipt, snapshot)
        event = snapshot.as_post_tick()

        for request in await project_task_dispatch_requests(event):
            value = await self._values.put_request(request)
            await self._catalog.admit_author(
                world_id=receipt.world_id,
                receipt=receipt,
                activity_id=request.dispatch_id,
                request=value,
            )

        pending = {
            delivery.activity_id: delivery
            for delivery in await self._catalog.pending_author_results(
                world_id=receipt.world_id,
            )
        }
        for projected in project_complete_author_activity_fact_bundles(event):
            observation = projected.marker
            delivery = pending.get(observation.activity_id)
            if delivery is None:
                continue
            request = await self._values.get_request(delivery.request)
            durable = await self._values.get_result(delivery.result)
            expected_result = durable.result
            if (
                request.dispatch_id != delivery.activity_id
                or expected_result.mission_id != request.mission_id
                or expected_result.task_id != request.task_id
                or expected_result.dispatch_id != request.dispatch_id
                or expected_result.dispatch_sequence != request.dispatch_sequence
            ):
                raise ValueError("durable author result does not match its admitted request")
            current_candidate = projected.bundle.components(Candidate)
            candidate_created_at_ms = 0
            if current_candidate:
                if len(current_candidate) != 1:
                    continue
                candidate_value = current_candidate[0].component
                assert isinstance(candidate_value, Candidate)
                candidate_created_at_ms = candidate_value.created_at_ms
            try:
                expected_bundle = complete_author_activity_fact_bundle(
                    request,
                    durable,
                    entity_ids=tuple(sorted(fact.entity_id for fact in projected.bundle.facts)),
                    prior_candidate_id=request.prior_candidate_entity_id or None,
                    candidate_created_at_ms=candidate_created_at_ms,
                )
            except ValueError:
                # Structural self-consistency is weaker than exact durable
                # result completeness. Leave an omission pending.
                continue
            expected = expected_bundle.marker(
                result=delivery.result,
                redaction_policy_id=durable.redaction_policy_id,
            )
            if observation != expected or expected_bundle.digest != projected.bundle.digest:
                continue
            await self._catalog.settle_author_observation(
                world_id=receipt.world_id,
                activity_id=observation.activity_id,
                result_digest=observation.result_digest,
                receipt=receipt,
            )

    @staticmethod
    def _validate_snapshot(
        receipt: CommittedTickReceipt,
        snapshot: CommittedMissionSnapshot,
    ) -> None:
        if snapshot.receipt_identity != receipt.identity:
            raise ValueError("mission intent snapshot does not match the exact committed receipt")


class MissionAuthorActivityWorker:
    """Run one local author claim and redeliver results until tick observation."""

    def __init__(
        self,
        *,
        world_id: str,
        owner: str,
        catalog: MissionAuthorActivityCatalog,
        values: MissionAuthorValueStore,
        executor: MissionAuthorExecutor,
        stager: MissionAuthorObservationStager,
    ) -> None:
        if not world_id.strip() or not owner.strip():
            raise ValueError("mission author worker world and owner cannot be empty")
        if not executor.provider.strip():
            raise ValueError("mission author executor provider cannot be empty")
        self._world_id = world_id
        self._owner = owner
        self._catalog = catalog
        self._values = values
        self._executor = executor
        self._stager = stager

    async def run_once(self) -> bool:
        """Make bounded progress without ever replaying provider-bound work."""

        progressed = await self._deliver_pending_results()
        claim = await self._catalog.claim_author(
            world_id=self._world_id,
            owner=self._owner,
        )
        if claim is None:
            return progressed
        if claim.world_id != self._world_id:
            raise ValueError("author activity catalog returned another world's claim")

        request = await self._values.get_request(claim.request)
        self._validate_request(claim, request)
        result_claim, observation = await self._execute_or_reconcile(claim, request)
        self._validate_provider_observation(result_claim, request, observation)
        result_ref = await self._values.put_result(observation)
        await self._catalog.record_author_result(result_claim, result_ref)
        await self._deliver_pending_results()
        return True

    async def _execute_or_reconcile(
        self,
        claim: AuthorActivityClaim,
        request: TaskDispatchRequest,
    ) -> tuple[AuthorActivityClaim, AuthorExecutionObservation]:
        if claim.provider_operation_id:
            if claim.provider != self._executor.provider:
                raise ValueError("author activity claim belongs to another provider adapter")
            reconciliation = await self._executor.reconcile(
                operation_id=claim.provider_operation_id,
                request=request,
            )
            if isinstance(reconciliation, AuthorRecovered):
                return claim, reconciliation.observation
            if isinstance(reconciliation, AuthorConfirmedAbsent):
                fresh = await self._catalog.confirm_provider_operation_absent(
                    claim,
                    reconciliation.guard,
                )
                if fresh.provider_operation_id or fresh.reconciliation_required:
                    raise RuntimeError(
                        "confirmed-absent reconciliation did not yield a fresh claim"
                    )
                if fresh.retry_guard != reconciliation.guard:
                    raise RuntimeError(
                        "confirmed-absent reconciliation returned another retry guard"
                    )
                return await self._bind_and_execute(
                    fresh,
                    request,
                    operation_id=claim.provider_operation_id,
                )
            if isinstance(reconciliation, AuthorRecoveryUnknown):
                raise AuthorActivityReconciliationRequired(
                    claim.activity_id,
                    claim.provider_operation_id,
                )
            raise TypeError("author executor returned an invalid reconciliation result")

        if claim.reconciliation_required:
            raise AssertionError("reconciliation claim lacks provider operation identity")

        operation_id = author_provider_operation_id(
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
        claim: AuthorActivityClaim,
        request: TaskDispatchRequest,
        *,
        operation_id: str,
    ) -> tuple[AuthorActivityClaim, AuthorExecutionObservation]:
        bound = await self._catalog.bind_provider_operation(
            claim,
            provider=self._executor.provider,
            operation_id=operation_id,
        )
        if bound.provider_operation_id != operation_id:
            raise RuntimeError("activity catalog returned another provider operation identity")
        if bound.provider != self._executor.provider:
            raise RuntimeError("activity catalog returned another provider adapter identity")
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
        for delivery in await self._catalog.pending_author_results(
            world_id=self._world_id,
        ):
            if delivery.world_id != self._world_id:
                raise ValueError("author activity catalog returned another world's result")
            request = await self._values.get_request(delivery.request)
            if request.dispatch_id != delivery.activity_id:
                raise ValueError("author result delivery does not match its request")
            observation = await self._values.get_result(delivery.result)
            self._validate_observation(delivery.activity_id, observation)
            await self._stager.stage_author_observation(
                world_id=delivery.world_id,
                activity_id=delivery.activity_id,
                request=request,
                result=delivery.result,
                observation=observation,
            )
            progressed = True
        return progressed

    @staticmethod
    def _validate_request(
        claim: AuthorActivityClaim,
        request: TaskDispatchRequest,
    ) -> None:
        if request.dispatch_id != claim.activity_id:
            raise ValueError("claimed author activity does not match its request")

    @staticmethod
    def _validate_observation(
        activity_id: str,
        observation: DurableAuthorExecutionObservation,
    ) -> None:
        if observation.result.dispatch_id != activity_id:
            raise ValueError("author activity result does not match its activity")

    @staticmethod
    def _validate_provider_observation(
        claim: AuthorActivityClaim,
        request: TaskDispatchRequest,
        observation: AuthorExecutionObservation,
    ) -> None:
        result = observation.result
        expected = (
            request.mission_id,
            request.task_id,
            request.dispatch_id,
            request.dispatch_sequence,
        )
        observed = (
            result.mission_id,
            result.task_id,
            result.dispatch_id,
            result.dispatch_sequence,
        )
        if observed != expected or result.dispatch_id != claim.activity_id:
            raise ValueError("provider author observation has another committed identity")


__all__ = [
    "AuthorActivityClaim",
    "AuthorActivityReconciliationRequired",
    "AuthorActivityResultDelivery",
    "CommittedMissionSnapshot",
    "MissionAuthorActivityCatalog",
    "MissionAuthorActivityProjector",
    "MissionAuthorActivityWorker",
    "MissionAuthorObservationStager",
    "MissionAuthorRedactor",
    "MissionAuthorValueStore",
    "MissionCommittedIntentReader",
]
