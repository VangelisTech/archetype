# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Intent-to-Activity-to-observation choreography for hosted Physical AI."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from daft import DataFrame, col

from archetype.activities import (
    ActivityAdmission,
    ActivityClaim,
    ActivityConflictError,
    ActivityResultRef,
    ActivityRetryGuard,
    ActivitySettlement,
    claim_next_pending,
    collect_pending_results,
    iActivityCoordinator,
)
from archetype.core.hooks import PostTick
from archetype.core.interfaces import ArchetypeSignature, CommittedTickReceipt
from archetype.errors import AvailabilityError
from archetype.physical_ai.hosted_activity_contracts import (
    HOSTED_EPISODE_ACTIVITY_KIND,
    HostedEpisodeActivityResultRef,
    HostedEpisodeConfirmedAbsent,
    HostedEpisodeIntent,
    HostedEpisodeObservation,
    HostedEpisodeProvider,
    HostedEpisodeProviderResult,
    HostedEpisodePublishedResult,
    HostedEpisodeRecovered,
    HostedEpisodeRecoveryUnknown,
    HostedEpisodeRequestIdentity,
    HostedEpisodeRequestRef,
    HostedEpisodeRetryGuard,
    hosted_episode_provider_operation_id,
    validate_hosted_provider_result,
)
from archetype.physical_ai.hosted_episode import (
    decode_hosted_episode_requests,
    hosted_episode_request_digest,
)

# Pending-scan page size, not a scan bound: claim and result scans page
# until the catalog is exhausted.  A module constant so tests can shrink
# it and prove the pagination property with few rows.
_CLAIM_SCAN_PAGE = 1_000


@dataclass(frozen=True, slots=True)
class HostedEpisodeActivityClaim:
    """One fenced delivery of a logical whole-episode Activity."""

    world_id: str
    activity_id: str
    attempt: int
    fence: int
    request: HostedEpisodeRequestIdentity
    provider: str = ""
    provider_operation_id: str = ""
    reconciliation_required: bool = False
    retry_guard: HostedEpisodeRetryGuard | None = None

    def __post_init__(self) -> None:
        if not self.world_id.strip() or not self.activity_id.strip():
            raise ValueError("hosted claim requires world and activity identities")
        if self.attempt < 1 or self.fence < 1:
            raise ValueError("hosted claim requires positive attempt and fence values")
        if bool(self.provider) != bool(self.provider_operation_id):
            raise ValueError("hosted claim provider identity must be complete")
        if self.reconciliation_required and not self.provider_operation_id:
            raise ValueError("hosted reconciliation claim requires provider identity")


@dataclass(frozen=True, slots=True)
class HostedEpisodeResultDelivery:
    """One complete result awaiting a committed factual observation."""

    world_id: str
    activity_id: str
    request: HostedEpisodeRequestIdentity
    result: HostedEpisodeActivityResultRef


@dataclass(frozen=True, slots=True)
class CommittedPhysicalSnapshot:
    """Physical-AI view read from one exact committed receipt."""

    world_id: str
    run_id: str
    committed_tick: int
    visibility_token: str
    results: Mapping[ArchetypeSignature, DataFrame]

    def __post_init__(self) -> None:
        if not self.world_id.strip() or not self.run_id.strip():
            raise ValueError("committed physical snapshot requires world and run identities")
        if self.committed_tick < 0:
            raise ValueError("committed physical snapshot tick cannot be negative")
        if not self.visibility_token.strip():
            raise ValueError("committed physical snapshot requires a visibility token")

    @property
    def receipt_identity(self) -> tuple[str, str, int, str]:
        return (
            self.world_id,
            self.run_id,
            self.committed_tick,
            self.visibility_token,
        )

    def as_post_tick(self) -> PostTick:
        return PostTick(
            world_id=self.world_id,
            tick=self.committed_tick + 1,
            results=dict(self.results),
        )


@runtime_checkable
class PhysicalCommittedIntentReader(Protocol):
    """Read only the exact physical snapshot named by a committed receipt."""

    async def read(self, receipt: CommittedTickReceipt) -> CommittedPhysicalSnapshot: ...


@runtime_checkable
class PhysicalHostedActivityCatalog(Protocol):
    """Physical-AI view over generic Activity coordination mechanics."""

    async def admit_episode(
        self,
        *,
        world_id: str,
        receipt: CommittedTickReceipt,
        activity_id: str,
        request: HostedEpisodeRequestRef,
    ) -> None: ...

    async def claim_episode(
        self,
        *,
        world_id: str,
        owner: str,
        activity_id: str | None = None,
    ) -> HostedEpisodeActivityClaim | None: ...

    async def bind_provider_operation(
        self,
        claim: HostedEpisodeActivityClaim,
        *,
        provider: str,
        operation_id: str,
    ) -> HostedEpisodeActivityClaim: ...

    async def confirm_provider_operation_absent(
        self,
        claim: HostedEpisodeActivityClaim,
        guard: HostedEpisodeRetryGuard,
    ) -> HostedEpisodeActivityClaim: ...

    async def record_episode_result(
        self,
        claim: HostedEpisodeActivityClaim,
        result: HostedEpisodeActivityResultRef,
    ) -> None: ...

    async def pending_episode_results(
        self,
        *,
        world_id: str,
    ) -> tuple[HostedEpisodeResultDelivery, ...]: ...

    async def episode_result(
        self,
        *,
        world_id: str,
        activity_id: str,
    ) -> HostedEpisodeResultDelivery | None: ...

    async def settle_episode_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        result_digest: str,
        receipt: CommittedTickReceipt,
    ) -> None: ...

    async def episode_settled(
        self,
        *,
        world_id: str,
        activity_id: str,
        result_digest: str,
    ) -> bool: ...

    async def has_unsettled_work(self, world_id: str) -> bool: ...


@runtime_checkable
class PhysicalHostedValueStore(Protocol):
    """Durable request, trajectory, result, and manifest publication."""

    async def put_request(self, request_ipc: bytes) -> HostedEpisodeRequestRef: ...

    async def get_request(
        self,
        value: HostedEpisodeRequestRef | HostedEpisodeRequestIdentity,
    ) -> bytes: ...

    async def publish_result(
        self,
        request: HostedEpisodeRequestRef,
        result: HostedEpisodeProviderResult,
    ) -> HostedEpisodePublishedResult: ...

    async def get_result(
        self,
        value: HostedEpisodeActivityResultRef,
    ) -> HostedEpisodePublishedResult: ...


@runtime_checkable
class PhysicalHostedObservationStager(Protocol):
    """Idempotently stage one factual completion marker for a later tick."""

    async def stage_hosted_episode_observation(
        self,
        *,
        world_id: str,
        observation: HostedEpisodeObservation,
    ) -> None: ...


class HostedEpisodeReconciliationRequired(AvailabilityError):
    """A provider-bound episode cannot safely be replayed."""

    public_detail = "Hosted episode provider state is temporarily unavailable"

    def __init__(self, activity_id: str, operation_id: str, reason: str) -> None:
        self.activity_id = activity_id
        self.operation_id = operation_id
        self.reason = reason
        super().__init__(
            f"hosted episode {activity_id!r} requires provider reconciliation "
            f"for operation {operation_id!r}: {reason}"
        )


def _component_rows(
    event: PostTick,
    component_type: type[HostedEpisodeIntent] | type[HostedEpisodeObservation],
) -> tuple[HostedEpisodeIntent | HostedEpisodeObservation, ...]:
    prefix = component_type.get_prefix()
    columns = [f"{prefix}{field}" for field in component_type.model_fields]
    values: list[HostedEpisodeIntent | HostedEpisodeObservation] = []
    for signature, frame in event.results.items():
        if component_type not in signature:
            continue
        selected = frame
        if "is_active" in frame.column_names:
            selected = selected.where(col("is_active"))
        for row in selected.select(*columns).to_pylist():
            values.append(
                component_type(
                    **{field: row[f"{prefix}{field}"] for field in component_type.model_fields}
                )
            )
    return tuple(values)


class PhysicalHostedActivityProjector:
    """Admit committed intents and settle exact later observation markers."""

    consumer_name = "physical-ai.hosted-episodes"

    def __init__(
        self,
        *,
        reader: PhysicalCommittedIntentReader,
        catalog: PhysicalHostedActivityCatalog,
        values: PhysicalHostedValueStore,
    ) -> None:
        self._reader = reader
        self._catalog = catalog
        self._values = values

    async def project(self, receipt: CommittedTickReceipt) -> None:
        if receipt.visibility_token is None:
            raise ValueError("hosted episode projection requires a visibility token")
        snapshot = await self._reader.read(receipt)
        if snapshot.receipt_identity != receipt.identity:
            raise ValueError("physical snapshot does not match the exact committed receipt")
        event = snapshot.as_post_tick()

        intents = self._unique_intents(
            tuple(
                value
                for value in _component_rows(event, HostedEpisodeIntent)
                if isinstance(value, HostedEpisodeIntent)
                and value.operation_id
                == hosted_episode_provider_operation_id(receipt.world_id, value.activity_id)
            )
        )
        for intent in intents.values():
            request_ref = HostedEpisodeRequestRef(
                ref=intent.request_ref,
                digest=intent.request_digest,
                size_bytes=intent.request_size_bytes,
            )
            request_ipc = await self._values.get_request(request_ref)
            self._validate_intent(receipt.world_id, intent, request_ipc)
            await self._catalog.admit_episode(
                world_id=receipt.world_id,
                receipt=receipt,
                activity_id=intent.activity_id,
                request=request_ref,
            )

        pending = {
            delivery.activity_id: delivery
            for delivery in await self._catalog.pending_episode_results(
                world_id=receipt.world_id,
            )
        }
        observations = self._unique_observations(
            tuple(
                value
                for value in _component_rows(event, HostedEpisodeObservation)
                if isinstance(value, HostedEpisodeObservation)
                and value.operation_id
                == hosted_episode_provider_operation_id(receipt.world_id, value.activity_id)
            )
        )
        for observation in observations.values():
            delivery = pending.get(observation.activity_id)
            if delivery is None:
                continue
            published = await self._values.get_result(delivery.result)
            expected = published.observation(observation.activity_id)
            if observation != expected:
                continue
            await self._catalog.settle_episode_observation(
                world_id=receipt.world_id,
                activity_id=observation.activity_id,
                result_digest=observation.result_digest,
                receipt=receipt,
            )

    @staticmethod
    def _unique_intents(
        intents: tuple[HostedEpisodeIntent | HostedEpisodeObservation, ...],
    ) -> dict[str, HostedEpisodeIntent]:
        unique: dict[str, HostedEpisodeIntent] = {}
        for value in intents:
            if not isinstance(value, HostedEpisodeIntent):
                raise TypeError("hosted intent projection returned another component")
            existing = unique.get(value.activity_id)
            if existing is not None and existing != value:
                raise ValueError("one hosted activity has conflicting committed intents")
            unique[value.activity_id] = value
        return unique

    @staticmethod
    def _unique_observations(
        observations: tuple[HostedEpisodeIntent | HostedEpisodeObservation, ...],
    ) -> dict[str, HostedEpisodeObservation]:
        # The committed view lists one durable marker once per persisted tick,
        # so equal re-listings collapse exactly as committed intents do; only
        # conflicting values for one Activity fail closed.
        unique: dict[str, HostedEpisodeObservation] = {}
        for value in observations:
            if not isinstance(value, HostedEpisodeObservation):
                raise TypeError("hosted observation projection returned another component")
            existing = unique.get(value.activity_id)
            if existing is not None and existing != value:
                raise ValueError(
                    "one hosted Activity has conflicting committed observation markers"
                )
            unique[value.activity_id] = value
        return unique

    @staticmethod
    def _validate_intent(
        world_id: str,
        intent: HostedEpisodeIntent,
        request_ipc: bytes,
    ) -> None:
        digest = hosted_episode_request_digest(request_ipc)
        rows = decode_hosted_episode_requests(request_ipc)
        expected_operation = hosted_episode_provider_operation_id(
            world_id,
            intent.activity_id,
        )
        if (
            intent.request_digest != digest
            or intent.request_size_bytes != len(request_ipc)
            or intent.operation_id != expected_operation
            or any(row["operation_id"] != expected_operation for row in rows)
            or intent.episode_count != len(rows)
        ):
            raise ValueError("committed hosted intent does not match its durable request")


async def prepare_hosted_episode_intent(
    values: PhysicalHostedValueStore,
    *,
    world_id: str,
    activity_id: str,
    request_ipc: bytes,
) -> HostedEpisodeIntent:
    """Publish an immutable request and return the Component to commit."""

    operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
    rows = decode_hosted_episode_requests(request_ipc)
    if any(row["operation_id"] != operation_id for row in rows):
        raise ValueError("hosted request operation does not match its world Activity")
    request = await values.put_request(request_ipc)
    if request.digest != hosted_episode_request_digest(request_ipc) or request.size_bytes != len(
        request_ipc
    ):
        raise ValueError("hosted value store returned another request identity")
    return HostedEpisodeIntent(
        activity_id=activity_id,
        operation_id=operation_id,
        request_ref=request.ref,
        request_digest=request.digest,
        request_size_bytes=request.size_bytes,
        episode_count=len(rows),
    )


class PhysicalHostedActivityCoordinator:
    """Translate generic mechanics into the Physical-AI family port."""

    def __init__(
        self,
        coordinator: iActivityCoordinator,
        *,
        lease_seconds: float = 300.0,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("hosted episode Activity lease must be positive")
        self._coordinator = coordinator
        self._lease_seconds = lease_seconds
        self._claims: dict[tuple[str, str, int], ActivityClaim] = {}

    async def admit_episode(
        self,
        *,
        world_id: str,
        receipt: CommittedTickReceipt,
        activity_id: str,
        request: HostedEpisodeRequestRef,
    ) -> None:
        if world_id != receipt.world_id:
            raise ValueError("hosted episode admission belongs to another world")
        admission = ActivityAdmission(
            activity_id=activity_id,
            kind=HOSTED_EPISODE_ACTIVITY_KIND,
            source=receipt,
            input_ref=request.ref,
            input_digest=request.digest,
        )
        existing = await self._coordinator.get(
            world_id,
            HOSTED_EPISODE_ACTIVITY_KIND,
            activity_id,
        )
        if existing is not None:
            self._validate_existing(existing.admission, admission)
            return
        try:
            await self._coordinator.admit(admission)
        except ActivityConflictError:
            existing = await self._coordinator.get(
                world_id,
                HOSTED_EPISODE_ACTIVITY_KIND,
                activity_id,
            )
            if existing is None:
                raise
            self._validate_existing(existing.admission, admission)

    async def claim_episode(
        self,
        *,
        world_id: str,
        owner: str,
        activity_id: str | None = None,
    ) -> HostedEpisodeActivityClaim | None:
        if activity_id is not None:
            # Operation-scoped delivery: claim exactly the admitted Activity so
            # one hosted call never executes another episode's older pending
            # claim.  An unacquired claim means the Activity is leased by
            # another live owner or already holds its durable result; both are
            # "nothing to execute here", not errors.
            generic = await self._coordinator.claim(
                world_id,
                HOSTED_EPISODE_ACTIVITY_KIND,
                activity_id,
                owner,
                lease_seconds=self._lease_seconds,
            )
            if not generic.acquired:
                return None
            return self._remember(generic)
        # Page until the catalog is exhausted: a finite prefix scan stranded
        # claimable Activities beyond the batch when the head of the pending
        # set was leased by other workers.  claim_next_pending carries this
        # invariant for the author, critic, and hosted coordinators alike.
        generic = await claim_next_pending(
            self._coordinator,
            kind=HOSTED_EPISODE_ACTIVITY_KIND,
            world_id=world_id,
            owner=owner,
            lease_seconds=self._lease_seconds,
            page_size=_CLAIM_SCAN_PAGE,
        )
        if generic is None:
            return None
        return self._remember(generic)

    async def bind_provider_operation(
        self,
        claim: HostedEpisodeActivityClaim,
        *,
        provider: str,
        operation_id: str,
    ) -> HostedEpisodeActivityClaim:
        generic = await self._coordinator.bind_provider_operation(
            self._resolve(claim),
            provider,
            operation_id,
        )
        return self._remember(generic)

    async def confirm_provider_operation_absent(
        self,
        claim: HostedEpisodeActivityClaim,
        guard: HostedEpisodeRetryGuard,
    ) -> HostedEpisodeActivityClaim:
        generic = await self._coordinator.confirm_provider_operation_absent(
            self._resolve(claim),
            ActivityRetryGuard(ref=guard.ref, digest=guard.digest),
            lease_seconds=self._lease_seconds,
        )
        return self._remember(generic)

    async def record_episode_result(
        self,
        claim: HostedEpisodeActivityClaim,
        result: HostedEpisodeActivityResultRef,
    ) -> None:
        await self._coordinator.record_result(
            self._resolve(claim),
            ActivityResultRef(
                ref=result.ref,
                digest=result.digest,
                media_type=result.media_type,
                size_bytes=result.size_bytes,
            ),
        )

    async def pending_episode_results(
        self,
        *,
        world_id: str,
    ) -> tuple[HostedEpisodeResultDelivery, ...]:
        # Page to exhaustion: the default 100-row prefix skipped durable
        # results beyond it, so an observation committed on the current
        # receipt could miss its delivery when a world held more than one
        # page of unobserved results — the results-side twin of the
        # claim_episode stranding fix.
        snapshots = await collect_pending_results(
            self._coordinator,
            kind=HOSTED_EPISODE_ACTIVITY_KIND,
            world_id=world_id,
            page_size=_CLAIM_SCAN_PAGE,
        )
        deliveries: list[HostedEpisodeResultDelivery] = []
        for snapshot in snapshots:
            result = snapshot.result
            if result is None:
                raise AssertionError("pending hosted Activity has no result")
            admission = snapshot.admission
            deliveries.append(
                HostedEpisodeResultDelivery(
                    world_id=admission.source.world_id,
                    activity_id=admission.activity_id,
                    request=HostedEpisodeRequestIdentity(
                        ref=admission.input_ref,
                        digest=admission.input_digest,
                    ),
                    result=HostedEpisodeActivityResultRef(
                        ref=result.ref,
                        digest=result.digest,
                        media_type=result.media_type,
                        size_bytes=result.size_bytes,
                    ),
                )
            )
        return tuple(deliveries)

    async def episode_result(
        self,
        *,
        world_id: str,
        activity_id: str,
    ) -> HostedEpisodeResultDelivery | None:
        snapshot = await self._coordinator.get(
            world_id,
            HOSTED_EPISODE_ACTIVITY_KIND,
            activity_id,
        )
        if snapshot is None or snapshot.result is None:
            return None
        return HostedEpisodeResultDelivery(
            world_id=snapshot.admission.source.world_id,
            activity_id=snapshot.admission.activity_id,
            request=HostedEpisodeRequestIdentity(
                ref=snapshot.admission.input_ref,
                digest=snapshot.admission.input_digest,
            ),
            result=HostedEpisodeActivityResultRef(
                ref=snapshot.result.ref,
                digest=snapshot.result.digest,
                media_type=snapshot.result.media_type,
                size_bytes=snapshot.result.size_bytes,
            ),
        )

    async def settle_episode_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        result_digest: str,
        receipt: CommittedTickReceipt,
    ) -> None:
        await self._coordinator.settle_observation(
            world_id,
            HOSTED_EPISODE_ACTIVITY_KIND,
            activity_id,
            ActivitySettlement(receipt=receipt, result_digest=result_digest),
        )

    async def episode_settled(
        self,
        *,
        world_id: str,
        activity_id: str,
        result_digest: str,
    ) -> bool:
        snapshot = await self._coordinator.get(
            world_id,
            HOSTED_EPISODE_ACTIVITY_KIND,
            activity_id,
        )
        if snapshot is None or snapshot.settlement is None:
            return False
        return snapshot.settlement.result_digest == result_digest

    async def has_unsettled_work(self, world_id: str) -> bool:
        return await self._coordinator.has_unsettled(world_id)

    def _remember(self, claim: ActivityClaim) -> HostedEpisodeActivityClaim:
        if not claim.acquired or claim.attempt is None or claim.fence is None:
            raise ValueError("hosted adapter requires an acquired generic claim")
        operation_id = (
            claim.reconciles_provider_operation_id
            if claim.reconciliation_required
            else claim.provider_operation_id
        )
        provider = claim.reconciles_provider if claim.reconciliation_required else claim.provider
        semantic = HostedEpisodeActivityClaim(
            world_id=claim.world_id,
            activity_id=claim.activity_id,
            attempt=claim.attempt,
            fence=claim.fence,
            request=HostedEpisodeRequestIdentity(
                ref=claim.snapshot.admission.input_ref,
                digest=claim.snapshot.admission.input_digest,
            ),
            provider=provider or "",
            provider_operation_id=operation_id or "",
            reconciliation_required=claim.reconciliation_required,
            retry_guard=(
                HostedEpisodeRetryGuard(
                    ref=claim.retry_guard.ref,
                    digest=claim.retry_guard.digest,
                )
                if claim.retry_guard is not None
                else None
            ),
        )
        self._claims[(claim.world_id, claim.activity_id, claim.fence)] = claim
        return semantic

    def _resolve(self, claim: HostedEpisodeActivityClaim) -> ActivityClaim:
        try:
            return self._claims[(claim.world_id, claim.activity_id, claim.fence)]
        except KeyError:
            raise ValueError("hosted Activity claim was not issued by this adapter") from None

    @staticmethod
    def _validate_existing(
        existing: ActivityAdmission,
        candidate: ActivityAdmission,
    ) -> None:
        if (
            existing.kind != candidate.kind
            or existing.input_ref != candidate.input_ref
            or existing.input_digest != candidate.input_digest
        ):
            raise ActivityConflictError(
                "hosted Activity identity has different immutable request content"
            )


class PhysicalHostedActivityWorker:
    """Execute/reconcile one episode and redeliver its factual observation."""

    def __init__(
        self,
        *,
        world_id: str,
        owner: str,
        catalog: PhysicalHostedActivityCatalog,
        values: PhysicalHostedValueStore,
        provider: HostedEpisodeProvider,
        stager: PhysicalHostedObservationStager,
    ) -> None:
        if not world_id.strip() or not owner.strip():
            raise ValueError("hosted worker world and owner cannot be empty")
        if not provider.provider.strip():
            raise ValueError("hosted provider identity cannot be empty")
        self._world_id = world_id
        self._owner = owner
        self._catalog = catalog
        self._values = values
        self._provider = provider
        self._stager = stager

    async def run_once(self, *, activity_id: str | None = None) -> bool:
        """Deliver durable results, then execute one claim.

        ``activity_id`` scopes the claim to one exact admitted Activity so an
        operation-driven call never executes another episode's older pending
        claim; ``None`` keeps drain semantics and claims the next pending
        episode in the world.
        """

        progressed = await self._deliver_pending_results()
        claim = await self._catalog.claim_episode(
            world_id=self._world_id,
            owner=self._owner,
            activity_id=activity_id,
        )
        if claim is None:
            return progressed
        if claim.world_id != self._world_id:
            raise ValueError("hosted catalog returned another world's claim")
        if activity_id is not None and claim.activity_id != activity_id:
            raise ValueError("hosted catalog returned another Activity's claim")
        request = await self._load_claim_request(claim)
        result_claim, provider_result = await self._execute_or_reconcile(
            claim,
            request,
        )
        validate_hosted_provider_result(
            provider_result,
            request_ipc=request,
            operation_id=result_claim.provider_operation_id,
        )
        published = await self._values.publish_result(
            self._request_ref(claim.request, request),
            provider_result,
        )
        await self._catalog.record_episode_result(
            result_claim,
            published.activity_result,
        )
        await self._deliver_pending_results()
        return True

    async def _execute_or_reconcile(
        self,
        claim: HostedEpisodeActivityClaim,
        request_ipc: bytes,
    ) -> tuple[HostedEpisodeActivityClaim, HostedEpisodeProviderResult]:
        if claim.provider_operation_id:
            if claim.provider != self._provider.provider:
                raise ValueError("hosted claim belongs to another provider adapter")
            reconciliation = await self._provider.reconcile(
                operation_id=claim.provider_operation_id,
                request_ipc=request_ipc,
            )
            if isinstance(reconciliation, HostedEpisodeRecovered):
                return claim, reconciliation.result
            if isinstance(reconciliation, HostedEpisodeConfirmedAbsent):
                fresh = await self._catalog.confirm_provider_operation_absent(
                    claim,
                    reconciliation.guard,
                )
                if fresh.provider_operation_id or fresh.reconciliation_required:
                    raise RuntimeError("confirmed absence did not produce an unbound fresh claim")
                if fresh.retry_guard != reconciliation.guard:
                    raise RuntimeError("catalog returned another hosted retry guard")
                return await self._bind_and_execute(
                    fresh,
                    request_ipc,
                    operation_id=claim.provider_operation_id,
                )
            if isinstance(reconciliation, HostedEpisodeRecoveryUnknown):
                raise HostedEpisodeReconciliationRequired(
                    claim.activity_id,
                    claim.provider_operation_id,
                    reconciliation.reason,
                )
            raise TypeError("hosted provider returned invalid reconciliation evidence")

        if claim.reconciliation_required:
            raise AssertionError("hosted reconciliation claim lacks provider identity")
        operation_id = hosted_episode_provider_operation_id(
            claim.world_id,
            claim.activity_id,
        )
        return await self._bind_and_execute(
            claim,
            request_ipc,
            operation_id=operation_id,
        )

    async def _bind_and_execute(
        self,
        claim: HostedEpisodeActivityClaim,
        request_ipc: bytes,
        *,
        operation_id: str,
    ) -> tuple[HostedEpisodeActivityClaim, HostedEpisodeProviderResult]:
        bound = await self._catalog.bind_provider_operation(
            claim,
            provider=self._provider.provider,
            operation_id=operation_id,
        )
        if bound.provider != self._provider.provider or bound.provider_operation_id != operation_id:
            raise RuntimeError("catalog returned another hosted provider identity")
        result = await self._provider.execute(
            operation_id=operation_id,
            request_ipc=request_ipc,
            attempt=bound.attempt,
            fence=bound.fence,
            retry_guard=bound.retry_guard,
        )
        return bound, result

    async def _deliver_pending_results(self) -> bool:
        progressed = False
        for delivery in await self._catalog.pending_episode_results(
            world_id=self._world_id,
        ):
            if delivery.world_id != self._world_id:
                raise ValueError("hosted catalog returned another world's result")
            published = await self._values.get_result(delivery.result)
            expected_operation = hosted_episode_provider_operation_id(
                delivery.world_id,
                delivery.activity_id,
            )
            if (
                published.operation_id != expected_operation
                or published.request.ref != delivery.request.ref
                or published.request.digest != delivery.request.digest
            ):
                raise ValueError("hosted result delivery does not match its Activity")
            await self._stager.stage_hosted_episode_observation(
                world_id=delivery.world_id,
                observation=published.observation(delivery.activity_id),
            )
            progressed = True
        return progressed

    async def _load_claim_request(self, claim: HostedEpisodeActivityClaim) -> bytes:
        request = await self._values.get_request(claim.request)
        self._validate_claim_request(claim, request)
        return request

    @staticmethod
    def _request_ref(
        value: HostedEpisodeRequestIdentity,
        request_ipc: bytes,
    ) -> HostedEpisodeRequestRef:
        return HostedEpisodeRequestRef(
            ref=value.ref,
            digest=value.digest,
            size_bytes=len(request_ipc),
        )

    @staticmethod
    def _validate_claim_request(
        claim: HostedEpisodeActivityClaim,
        request_ipc: bytes,
    ) -> None:
        operation_id = hosted_episode_provider_operation_id(
            claim.world_id,
            claim.activity_id,
        )
        if hosted_episode_request_digest(request_ipc) != claim.request.digest or any(
            row["operation_id"] != operation_id
            for row in decode_hosted_episode_requests(request_ipc)
        ):
            raise ValueError("claimed hosted Activity does not match its request")


__all__ = [
    "CommittedPhysicalSnapshot",
    "HostedEpisodeActivityClaim",
    "HostedEpisodeReconciliationRequired",
    "HostedEpisodeResultDelivery",
    "PhysicalCommittedIntentReader",
    "PhysicalHostedActivityCatalog",
    "PhysicalHostedActivityCoordinator",
    "PhysicalHostedActivityProjector",
    "PhysicalHostedActivityWorker",
    "PhysicalHostedObservationStager",
    "PhysicalHostedValueStore",
    "prepare_hosted_episode_intent",
]
