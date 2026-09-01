# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Claim-free ECS admission and settlement for Temporal-owned Mission jobs."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from archetype.activities import (
    ActivityAdmission,
    ActivityConflictError,
    ActivityExecutionIdentity,
    ActivityResultRef,
    ActivitySettlement,
    ActivitySnapshot,
    collect_pending_results,
    iActivitySettlementIndex,
)
from archetype.core.interfaces import CommittedTickReceipt
from archetype.errors import AvailabilityError
from archetype.missions.activities import (
    AUTHOR_ACTIVITY_KIND,
    AuthorActivityRequestRef,
    AuthorActivityResultRef,
    DurableAuthorExecutionObservation,
    author_provider_operation_id,
)
from archetype.missions.activity_world import (
    StorageMissionCommittedIntentReader,
    WorldMissionAuthorObservationStager,
)
from archetype.missions.author_activity import (
    AuthorActivityResultDelivery,
    MissionAuthorActivityProjector,
)
from archetype.missions.critic_activity import (
    CriticActivityResultDelivery,
    MissionCriticActivityProjector,
)
from archetype.missions.critic_activity_world import (
    WorldMissionCriticObservationStager,
)
from archetype.missions.critics import (
    CRITIC_ACTIVITY_KIND,
    CRITIC_ACTIVITY_MEDIA_TYPE,
    CriticActivityRequestRef,
    CriticActivityResultRef,
    critic_provider_operation_id,
)
from archetype.world.simulation import RequiredProjector

from .activity_values import (
    MissionModalActivityValueStore,
    MissionModalAuthorValueStore,
    MissionModalCriticValueStore,
)
from .contracts import (
    MissionJobValueRef,
    MissionModalJobFamily,
    MissionModalJobWorkflowInput,
    MissionModalJobWorkflowState,
    mission_modal_job_workflow_id,
)
from .modal_job_client import (
    MissionModalJobWorkflowHandle,
    MissionModalJobWorkflowLauncher,
)

_TEMPORAL_PROVIDER = "temporal"
_AUTHOR_RESULT_MEDIA_TYPE = "application/json"
_RESULT_SCAN_PAGE = 1_000


class MissionTemporalActivityUnavailable(AvailabilityError):
    """A durable provider job cannot yield replay authority for an ECS result."""

    public_detail = "Mission provider activity is temporarily unavailable"

    def __init__(self, family: MissionModalJobFamily, activity_id: str, reason: str) -> None:
        self.family = family
        self.activity_id = activity_id
        self.reason = reason
        super().__init__(
            f"Mission {family} activity {activity_id!r} has no replay-safe result: {reason}"
        )


@dataclass(frozen=True, slots=True)
class _StartedWorkflow:
    activity_id: str
    command: MissionModalJobWorkflowInput
    execution: ActivityExecutionIdentity
    handle: MissionModalJobWorkflowHandle


class _MissionTemporalFamilyCatalog:
    """Shared admission-to-Workflow handoff for one Mission family."""

    def __init__(
        self,
        *,
        family: MissionModalJobFamily,
        kind: str,
        result_media_type: str,
        index: iActivitySettlementIndex,
        workflows: MissionModalJobWorkflowLauncher,
        namespace_digest: str,
    ) -> None:
        if family not in {"author", "critic"}:
            raise ValueError("Mission Temporal activity family is invalid")
        if not kind.strip():
            raise ValueError("Mission Temporal activity kind cannot be empty")
        if len(namespace_digest) != 64 or any(
            character not in "0123456789abcdef" for character in namespace_digest
        ):
            raise ValueError("Mission Temporal activity namespace digest is invalid")
        self.family = family
        self.kind = kind
        self._result_media_type = result_media_type
        self._index = index
        self._workflows = workflows
        self._namespace_digest = namespace_digest
        self._started: dict[str, _StartedWorkflow] = {}

    async def admit_and_start(
        self,
        *,
        world_id: str,
        receipt: CommittedTickReceipt,
        activity_id: str,
        request: MissionJobValueRef,
        provider_operation_id: str,
    ) -> None:
        if world_id != receipt.world_id:
            raise ValueError("Mission Temporal activity admission belongs to another world")
        workflow_id = mission_modal_job_workflow_id(
            self.family,
            provider_operation_id,
            self._namespace_digest,
        )
        execution = ActivityExecutionIdentity(
            provider=_TEMPORAL_PROVIDER,
            operation_id=workflow_id,
        )
        admission = ActivityAdmission(
            activity_id=activity_id,
            kind=self.kind,
            source=receipt,
            input_ref=request.ref,
            input_digest=request.digest,
        )
        existing = await self._index.get(world_id, self.kind, activity_id)
        if existing is None:
            try:
                existing = await self._index.admit(admission, execution)
            except ActivityConflictError:
                existing = await self._index.get(world_id, self.kind, activity_id)
                if existing is None:
                    raise
        self._validate_existing(existing, admission, execution)

        command = MissionModalJobWorkflowInput(
            family=self.family,
            operation_id=provider_operation_id,
            request=request,
            namespace_digest=self._namespace_digest,
        )
        # Admission is committed before Workflow start. If the process dies
        # after Temporal accepts this start but before returning, projection
        # repeats the same deterministic Workflow ID and immutable digest.
        handle = await self._workflows.start(command)
        self._started[activity_id] = _StartedWorkflow(
            activity_id=activity_id,
            command=command,
            execution=execution,
            handle=handle,
        )

    async def complete_started(self, *, world_id: str) -> bool:
        progressed = False
        for activity_id, started in tuple(sorted(self._started.items())):
            state = await started.handle.result()
            result = self._require_result(started, state)
            await self._index.record_orchestrated_result(
                world_id,
                self.kind,
                activity_id,
                started.execution,
                ActivityResultRef(
                    ref=result.ref,
                    digest=result.digest,
                    media_type=self._result_media_type,
                    size_bytes=result.size_bytes,
                ),
            )
            self._started.pop(activity_id, None)
            progressed = True
        return progressed

    async def pending_results(self, *, world_id: str) -> tuple[ActivitySnapshot, ...]:
        return await collect_pending_results(
            self._index,
            kind=self.kind,
            world_id=world_id,
            page_size=_RESULT_SCAN_PAGE,
        )

    async def settle(
        self,
        *,
        world_id: str,
        activity_id: str,
        result_digest: str,
        receipt: CommittedTickReceipt,
    ) -> None:
        await self._index.settle_observation(
            world_id,
            self.kind,
            activity_id,
            ActivitySettlement(receipt=receipt, result_digest=result_digest),
        )

    async def has_unsettled_work(self, world_id: str) -> bool:
        return await self._index.has_unsettled(world_id)

    @staticmethod
    def _validate_existing(
        existing: ActivitySnapshot,
        candidate: ActivityAdmission,
        execution: ActivityExecutionIdentity,
    ) -> None:
        admission = existing.admission
        if (
            admission.kind != candidate.kind
            or admission.input_ref != candidate.input_ref
            or admission.input_digest != candidate.input_digest
        ):
            raise ActivityConflictError(
                "Mission Temporal activity identity has different immutable request content"
            )
        if existing.execution != execution:
            raise ActivityConflictError(
                "Mission Temporal activity identity has different orchestration authority"
            )

    def _require_result(
        self,
        started: _StartedWorkflow,
        state: MissionModalJobWorkflowState,
    ) -> MissionJobValueRef:
        command = started.command
        if (
            state.family != command.family
            or state.operation_id != command.operation_id
            or state.request_digest != command.request.digest
        ):
            raise ActivityConflictError(
                "Mission Temporal Workflow returned another immutable activity identity"
            )
        if state.status != "succeeded" or state.result is None:
            reason = state.failure_reason or f"Workflow terminated as {state.status}"
            raise MissionTemporalActivityUnavailable(
                self.family,
                started.activity_id,
                reason,
            )
        return state.result


class MissionTemporalAuthorActivityCatalog:
    """Author projector port with admission-time Temporal ownership."""

    def __init__(
        self,
        *,
        index: iActivitySettlementIndex,
        workflows: MissionModalJobWorkflowLauncher,
        values: MissionModalActivityValueStore,
        namespace_digest: str,
    ) -> None:
        self._family = _MissionTemporalFamilyCatalog(
            family="author",
            kind=AUTHOR_ACTIVITY_KIND,
            result_media_type=_AUTHOR_RESULT_MEDIA_TYPE,
            index=index,
            workflows=workflows,
            namespace_digest=namespace_digest,
        )
        self._values = values

    async def admit_author(
        self,
        *,
        world_id: str,
        receipt: CommittedTickReceipt,
        activity_id: str,
        request: AuthorActivityRequestRef,
    ) -> None:
        await self._family.admit_and_start(
            world_id=world_id,
            receipt=receipt,
            activity_id=activity_id,
            request=await self._values.author_request(request),
            provider_operation_id=author_provider_operation_id(world_id, activity_id),
        )

    async def complete_started(self, *, world_id: str) -> bool:
        return await self._family.complete_started(world_id=world_id)

    async def pending_author_results(
        self,
        *,
        world_id: str,
    ) -> tuple[AuthorActivityResultDelivery, ...]:
        deliveries: list[AuthorActivityResultDelivery] = []
        for snapshot in await self._family.pending_results(world_id=world_id):
            if snapshot.result is None:
                raise AssertionError("pending author Activity result has no result reference")
            admission = snapshot.admission
            deliveries.append(
                AuthorActivityResultDelivery(
                    world_id=admission.source.world_id,
                    activity_id=admission.activity_id,
                    request=AuthorActivityRequestRef(
                        ref=admission.input_ref,
                        digest=admission.input_digest,
                    ),
                    result=AuthorActivityResultRef(
                        ref=snapshot.result.ref,
                        digest=snapshot.result.digest,
                        media_type=snapshot.result.media_type,
                        size_bytes=snapshot.result.size_bytes,
                    ),
                )
            )
        return tuple(deliveries)

    async def settle_author_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        result_digest: str,
        receipt: CommittedTickReceipt,
    ) -> None:
        await self._family.settle(
            world_id=world_id,
            activity_id=activity_id,
            result_digest=result_digest,
            receipt=receipt,
        )

    async def has_unsettled_work(self, world_id: str) -> bool:
        return await self._family.has_unsettled_work(world_id)


class MissionTemporalCriticActivityCatalog:
    """Critic projector port with admission-time Temporal ownership."""

    def __init__(
        self,
        *,
        index: iActivitySettlementIndex,
        workflows: MissionModalJobWorkflowLauncher,
        values: MissionModalActivityValueStore,
        namespace_digest: str,
    ) -> None:
        self._family = _MissionTemporalFamilyCatalog(
            family="critic",
            kind=CRITIC_ACTIVITY_KIND,
            result_media_type=CRITIC_ACTIVITY_MEDIA_TYPE,
            index=index,
            workflows=workflows,
            namespace_digest=namespace_digest,
        )
        self._values = values

    async def admit_critic(
        self,
        *,
        world_id: str,
        receipt: CommittedTickReceipt,
        activity_id: str,
        request: CriticActivityRequestRef,
    ) -> None:
        await self._family.admit_and_start(
            world_id=world_id,
            receipt=receipt,
            activity_id=activity_id,
            request=await self._values.critic_request(request),
            provider_operation_id=critic_provider_operation_id(world_id, activity_id),
        )

    async def complete_started(self, *, world_id: str) -> bool:
        return await self._family.complete_started(world_id=world_id)

    async def pending_critic_results(
        self,
        *,
        world_id: str,
    ) -> tuple[CriticActivityResultDelivery, ...]:
        deliveries: list[CriticActivityResultDelivery] = []
        for snapshot in await self._family.pending_results(world_id=world_id):
            if snapshot.result is None:
                raise AssertionError("pending critic Activity result has no result reference")
            admission = snapshot.admission
            deliveries.append(
                CriticActivityResultDelivery(
                    world_id=admission.source.world_id,
                    activity_id=admission.activity_id,
                    request=CriticActivityRequestRef(
                        ref=admission.input_ref,
                        digest=admission.input_digest,
                    ),
                    result=CriticActivityResultRef(
                        ref=snapshot.result.ref,
                        digest=snapshot.result.digest,
                        media_type=snapshot.result.media_type,
                        size_bytes=snapshot.result.size_bytes,
                    ),
                )
            )
        return tuple(deliveries)

    async def settle_critic_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        result_digest: str,
        receipt: CommittedTickReceipt,
    ) -> None:
        await self._family.settle(
            world_id=world_id,
            activity_id=activity_id,
            result_digest=result_digest,
            receipt=receipt,
        )

    async def has_unsettled_work(self, world_id: str) -> bool:
        return await self._family.has_unsettled_work(world_id)


class MissionTemporalActivityWorker:
    """Await exact Workflows, record their refs, and stage ECS observations."""

    def __init__(
        self,
        *,
        world_id: str,
        author: MissionTemporalAuthorActivityCatalog,
        critic: MissionTemporalCriticActivityCatalog,
        author_values: MissionModalAuthorValueStore,
        critic_values: MissionModalCriticValueStore,
        author_stager: WorldMissionAuthorObservationStager,
        critic_stager: WorldMissionCriticObservationStager,
    ) -> None:
        if not world_id.strip():
            raise ValueError("Mission Temporal Activity worker requires a world identity")
        self._world_id = world_id
        self._author = author
        self._critic = critic
        self._author_values = author_values
        self._critic_values = critic_values
        self._author_stager = author_stager
        self._critic_stager = critic_stager

    async def run_once(self) -> bool:
        return await self.run_until_idle()

    async def run_until_idle(self) -> bool:
        progressed = await self._author.complete_started(world_id=self._world_id)
        progressed = await self._critic.complete_started(world_id=self._world_id) or progressed
        progressed = await self._deliver_author_results() or progressed
        return await self._deliver_critic_results() or progressed

    async def _deliver_author_results(self) -> bool:
        progressed = False
        for delivery in await self._author.pending_author_results(world_id=self._world_id):
            if delivery.world_id != self._world_id:
                raise ValueError("author Activity catalog returned another world's result")
            request = await self._author_values.get_request(delivery.request)
            if request.dispatch_id != delivery.activity_id:
                raise ValueError("author result delivery does not match its request")
            observation = await self._author_values.get_result(delivery.result)
            self._validate_author_observation(delivery.activity_id, observation)
            await self._author_stager.stage_author_observation(
                world_id=delivery.world_id,
                activity_id=delivery.activity_id,
                request=request,
                result=delivery.result,
                observation=observation,
            )
            progressed = True
        return progressed

    async def _deliver_critic_results(self) -> bool:
        progressed = False
        for delivery in await self._critic.pending_critic_results(world_id=self._world_id):
            if delivery.world_id != self._world_id:
                raise ValueError("critic Activity catalog returned another world's result")
            request = await self._critic_values.get_request(delivery.request)
            if request.review_id != delivery.activity_id:
                raise ValueError("critic result delivery does not match its request")
            result = await self._critic_values.get_result(delivery.result)
            if result.review_id != delivery.activity_id:
                raise ValueError("critic Activity result does not match its activity")
            await self._critic_stager.stage_critic_observation(
                world_id=delivery.world_id,
                activity_id=delivery.activity_id,
                request=request,
                result=delivery.result,
                observation=result,
            )
            progressed = True
        return progressed

    @staticmethod
    def _validate_author_observation(
        activity_id: str,
        observation: DurableAuthorExecutionObservation,
    ) -> None:
        if observation.result.dispatch_id != activity_id:
            raise ValueError("author Activity result does not match its activity")


class MissionTemporalActivityBinding:
    """Bind one Mission world to claim-free Temporal execution and settlement."""

    def __init__(
        self,
        *,
        world_id: str,
        reader: StorageMissionCommittedIntentReader,
        author: MissionTemporalAuthorActivityCatalog,
        critic: MissionTemporalCriticActivityCatalog,
        author_values: MissionModalAuthorValueStore,
        critic_values: MissionModalCriticValueStore,
        author_stager: WorldMissionAuthorObservationStager,
        critic_stager: WorldMissionCriticObservationStager,
        close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if not world_id.strip():
            raise ValueError("Mission Temporal Activity binding requires a world identity")
        self.world_id = world_id
        self.author_projector = MissionAuthorActivityProjector(
            reader=reader,
            catalog=author,
            values=author_values,
        )
        self.critic_projector = MissionCriticActivityProjector(
            reader=reader,
            catalog=critic,
            values=critic_values,
        )
        self.worker = MissionTemporalActivityWorker(
            world_id=world_id,
            author=author,
            critic=critic,
            author_values=author_values,
            critic_values=critic_values,
            author_stager=author_stager,
            critic_stager=critic_stager,
        )
        self.required_projector = RequiredProjector(
            consumer_name="missions.temporal-activities",
            project=self._project,
        )
        self._author = author
        self._critic = critic
        self._close = close
        self._closed = False

    async def _project(self, receipt: CommittedTickReceipt) -> None:
        await self.author_projector.project(receipt)
        await self.critic_projector.project(receipt)

    def required_projector_for(self, world_id: str) -> RequiredProjector | None:
        return self.required_projector if str(world_id) == self.world_id else None

    async def has_unsettled_work(self, world_id: str) -> bool:
        if str(world_id) != self.world_id:
            return False
        return await self._author.has_unsettled_work(
            world_id
        ) or await self._critic.has_unsettled_work(world_id)

    async def aclose(self) -> None:
        if self._closed:
            return
        if self._close is not None:
            await self._close()
        self._closed = True


__all__ = [
    "MissionTemporalActivityBinding",
    "MissionTemporalActivityUnavailable",
    "MissionTemporalActivityWorker",
    "MissionTemporalAuthorActivityCatalog",
    "MissionTemporalCriticActivityCatalog",
]
