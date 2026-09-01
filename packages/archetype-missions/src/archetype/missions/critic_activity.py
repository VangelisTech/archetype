# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable exact-candidate critic Activity choreography owned by Missions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from archetype.core.interfaces import CommittedTickReceipt
from archetype.missions.components import CriticExecution
from archetype.missions.critics import (
    CandidateReviewRequest,
    CriticActivityRequest,
    CriticActivityRequestRef,
    CriticActivityResult,
    CriticActivityResultRef,
    complete_critic_activity_fact_bundle,
)
from archetype.missions.projections import (
    project_complete_critic_activity_fact_bundles,
    project_critic_activity_requests,
)

from .author_activity import CommittedMissionSnapshot, MissionCommittedIntentReader


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
class MissionCriticActivityProjectionCatalog(Protocol):
    """Admission and settlement facts consumed by the committed projector."""

    async def admit_critic(
        self,
        *,
        world_id: str,
        receipt: CommittedTickReceipt,
        activity_id: str,
        request: CriticActivityRequestRef,
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


class MissionCriticActivityProjector:
    """Admit exact-candidate reviews and settle complete committed observations."""

    consumer_name = "missions.critic-activities"

    def __init__(
        self,
        *,
        reader: MissionCommittedIntentReader,
        catalog: MissionCriticActivityProjectionCatalog,
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


__all__ = [
    "CriticActivityResultDelivery",
    "MissionCriticActivityProjector",
    "MissionCriticObservationStager",
    "MissionCriticValueStore",
]
