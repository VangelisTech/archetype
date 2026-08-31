# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable author-activity choreography owned by Agent Missions.

This module deliberately defines application ports instead of a second activity
catalog.  Concrete composition adapts these ports to the generic activity
coordinator and durable value storage.  The mission family retains the meaning
of author requests, provider reconciliation, and returned observations.

The concrete receipt-pinned reader, atomic complete fact-bundle stager, and
per-world binding live in :mod:`archetype.missions.activity_world`.
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
from archetype.missions.activities import (
    AuthorActivityRequestRef,
    AuthorActivityResultRef,
    DurableAuthorExecutionObservation,
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
class MissionAuthorActivityProjectionCatalog(Protocol):
    """Admission and settlement facts consumed by the committed projector."""

    async def admit_author(
        self,
        *,
        world_id: str,
        receipt: CommittedTickReceipt,
        activity_id: str,
        request: AuthorActivityRequestRef,
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


@runtime_checkable
class MissionAuthorValueStore(Protocol):
    """Durable mission codec/store; values remain outside the control catalog."""

    async def put_request(self, request: TaskDispatchRequest) -> AuthorActivityRequestRef: ...

    async def get_request(self, value: AuthorActivityRequestRef) -> TaskDispatchRequest: ...

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


class MissionAuthorActivityProjector:
    """Admit committed dispatches and settle committed author observations."""

    consumer_name = "missions.author-activities"

    def __init__(
        self,
        *,
        reader: MissionCommittedIntentReader,
        catalog: MissionAuthorActivityProjectionCatalog,
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


__all__ = [
    "AuthorActivityResultDelivery",
    "CommittedMissionSnapshot",
    "MissionAuthorActivityProjector",
    "MissionAuthorObservationStager",
    "MissionAuthorRedactor",
    "MissionAuthorValueStore",
    "MissionCommittedIntentReader",
]
