# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Concrete world adapters for the Mission author Activity boundary."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Mapping

from daft import DataFrame

from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.interfaces import ArchetypeSignature, CommittedTickReceipt
from archetype.missions.activities import (
    AuthorActivityEntityFact,
    AuthorActivityResultRef,
    DurableAuthorExecutionObservation,
    MissionAuthorExecutor,
    complete_author_activity_fact_bundle,
    complete_author_activity_fact_count,
)
from archetype.missions.coding_agents.contracts import TaskDispatchRequest
from archetype.missions.components import (
    AgentExecution,
    AuthorActivityObservation,
    Candidate,
    Checkpoint,
    Commit,
    CompleteAuthorActivityObservation,
    CriticFinding,
    FrictionLog,
    Sandbox,
    Task,
    TaskCriticPolicy,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    TaskWorkspace,
    ValidationResult,
)
from archetype.missions.projections import (
    COMPLETE_AUTHOR_ACTIVITY_FACT_TYPES,
    ProjectedAuthorActivityFactBundle,
    project_complete_author_activity_fact_bundles,
    reconstruct_complete_author_activity_fact_bundle,
)
from archetype.missions.relations import (
    AuthoredBy,
    CandidateFor,
    Executes,
    Guards,
    PartOfMission,
    ProducedBy,
    RunsIn,
    Supersedes,
)
from archetype.projections import latest
from archetype.storage.catalog import SignatureRecord
from archetype.storage.interfaces import iStorageService
from archetype.world.interfaces import iWorldRegistry
from archetype.world.mutation import (
    _create_entities_atomically_locked,
    pending_components_locked,
    preview_entity_ids_locked,
)
from archetype.world.query import (
    PinnedWorldQuerySnapshot,
    pin_query_snapshot,
    pin_query_snapshot_for_receipt,
    query_components,
)
from archetype.world.simulation import RequiredProjector

from .activities import (
    CommittedMissionSnapshot,
    MissionAuthorActivityCatalog,
    MissionAuthorActivityProjector,
    MissionAuthorActivityWorker,
    MissionAuthorValueStore,
)

_MISSION_QUERY_GROUPS: tuple[tuple[type[Component], ...], ...] = (
    (
        Task,
        TaskWorkspace,
        TaskPolicy,
        TaskCriticPolicy,
        TaskState,
        TaskDispatch,
    ),
    (PartOfMission,),
    (TaskValidator,),
    (Guards,),
    (AgentExecution,),
    (ValidationResult,),
    (Commit,),
    (Checkpoint,),
    (FrictionLog,),
    (Sandbox,),
    (Candidate,),
    (CriticFinding,),
    (AuthorActivityObservation,),
    (CompleteAuthorActivityObservation,),
    (Executes,),
    (RunsIn,),
    (ProducedBy,),
    (CandidateFor,),
    (AuthoredBy,),
    (Supersedes,),
)


async def _query_groups(
    storage: iStorageService,
    storage_config: StorageConfig,
    snapshot: PinnedWorldQuerySnapshot,
    groups: tuple[tuple[type[Component], ...], ...],
) -> Mapping[ArchetypeSignature, DataFrame]:
    records = await storage.get_control_catalog(storage_config).list_signatures()
    results: dict[ArchetypeSignature, DataFrame] = {}
    for group in groups:
        if not _has_recorded_group(records, group):
            continue
        frame = await query_components(
            storage,
            list(group),
            snapshot.world_id,
            snapshot.run_id,
            storage_config,
            snapshot=snapshot,
        )
        results[group] = await storage.materialize(latest(frame))
    return results


def _has_recorded_group(
    records: list[SignatureRecord],
    group: tuple[type[Component], ...],
) -> bool:
    requested = {component.__name__ for component in group}
    return any(requested.issubset(record.component_names) for record in records)


class StorageMissionCommittedIntentReader:
    """Read one Mission view only from the exact manifest receipt being projected."""

    def __init__(
        self,
        storage: iStorageService,
        storage_config: StorageConfig | None = None,
    ) -> None:
        self._storage = storage
        self._storage_config = storage_config or StorageConfig()

    async def read(self, receipt: CommittedTickReceipt) -> CommittedMissionSnapshot:
        snapshot = await pin_query_snapshot_for_receipt(
            self._storage,
            receipt,
            self._storage_config,
        )
        if snapshot.head_tick is None:
            raise AssertionError("receipt-pinned snapshot has no committed head")
        return CommittedMissionSnapshot(
            world_id=snapshot.world_id,
            run_id=snapshot.run_id,
            committed_tick=snapshot.head_tick,
            visibility_token=snapshot.head_tokens[0],
            results=await _query_groups(
                self._storage,
                self._storage_config,
                snapshot,
                _MISSION_QUERY_GROUPS,
            ),
        )


def _component_from_row(
    component_type: type[Component],
    row: dict[str, object],
) -> Component:
    prefix = component_type.get_prefix()
    return component_type(
        **{field: row[f"{prefix}{field}"] for field in component_type.model_fields}
    )


def _pending_component_facts(
    world: AsyncWorld,
    component_type: type[Component],
) -> tuple[AuthorActivityEntityFact, ...]:
    return tuple(
        AuthorActivityEntityFact(entity_id=entity_id, component=component)
        for entity_id, component in pending_components_locked(world, component_type)
    )


def _pending_author_bundle(
    world: AsyncWorld,
    activity_id: str,
) -> ProjectedAuthorActivityFactBundle | None:
    markers = tuple(
        fact
        for fact in _pending_component_facts(world, CompleteAuthorActivityObservation)
        if isinstance(fact.component, CompleteAuthorActivityObservation)
        and fact.component.activity_id == activity_id
    )
    if not markers:
        return None
    if len(markers) != 1:
        raise ValueError("author activity has multiple pending completion markers")
    marker = markers[0].component
    assert isinstance(marker, CompleteAuthorActivityObservation)
    facts_by_type = {
        component_type: _pending_component_facts(world, component_type)
        for component_type in COMPLETE_AUTHOR_ACTIVITY_FACT_TYPES
    }
    bundle = reconstruct_complete_author_activity_fact_bundle(marker, facts_by_type)
    if bundle is None:
        raise ValueError("author activity has an incomplete pending fact bundle")
    return ProjectedAuthorActivityFactBundle(marker=marker, bundle=bundle)


def _assert_exact_delivery_bundle(
    projected: ProjectedAuthorActivityFactBundle,
    *,
    request: TaskDispatchRequest,
    result: AuthorActivityResultRef,
    observation: DurableAuthorExecutionObservation,
    prior_candidate_id: int | None,
) -> None:
    candidate_facts = projected.bundle.components(Candidate)
    candidate_created_at_ms = 0
    if candidate_facts:
        if len(candidate_facts) != 1:
            raise ValueError("author activity has multiple result candidates")
        candidate = candidate_facts[0].component
        assert isinstance(candidate, Candidate)
        candidate_created_at_ms = candidate.created_at_ms
    try:
        expected_bundle = complete_author_activity_fact_bundle(
            request,
            observation,
            entity_ids=tuple(sorted(fact.entity_id for fact in projected.bundle.facts)),
            prior_candidate_id=prior_candidate_id,
            candidate_created_at_ms=candidate_created_at_ms,
        )
    except ValueError as exc:
        raise ValueError("author activity fact bundle conflicts with its durable result") from exc
    expected_marker = expected_bundle.marker(
        result=result,
        redaction_policy_id=observation.redaction_policy_id,
    )
    if projected.marker != expected_marker or projected.bundle.digest != expected_bundle.digest:
        raise ValueError("author activity fact bundle conflicts with its durable result")


class WorldMissionAuthorObservationStager:
    """Stage one complete result-derived Mission mutation batch idempotently."""

    def __init__(
        self,
        *,
        storage: iStorageService,
        registry: iWorldRegistry,
    ) -> None:
        self._storage = storage
        self._registry = registry

    async def stage_author_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        request: TaskDispatchRequest,
        result: AuthorActivityResultRef,
        observation: DurableAuthorExecutionObservation,
    ) -> None:
        self._validate_delivery(
            world_id=world_id,
            activity_id=activity_id,
            request=request,
            observation=observation,
        )
        async with self._registry.operation(world_id) as world:
            if not isinstance(world, AsyncWorld):
                raise TypeError("mission author observations require an AsyncWorld")

            storage_record = await self._registry.storage_record(world_id)
            storage_config = storage_record[0] if storage_record is not None else StorageConfig()
            snapshot = await pin_query_snapshot(
                self._storage,
                world_id,
                str(world.run_id),
                storage_config,
            )
            prior_candidate_id = request.prior_candidate_entity_id or None
            if await self._committed_bundle(
                snapshot,
                storage_config,
                request=request,
                result=result,
                observation=observation,
                prior_candidate_id=prior_candidate_id,
            ):
                return

            pending = _pending_author_bundle(world, activity_id)
            if pending is not None:
                _assert_exact_delivery_bundle(
                    pending,
                    request=request,
                    result=result,
                    observation=observation,
                    prior_candidate_id=prior_candidate_id,
                )
                return

            fact_count = complete_author_activity_fact_count(
                request,
                observation,
                prior_candidate_id=prior_candidate_id,
            )
            staged_ids = preview_entity_ids_locked(world, fact_count + 1)
            fact_ids = staged_ids[:-1]
            marker_entity_id = staged_ids[-1]
            bundle = complete_author_activity_fact_bundle(
                request,
                observation,
                entity_ids=fact_ids,
                prior_candidate_id=prior_candidate_id,
                candidate_created_at_ms=int(time.time() * 1000),
            )
            entities = bundle.staged_entities(
                marker_entity_id=marker_entity_id,
                result=result,
                redaction_policy_id=observation.redaction_policy_id,
            )
            await _create_entities_atomically_locked(world, entities)

    async def _committed_bundle(
        self,
        snapshot: PinnedWorldQuerySnapshot,
        storage_config: StorageConfig,
        *,
        request: TaskDispatchRequest,
        result: AuthorActivityResultRef,
        observation: DurableAuthorExecutionObservation,
        prior_candidate_id: int | None,
    ) -> bool:
        records = await self._storage.get_control_catalog(storage_config).list_signatures()
        if not _has_recorded_group(records, (CompleteAuthorActivityObservation,)):
            return False
        results = await _query_groups(
            self._storage,
            storage_config,
            snapshot,
            _MISSION_QUERY_GROUPS,
        )
        marker_frame = results.get((CompleteAuthorActivityObservation,))
        if marker_frame is None:
            raise RuntimeError("recorded author activity marker table was not read")
        materialized_markers = await self._storage.materialize(marker_frame)
        matches: list[CompleteAuthorActivityObservation] = []
        for row in materialized_markers.to_pylist():
            marker = _component_from_row(CompleteAuthorActivityObservation, row)
            assert isinstance(marker, CompleteAuthorActivityObservation)
            if marker.activity_id == request.dispatch_id:
                matches.append(marker)
        if not matches:
            return False
        if len(matches) != 1:
            raise ValueError("author activity has multiple committed completion markers")
        if snapshot.head_tick is None or len(snapshot.head_tokens) != 1:
            raise RuntimeError("committed author marker has no exact manifest head")
        committed = CommittedMissionSnapshot(
            world_id=snapshot.world_id,
            run_id=snapshot.run_id,
            committed_tick=snapshot.head_tick,
            visibility_token=snapshot.head_tokens[0],
            results=results,
        )
        projected = tuple(
            item
            for item in project_complete_author_activity_fact_bundles(committed.as_post_tick())
            if item.marker.activity_id == request.dispatch_id
        )
        if len(projected) != 1:
            raise ValueError("author activity has an incomplete committed fact bundle")
        _assert_exact_delivery_bundle(
            projected[0],
            request=request,
            result=result,
            observation=observation,
            prior_candidate_id=prior_candidate_id,
        )
        return True

    @staticmethod
    def _validate_delivery(
        *,
        world_id: str,
        activity_id: str,
        request: TaskDispatchRequest,
        observation: DurableAuthorExecutionObservation,
    ) -> None:
        result = observation.result
        if not world_id.strip() or activity_id != request.dispatch_id:
            raise ValueError("author delivery does not match its world-local request")
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
        if observed != expected:
            raise ValueError("author delivery result does not match its request")


class MissionAuthorActivityBinding:
    """Bind one exact world to the Activity-backed author choreography.

    Runtime composition installs this binding for supported Modal Missions.
    Maintainer hosts may also pass ``required_projector_for`` and
    ``has_unsettled_work`` into world lifecycle composition explicitly. The
    worker always runs outside tick locks.
    """

    def __init__(
        self,
        *,
        world_id: str,
        owner: str,
        reader: StorageMissionCommittedIntentReader,
        catalog: MissionAuthorActivityCatalog,
        values: MissionAuthorValueStore,
        executor: MissionAuthorExecutor,
        stager: WorldMissionAuthorObservationStager,
        close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if not world_id.strip():
            raise ValueError("mission author binding requires a world identity")
        self.world_id = world_id
        self.projector = MissionAuthorActivityProjector(
            reader=reader,
            catalog=catalog,
            values=values,
        )
        self.required_projector = RequiredProjector(
            consumer_name=self.projector.consumer_name,
            project=self.projector.project,
        )
        self.worker = MissionAuthorActivityWorker(
            world_id=world_id,
            owner=owner,
            catalog=catalog,
            values=values,
            executor=executor,
            stager=stager,
        )
        self._catalog = catalog
        self._close = close
        self._closed = False

    def required_projector_for(self, world_id: str) -> RequiredProjector | None:
        """Bind the sole projector slot only for this explicitly selected world."""

        return self.required_projector if str(world_id) == self.world_id else None

    async def has_unsettled_work(self, world_id: str) -> bool:
        """Report only this binding's exact world to the lifecycle gate."""

        if str(world_id) != self.world_id:
            return False
        return await self._catalog.has_unsettled_work(self.world_id)

    async def aclose(self) -> None:
        """Release the concrete catalog only after its world has closed."""

        if self._closed:
            return
        if self._close is not None:
            await self._close()
        self._closed = True


__all__ = [
    "MissionAuthorActivityBinding",
    "StorageMissionCommittedIntentReader",
    "WorldMissionAuthorObservationStager",
]
