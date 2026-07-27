# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Concrete world adapters for the Mission critic Activity boundary."""

from __future__ import annotations

import time

from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.missions.components import (
    CompleteCriticActivityObservation,
    CriticExecution,
)
from archetype.missions.critics import (
    CriticActivityEntityFact,
    CriticActivityRequest,
    CriticActivityResult,
    CriticActivityResultRef,
    MissionCriticExecutor,
    complete_critic_activity_fact_bundle,
    complete_critic_activity_fact_count,
)
from archetype.missions.projections import (
    COMPLETE_CRITIC_ACTIVITY_FACT_TYPES,
    ProjectedCriticActivityFactBundle,
    project_complete_critic_activity_fact_bundles,
    reconstruct_complete_critic_activity_fact_bundle,
)
from archetype.storage.interfaces import iStorageService
from archetype.world.interfaces import iWorldRegistry
from archetype.world.mutation import _create_entities_atomically_locked
from archetype.world.query import PinnedWorldQuerySnapshot, pin_query_snapshot
from archetype.world.simulation import RequiredProjector

from .activity_world import (
    _MISSION_QUERY_GROUPS,
    StorageMissionCommittedIntentReader,
    _component_from_row,
    _has_recorded_group,
    _query_groups,
)
from .author_activity import CommittedMissionSnapshot
from .critic_activity import (
    MissionCriticActivityCatalog,
    MissionCriticActivityProjector,
    MissionCriticActivityWorker,
    MissionCriticValueStore,
)


def _pending_component_facts(
    world: AsyncWorld,
    component_type: type[Component],
) -> tuple[CriticActivityEntityFact, ...]:
    facts: list[CriticActivityEntityFact] = []
    prefix = component_type.get_prefix()
    required_columns = {f"{prefix}{field}" for field in component_type.model_fields}
    for signature, rows in world.spawn_cache.items():
        for row in rows:
            if not required_columns.issubset(row):
                continue
            entity_id = int(row["entity_id"])
            if world.entity2sig.get(entity_id) != signature:
                continue
            facts.append(
                CriticActivityEntityFact(
                    entity_id=entity_id,
                    component=_component_from_row(component_type, row),
                )
            )
    return tuple(facts)


def _pending_critic_bundle(
    world: AsyncWorld,
    activity_id: str,
) -> ProjectedCriticActivityFactBundle | None:
    markers = tuple(
        fact
        for fact in _pending_component_facts(world, CompleteCriticActivityObservation)
        if isinstance(fact.component, CompleteCriticActivityObservation)
        and fact.component.activity_id == activity_id
    )
    if not markers:
        return None
    if len(markers) != 1:
        raise ValueError("critic Activity has multiple pending completion markers")
    marker = markers[0].component
    assert isinstance(marker, CompleteCriticActivityObservation)
    facts_by_type = {
        component_type: _pending_component_facts(world, component_type)
        for component_type in COMPLETE_CRITIC_ACTIVITY_FACT_TYPES
    }
    bundle = reconstruct_complete_critic_activity_fact_bundle(marker, facts_by_type)
    if bundle is None:
        raise ValueError("critic Activity has an incomplete pending fact bundle")
    return ProjectedCriticActivityFactBundle(marker=marker, bundle=bundle)


def _assert_exact_delivery_bundle(
    projected: ProjectedCriticActivityFactBundle,
    *,
    request: CriticActivityRequest,
    result: CriticActivityResultRef,
    observation: CriticActivityResult,
) -> None:
    executions = projected.bundle.components(CriticExecution)
    if len(executions) != 1:
        raise ValueError("critic Activity has an invalid execution bundle")
    execution = executions[0].component
    assert isinstance(execution, CriticExecution)
    try:
        expected_bundle = complete_critic_activity_fact_bundle(
            request,
            observation,
            entity_ids=tuple(sorted(fact.entity_id for fact in projected.bundle.facts)),
            receipt_staged_at_ms=execution.receipt_staged_at_ms,
        )
        expected_marker = expected_bundle.marker(
            request=request,
            result=observation,
            result_ref=result,
        )
    except ValueError as exc:
        raise ValueError("critic Activity fact bundle conflicts with its durable result") from exc
    if projected.marker != expected_marker or projected.bundle.digest != expected_bundle.digest:
        raise ValueError("critic Activity fact bundle conflicts with its durable result")


class WorldMissionCriticObservationStager:
    """Stage one complete result-derived critic mutation batch idempotently."""

    def __init__(
        self,
        *,
        storage: iStorageService,
        registry: iWorldRegistry,
    ) -> None:
        self._storage = storage
        self._registry = registry

    async def stage_critic_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        request: CriticActivityRequest,
        result: CriticActivityResultRef,
        observation: CriticActivityResult,
    ) -> None:
        self._validate_delivery(
            world_id=world_id,
            activity_id=activity_id,
            request=request,
            observation=observation,
        )
        async with self._registry.operation(world_id) as world:
            if not isinstance(world, AsyncWorld):
                raise TypeError("mission critic observations require an AsyncWorld")

            storage_record = await self._registry.storage_record(world_id)
            storage_config = storage_record[0] if storage_record is not None else StorageConfig()
            snapshot = await pin_query_snapshot(
                self._storage,
                world_id,
                str(world.run_id),
                storage_config,
            )
            if await self._committed_bundle(
                snapshot,
                storage_config,
                request=request,
                result=result,
                observation=observation,
            ):
                return

            pending = _pending_critic_bundle(world, activity_id)
            if pending is not None:
                _assert_exact_delivery_bundle(
                    pending,
                    request=request,
                    result=result,
                    observation=observation,
                )
                return

            fact_count = complete_critic_activity_fact_count(observation)
            first_entity_id = int(world.next_entity_id)
            fact_ids = tuple(range(first_entity_id, first_entity_id + fact_count))
            marker_entity_id = first_entity_id + fact_count
            bundle = complete_critic_activity_fact_bundle(
                request,
                observation,
                entity_ids=fact_ids,
                receipt_staged_at_ms=(
                    int(time.time() * 1000) if observation.receipt is not None else 0
                ),
            )
            await _create_entities_atomically_locked(
                world,
                bundle.staged_entities(
                    marker_entity_id=marker_entity_id,
                    request=request,
                    result=observation,
                    result_ref=result,
                ),
            )

    async def _committed_bundle(
        self,
        snapshot: PinnedWorldQuerySnapshot,
        storage_config: StorageConfig,
        *,
        request: CriticActivityRequest,
        result: CriticActivityResultRef,
        observation: CriticActivityResult,
    ) -> bool:
        records = await self._storage.get_control_catalog(storage_config).list_signatures()
        if not _has_recorded_group(records, (CompleteCriticActivityObservation,)):
            return False
        results = await _query_groups(
            self._storage,
            storage_config,
            snapshot,
            _MISSION_QUERY_GROUPS,
        )
        marker_frame = results.get((CompleteCriticActivityObservation,))
        if marker_frame is None:
            raise RuntimeError("recorded critic Activity marker table was not read")
        materialized_markers = await self._storage.materialize(marker_frame)
        matches: list[CompleteCriticActivityObservation] = []
        for row in materialized_markers.to_pylist():
            marker = _component_from_row(CompleteCriticActivityObservation, row)
            assert isinstance(marker, CompleteCriticActivityObservation)
            if marker.activity_id == request.review_id:
                matches.append(marker)
        if not matches:
            return False
        if len(matches) != 1:
            raise ValueError("critic Activity has multiple committed completion markers")
        if snapshot.head_tick is None or len(snapshot.head_tokens) != 1:
            raise RuntimeError("committed critic marker has no exact manifest head")
        committed = CommittedMissionSnapshot(
            world_id=snapshot.world_id,
            run_id=snapshot.run_id,
            committed_tick=snapshot.head_tick,
            visibility_token=snapshot.head_tokens[0],
            results=results,
        )
        projected = tuple(
            item
            for item in project_complete_critic_activity_fact_bundles(committed.as_post_tick())
            if item.marker.activity_id == request.review_id
        )
        if len(projected) != 1:
            raise ValueError("critic Activity has an incomplete committed fact bundle")
        _assert_exact_delivery_bundle(
            projected[0],
            request=request,
            result=result,
            observation=observation,
        )
        return True

    @staticmethod
    def _validate_delivery(
        *,
        world_id: str,
        activity_id: str,
        request: CriticActivityRequest,
        observation: CriticActivityResult,
    ) -> None:
        if not world_id.strip() or activity_id != request.review_id:
            raise ValueError("critic delivery does not match its world-local request")
        expected = (
            request.review_id,
            request.domain_review_attempt,
            request.candidate_digest,
            request.policy.digest,
            request.base_revision,
            request.head_revision,
            request.diff_digest,
            request.validator_bundle_digest,
            request.author_sandbox_id,
            request.redaction_policy_id,
        )
        observed = (
            observation.review_id,
            observation.domain_review_attempt,
            observation.candidate_digest,
            observation.policy_digest,
            observation.base_revision,
            observation.head_revision,
            observation.diff_digest,
            observation.validator_bundle_digest,
            observation.author_sandbox_id,
            observation.redaction_policy_id,
        )
        if observed != expected:
            raise ValueError("critic delivery result does not match its request")
        if observation.sandbox.sandbox_id == request.author_sandbox_id:
            raise ValueError("critic delivery reused the author sandbox identity")


class MissionCriticActivityBinding:
    """Opt one exact world into the Activity-backed critic choreography."""

    def __init__(
        self,
        *,
        world_id: str,
        owner: str,
        reader: StorageMissionCommittedIntentReader,
        catalog: MissionCriticActivityCatalog,
        values: MissionCriticValueStore,
        executor: MissionCriticExecutor,
        stager: WorldMissionCriticObservationStager,
    ) -> None:
        if not world_id.strip():
            raise ValueError("mission critic binding requires a world identity")
        self.world_id = world_id
        self.projector = MissionCriticActivityProjector(
            reader=reader,
            catalog=catalog,
            values=values,
        )
        self.required_projector = RequiredProjector(
            consumer_name=self.projector.consumer_name,
            project=self.projector.project,
        )
        self.worker = MissionCriticActivityWorker(
            world_id=world_id,
            owner=owner,
            catalog=catalog,
            values=values,
            executor=executor,
            stager=stager,
        )
        self._catalog = catalog

    def required_projector_for(self, world_id: str) -> RequiredProjector | None:
        """Bind the sole projector slot only for this explicitly selected world."""

        return self.required_projector if str(world_id) == self.world_id else None

    async def has_unsettled_work(self, world_id: str) -> bool:
        """Report only this binding's exact world to the lifecycle gate."""

        if str(world_id) != self.world_id:
            return False
        return await self._catalog.has_unsettled_work(self.world_id)


__all__ = [
    "MissionCriticActivityBinding",
    "WorldMissionCriticObservationStager",
]
