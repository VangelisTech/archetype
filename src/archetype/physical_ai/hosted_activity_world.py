# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact world adapters for hosted Physical-AI Activity choreography."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.interfaces import ArchetypeSignature, CommittedTickReceipt
from archetype.physical_ai.hosted_activities import (
    CommittedPhysicalSnapshot,
    PhysicalHostedActivityCatalog,
    PhysicalHostedActivityProjector,
    PhysicalHostedActivityWorker,
    PhysicalHostedValueStore,
    prepare_hosted_episode_intent,
)
from archetype.physical_ai.hosted_activity_contracts import (
    HostedEpisodeIntent,
    HostedEpisodeObservation,
    HostedEpisodeProvider,
    hosted_episode_provider_operation_id,
)
from archetype.storage.catalog import SignatureRecord
from archetype.storage.interfaces import iStorageService
from archetype.world.interfaces import iWorldRegistry
from archetype.world.query import (
    PinnedWorldQuerySnapshot,
    pin_query_snapshot,
    query_components,
)
from archetype.world.simulation import RequiredProjector

_PHYSICAL_ACTIVITY_QUERY_GROUPS: tuple[tuple[type[Component], ...], ...] = (
    # Keep intent/observation separate so either can exist without requiring
    # an archetype that contains both.
    (HostedEpisodeIntent,),
    (HostedEpisodeObservation,),
)


def _has_recorded_group(
    records: list[SignatureRecord],
    group: tuple[type[Component], ...],
) -> bool:
    requested = {component.__name__ for component in group}
    return any(requested.issubset(record.component_names) for record in records)


async def _query_groups(
    storage: iStorageService,
    storage_config: StorageConfig,
    snapshot: PinnedWorldQuerySnapshot,
    groups: tuple[tuple[type[Component], ...], ...],
) -> Mapping[ArchetypeSignature, DataFrame]:
    # Activity delivery control is exact-world state. A child may observe a
    # settled parent fact through ordinary lineage queries, but it must never
    # read and re-admit the parent's intent as a child-world Activity.
    visibility = snapshot.current.visibility_tokens
    if visibility is None:
        raise ValueError("physical Activity reads require coordinated visibility")
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
            visibility_tokens=list(visibility),
        )
        results[group] = await storage.materialize(frame)
    return results


class StoragePhysicalCommittedIntentReader:
    """Read the exact singleton manifest head named by a required receipt."""

    def __init__(
        self,
        storage: iStorageService,
        storage_config: StorageConfig | None = None,
    ) -> None:
        self._storage = storage
        self._storage_config = storage_config or StorageConfig()

    async def read(self, receipt: CommittedTickReceipt) -> CommittedPhysicalSnapshot:
        if receipt.visibility_token is None:
            raise ValueError("physical Activity reader requires a visibility token")
        snapshot = await pin_query_snapshot(
            self._storage,
            receipt.world_id,
            receipt.run_id,
            self._storage_config,
        )
        if (
            snapshot.world_id != receipt.world_id
            or snapshot.run_id != receipt.run_id
            or snapshot.head_tick != receipt.committed_tick
            or snapshot.head_tokens != (receipt.visibility_token,)
        ):
            raise ValueError("physical Activity read is not the exact committed receipt")
        return CommittedPhysicalSnapshot(
            world_id=snapshot.world_id,
            run_id=snapshot.run_id,
            committed_tick=receipt.committed_tick,
            visibility_token=receipt.visibility_token,
            results=await _query_groups(
                self._storage,
                self._storage_config,
                snapshot,
                _PHYSICAL_ACTIVITY_QUERY_GROUPS,
            ),
        )


def _component_from_row(
    component_type: type[HostedEpisodeObservation],
    row: dict[str, object],
) -> HostedEpisodeObservation:
    prefix = component_type.get_prefix()
    return component_type.model_validate(
        {field: row[f"{prefix}{field}"] for field in component_type.model_fields}
    )


def _pending_observations(world: Any) -> tuple[HostedEpisodeObservation, ...]:
    spawn_cache = world.spawn_cache
    entity2sig = world.entity2sig
    prefix = HostedEpisodeObservation.get_prefix()
    required = {f"{prefix}{field}" for field in HostedEpisodeObservation.model_fields}
    observations: list[HostedEpisodeObservation] = []
    for signature, rows in spawn_cache.items():
        for row in rows:
            if not required.issubset(row):
                continue
            if entity2sig.get(int(row["entity_id"])) != signature:
                continue
            observations.append(_component_from_row(HostedEpisodeObservation, row))
    return tuple(observations)


class WorldHostedEpisodeObservationStager:
    """Stage one complete marker idempotently under the exact world lock."""

    def __init__(
        self,
        *,
        storage: iStorageService,
        registry: iWorldRegistry,
    ) -> None:
        self._storage = storage
        self._registry = registry

    async def stage_hosted_episode_observation(
        self,
        *,
        world_id: str,
        observation: HostedEpisodeObservation,
    ) -> None:
        if not world_id.strip() or not observation.activity_id.strip():
            raise ValueError("hosted observation requires world and Activity identities")
        async with self._registry.operation(world_id) as world:
            committed = tuple(
                item
                for item in await self._committed_observations(
                    world_id,
                    world,
                    observation.activity_id,
                )
                if item.activity_id == observation.activity_id
            )
            self._accept_existing(committed, observation)
            if committed:
                return
            pending = tuple(
                item
                for item in _pending_observations(world)
                if item.activity_id == observation.activity_id
            )
            self._accept_existing(pending, observation)
            if pending:
                return
            await world.create_entity([observation])

    async def _committed_observations(
        self,
        world_id: str,
        world: Any,
        activity_id: str,
    ) -> tuple[HostedEpisodeObservation, ...]:
        storage_record = await self._registry.storage_record(world_id)
        storage_config = storage_record[0] if storage_record is not None else StorageConfig()
        records = await self._storage.get_control_catalog(storage_config).list_signatures()
        if not _has_recorded_group(records, (HostedEpisodeObservation,)):
            return ()
        snapshot = await pin_query_snapshot(
            self._storage,
            world_id,
            str(world.run_id),
            storage_config,
        )
        # Idempotency is scoped to the world-qualified Activity key. An
        # inherited parent observation is an ordinary fact, not settlement for
        # a child-world Activity with the same family-local activity_id.
        visibility = snapshot.current.visibility_tokens
        if visibility is None:
            raise ValueError("hosted observation staging requires coordinated visibility")
        frame = await query_components(
            self._storage,
            [HostedEpisodeObservation],
            snapshot.world_id,
            snapshot.run_id,
            storage_config,
            visibility_tokens=list(visibility),
        )
        prefix = HostedEpisodeObservation.get_prefix()
        operation_id = hosted_episode_provider_operation_id(world_id, activity_id)
        matching = frame.where(
            cast(
                Expression,
                (col(f"{prefix}activity_id") == activity_id)
                & (col(f"{prefix}operation_id") == operation_id),
            )
        )
        materialized = await self._storage.materialize(matching)
        return tuple(
            _component_from_row(HostedEpisodeObservation, row) for row in materialized.to_pylist()
        )

    @staticmethod
    def _accept_existing(
        existing: tuple[HostedEpisodeObservation, ...],
        candidate: HostedEpisodeObservation,
    ) -> None:
        matching = tuple(value for value in existing if value.activity_id == candidate.activity_id)
        if not matching:
            return
        if len(matching) != 1 or matching[0] != candidate:
            raise ValueError("hosted Activity has conflicting or duplicate observation markers")


class PhysicalHostedActivityBinding:
    """Bind one world to required projection and an out-of-lock worker."""

    def __init__(
        self,
        *,
        world_id: str,
        owner: str,
        reader: StoragePhysicalCommittedIntentReader,
        catalog: PhysicalHostedActivityCatalog,
        values: PhysicalHostedValueStore,
        provider: HostedEpisodeProvider,
        stager: WorldHostedEpisodeObservationStager,
        close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if not world_id.strip():
            raise ValueError("hosted Physical-AI binding requires a world identity")
        self.world_id = world_id
        self.projector = PhysicalHostedActivityProjector(
            reader=reader,
            catalog=catalog,
            values=values,
        )
        self.required_projector = RequiredProjector(
            consumer_name=self.projector.consumer_name,
            project=self.projector.project,
        )
        self.worker = PhysicalHostedActivityWorker(
            world_id=world_id,
            owner=owner,
            catalog=catalog,
            values=values,
            provider=provider,
            stager=stager,
        )
        self._catalog = catalog
        self._values = values
        self._close = close

    def required_projector_for(self, world_id: str) -> RequiredProjector | None:
        return self.required_projector if str(world_id) == self.world_id else None

    async def has_unsettled_work(self, world_id: str) -> bool:
        if str(world_id) != self.world_id:
            return False
        return await self._catalog.has_unsettled_work(self.world_id)

    async def observation(self, activity_id: str) -> HostedEpisodeObservation | None:
        delivery = await self._catalog.episode_result(
            world_id=self.world_id,
            activity_id=activity_id,
        )
        if delivery is None:
            return None
        published = await self._values.get_result(delivery.result)
        return published.observation(activity_id)

    async def prepare_intent(
        self,
        *,
        activity_id: str,
        request_ipc: bytes,
    ) -> HostedEpisodeIntent:
        return await prepare_hosted_episode_intent(
            self._values,
            world_id=self.world_id,
            activity_id=activity_id,
            request_ipc=request_ipc,
        )

    async def aclose(self) -> None:
        if self._close is not None:
            await self._close()


__all__ = [
    "PhysicalHostedActivityBinding",
    "StoragePhysicalCommittedIntentReader",
    "WorldHostedEpisodeObservationStager",
]
