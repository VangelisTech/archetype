# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""World-owned interpretation of manifest-pinned physical rows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from daft import DataFrame, col

from archetype.core.config import StorageConfig
from archetype.storage.catalog import ControlCatalog, SignatureRecord, WorldRecord
from archetype.storage.service import PinnedVisibility, VisibleWorldRows
from archetype.storage.signatures import resolve_signature_records


class ResumeStorage(Protocol):
    """Physical storage operations consumed by world resume interpretation."""

    async def pin_visibility(
        self,
        storage_config: StorageConfig,
        world_id: str,
        *,
        run_id: str | None = None,
        max_tick: int | None = None,
    ) -> PinnedVisibility: ...

    async def scan_visible_world_rows(
        self,
        storage_config: StorageConfig,
        world_record: WorldRecord,
        visibility: PinnedVisibility,
    ) -> VisibleWorldRows: ...

    async def materialize(self, frame: DataFrame) -> DataFrame: ...


@dataclass(frozen=True, slots=True)
class ResumeSnapshot:
    """Durable state needed to reconstruct one mutable world."""

    directory: dict[int, SignatureRecord]
    next_entity_id: int
    resume_tick: int
    visibility: PinnedVisibility


def apply_row_to_directory(
    directory: dict[int, SignatureRecord],
    latest_seen: dict[int, int],
    signature: SignatureRecord,
    row: Mapping[str, Any],
) -> int | None:
    """Merge one physical row under latest-wins/active-wins semantics.

    Negative entity IDs are metadata and never enter the live directory.
    Same-tick inactive/active pairs are expected during archetype migration;
    an active row wins regardless of signature-table scan order.
    """

    entity_id = int(row["entity_id"])
    if entity_id < 0:
        return None
    tick = int(row["tick"])
    is_active = bool(row["is_active"])
    prior_tick = latest_seen.get(entity_id)
    if prior_tick is not None and prior_tick > tick:
        return entity_id
    if prior_tick == tick:
        if is_active:
            directory[entity_id] = signature
        return entity_id

    latest_seen[entity_id] = tick
    if is_active:
        directory[entity_id] = signature
    else:
        directory.pop(entity_id, None)
    return entity_id


def derive_resume_tick(
    visibility: PinnedVisibility,
    *,
    lineage: Sequence[tuple[str, str, int]],
    latest_physical_tick: int | None,
) -> int:
    """Derive the next tick from manifest authority or legacy visible rows."""

    lineage_tick = int(lineage[-1][2]) + 1 if lineage else 0
    if visibility.visibility_tokens is not None:
        return int(visibility.head_tick) + 1 if visibility.head_tick is not None else lineage_tick
    return int(latest_physical_tick) + 1 if latest_physical_tick is not None else lineage_tick


async def _merge_visible_segment(
    storage: ResumeStorage,
    storage_config: StorageConfig,
    world_record: WorldRecord,
    visibility: PinnedVisibility,
    directory: dict[int, SignatureRecord],
    latest_seen: dict[int, int],
) -> tuple[int, int | None]:
    """Merge one raw physical world/run segment into a resume directory."""

    max_entity_id = 0
    scanned = await storage.scan_visible_world_rows(
        storage_config,
        world_record,
        visibility,
    )
    for table in scanned.tables:
        frame = table.frame
        latest = frame.groupby("entity_id").agg(col("tick").max().alias("latest_tick"))
        current = frame.join(
            latest,
            left_on=["entity_id", "tick"],
            right_on=["entity_id", "latest_tick"],
        ).select("entity_id", "tick", "is_active")
        materialized = await storage.materialize(current)
        for row in materialized.to_pylist():
            entity_id = apply_row_to_directory(
                directory,
                latest_seen,
                table.signature,
                row,
            )
            if entity_id is not None:
                max_entity_id = max(max_entity_id, entity_id)
    return max_entity_id, scanned.latest_physical_tick


async def reconstruct_resume_snapshot(
    storage: ResumeStorage,
    catalog: ControlCatalog,
    storage_config: StorageConfig,
    world_record: WorldRecord,
    *,
    run_id: str,
    lineage: Sequence[tuple[str, str, int]],
) -> ResumeSnapshot:
    """Reconstruct liveness, counters, and tick from one pinned snapshot."""

    world_id = str(world_record.world_id)
    visibility = await storage.pin_visibility(
        storage_config,
        world_id,
        run_id=run_id,
    )
    directory: dict[int, SignatureRecord] = {}
    latest_seen: dict[int, int] = {}
    max_entity_id = 0

    for ancestor_world, ancestor_run, up_to_tick in lineage:
        ancestor_record = await catalog.get_world(str(ancestor_world))
        if ancestor_record is None:
            raise RuntimeError(f"lineage references missing ancestor world {ancestor_world}")
        ancestor_visibility = await storage.pin_visibility(
            storage_config,
            str(ancestor_world),
            run_id=str(ancestor_run),
            max_tick=int(up_to_tick),
        )
        segment_max_entity, _ = await _merge_visible_segment(
            storage,
            storage_config,
            ancestor_record,
            ancestor_visibility,
            directory,
            latest_seen,
        )
        max_entity_id = max(max_entity_id, segment_max_entity)

    own_max_entity, own_physical_head = await _merge_visible_segment(
        storage,
        storage_config,
        world_record,
        visibility,
        directory,
        latest_seen,
    )
    max_entity_id = max(max_entity_id, own_max_entity)
    return ResumeSnapshot(
        directory=directory,
        next_entity_id=max_entity_id + 1,
        resume_tick=derive_resume_tick(
            visibility,
            lineage=lineage,
            latest_physical_tick=own_physical_head,
        ),
        visibility=visibility,
    )


def resolve_live_signatures(
    directory: Mapping[int, SignatureRecord],
) -> tuple[dict[int, tuple[Any, ...]], set[str]]:
    """Resolve component classes only for signatures that still own entities."""

    resolved = resolve_signature_records(directory.values(), operation="resume")
    entity2sig = {entity_id: resolved[record.table_id] for entity_id, record in directory.items()}
    return entity2sig, set(resolved)
