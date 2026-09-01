# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ECS admission and settlement evidence for Temporal-owned work."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Protocol

from archetype.activities.contracts import (
    ActivityAdmission,
    ActivityConflictError,
    ActivityExecutionIdentity,
    ActivityNotFoundError,
    ActivityResultRef,
    ActivitySettlement,
    ActivitySnapshot,
)
from archetype.core.interfaces import CommittedTickReceipt
from archetype.storage.activity_catalog.interfaces import ActivityCatalog
from archetype.storage.activity_catalog.records import (
    ActivityAdmissionRecord,
    ActivityCatalogConflictError,
    ActivityCatalogNotFoundError,
    ActivityRecord,
)


class ActivityCoordinator:
    """Persist admission and settlement facts; Temporal owns execution."""

    def __init__(self, catalog: ActivityCatalog) -> None:
        self._catalog = catalog

    async def admit(
        self,
        admission: ActivityAdmission,
        execution: ActivityExecutionIdentity,
    ) -> ActivitySnapshot:
        if not isinstance(execution, ActivityExecutionIdentity):
            raise TypeError("execution must be an ActivityExecutionIdentity")
        record = await _translate_catalog_errors(
            self._catalog.admit_activity(
                _admission_record(admission),
                execution_provider=execution.provider,
                execution_operation_id=execution.operation_id,
            )
        )
        return _snapshot(record)

    async def get(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
    ) -> ActivitySnapshot | None:
        record = await _translate_catalog_errors(
            self._catalog.get_activity(world_id, kind, activity_id)
        )
        return _snapshot(record) if record is not None else None

    async def record_orchestrated_result(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        execution: ActivityExecutionIdentity,
        result: ActivityResultRef,
    ) -> ActivitySnapshot:
        """Record a Temporal-owned result without creating a lease attempt."""

        if not isinstance(execution, ActivityExecutionIdentity):
            raise TypeError("execution must be an ActivityExecutionIdentity")
        record = await _translate_catalog_errors(
            self._catalog.record_orchestrated_activity_result(
                world_id,
                kind,
                activity_id,
                provider=execution.provider,
                provider_operation_id=execution.operation_id,
                result_ref=result.ref,
                result_digest=result.digest,
                result_media_type=result.media_type,
                result_size_bytes=result.size_bytes,
            )
        )
        return _snapshot(record)

    async def has_unsettled(self, world_id: str) -> bool:
        return await _translate_catalog_errors(self._catalog.has_unsettled_activities(world_id))

    async def pending_results(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        after_sequence: int = 0,
    ) -> tuple[ActivitySnapshot, ...]:
        records = await _translate_catalog_errors(
            self._catalog.list_unobserved_results(
                kind=kind,
                world_id=world_id,
                limit=limit,
                after_sequence=after_sequence,
            )
        )
        return tuple(_snapshot(record) for record in records)

    async def settle_observation(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        settlement: ActivitySettlement,
    ) -> ActivitySnapshot:
        receipt = settlement.receipt
        record = await _translate_catalog_errors(
            self._catalog.settle_activity_observation(
                world_id,
                kind,
                activity_id,
                observed_world_id=receipt.world_id,
                observed_run_id=receipt.run_id,
                observed_tick=receipt.committed_tick,
                observed_visibility_token=receipt.visibility_token,
                expected_result_digest=settlement.result_digest,
            )
        )
        return _snapshot(record)


class _ActivityResultIndex(Protocol):
    """Result-delivery subset shared by legacy and orchestrated indexes."""

    async def pending_results(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        after_sequence: int = 0,
    ) -> tuple[ActivitySnapshot, ...]: ...


async def collect_pending_results(
    coordinator: _ActivityResultIndex,
    *,
    kind: str,
    world_id: str,
    page_size: int = 1_000,
) -> tuple[ActivitySnapshot, ...]:
    """Collect every unobserved durable result, paging until exhaustion.

    A finite prefix silently drops durable results beyond it, so an observation committed
    on the current receipt could miss deliveries whenever a world holds
    more than one page of unobserved results.  Pages advance by the same
    admission-sequence keyset cursor, so results observed by other workers
    mid-scan leave the set without shifting later rows out of the pass —
    every result that stays unobserved throughout the scan is collected.
    """

    if page_size < 1:
        raise ValueError("result scan page size must be positive")
    snapshots: list[ActivitySnapshot] = []
    after_sequence = 0
    while True:
        page = await coordinator.pending_results(
            kind=kind,
            world_id=world_id,
            limit=page_size,
            after_sequence=after_sequence,
        )
        snapshots.extend(page)
        if len(page) < page_size:
            return tuple(snapshots)
        after_sequence = _scan_cursor(page[-1])


def _scan_cursor(snapshot: ActivitySnapshot) -> int:
    if snapshot.sequence is None:
        raise RuntimeError(
            "activity scan paging requires the catalog-assigned admission "
            "sequence on every returned snapshot"
        )
    return snapshot.sequence


def _admission_record(admission: ActivityAdmission) -> ActivityAdmissionRecord:
    return ActivityAdmissionRecord(
        activity_id=admission.activity_id,
        kind=admission.kind,
        source_world_id=admission.source.world_id,
        source_run_id=admission.source.run_id,
        source_tick=admission.source.committed_tick,
        source_visibility_token=admission.source.visibility_token,
        input_ref=admission.input_ref,
        input_digest=admission.input_digest,
    )


def _snapshot(record: ActivityRecord) -> ActivitySnapshot:
    source = CommittedTickReceipt(
        world_id=record.source_world_id,
        run_id=record.source_run_id,
        committed_tick=record.source_tick,
        visibility_token=record.source_visibility_token,
        commands_applied=0,
    )
    admission = ActivityAdmission(
        activity_id=record.activity_id,
        kind=record.kind,
        source=source,
        input_ref=record.input_ref,
        input_digest=record.input_digest,
    )
    result_fields = (
        record.result_ref,
        record.result_digest,
        record.result_media_type,
        record.result_size_bytes,
    )
    result: ActivityResultRef | None
    if all(value is None for value in result_fields):
        result = None
    elif any(value is None for value in result_fields):
        raise RuntimeError("activity catalog contains a partial result reference")
    else:
        assert record.result_ref is not None
        assert record.result_digest is not None
        assert record.result_media_type is not None
        assert record.result_size_bytes is not None
        result = ActivityResultRef(
            ref=record.result_ref,
            digest=record.result_digest,
            media_type=record.result_media_type,
            size_bytes=record.result_size_bytes,
        )

    observation_fields = (
        record.observed_world_id,
        record.observed_run_id,
        record.observed_tick,
        record.observed_result_digest,
    )
    settlement: ActivitySettlement | None
    if all(value is None for value in observation_fields):
        settlement = None
    elif any(value is None for value in observation_fields):
        raise RuntimeError("activity catalog contains a partial observation settlement")
    else:
        assert record.observed_world_id is not None
        assert record.observed_run_id is not None
        assert record.observed_tick is not None
        assert record.observed_result_digest is not None
        settlement = ActivitySettlement(
            CommittedTickReceipt(
                world_id=record.observed_world_id,
                run_id=record.observed_run_id,
                committed_tick=record.observed_tick,
                visibility_token=record.observed_visibility_token,
                commands_applied=0,
            ),
            result_digest=record.observed_result_digest,
        )
    return ActivitySnapshot(
        admission=admission,
        execution=(
            ActivityExecutionIdentity(
                provider=record.execution_provider,
                operation_id=record.execution_operation_id,
            )
            if record.execution_provider is not None and record.execution_operation_id is not None
            else None
        ),
        result=result,
        settlement=settlement,
        sequence=record.sequence,
    )


async def _translate_catalog_errors[T](awaitable: Awaitable[T]) -> T:
    try:
        return await awaitable
    except ActivityCatalogNotFoundError as exc:
        raise ActivityNotFoundError(*exc.args) from exc
    except ActivityCatalogConflictError as exc:
        raise ActivityConflictError(str(exc)) from exc


__all__ = [
    "ActivityCoordinator",
    "collect_pending_results",
]
