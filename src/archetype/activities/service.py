# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Generic durable-activity coordination over a physical catalog."""

from __future__ import annotations

from collections.abc import Awaitable

from archetype.activities.contracts import (
    ActivityAdmission,
    ActivityClaim,
    ActivityClaimError,
    ActivityConflictError,
    ActivityNotFoundError,
    ActivityResultRef,
    ActivityRetryGuard,
    ActivitySettlement,
    ActivitySnapshot,
)
from archetype.core.interfaces import CommittedTickReceipt
from archetype.storage.activity_catalog.interfaces import ActivityCatalog
from archetype.storage.activity_catalog.records import (
    ActivityAdmissionRecord,
    ActivityCatalogClaimError,
    ActivityCatalogConflictError,
    ActivityCatalogNotFoundError,
    ActivityClaimRecord,
    ActivityRecord,
)


class ActivityCoordinator:
    """Coordinate admissions, fenced attempts, results, and observations.

    Provider-specific execution and recovery remain above this class.  In
    particular, a reconciliation claim carries prior provider identity but
    provides no generic decision about whether an effect happened.
    """

    def __init__(self, catalog: ActivityCatalog) -> None:
        self._catalog = catalog

    async def admit(self, admission: ActivityAdmission) -> ActivitySnapshot:
        record = await _translate_catalog_errors(
            self._catalog.admit_activity(_admission_record(admission))
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

    async def claim(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        owner: str,
        *,
        lease_seconds: float = 300.0,
    ) -> ActivityClaim:
        record = await _translate_catalog_errors(
            self._catalog.claim_activity(
                world_id,
                kind,
                activity_id,
                owner,
                lease_seconds=lease_seconds,
            )
        )
        return _claim(record)

    async def bind_provider_operation(
        self,
        claim: ActivityClaim,
        provider: str,
        operation_id: str,
    ) -> ActivityClaim:
        record = await _translate_catalog_errors(
            self._catalog.bind_provider_operation(
                _claim_record(claim),
                provider,
                operation_id,
            )
        )
        return _claim(record)

    async def confirm_provider_operation_absent(
        self,
        claim: ActivityClaim,
        guard: ActivityRetryGuard,
        *,
        lease_seconds: float = 300.0,
    ) -> ActivityClaim:
        if not isinstance(guard, ActivityRetryGuard):
            raise TypeError("guard must be an ActivityRetryGuard")
        record = await _translate_catalog_errors(
            self._catalog.confirm_provider_operation_absent(
                _claim_record(claim),
                guard.ref,
                guard.digest,
                lease_seconds=lease_seconds,
            )
        )
        return _claim(record)

    async def record_result(
        self,
        claim: ActivityClaim,
        result: ActivityResultRef,
    ) -> ActivitySnapshot:
        record = await _translate_catalog_errors(
            self._catalog.record_activity_result(
                _claim_record(claim),
                result_ref=result.ref,
                result_digest=result.digest,
                result_media_type=result.media_type,
                result_size_bytes=result.size_bytes,
            )
        )
        return _snapshot(record)

    async def release(self, claim: ActivityClaim) -> None:
        await _translate_catalog_errors(self._catalog.release_activity(_claim_record(claim)))

    async def has_unsettled(self, world_id: str) -> bool:
        return await _translate_catalog_errors(self._catalog.has_unsettled_activities(world_id))

    async def pending(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[ActivitySnapshot, ...]:
        records = await _translate_catalog_errors(
            self._catalog.list_incomplete_activities(
                kind=kind,
                world_id=world_id,
                limit=limit,
                offset=offset,
            )
        )
        return tuple(_snapshot(record) for record in records)

    async def pending_results(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[ActivitySnapshot, ...]:
        records = await _translate_catalog_errors(
            self._catalog.list_unobserved_results(
                kind=kind,
                world_id=world_id,
                limit=limit,
                offset=offset,
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


def _claim_record(claim: ActivityClaim) -> ActivityClaimRecord:
    return ActivityClaimRecord(
        activity=_activity_record(claim.snapshot),
        acquired=claim.acquired,
        attempt=claim.attempt,
        fence=claim.fence,
        owner=claim.owner,
        lease_expires_at=claim.lease_expires_at,
        provider=claim.provider,
        provider_operation_id=claim.provider_operation_id,
        retry_guard_ref=(claim.retry_guard.ref if claim.retry_guard is not None else None),
        retry_guard_digest=(claim.retry_guard.digest if claim.retry_guard is not None else None),
        reconciles_attempt=claim.reconciles_attempt,
        reconciles_provider=claim.reconciles_provider,
        reconciles_provider_operation_id=claim.reconciles_provider_operation_id,
    )


def _activity_record(snapshot: ActivitySnapshot) -> ActivityRecord:
    admission = snapshot.admission
    result = snapshot.result
    settlement = snapshot.settlement
    return ActivityRecord(
        activity_id=admission.activity_id,
        kind=admission.kind,
        source_world_id=admission.source.world_id,
        source_run_id=admission.source.run_id,
        source_tick=admission.source.committed_tick,
        source_visibility_token=admission.source.visibility_token,
        input_ref=admission.input_ref,
        input_digest=admission.input_digest,
        result_ref=result.ref if result is not None else None,
        result_digest=result.digest if result is not None else None,
        result_media_type=result.media_type if result is not None else None,
        result_size_bytes=result.size_bytes if result is not None else None,
        result_attempt=snapshot.result_attempt,
        result_fence=snapshot.result_fence,
        result_recorded_at=None,
        observed_world_id=(settlement.receipt.world_id if settlement is not None else None),
        observed_run_id=settlement.receipt.run_id if settlement is not None else None,
        observed_tick=(settlement.receipt.committed_tick if settlement is not None else None),
        observed_visibility_token=(
            settlement.receipt.visibility_token if settlement is not None else None
        ),
        observed_result_digest=(settlement.result_digest if settlement is not None else None),
        observed_at=None,
        created_at="",
        updated_at="",
    )


def _claim(record: ActivityClaimRecord) -> ActivityClaim:
    return ActivityClaim(
        snapshot=_snapshot(record.activity),
        acquired=record.acquired,
        attempt=record.attempt,
        fence=record.fence,
        owner=record.owner,
        lease_expires_at=record.lease_expires_at,
        provider=record.provider,
        provider_operation_id=record.provider_operation_id,
        retry_guard=_retry_guard(record),
        reconciles_attempt=record.reconciles_attempt,
        reconciles_provider=record.reconciles_provider,
        reconciles_provider_operation_id=record.reconciles_provider_operation_id,
    )


def _retry_guard(record: ActivityClaimRecord) -> ActivityRetryGuard | None:
    values = (record.retry_guard_ref, record.retry_guard_digest)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise RuntimeError("activity catalog contains a partial retry guard")
    assert record.retry_guard_ref is not None
    assert record.retry_guard_digest is not None
    return ActivityRetryGuard(
        ref=record.retry_guard_ref,
        digest=record.retry_guard_digest,
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
        result=result,
        settlement=settlement,
        result_attempt=record.result_attempt,
        result_fence=record.result_fence,
    )


async def _translate_catalog_errors[T](awaitable: Awaitable[T]) -> T:
    try:
        return await awaitable
    except ActivityCatalogNotFoundError as exc:
        raise ActivityNotFoundError(*exc.args) from exc
    except ActivityCatalogClaimError as exc:
        raise ActivityClaimError(str(exc)) from exc
    except ActivityCatalogConflictError as exc:
        raise ActivityConflictError(str(exc)) from exc


__all__ = ["ActivityCoordinator"]
