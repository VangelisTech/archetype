# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Crash contracts for the first local durable-activity control plane."""

from __future__ import annotations

import asyncio
import threading

import pytest

from archetype.activities import (
    ActivityAdmission,
    ActivityClaimError,
    ActivityConflictError,
    ActivityCoordinator,
    ActivityNotFoundError,
    ActivityResultRef,
    ActivityRetryGuard,
    ActivitySettlement,
    iActivityCoordinator,
)
from archetype.core.interfaces import CommittedTickReceipt
from archetype.storage.activity_catalog import (
    ActivityAdmissionRecord,
    ActivityCatalog,
    SqliteActivityCatalog,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("activities.durable_control"),
]

_KIND = "missions.author"
_OPERATION_ID = "missions.author:world-a:dispatch-1"


def _receipt(
    world_id: str = "world-a",
    *,
    run_id: str = "run-a",
    tick: int = 4,
    token: str | None = "manifest-4",
) -> CommittedTickReceipt:
    return CommittedTickReceipt(
        world_id=world_id,
        run_id=run_id,
        committed_tick=tick,
        visibility_token=token,
        commands_applied=7,
    )


def _admission(
    world_id: str = "world-a",
    *,
    kind: str = _KIND,
    activity_id: str = "dispatch-1",
    input_digest: str = "input-digest",
) -> ActivityAdmission:
    return ActivityAdmission(
        activity_id=activity_id,
        kind=kind,
        source=_receipt(world_id),
        input_ref=f"mission-input://{world_id}/{kind}/{activity_id}",
        input_digest=input_digest,
    )


def _result(digest: str = "result-digest") -> ActivityResultRef:
    return ActivityResultRef(
        ref=f"artifact://mission-results/{digest}",
        digest=digest,
        media_type="application/vnd.archetype.mission-author+json",
        size_bytes=123,
    )


def _guard(digest: str = "retry-guard-digest") -> ActivityRetryGuard:
    return ActivityRetryGuard(
        ref=f"provider-guard://git/{digest}",
        digest=digest,
    )


def _settlement(
    *,
    tick: int = 5,
    token: str = "manifest-5",
    run_id: str = "run-a",
    result_digest: str = "result-digest",
) -> ActivitySettlement:
    return ActivitySettlement(
        _receipt("world-a", run_id=run_id, tick=tick, token=token),
        result_digest=result_digest,
    )


async def test_admission_is_idempotent_and_activity_identity_is_world_scoped(
    tmp_path,
) -> None:
    catalog = SqliteActivityCatalog(tmp_path / "activities.db")
    coordinator = ActivityCoordinator(catalog)
    try:
        first = await coordinator.admit(_admission("world-a"))
        repeated = await coordinator.admit(_admission("world-a"))
        collision = await coordinator.admit(
            _admission("world-b", input_digest="different-world-input")
        )

        assert first == repeated
        assert first.admission.activity_id == collision.admission.activity_id
        assert first.admission.source.world_id == "world-a"
        assert collision.admission.source.world_id == "world-b"
        assert await coordinator.get("world-a", _KIND, "dispatch-1") == first
        assert await coordinator.get("world-b", _KIND, "dispatch-1") == collision

        with pytest.raises(ActivityConflictError, match="different immutable content"):
            await coordinator.admit(_admission("world-a", input_digest="changed"))

        assert [
            item.admission.source.world_id for item in await coordinator.pending(kind=_KIND)
        ] == ["world-a", "world-b"]
        assert [
            item.admission.source.world_id for item in await coordinator.pending(world_id="world-b")
        ] == ["world-b"]
    finally:
        await catalog.close()


async def test_same_local_id_is_isolated_by_world_and_kind(tmp_path) -> None:
    catalog = SqliteActivityCatalog(tmp_path / "activities.db")
    coordinator = ActivityCoordinator(catalog)
    critic_kind = "missions.critic"
    try:
        author = await coordinator.admit(_admission())
        critic = await coordinator.admit(_admission(kind=critic_kind, input_digest="critic-input"))
        other_world = await coordinator.admit(_admission("world-b"))

        author_claim = await coordinator.claim(
            "world-a",
            _KIND,
            "dispatch-1",
            "author-worker",
        )
        critic_claim = await coordinator.claim(
            "world-a",
            critic_kind,
            "dispatch-1",
            "critic-worker",
        )
        other_world_claim = await coordinator.claim(
            "world-b",
            _KIND,
            "dispatch-1",
            "other-author-worker",
        )

        assert author.admission.kind == _KIND
        assert critic.admission.kind == critic_kind
        assert other_world.admission.source.world_id == "world-b"
        assert author_claim.acquired and critic_claim.acquired and other_world_claim.acquired
        assert author_claim.attempt == critic_claim.attempt == other_world_claim.attempt == 1
        assert author_claim.fence == critic_claim.fence == other_world_claim.fence == 1
        assert await coordinator.get("world-a", _KIND, "dispatch-1") == author
        assert await coordinator.get("world-a", critic_kind, "dispatch-1") == critic
        assert await coordinator.get("world-b", _KIND, "dispatch-1") == other_world

        author_bound = await coordinator.bind_provider_operation(
            author_claim,
            "local",
            _OPERATION_ID,
        )
        with pytest.raises(
            ActivityConflictError,
            match="provider operation is already bound to another activity",
        ):
            await coordinator.bind_provider_operation(
                critic_claim,
                "local",
                _OPERATION_ID,
            )
        with pytest.raises(
            ActivityConflictError,
            match="provider operation is already bound to another activity",
        ):
            await coordinator.bind_provider_operation(
                other_world_claim,
                "local",
                _OPERATION_ID,
            )
        critic_bound = await coordinator.bind_provider_operation(
            critic_claim,
            "local",
            "missions.critic:world-a:dispatch-1",
        )
        other_world_bound = await coordinator.bind_provider_operation(
            other_world_claim,
            "local",
            "missions.author:world-b:dispatch-1",
        )
        assert critic_bound.provider_operation_id != author_bound.provider_operation_id
        assert other_world_bound.provider_operation_id != author_bound.provider_operation_id
        recorded = await coordinator.record_result(author_bound, _result())
        assert await coordinator.pending(kind=_KIND, world_id="world-a") == ()
        assert await coordinator.pending(kind=critic_kind, world_id="world-a") == (critic,)
        assert await coordinator.pending_results(kind=_KIND, world_id="world-a") == (recorded,)
        await coordinator.settle_observation(
            "world-a",
            _KIND,
            "dispatch-1",
            _settlement(),
        )
        assert await coordinator.pending_results(kind=_KIND, world_id="world-a") == ()
        assert await coordinator.get("world-a", critic_kind, "dispatch-1") == critic
    finally:
        await catalog.close()


async def test_provider_operation_reservation_survives_restart_and_settlement(
    tmp_path,
) -> None:
    path = tmp_path / "activities.db"
    first_catalog = SqliteActivityCatalog(path)
    first = ActivityCoordinator(first_catalog)
    try:
        await first.admit(_admission())
        await first.admit(_admission("world-b"))
        claim = await first.claim("world-a", _KIND, "dispatch-1", "owner-a")
        bound = await first.bind_provider_operation(
            claim,
            "local",
            _OPERATION_ID,
        )
        await first.record_result(bound, _result())
        await first.settle_observation(
            "world-a",
            _KIND,
            "dispatch-1",
            _settlement(),
        )
    finally:
        await first_catalog.close()

    recovered_catalog = SqliteActivityCatalog(path)
    recovered = ActivityCoordinator(recovered_catalog)
    try:
        other = await recovered.claim(
            "world-b",
            _KIND,
            "dispatch-1",
            "owner-b",
        )
        with pytest.raises(
            ActivityConflictError,
            match="provider operation is already bound to another activity",
        ):
            await recovered.bind_provider_operation(
                other,
                "local",
                _OPERATION_ID,
            )
        unique = await recovered.bind_provider_operation(
            other,
            "local",
            "missions.author:world-b:dispatch-1",
        )
        assert unique.provider_operation_id == "missions.author:world-b:dispatch-1"
    finally:
        await recovered_catalog.close()


async def test_two_catalog_instances_serialize_one_live_claim(tmp_path) -> None:
    path = tmp_path / "activities.db"
    first_catalog = SqliteActivityCatalog(path)
    second_catalog = SqliteActivityCatalog(path)
    first = ActivityCoordinator(first_catalog)
    second = ActivityCoordinator(second_catalog)
    try:
        await first.admit(_admission())
        owner_a = await first.claim("world-a", _KIND, "dispatch-1", "owner-a")
        waiting = await second.claim("world-a", _KIND, "dispatch-1", "owner-b")
        same_owner = await first.claim("world-a", _KIND, "dispatch-1", "owner-a")

        assert owner_a.acquired
        assert owner_a.attempt == owner_a.fence == 1
        assert not waiting.acquired
        assert waiting.owner == "owner-a"
        assert not same_owner.acquired
        assert same_owner.attempt == owner_a.attempt
        assert same_owner.fence == owner_a.fence
        assert same_owner.lease_expires_at == owner_a.lease_expires_at
    finally:
        await second_catalog.close()
        await first_catalog.close()


async def test_released_pre_provider_claim_is_safely_fenced_and_reclaimable(
    tmp_path,
) -> None:
    catalog = SqliteActivityCatalog(tmp_path / "activities.db")
    coordinator = ActivityCoordinator(catalog)
    try:
        await coordinator.admit(_admission())
        first = await coordinator.claim("world-a", _KIND, "dispatch-1", "owner-a")
        await coordinator.release(first)
        await coordinator.release(first)

        second = await coordinator.claim("world-a", _KIND, "dispatch-1", "owner-b")
        assert second.acquired
        assert second.attempt == 2
        assert second.fence == 2
        assert not second.reconciliation_required

        with pytest.raises(ActivityClaimError, match="stale"):
            await coordinator.bind_provider_operation(
                first,
                "local",
                _OPERATION_ID,
            )

        bound = await coordinator.bind_provider_operation(
            second,
            "local",
            _OPERATION_ID,
        )
        with pytest.raises(ActivityClaimError, match="cannot be released"):
            await coordinator.release(bound)
    finally:
        await catalog.close()


async def test_expired_provider_bound_attempt_becomes_reconcile_only(tmp_path) -> None:
    path = tmp_path / "activities.db"
    first_catalog = SqliteActivityCatalog(path)
    second_catalog = SqliteActivityCatalog(path)
    first = ActivityCoordinator(first_catalog)
    second = ActivityCoordinator(second_catalog)
    try:
        await first.admit(_admission())
        initial = await first.claim(
            "world-a",
            _KIND,
            "dispatch-1",
            "owner-a",
            lease_seconds=0.01,
        )
        bound = await first.bind_provider_operation(
            initial,
            "git",
            _OPERATION_ID,
        )
        await first_catalog.close()
        same_owner_after_restart = await second.claim(
            "world-a",
            _KIND,
            "dispatch-1",
            "owner-a",
            lease_seconds=30,
        )
        assert not same_owner_after_restart.acquired
        assert same_owner_after_restart.attempt == bound.attempt
        assert same_owner_after_restart.fence == bound.fence
        assert same_owner_after_restart.provider_operation_id == _OPERATION_ID
        await asyncio.sleep(0.02)

        recovery = await second.claim(
            "world-a",
            _KIND,
            "dispatch-1",
            "owner-b",
            lease_seconds=30,
        )

        assert recovery.acquired
        assert recovery.fence == 2
        assert recovery.reconciliation_required
        assert recovery.reconciles_attempt == 1
        assert recovery.reconciles_provider == "git"
        assert recovery.reconciles_provider_operation_id == _OPERATION_ID
        with pytest.raises(ActivityClaimError, match="cannot invoke"):
            await second.bind_provider_operation(
                recovery,
                "git",
                "missions.author:world-a:dispatch-1-replay",
            )
        with pytest.raises(ActivityClaimError, match="cannot be released"):
            await second.release(recovery)
        with pytest.raises(ActivityClaimError, match="stale"):
            await first.record_result(bound, _result())

        reconciled = await second.record_result(recovery, _result())
        assert reconciled.result == _result()
        assert reconciled.result_attempt == 2
        assert reconciled.result_fence == 2
    finally:
        await second_catalog.close()
        await first_catalog.close()


async def test_confirmed_provider_absence_mints_fresh_execution_fence(tmp_path) -> None:
    path = tmp_path / "activities.db"
    catalog = SqliteActivityCatalog(path)
    coordinator = ActivityCoordinator(catalog)
    try:
        await coordinator.admit(_admission())
        initial = await coordinator.claim(
            "world-a",
            _KIND,
            "dispatch-1",
            "owner-a",
            lease_seconds=0.01,
        )
        bound = await coordinator.bind_provider_operation(
            initial,
            "git",
            _OPERATION_ID,
        )
        await asyncio.sleep(0.02)

        unknown = await coordinator.claim(
            "world-a",
            _KIND,
            "dispatch-1",
            "owner-b",
            lease_seconds=0.01,
        )
        assert unknown.reconciliation_required
        await asyncio.sleep(0.02)

        recovery = await coordinator.claim(
            "world-a",
            _KIND,
            "dispatch-1",
            "owner-c",
            lease_seconds=30,
        )
        assert recovery.reconciliation_required
        assert recovery.reconciles_provider_operation_id == _OPERATION_ID
        with pytest.raises(ActivityClaimError, match="stale"):
            await coordinator.confirm_provider_operation_absent(unknown, _guard())
        with pytest.raises(ActivityClaimError, match="stale"):
            await coordinator.confirm_provider_operation_absent(bound, _guard())
        with pytest.raises(TypeError, match="ActivityRetryGuard"):
            await coordinator.confirm_provider_operation_absent(
                recovery,
                None,  # type: ignore[arg-type] - absence must not authorize replay
            )

        authorized = await coordinator.confirm_provider_operation_absent(
            recovery,
            _guard(),
        )
        with pytest.raises(ActivityConflictError, match="different retry guard"):
            await coordinator.confirm_provider_operation_absent(
                recovery,
                _guard("different-guard"),
            )
        repeated = await coordinator.confirm_provider_operation_absent(
            recovery,
            _guard(),
        )

        assert authorized.acquired
        assert authorized.attempt == authorized.fence == 4
        assert not authorized.reconciliation_required
        assert authorized.provider_operation_id is None
        assert authorized.retry_guard == _guard()
        assert repeated.attempt == authorized.attempt
        assert repeated.fence == authorized.fence
        assert repeated.retry_guard == _guard()

        with pytest.raises(
            ActivityConflictError,
            match="reconciled provider operation identity",
        ):
            await coordinator.bind_provider_operation(
                authorized,
                "modal",
                _OPERATION_ID,
            )
        with pytest.raises(
            ActivityConflictError,
            match="reconciled provider operation identity",
        ):
            await coordinator.bind_provider_operation(
                authorized,
                "git",
                "missions.author:world-a:different-dispatch",
            )

        await coordinator.release(authorized)
        await catalog.close()

        catalog = SqliteActivityCatalog(path)
        coordinator = ActivityCoordinator(catalog)
        replacement = await coordinator.claim(
            "world-a",
            _KIND,
            "dispatch-1",
            "owner-d",
        )
        assert replacement.acquired
        assert replacement.attempt == replacement.fence == 5
        assert replacement.retry_guard == _guard()
        with pytest.raises(
            ActivityConflictError,
            match="reconciled provider operation identity",
        ):
            await coordinator.bind_provider_operation(
                replacement,
                "git",
                "missions.author:world-a:different-dispatch",
            )

        rebound = await coordinator.bind_provider_operation(
            replacement,
            "git",
            _OPERATION_ID,
        )
        result = await coordinator.record_result(rebound, _result())
        assert result.result_attempt == result.result_fence == 5
    finally:
        await catalog.close()


async def test_result_survives_restart_until_exact_later_tick_settles_it(
    tmp_path,
) -> None:
    path = tmp_path / "activities.db"
    first_catalog = SqliteActivityCatalog(path)
    first = ActivityCoordinator(first_catalog)
    await first.admit(_admission())
    claim = await first.claim("world-a", _KIND, "dispatch-1", "worker")
    bound = await first.bind_provider_operation(
        claim,
        "local",
        _OPERATION_ID,
    )
    recorded = await first.record_result(bound, _result())
    await first_catalog.close()

    second_catalog = SqliteActivityCatalog(path)
    second = ActivityCoordinator(second_catalog)
    try:
        assert recorded.result_pending_observation
        assert await second.pending(kind=_KIND, world_id="world-a") == ()
        assert await second.pending_results(kind=_KIND, world_id="world-a") == (recorded,)

        completed_claim = await second.claim(
            "world-a",
            _KIND,
            "dispatch-1",
            "new-worker",
        )
        assert not completed_claim.acquired
        assert completed_claim.snapshot == recorded

        observation = _settlement()
        settled = await second.settle_observation(
            "world-a",
            _KIND,
            "dispatch-1",
            observation,
        )
        repeated = await second.settle_observation(
            "world-a",
            _KIND,
            "dispatch-1",
            observation,
        )

        assert settled == repeated
        assert settled.settlement == observation
        assert await second.pending_results(world_id="world-a") == ()

        with pytest.raises(ActivityConflictError, match="different observation"):
            await second.settle_observation(
                "world-a",
                _KIND,
                "dispatch-1",
                _settlement(tick=6, token="manifest-6"),
            )
    finally:
        await second_catalog.close()


async def test_result_recording_requires_prebound_provider_and_is_idempotent(
    tmp_path,
) -> None:
    catalog = SqliteActivityCatalog(tmp_path / "activities.db")
    coordinator = ActivityCoordinator(catalog)
    try:
        await coordinator.admit(_admission())
        claim = await coordinator.claim("world-a", _KIND, "dispatch-1", "worker")
        with pytest.raises(ActivityClaimError, match="pre-bound"):
            await coordinator.record_result(claim, _result())

        bound = await coordinator.bind_provider_operation(
            claim,
            "local",
            _OPERATION_ID,
        )
        first = await coordinator.record_result(bound, _result())
        repeated = await coordinator.record_result(bound, _result())
        assert first == repeated

        with pytest.raises(ActivityConflictError, match="different durable result"):
            await coordinator.record_result(bound, _result("different-result"))
    finally:
        await catalog.close()


async def test_settlement_rejects_missing_result_wrong_run_and_non_later_tick(
    tmp_path,
) -> None:
    catalog = SqliteActivityCatalog(tmp_path / "activities.db")
    coordinator = ActivityCoordinator(catalog)
    try:
        await coordinator.admit(_admission())
        with pytest.raises(ActivityConflictError, match="before its result"):
            await coordinator.settle_observation(
                "world-a",
                _KIND,
                "dispatch-1",
                _settlement(),
            )

        claim = await coordinator.claim("world-a", _KIND, "dispatch-1", "worker")
        bound = await coordinator.bind_provider_operation(
            claim,
            "local",
            _OPERATION_ID,
        )
        await coordinator.record_result(bound, _result())

        with pytest.raises(ActivityConflictError, match="durable result digest"):
            await coordinator.settle_observation(
                "world-a",
                _KIND,
                "dispatch-1",
                _settlement(result_digest="different-result"),
            )
        assert len(await coordinator.pending_results(world_id="world-a")) == 1
        with pytest.raises(ActivityConflictError, match="source world and run"):
            await coordinator.settle_observation(
                "world-a",
                _KIND,
                "dispatch-1",
                _settlement(run_id="other-run"),
            )
        with pytest.raises(ActivityConflictError, match="later tick"):
            await coordinator.settle_observation(
                "world-a",
                _KIND,
                "dispatch-1",
                _settlement(tick=4, token="manifest-4-observation"),
            )
    finally:
        await catalog.close()


async def test_unsettled_oracle_covers_pending_and_result_pending_observation(
    tmp_path,
) -> None:
    catalog = SqliteActivityCatalog(tmp_path / "activities.db")
    coordinator = ActivityCoordinator(catalog)
    try:
        assert not await coordinator.has_unsettled("world-a")
        admitted = await coordinator.admit(_admission())

        assert await coordinator.has_unsettled("world-a")
        assert await coordinator.pending(world_id="world-a") == (admitted,)
        assert await coordinator.pending_results(world_id="world-a") == ()
        assert not await coordinator.has_unsettled("world-b")

        claim = await coordinator.claim(
            "world-a",
            _KIND,
            "dispatch-1",
            "worker",
        )
        bound = await coordinator.bind_provider_operation(
            claim,
            "local",
            _OPERATION_ID,
        )
        recorded = await coordinator.record_result(bound, _result())

        assert await coordinator.has_unsettled("world-a")
        assert await coordinator.pending(world_id="world-a") == ()
        assert await coordinator.pending_results(world_id="world-a") == (recorded,)

        await coordinator.settle_observation(
            "world-a",
            _KIND,
            "dispatch-1",
            _settlement(),
        )
        assert not await coordinator.has_unsettled("world-a")
    finally:
        await catalog.close()


async def test_protocols_and_unknown_activity_are_truthful(tmp_path) -> None:
    catalog = SqliteActivityCatalog(tmp_path / "activities.db")
    coordinator = ActivityCoordinator(catalog)
    try:
        assert isinstance(catalog, ActivityCatalog)
        assert isinstance(coordinator, iActivityCoordinator)
        assert await coordinator.get("world", _KIND, "missing") is None
        with pytest.raises(ActivityNotFoundError):
            await coordinator.claim("world", _KIND, "missing", "worker")
    finally:
        await catalog.close()


async def test_claim_and_provider_coordinates_reject_unbounded_or_nonfinite_values(
    tmp_path,
) -> None:
    catalog = SqliteActivityCatalog(tmp_path / "activities.db")
    coordinator = ActivityCoordinator(catalog)
    try:
        await coordinator.admit(_admission())

        with pytest.raises(ValueError, match="at most 512"):
            await coordinator.claim("world-a", _KIND, "dispatch-1", "x" * 513)
        with pytest.raises(ValueError, match="finite and positive"):
            await coordinator.claim(
                "world-a",
                _KIND,
                "dispatch-1",
                "worker",
                lease_seconds=float("nan"),
            )
        with pytest.raises(ValueError, match="finite and positive"):
            await coordinator.claim(
                "world-a",
                _KIND,
                "dispatch-1",
                "worker",
                lease_seconds=float("inf"),
            )
        with pytest.raises(TypeError, match="must be a number"):
            await coordinator.claim(
                "world-a",
                _KIND,
                "dispatch-1",
                "worker",
                lease_seconds=True,
            )

        claim = await coordinator.claim("world-a", _KIND, "dispatch-1", "worker")
        assert claim.attempt == 1, "invalid claims must not mutate durable attempts"
        with pytest.raises(ValueError, match="at most 255"):
            await coordinator.bind_provider_operation(claim, "p" * 256, "operation")
        with pytest.raises(ValueError, match="at most 1024"):
            await coordinator.bind_provider_operation(claim, "local", "o" * 1025)

        bound = await coordinator.bind_provider_operation(
            claim,
            "local",
            _OPERATION_ID,
        )
        assert bound.provider == "local"
        assert bound.provider_operation_id == _OPERATION_ID
    finally:
        await catalog.close()


async def test_activity_boundary_rejects_uncoordinated_commit_receipts() -> None:
    with pytest.raises(ValueError, match="visibility_token must be present"):
        ActivityAdmission(
            activity_id="dispatch",
            kind=_KIND,
            source=_receipt(token=None),
            input_ref="mission-input://world/dispatch",
            input_digest="digest",
        )


async def test_physical_catalog_rejects_tokenless_commit_coordinates(
    tmp_path,
) -> None:
    catalog = SqliteActivityCatalog(tmp_path / "activities.db")
    try:
        with pytest.raises(ValueError, match="source visibility token"):
            await catalog.admit_activity(
                ActivityAdmissionRecord(
                    activity_id="dispatch",
                    kind=_KIND,
                    source_world_id="world",
                    source_run_id="run",
                    source_tick=1,
                    source_visibility_token=None,
                    input_ref="mission-input://world/dispatch",
                    input_digest="digest",
                )
            )
        with pytest.raises(ValueError, match="observation visibility token"):
            await catalog.settle_activity_observation(
                "world",
                _KIND,
                "dispatch",
                observed_world_id="world",
                observed_run_id="run",
                observed_tick=2,
                observed_visibility_token=None,
                expected_result_digest="result-digest",
            )
    finally:
        await catalog.close()


async def test_cancelled_sqlite_worker_retains_lock_until_failed_thread_finishes(
    tmp_path,
) -> None:
    catalog = SqliteActivityCatalog(tmp_path / "activities.db")
    await catalog.admit_activity(
        ActivityAdmissionRecord(
            activity_id="dispatch",
            kind=_KIND,
            source_world_id="world",
            source_run_id="run",
            source_tick=1,
            source_visibility_token="manifest-1",
            input_ref="mission-input://world/dispatch",
            input_digest="digest",
        )
    )
    started = threading.Event()
    release = threading.Event()

    def _blocked_failure() -> None:
        conn = catalog._connect_sync()  # noqa: SLF001 - shared-connection race oracle
        started.set()
        assert release.wait(timeout=5)
        conn.execute("SELECT 1").fetchone()
        raise RuntimeError("worker failed after caller cancellation")

    running = asyncio.create_task(
        catalog._run(_blocked_failure)  # noqa: SLF001 - cancellation boundary oracle
    )
    assert await asyncio.to_thread(started.wait, 5)

    running.cancel("first cancellation")
    await asyncio.sleep(0)
    assert not running.done()
    running.cancel("repeated cancellation")
    await asyncio.sleep(0)
    assert not running.done()

    closing = asyncio.create_task(catalog.close())
    follow_on = asyncio.create_task(catalog.get_activity("world", _KIND, "dispatch"))
    await asyncio.sleep(0)
    assert not closing.done()
    assert not follow_on.done()

    release.set()
    with pytest.raises(asyncio.CancelledError, match="first cancellation"):
        await running
    await closing
    assert (await follow_on) is not None
    await catalog.close()
