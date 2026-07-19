# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable control-catalog contracts for fleet recovery coordination."""

import asyncio
import hashlib
import sqlite3

import pytest

from archetype.app.storage import catalog as catalog_module
from archetype.app.storage.catalog import (
    RecoveryExceptionConflictError,
    RecoverySweepConflictError,
    RecoverySweepPendingError,
    RecoverySweepStaleError,
    SqliteControlCatalog,
    WorldRecord,
    recovery_exception_key,
    recovery_sweep_key,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("recovery.control.fenced"),
]

_FINGERPRINT = "a" * 64
_KIND = "mission_model_recovery"
_SUBJECT = hashlib.sha256(b"attempt-1").hexdigest()
_AUTHORITY = hashlib.sha256(b"attempt-authority-1").hexdigest()
_CURSOR = hashlib.sha256(b"page-7").hexdigest()


def _world(world_id: str, *, status: str = "active") -> WorldRecord:
    return WorldRecord(
        world_id=world_id,
        name=world_id,
        run_id="run-1",
        parent_world_id=None,
        status=status,
        tick_head=0,
    )


async def _catalog_with_world(tmp_path, name: str = "catalog.db"):
    catalog = SqliteControlCatalog(tmp_path / name)
    await catalog.register_world(_world("world-1"))
    return catalog


async def test_world_discovery_is_stable_and_paged(tmp_path):
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        for world_id in ("world-3", "world-1", "world-4", "world-2"):
            await catalog.register_world(_world(world_id))
        await catalog.set_world_status("world-2", "destroyed")

        first = await catalog.list_worlds_page(limit=2)
        second = await catalog.list_worlds_page(after_world_id=first[-1].world_id, limit=2)

        assert [record.world_id for record in first] == ["world-1", "world-2"]
        assert first[1].status == "destroyed"
        assert [record.world_id for record in second] == ["world-3", "world-4"]
        assert await catalog.list_worlds_page(after_world_id="world-4", limit=2) == []
        with pytest.raises(ValueError, match="between 1 and 10000"):
            await catalog.list_worlds_page(limit=0)
    finally:
        await catalog.close()


async def test_sweep_lease_takeover_preserves_crash_locality_and_rejects_stale_fence(
    tmp_path, monkeypatch
):
    now_ms = [1_000_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = await _catalog_with_world(tmp_path)
    try:
        created = await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            _KIND,
            max_consecutive_failures=3,
            initial_delay_ms=10,
        )
        assert created.sweep_key == recovery_sweep_key(_FINGERPRINT, "world-1", _KIND)
        assert created.status == "idle"
        assert created.next_due_at_ms == now_ms[0] + 10

        outcome, not_due = await catalog.lease_recovery_sweep(
            "world-1", _KIND, "worker-1", lease_ms=100
        )
        assert outcome == "not_due"
        assert not_due.fence_epoch == 0

        now_ms[0] += 10
        outcome, leased = await catalog.lease_recovery_sweep(
            "world-1", _KIND, "worker-1", lease_ms=100
        )
        assert (outcome, leased.status, leased.fence_epoch, leased.cycle) == (
            "acquired",
            "leased",
            1,
            1,
        )
        outcome, owned = await catalog.lease_recovery_sweep(
            "world-1", _KIND, "worker-1", lease_ms=100
        )
        assert outcome == "owned"
        assert owned.lease_expires_at_ms == leased.lease_expires_at_ms
        with pytest.raises(RecoverySweepPendingError):
            await catalog.lease_recovery_sweep("world-1", _KIND, "worker-2", lease_ms=100)

        with pytest.raises(ValueError, match="lowercase SHA-256"):
            await catalog.checkpoint_recovery_sweep(
                "world-1",
                _KIND,
                "worker-1",
                leased.fence_epoch,
                cursor="raw-page-token",
            )

        checkpoint = await catalog.checkpoint_recovery_sweep(
            "world-1",
            _KIND,
            "worker-1",
            leased.fence_epoch,
            cursor=_CURSOR,
            active_subject_key=_SUBJECT,
        )
        assert checkpoint.cursor == _CURSOR
        assert checkpoint.active_subject_key == _SUBJECT

        # The Worker clock, not a caller timestamp, expires the lease.
        now_ms[0] = leased.lease_expires_at_ms + 1
        outcome, recovered = await catalog.lease_recovery_sweep(
            "world-1", _KIND, "worker-2", lease_ms=200
        )
        assert outcome == "recovered"
        assert recovered.fence_epoch == leased.fence_epoch + 1
        assert recovered.cycle == leased.cycle + 1
        assert recovered.cursor == _CURSOR
        assert recovered.active_subject_key == _SUBJECT

        with pytest.raises(RecoverySweepStaleError):
            await catalog.checkpoint_recovery_sweep(
                "world-1",
                _KIND,
                "worker-1",
                leased.fence_epoch,
                cursor=hashlib.sha256(b"stale-cursor").hexdigest(),
            )
        renewed = await catalog.renew_recovery_sweep(
            "world-1",
            _KIND,
            "worker-2",
            recovered.fence_epoch,
            lease_ms=300,
        )
        assert renewed.lease_expires_at_ms == now_ms[0] + 300
    finally:
        await catalog.close()


async def test_sweep_failure_pause_and_redrive_graph_uses_server_backoff(tmp_path, monkeypatch):
    now_ms = [2_000_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = await _catalog_with_world(tmp_path)
    try:
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            _KIND,
            max_consecutive_failures=2,
        )
        _, first = await catalog.lease_recovery_sweep("world-1", _KIND, "worker", lease_ms=100)
        first = await catalog.checkpoint_recovery_sweep(
            "world-1",
            _KIND,
            "worker",
            first.fence_epoch,
            cursor=_CURSOR,
            active_subject_key=_SUBJECT,
        )
        failed = await catalog.fail_recovery_sweep(
            "world-1",
            _KIND,
            "worker",
            first.fence_epoch,
            error_code="handler_failed",
            error_detail="catalog unavailable",
            retry_delay_ms=25,
        )
        assert failed.status == "retry_wait"
        assert failed.consecutive_failures == 1
        assert failed.next_due_at_ms == now_ms[0] + 25
        assert failed.active_subject_key == _SUBJECT
        # Lost-response retries do not double-charge the failure budget.
        replay = await catalog.fail_recovery_sweep(
            "world-1",
            _KIND,
            "worker",
            first.fence_epoch,
            error_code="handler_failed",
            error_detail="catalog unavailable",
            retry_delay_ms=25,
        )
        assert replay == failed

        outcome, _ = await catalog.lease_recovery_sweep("world-1", _KIND, "worker", lease_ms=100)
        assert outcome == "not_due"
        now_ms[0] += 25
        _, second = await catalog.lease_recovery_sweep("world-1", _KIND, "worker", lease_ms=100)
        assert second.active_subject_key == _SUBJECT
        assert second.cursor == _CURSOR
        paused = await catalog.fail_recovery_sweep(
            "world-1",
            _KIND,
            "worker",
            second.fence_epoch,
            error_code="handler_failed",
            error_detail="still unavailable",
            retry_delay_ms=50,
        )
        assert paused.status == "paused"
        assert paused.paused_at_ms == now_ms[0]
        outcome, still_paused = await catalog.lease_recovery_sweep(
            "world-1", _KIND, "another-worker", lease_ms=100
        )
        assert outcome == "paused"
        assert still_paused == paused

        redriven = await catalog.redrive_recovery_sweep(
            "world-1",
            _KIND,
            expected_fence_epoch=paused.fence_epoch,
            delay_ms=10,
        )
        assert redriven.status == "idle"
        assert redriven.fence_epoch == paused.fence_epoch + 1
        assert redriven.consecutive_failures == 0
        assert redriven.claimant == ""
        assert redriven.active_subject_key == _SUBJECT
        assert redriven.cursor == _CURSOR
        assert (
            await catalog.redrive_recovery_sweep(
                "world-1",
                _KIND,
                expected_fence_epoch=paused.fence_epoch,
                delay_ms=10,
            )
            == redriven
        )
        with pytest.raises(RecoverySweepStaleError):
            await catalog.pause_recovery_sweep(
                "world-1",
                _KIND,
                "worker",
                second.fence_epoch,
                error_code="handler_failed",
                error_detail="stale worker",
            )
    finally:
        await catalog.close()


async def test_sparse_exception_retry_dlq_redrive_resolve_and_exact_lookup(tmp_path, monkeypatch):
    now_ms = [3_000_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = await _catalog_with_world(tmp_path)
    try:
        sweep = await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            _KIND,
            max_consecutive_failures=3,
        )
        _, lease = await catalog.lease_recovery_sweep("world-1", _KIND, "worker", lease_ms=10_000)
        first = await catalog.retry_recovery_exception(
            "world-1",
            _KIND,
            "worker",
            lease.fence_epoch,
            subject_key=_SUBJECT,
            authority_key=_AUTHORITY,
            expected_attempt_count=0,
            error_code="handler_failed",
            error_detail="provider timeout",
            retry_delay_ms=20,
            max_attempts=3,
        )
        assert first.exception_key == recovery_exception_key(sweep.sweep_key, _SUBJECT)
        assert first.status == "retry_wait"
        assert first.attempt_count == 1
        assert first.retry_at_ms == now_ms[0] + 20
        assert await catalog.get_recovery_exception("world-1", _KIND, first.exception_key) == first
        assert await catalog.list_recovery_exceptions("world-1", kind=_KIND, due_only=True) == []

        # The same expected count and receipt is an idempotent lost-response replay.
        replay = await catalog.retry_recovery_exception(
            "world-1",
            _KIND,
            "worker",
            lease.fence_epoch,
            subject_key=_SUBJECT,
            authority_key=_AUTHORITY,
            expected_attempt_count=0,
            error_code="handler_failed",
            error_detail="provider timeout",
            retry_delay_ms=20,
            max_attempts=3,
        )
        assert replay == first
        with pytest.raises(RecoveryExceptionConflictError):
            await catalog.retry_recovery_exception(
                "world-1",
                _KIND,
                "worker",
                lease.fence_epoch,
                subject_key=_SUBJECT,
                authority_key=hashlib.sha256(b"different-authority").hexdigest(),
                expected_attempt_count=1,
                error_code="handler_failed",
                error_detail="provider timeout",
                retry_delay_ms=20,
                max_attempts=3,
            )

        now_ms[0] += 20
        assert [
            record.exception_key
            for record in await catalog.list_recovery_exceptions(
                "world-1", kind=_KIND, due_only=True
            )
        ] == [first.exception_key]
        second = await catalog.retry_recovery_exception(
            "world-1",
            _KIND,
            "worker",
            lease.fence_epoch,
            subject_key=_SUBJECT,
            authority_key=_AUTHORITY,
            expected_attempt_count=1,
            error_code="handler_failed",
            error_detail="provider timeout again",
            retry_delay_ms=0,
            max_attempts=3,
        )
        dead = await catalog.retry_recovery_exception(
            "world-1",
            _KIND,
            "worker",
            lease.fence_epoch,
            subject_key=_SUBJECT,
            authority_key=_AUTHORITY,
            expected_attempt_count=second.attempt_count,
            error_code="handler_failed",
            error_detail="third failure",
            retry_delay_ms=0,
            max_attempts=3,
        )
        assert dead.status == "dead_letter"
        assert dead.attempt_count == 3
        assert dead.dead_lettered_at_ms == now_ms[0]
        assert await catalog.list_recovery_exceptions("world-1", status="dead_letter") == [dead]

        redriven = await catalog.redrive_recovery_exception(
            "world-1",
            _KIND,
            "worker",
            lease.fence_epoch,
            dead.exception_key,
            expected_attempt_count=dead.attempt_count,
            retry_delay_ms=5,
        )
        assert redriven.status == "retry_wait"
        assert redriven.attempt_count == dead.attempt_count
        resolved = await catalog.resolve_recovery_exception(
            "world-1",
            _KIND,
            "worker",
            lease.fence_epoch,
            dead.exception_key,
        )
        assert resolved.status == "resolved"
        assert resolved.resolved_at_ms == now_ms[0]
        assert (
            await catalog.resolve_recovery_exception(
                "world-1",
                _KIND,
                "worker",
                lease.fence_epoch,
                dead.exception_key,
            )
            == resolved
        )
    finally:
        await catalog.close()


async def test_only_one_catalog_instance_wins_a_live_sweep_lease(tmp_path):
    path = tmp_path / "catalog.db"
    first = SqliteControlCatalog(path)
    second = SqliteControlCatalog(path)
    try:
        await first.register_world(_world("world-1"))
        await first.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            _KIND,
            max_consecutive_failures=3,
        )
        results = await asyncio.gather(
            first.lease_recovery_sweep("world-1", _KIND, "worker-1", lease_ms=1000),
            second.lease_recovery_sweep("world-1", _KIND, "worker-2", lease_ms=1000),
            return_exceptions=True,
        )
        winners = [result for result in results if isinstance(result, tuple)]
        losers = [result for result in results if isinstance(result, BaseException)]
        assert len(winners) == 1
        assert winners[0][0] == "acquired"
        assert len(losers) == 1
        assert isinstance(losers[0], RecoverySweepPendingError)
    finally:
        await first.close()
        await second.close()


@pytest.mark.parametrize(
    ("invalid_fence", "error_type"),
    [
        (True, TypeError),
        (1.5, TypeError),
        ("1", TypeError),
        (-1, ValueError),
        (1 << 53, ValueError),
        (1 << 63, ValueError),
    ],
)
async def test_every_local_recovery_mutation_rejects_non_portable_fences(
    tmp_path, invalid_fence, error_type
):
    catalog = SqliteControlCatalog(tmp_path / "invalid-fence.db")
    calls = (
        lambda: catalog.renew_recovery_sweep(
            "world-1", _KIND, "worker", invalid_fence, lease_ms=100
        ),
        lambda: catalog.checkpoint_recovery_sweep(
            "world-1", _KIND, "worker", invalid_fence, cursor=""
        ),
        lambda: catalog.yield_recovery_sweep(
            "world-1", _KIND, "worker", invalid_fence, next_delay_ms=0
        ),
        lambda: catalog.fail_recovery_sweep(
            "world-1",
            _KIND,
            "worker",
            invalid_fence,
            error_code="handler_failed",
            error_detail="",
            retry_delay_ms=0,
        ),
        lambda: catalog.pause_recovery_sweep(
            "world-1",
            _KIND,
            "worker",
            invalid_fence,
            error_code="capability_unavailable",
            error_detail="",
        ),
        lambda: catalog.redrive_recovery_sweep(
            "world-1", _KIND, expected_fence_epoch=invalid_fence
        ),
        lambda: catalog.retry_recovery_exception(
            "world-1",
            _KIND,
            "worker",
            invalid_fence,
            subject_key=_SUBJECT,
            authority_key=_AUTHORITY,
            expected_attempt_count=0,
            error_code="handler_failed",
            error_detail="",
            retry_delay_ms=0,
            max_attempts=3,
        ),
        lambda: catalog.resolve_recovery_exception(
            "world-1", _KIND, "worker", invalid_fence, _SUBJECT
        ),
        lambda: catalog.redrive_recovery_exception(
            "world-1",
            _KIND,
            "worker",
            invalid_fence,
            _SUBJECT,
            expected_attempt_count=1,
        ),
    )
    try:
        for call in calls:
            with pytest.raises(error_type):
                await call()
    finally:
        await catalog.close()


@pytest.mark.parametrize("counter", ["fence_epoch", "cycle"])
async def test_local_recovery_lease_fails_closed_before_counter_increment(tmp_path, counter):
    path = tmp_path / "counter-exhaustion.db"
    catalog = await _catalog_with_world(tmp_path, path.name)
    try:
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            _KIND,
            max_consecutive_failures=3,
        )
        with sqlite3.connect(path) as conn:
            conn.execute(
                f"UPDATE fleet_recovery_sweeps SET {counter}=?",  # noqa: S608 -- fixed param
                ((1 << 53) - 1,),
            )
        with pytest.raises(RecoverySweepConflictError, match="portable counter"):
            await catalog.lease_recovery_sweep("world-1", _KIND, "worker", lease_ms=100)
    finally:
        await catalog.close()


async def test_local_recovery_kind_set_is_closed_before_storage(tmp_path):
    catalog = SqliteControlCatalog(tmp_path / "closed-kinds.db")
    try:
        with pytest.raises(ValueError, match="unsupported recovery kind"):
            await catalog.ensure_recovery_sweep(
                _FINGERPRINT,
                "world-1",
                "eighth_recovery_kind",
                max_consecutive_failures=3,
            )
    finally:
        await catalog.close()


async def test_local_sweep_transition_authority_rejects_corrupt_state_before_mutation(tmp_path):
    path = tmp_path / "corrupt-sweep.db"
    catalog = await _catalog_with_world(tmp_path, path.name)
    try:
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            _KIND,
            max_consecutive_failures=3,
        )
        with sqlite3.connect(path) as conn:
            conn.execute("UPDATE fleet_recovery_sweeps SET status='invented'")

        with pytest.raises(ValueError, match="unknown recovery sweep state"):
            await catalog.lease_recovery_sweep("world-1", _KIND, "worker", lease_ms=100)

        with sqlite3.connect(path) as conn:
            assert conn.execute(
                "SELECT status, fence_epoch, cycle FROM fleet_recovery_sweeps"
            ).fetchone() == ("invented", 0, 0)
    finally:
        await catalog.close()


async def test_local_exception_transition_authority_rejects_corrupt_state_before_mutation(
    tmp_path,
):
    path = tmp_path / "corrupt-exception.db"
    catalog = await _catalog_with_world(tmp_path, path.name)
    try:
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            _KIND,
            max_consecutive_failures=3,
        )
        _, leased = await catalog.lease_recovery_sweep("world-1", _KIND, "worker", lease_ms=10_000)
        recorded = await catalog.retry_recovery_exception(
            "world-1",
            _KIND,
            "worker",
            leased.fence_epoch,
            subject_key=_SUBJECT,
            authority_key=_AUTHORITY,
            expected_attempt_count=0,
            error_code="handler_failed",
            error_detail="",
            retry_delay_ms=0,
            max_attempts=3,
        )
        with sqlite3.connect(path) as conn:
            conn.execute(
                "UPDATE fleet_recovery_exceptions SET status='invented' WHERE exception_key=?",
                (recorded.exception_key,),
            )

        with pytest.raises(ValueError, match="unknown recovery exception state"):
            await catalog.resolve_recovery_exception(
                "world-1",
                _KIND,
                "worker",
                leased.fence_epoch,
                recorded.exception_key,
            )

        with sqlite3.connect(path) as conn:
            assert conn.execute(
                "SELECT status, resolved_at_ms FROM fleet_recovery_exceptions "
                "WHERE exception_key=?",
                (recorded.exception_key,),
            ).fetchone() == ("invented", None)
    finally:
        await catalog.close()


async def test_local_recovery_durable_inputs_require_exact_safe_types(tmp_path):
    catalog = SqliteControlCatalog(tmp_path / "invalid-durable-input.db")
    try:
        with pytest.raises(ValueError, match="lowercase SHA-256"):
            await catalog.ensure_recovery_sweep(
                "a" * 62 + "  ",
                "world-1",
                _KIND,
                max_consecutive_failures=3,
            )
        with pytest.raises(TypeError, match="permanent must be a boolean"):
            await catalog.retry_recovery_exception(
                "world-1",
                _KIND,
                "worker",
                1,
                subject_key=_SUBJECT,
                authority_key=_AUTHORITY,
                expected_attempt_count=0,
                error_code="handler_failed",
                error_detail="",
                retry_delay_ms=0,
                max_attempts=3,
                permanent=1,
            )
        for error_code, error_detail in ((1, ""), ("handler_failed", 1)):
            with pytest.raises(TypeError, match="must be a string"):
                await catalog.fail_recovery_sweep(
                    "world-1",
                    _KIND,
                    "worker",
                    1,
                    error_code=error_code,
                    error_detail=error_detail,
                    retry_delay_ms=0,
                )
    finally:
        await catalog.close()


async def test_v8_catalog_upgrade_installs_recovery_tables_and_updates_version(tmp_path):
    path = tmp_path / "catalog.db"
    catalog = SqliteControlCatalog(path)
    await catalog.register_world(_world("world-1"))
    await catalog.close()

    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE fleet_recovery_exceptions")
        conn.execute("DROP TABLE fleet_recovery_sweeps")
        conn.execute("UPDATE catalog_meta SET value='8' WHERE key='schema_version'")

    upgraded = SqliteControlCatalog(path)
    try:
        sweep = await upgraded.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            _KIND,
            max_consecutive_failures=3,
        )
        assert sweep.status == "idle"
        with sqlite3.connect(path) as conn:
            assert conn.execute(
                "SELECT value FROM catalog_meta WHERE key='schema_version'"
            ).fetchone() == ("9",)
            table_names = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert {"fleet_recovery_sweeps", "fleet_recovery_exceptions"} <= table_names
    finally:
        await upgraded.close()
