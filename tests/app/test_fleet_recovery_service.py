# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for bounded, restart-safe fleet maintenance."""

import asyncio
import hashlib
from dataclasses import replace

import pytest

from archetype.app.recovery import (
    FleetRecoveryCursor,
    FleetRecoveryService,
    RecoveryExceptionStatus,
    RecoveryItemDisposition,
    RecoveryItemResult,
    RecoveryKind,
    RecoveryLimits,
    RecoveryPage,
    RecoveryPolicy,
    RecoverySubject,
    recovery_subject_key,
)
from archetype.app.storage import catalog as catalog_module
from archetype.app.storage.catalog import SqliteControlCatalog, WorldRecord

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("recovery.control.fenced"),
]

_FINGERPRINT = "b" * 64


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _world(world_id: str) -> WorldRecord:
    return WorldRecord(
        world_id=world_id,
        name=world_id,
        run_id="run-1",
        parent_world_id=None,
        status="active",
        tick_head=0,
    )


def _subject(world_id: str, name: str) -> RecoverySubject:
    authority_key = _digest(f"{world_id}:{name}")
    return RecoverySubject(
        world_id=world_id,
        kind=RecoveryKind.ARTIFACT_PUBLICATION,
        subject_key=recovery_subject_key(
            RecoveryKind.ARTIFACT_PUBLICATION,
            world_id,
            authority_key,
        ),
        authority_key=authority_key,
        cursor_after=authority_key,
    )


class _Source:
    kind = RecoveryKind.ARTIFACT_PUBLICATION

    def __init__(self, subjects: dict[str, list[RecoverySubject]]) -> None:
        self.subjects = subjects
        self.discover_calls: list[str] = []
        self.resolve_calls: list[tuple[str, str]] = []

    async def discover(self, world_id: str, cursor: str, *, limit: int) -> RecoveryPage:
        self.discover_calls.append(world_id)
        values = sorted(
            (
                subject
                for subject in self.subjects.get(world_id, ())
                if subject.cursor_after > cursor
            ),
            key=lambda subject: subject.cursor_after,
        )[:limit]
        full_page = len(values) == limit
        return RecoveryPage(
            subjects=tuple(values),
            next_cursor=values[-1].cursor_after if full_page else "",
            exhausted=not full_page,
        )

    async def resolve(self, world_id: str, authority_key: str) -> RecoverySubject | None:
        self.resolve_calls.append((world_id, authority_key))
        return next(
            (
                subject
                for subject in self.subjects.get(world_id, ())
                if subject.authority_key == authority_key
            ),
            None,
        )

    def complete(self, subject: RecoverySubject) -> None:
        self.subjects[subject.world_id] = [
            candidate
            for candidate in self.subjects.get(subject.world_id, ())
            if candidate.subject_key != subject.subject_key
        ]


class _Handler:
    kind = RecoveryKind.ARTIFACT_PUBLICATION

    def __init__(self, source: _Source, failures: dict[str, int] | None = None) -> None:
        self.source = source
        self.failures = failures or {}
        self.calls: list[str] = []

    async def recover(self, subject: RecoverySubject) -> RecoveryItemResult:
        self.calls.append(subject.subject_key)
        remaining = self.failures.get(subject.subject_key, 0)
        if remaining:
            self.failures[subject.subject_key] = remaining - 1
            raise RuntimeError("credential=must-never-be-persisted")
        self.source.complete(subject)
        return RecoveryItemResult(
            subject_key=subject.subject_key,
            disposition=RecoveryItemDisposition.COMPLETED,
        )


def _service(catalog, source, handler, *, policy=None) -> FleetRecoveryService:
    return FleetRecoveryService(
        catalog,
        storage_fingerprint=_FINGERPRINT,
        sources=(source,),
        handlers=(handler,),
        policy=policy
        or RecoveryPolicy(
            lease_ms=10_000,
            recurring_delay_ms=0,
            initial_retry_delay_ms=10,
            maximum_retry_delay_ms=10,
            jitter_basis_points=0,
        ),
    )


async def test_catalog_transport_timeout_is_not_misclassified_as_pass_deadline(
    tmp_path,
) -> None:
    class _TransportTimeoutCatalog:
        def __init__(self, delegate) -> None:
            self.delegate = delegate

        def __getattr__(self, name):
            return getattr(self.delegate, name)

        async def list_worlds_page(self, *, after_world_id: str, limit: int):
            raise TimeoutError("catalog transport unavailable")

    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        source = _Source({})
        service = _service(_TransportTimeoutCatalog(catalog), source, _Handler(source))

        with pytest.raises(TimeoutError, match="catalog transport unavailable"):
            await service.run_once(
                claimant="worker-a",
                limits=RecoveryLimits(max_elapsed_ms=5_000),
            )
    finally:
        await catalog.close()


@pytest.mark.parametrize(
    ("rows", "cursor", "page_size", "message"),
    [
        ([_world("world-1"), _world("world-2")], FleetRecoveryCursor(), 1, "over-delivered"),
        (
            [_world("world-2"), _world("world-1")],
            FleetRecoveryCursor(),
            2,
            "increase strictly",
        ),
        (
            [_world("world-1"), _world("world-1")],
            FleetRecoveryCursor(),
            2,
            "increase strictly",
        ),
        (
            [_world("world-1")],
            FleetRecoveryCursor(after_world_id="world-1"),
            1,
            "did not advance",
        ),
    ],
)
async def test_catalog_world_page_is_validated_before_any_sweep_dispatch(
    tmp_path,
    rows,
    cursor,
    page_size,
    message,
) -> None:
    class _WorldPageCatalog:
        def __init__(self, delegate) -> None:
            self.delegate = delegate

        def __getattr__(self, name):
            return getattr(self.delegate, name)

        async def list_worlds_page(self, *, after_world_id: str = "", limit: int):
            return rows

    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        source = _Source({})
        handler = _Handler(source)
        service = _service(_WorldPageCatalog(catalog), source, handler)

        with pytest.raises(ValueError, match=message):
            await service.run_once(
                claimant="worker-a",
                cursor=cursor,
                limits=RecoveryLimits(world_page_size=page_size, max_elapsed_ms=5_000),
            )
        assert handler.calls == []
        assert await catalog.list_recovery_sweeps("world-1") == []
    finally:
        await catalog.close()


@pytest.mark.parametrize("mode", ["unknown-outcome", "substituted-sweep"])
async def test_catalog_lease_result_is_closed_and_authority_bound_before_dispatch(
    tmp_path,
    mode,
) -> None:
    class _CorruptLeaseCatalog:
        def __init__(self, delegate) -> None:
            self.delegate = delegate

        def __getattr__(self, name):
            return getattr(self.delegate, name)

        async def lease_recovery_sweep(self, *args, **kwargs):
            outcome, row = await self.delegate.lease_recovery_sweep(*args, **kwargs)
            if mode == "unknown-outcome":
                return "invented", row
            return outcome, replace(row, world_id="world-substituted")

    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        source = _Source({"world-1": [_subject("world-1", "one")]})
        handler = _Handler(source)
        service = _service(_CorruptLeaseCatalog(catalog), source, handler)

        with pytest.raises(ValueError, match="unknown lease outcome|substituted sweep authority"):
            await service.run_once(
                claimant="worker-a",
                limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
            )
        assert handler.calls == []
    finally:
        await catalog.close()


async def test_secret_bearing_failure_metadata_never_crosses_recovery_durability_boundary(
    tmp_path,
) -> None:
    secret = "sk_live_DO_NOT_PERSIST_123"
    SecretBearingError = type(secret, (RuntimeError,), {})

    class _SecretSource(_Source):
        async def discover(self, world_id: str, cursor: str, *, limit: int) -> RecoveryPage:
            if world_id == "world-2":
                raise SecretBearingError(f"credential={secret}")
            return await super().discover(world_id, cursor, limit=limit)

    class _SecretHandler(_Handler):
        async def recover(self, subject: RecoverySubject) -> RecoveryItemResult:
            self.calls.append(subject.subject_key)
            raise SecretBearingError(f"credential={secret}")

    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        await catalog.register_world(_world("world-2"))
        source = _SecretSource({"world-1": [_subject("world-1", "one")]})
        service = _service(catalog, source, _SecretHandler(source))

        result = await service.run_once(
            claimant="worker-a",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )

        assert result.failed == 1
        [exception] = await catalog.list_recovery_exceptions(
            "world-1",
            kind=RecoveryKind.ARTIFACT_PUBLICATION.value,
            limit=10,
        )
        [failed_sweep] = await catalog.list_recovery_sweeps("world-2")
        assert exception.last_error_detail == ""
        assert failed_sweep.last_error_detail == ""
        assert secret not in repr((exception, failed_sweep))
    finally:
        await catalog.close()


async def test_run_once_pages_worlds_and_returns_restart_cursor(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-2"))
        await catalog.register_world(_world("world-1"))
        source = _Source(
            {
                "world-1": [_subject("world-1", "one")],
                "world-2": [_subject("world-2", "two")],
            }
        )
        handler = _Handler(source)
        service = _service(catalog, source, handler)
        limits = RecoveryLimits(
            world_page_size=1,
            max_sweeps=1,
            items_per_sweep=1,
            max_elapsed_ms=5_000,
        )

        first = await service.run_once(claimant="worker-a", limits=limits)
        assert first.worlds_examined == first.sweeps_acquired == first.completed == 1
        assert first.cursor == FleetRecoveryCursor(after_world_id="world-1")

        second = await service.run_once(
            claimant="worker-a",
            cursor=first.cursor,
            limits=limits,
        )
        assert second.completed == 1
        assert second.cursor == FleetRecoveryCursor(after_world_id="world-2")

        wrapped = await service.run_once(
            claimant="worker-a",
            cursor=second.cursor,
            limits=limits,
        )
        assert wrapped.worlds_examined == 0
        assert wrapped.cursor == FleetRecoveryCursor()
        assert handler.calls == [
            _subject("world-1", "one").subject_key,
            _subject("world-2", "two").subject_key,
        ]

        sweeps = await service.list_sweeps(limit=10)
        assert len(sweeps) == 2
        assert all(sweep.status.value == "idle" for sweep in sweeps)
        assert all(sweep.claimant_digest == "" for sweep in sweeps)
        assert all("worker-a" not in sweep.model_dump_json() for sweep in sweeps)
    finally:
        await catalog.close()


async def test_poison_subject_is_sparse_retried_and_does_not_block_tail(
    tmp_path, monkeypatch
) -> None:
    now_ms = [1_000_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        first = _subject("world-1", "poison")
        second = _subject("world-1", "tail")
        source = _Source({"world-1": [first, second]})
        handler = _Handler(source, failures={first.subject_key: 1})
        service = _service(catalog, source, handler)

        result = await service.run_once(
            claimant="worker-a",
            limits=RecoveryLimits(items_per_sweep=2, max_elapsed_ms=5_000),
        )
        assert result.items_examined == 2
        assert result.failed == 1
        assert result.completed == 1
        assert handler.calls == [
            subject.subject_key
            for subject in sorted((first, second), key=lambda item: item.cursor_after)
        ]

        exceptions = await service.list_exceptions(world_id="world-1")
        assert len(exceptions) == 1
        assert exceptions[0].status is RecoveryExceptionStatus.RETRY_WAIT
        assert exceptions[0].last_error_code == "handler_failed"
        serialized = exceptions[0].model_dump_json()
        assert "credential" not in serialized and "must-never" not in serialized

        now_ms[0] += 10
        recovered = await service.run_once(
            claimant="worker-b",
            limits=RecoveryLimits(items_per_sweep=2, max_elapsed_ms=5_000),
        )
        assert recovered.completed == 1 and recovered.failed == 0
        [resolved] = await service.list_exceptions(world_id="world-1")
        assert resolved.status is RecoveryExceptionStatus.RESOLVED
        assert source.subjects["world-1"] == []
    finally:
        await catalog.close()


async def test_restart_preserves_the_sparse_exceptions_original_attempt_ceiling(
    tmp_path,
    monkeypatch,
) -> None:
    now_ms = [1_250_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        subject = _subject("world-1", "poison")
        source = _Source({"world-1": [subject]})
        handler = _Handler(source, failures={subject.subject_key: 2})
        first = _service(
            catalog,
            source,
            handler,
            policy=RecoveryPolicy(
                lease_ms=10_000,
                recurring_delay_ms=0,
                initial_retry_delay_ms=10,
                maximum_retry_delay_ms=10,
                maximum_exception_attempts=8,
                jitter_basis_points=0,
            ),
        )

        initial = await first.run_once(
            claimant="worker-before-restart",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )
        assert initial.failed == 1
        now_ms[0] += 10

        restarted = _service(
            catalog,
            source,
            handler,
            policy=RecoveryPolicy(
                lease_ms=10_000,
                recurring_delay_ms=0,
                initial_retry_delay_ms=10,
                maximum_retry_delay_ms=10,
                maximum_exception_attempts=4,
                jitter_basis_points=0,
            ),
        )
        retried = await restarted.run_once(
            claimant="worker-after-restart",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )

        assert retried.failed == 1
        [exception] = await restarted.list_exceptions(world_id="world-1")
        assert exception.attempt_count == 2
        assert exception.maximum_attempts == 8
        [sweep] = await restarted.list_sweeps(world_id="world-1")
        assert sweep.last_error_code == ""
    finally:
        await catalog.close()


async def test_retry_pressure_reserves_capacity_for_fresh_work(tmp_path, monkeypatch) -> None:
    now_ms = [1_500_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        subjects = sorted(
            (_subject("world-1", f"item-{index}") for index in range(4)),
            key=lambda subject: subject.cursor_after,
        )
        retry_subjects = subjects[:3]
        fresh = subjects[3]
        source = _Source({"world-1": list(subjects)})
        handler = _Handler(source)
        service = _service(catalog, source, handler)
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            max_consecutive_failures=8,
        )
        _, lease = await catalog.lease_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease_ms=10_000,
        )
        for subject in retry_subjects:
            await catalog.retry_recovery_exception(
                "world-1",
                RecoveryKind.ARTIFACT_PUBLICATION.value,
                "seed-worker",
                lease.fence_epoch,
                subject_key=subject.subject_key,
                authority_key=subject.authority_key,
                expected_attempt_count=0,
                error_code="handler_failed",
                error_detail="RuntimeError",
                retry_delay_ms=0,
                max_attempts=8,
            )
        due = await catalog.list_recovery_exceptions(
            "world-1",
            kind=RecoveryKind.ARTIFACT_PUBLICATION.value,
            due_only=True,
            limit=10,
        )
        await catalog.yield_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease.fence_epoch,
            next_delay_ms=0,
        )

        result = await service.run_once(
            claimant="fleet-worker",
            limits=RecoveryLimits(items_per_sweep=4, max_elapsed_ms=5_000),
        )
        assert result.completed == 2
        assert set(handler.calls) == {due[0].subject_key, fresh.subject_key}
        assert fresh not in source.subjects["world-1"]
    finally:
        await catalog.close()


async def test_catalog_cannot_overdeliver_due_exceptions_past_item_budget(
    tmp_path,
    monkeypatch,
) -> None:
    class _OverdeliveringExceptionCatalog:
        def __init__(self, delegate) -> None:
            self.delegate = delegate

        def __getattr__(self, name):
            return getattr(self.delegate, name)

        async def list_recovery_exceptions(self, world_id: str, **kwargs):
            kwargs["limit"] = 10
            return await self.delegate.list_recovery_exceptions(world_id, **kwargs)

    now_ms = [1_625_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        subjects = sorted(
            (_subject("world-1", f"retry-{index}") for index in range(3)),
            key=lambda subject: subject.cursor_after,
        )
        source = _Source({"world-1": list(subjects)})
        handler = _Handler(source)
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            max_consecutive_failures=8,
        )
        _, lease = await catalog.lease_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease_ms=10_000,
        )
        for subject in subjects:
            await catalog.retry_recovery_exception(
                "world-1",
                RecoveryKind.ARTIFACT_PUBLICATION.value,
                "seed-worker",
                lease.fence_epoch,
                subject_key=subject.subject_key,
                authority_key=subject.authority_key,
                expected_attempt_count=0,
                error_code="handler_failed",
                error_detail="",
                retry_delay_ms=0,
                max_attempts=8,
            )
        await catalog.yield_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease.fence_epoch,
            next_delay_ms=0,
        )
        service = _service(_OverdeliveringExceptionCatalog(catalog), source, handler)

        result = await service.run_once(
            claimant="worker-a",
            limits=RecoveryLimits(items_per_sweep=2, max_elapsed_ms=5_000),
        )

        assert result.items_examined == result.completed == result.failed == 0
        assert handler.calls == []
        [sweep] = await catalog.list_recovery_sweeps("world-1")
        assert sweep.status == "retry_wait"
        assert sweep.last_error_code == "discovery_failed"
    finally:
        await catalog.close()


async def test_dead_letter_head_advances_cursor_and_cannot_hide_tail(tmp_path, monkeypatch) -> None:
    now_ms = [1_750_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        subjects = sorted(
            (_subject("world-1", f"item-{index}") for index in range(3)),
            key=lambda subject: subject.cursor_after,
        )
        dead, middle, tail = subjects
        source = _Source({"world-1": list(subjects)})
        handler = _Handler(source)
        service = _service(catalog, source, handler)
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            max_consecutive_failures=8,
        )
        _, lease = await catalog.lease_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease_ms=10_000,
        )
        await catalog.retry_recovery_exception(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease.fence_epoch,
            subject_key=dead.subject_key,
            authority_key=dead.authority_key,
            expected_attempt_count=0,
            error_code="handler_failed",
            error_detail="RuntimeError",
            retry_delay_ms=0,
            max_attempts=8,
            permanent=True,
        )
        await catalog.yield_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease.fence_epoch,
            next_delay_ms=0,
        )

        first = await service.run_once(
            claimant="fleet-worker",
            limits=RecoveryLimits(items_per_sweep=2, max_elapsed_ms=5_000),
        )
        assert first.completed == 1
        [after_first] = await service.list_sweeps(world_id="world-1")
        assert after_first.cursor == middle.cursor_after

        second = await service.run_once(
            claimant="fleet-worker",
            limits=RecoveryLimits(items_per_sweep=2, max_elapsed_ms=5_000),
        )
        assert second.completed == 1
        assert handler.calls == [middle.subject_key, tail.subject_key]
        assert source.subjects["world-1"] == [dead]
    finally:
        await catalog.close()


async def test_same_host_claimant_cannot_dispatch_concurrent_handlers(tmp_path) -> None:
    class _BlockingHandler(_Handler):
        def __init__(self, source: _Source) -> None:
            super().__init__(source)
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def recover(self, subject: RecoverySubject) -> RecoveryItemResult:
            self.started.set()
            await self.release.wait()
            return await super().recover(subject)

    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        source = _Source({"world-1": [_subject("world-1", "one")]})
        handler = _BlockingHandler(source)
        service = _service(catalog, source, handler)
        limits = RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000)

        first_task = asyncio.create_task(service.run_once(claimant="same-host", limits=limits))
        await asyncio.wait_for(handler.started.wait(), timeout=1)
        [active] = await service.list_sweeps(world_id="world-1")
        assert active.status.value == "leased"
        assert len(active.claimant_digest) == 64
        assert "same-host" not in active.model_dump_json()
        contender = await service.run_once(claimant="same-host", limits=limits)
        assert contender.lease_contentions == 1
        assert contender.items_examined == 0
        handler.release.set()
        first = await first_task
        assert first.completed == 1
        assert len(handler.calls) == 1
    finally:
        await catalog.close()


async def test_pass_deadline_cancels_slow_handler_without_advancing_page_cursor(
    tmp_path,
) -> None:
    class _SlowHandler(_Handler):
        async def recover(self, subject: RecoverySubject) -> RecoveryItemResult:
            if self.calls:
                await asyncio.sleep(10)
            return await super().recover(subject)

    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        subjects = sorted(
            (_subject("world-1", f"item-{index}") for index in range(3)),
            key=lambda subject: subject.cursor_after,
        )
        source = _Source({"world-1": list(subjects)})
        handler = _SlowHandler(source)
        service = _service(catalog, source, handler)

        result = await service.run_once(
            claimant="deadline-worker",
            limits=RecoveryLimits(items_per_sweep=3, max_elapsed_ms=1_000),
        )
        assert result.completed == 1
        assert result.elapsed_ms < 2_000
        [sweep] = await service.list_sweeps(world_id="world-1")
        assert sweep.cursor == subjects[0].cursor_after
        assert sweep.active_subject_key == subjects[1].authority_key
        assert sweep.cursor != subjects[-1].cursor_after
    finally:
        await catalog.close()


async def test_source_cannot_exceed_the_hard_item_budget(tmp_path) -> None:
    class _OverdeliveringSource(_Source):
        async def discover(self, world_id: str, cursor: str, *, limit: int) -> RecoveryPage:
            values = sorted(self.subjects[world_id], key=lambda subject: subject.cursor_after)
            return RecoveryPage(subjects=tuple(values), exhausted=True)

    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        source = _OverdeliveringSource(
            {
                "world-1": [
                    _subject("world-1", "one"),
                    _subject("world-1", "two"),
                ]
            }
        )
        handler = _Handler(source)
        service = _service(catalog, source, handler)

        result = await service.run_once(
            claimant="bounded-worker",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )

        assert result.items_examined == 0
        assert handler.calls == []
        [sweep] = await service.list_sweeps(world_id="world-1")
        assert sweep.status.value == "retry_wait"
        assert sweep.last_error_code.value == "source_corrupt"
    finally:
        await catalog.close()


async def test_structural_source_outputs_are_revalidated_before_dispatch(tmp_path) -> None:
    class _UntypedSource(_Source):
        async def discover(self, world_id: str, cursor: str, *, limit: int):
            return {"subjects": [], "next_cursor": "", "exhausted": 1}

    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        source = _UntypedSource({"world-1": []})
        handler = _Handler(source)
        service = _service(catalog, source, handler)

        result = await service.run_once(
            claimant="bounded-worker",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )

        assert result.items_examined == 0
        assert handler.calls == []
        [sweep] = await service.list_sweeps(world_id="world-1")
        assert sweep.status.value == "retry_wait"
        assert sweep.last_error_code.value == "source_corrupt"
    finally:
        await catalog.close()


async def test_constructed_source_and_handler_models_are_revalidated_before_progress(
    tmp_path,
) -> None:
    class _ConstructedPageSource(_Source):
        async def discover(self, world_id: str, cursor: str, *, limit: int) -> RecoveryPage:
            forged = RecoverySubject.model_construct(
                world_id=world_id,
                kind=RecoveryKind.ARTIFACT_PUBLICATION,
                subject_key="a" * 64,
                authority_key="b" * 64,
                cursor_after="b" * 64,
            )
            return RecoveryPage.model_construct(subjects=(forged,), exhausted=True)

    catalog = SqliteControlCatalog(tmp_path / "forged-page.db")
    try:
        await catalog.register_world(_world("world-1"))
        source = _ConstructedPageSource({"world-1": []})
        handler = _Handler(source)
        service = _service(catalog, source, handler)

        result = await service.run_once(
            claimant="worker-a",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )
        assert result.items_examined == 0
        assert handler.calls == []
        [sweep] = await service.list_sweeps(world_id="world-1")
        assert sweep.last_error_code.value == "source_corrupt"
    finally:
        await catalog.close()

    class _ConstructedResultHandler(_Handler):
        async def recover(self, subject: RecoverySubject) -> RecoveryItemResult:
            self.calls.append(subject.subject_key)
            return RecoveryItemResult.model_construct(
                subject_key=subject.subject_key,
                disposition="invented",
            )

    catalog = SqliteControlCatalog(tmp_path / "forged-result.db")
    try:
        await catalog.register_world(_world("world-1"))
        subject = _subject("world-1", "one")
        source = _Source({"world-1": [subject]})
        handler = _ConstructedResultHandler(source)
        service = _service(catalog, source, handler)

        result = await service.run_once(
            claimant="worker-a",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )
        assert result.failed == 1 and result.completed == 0
        [exception] = await service.list_exceptions(world_id="world-1")
        assert exception.subject_key == subject.subject_key
        assert source.subjects["world-1"] == [subject]
    finally:
        await catalog.close()


async def test_source_page_must_advance_beyond_the_durable_cursor(tmp_path) -> None:
    class _NonAdvancingSource(_Source):
        async def discover(self, world_id: str, cursor: str, *, limit: int) -> RecoveryPage:
            subject = RecoverySubject(
                world_id=world_id,
                kind=RecoveryKind.ARTIFACT_PUBLICATION,
                subject_key=recovery_subject_key(
                    RecoveryKind.ARTIFACT_PUBLICATION,
                    world_id,
                    cursor,
                ),
                authority_key=cursor,
                cursor_after=cursor,
            )
            return RecoveryPage(subjects=(subject,), exhausted=True)

    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        cursor = _digest("durable-cursor")
        source = _NonAdvancingSource({"world-1": []})
        handler = _Handler(source)
        service = _service(catalog, source, handler)
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            max_consecutive_failures=8,
        )
        _, lease = await catalog.lease_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease_ms=10_000,
        )
        await catalog.checkpoint_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease.fence_epoch,
            cursor=cursor,
        )
        await catalog.yield_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease.fence_epoch,
            next_delay_ms=0,
        )

        result = await service.run_once(
            claimant="bounded-worker",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )

        assert result.items_examined == 0
        assert handler.calls == []
        [sweep] = await service.list_sweeps(world_id="world-1")
        assert sweep.cursor == cursor
        assert sweep.last_error_code.value == "source_corrupt"
    finally:
        await catalog.close()


async def test_expired_sweep_resumes_active_subject_before_discovery(tmp_path, monkeypatch) -> None:
    now_ms = [2_000_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        active = _subject("world-1", "active")
        source = _Source({"world-1": [active]})
        handler = _Handler(source)
        service = _service(catalog, source, handler)
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            max_consecutive_failures=8,
        )
        _, old = await catalog.lease_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "old-worker",
            lease_ms=10,
        )
        await catalog.checkpoint_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "old-worker",
            old.fence_epoch,
            cursor="",
            active_subject_key=active.authority_key,
        )
        now_ms[0] += 11

        result = await service.run_once(
            claimant="new-worker",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )
        assert result.completed == 1
        assert source.resolve_calls == [("world-1", active.authority_key)]
        assert source.discover_calls == []
    finally:
        await catalog.close()


async def test_active_resolution_cannot_substitute_another_authority(tmp_path, monkeypatch) -> None:
    class _SubstitutingSource(_Source):
        async def resolve(self, world_id: str, authority_key: str) -> RecoverySubject | None:
            return _subject(world_id, "different-authority")

    now_ms = [2_500_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        active = _subject("world-1", "active")
        source = _SubstitutingSource({"world-1": [active]})
        handler = _Handler(source)
        service = _service(catalog, source, handler)
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            max_consecutive_failures=8,
        )
        _, old = await catalog.lease_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "old-worker",
            lease_ms=10,
        )
        await catalog.checkpoint_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "old-worker",
            old.fence_epoch,
            cursor="",
            active_subject_key=active.authority_key,
        )
        now_ms[0] += 11

        result = await service.run_once(
            claimant="new-worker",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )

        assert result.items_examined == 0
        assert handler.calls == []
        [sweep] = await service.list_sweeps(world_id="world-1")
        assert sweep.last_error_code.value == "source_corrupt"
        assert sweep.active_subject_key == active.authority_key
    finally:
        await catalog.close()


async def test_crash_recovery_resolver_cannot_advance_the_discovery_cursor(
    tmp_path, monkeypatch
) -> None:
    class _SimulatedProcessLoss(BaseException):
        pass

    class _CursorDriftSource(_Source):
        async def resolve(self, world_id: str, authority_key: str) -> RecoverySubject | None:
            subject = await super().resolve(world_id, authority_key)
            assert subject is not None
            return subject.model_copy(update={"cursor_after": "f" * 64})

    class _CrashAfterActiveClear:
        def __init__(self, delegate) -> None:
            self.delegate = delegate

        def __getattr__(self, name):
            return getattr(self.delegate, name)

        async def checkpoint_recovery_sweep(self, *args, **kwargs):
            result = await self.delegate.checkpoint_recovery_sweep(*args, **kwargs)
            if kwargs.get("active_subject_key", "") == "":
                raise _SimulatedProcessLoss
            return result

    now_ms = [2_625_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        active = _subject("world-1", "active")
        predecessor = "0" * 64
        source = _CursorDriftSource({"world-1": [active]})
        handler = _Handler(source)
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            max_consecutive_failures=8,
        )
        _, old = await catalog.lease_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "old-worker",
            lease_ms=10,
        )
        await catalog.checkpoint_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "old-worker",
            old.fence_epoch,
            cursor=predecessor,
            active_subject_key=active.authority_key,
        )
        now_ms[0] += 11
        service = _service(_CrashAfterActiveClear(catalog), source, handler)

        with pytest.raises(_SimulatedProcessLoss):
            await service.run_once(
                claimant="new-worker",
                limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
            )

        [persisted] = await catalog.list_recovery_sweeps("world-1")
        assert persisted.cursor == predecessor
        assert persisted.active_subject_key == ""
        assert persisted.cursor != "f" * 64
    finally:
        await catalog.close()


async def test_due_exception_resolution_cannot_substitute_subject_or_authority(
    tmp_path, monkeypatch
) -> None:
    class _SubstitutingSource(_Source):
        async def resolve(self, world_id: str, authority_key: str) -> RecoverySubject | None:
            return _subject(world_id, "substituted-retry")

    now_ms = [2_750_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        scheduled = _subject("world-1", "scheduled-retry")
        source = _SubstitutingSource({"world-1": [scheduled]})
        handler = _Handler(source)
        service = _service(catalog, source, handler)
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            max_consecutive_failures=8,
        )
        _, seed = await catalog.lease_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            lease_ms=10_000,
        )
        await catalog.retry_recovery_exception(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            seed.fence_epoch,
            subject_key=scheduled.subject_key,
            authority_key=scheduled.authority_key,
            expected_attempt_count=0,
            error_code="handler_failed",
            error_detail="RuntimeError",
            retry_delay_ms=0,
            max_attempts=8,
        )
        await catalog.yield_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "seed-worker",
            seed.fence_epoch,
            next_delay_ms=0,
        )

        result = await service.run_once(
            claimant="retry-worker",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )

        assert result.items_examined == 0
        assert handler.calls == []
        [sweep] = await service.list_sweeps(world_id="world-1")
        assert sweep.status.value == "retry_wait"
        assert sweep.last_error_code.value == "source_corrupt"
        [exception] = await service.list_exceptions(world_id="world-1")
        assert exception.subject_key == scheduled.subject_key
        assert exception.authority_key == scheduled.authority_key
        assert exception.status is RecoveryExceptionStatus.RETRY_WAIT
    finally:
        await catalog.close()


@pytest.mark.parametrize(
    ("permanent", "expected_status"),
    [
        (False, RecoveryExceptionStatus.RETRY_WAIT),
        (True, RecoveryExceptionStatus.DEAD_LETTER),
    ],
)
async def test_active_takeover_never_bypasses_sparse_backoff_or_dlq(
    tmp_path,
    monkeypatch,
    permanent,
    expected_status,
) -> None:
    now_ms = [3_000_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / f"catalog-{permanent}.db")
    try:
        await catalog.register_world(_world("world-1"))
        subject = _subject("world-1", "scheduled")
        source = _Source({"world-1": [subject]})
        handler = _Handler(source)
        service = _service(catalog, source, handler)
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            max_consecutive_failures=8,
        )
        _, old = await catalog.lease_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "old-worker",
            lease_ms=10,
        )
        exception = await catalog.retry_recovery_exception(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "old-worker",
            old.fence_epoch,
            subject_key=subject.subject_key,
            authority_key=subject.authority_key,
            expected_attempt_count=0,
            error_code="handler_failed",
            error_detail="RuntimeError",
            retry_delay_ms=100,
            max_attempts=2,
            permanent=permanent,
        )
        assert RecoveryExceptionStatus(exception.status) is expected_status
        await catalog.checkpoint_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            "old-worker",
            old.fence_epoch,
            cursor="",
            active_subject_key=subject.authority_key,
        )
        now_ms[0] += 11

        result = await service.run_once(
            claimant="new-worker",
            limits=RecoveryLimits(items_per_sweep=1, max_elapsed_ms=5_000),
        )
        assert result.items_examined == 0
        assert handler.calls == []
        [persisted] = await service.list_exceptions(world_id="world-1")
        assert persisted.status is expected_status
    finally:
        await catalog.close()


async def test_maintenance_service_rejects_every_model_capability_path(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    source = _Source({})
    handler = _Handler(source)
    try:
        service = _service(catalog, source, handler)
        with pytest.raises(ValueError, match="#504 supervisor"):
            await service.run_once(
                claimant="worker",
                kinds=(RecoveryKind.MISSION_MODEL_RECOVERY,),
            )

        source.kind = RecoveryKind.MISSION_MODEL_RECOVERY
        handler.kind = RecoveryKind.MISSION_MODEL_RECOVERY
        with pytest.raises(ValueError, match="cannot register model recovery"):
            FleetRecoveryService(
                catalog,
                storage_fingerprint=_FINGERPRINT,
                sources=(source,),
                handlers=(handler,),
            )

        class _DualCapability(_Handler):
            kind = RecoveryKind.ARTIFACT_PUBLICATION

            async def recover_model(self, subject: RecoverySubject) -> RecoveryItemResult:
                return await self.recover(subject)

        source.kind = RecoveryKind.ARTIFACT_PUBLICATION
        dual = _DualCapability(source)
        with pytest.raises(ValueError, match="cannot possess model recovery"):
            FleetRecoveryService(
                catalog,
                storage_fingerprint=_FINGERPRINT,
                sources=(source,),
                handlers=(dual,),
            )

        class _KindOnly:
            kind = RecoveryKind.ARTIFACT_PUBLICATION

        with pytest.raises(ValueError, match="source capability is unavailable"):
            FleetRecoveryService(
                catalog,
                storage_fingerprint=_FINGERPRINT,
                sources=(_KindOnly(),),  # type: ignore[arg-type]
                handlers=(handler,),
            )
        with pytest.raises(ValueError, match="handler capability is unavailable"):
            FleetRecoveryService(
                catalog,
                storage_fingerprint=_FINGERPRINT,
                sources=(source,),
                handlers=(_KindOnly(),),  # type: ignore[arg-type]
            )
    finally:
        await catalog.close()


async def test_run_once_rejects_unbounded_or_non_string_claimants(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    source = _Source({})
    handler = _Handler(source)
    try:
        service = _service(catalog, source, handler)
        with pytest.raises(TypeError, match="must be a string"):
            await service.run_once(claimant=True)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="at most 512"):
            await service.run_once(claimant="x" * 513)
        with pytest.raises(ValueError, match="between 1 and 10000"):
            await service.list_sweeps(limit=True)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="non-empty string"):
            await service.list_exceptions(world_id="")
        with pytest.raises(ValueError, match="at most 512"):
            await service.list_sweeps(world_id="w" * 513)
        with pytest.raises(ValueError, match="cannot exceed 10000"):
            await service.run_once(
                claimant="worker",
                limits=RecoveryLimits().model_copy(update={"items_per_sweep": 10_001}),
            )
        with pytest.raises(ValueError, match="world cursor is too long"):
            await service.run_once(
                claimant="worker",
                cursor=FleetRecoveryCursor().model_copy(update={"after_world_id": "w" * 513}),
            )
    finally:
        await catalog.close()


async def test_inspector_always_domain_hashes_even_digest_shaped_claimants(tmp_path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        await catalog.register_world(_world("world-1"))
        raw_claimant = "a" * 64
        await catalog.ensure_recovery_sweep(
            _FINGERPRINT,
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            max_consecutive_failures=8,
        )
        await catalog.lease_recovery_sweep(
            "world-1",
            RecoveryKind.ARTIFACT_PUBLICATION.value,
            raw_claimant,
            lease_ms=10_000,
        )
        source = _Source({})
        service = _service(catalog, source, _Handler(source))

        [projection] = await service.list_sweeps(world_id="world-1")
        assert len(projection.claimant_digest) == 64
        assert projection.claimant_digest != raw_claimant
    finally:
        await catalog.close()
