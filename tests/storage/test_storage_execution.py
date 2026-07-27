# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for storage-owned Daft execution and app table operations."""

import asyncio
import threading
import time

import daft
import pytest
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException

from archetype.core.aio import AsyncUpdateManager
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.core.interfaces import CommitContext
from archetype.storage.service import AmbiguousCommitError, StorageService
from archetype.storage.session import configure_session
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.registry import WorldRegistry


class Position(Component):
    x: int = 0


class ManagedWriteProbe(Component):
    value: int = 0


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )


def _managed_rows(value: int) -> daft.DataFrame:
    return daft.from_pydict(
        {
            "entity_id": [value],
            "is_active": [True],
            "managedwriteprobe__value": [value],
        }
    )


@pytest.mark.contract("storage.execution.single_authority")
@pytest.mark.asyncio
async def test_terminal_materializations_share_one_execution_lane(monkeypatch):
    service = StorageService()
    first = daft.from_pydict({"value": [1]})
    second = daft.from_pydict({"value": [2]})
    original = daft.DataFrame.collect
    state_lock = threading.Lock()
    active = 0
    maximum = 0

    def observed_collect(frame, *args, **kwargs):
        nonlocal active, maximum
        with state_lock:
            active += 1
            maximum = max(maximum, active)
        time.sleep(0.03)
        try:
            return original(frame, *args, **kwargs)
        finally:
            with state_lock:
                active -= 1

    monkeypatch.setattr(daft.DataFrame, "collect", observed_collect)
    try:
        materialized = await asyncio.gather(
            service.materialize(first),
            service.materialize(second),
        )
        assert [frame.to_pydict()["value"] for frame in materialized] == [[1], [2]]
        assert maximum == 1
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_cached_tick_can_reenter_gate_during_threshold_flush(tmp_path):
    storage_service = StorageService()
    worlds = WorldLifecycle(storage_service, WorldRegistry())
    try:
        world = await worlds.create_world(
            WorldConfig(name="cached"),
            _storage(tmp_path),
            CacheConfig(flush_rows=1, flush_mb=1, global_mb=1, idle_sec=999),
        )
        await world.create_entity([Position(x=1)])

        await asyncio.wait_for(world.step(RunConfig()), timeout=10)

        assert world.tick == 1
    finally:
        await storage_service.shutdown()


@pytest.mark.asyncio
async def test_plain_and_conditional_appends_round_trip(tmp_path):
    service = StorageService()
    storage = _storage(tmp_path)
    try:
        assert (
            await service.append_table(
                storage,
                "plain_events",
                daft.from_pydict({"event_id": ["e1"]}),
            )
            == 1
        )
        assert (
            await service.append_missing(
                storage,
                "unique_events",
                daft.from_pydict({"event_id": ["e1"], "value": [1]}),
                key_columns=("event_id",),
            )
            == 1
        )
        assert (
            await service.append_missing(
                storage,
                "unique_events",
                daft.from_pydict({"event_id": ["e1"], "value": [1]}),
                key_columns=("event_id",),
            )
            == 0
        )

        assert (await service.read_table(storage, "plain_events")).to_pylist() == [
            {"event_id": "e1"}
        ]
        assert (await service.read_table(storage, "unique_events")).to_pylist() == [
            {"event_id": "e1", "value": 1}
        ]
    finally:
        await service.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("values", ([1, 1], [1, 2]))
async def test_conditional_append_rejects_duplicate_keys_within_candidate_batch(tmp_path, values):
    service = StorageService()
    storage = _storage(tmp_path)
    candidates = daft.from_pydict(
        {
            "event_id": ["same", "same"],
            "value": values,
        }
    )
    try:
        with pytest.raises(
            ValueError,
            match=r"conditional append .* contains duplicate key values .*event_id",
        ):
            await service.append_missing(
                storage,
                "unique_events",
                candidates,
                key_columns=("event_id",),
            )

        rows = (await service.read_table(storage, "unique_events")).to_pylist()
        assert rows == []
    finally:
        await service.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("null_key_column", ("event_id", "scope"))
async def test_conditional_append_rejects_null_keys_without_writing(tmp_path, null_key_column):
    service = StorageService()
    storage = _storage(tmp_path)
    values = {
        "event_id": ["e1", "e2"],
        "scope": ["primary", "secondary"],
        "value": [1, 2],
    }
    values[null_key_column][1] = None
    candidates = daft.from_pydict(values)
    try:
        for _ in range(2):
            with pytest.raises(
                ValueError,
                match=r"conditional append .* contains null key values .*key columns must be non-null",
            ):
                await service.append_missing(
                    storage,
                    "unique_events",
                    candidates,
                    key_columns=("event_id", "scope"),
                )

        rows = (await service.read_table(storage, "unique_events")).to_pylist()
        assert rows == []
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_app_table_schema_drift_fails_before_write(tmp_path):
    service = StorageService()
    storage = _storage(tmp_path)
    try:
        await service.append_table(
            storage,
            "events",
            daft.from_pydict({"event_id": ["e1"], "value": [1]}),
        )

        with pytest.raises(ValueError, match="different typed schema"):
            await service.append_table(
                storage,
                "events",
                daft.from_pydict({"event_id": ["e2"], "value": ["wrong"]}),
            )
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_concurrent_first_table_registration_recovers_losing_creator(tmp_path, monkeypatch):
    """A first-use creator that loses the catalog race resolves the winning table."""
    storage = _storage(tmp_path)
    first = StorageService(session=configure_session(storage))
    second = StorageService(session=configure_session(storage))
    rows = daft.from_pydict({"event_id": ["e1"]})
    barrier = threading.Barrier(2)
    try:
        first_store, second_store = await asyncio.gather(
            first.get_or_create_store(storage),
            second.get_or_create_store(storage),
        )
        for store in (first_store, second_store):
            catalog = store.session.current_catalog()
            original_has_table = catalog.has_table

            def synchronized_has_table(identifier, *, _has_table=original_has_table):
                exists = _has_table(identifier)
                assert not exists
                barrier.wait(timeout=10)
                return exists

            monkeypatch.setattr(catalog, "has_table", synchronized_has_table)

        tables = await asyncio.wait_for(
            asyncio.gather(
                asyncio.to_thread(first._ensure_table, first_store, "events", rows.schema()),
                asyncio.to_thread(second._ensure_table, second_store, "events", rows.schema()),
            ),
            timeout=20,
        )

        assert all(table.name == "events" for table in tables)
    finally:
        await first.shutdown()
        await second.shutdown()


@pytest.mark.contract("storage.execution.single_authority")
@pytest.mark.asyncio
async def test_managed_iceberg_conflict_retries_frozen_payload_once_per_writer(
    tmp_path,
    monkeypatch,
):
    """Two catalog clients commit one copy each after a real Iceberg CAS race."""
    storage = _storage(tmp_path)
    first = StorageService(session=configure_session(storage))
    second = StorageService(session=configure_session(storage))
    signature = Archetype.sig_from_components([ManagedWriteProbe()])
    barrier = threading.Barrier(2)
    try:
        first_store, second_store = await asyncio.gather(
            first.get_or_create_store(storage),
            second.get_or_create_store(storage),
        )
        first_updater = AsyncUpdateManager(first_store)
        second_updater = AsyncUpdateManager(second_store)
        await first_updater.update(
            _managed_rows(0),
            signature,
            0,
            "seed-world",
            "seed-run",
            commit=CommitContext(commit_token="seed-token", writer_epoch=1),
        )

        def arm(catalog):
            original = catalog._write_metadata
            armed = True

            def synchronized_metadata(*args, **kwargs):
                nonlocal armed
                result = original(*args, **kwargs)
                if armed:
                    armed = False
                    barrier.wait(timeout=10)
                return result

            catalog._write_metadata = synchronized_metadata

        table_id = Archetype.get_name(signature)
        for store in (first_store, second_store):
            native = store.session.current_catalog().get_table(f"ns.{table_id}")._inner
            arm(native.catalog)

        original_to_arrow = daft.DataFrame.to_arrow
        materializations = 0

        def counted_to_arrow(frame):
            nonlocal materializations
            materializations += 1
            return original_to_arrow(frame)

        monkeypatch.setattr(daft.DataFrame, "to_arrow", counted_to_arrow)
        await asyncio.wait_for(
            asyncio.gather(
                first_updater.update(
                    _managed_rows(1),
                    signature,
                    1,
                    "first-world",
                    "first-run",
                    commit=CommitContext(commit_token="first-token", writer_epoch=2),
                ),
                second_updater.update(
                    _managed_rows(2),
                    signature,
                    1,
                    "second-world",
                    "second-run",
                    commit=CommitContext(commit_token="second-token", writer_epoch=3),
                ),
            ),
            timeout=20,
        )

        assert materializations == 2
        first_rows = (
            await first_store.get_archetype_df(signature, "first-world", "first-run")
        ).to_pylist()
        second_rows = (
            await first_store.get_archetype_df(signature, "second-world", "second-run")
        ).to_pylist()
        assert [(row["managedwriteprobe__value"], row["commit_token"]) for row in first_rows] == [
            (1, "first-token")
        ]
        assert [(row["managedwriteprobe__value"], row["commit_token"]) for row in second_rows] == [
            (2, "second-token")
        ]
    finally:
        await first.shutdown()
        await second.shutdown()


@pytest.mark.contract("storage.execution.single_authority")
@pytest.mark.asyncio
async def test_managed_iceberg_conflict_retry_is_bounded_without_rematerializing(
    tmp_path,
    monkeypatch,
):
    storage = _storage(tmp_path)
    service = StorageService(session=configure_session(storage))
    signature = Archetype.sig_from_components([ManagedWriteProbe()])
    attempts = 0
    materializations = 0
    try:
        store = await service.get_or_create_store(storage)
        original_to_arrow = daft.DataFrame.to_arrow

        def counted_to_arrow(frame):
            nonlocal materializations
            materializations += 1
            return original_to_arrow(frame)

        def conflict(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            raise CommitFailedException("induced definite conflict")

        async def no_wait(_delay):
            return None

        monkeypatch.setattr(daft.DataFrame, "to_arrow", counted_to_arrow)
        monkeypatch.setattr(store, "_append_table", conflict)
        monkeypatch.setattr(asyncio, "sleep", no_wait)

        with pytest.raises(CommitFailedException, match="induced definite conflict"):
            await AsyncUpdateManager(store).update(
                _managed_rows(1),
                signature,
                1,
                "world",
                "run",
                commit=CommitContext(commit_token="bounded-token", writer_epoch=4),
            )

        assert attempts == 16
        assert materializations == 1
    finally:
        await service.shutdown()


@pytest.mark.contract("storage.execution.single_authority")
@pytest.mark.asyncio
async def test_managed_iceberg_ambiguous_commit_is_typed_and_never_replayed(
    tmp_path,
    monkeypatch,
):
    storage = _storage(tmp_path)
    service = StorageService(session=configure_session(storage))
    observer = StorageService(session=configure_session(storage))
    signature = Archetype.sig_from_components([ManagedWriteProbe()])
    token = "ambiguous-token"
    commit_calls = 0
    materializations = 0
    arrow_iterations = 0
    try:
        store = await service.get_or_create_store(
            storage,
            CacheConfig(flush_rows=1, flush_mb=1, global_mb=1, idle_sec=999),
        )
        await AsyncUpdateManager(store).update(
            _managed_rows(0),
            signature,
            0,
            "seed-world",
            "seed-run",
            commit=CommitContext(commit_token="seed-token", writer_epoch=1),
        )
        table_id = Archetype.get_name(signature)
        inner_store = store._inner
        native = inner_store.session.current_catalog().get_table(f"ns.{table_id}")._inner
        original_commit = native.catalog.commit_table
        original_to_arrow = daft.DataFrame.to_arrow
        original_to_arrow_iter = daft.DataFrame.to_arrow_iter

        def commit_then_lose_response(*args, **kwargs):
            nonlocal commit_calls
            commit_calls += 1
            original_commit(*args, **kwargs)
            raise CommitStateUnknownException("induced lost commit response")

        def counted_to_arrow(frame):
            nonlocal materializations
            materializations += 1
            return original_to_arrow(frame)

        def counted_to_arrow_iter(frame, *args, **kwargs):
            nonlocal arrow_iterations
            arrow_iterations += 1
            return original_to_arrow_iter(frame, *args, **kwargs)

        monkeypatch.setattr(native.catalog, "commit_table", commit_then_lose_response)
        monkeypatch.setattr(daft.DataFrame, "to_arrow", counted_to_arrow)
        monkeypatch.setattr(daft.DataFrame, "to_arrow_iter", counted_to_arrow_iter)

        with pytest.raises(AmbiguousCommitError) as raised:
            await AsyncUpdateManager(store).update(
                _managed_rows(7),
                signature,
                7,
                "ambiguous-world",
                "ambiguous-run",
                commit=CommitContext(commit_token=token, writer_epoch=9),
            )

        outcome = raised.value
        assert outcome.table_id == table_id
        assert outcome.physical_identity == (
            table_id,
            "ambiguous-world",
            "ambiguous-run",
            7,
        )
        assert outcome.commit_token == token
        assert outcome.writer_epoch == 9
        assert isinstance(outcome.__cause__, CommitStateUnknownException)
        assert materializations == 1
        assert commit_calls == 1
        iterations_after_ambiguity = arrow_iterations

        with pytest.raises(AmbiguousCommitError) as frozen:
            await AsyncUpdateManager(store).update(
                _managed_rows(8),
                signature,
                8,
                "later-world",
                "later-run",
                commit=CommitContext(commit_token="later-token", writer_epoch=10),
            )
        assert frozen.value.commit_token == token
        assert frozen.value.physical_identity == outcome.physical_identity
        assert materializations == 1
        assert commit_calls == 1
        assert arrow_iterations == iterations_after_ambiguity

        observer_store = await observer.get_or_create_store(storage)
        physical = (
            await observer_store.get_archetype_df(
                signature,
                "ambiguous-world",
                "ambiguous-run",
            )
        ).to_pylist()
        assert [(row["managedwriteprobe__value"], row["commit_token"]) for row in physical] == [
            (7, token)
        ]
    finally:
        try:
            await service.shutdown()
        except RuntimeError as exc:
            assert isinstance(exc.__cause__, AmbiguousCommitError)
        await observer.shutdown()


@pytest.mark.asyncio
async def test_cancelled_conditional_append_holds_until_commit_settles(tmp_path, monkeypatch):
    """Cancellation cannot orphan a live Iceberg commit thread (issue #704).

    The commit worker cannot be interrupted, so the service must not let
    CancelledError escape — releasing the execution gate — while the commit is
    still in flight: a retry issued after such an escape would race the
    orphaned commit and double-append the same payload.
    """
    service = StorageService()
    storage = _storage(tmp_path)
    rows = {"event_id": ["e1"], "value": [1]}
    order: list[str] = []
    commit_started = threading.Event()
    original_write = daft.DataFrame.write_iceberg

    def slow_write(frame, *args, **kwargs):
        commit_started.set()
        time.sleep(0.25)
        result = original_write(frame, *args, **kwargs)
        order.append("commit-settled")
        return result

    monkeypatch.setattr(daft.DataFrame, "write_iceberg", slow_write)
    try:
        append = asyncio.create_task(
            service.append_missing(
                storage,
                "unique_events",
                daft.from_pydict(rows),
                key_columns=("event_id",),
            )
        )
        assert await asyncio.to_thread(commit_started.wait, 10)
        append.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(append, timeout=30)
        order.append("cancellation-escaped")
        assert order == ["commit-settled", "cancellation-escaped"]

        monkeypatch.setattr(daft.DataFrame, "write_iceberg", original_write)
        retried = await asyncio.wait_for(
            service.append_missing(
                storage,
                "unique_events",
                daft.from_pydict(rows),
                key_columns=("event_id",),
            ),
            timeout=30,
        )
        assert retried == 0
        stored = (await service.read_table(storage, "unique_events")).to_pylist()
        assert stored == [{"event_id": "e1", "value": 1}]
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_cancelled_managed_commit_is_durable_and_never_replayed(tmp_path, monkeypatch):
    """A managed tick commit that lands during cancellation is not re-appended.

    The commit outcome settles before CancelledError escapes the store, the
    committed-signature bookkeeping reflects the landed commit, and retrying
    the identical payload resolves durably without a second physical copy
    (issue #704).
    """
    storage = _storage(tmp_path)
    service = StorageService(session=configure_session(storage))
    signature = Archetype.sig_from_components([ManagedWriteProbe()])
    order: list[str] = []
    commit_started = threading.Event()
    try:
        store = await service.get_or_create_store(storage)
        original_append_table = store._append_table

        def slow_append_table(table, frame):
            commit_started.set()
            time.sleep(0.25)
            original_append_table(table, frame)
            order.append("commit-settled")

        monkeypatch.setattr(store, "_append_table", slow_append_table)
        updater = AsyncUpdateManager(store)
        update = asyncio.create_task(
            updater.update(
                _managed_rows(5),
                signature,
                5,
                "cancel-world",
                "cancel-run",
                commit=CommitContext(commit_token="cancel-token", writer_epoch=2),
            )
        )
        assert await asyncio.to_thread(commit_started.wait, 10)
        update.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(update, timeout=30)
        order.append("cancellation-escaped")
        assert order == ["commit-settled", "cancellation-escaped"]

        # The slow write stays patched: a replay would append a second
        # "commit-settled" entry and a second physical row.
        await asyncio.wait_for(
            updater.update(
                _managed_rows(5),
                signature,
                5,
                "cancel-world",
                "cancel-run",
                commit=CommitContext(commit_token="cancel-token", writer_epoch=2),
            ),
            timeout=30,
        )
        assert order == ["commit-settled", "cancellation-escaped"]
        physical = (
            await store.get_archetype_df(signature, "cancel-world", "cancel-run")
        ).to_pylist()
        assert [(row["managedwriteprobe__value"], row["commit_token"]) for row in physical] == [
            (5, "cancel-token")
        ]
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_real_iceberg_conflict_recomputes_conditional_append(tmp_path):
    """A losing writer refreshes and anti-joins again instead of duplicating a key."""
    storage = _storage(tmp_path)
    first = StorageService(session=configure_session(storage))
    second = StorageService(session=configure_session(storage))
    barrier = threading.Barrier(2)
    try:
        await first.append_table(
            storage,
            "events",
            daft.from_pydict({"event_id": ["seed"], "writer": ["seed"]}),
        )

        def arm(catalog):
            original = catalog._write_metadata
            armed = True

            def synchronized_metadata(*args, **kwargs):
                nonlocal armed
                result = original(*args, **kwargs)
                if armed:
                    armed = False
                    barrier.wait(timeout=10)
                return result

            catalog._write_metadata = synchronized_metadata

        for service in (first, second):
            store = await service.get_or_create_store(storage)
            native = store.session.current_catalog().get_table("ns.events")._inner
            arm(native.catalog)

        results = await asyncio.wait_for(
            asyncio.gather(
                first.append_missing(
                    storage,
                    "events",
                    daft.from_pydict({"event_id": ["same"], "writer": ["first"]}),
                    key_columns=("event_id",),
                ),
                second.append_missing(
                    storage,
                    "events",
                    daft.from_pydict({"event_id": ["same"], "writer": ["second"]}),
                    key_columns=("event_id",),
                ),
            ),
            timeout=20,
        )

        assert sorted(results) == [0, 1]
        rows = (await first.read_table(storage, "events")).sort("event_id").to_pylist()
        assert [row["event_id"] for row in rows] == ["same", "seed"]
    finally:
        await first.shutdown()
        await second.shutdown()
