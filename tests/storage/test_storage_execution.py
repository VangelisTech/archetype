# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for storage-owned Daft execution and app table operations."""

import asyncio
import threading
import time

import daft
import pytest

from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.storage.service import StorageService
from archetype.storage.session import configure_session
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.registry import WorldRegistry


class Position(Component):
    x: int = 0


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
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
