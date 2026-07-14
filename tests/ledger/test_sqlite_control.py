# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import multiprocessing
import queue
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from archetype.ledger.errors import (
    DurableRecordCASMismatchError,
    DurableRecordConflictError,
)
from archetype.ledger.records import DurableRecord, iAsyncAtomicRecordStore
from archetype.ledger.sqlite_control import SQLiteAtomicRecordStore

_PROCESS_COUNT = 8


def _catalog_path(root: Path) -> Path:
    return root / "lancedb" / "tenant" / ".archetype" / "catalog-v1.sqlite3"


def _record(
    value: str,
    *,
    kind: str = "test-record",
    scope: str = "test-scope",
    key: str = "test-key",
    revision: int = 0,
    previous_digest: str | None = None,
    committed_at_ms: int = 1,
) -> DurableRecord:
    return DurableRecord.create(
        kind=kind,
        scope=scope,
        key=key,
        revision=revision,
        previous_digest=previous_digest,
        payload={"value": value},
        committed_at_ms=committed_at_ms,
    )


def _race_worker(
    database_path: str,
    barrier: Any,
    results: Any,
    operation: str,
    value: str,
    base_digest: str | None,
) -> None:
    """Run one operation in a fresh spawned interpreter."""

    try:
        store = SQLiteAtomicRecordStore(database_path, busy_timeout_ms=20_000)
        record = (
            _record(value)
            if operation == "put"
            else _record(value, revision=1, previous_digest=base_digest)
        )
        barrier.wait(timeout=60)
        if operation == "put":
            result = asyncio.run(store.put_if_absent(record))
        elif operation == "cas":
            result = asyncio.run(
                store.compare_and_swap(
                    record,
                    expected_revision=0,
                    expected_digest=base_digest,
                )
            )
        else:  # pragma: no cover - parent supplies a fixed operation
            raise ValueError(f"unknown race operation: {operation}")
        results.put(("ok", result.replayed, result.record.content_digest))
    except BaseException as exc:  # process boundary reports failures to the parent
        results.put(("error", type(exc).__name__, str(exc)))


def _run_process_race(
    database_path: Path,
    operation: str,
    values: list[str],
    *,
    base_digest: str | None = None,
) -> list[tuple]:
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(len(values))
    results = context.Queue()
    processes = [
        context.Process(
            target=_race_worker,
            args=(str(database_path), barrier, results, operation, value, base_digest),
        )
        for value in values
    ]
    for process in processes:
        process.start()

    received: list[tuple] = []
    try:
        for _ in processes:
            received.append(results.get(timeout=90))
        for process in processes:
            process.join(timeout=30)
        assert all(process.exitcode == 0 for process in processes)
    except (AssertionError, queue.Empty):
        for process in processes:
            if process.is_alive():
                process.terminate()
        for process in processes:
            process.join(timeout=5)
        raise
    finally:
        results.close()
        results.join_thread()
    return received


def _physical_row_count(database_path: Path) -> int:
    with closing(sqlite3.connect(database_path)) as connection:
        return connection.execute("SELECT COUNT(*) FROM durable_records").fetchone()[0]


@pytest.mark.asyncio
async def test_initialize_creates_strict_local_sqlite_profile(tmp_path: Path) -> None:
    database_path = _catalog_path(tmp_path)
    store = SQLiteAtomicRecordStore(database_path, busy_timeout_ms=7_000)

    await store.initialize()

    assert database_path.is_file()
    assert isinstance(store, iAsyncAtomicRecordStore)
    with closing(store._connect()) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 7_000
        schema = connection.execute("PRAGMA table_info(durable_records)").fetchall()
        primary_key = tuple(
            row["name"] for row in sorted(schema, key=lambda item: item["pk"]) if row["pk"]
        )
        assert primary_key == ("kind", "scope", "key", "revision")

    await store.shutdown()
    await store.shutdown()
    assert (await store.scan(kind="test-record")).collect().to_pylist() == []


@pytest.mark.asyncio
async def test_put_if_absent_replays_original_and_conflicts_on_new_content(
    tmp_path: Path,
) -> None:
    store = SQLiteAtomicRecordStore(_catalog_path(tmp_path))
    await store.initialize()
    original = _record("original", committed_at_ms=10)
    delayed_retry = _record("original", committed_at_ms=99)

    inserted = await store.put_if_absent(original)
    replayed = await store.put_if_absent(delayed_retry)

    assert inserted.record == original
    assert inserted.replayed is False
    assert replayed.record == original
    assert replayed.replayed is True
    assert replayed.record.committed_at_ms == 10

    conflicting = _record("different", committed_at_ms=100)
    with pytest.raises(DurableRecordConflictError) as raised:
        await store.put_if_absent(conflicting)
    assert raised.value.expected_digest == conflicting.content_digest
    assert raised.value.actual_digest == original.content_digest
    assert (
        await store.get(
            kind=original.kind,
            scope=original.scope,
            key=original.key,
        )
        == original
    )


@pytest.mark.asyncio
async def test_get_latest_and_scan_are_scoped_and_deterministic(tmp_path: Path) -> None:
    store = SQLiteAtomicRecordStore(_catalog_path(tmp_path))
    await store.initialize()
    first = _record("first", scope="a", key="b")
    second = _record(
        "second",
        scope="a",
        key="b",
        revision=1,
        previous_digest=first.content_digest,
    )
    other_scope = _record("other", scope="z", key="a")
    other_kind = _record("ignored", kind="another-kind", scope="a", key="a")
    for record in (first, second, other_scope, other_kind):
        await store.put_if_absent(record)

    assert await store.get_latest(kind="test-record", scope="a", key="b") == second
    assert await store.get(kind="test-record", scope="a", key="b", revision=5) is None
    rows = (await store.scan(kind="test-record")).collect().to_pylist()
    assert [(row["scope"], row["key"], row["revision"]) for row in rows] == [
        ("a", "b", 0),
        ("a", "b", 1),
        ("z", "a", 0),
    ]
    scoped_rows = (await store.scan(kind="test-record", scope="a")).collect().to_pylist()
    assert [row["content_digest"] for row in scoped_rows] == [
        first.content_digest,
        second.content_digest,
    ]
    assert (await store.scan(kind="missing")).collect().to_pylist() == []
    latest_rows = (await store.scan_latest(kind="test-record")).collect().to_pylist()
    assert [(row["scope"], row["key"], row["revision"]) for row in latest_rows] == [
        ("a", "b", 1),
        ("z", "a", 0),
    ]


@pytest.mark.asyncio
async def test_compare_and_swap_replays_exact_revision_after_head_advances(
    tmp_path: Path,
) -> None:
    store = SQLiteAtomicRecordStore(_catalog_path(tmp_path))
    await store.initialize()
    first = _record("zero", committed_at_ms=10)
    second = _record(
        "one",
        revision=1,
        previous_digest=first.content_digest,
        committed_at_ms=20,
    )
    third = _record(
        "two",
        revision=2,
        previous_digest=second.content_digest,
        committed_at_ms=30,
    )

    created = await store.compare_and_swap(
        first,
        expected_revision=None,
        expected_digest=None,
    )
    advanced = await store.compare_and_swap(
        second,
        expected_revision=0,
        expected_digest=first.content_digest,
    )
    await store.compare_and_swap(
        third,
        expected_revision=1,
        expected_digest=second.content_digest,
    )
    late_retry = _record(
        "one",
        revision=1,
        previous_digest=first.content_digest,
        committed_at_ms=999,
    )
    replayed = await store.compare_and_swap(
        late_retry,
        expected_revision=0,
        expected_digest=first.content_digest,
    )

    assert created.replayed is False
    assert advanced.replayed is False
    assert replayed.replayed is True
    assert replayed.record == second
    assert await store.get_latest(kind=first.kind, scope=first.scope, key=first.key) == third


@pytest.mark.asyncio
async def test_compare_and_swap_rejects_stale_or_divergent_requests(tmp_path: Path) -> None:
    store = SQLiteAtomicRecordStore(_catalog_path(tmp_path))
    await store.initialize()
    first = _record("winner")
    await store.compare_and_swap(first, expected_revision=None, expected_digest=None)

    fake_digest = "sha256:" + "f" * 64
    stale = _record(
        "stale",
        revision=1,
        previous_digest=fake_digest,
    )
    with pytest.raises(DurableRecordCASMismatchError) as raised:
        await store.compare_and_swap(
            stale,
            expected_revision=0,
            expected_digest=fake_digest,
        )
    assert raised.value.latest_record == first

    divergent = _record("loser")
    with pytest.raises(DurableRecordConflictError):
        await store.compare_and_swap(
            divergent,
            expected_revision=None,
            expected_digest=None,
        )
    assert _physical_row_count(store.database_path) == 1


@pytest.mark.asyncio
async def test_compare_and_swap_rejects_revision_gaps_before_writing(tmp_path: Path) -> None:
    store = SQLiteAtomicRecordStore(_catalog_path(tmp_path))
    await store.initialize()
    first = _record("zero")
    await store.compare_and_swap(first, expected_revision=None, expected_digest=None)
    skipped = _record(
        "two",
        revision=2,
        previous_digest=first.content_digest,
    )

    with pytest.raises(ValueError, match="target revision"):
        await store.compare_and_swap(
            skipped,
            expected_revision=0,
            expected_digest=first.content_digest,
        )
    assert _physical_row_count(store.database_path) == 1


def test_eight_process_identical_puts_create_one_row_and_replay_one_winner(
    tmp_path: Path,
) -> None:
    database_path = _catalog_path(tmp_path)
    asyncio.run(SQLiteAtomicRecordStore(database_path).initialize())

    results = _run_process_race(database_path, "put", ["identical"] * _PROCESS_COUNT)

    assert all(result[0] == "ok" for result in results), results
    assert sum(result[1] is False for result in results) == 1
    assert sum(result[1] is True for result in results) == _PROCESS_COUNT - 1
    assert len({result[2] for result in results}) == 1
    assert _physical_row_count(database_path) == 1


def test_eight_process_divergent_cas_has_one_winner_and_seven_conflicts(
    tmp_path: Path,
) -> None:
    database_path = _catalog_path(tmp_path)
    store = SQLiteAtomicRecordStore(database_path)
    base = _record("base")
    asyncio.run(store.initialize())
    asyncio.run(store.compare_and_swap(base, expected_revision=None, expected_digest=None))

    results = _run_process_race(
        database_path,
        "cas",
        [f"contender-{index}" for index in range(_PROCESS_COUNT)],
        base_digest=base.content_digest,
    )

    successes = [result for result in results if result[0] == "ok"]
    failures = [result for result in results if result[0] == "error"]
    assert len(successes) == 1, results
    assert successes[0][1] is False
    assert len(failures) == _PROCESS_COUNT - 1, results
    assert {failure[1] for failure in failures} == {"DurableRecordConflictError"}
    assert _physical_row_count(database_path) == 2
    with closing(sqlite3.connect(database_path)) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM durable_records WHERE revision = 1"
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM durable_records "
                "GROUP BY kind, scope, key, revision HAVING COUNT(*) > 1"
            ).fetchall()
            == []
        )
    latest = asyncio.run(store.get_latest(kind=base.kind, scope=base.scope, key=base.key))
    assert latest is not None
    assert latest.revision == 1
    assert latest.content_digest == successes[0][2]
