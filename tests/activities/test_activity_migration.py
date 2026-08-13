# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Read-only Activity-history preflight contracts for storage migration."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from archetype.storage.activity_catalog import (
    ActivityAdmissionRecord,
    ActivityCatalogInspectionError,
    ActivityCatalogMigrationInspector,
    SqliteActivityCatalog,
    SqliteActivityCatalogMigrationInspector,
    inspect_sqlite_activity_catalog,
)

pytestmark = pytest.mark.asyncio


def _admission() -> ActivityAdmissionRecord:
    return ActivityAdmissionRecord(
        activity_id="activity-a",
        kind="missions.author",
        source_world_id="world-a",
        source_run_id="run-a",
        source_tick=4,
        source_visibility_token="manifest-4",
        input_ref="artifact://mission-input/a",
        input_digest="input-digest",
    )


async def test_absent_catalog_is_empty_without_creating_parent(tmp_path: Path) -> None:
    path = tmp_path / "absent" / "activities.db"

    inventory = inspect_sqlite_activity_catalog(path)

    assert inventory.is_empty
    assert not inventory.catalog_present
    assert inventory.schema_version is None
    assert not path.parent.exists()


async def test_initialized_empty_catalog_is_inspected_read_only(tmp_path: Path) -> None:
    path = tmp_path / "activities.db"
    catalog = SqliteActivityCatalog(path)
    await catalog.close()
    # Closing an unopened catalog does not initialize it; one read operation does.
    catalog = SqliteActivityCatalog(path)
    assert await catalog.get_activity("world", "kind", "missing") is None
    await catalog.close()
    before = path.read_bytes()

    inspector = SqliteActivityCatalogMigrationInspector(path)
    assert isinstance(inspector, ActivityCatalogMigrationInspector)
    inventory = await inspector.inspect_activity_catalog()

    assert inventory.is_empty
    assert inventory.catalog_present
    assert inventory.schema_version == 1
    assert path.read_bytes() == before


async def test_admission_alone_is_nonempty_history(tmp_path: Path) -> None:
    path = tmp_path / "activities.db"
    catalog = SqliteActivityCatalog(path)
    await catalog.admit_activity(_admission())
    await catalog.close()

    inventory = inspect_sqlite_activity_catalog(path)

    assert not inventory.is_empty
    assert inventory.activity_count == 1
    assert inventory.attempt_count == 0
    assert inventory.provider_operation_count == 0


async def test_attempt_and_provider_operation_are_counted(tmp_path: Path) -> None:
    path = tmp_path / "activities.db"
    catalog = SqliteActivityCatalog(path)
    await catalog.admit_activity(_admission())
    claim = await catalog.claim_activity(
        "world-a",
        "missions.author",
        "activity-a",
        "worker-a",
    )
    await catalog.bind_provider_operation(claim, "local", "provider-operation-a")
    await catalog.close()

    inventory = inspect_sqlite_activity_catalog(path)

    assert not inventory.is_empty
    assert inventory.activity_count == 1
    assert inventory.attempt_count == 1
    assert inventory.provider_operation_count == 1


async def test_completed_and_settled_activity_remains_nonempty_history(
    tmp_path: Path,
) -> None:
    path = tmp_path / "activities.db"
    catalog = SqliteActivityCatalog(path)
    await catalog.admit_activity(_admission())
    claim = await catalog.claim_activity(
        "world-a",
        "missions.author",
        "activity-a",
        "worker-a",
    )
    claim = await catalog.bind_provider_operation(
        claim,
        "local",
        "settled-provider-operation-a",
    )
    await catalog.record_activity_result(
        claim,
        result_ref="artifact://mission-result/a",
        result_digest="result-digest",
        result_media_type="application/json",
        result_size_bytes=42,
    )
    await catalog.settle_activity_observation(
        "world-a",
        "missions.author",
        "activity-a",
        observed_world_id="world-a",
        observed_run_id="run-a",
        observed_tick=5,
        observed_visibility_token="manifest-5",
        expected_result_digest="result-digest",
    )
    await catalog.close()

    inventory = inspect_sqlite_activity_catalog(path)

    assert not inventory.is_empty
    assert inventory.activity_count == 1
    assert inventory.attempt_count == 1


async def test_malformed_or_unknown_existing_catalog_fails_closed(tmp_path: Path) -> None:
    malformed = tmp_path / "malformed.db"
    malformed.write_bytes(b"not a SQLite catalog")
    with pytest.raises(ActivityCatalogInspectionError, match="inspected read-only"):
        inspect_sqlite_activity_catalog(malformed)
    assert malformed.read_bytes() == b"not a SQLite catalog"

    unknown = tmp_path / "unknown.db"
    connection = sqlite3.connect(unknown)
    connection.execute("CREATE TABLE unrelated (value TEXT)")
    connection.commit()
    connection.close()
    with pytest.raises(ActivityCatalogInspectionError, match="unsupported table inventory"):
        inspect_sqlite_activity_catalog(unknown)


async def test_unsupported_schema_version_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "activities.db"
    catalog = SqliteActivityCatalog(path)
    assert await catalog.get_activity("world", "kind", "missing") is None
    await catalog.close()
    connection = sqlite3.connect(path)
    connection.execute("UPDATE activity_catalog_meta SET value='999' WHERE key='schema_version'")
    connection.commit()
    connection.close()

    with pytest.raises(ActivityCatalogInspectionError, match="unsupported schema version"):
        inspect_sqlite_activity_catalog(path)


async def test_matching_table_names_with_malformed_schema_fail_closed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "malformed-schema.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE activity_catalog_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO activity_catalog_meta VALUES ('schema_version', '1');
        CREATE TABLE activities (value TEXT);
        CREATE TABLE activity_attempts (value TEXT);
        CREATE TABLE activity_provider_operations (value TEXT);
        """
    )
    connection.commit()
    connection.close()

    with pytest.raises(ActivityCatalogInspectionError, match="unsupported table schema"):
        inspect_sqlite_activity_catalog(path)
