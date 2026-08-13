# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact local control-catalog migration contracts."""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from archetype.core.interfaces import StaleWriterError
from archetype.storage.catalog import (
    CatalogConflictError,
    CommandAdmission,
    SignatureRecord,
    SqliteControlCatalog,
    WorldRecord,
    control_snapshot_digest,
)


async def _populated_source(path: Path) -> SqliteControlCatalog:
    catalog = SqliteControlCatalog(path)
    await catalog.register_world(
        WorldRecord(
            world_id="active-world",
            name="Active",
            run_id="run-active",
            parent_world_id=None,
            status="active",
            tick_head=0,
        )
    )
    await catalog.register_world(
        WorldRecord(
            world_id="destroyed-world",
            name="Destroyed",
            run_id="run-destroyed",
            parent_world_id="active-world",
            status="active",
            tick_head=0,
        )
    )
    await catalog.register_signature(
        SignatureRecord(
            table_id="table-1",
            component_names=("Position", "Velocity"),
            schema_json='{"fields":[]}',
            fingerprint="schema-digest",
        )
    )

    active_epoch = await catalog.acquire_fence("active-world", "source-holder")
    first = await catalog.admit_commands(
        "active-world",
        [
            CommandAdmission(
                command_id="applied-command",
                scheduled_tick=1,
                priority=2,
                command_type="spawn",
                payload_json='{"entity":1}',
                payload_digest="payload-applied",
                version=3,
                principal_id="actor-1",
                origin="test",
                reserved_entity_id=11,
                max_attempts=4,
            )
        ],
    )
    assert len(first) == 1
    leased = await catalog.lease_commands("active-world", 1, "worker-1")
    assert [record.command_id for record in leased] == ["applied-command"]
    await catalog.publish_manifest(
        "active-world",
        "run-active",
        1,
        "commit-1",
        active_epoch,
        ["table-1"],
        command_ids=["applied-command"],
        lease_owner="worker-1",
    )

    await catalog.admit_commands(
        "active-world",
        [
            CommandAdmission(
                command_id="rejected-command",
                scheduled_tick=2,
                priority=1,
                command_type="update",
                payload_json='{"entity":2}',
                payload_digest="payload-rejected",
                version=1,
                principal_id=None,
                origin="test",
            )
        ],
    )
    rejected_lease = await catalog.lease_commands("active-world", 2, "worker-2")
    assert [record.command_id for record in rejected_lease] == ["rejected-command"]
    await catalog.fail_command(
        "active-world",
        "rejected-command",
        "worker-2",
        status="REJECTED",
        error_code="invalid",
        error_detail="invalid test command",
    )

    complete = await catalog.lease_evaluation(
        "active-world",
        "run-active",
        "evaluation-complete",
        "subject-a",
        "contract-a",
        "grader-a",
    )
    assert complete.acquired
    await catalog.complete_evaluation(
        "active-world",
        "run-active",
        "evaluation-complete",
        "grader-a",
    )
    retryable = await catalog.lease_evaluation(
        "active-world",
        "run-active",
        "evaluation-retryable",
        "subject-b",
        "contract-b",
        "grader-b",
    )
    assert retryable.acquired
    await catalog.release_evaluation(
        "active-world",
        "run-active",
        "evaluation-retryable",
        "grader-b",
    )

    pending_events = await catalog.read_outbox("active-world")
    await catalog.mark_outbox_projected(
        "active-world",
        [pending_events[0].event_id],
    )
    assert await catalog.acquire_fence("destroyed-world", "doomed-holder") == 1
    await catalog.set_world_status("destroyed-world", "destroyed")
    return catalog


@pytest.mark.asyncio
async def test_snapshot_stage_activation_round_trip_and_fence_floor(tmp_path: Path) -> None:
    source = await _populated_source(tmp_path / "source.db")
    destination = SqliteControlCatalog(tmp_path / "destination.db")
    try:
        snapshot = await source.export_migration_snapshot()
        assert {record.status for record in snapshot.commands} == {"APPLIED", "REJECTED"}
        assert {record.status for record in snapshot.evaluations} == {"COMPLETE", "RETRYABLE"}
        assert any(record.projected_at is not None for record in snapshot.outbox)
        assert any(record.projected_at is None for record in snapshot.outbox)
        digest = control_snapshot_digest(snapshot)

        reservation = await destination.reserve_migration(
            "migration-1",
            "plan-digest",
            '{"format_version":1}',
        )
        assert reservation.status == "RESERVED"
        await destination.stage_migration_control(
            "migration-1",
            "plan-digest",
            snapshot,
        )
        await destination.stage_migration_control(
            "migration-1",
            "plan-digest",
            snapshot,
        )
        assert await destination.list_worlds() == []
        assert await destination.get_world("active-world") is None
        staged = await destination.get_migration_reservation("migration-1")
        assert staged is not None
        assert staged.status == "STAGED"
        assert staged.control_snapshot_digest == digest

        await destination.activate_migration("migration-1", "plan-digest", snapshot)
        await destination.activate_migration("migration-1", "plan-digest", snapshot)
        assert await destination.export_migration_snapshot() == snapshot
        assert control_snapshot_digest(await destination.export_migration_snapshot()) == digest

        destroyed_floor = next(
            record.epoch for record in snapshot.fence_floors if record.world_id == "destroyed-world"
        )
        assert await destination.visible_tokens("destroyed-world", "run-destroyed") == {}
        with pytest.raises(StaleWriterError):
            await destination.publish_manifest(
                "destroyed-world",
                "run-destroyed",
                1,
                "must-not-activate",
                destroyed_floor,
                [],
            )
        assert (
            await destination.acquire_fence("destroyed-world", "destination-holder")
            == destroyed_floor + 1
        )

        await destination.complete_migration(
            "migration-1",
            "plan-digest",
            "receipt-digest",
            '{"receipt_digest":"receipt-digest"}',
        )
        await destination.complete_migration(
            "migration-1",
            "plan-digest",
            "receipt-digest",
            '{"receipt_digest":"receipt-digest"}',
        )
        complete_reservation = await destination.get_migration_reservation("migration-1")
        assert complete_reservation is not None
        assert complete_reservation.status == "COMPLETE"
        assert complete_reservation.receipt_digest == "receipt-digest"
        assert complete_reservation.receipt_json == '{"receipt_digest":"receipt-digest"}'
        with pytest.raises(CatalogConflictError):
            await destination.complete_migration(
                "migration-1",
                "plan-digest",
                "different-receipt",
                '{"receipt_digest":"different-receipt"}',
            )
        with pytest.raises(CatalogConflictError):
            await destination.complete_migration(
                "migration-1",
                "plan-digest",
                "receipt-digest",
                '{"receipt_digest":"receipt-digest","unexpected":true}',
            )
    finally:
        await source.close()
        await destination.close()


@pytest.mark.asyncio
async def test_reservation_is_exact_exclusive_and_requires_empty_catalog(tmp_path: Path) -> None:
    catalog = SqliteControlCatalog(tmp_path / "reserved.db")
    nonempty = SqliteControlCatalog(tmp_path / "nonempty.db")
    try:
        first = await catalog.reserve_migration("migration-1", "digest-1", '{"a":1}')
        replay = await catalog.reserve_migration("migration-1", "digest-1", '{"a":1}')
        assert replay == first
        assert await catalog.list_migration_reservations() == (first,)
        with pytest.raises(CatalogConflictError):
            await catalog.reserve_migration("migration-1", "changed", '{"a":1}')
        with pytest.raises(CatalogConflictError):
            await catalog.reserve_migration("migration-1", "digest-1", '{"a":2}')
        with pytest.raises(CatalogConflictError):
            await catalog.reserve_migration("migration-2", "digest-2", '{"a":2}')

        await nonempty.register_world(
            WorldRecord(
                world_id="existing",
                name=None,
                run_id=None,
                parent_world_id=None,
                status="active",
                tick_head=0,
            )
        )
        with pytest.raises(CatalogConflictError, match="not empty"):
            await nonempty.reserve_migration("migration", "digest", "{}")
    finally:
        await catalog.close()
        await nonempty.close()


@pytest.mark.asyncio
async def test_stage_rejects_unsettled_control_rows_without_partial_import(
    tmp_path: Path,
) -> None:
    source = await _populated_source(tmp_path / "source.db")
    destination = SqliteControlCatalog(tmp_path / "destination.db")
    try:
        snapshot = await source.export_migration_snapshot()
        unsettled_command = replace(snapshot.commands[0], status="PENDING")
        invalid = replace(snapshot, commands=(unsettled_command, *snapshot.commands[1:]))
        await destination.reserve_migration("migration", "plan", "{}")
        with pytest.raises(ValueError, match="unsettled"):
            await destination.stage_migration_control("migration", "plan", invalid)
        assert await destination.list_worlds() == []
        assert await destination.list_signatures() == []
        reservation = await destination.get_migration_reservation("migration")
        assert reservation is not None
        assert reservation.status == "RESERVED"

        running_evaluation = replace(
            snapshot.evaluations[0],
            status="RUNNING",
            owner="grader",
            lease_expires_at=123.0,
        )
        invalid = replace(
            snapshot,
            evaluations=(running_evaluation, *snapshot.evaluations[1:]),
        )
        with pytest.raises(ValueError, match="unsettled"):
            await destination.stage_migration_control("migration", "plan", invalid)
    finally:
        await source.close()
        await destination.close()


@pytest.mark.asyncio
async def test_schema_v10_is_upgraded_additively(tmp_path: Path) -> None:
    path = tmp_path / "v10.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE catalog_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO catalog_meta (key, value) VALUES ('schema_version', '10');
            CREATE TABLE worlds (
                world_id TEXT PRIMARY KEY,
                name TEXT,
                run_id TEXT,
                parent_world_id TEXT,
                status TEXT NOT NULL,
                tick_head INTEGER NOT NULL DEFAULT 0,
                writer_mode TEXT NOT NULL DEFAULT 'resumable'
            );
            INSERT INTO worlds
                (world_id, name, run_id, status, tick_head, writer_mode)
                VALUES ('legacy-world', 'Legacy', 'legacy-run', 'destroyed', 7, 'resumable');
            """
        )
        conn.commit()
    finally:
        conn.close()

    catalog = SqliteControlCatalog(path)
    try:
        snapshot = await catalog.export_migration_snapshot()
        assert snapshot.catalog_schema_version == 11
        assert snapshot.worlds == (
            WorldRecord(
                world_id="legacy-world",
                name="Legacy",
                run_id="legacy-run",
                parent_world_id=None,
                status="destroyed",
                tick_head=7,
            ),
        )
        assert await catalog.list_migration_reservations() == ()
    finally:
        await catalog.close()


@pytest.mark.asyncio
async def test_early_schema_v11_adds_recoverable_receipt_without_version_bump(
    tmp_path: Path,
) -> None:
    path = tmp_path / "early-v11.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE catalog_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO catalog_meta (key, value) VALUES ('schema_version', '11');
            CREATE TABLE migration_reservations (
                migration_id TEXT PRIMARY KEY,
                plan_digest TEXT NOT NULL,
                plan_json TEXT NOT NULL,
                status TEXT NOT NULL,
                control_snapshot_digest TEXT,
                receipt_digest TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()

    catalog = SqliteControlCatalog(path)
    try:
        assert await catalog.list_migration_reservations() == ()
    finally:
        await catalog.close()

    conn = sqlite3.connect(path)
    try:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(migration_reservations)")}
        version = conn.execute(
            "SELECT value FROM catalog_meta WHERE key='schema_version'"
        ).fetchone()
        assert version == ("11",)
        assert "receipt_json" in columns
    finally:
        conn.close()
