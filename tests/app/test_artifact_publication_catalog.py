# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Control-plane contracts for resumable artifact publication."""

import asyncio
import json
import threading
import time

import pytest

from archetype.app.storage.catalog import (
    ArtifactPublicationConflictError,
    ArtifactPublicationPendingError,
    SqliteControlCatalog,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("artifacts.bundle.publication_replay"),
]


async def _acquire(catalog, *, claimant="owner-1", digest="digest-1", lease=60.0):
    return await catalog.acquire_artifact_publication(
        world_id="world-1",
        run_id="run-1",
        attempt_id="attempt-1",
        idempotency_key="bundle-1",
        request_digest=digest,
        request_json=json.dumps({"request": 1}),
        claimant=claimant,
        retry_until_ms=int(time.time() * 1000) + 60_000,
        lease_seconds=lease,
    )


async def test_publication_records_request_before_io_and_cas_transitions(tmp_path):
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        outcome, publication = await _acquire(catalog)
        assert outcome == "acquired"
        assert publication.status == "PENDING"
        assert publication.request_json == '{"request": 1}'
        assert publication.attempt_count == 1

        with pytest.raises(ArtifactPublicationPendingError):
            await _acquire(catalog, claimant="owner-2")
        with pytest.raises(ArtifactPublicationConflictError):
            await _acquire(catalog, claimant="owner-2", digest="different")

        records_json = json.dumps([{"artifact_id": "a1"}])
        await catalog.record_artifact_uploads(
            "world-1",
            publication.publication_key,
            "owner-1",
            records_json,
            "s3://bucket/manifest",
        )
        uploaded = await catalog.get_artifact_publication("world-1", publication.publication_key)
        assert uploaded is not None
        assert uploaded.status == "UPLOADED"
        assert uploaded.records_json == records_json

        await catalog.complete_artifact_publication(
            "world-1", publication.publication_key, "owner-1", 42
        )
        await catalog.complete_artifact_publication(
            "world-1", publication.publication_key, "any-retry", 42
        )
        outcome, duplicate = await _acquire(catalog, claimant="owner-3")
        assert outcome == "duplicate"
        assert duplicate.status == "INDEXED"
        assert duplicate.index_snapshot_id == 42
        assert duplicate.manifest_uri == "s3://bucket/manifest"
    finally:
        await catalog.close()


async def test_failed_phase_is_due_and_recovered_without_losing_upload_metadata(tmp_path):
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        _, publication = await _acquire(catalog, lease=60.0)
        await catalog.record_artifact_uploads(
            "world-1",
            publication.publication_key,
            "owner-1",
            '[{"artifact_id":"a1"}]',
            "file:///manifest",
        )
        await catalog.fail_artifact_publication(
            "world-1",
            publication.publication_key,
            "owner-1",
            "index unavailable",
            retry_at=0.0,
        )

        due = await catalog.list_due_artifact_publications("world-1", now=time.time(), limit=10)
        assert [row.publication_key for row in due] == [publication.publication_key]
        assert due[0].status == "UPLOADED"

        outcome, recovered = await _acquire(catalog, claimant="reconciler")
        assert outcome == "recovered"
        assert recovered.status == "UPLOADED"
        assert recovered.records_json == '[{"artifact_id":"a1"}]'
        assert recovered.attempt_count == 2
    finally:
        await catalog.close()


async def test_fail_holds_write_lock_until_claimant_check_and_update_finish(tmp_path):
    """A stale failure reporter cannot corrupt a replacement claimant's lease."""

    class PauseAfterClaimantRead:
        def __init__(self, conn, selected: threading.Event, release: threading.Event):
            self._conn = conn
            self._selected = selected
            self._release = release

        def __enter__(self):
            self._conn.__enter__()
            return self

        def __exit__(self, *args):
            return self._conn.__exit__(*args)

        def execute(self, sql, parameters=()):
            cursor = self._conn.execute(sql, parameters)
            if "SELECT status, claimant FROM artifact_publications" in sql:
                self._selected.set()
                if not self._release.wait(timeout=2.0):
                    raise AssertionError("timed out waiting to release failure transaction")
            return cursor

        def __getattr__(self, name):
            return getattr(self._conn, name)

    path = tmp_path / "catalog.db"
    failing_catalog = SqliteControlCatalog(path)
    replacement_catalog = SqliteControlCatalog(path)
    release = threading.Event()
    selected = threading.Event()
    try:
        _, publication = await _acquire(failing_catalog, lease=0.0)
        failing_catalog._conn = PauseAfterClaimantRead(  # type: ignore[assignment]
            failing_catalog._connect_sync(), selected, release
        )

        fail_task = asyncio.create_task(
            failing_catalog.fail_artifact_publication(
                "world-1",
                publication.publication_key,
                "owner-1",
                "upload failed",
                retry_at=0.0,
            )
        )
        assert await asyncio.to_thread(selected.wait, 1.0)

        takeover_task = asyncio.create_task(_acquire(replacement_catalog, claimant="owner-2"))
        try:
            await asyncio.wait_for(asyncio.shield(takeover_task), timeout=0.2)
        except TimeoutError:
            takeover_was_blocked = True
        else:
            takeover_was_blocked = False
        finally:
            release.set()

        await fail_task
        outcome, replacement = await takeover_task
        assert takeover_was_blocked
        assert outcome == "recovered"
        assert replacement.claimant == "owner-2"
        assert replacement.lease_expires_at > time.time()
    finally:
        release.set()
        await failing_catalog.close()
        await replacement_catalog.close()


async def test_pending_publication_can_expire_but_uploaded_publication_cannot(tmp_path):
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        _, pending = await _acquire(catalog)
        await catalog.expire_artifact_publication(
            "world-1", pending.publication_key, "owner-1", "checkpoint expired"
        )
        outcome, expired = await _acquire(catalog, claimant="later")
        assert outcome == "expired"
        assert expired.status == "EXPIRED"
        assert expired.last_error == "checkpoint expired"
    finally:
        await catalog.close()
