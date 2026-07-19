# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Control-plane contracts for resumable artifact publication."""

import asyncio
import json
import threading

import pytest

from archetype.app.storage import catalog as catalog_module
from archetype.app.storage.catalog import (
    ArtifactPublicationConflictError,
    ArtifactPublicationExpiredError,
    ArtifactPublicationPendingError,
    SqliteControlCatalog,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("artifacts.bundle.publication_replay"),
]


async def _acquire(
    catalog,
    *,
    claimant="owner-1",
    digest="digest-1",
    lease_ms=60_000,
    idempotency_key="bundle-1",
):
    return await catalog.acquire_artifact_publication(
        world_id="world-1",
        run_id="run-1",
        attempt_id="attempt-1",
        idempotency_key=idempotency_key,
        request_digest=digest,
        request_json=json.dumps({"request": 1}),
        claimant=claimant,
        retry_window_ms=60_000,
        lease_ms=lease_ms,
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
        _, publication = await _acquire(catalog, lease_ms=60_000)
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
            retry_delay_ms=0,
        )

        due = await catalog.list_due_artifact_publications("world-1", limit=10)
        assert [row.publication_key for row in due] == [publication.publication_key]
        assert vars(due[0]) == {"publication_key": publication.publication_key}

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
            if "SELECT * FROM artifact_publications" in sql:
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
        _, publication = await _acquire(failing_catalog, lease_ms=60_000)
        failing_catalog._conn = PauseAfterClaimantRead(  # type: ignore[assignment]
            failing_catalog._connect_sync(), selected, release
        )

        fail_task = asyncio.create_task(
            failing_catalog.fail_artifact_publication(
                "world-1",
                publication.publication_key,
                "owner-1",
                "upload failed",
                retry_delay_ms=0,
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
        assert replacement.lease_expires_at > catalog_module._now_ms() / 1000
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


async def test_due_publication_pages_advance_lexicographically_past_sparse_work(tmp_path):
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        publications = []
        for index in range(3):
            _, publication = await _acquire(
                catalog,
                claimant=f"owner-{index}",
                lease_ms=60_000,
                idempotency_key=f"bundle-{index}",
            )
            await catalog.fail_artifact_publication(
                "world-1",
                publication.publication_key,
                f"owner-{index}",
                "make publication immediately due",
                retry_delay_ms=0,
            )
            publications.append(publication)
        expected_keys = sorted(publication.publication_key for publication in publications)
        first = await catalog.list_due_artifact_publications("world-1", limit=2)
        second = await catalog.list_due_artifact_publications(
            "world-1",
            limit=2,
            after_publication_key=first[-1].publication_key,
        )

        assert [publication.publication_key for publication in first] == expected_keys[:2]
        assert [publication.publication_key for publication in second] == expected_keys[2:]
        with pytest.raises(ValueError, match="lowercase SHA-256"):
            await catalog.list_due_artifact_publications(
                "world-1",
                after_publication_key="raw-publication-id",
            )
    finally:
        await catalog.close()


async def test_exact_recovery_uses_catalog_clock_and_preserves_uploaded_authority(
    tmp_path, monkeypatch
):
    now_ms = [1_000_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        outcome, missing = await catalog.recover_artifact_publication(
            "world-1", "f" * 64, "reconciler", lease_ms=1_000
        )
        assert outcome == "obsolete" and missing is None

        _, publication = await _acquire(catalog, lease_ms=1_000)
        original_lease = publication.lease_expires_at
        now_ms[0] += 100
        outcome, owned = await catalog.recover_artifact_publication(
            "world-1", publication.publication_key, "owner-1", lease_ms=2_000
        )
        assert outcome == "owned" and owned is not None
        assert owned.attempt_count == 1
        assert owned.lease_expires_at == (now_ms[0] + 2_000) / 1000
        assert owned.lease_expires_at > original_lease
        with pytest.raises(ArtifactPublicationPendingError):
            await catalog.recover_artifact_publication(
                "world-1", publication.publication_key, "other", lease_ms=1_000
            )

        now_ms[0] += 2_001
        outcome, recovered = await catalog.recover_artifact_publication(
            "world-1", publication.publication_key, "other", lease_ms=1_000
        )
        assert outcome == "recovered" and recovered is not None
        assert recovered.attempt_count == 2

        _, deadline = await catalog.acquire_artifact_publication(
            world_id="world-1",
            run_id="run-1",
            attempt_id="attempt-deadline",
            idempotency_key="deadline-before-live-owner",
            request_digest="digest-deadline",
            request_json="{}",
            claimant="deadline-owner",
            retry_window_ms=100,
            lease_ms=1_000,
        )
        now_ms[0] += 101
        outcome, expired = await catalog.recover_artifact_publication(
            "world-1", deadline.publication_key, "different-owner", lease_ms=1_000
        )
        assert outcome == "expired" and expired is not None
        assert expired.status == "EXPIRED"

        _, uploaded = await catalog.acquire_artifact_publication(
            world_id="world-1",
            run_id="run-1",
            attempt_id="attempt-uploaded",
            idempotency_key="uploaded-outlives-deadline",
            request_digest="digest-uploaded",
            request_json="{}",
            claimant="uploader",
            retry_window_ms=100,
            lease_ms=1_000,
        )
        await catalog.record_artifact_uploads(
            "world-1", uploaded.publication_key, "uploader", "[]", "file:///manifest"
        )
        now_ms[0] += 1_001
        outcome, uploaded_recovery = await catalog.recover_artifact_publication(
            "world-1", uploaded.publication_key, "indexer", lease_ms=1_000
        )
        assert outcome == "recovered" and uploaded_recovery is not None
        assert uploaded_recovery.status == "UPLOADED"
        await catalog.complete_artifact_publication(
            "world-1", uploaded.publication_key, "indexer", 42
        )
        outcome, duplicate = await catalog.recover_artifact_publication(
            "world-1", uploaded.publication_key, "later", lease_ms=1_000
        )
        assert outcome == "duplicate" and duplicate is not None
        assert duplicate.status == "INDEXED"
    finally:
        await catalog.close()


async def test_source_mutations_require_a_live_catalog_lease(tmp_path, monkeypatch):
    now_ms = [2_000_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        _, pending = await _acquire(catalog, lease_ms=100)
        now_ms[0] += 101
        with pytest.raises(ArtifactPublicationPendingError, match="lease expired"):
            await catalog.renew_artifact_publication(
                "world-1", pending.publication_key, "owner-1", lease_seconds=1.0
            )
        with pytest.raises(ArtifactPublicationPendingError, match="lease expired"):
            await catalog.record_artifact_uploads(
                "world-1", pending.publication_key, "owner-1", "[]", "file:///manifest"
            )
        with pytest.raises(ArtifactPublicationPendingError, match="lease expired"):
            await catalog.expire_artifact_publication(
                "world-1", pending.publication_key, "owner-1", "manual expiry"
            )
        with pytest.raises(ArtifactPublicationPendingError, match="lease expired"):
            await catalog.fail_artifact_publication(
                "world-1",
                pending.publication_key,
                "owner-1",
                "late failure",
                retry_delay_ms=0,
            )

        _, uploaded = await _acquire(
            catalog,
            lease_ms=100,
            idempotency_key="uploaded-stale-completion",
        )
        await catalog.record_artifact_uploads(
            "world-1", uploaded.publication_key, "owner-1", "[]", "file:///manifest"
        )
        now_ms[0] += 101
        with pytest.raises(ArtifactPublicationPendingError, match="lease expired"):
            await catalog.complete_artifact_publication(
                "world-1", uploaded.publication_key, "owner-1", 42
            )
    finally:
        await catalog.close()


async def test_pending_upload_crossing_retry_deadline_commits_expiry(tmp_path, monkeypatch):
    now_ms = [3_000_000]
    monkeypatch.setattr(catalog_module, "_now_ms", lambda: now_ms[0])
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        _, publication = await catalog.acquire_artifact_publication(
            world_id="world-1",
            run_id="run-1",
            attempt_id="attempt-1",
            idempotency_key="deadline-upload",
            request_digest="digest-1",
            request_json="{}",
            claimant="owner-1",
            retry_window_ms=50,
            lease_ms=1_000,
        )
        now_ms[0] += 51
        with pytest.raises(ArtifactPublicationExpiredError):
            await catalog.record_artifact_uploads(
                "world-1", publication.publication_key, "owner-1", "[]", "file:///manifest"
            )
        expired = await catalog.get_artifact_publication("world-1", publication.publication_key)
        assert expired is not None and expired.status == "EXPIRED"
    finally:
        await catalog.close()


@pytest.mark.parametrize("lease_ms", [0, True, 1.0, "1"])
async def test_artifact_lease_duration_is_strictly_positive(tmp_path, lease_ms):
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        with pytest.raises((TypeError, ValueError)):
            await _acquire(catalog, lease_ms=lease_ms)
        with pytest.raises((TypeError, ValueError)):
            await catalog.recover_artifact_publication(
                "world-1", "f" * 64, "owner", lease_ms=lease_ms
            )
    finally:
        await catalog.close()


@pytest.mark.parametrize("lease_seconds", [True, 0, -1, float("inf"), float("nan"), 86_401])
async def test_artifact_renewal_duration_is_finite_and_bounded(tmp_path, lease_seconds):
    catalog = SqliteControlCatalog(tmp_path / "catalog.db")
    try:
        with pytest.raises((TypeError, ValueError)):
            await catalog.renew_artifact_publication(
                "world-1", "f" * 64, "owner", lease_seconds=lease_seconds
            )
    finally:
        await catalog.close()


@pytest.mark.parametrize(
    ("field", "statement", "value", "message"),
    [
        (
            "status",
            "UPDATE artifact_publications SET status=? WHERE publication_key=?",
            "pending",
            "invalid status",
        ),
        (
            "retry",
            "UPDATE artifact_publications SET retry_until_ms=? WHERE publication_key=?",
            1.5,
            "invalid retry_until_ms",
        ),
        (
            "attempt",
            "UPDATE artifact_publications SET attempt_count=? WHERE publication_key=?",
            1.5,
            "invalid attempt_count",
        ),
        (
            "lease",
            "UPDATE artifact_publications SET lease_expires_at=? WHERE publication_key=?",
            "not-a-clock",
            "invalid lease_expires_at",
        ),
    ],
)
async def test_local_artifact_rows_reject_corrupt_durable_scalars(
    tmp_path, field, statement, value, message
):
    catalog = SqliteControlCatalog(tmp_path / f"catalog-{field}.db")
    try:
        _, publication = await _acquire(catalog)
        connection = catalog._connect_sync()
        with connection:
            connection.execute(statement, (value, publication.publication_key))
        with pytest.raises(RuntimeError, match=message):
            await catalog.get_artifact_publication("world-1", publication.publication_key)
    finally:
        await catalog.close()
