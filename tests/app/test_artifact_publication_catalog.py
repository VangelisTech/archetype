# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Control-plane contracts for resumable artifact publication."""

import json
import time

import pytest

from archetype.app._catalog import (
    ArtifactPublicationConflictError,
    ArtifactPublicationPendingError,
    SqliteControlCatalog,
)

pytestmark = pytest.mark.asyncio


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
