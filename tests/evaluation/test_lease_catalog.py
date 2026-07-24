# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable serialization contracts for evaluation grader execution."""

import asyncio

import pytest

from archetype.storage.catalog import CatalogConflictError, SqliteControlCatalog

pytestmark = pytest.mark.asyncio


async def test_two_catalog_instances_serialize_one_evaluation(tmp_path):
    path = tmp_path / "catalog.db"
    first_catalog = SqliteControlCatalog(path)
    second_catalog = SqliteControlCatalog(path)
    try:
        first = await first_catalog.lease_evaluation(
            "world",
            "run",
            "evaluation",
            "subject",
            "contract",
            "owner-a",
            lease_seconds=30,
        )
        waiting = await second_catalog.lease_evaluation(
            "world",
            "run",
            "evaluation",
            "subject",
            "contract",
            "owner-b",
            lease_seconds=30,
        )
        mismatch = await second_catalog.lease_evaluation(
            "world",
            "run",
            "evaluation",
            "different-subject",
            "contract",
            "owner-b",
            lease_seconds=30,
        )

        assert first.acquired and first.owner == "owner-a"
        assert not waiting.acquired and waiting.owner == "owner-a"
        assert not mismatch.acquired and mismatch.subject_digest == "subject"

        renewed = await first_catalog.lease_evaluation(
            "world",
            "run",
            "evaluation",
            "subject",
            "contract",
            "owner-a",
            lease_seconds=60,
        )
        assert renewed.acquired and renewed.lease_expires_at > first.lease_expires_at

        await first_catalog.complete_evaluation("world", "run", "evaluation", "owner-a")
        await first_catalog.complete_evaluation("world", "run", "evaluation", "owner-a")
        complete = await second_catalog.lease_evaluation(
            "world",
            "run",
            "evaluation",
            "subject",
            "contract",
            "owner-b",
        )
        assert complete.status == "COMPLETE"
        assert not complete.acquired and complete.owner is None
    finally:
        await second_catalog.close()
        await first_catalog.close()


async def test_failed_and_expired_evaluation_leases_are_recoverable(tmp_path):
    path = tmp_path / "catalog.db"
    first_catalog = SqliteControlCatalog(path)
    second_catalog = SqliteControlCatalog(path)
    try:
        await first_catalog.lease_evaluation(
            "world",
            "run",
            "released",
            "subject",
            "contract",
            "owner-a",
        )
        await first_catalog.release_evaluation("world", "run", "released", "owner-a")
        released = await second_catalog.lease_evaluation(
            "world",
            "run",
            "released",
            "subject",
            "contract",
            "owner-b",
        )
        assert released.acquired and released.owner == "owner-b"

        await first_catalog.lease_evaluation(
            "world",
            "run",
            "expired",
            "subject",
            "contract",
            "owner-a",
            lease_seconds=0.01,
        )
        await asyncio.sleep(0.02)
        recovered = await second_catalog.lease_evaluation(
            "world",
            "run",
            "expired",
            "subject",
            "contract",
            "owner-b",
        )
        assert recovered.acquired and recovered.owner == "owner-b"
        with pytest.raises(CatalogConflictError, match="not leased"):
            await first_catalog.complete_evaluation("world", "run", "expired", "owner-a")
        with pytest.raises(CatalogConflictError, match="no durable execution lease"):
            await first_catalog.complete_evaluation("world", "run", "missing", "owner-a")
        with pytest.raises(ValueError, match="evaluation_id"):
            await first_catalog.lease_evaluation(
                "world", "run", " ", "subject", "contract", "owner"
            )
        with pytest.raises(ValueError, match="owner"):
            await first_catalog.lease_evaluation(
                "world", "run", "evaluation", "subject", "contract", " "
            )
        with pytest.raises(ValueError, match="lease_seconds"):
            await first_catalog.lease_evaluation(
                "world",
                "run",
                "evaluation",
                "subject",
                "contract",
                "owner",
                lease_seconds=0,
            )
    finally:
        await second_catalog.close()
        await first_catalog.close()
