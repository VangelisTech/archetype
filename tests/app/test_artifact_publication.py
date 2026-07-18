# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable external artifacts (issue #274, B).

Exactly-once means exactly one logically VISIBLE artifact per
(storage, world, run, producer, external_id). Physical appends may retry;
visibility is the unit of truth. Artifacts ride the commit-identity machinery:
a artifact is visible iff its claim is COMPLETE.
"""

import asyncio
from dataclasses import replace

import pytest

from archetype.app.artifacts.models import ArtifactMeta, artifact_payload_digest
from archetype.app.container import ServiceContainer
from archetype.app.storage.catalog import (
    ClaimConflictError,
    SqliteControlCatalog,
    claim_scope_key,
)
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import iAsyncProcessor

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("artifacts.publication.exactly_once_visible"),
    pytest.mark.integration,
    pytest.mark.race,
]


class Reading(Component):
    value: float = 0.0
    unit: str = ""


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


async def _world(c: ServiceContainer, storage):
    world = await c.world_service.create_world(WorldConfig(name="w"), storage)
    await c.mutation_service.create_entity(world.world_id, [Reading(value=0.0, unit="genesis")])
    await c.simulation_service.step(world.world_id, RunConfig())
    return world


async def _visible_facts(c, world, storage) -> list[dict]:
    df = await c.query_service.query_components(
        [ArtifactMeta], str(world.world_id), str(world.run_id), storage
    )
    return df.to_pylist()


async def test_publish_appends_visible_artifact_with_external_key_on_rows(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(c, storage)

        receipt = await c.artifact_service.publish(
            str(world.world_id),
            [Reading(value=21.5, unit="C")],
            external_id="sensor-001:evt-1",
            producer="sensor-001",
        )
        assert not receipt.duplicate
        assert receipt.artifact_entity_id < 0, "artifacts live in the metadata id band"
        assert receipt.payload_digest == artifact_payload_digest([Reading(value=21.5, unit="C")])

        rows = await _visible_facts(c, world, storage)
        assert len(rows) == 1
        assert rows[0]["artifactmeta__external_id"] == "sensor-001:evt-1"
        assert rows[0]["artifactmeta__commit_id"] == receipt.commit_token, (
            "the external key and commit id ride the data plane"
        )
    finally:
        await c.shutdown()


async def test_artifact_tick_uses_manifest_head_when_directory_head_lags(tmp_path, monkeypatch):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(c, storage)
        await c.simulation_service.step(world.world_id, RunConfig())
        catalog = c.storage_service.get_control_catalog(storage)
        real_get_world = catalog.get_world

        async def stale_get_world(world_id):
            record = await real_get_world(world_id)
            return replace(record, tick_head=0) if record is not None else None

        monkeypatch.setattr(catalog, "get_world", stale_get_world)
        receipt = await c.artifact_service.publish(
            str(world.world_id),
            [Reading(value=21.5, unit="C")],
            external_id="after-derived-head-failure",
            producer="sensor-001",
        )

        assert receipt.tick == 1
    finally:
        await c.shutdown()


async def test_duplicate_ids_dedupe_and_conflicts_fail_loudly(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(c, storage)
        wid = str(world.world_id)

        first = await c.artifact_service.publish(
            wid, [Reading(value=1.0)], external_id="e-1", producer="p"
        )
        again = await c.artifact_service.publish(
            wid, [Reading(value=1.0)], external_id="e-1", producer="p"
        )
        assert again.duplicate and again.commit_token == first.commit_token
        assert len(await _visible_facts(c, world, storage)) == 1

        with pytest.raises(ClaimConflictError):
            await c.artifact_service.publish(
                wid, [Reading(value=2.0)], external_id="e-1", producer="p"
            )
    finally:
        await c.shutdown()


async def test_hundred_concurrent_identical_submissions_one_visible_artifact(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(c, storage)
        wid = str(world.world_id)

        async def submit():
            return await c.artifact_service.publish(
                wid, [Reading(value=7.0)], external_id="burst-1", producer="p"
            )

        receipts = await asyncio.gather(*(submit() for _ in range(100)))
        tokens = {r.commit_token for r in receipts}
        assert len(tokens) == 1, "every caller converges on the one visible artifact"
        assert sum(1 for r in receipts if not r.duplicate) == 1, (
            "exactly one caller performed the ingestion"
        )
        assert len(await _visible_facts(c, world, storage)) == 1
    finally:
        await c.shutdown()


async def test_crash_between_append_and_complete_recovers_without_duplication(
    tmp_path, monkeypatch
):
    """The orphan is found by its embedded key: takeover completes the claim
    WITHOUT re-appending."""
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(c, storage)
        wid = str(world.world_id)

        real_complete = SqliteControlCatalog.complete_claim

        async def _crash(self, *args, **kwargs):
            raise RuntimeError("injected crash after append, before complete")

        monkeypatch.setattr(SqliteControlCatalog, "complete_claim", _crash)
        with pytest.raises(RuntimeError, match="injected crash"):
            await c.artifact_service.publish(
                wid, [Reading(value=3.0)], external_id="crashy", producer="p"
            )
        monkeypatch.setattr(SqliteControlCatalog, "complete_claim", real_complete)

        assert await _visible_facts(c, world, storage) == [], (
            "an incomplete claim's rows are invisible"
        )

        # Lease takeover after "owner death" (expire the lease immediately).
        catalog = c.storage_service.get_control_catalog(storage)
        scope = claim_scope_key(wid, str(world.run_id), "p", "crashy")

        def _expire():
            conn = catalog._connect_sync()
            with conn:
                conn.execute("UPDATE claims SET lease_expires_at=0 WHERE scope_key=?", (scope,))

        await catalog._run(_expire)

        receipt = await c.artifact_service.publish(
            wid, [Reading(value=3.0)], external_id="crashy", producer="p"
        )
        assert not receipt.duplicate
        rows = await _visible_facts(c, world, storage)
        assert len(rows) == 1, "recovery completed the orphan without re-appending"
        assert rows[0]["artifactmeta__external_id"] == "crashy"
    finally:
        await c.shutdown()


async def test_recovery_rearms_before_append_so_expired_writer_stays_invisible(
    tmp_path, monkeypatch
):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(c, storage)
        wid = str(world.world_id)

        from archetype.app.artifacts.service import ArtifactService

        real_append = ArtifactService._append_artifact
        expired_append: tuple | None = None

        async def _controlled_append(self, *args, **kwargs):
            nonlocal expired_append
            if expired_append is None:
                expired_append = args
                raise RuntimeError("injected crash before append")

            # The expired writer resumes after takeover and appends with its
            # stale token immediately before the recovery's fresh append.
            await real_append(self, *expired_append)
            await real_append(self, *args, **kwargs)

        monkeypatch.setattr(ArtifactService, "_append_artifact", _controlled_append)
        with pytest.raises(RuntimeError, match="injected crash"):
            await c.artifact_service.publish(
                wid, [Reading(value=4.0)], external_id="pre-append", producer="p"
            )
        assert expired_append is not None
        expired_claim = expired_append[2]

        catalog = c.storage_service.get_control_catalog(storage)
        scope = claim_scope_key(wid, str(world.run_id), "p", "pre-append")

        def _expire():
            conn = catalog._connect_sync()
            with conn:
                conn.execute("UPDATE claims SET lease_expires_at=0 WHERE scope_key=?", (scope,))

        await catalog._run(_expire)

        receipt = await c.artifact_service.publish(
            wid, [Reading(value=4.0)], external_id="pre-append", producer="p"
        )
        assert not receipt.duplicate
        assert receipt.commit_token != expired_claim.commit_token

        visible = await _visible_facts(c, world, storage)
        assert len(visible) == 1
        assert visible[0]["artifactmeta__commit_id"] == receipt.commit_token

        store = await c.storage_service.get_or_create_store(storage)
        physical = await store.get_existing_table_df(receipt.table_id, wid, str(world.run_id))
        assert physical.count_rows() == 2, (
            "the late old-token row may exist physically but must stay invisible"
        )
    finally:
        await c.shutdown()


async def test_facts_never_trigger_processors_and_survive_steps(tmp_path):
    seen_entities: list[int] = []

    class Counter(iAsyncProcessor):
        components = (Reading,)
        priority = 0

        async def process(self, df, **kwargs):
            seen_entities.extend(int(r["entity_id"]) for r in df.to_pylist())
            return df

    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await _world(c, storage)
        wid = str(world.world_id)
        await c.mutation_service.add_processor(wid, Counter())

        receipt = await c.artifact_service.publish(
            wid, [Reading(value=9.0)], external_id="quiet", producer="p"
        )
        seen_entities.clear()
        await c.simulation_service.step(wid, RunConfig())
        await c.simulation_service.step(wid, RunConfig())

        assert receipt.artifact_entity_id not in seen_entities, (
            "artifacts are non-processable: the step loop never sees them"
        )
        # The artifact did not multiply across ticks (no quadratic history).
        rows = await _visible_facts(c, world, storage)
        assert len(rows) == 1
    finally:
        await c.shutdown()


async def test_gate_and_runtime_surface(tmp_path):
    from archetype import ArchetypeRuntime

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("w", storage=_storage(tmp_path))
        await world.spawn(Reading(value=0.0))
        await world.run(steps=1)

        receipt = await world.publish(
            Reading(value=42.0, unit="K"), external_id="rt-1", producer="probe"
        )
        assert not receipt.duplicate
        dup = await world.publish(
            Reading(value=42.0, unit="K"), external_id="rt-1", producer="probe"
        )
        assert dup.duplicate and dup.commit_token == receipt.commit_token
