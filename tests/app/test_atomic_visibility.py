# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Atomic tick visibility (issue #273, A2).

The contract under test: a tick is visible iff its manifest is published.
Crashed partial writes, failed publishes, and stale-writer appends leave
rows on disk but never in a manifest — invisible by construction. Exactly
one commit attempt per tick ever becomes visible.
"""

import asyncio

import pytest

from archetype.app.container import ServiceContainer
from archetype.app.storage.catalog import SqliteControlCatalog
from archetype.app.storage.commit import CatalogCommitCoordinator
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.errors import TickExecutionError
from archetype.core.interfaces import StaleWriterError

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("world.tick.atomic_visibility"),
    pytest.mark.contract("world.writer.fenced"),
    pytest.mark.integration,
]


class Counter(Component):
    value: float = 0.0


class Gauge(Component):
    value: float = 0.0


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


async def _spawn_and_step(c: ServiceContainer, world, n_steps: int = 1) -> None:
    await c.mutation_service.create_entity(world.world_id, [Counter(value=1.0)])
    for _ in range(n_steps):
        await c.simulation_service.step(world.world_id, RunConfig())


async def _visible_rows(
    c: ServiceContainer,
    world,
    storage,
    ticks=None,
    component: type[Component] = Counter,
) -> list[dict]:
    df = await c.query_service.query_components(
        [component], str(world.world_id), str(world.run_id), storage, ticks=ticks
    )
    return df.to_pylist()


# ─────────────────────────────────────────────────────────────────────────────
# P0: crash injection at every boundary
# ─────────────────────────────────────────────────────────────────────────────


async def test_service_worlds_publish_manifests_per_tick(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage)
        assert world.commit_coordinator is not None, "service worlds are coordinated"
        await _spawn_and_step(c, world, n_steps=2)

        catalog = c.storage_service.get_control_catalog(storage)
        manifests = await catalog.list_manifests(str(world.world_id))
        assert [m.tick for m in manifests] == [0, 1]
        assert all(m.writer_epoch == 1 for m in manifests)
        assert (await catalog.get_world(str(world.world_id))).tick_head == 1
    finally:
        await c.shutdown()


async def test_failed_publish_leaves_tick_invisible_and_retry_wins(tmp_path, monkeypatch):
    """Crash between appends and head publish: rows exist, tick invisible.

    The retried tick recomputes (caches intact), appends under a fresh
    token, and publishes — exactly one attempt visible, no lost spawns,
    no duplicate visible rows.
    """
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage)
        await c.mutation_service.create_entity(world.world_id, [Counter(value=1.0)])

        real_publish = SqliteControlCatalog.publish_manifest

        async def _crash(self, *args, **kwargs):
            raise RuntimeError("injected crash before head publish")

        monkeypatch.setattr(SqliteControlCatalog, "publish_manifest", _crash)
        with pytest.raises(RuntimeError, match="injected crash"):
            await c.simulation_service.step(world.world_id, RunConfig())

        assert world.tick == 0, "a tick that did not publish did not happen"
        assert await _visible_rows(c, world, storage) == [], "unmanifested rows must be invisible"

        # Recovery: the same writer retries the tick.
        monkeypatch.setattr(SqliteControlCatalog, "publish_manifest", real_publish)
        await c.simulation_service.step(world.world_id, RunConfig())

        rows = await _visible_rows(c, world, storage, ticks=[0])
        assert len(rows) == 1, (
            f"exactly one visible row despite two physical attempts, saw {len(rows)}"
        )
        assert rows[0]["counter__value"] == 1.0
    finally:
        await c.shutdown()


async def test_partial_archetype_append_is_invisible_and_retry_is_atomic(tmp_path, monkeypatch):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage)
        await c.mutation_service.create_entity(world.world_id, [Counter(value=1.0)])
        await c.mutation_service.create_entity(world.world_id, [Gauge(value=2.0)])

        store = world.updater.store
        real_append = store.append
        counter_table = Archetype.get_name(Archetype.sig_from_components([Counter()]))
        counter_committed = asyncio.Event()

        async def fail_after_counter(sig, frame):
            if Archetype.get_name(sig) == counter_table:
                receipt = await real_append(sig, frame)
                counter_committed.set()
                return receipt
            await counter_committed.wait()
            raise RuntimeError("injected second-archetype append failure")

        monkeypatch.setattr(store, "append", fail_after_counter)
        # #444: the commit-phase aggregate names the failed table only; the
        # injected append error rides in failures with its text intact.
        with pytest.raises(TickExecutionError) as raised:
            await c.simulation_service.step(world.world_id, RunConfig())
        assert raised.value.phase == "commit"
        assert any("second-archetype append failure" in str(f.error) for f in raised.value.failures)

        assert world.tick == 0
        assert await _visible_rows(c, world, storage, component=Counter) == []

        monkeypatch.setattr(store, "append", real_append)
        await c.simulation_service.step(world.world_id, RunConfig())

        assert len(await _visible_rows(c, world, storage, ticks=[0], component=Counter)) == 1
        assert len(await _visible_rows(c, world, storage, ticks=[0], component=Gauge)) == 1
        catalog = c.storage_service.get_control_catalog(storage)
        manifests = await catalog.list_manifests(str(world.world_id), str(world.run_id))
        assert [manifest.tick for manifest in manifests] == [0]
    finally:
        await c.shutdown()


async def test_cache_enabled_head_never_claims_ram_only_rows(tmp_path):
    """With the caching store, flush is forced before publish: after a step,
    a cold reader over the same storage (fresh container, no memtables)
    sees every manifested row."""
    from archetype.core.config import CacheConfig

    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        cache = CacheConfig(flush_rows=1_000_000, flush_mb=1_000, idle_sec=3600)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage, cache)
        await _spawn_and_step(c, world, n_steps=2)
        wid, rid = str(world.world_id), str(world.run_id)
    finally:
        await c.shutdown()

    cold = ServiceContainer()
    try:
        df = await cold.query_service.query_components([Counter], wid, rid, storage)
        rows = df.to_pylist()
        assert {r["tick"] for r in rows} == {0, 1}, (
            "every published tick must be durably readable cold"
        )
    finally:
        await cold.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# P0: fencing and stale writers
# ─────────────────────────────────────────────────────────────────────────────


async def test_second_fence_acquisition_stales_first_writer(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage)
        await _spawn_and_step(c, world)

        # A second writer (e.g. another process resuming this world) takes
        # the fence. The first writer's next publish must fail closed.
        catalog = c.storage_service.get_control_catalog(storage)
        await catalog.acquire_fence(str(world.world_id), "intruder:999")

        with pytest.raises(RuntimeError) as exc_info:
            await c.simulation_service.step(world.world_id, RunConfig())
        assert "StaleWriter" in type(exc_info.value).__name__ or "not the" in str(exc_info.value)
        assert world.tick == 1, "stale writer must not advance"
        rows = await _visible_rows(c, world, storage)
        assert {r["tick"] for r in rows} == {0}, "stale attempt stays invisible"
    finally:
        await c.shutdown()


async def test_stale_epoch_rows_at_visible_tick_are_excluded(tmp_path):
    """A stale writer appending at an ALREADY-VISIBLE tick is excluded by the
    token allowlist — this is why readers match manifests, not epochs-at-head."""
    import daft
    import pyarrow as pa

    from archetype.core.archetype import Archetype

    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage)
        await _spawn_and_step(c, world)
        assert len(await _visible_rows(c, world, storage, ticks=[0])) == 1

        # Forge a late, unmanifested append at the visible tick 0.
        sig = (Counter,)
        schema = Archetype.get_archetype_schema(sig)
        store = await c.storage_service.get_or_create_store(storage)
        forged = {
            "world_id": str(world.world_id),
            "run_id": str(world.run_id),
            "entity_id": 999,
            "tick": 0,
            "is_active": True,
            "commit_token": "stale-attempt",
            "writer_epoch": 0,
            "counter__value": 666.0,
        }
        await store.append(sig, daft.from_arrow(pa.Table.from_pylist([forged], schema=schema)))

        rows = await _visible_rows(c, world, storage, ticks=[0])
        assert len(rows) == 1 and rows[0]["counter__value"] == 1.0, (
            "unmanifested stale-epoch rows at a visible tick must not surface"
        )
    finally:
        await c.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# P0: receipts, legacy readability, catalog upgrade
# ─────────────────────────────────────────────────────────────────────────────


async def test_append_returns_durable_receipts(tmp_path):
    import daft
    import pyarrow as pa

    from archetype.core.aio import AsyncLancedbStore
    from archetype.core.archetype import Archetype

    sig = (Counter,)
    schema = Archetype.get_archetype_schema(sig)
    row = {
        "world_id": "w",
        "run_id": "r",
        "entity_id": 1,
        "tick": 0,
        "is_active": True,
        "commit_token": "t",
        "writer_epoch": 1,
        "counter__value": 2.0,
    }
    store = AsyncLancedbStore(uri=str(tmp_path), namespace="ns")
    try:
        receipt = await store.append(
            sig, daft.from_arrow(pa.Table.from_pylist([row], schema=schema))
        )
        assert receipt.durable and receipt.rows == 1
        assert receipt.table_id == Archetype.get_name(sig)
    finally:
        await store.shutdown()


async def test_legacy_v02_tables_stay_readable(tmp_path):
    """A table written under the v0.2 schema/name (no commit columns) reads
    through the same paths as implicit epoch-0 history."""
    import lancedb
    import pyarrow as pa

    from archetype.core.aio import AsyncLancedbStore
    from archetype.core.archetype import Archetype

    sig = (Counter,)
    legacy_schema = Archetype.get_legacy_schema(sig)
    legacy_name = Archetype.get_legacy_name(sig)
    assert legacy_name != Archetype.get_name(sig), "generations must not collide"

    # Write the legacy table the way v0.2 did: directly under the legacy id.
    db = await lancedb.connect_async(str(tmp_path / "ns" / "lance"))
    legacy_row = {
        "world_id": "w",
        "run_id": "r",
        "entity_id": 7,
        "tick": 3,
        "is_active": True,
        "counter__value": 9.0,
    }
    table = await db.create_table(
        name=legacy_name,
        schema=legacy_schema,
        exist_ok=True,
    )
    await table.add(pa.Table.from_pylist([legacy_row], schema=legacy_schema))

    store = AsyncLancedbStore(uri=str(tmp_path), namespace="ns")
    try:
        df = await store.get_archetype_df(sig, "w", "r", active_only=True)
        rows = df.to_pylist()
        assert len(rows) == 1
        assert rows[0]["counter__value"] == 9.0
        assert rows[0]["commit_token"] == "" and rows[0]["writer_epoch"] == 0, (
            "legacy rows surface as implicit epoch-0"
        )

        # The allowlist never applies to legacy rows.
        df = await store.get_archetype_df(
            sig, "w", "r", active_only=True, commit_tokens=["only-this-token"]
        )
        assert len(df.to_pylist()) == 1, "epoch-0 legacy history is always visible"
    finally:
        await store.shutdown()


async def test_v1_catalog_upgrades_additively(tmp_path):
    import sqlite3

    path = tmp_path / "cat.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE catalog_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE worlds (
            world_id TEXT PRIMARY KEY, name TEXT, run_id TEXT,
            parent_world_id TEXT, status TEXT NOT NULL,
            tick_head INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE signatures (
            table_id TEXT PRIMARY KEY, component_names TEXT NOT NULL,
            schema_json TEXT NOT NULL, fingerprint TEXT NOT NULL
        );
        INSERT INTO catalog_meta (key, value) VALUES ('schema_version', '1');
        INSERT INTO worlds VALUES ('w1', 'alpha', 'r1', NULL, 'active', 5);
        """
    )
    conn.commit()
    conn.close()

    catalog = SqliteControlCatalog(path)
    record = await catalog.get_world("w1")
    assert record is not None and record.tick_head == 5, "v1 data survives"
    epoch = await catalog.acquire_fence("w1", "h")
    assert epoch == 1, "v2 tables exist after additive upgrade"
    await catalog.close()


async def test_v4_catalog_renames_fact_claim_identity_without_data_loss(tmp_path):
    """The artifact vocabulary migration preserves already-published claims."""
    import sqlite3

    path = tmp_path / "cat.db"
    catalog = SqliteControlCatalog(path)
    assert await catalog.get_world("missing") is None  # create the current schema
    await catalog.close()

    conn = sqlite3.connect(path)
    conn.execute("ALTER TABLE claims RENAME COLUMN artifact_entity_id TO fact_entity_id")
    conn.execute("UPDATE catalog_meta SET value='4' WHERE key='schema_version'")
    conn.execute(
        """
        INSERT INTO claims (
            scope_key, world_id, run_id, producer, external_id, payload_digest,
            status, commit_token, tick, fact_entity_id, table_id, claimant,
            lease_expires_at, fence_epoch, created_at, completed_at
        ) VALUES (
            'scope', 'w1', 'r1', 'producer', 'external', 'digest', 'COMPLETE',
            'token', 3, -42, 'artifact_table', 'writer', 0.0, 1,
            '2026-01-01T00:00:00Z', '2026-01-01T00:00:01Z'
        )
        """
    )
    conn.commit()
    conn.close()

    upgraded = SqliteControlCatalog(path)
    claim = await upgraded.get_claim("w1", "scope")
    assert claim is not None
    assert claim.artifact_entity_id == -42
    assert claim.table_id == "artifact_table"
    await upgraded.close()

    conn = sqlite3.connect(path)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(claims)")}
    version = conn.execute("SELECT value FROM catalog_meta WHERE key='schema_version'").fetchone()[
        0
    ]
    conn.close()
    assert "artifact_entity_id" in columns and "fact_entity_id" not in columns
    assert version == "8"


async def test_v6_catalog_adds_attempt_claims_with_redaction_evidence(tmp_path):
    """The current schema adds the complete claim authority to a released v6 catalog."""
    import sqlite3

    path = tmp_path / "cat.db"
    catalog = SqliteControlCatalog(path)
    assert await catalog.get_world("missing") is None  # create the current schema
    await catalog.close()

    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE mission_attempt_claims")
    conn.execute("UPDATE catalog_meta SET value='6' WHERE key='schema_version'")
    conn.commit()
    conn.close()

    upgraded = SqliteControlCatalog(path)
    assert await upgraded.get_attempt_claim("w1", "missing") is None
    await upgraded.close()

    conn = sqlite3.connect(path)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(mission_attempt_claims)")}
    version = conn.execute("SELECT value FROM catalog_meta WHERE key='schema_version'").fetchone()[
        0
    ]
    conn.close()
    assert {
        "redaction_policy_id",
        "redaction_acquisition_evidence_json",
        "redaction_evidence_json",
    } <= columns
    assert version == "8"


async def test_v7_catalog_preserves_settled_claim_while_adding_finalization_outbox(tmp_path):
    """The additive v8 upgrade keeps terminal v7 evidence replay-readable."""
    import sqlite3

    path = tmp_path / "cat.db"
    base = {
        "claim_key": "claim-settled-v7",
        "world_id": "world-v7",
        "run_id": "run-v7",
        "mission_id": "mission-v7",
        "task_id": "task-v7",
        "attempt_id": "attempt-v7",
        "idempotency_key": "idempotency-v7",
        "request_fingerprint": "request-v7",
        "request_json": '{"request":"v7"}',
        "redaction_policy_id": "redaction-v1",
        "redaction_evidence_json": '{"phase":"settled"}',
        "provider": "modal",
        "provider_request_fingerprint": "provider-v7",
        "supports_idempotent_replay": False,
        "supports_session_resume": True,
        "provider_idempotency_key": "",
    }
    catalog = SqliteControlCatalog(path)
    _, claim = await catalog.acquire_attempt_claim(**base, claimant="worker-v7")
    settled = await catalog.transition_attempt_claim(
        base["world_id"],
        claim.claim_key,
        claim.claimant,
        claim.fence_epoch,
        expected_status="claimed",
        target_status="settled",
        redaction_evidence_json='{"phase":"settled"}',
        settlement_status="failed",
        outcome_digest="outcome-v7",
        outcome_json='{"status":"failed"}',
    )
    assert settled.status == "settled"
    await catalog.close()

    conn = sqlite3.connect(path)
    for column in (
        "artifact_request_json",
        "artifact_request_digest",
        "artifact_publication_key",
        "finalizing_at",
    ):
        conn.execute(f"ALTER TABLE mission_attempt_claims DROP COLUMN {column}")
    conn.execute("UPDATE catalog_meta SET value='7' WHERE key='schema_version'")
    conn.commit()
    conn.close()

    upgraded = SqliteControlCatalog(path)
    restored = await upgraded.get_attempt_claim(base["world_id"], base["claim_key"])
    assert restored is not None
    assert restored.status == "settled"
    assert restored.outcome_digest == "outcome-v7"
    assert restored.outcome_json == '{"status":"failed"}'
    assert restored.artifact_request_json == ""
    assert restored.artifact_request_digest == ""
    assert restored.artifact_publication_key == ""
    # A malformed failed v7 request remains replayable catalog evidence, but it
    # cannot acquire the narrow accepted INDEXED compatibility authority.
    assert restored.legacy_unbound_eligible is False
    assert restored.finalizing_at is None
    outcome, duplicate = await upgraded.acquire_attempt_claim(**base, claimant="replay-v8")
    assert outcome == "duplicate"
    assert duplicate == restored
    assert (
        await upgraded.list_due_attempt_claims(base["world_id"], now=restored.lease_expires_at + 1)
        == []
    )
    await upgraded.close()

    conn = sqlite3.connect(path)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(mission_attempt_claims)")}
    version = conn.execute("SELECT value FROM catalog_meta WHERE key='schema_version'").fetchone()[
        0
    ]
    conn.close()
    assert {
        "artifact_request_json",
        "artifact_request_digest",
        "artifact_publication_key",
        "legacy_unbound_eligible",
        "finalizing_at",
    } <= columns
    assert version == "8"


async def test_catalog_failure_fails_reads_closed_not_open(tmp_path, monkeypatch):
    """A broken control catalog must fail coordinated reads, never widen
    them: returning 'no allowlist' on error would surface rows from crashed
    or stale commit attempts that no manifest authorized (Codex P1, #280)."""
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage)
        await _spawn_and_step(c, world)
        assert len(await _visible_rows(c, world, storage, ticks=[0])) == 1

        async def _broken(self, *args, **kwargs):
            raise RuntimeError("catalog corrupt")

        monkeypatch.setattr(SqliteControlCatalog, "visible_tokens", _broken)
        with pytest.raises(RuntimeError, match="catalog corrupt"):
            await _visible_rows(c, world, storage, ticks=[0])
    finally:
        await c.shutdown()


async def test_querier_without_commit_tokens_support_fails_closed(tmp_path):
    """A querier whose signature cannot accept the visibility allowlist must
    refuse coordinated reads — never silently retry unfiltered (footgun
    finding on #280: the old TypeError fallback dropped commit_tokens)."""
    from archetype.core.aio import AsyncWorld

    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage)
        await _spawn_and_step(c, world)

        class NoTokenQuerier:
            def __init__(self, inner):
                self._inner = inner

            async def list_signatures(self):
                return await self._inner.list_signatures()

            async def query_archetype(
                self, sig, world_id, ticks=None, entity_ids=None, components=None, run_id=None
            ):
                raise AssertionError("must not be reached for a coordinated world")

            async def query_components(
                self, components, world_id, run_id, *, ticks=None, entity_ids=None
            ):
                raise AssertionError("must not be reached for a coordinated world")

        assert isinstance(world, AsyncWorld)
        world.querier = NoTokenQuerier(world.querier)
        world._querier_caps = None  # re-inspect the replaced querier

        with pytest.raises(RuntimeError, match="commit_tokens.*fail closed"):
            await world.query_archetype((Counter,), ticks=[0])
        with pytest.raises(RuntimeError, match="commit_tokens.*fail closed"):
            await world.get_components([Counter])
    finally:
        await c.shutdown()


async def test_coordinator_epoch_and_manifest_roundtrip(tmp_path):
    catalog = SqliteControlCatalog(tmp_path / "cat.db")
    epoch = await catalog.acquire_fence("w", "h1")
    coordinator = CatalogCommitCoordinator(catalog, epoch=epoch)

    ctx = await coordinator.begin_tick("w", "r", 0)
    assert ctx.writer_epoch == epoch and ctx.commit_token

    await coordinator.publish_tick("w", "r", 0, ctx, [(Counter,)])
    await coordinator.publish_tick("w", "r", 0, ctx, [(Counter,)])  # idempotent retry

    visible = await coordinator.visible_tokens("w", "r", [0, 1])
    assert visible == {0: [ctx.commit_token]}

    # A newer fence stales this coordinator.
    await catalog.acquire_fence("w", "h2")
    ctx2 = await coordinator.begin_tick("w", "r", 1)
    with pytest.raises(StaleWriterError):
        await coordinator.publish_tick("w", "r", 1, ctx2, [(Counter,)])
    await catalog.close()
