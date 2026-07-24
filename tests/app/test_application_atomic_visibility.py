# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Atomic tick visibility (issue #273, A2).

The contract under test: a tick is visible iff its manifest is published.
Crashed partial writes, failed publishes, and stale-writer appends leave
rows on disk but never in a manifest — invisible by construction. Exactly
one commit attempt per tick ever becomes visible.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any

import pytest

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.core.errors import TickExecutionError
from archetype.core.interfaces import StaleWriterError
from archetype.storage.catalog import SqliteControlCatalog
from archetype.storage.commit import CatalogCommitCoordinator
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.world.models import (
    ComponentTypeRef,
    CreateWorld,
    QueryComponents,
    Spawn,
    Step,
)
from tests._runtime import build_test_runtime

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


@asynccontextmanager
async def _runtime(tmp_path):
    storage_service = StorageService(
        control_catalog_config=ControlCatalogConfig(catalog_dir=tmp_path / "control")
    )
    resources = build_test_runtime(tmp_path, storage_service=storage_service)
    try:
        yield resources.dispatcher, storage_service
    finally:
        await resources.aclose()
        await storage_service.shutdown()


def _world_registry(dispatcher: Any) -> Any:
    return dispatcher._registry.resolve_name("step").handler.args[0]


async def _create_world(
    dispatcher: Any,
    storage: StorageConfig,
    *,
    cache: CacheConfig | None = None,
) -> Any:
    info = await dispatcher.apply(
        CreateWorld(
            config=WorldConfig(name="w"),
            storage_config=storage,
            cache_config=cache,
        )
    )
    world = await _world_registry(dispatcher).live_world(str(info.world_id))
    assert world is not None
    return world


async def _spawn_and_step(dispatcher: Any, world: Any, n_steps: int = 1) -> None:
    await dispatcher.apply(
        Spawn.from_components(
            world_id=world.world_id,
            components=[Counter(value=1.0)],
        )
    )
    for _ in range(n_steps):
        await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))


async def _visible_rows(
    dispatcher: Any,
    world,
    storage,
    ticks=None,
    component: type[Component] = Counter,
) -> list[dict]:
    df = await dispatcher.apply(
        QueryComponents(
            components=(ComponentTypeRef.from_type(component),),
            world_id=world.world_id,
            run_id=world.run_id,
            storage_config=storage,
            ticks=None if ticks is None else tuple(ticks),
        )
    )
    return df.to_pylist()


# ─────────────────────────────────────────────────────────────────────────────
# P0: crash injection at every boundary
# ─────────────────────────────────────────────────────────────────────────────


async def test_service_worlds_publish_manifests_per_tick(tmp_path):
    async with _runtime(tmp_path) as (dispatcher, storage_service):
        storage = _storage(tmp_path)
        world = await _create_world(dispatcher, storage)
        assert world.commit_coordinator is not None, "service worlds are coordinated"
        await _spawn_and_step(dispatcher, world, n_steps=2)

        catalog = storage_service.get_control_catalog(storage)
        manifests = await catalog.list_manifests(str(world.world_id))
        assert [m.tick for m in manifests] == [0, 1]
        assert all(m.writer_epoch == 1 for m in manifests)
        assert (await catalog.get_world(str(world.world_id))).tick_head == 1


async def test_failed_publish_leaves_tick_invisible_and_retry_wins(tmp_path, monkeypatch):
    """A confirmed pre-effect publish failure permits one fresh append attempt.

    The visibility authority proves the first POST had no effect. The retried
    tick therefore recomputes (caches intact), appends under a fresh token,
    and publishes — exactly one attempt visible, no lost spawns.
    """
    async with _runtime(tmp_path) as (dispatcher, _storage_service):
        storage = _storage(tmp_path)
        world = await _create_world(dispatcher, storage)
        await dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[Counter(value=1.0)],
            )
        )
        coordinator = world.commit_coordinator
        assert coordinator is not None
        real_begin = coordinator.begin_tick
        attempt_tokens: list[str] = []

        async def _record_begin(tick):
            context = await real_begin(tick)
            attempt_tokens.append(context.commit_token)
            return context

        monkeypatch.setattr(coordinator, "begin_tick", _record_begin)

        real_publish = SqliteControlCatalog.publish_manifest

        async def _crash(self, *args, **kwargs):
            raise RuntimeError("injected crash before head publish")

        monkeypatch.setattr(SqliteControlCatalog, "publish_manifest", _crash)
        with pytest.raises(RuntimeError, match="injected crash"):
            await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))

        assert world.tick == 0, "a tick that did not publish did not happen"
        assert await _visible_rows(dispatcher, world, storage) == [], (
            "unmanifested rows must be invisible"
        )

        # Recovery: the same writer retries the tick.
        monkeypatch.setattr(SqliteControlCatalog, "publish_manifest", real_publish)
        await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))

        assert len(attempt_tokens) == 2
        assert attempt_tokens[0] != attempt_tokens[1]
        rows = await _visible_rows(dispatcher, world, storage, ticks=[0])
        assert len(rows) == 1, (
            f"exactly one visible row despite two physical attempts, saw {len(rows)}"
        )
        assert rows[0]["counter__value"] == 1.0
        signature = Archetype.sig_from_components([Counter()])
        physical_rows = (
            await world.updater.store.get_archetype_df(
                signature,
                str(world.world_id),
                str(world.run_id),
                ticks=[0],
            )
        ).to_pylist()
        assert len(physical_rows) == 2
        assert {row["commit_token"] for row in physical_rows} == set(attempt_tokens)


async def test_partial_archetype_append_is_invisible_and_retry_is_atomic(tmp_path, monkeypatch):
    async with _runtime(tmp_path) as (dispatcher, storage_service):
        storage = _storage(tmp_path)
        world = await _create_world(dispatcher, storage)
        await dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[Counter(value=1.0)],
            )
        )
        await dispatcher.apply(
            Spawn.from_components(
                world_id=world.world_id,
                components=[Gauge(value=2.0)],
            )
        )

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
            await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))
        assert raised.value.phase == "commit"
        assert any("second-archetype append failure" in str(f.error) for f in raised.value.failures)

        assert world.tick == 0
        assert await _visible_rows(dispatcher, world, storage, component=Counter) == []

        monkeypatch.setattr(store, "append", real_append)
        await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))

        assert (
            len(
                await _visible_rows(
                    dispatcher,
                    world,
                    storage,
                    ticks=[0],
                    component=Counter,
                )
            )
            == 1
        )
        assert (
            len(
                await _visible_rows(
                    dispatcher,
                    world,
                    storage,
                    ticks=[0],
                    component=Gauge,
                )
            )
            == 1
        )
        catalog = storage_service.get_control_catalog(storage)
        manifests = await catalog.list_manifests(str(world.world_id), str(world.run_id))
        assert [manifest.tick for manifest in manifests] == [0]


async def test_cache_enabled_head_never_claims_ram_only_rows(tmp_path):
    """With the caching store, flush is forced before publish: after a step,
    a cold reader over the same storage (fresh container, no memtables)
    sees every manifested row."""
    async with _runtime(tmp_path) as (dispatcher, _storage_service):
        storage = _storage(tmp_path)
        cache = CacheConfig(flush_rows=1_000_000, flush_mb=1_000, idle_sec=3600)
        world = await _create_world(dispatcher, storage, cache=cache)
        await _spawn_and_step(dispatcher, world, n_steps=2)
        wid, rid = str(world.world_id), str(world.run_id)

    async with _runtime(tmp_path) as (cold_dispatcher, _cold_storage):
        df = await cold_dispatcher.apply(
            QueryComponents(
                components=(ComponentTypeRef.from_type(Counter),),
                world_id=wid,
                run_id=rid,
                storage_config=storage,
            )
        )
        rows = df.to_pylist()
        assert {r["tick"] for r in rows} == {0, 1}, (
            "every published tick must be durably readable cold"
        )


# ─────────────────────────────────────────────────────────────────────────────
# P0: fencing and stale writers
# ─────────────────────────────────────────────────────────────────────────────


async def test_second_fence_acquisition_stales_first_writer(tmp_path):
    async with _runtime(tmp_path) as (dispatcher, storage_service):
        storage = _storage(tmp_path)
        world = await _create_world(dispatcher, storage)
        await _spawn_and_step(dispatcher, world)

        # A second writer (e.g. another process resuming this world) takes
        # the fence. The first writer's next publish must fail closed.
        catalog = storage_service.get_control_catalog(storage)
        await catalog.acquire_fence(str(world.world_id), "intruder:999")

        with pytest.raises(RuntimeError) as exc_info:
            await dispatcher.apply(Step(world_id=world.world_id, run_config=RunConfig()))
        assert "StaleWriter" in type(exc_info.value).__name__ or "not the" in str(exc_info.value)
        assert world.tick == 1, "stale writer must not advance"
        rows = await _visible_rows(dispatcher, world, storage)
        assert {r["tick"] for r in rows} == {0}, "stale attempt stays invisible"


async def test_stale_epoch_rows_at_visible_tick_are_excluded(tmp_path):
    """A stale writer appending at an ALREADY-VISIBLE tick is excluded by the
    token allowlist — this is why readers match manifests, not epochs-at-head."""
    import daft
    import pyarrow as pa

    from archetype.core.archetype import Archetype

    async with _runtime(tmp_path) as (dispatcher, storage_service):
        storage = _storage(tmp_path)
        world = await _create_world(dispatcher, storage)
        await _spawn_and_step(dispatcher, world)
        assert len(await _visible_rows(dispatcher, world, storage, ticks=[0])) == 1

        # Forge a late, unmanifested append at the visible tick 0.
        sig = (Counter,)
        schema = Archetype.get_archetype_schema(sig)
        store = await storage_service.get_or_create_store(storage)
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

        rows = await _visible_rows(dispatcher, world, storage, ticks=[0])
        assert len(rows) == 1 and rows[0]["counter__value"] == 1.0, (
            "unmanifested stale-epoch rows at a visible tick must not surface"
        )


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


async def test_catalog_failure_fails_reads_closed_not_open(tmp_path, monkeypatch):
    """A broken control catalog must fail coordinated reads, never widen
    them: returning 'no allowlist' on error would surface rows from crashed
    or stale commit attempts that no manifest authorized (Codex P1, #280)."""
    async with _runtime(tmp_path) as (dispatcher, _storage_service):
        storage = _storage(tmp_path)
        world = await _create_world(dispatcher, storage)
        await _spawn_and_step(dispatcher, world)
        assert len(await _visible_rows(dispatcher, world, storage, ticks=[0])) == 1

        async def _broken(self, *args, **kwargs):
            raise RuntimeError("catalog corrupt")

        monkeypatch.setattr(SqliteControlCatalog, "visible_tokens", _broken)
        with pytest.raises(RuntimeError, match="catalog corrupt"):
            await _visible_rows(dispatcher, world, storage, ticks=[0])


async def test_querier_without_commit_tokens_support_fails_closed(tmp_path):
    """A querier whose signature cannot accept the visibility allowlist must
    refuse coordinated reads — never silently retry unfiltered (footgun
    finding on #280: the old TypeError fallback dropped commit_tokens)."""
    from archetype.core.aio import AsyncWorld

    async with _runtime(tmp_path) as (dispatcher, _storage_service):
        storage = _storage(tmp_path)
        world = await _create_world(dispatcher, storage)
        await _spawn_and_step(dispatcher, world)

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


async def test_coordinator_epoch_and_manifest_roundtrip(tmp_path):
    catalog = SqliteControlCatalog(tmp_path / "cat.db")
    epoch = await catalog.acquire_fence("w", "h1")
    coordinator = CatalogCommitCoordinator.bound(catalog, "w", "r", epoch)

    ctx = await coordinator.begin_tick(0)
    assert ctx.writer_epoch == epoch and ctx.commit_token

    await coordinator.publish_tick(0, ctx, [(Counter,)])
    await coordinator.publish_tick(0, ctx, [(Counter,)])  # idempotent retry

    visible = await coordinator.visible_tokens("w", "r", [0, 1])
    assert visible == {0: [ctx.commit_token]}

    # A newer fence stales this coordinator.
    await catalog.acquire_fence("w", "h2")
    ctx2 = await coordinator.begin_tick(1)
    with pytest.raises(StaleWriterError):
        await coordinator.publish_tick(1, ctx2, [(Counter,)])
    await catalog.close()
