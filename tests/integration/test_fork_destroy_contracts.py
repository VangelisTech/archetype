# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Contract tests for fork and destroy semantics.

Verifies:
  - Pending spawn_cache transfers to fork and materializes in both worlds.
  - Hook isolation: hooks added to source after fork do not fire on the fork.
  - Destroy preserves storage rows (append-only invariant).
  - Destroy preserves audit rows.
  - 10-world destroy stress: all query data survives destroy.
  - Audit row monotonicity: row count never decreases across operations + destroys.
"""

import pytest
from uuid_utils import uuid7

import archetype.app.gateway.auth.guard as guard
from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.guard import reset_daily_tokens
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.models import Command, CommandType
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.errors import AmbiguousTickCommitError
from archetype.core.hooks import PostTick
from archetype.world.models import Despawn
from archetype.world.query import get_lineage

_TEARDOWN_SENTINEL_ENTITY_ID = 2_147_483_647

# ---------------------------------------------------------------------------
# Test component
# ---------------------------------------------------------------------------


class Tag(Component):
    label: str = ""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_quotas():
    guard._tick_counters.clear()
    reset_daily_tokens()
    yield
    guard._tick_counters.clear()
    reset_daily_tokens()


def _admin_ctx() -> ActorCtx:
    return ActorCtx(id=uuid7(), roles={"admin"})


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


# ---------------------------------------------------------------------------
# 1. Spawn-then-fork pending mutation transfer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spawn_then_fork_pending_mutation_transfer(tmp_path):
    """Spawn an entity (goes to spawn_cache), fork before stepping.
    Step both source and fork. Entity should exist in both."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="src"), storage)
        # Spawn goes to spawn_cache (not yet materialized)
        eid = await c.application.create_entity(source.world_id, [Tag(label="pending")])
        assert eid in source.entity2sig

        # Fork before stepping — spawn_cache should transfer
        fork = await c.world_lifecycle.fork_world(
            source.world_id, name="fork", storage_config=storage
        )
        assert eid in fork.entity2sig

        # Step both worlds to materialize
        await c.application.step(source.world_id, RunConfig())
        await c.application.step(fork.world_id, RunConfig())

        # Query both — entity should exist in each
        source_df = await c.application.query_components(
            [Tag],
            world_id=str(source.world_id),
            run_id=str(source.run_id),
            storage_config=storage,
        )
        fork_df = await c.application.query_components(
            [Tag],
            world_id=str(fork.world_id),
            run_id=str(fork.run_id),
            storage_config=storage,
        )
        assert source_df.count_rows() >= 1
        assert fork_df.count_rows() >= 1
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_fork_reconciles_exact_committed_source_before_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    c = ServiceContainer()
    storage = _storage(tmp_path)
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="source"), storage)
        await c.application.create_entity(source.world_id, [Tag(label="committed")])
        catalog = c.storage_service.get_control_catalog(storage)
        real_publish = catalog.publish_manifest
        real_visible = catalog.visible_tokens
        published = False
        lose_visibility = True
        publish_tokens: list[str] = []

        async def commit_then_lose_response(*args, **kwargs):
            nonlocal published
            publish_tokens.append(str(kwargs["commit_token"]))
            result = await real_publish(*args, **kwargs)
            if not published:
                published = True
                raise RuntimeError("manifest committed but response was lost")
            return result

        async def lose_first_reconciliation_read(*args, **kwargs):
            nonlocal lose_visibility
            if published and lose_visibility:
                lose_visibility = False
                raise RuntimeError("visibility response unavailable")
            return await real_visible(*args, **kwargs)

        monkeypatch.setattr(catalog, "publish_manifest", commit_then_lose_response)
        monkeypatch.setattr(catalog, "visible_tokens", lose_first_reconciliation_read)

        with pytest.raises(AmbiguousTickCommitError):
            await c.application.step(source.world_id, RunConfig())

        fork = await c.world_lifecycle.fork_world(
            source.world_id,
            name="reconciled-fork",
            storage_config=storage,
        )

        assert source.tick == 1
        assert fork.tick == 1
        assert fork.lineage[-1] == (
            str(source.world_id),
            str(source.run_id),
            0,
        )
        assert publish_tokens == [publish_tokens[0], publish_tokens[0]]
        assert not source.has_prepared_tick_commit
        assert source.spawn_cache == {}
        manifests = await catalog.list_manifests(
            str(source.world_id),
            str(source.run_id),
        )
        assert len(manifests) == 1
        assert manifests[0].commit_token == publish_tokens[0]
        signature = source.entity2sig[1]
        physical_rows = (
            await source.updater.store.get_archetype_df(
                signature,
                str(source.world_id),
                str(source.run_id),
                ticks=[0],
            )
        ).to_pylist()
        assert len(physical_rows) == 1
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_fork_registers_no_child_while_source_publish_is_unreadable(
    tmp_path,
    monkeypatch,
) -> None:
    c = ServiceContainer()
    storage = _storage(tmp_path)
    catalog = None
    real_publish = None
    real_visible = None
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="source"), storage)
        await c.application.create_entity(source.world_id, [Tag(label="ambiguous")])
        catalog = c.storage_service.get_control_catalog(storage)
        real_publish = catalog.publish_manifest
        real_visible = catalog.visible_tokens
        publish_attempted = False

        async def unreadable_publish(*args, **kwargs):
            nonlocal publish_attempted
            publish_attempted = True
            raise RuntimeError("manifest response unavailable")

        async def unreadable_visibility(*args, **kwargs):
            if publish_attempted:
                raise RuntimeError("visibility response unavailable")
            return await real_visible(*args, **kwargs)

        monkeypatch.setattr(catalog, "publish_manifest", unreadable_publish)
        monkeypatch.setattr(catalog, "visible_tokens", unreadable_visibility)

        with pytest.raises(AmbiguousTickCommitError):
            await c.application.step(source.world_id, RunConfig())
        with pytest.raises(AmbiguousTickCommitError):
            await c.world_lifecycle.fork_world(
                source.world_id,
                name="must-not-register",
                storage_config=storage,
            )

        assert source.tick == 0
        assert source.has_prepared_tick_commit
        assert [world.name for world in await c.world_registry.list_worlds()] == ["source"]
        assert [record.name for record in await catalog.list_worlds()] == ["source"]
    finally:
        if catalog is not None and real_publish is not None and real_visible is not None:
            monkeypatch.setattr(catalog, "publish_manifest", real_publish)
            monkeypatch.setattr(catalog, "visible_tokens", real_visible)
        await c.shutdown()


# ---------------------------------------------------------------------------
# 2. Post-fork hook isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_fork_hook_isolation(tmp_path):
    """Register a hook on source AFTER forking. Step the fork.
    The hook should NOT fire on the fork."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    hook_fired: list[str] = []
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="src"), storage)
        await c.application.create_entity(source.world_id, [Tag(label="x")])

        # Fork first
        fork = await c.world_lifecycle.fork_world(
            source.world_id, name="fork", storage_config=storage
        )

        # Register hook on source AFTER the fork
        async def _on_post_tick(event: PostTick):
            hook_fired.append("source")

        await c.application.add_hook(source.world_id, PostTick, _on_post_tick)

        # Step the fork — the hook must NOT fire
        await c.application.step(fork.world_id, RunConfig())
        assert hook_fired == [], f"Hook fired on fork unexpectedly: {hook_fired}"

        # Step the source — the hook SHOULD fire
        await c.application.step(source.world_id, RunConfig())
        assert hook_fired == ["source"]
    finally:
        await c.shutdown()


# ---------------------------------------------------------------------------
# 3. Destroy storage row preservation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_destroy_storage_row_preservation(tmp_path):
    """Create world, spawn, step (rows in store). Destroy world.
    Query the store directly through the world query family — rows still exist."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    try:
        world = await c.world_lifecycle.create_world(WorldConfig(name="w"), storage)
        await c.application.create_entity(world.world_id, [Tag(label="keep")])
        await c.application.step(world.world_id, RunConfig())

        world_id = str(world.world_id)
        run_id = str(world.run_id)

        # Rows should be in the store
        df_before = await c.application.query_components(
            [Tag],
            world_id=world_id,
            run_id=run_id,
            storage_config=storage,
        )
        rows_before = df_before.count_rows()
        assert rows_before >= 1

        # Destroy the world (in-memory only; storage preserved)
        await c.world_lifecycle.destroy_world(world.world_id)

        # Query again — rows must still be present
        df_after = await c.application.query_components(
            [Tag],
            world_id=world_id,
            run_id=run_id,
            storage_config=storage,
        )
        assert df_after.count_rows() == rows_before
    finally:
        await c.shutdown()


# ---------------------------------------------------------------------------
# 4. Destroy audit row preservation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_destroy_audit_row_preservation(tmp_path):
    """Create world, do operations (audit rows emitted). Destroy.
    Query audit log — rows still there."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    ctx = _admin_ctx()
    try:
        info = await c.command_gateway.create_world(ctx, WorldConfig(name="aw"), storage)
        await c.command_gateway.create_entity(ctx, info.world_id, [Tag(label="audited")])
        await c.command_gateway.step(ctx, info.world_id, RunConfig())

        # Audit rows should exist
        audit_df_before = await c.audit_log.query(world_id=info.world_id)
        rows_before = audit_df_before.count_rows()
        assert rows_before >= 1

        # Destroy
        await c.command_gateway.destroy_world(ctx, info.world_id)

        # Audit rows must survive (append-only). The destroy itself adds a row too.
        audit_df_after = await c.audit_log.query(world_id=info.world_id)
        rows_after = audit_df_after.count_rows()
        assert rows_after >= rows_before
    finally:
        await c.shutdown()


# ---------------------------------------------------------------------------
# 5. Destroy terminally settles only the target world's pending commands
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gated_destroy_rejects_only_target_world_pending_commands(tmp_path):
    """Destroy preserves durable history and leaves sibling commands untouched."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    ctx = _admin_ctx()
    try:
        target = await c.command_gateway.create_world(ctx, WorldConfig(name="target"), storage)
        sibling = await c.command_gateway.create_world(ctx, WorldConfig(name="sibling"), storage)

        target_command = Command(
            type=CommandType.DESPAWN,
            tick=10_000,
            payload={"entity_id": _TEARDOWN_SENTINEL_ENTITY_ID},
        )
        sibling_command = Command(
            type=CommandType.DESPAWN,
            tick=10_000,
            payload={"entity_id": _TEARDOWN_SENTINEL_ENTITY_ID},
        )
        await c.command_gateway.submit(ctx, target.world_id, target_command)
        await c.command_gateway.submit(ctx, sibling.world_id, sibling_command)

        assert await c.command_scheduler.pending_count(target.world_id) == 1
        assert await c.command_scheduler.pending_count(sibling.world_id) == 1

        await c.command_gateway.destroy_world(ctx, target.world_id)

        assert await c.command_scheduler.pending_count(target.world_id) == 0
        target_records = await c.command_scheduler.records(target.world_id)
        assert [record.status for record in target_records] == ["REJECTED"]
        assert await c.command_scheduler.pending_count(sibling.world_id) == 1
        sibling_records = await c.command_scheduler.records(sibling.world_id)
        assert [record.command_id for record in sibling_records] == [str(sibling_command.id)]
        assert [record.status for record in sibling_records] == ["PENDING"]
        assert list(await c.command_scheduler.history(sibling.world_id)) == [
            Despawn(
                world_id=sibling.world_id,
                entity_id=_TEARDOWN_SENTINEL_ENTITY_ID,
            )
        ]
        live_sibling = await c.world_registry.live_world(str(sibling.world_id))
        assert live_sibling is not None
        assert live_sibling.world_id == sibling.world_id
    finally:
        await c.shutdown()


# ---------------------------------------------------------------------------
# 6. 10-world destroy stress test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_10_world_destroy_stress(tmp_path):
    """Create 10 worlds, spawn + step in each. Count total store queries
    possible. Destroy all 10. Verify same queries still return data."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    try:
        worlds = []
        for i in range(10):
            w = await c.world_lifecycle.create_world(WorldConfig(name=f"stress-{i}"), storage)
            await c.application.create_entity(w.world_id, [Tag(label=f"e{i}")])
            await c.application.step(w.world_id, RunConfig())
            worlds.append(w)

        # Collect row counts before destroy
        counts_before: dict[str, int] = {}
        for w in worlds:
            df = await c.application.query_components(
                [Tag],
                world_id=str(w.world_id),
                run_id=str(w.run_id),
                storage_config=storage,
            )
            counts_before[str(w.world_id)] = df.count_rows()
            assert counts_before[str(w.world_id)] >= 1

        # Destroy all 10
        for w in worlds:
            await c.world_lifecycle.destroy_world(w.world_id)

        # All queries must still return the same data
        for w in worlds:
            df = await c.application.query_components(
                [Tag],
                world_id=str(w.world_id),
                run_id=str(w.run_id),
                storage_config=storage,
            )
            assert df.count_rows() == counts_before[str(w.world_id)]
    finally:
        await c.shutdown()


# ---------------------------------------------------------------------------
# 7. Audit row monotonicity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_row_monotonicity(tmp_path):
    """Across a sequence of operations + destroys, audit row count never
    decreases."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    ctx = _admin_ctx()
    try:
        high_water = 0

        for i in range(3):
            info = await c.command_gateway.create_world(ctx, WorldConfig(name=f"mono-{i}"), storage)
            await c.command_gateway.create_entity(ctx, info.world_id, [Tag(label=f"m{i}")])
            await c.command_gateway.step(ctx, info.world_id, RunConfig())

            # Check monotonicity after operations
            current = (await c.audit_log.query()).count_rows()
            assert current >= high_water, f"Audit rows decreased: {current} < {high_water}"
            high_water = current

            # Destroy
            await c.command_gateway.destroy_world(ctx, info.world_id)

            # Check monotonicity after destroy
            current = (await c.audit_log.query()).count_rows()
            assert current >= high_water, (
                f"Audit rows decreased after destroy: {current} < {high_water}"
            )
            high_water = current

        # Final count should reflect all operations + destroys
        assert high_water > 0
    finally:
        await c.shutdown()


# ---------------------------------------------------------------------------
# 8. Fork inherits source's storage by default
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fork_inherits_source_storage(tmp_path):
    """spec: docs/guide/world-lifecycle.md § 4.5 — "The fork writes to the same
    physical store as the source by default".

    A fork created without an explicit storage_config must land in the source's
    store, not in a fresh default StorageConfig() (./archetype_db).
    """
    c = ServiceContainer()
    source_storage = StorageConfig(uri=str(tmp_path / "src_store"), namespace="ns")
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="src"), source_storage)
        await c.application.create_entity(source.world_id, [Tag(label="seed")])
        await c.application.step(source.world_id, RunConfig())

        # Fork without specifying storage — must inherit source's store.
        fork = await c.world_lifecycle.fork_world(source.world_id, name="fork")

        # Spawn a fresh entity in the fork and step it so a write is forced.
        fork_eid = await c.application.create_entity(fork.world_id, [Tag(label="fork-only")])
        await c.application.step(fork.world_id, RunConfig())

        rows_in_source_store = (
            await c.application.query_components(
                [Tag],
                world_id=str(fork.world_id),
                run_id=str(fork.run_id),
                storage_config=source_storage,
                entity_ids=[fork_eid],
            )
        ).count_rows()
        with pytest.raises(KeyError, match="not recorded in catalog"):
            await c.application.query_components(
                [Tag],
                world_id=str(fork.world_id),
                run_id=str(fork.run_id),
                storage_config=StorageConfig(),
                entity_ids=[fork_eid],
            )

        assert rows_in_source_store >= 1, (
            f"fork's data must land in source's store; got {rows_in_source_store} rows there"
        )
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_fork_explicit_storage_override(tmp_path):
    """spec: docs/guide/world-lifecycle.md § 4.5 — "The optional storage_config
    argument allows the fork to write to a different store entirely."

    Override path stays intact: an explicit storage_config wins over the
    source's store.
    """
    c = ServiceContainer()
    source_storage = StorageConfig(uri=str(tmp_path / "src_store"), namespace="ns")
    fork_storage = StorageConfig(uri=str(tmp_path / "fork_store"), namespace="ns")
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="src"), source_storage)
        await c.application.create_entity(source.world_id, [Tag(label="seed")])
        await c.application.step(source.world_id, RunConfig())

        fork = await c.world_lifecycle.fork_world(
            source.world_id, name="fork", storage_config=fork_storage
        )
        fork_eid = await c.application.create_entity(fork.world_id, [Tag(label="fork-only")])
        await c.application.step(fork.world_id, RunConfig())

        rows_in_fork_store = (
            await c.application.query_components(
                [Tag],
                world_id=str(fork.world_id),
                run_id=str(fork.run_id),
                storage_config=fork_storage,
                entity_ids=[fork_eid],
            )
        ).count_rows()
        with pytest.raises(KeyError, match="not recorded in catalog"):
            await c.application.query_components(
                [Tag],
                world_id=str(fork.world_id),
                run_id=str(fork.run_id),
                storage_config=source_storage,
                entity_ids=[fork_eid],
            )

        assert rows_in_fork_store >= 1
    finally:
        await c.shutdown()


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# 9. Fork lineage: pre-fork ticks readable through ancestry
# ---------------------------------------------------------------------------


class Score(Component):
    value: float = 0.0


@pytest.mark.asyncio
async def test_fork_after_step_reads_parent_history(tmp_path):
    """Fork a world that has already materialized rows. The fork's
    pre-fork ticks resolve to the parent's run via lineage."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="src"), storage)
        await c.application.create_entity(source.world_id, [Score(value=7.0)])
        await c.application.step(source.world_id, RunConfig())

        fork = await c.world_lifecycle.fork_world(
            source.world_id, name="fork", storage_config=storage
        )
        assert fork.lineage == [(str(source.world_id), str(source.run_id), source.tick - 1)]

        fork_df = await c.application.query_components(
            [Score],
            world_id=str(fork.world_id),
            run_id=str(fork.run_id),
            storage_config=storage,
        )
        rows = fork_df.to_pydict()
        assert rows["score__value"] == [7.0]
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_fork_after_step_processes_parent_state(tmp_path):
    """The fork's first step must read the parent's last tick as input,
    not an empty frame: state continues across the fork point."""
    from daft import DataFrame, col

    from archetype.core.aio.async_processor import AsyncProcessor

    class Inc(AsyncProcessor):
        components = (Score,)
        priority = 10

        async def process(self, df: DataFrame, **kw) -> DataFrame:
            return df.with_column("score__value", col("score__value") + 1.0)

    c = ServiceContainer()
    storage = _storage(tmp_path)
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="src"), storage)
        source.system.processors = [Inc()]
        await c.application.create_entity(source.world_id, [Score(value=0.0)])
        # Initial conditions persist raw at tick 0; the transition applies next tick.
        await c.application.step(source.world_id, RunConfig())  # 0.0 at tick 0
        await c.application.step(source.world_id, RunConfig())  # 1.0 at tick 1

        fork = await c.world_lifecycle.fork_world(
            source.world_id, name="fork", storage_config=storage
        )
        await c.application.step(fork.world_id, RunConfig())  # must see 1.0 -> 2.0

        fork_df = await c.application.query_components(
            [Score],
            world_id=str(fork.world_id),
            run_id=str(fork.run_id),
            storage_config=storage,
            ticks=[fork.tick - 1],
        )
        assert fork_df.to_pydict()["score__value"] == [2.0]
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_fork_lineage_excludes_parent_post_fork_rows(tmp_path):
    """A parent that keeps running after the fork must not leak its
    post-fork rows into the fork's history."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="src"), storage)
        await c.application.create_entity(source.world_id, [Score(value=1.0)])
        await c.application.step(source.world_id, RunConfig())

        fork = await c.world_lifecycle.fork_world(
            source.world_id, name="fork", storage_config=storage
        )
        # Parent advances past the fork point
        await c.application.step(source.world_id, RunConfig())
        await c.application.step(source.world_id, RunConfig())

        fork_df = await c.application.query_components(
            [Score],
            world_id=str(fork.world_id),
            run_id=str(fork.run_id),
            storage_config=storage,
        )
        # Only the single pre-fork tick — not the parent's two later ticks
        assert fork_df.count_rows() == 1
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_fork_of_fork_lineage_chain(tmp_path):
    """Lineage flattens across fork generations: a fork of a fork reads
    base history, mid-fork history, and its own rows."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    try:
        base = await c.world_lifecycle.create_world(WorldConfig(name="base"), storage)
        await c.application.create_entity(base.world_id, [Score(value=5.0)])
        await c.application.step(base.world_id, RunConfig())

        mid = await c.world_lifecycle.fork_world(base.world_id, name="mid", storage_config=storage)
        await c.application.step(mid.world_id, RunConfig())

        leaf = await c.world_lifecycle.fork_world(mid.world_id, name="leaf", storage_config=storage)
        assert len(leaf.lineage) == 2
        await c.application.step(leaf.world_id, RunConfig())

        leaf_df = await c.application.query_components(
            [Score],
            world_id=str(leaf.world_id),
            run_id=str(leaf.run_id),
            storage_config=storage,
        )
        ticks = sorted(leaf_df.to_pydict()["tick"])
        assert ticks == [0, 1, 2]  # base tick, mid tick, leaf tick
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_unstepped_fork_has_no_lineage_segment(tmp_path):
    """Forking an unstepped world adds no lineage segment — the
    pending-spawn-transfer contract covers that case unchanged."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="src"), storage)
        await c.application.create_entity(source.world_id, [Score(value=1.0)])

        fork = await c.world_lifecycle.fork_world(
            source.world_id, name="fork", storage_config=storage
        )
        assert fork.lineage == []
    finally:
        await c.shutdown()


# ---------------------------------------------------------------------------
# 10. Persisted lineage: ancestry survives dead worlds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_destroyed_fork_ancestry_remains_resolvable(tmp_path):
    """Lineage is persisted append-only at fork time: after the fork is
    destroyed, gated reads still resolve pre-fork ticks through ancestry."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    ctx = _admin_ctx()
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="src"), storage)
        await c.application.create_entity(source.world_id, [Score(value=9.0)])
        await c.application.step(source.world_id, RunConfig())

        fork = await c.world_lifecycle.fork_world(
            source.world_id, name="fork", storage_config=storage
        )
        await c.application.step(fork.world_id, RunConfig())
        fork_world_id, fork_run_id = str(fork.world_id), str(fork.run_id)
        expected_lineage = list(fork.lineage)

        await c.command_gateway.destroy_world(ctx, fork.world_id)

        # Persisted lineage is recoverable without the live world object
        recovered = await get_lineage(
            c.storage_service,
            fork_world_id,
            fork_run_id,
            storage_config=storage,
        )
        assert recovered == expected_lineage

        # The trusted durable read remains available after live-world cleanup.
        df = await c.application.query_components(
            [Score],
            fork_world_id,
            fork_run_id,
            storage_config=storage,
        )
        assert sorted(df.to_pydict()["tick"]) == [0, 1]
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_root_world_has_no_persisted_lineage(tmp_path):
    """Root worlds record nothing; get_lineage returns None, not []."""
    c = ServiceContainer()
    storage = _storage(tmp_path)
    try:
        source = await c.world_lifecycle.create_world(WorldConfig(name="src"), storage)
        await c.application.create_entity(source.world_id, [Score(value=1.0)])
        await c.application.step(source.world_id, RunConfig())

        recovered = await get_lineage(
            c.storage_service,
            str(source.world_id),
            str(source.run_id),
            storage_config=storage,
        )
        assert recovered is None
    finally:
        await c.shutdown()
