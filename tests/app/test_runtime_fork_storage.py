# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runtime-layer fork storage contracts.

The service layer inherits a fork's storage from its source when no
override is given (world-lifecycle.md § 4.5) and reads pre-fork ticks
through lineage. These tests pin the RUNTIME layer's side of that
bargain: RuntimeWorld.fork() must not manufacture a default
StorageConfig (which would strand the fork on a store its source never
wrote to), gated reads must resolve a world's recorded store, and the
fork's persisted lineage rows must be keyed by its real run_id.

The service-level tests in tests/integration/test_fork_destroy_contracts.py
pass storage_config=None directly, so they cannot catch a runtime layer
that replaces None with a default before the service ever sees it.
"""

from __future__ import annotations

import pytest
from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.core.config import StorageConfig


class Meter(Component):
    value: float = 0.0


class Inc(AsyncProcessor):
    components = (Meter,)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column("meter__value", col("meter__value") + 1.0)


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


@pytest.mark.asyncio
async def test_runtime_fork_reads_parent_history(tmp_path):
    """fork() with no storage argument sees the source's persisted ticks."""
    async with ArchetypeRuntime() as rt:
        world = rt.world("src", storage=_storage(tmp_path), processors=[Inc()])
        await world.spawn(Meter(value=0.0))
        await world.step()
        await world.step()

        fork = await world.fork("fork")
        df = await fork.query(Meter)
        rows = df.to_pylist()
        assert len(rows) == 2, (
            f"fork must read its pre-fork history through lineage; got {len(rows)} rows"
        )
        assert sorted(r["tick"] for r in rows) == [0, 1]


@pytest.mark.asyncio
async def test_runtime_fork_step_continues_parent_state(tmp_path):
    """The fork's first step transforms the source's last state, not an
    empty frame: values continue across the fork point."""
    async with ArchetypeRuntime() as rt:
        world = rt.world("src", storage=_storage(tmp_path), processors=[Inc()])
        await world.spawn(Meter(value=0.0))
        await world.step()  # initial conditions persist: 0.0 at tick 0
        await world.step()  # value -> 1.0 at tick 1

        fork = await world.fork("fork")
        await fork.step()  # must read 1.0, write 2.0 at tick 2

        df = await fork.query(Meter)
        by_tick = {r["tick"]: r["meter__value"] for r in df.to_pylist()}
        assert by_tick[2] == 2.0, f"fork did not continue source state: {by_tick}"
        # Pre-fork history still visible alongside the fork's own row
        assert by_tick[0] == 0.0 and by_tick[1] == 1.0


@pytest.mark.asyncio
async def test_runtime_fork_writes_land_in_source_store(tmp_path):
    """A fork created without a storage override writes to the source's
    store — never to a fresh default ./archetype_db."""
    storage = _storage(tmp_path)
    async with ArchetypeRuntime() as rt:
        world = rt.world("src", storage=storage, processors=[Inc()])
        await world.spawn(Meter(value=0.0))
        await world.step()

        fork = await world.fork("fork")
        await fork.step()

        container = rt._container
        fork_world = container.world_service.get_world(fork.world_id)
        rows_in_source_store = (
            await container.query_service.query_components(
                [Meter],
                world_id=str(fork.world_id),
                run_id=str(fork_world.run_id),
                storage_config=storage,
            )
        ).count_rows()
        rows_in_default_store = (
            await container.query_service.query_components(
                [Meter],
                world_id=str(fork.world_id),
                run_id=str(fork_world.run_id),
                storage_config=StorageConfig(),
            )
        ).count_rows()
        assert rows_in_source_store >= 1
        assert rows_in_default_store == 0, (
            "fork wrote to the default store instead of inheriting the source's"
        )


@pytest.mark.asyncio
async def test_runtime_fork_explicit_storage_override_still_wins(tmp_path, caplog):
    """An explicit storage argument routes the fork to a different store.

    Cross-store forks sever read lineage (world-lifecycle.md § 4.5): only
    pending state carries over, so new writes land in the override store
    and the engine warns that persisted history stays behind.
    """
    import logging

    fork_storage = StorageConfig(uri=str(tmp_path / "fork_store"), namespace="ns")
    async with ArchetypeRuntime() as rt:
        world = rt.world("src", storage=_storage(tmp_path), processors=[Inc()])
        await world.spawn(Meter(value=0.0))
        await world.step()

        with caplog.at_level(logging.WARNING):
            fork = await world.fork("fork", storage=fork_storage)
        assert any("persisted history" in rec.message for rec in caplog.records), (
            "cross-store fork of a stepped source must warn that lineage is severed"
        )

        # New state spawned in the fork lands in the override store.
        await fork.spawn(Meter(value=10.0))
        await fork.step()

        container = rt._container
        fork_world = container.world_service.get_world(fork.world_id)
        record = container.world_service.storage_record(fork.world_id)
        assert record is not None
        assert record[0].uri == fork_storage.uri
        rows = (
            await container.query_service.query_components(
                [Meter],
                world_id=str(fork.world_id),
                run_id=str(fork_world.run_id),
                storage_config=fork_storage,
            )
        ).count_rows()
        assert rows >= 1


@pytest.mark.asyncio
async def test_fork_lineage_persisted_under_fork_run_id(tmp_path):
    """The fork's run_id exists at fork time, so its durable lineage rows
    are keyed by the id its own rows will carry — not by str(None)."""
    storage = _storage(tmp_path)
    async with ArchetypeRuntime() as rt:
        world = rt.world("src", storage=storage, processors=[Inc()])
        await world.spawn(Meter(value=0.0))
        await world.step()

        fork = await world.fork("fork")
        container = rt._container
        fork_world = container.world_service.get_world(fork.world_id)
        assert fork_world.run_id, "fork must mint its run_id at fork time"

        recovered = await container.query_service.get_lineage(
            str(fork.world_id), str(fork_world.run_id), storage_config=storage
        )
        assert recovered == list(fork_world.lineage)
        assert recovered, "persisted lineage must be recoverable by the fork's run_id"


@pytest.mark.asyncio
async def test_gated_query_resolves_world_storage_without_config(tmp_path):
    """The gate knows where a world's rows live: a gated read with no
    storage_config resolves the world's recorded store."""
    from archetype.runtime._actor import default_actor_ctx

    storage = _storage(tmp_path)
    async with ArchetypeRuntime() as rt:
        world = rt.world("src", storage=storage, processors=[Inc()])
        await world.spawn(Meter(value=0.0))
        await world.step()

        container = rt._container
        live = container.world_service.get_world(world.world_id)
        df = await container.command_service.query_components(
            default_actor_ctx(),
            [Meter],
            str(world.world_id),
            str(live.run_id),
            # no storage_config: the gate must find the rows anyway
        )
        assert df.count_rows() >= 1
