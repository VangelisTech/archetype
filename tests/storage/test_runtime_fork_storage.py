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

from functools import partial

import pytest
from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component
from archetype.core.config import StorageConfig
from archetype.storage.service import StorageService
from archetype.world.models import ComponentTypeRef, QueryComponents
from archetype.world.query import get_lineage
from archetype.world.registry import WorldRegistry


class Meter(Component):
    value: float = 0.0


class Inc(AsyncProcessor):
    components = (Meter,)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column("meter__value", col("meter__value") + 1.0)


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


def _query_owners(runtime: ArchetypeRuntime) -> tuple[WorldRegistry, StorageService]:
    """Return the true owners bound into the exact query operation."""

    handler = runtime._resources.dispatcher._registry.resolve_name("query_components").handler
    assert isinstance(handler, partial)
    assert len(handler.args) == 2
    registry, storage = handler.args
    assert isinstance(registry, WorldRegistry)
    assert isinstance(storage, StorageService)
    return registry, storage


async def _query_exact(
    runtime: ArchetypeRuntime,
    component: type[Component],
    *,
    world_id: object,
    run_id: object,
    storage_config: StorageConfig | None = None,
):
    return await runtime._resources.dispatcher.apply(
        QueryComponents(
            components=(ComponentTypeRef.from_type(component),),
            world_id=world_id,
            run_id=run_id,
            storage_config=storage_config,
        )
    )


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

        fork_info = await fork.info()
        rows_in_source_store = (
            await _query_exact(
                rt,
                Meter,
                world_id=fork.world_id,
                run_id=fork_info.run_id,
                storage_config=storage,
            )
        ).count_rows()
        with pytest.raises(KeyError, match="not recorded in catalog"):
            await _query_exact(
                rt,
                Meter,
                world_id=fork.world_id,
                run_id=fork_info.run_id,
                storage_config=StorageConfig(),
            )
        assert rows_in_source_store >= 1


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

        fork_info = await fork.info()
        registry, _storage_service = _query_owners(rt)
        record = await registry.storage_record(str(fork.world_id))
        assert record is not None
        assert record[0].uri == fork_storage.uri
        rows = (
            await _query_exact(
                rt,
                Meter,
                world_id=fork.world_id,
                run_id=fork_info.run_id,
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
        fork_info = await fork.info()
        registry, storage_service = _query_owners(rt)
        fork_world = await registry.live_world(str(fork.world_id))
        assert fork_world is not None
        assert fork_info.run_id, "fork must mint its run_id at fork time"

        recovered = await get_lineage(
            storage_service,
            str(fork.world_id),
            str(fork_info.run_id),
            storage_config=storage,
        )
        assert recovered == list(fork_world.lineage)
        assert recovered, "persisted lineage must be recoverable by the fork's run_id"


@pytest.mark.asyncio
async def test_dispatcher_query_resolves_world_storage_without_config(tmp_path):
    """The dispatcher knows where a world's rows live: a read with no
    storage_config resolves the world's recorded store."""
    storage = _storage(tmp_path)
    async with ArchetypeRuntime() as rt:
        world = rt.world("src", storage=storage, processors=[Inc()])
        await world.spawn(Meter(value=0.0))
        await world.step()

        info = await world.info()
        df = await _query_exact(
            rt,
            Meter,
            world_id=world.world_id,
            run_id=info.run_id,
            # No storage config: the exact query must find the rows anyway.
        )
        assert df.count_rows() >= 1
