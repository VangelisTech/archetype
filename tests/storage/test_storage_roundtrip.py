# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Storage integration tests: write -> read -> verify round-trip."""

import pytest
from uuid_utils import uuid7

from archetype.core.aio import AsyncQueryManager, AsyncUpdateManager
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from tests.conftest import make_storage_service


class Position(Component):
    x: int
    y: int


class TestStorageServiceMultiton:
    @pytest.mark.asyncio
    async def test_get_backend_returns_triplet(self, tmp_path):
        ss = make_storage_service()
        try:
            store = await ss.get_or_create_store(
                StorageConfig(uri=str(tmp_path / "s"), namespace="ns")
            )
            querier = AsyncQueryManager(store=store)
            updater = AsyncUpdateManager(store=store)
            assert store is not None
            assert hasattr(store, "append")
            assert isinstance(querier, AsyncQueryManager)
            assert isinstance(updater, AsyncUpdateManager)
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_multiton_same_config_same_instances(self, tmp_path):
        ss = make_storage_service()
        try:
            cfg = StorageConfig(uri=str(tmp_path / "s"), namespace="ns")
            s1 = await ss.get_or_create_store(cfg)
            s2 = await ss.get_or_create_store(cfg)
            assert s1 is s2
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_multiton_different_config_different_instances(self, tmp_path):
        ss = make_storage_service()
        try:
            s1 = await ss.get_or_create_store(
                StorageConfig(uri=str(tmp_path / "a"), namespace="ns")
            )
            s2 = await ss.get_or_create_store(
                StorageConfig(uri=str(tmp_path / "b"), namespace="ns")
            )
            assert s1 is not s2
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_clears_instances(self, tmp_path):
        ss = make_storage_service()
        await ss.get_or_create_store(StorageConfig(uri=str(tmp_path / "s"), namespace="ns"))
        assert len(ss._instances) == 1
        await ss.shutdown()
        assert len(ss._instances) == 0


class TestStorageRoundTrip:
    @pytest.mark.asyncio
    async def test_write_and_read_back(self, tmp_path):
        """Write a DataFrame via updater, read back via querier, verify data."""
        ss = make_storage_service()
        try:
            store = await ss.get_or_create_store(
                StorageConfig(uri=str(tmp_path / "rt"), namespace="ns")
            )
            querier = AsyncQueryManager(store=store)
            updater = AsyncUpdateManager(store=store)

            # Build a signature and a row
            sig = Archetype.sig_from_components([Position(x=0, y=0)])
            world_id = str(uuid7())
            run_id = str(uuid7())

            row = Archetype.to_row_dict(
                entity_id=1,
                tick=0,
                components=[Position(x=10, y=20)],
                world_id=world_id,
                run_id=run_id,
            )

            import daft
            import pyarrow as pa

            schema = Archetype.get_archetype_schema(sig)
            table = pa.Table.from_pylist([row], schema=schema)
            df = daft.from_arrow(table)

            await updater.update(df, sig, tick=0, world_id=world_id, run_id=run_id)

            # Read back
            result_df = await querier.query_archetype(
                sig=sig,
                world_id=world_id,
                run_id=run_id,
                ticks=[0],
            )
            rows = result_df.collect().to_pylist()
            assert len(rows) == 1
            assert rows[0]["position__x"] == 10
            assert rows[0]["position__y"] == 20
            assert rows[0]["entity_id"] == 1
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_multiple_entities_round_trip(self, tmp_path):
        ss = make_storage_service()
        try:
            store = await ss.get_or_create_store(
                StorageConfig(uri=str(tmp_path / "rt2"), namespace="ns")
            )
            querier = AsyncQueryManager(store=store)
            updater = AsyncUpdateManager(store=store)

            sig = Archetype.sig_from_components([Position(x=0, y=0)])
            world_id = str(uuid7())
            run_id = str(uuid7())

            rows = [
                Archetype.to_row_dict(1, 0, [Position(x=1, y=2)], world_id, run_id),
                Archetype.to_row_dict(2, 0, [Position(x=3, y=4)], world_id, run_id),
            ]

            import daft
            import pyarrow as pa

            schema = Archetype.get_archetype_schema(sig)
            table = pa.Table.from_pylist(rows, schema=schema)
            df = daft.from_arrow(table)

            await updater.update(df, sig, tick=0, world_id=world_id, run_id=run_id)

            result_df = await querier.query_archetype(
                sig=sig,
                world_id=world_id,
                run_id=run_id,
                ticks=[0],
            )
            result = result_df.collect().to_pylist()
            assert len(result) == 2
            xs = sorted(r["position__x"] for r in result)
            assert xs == [1, 3]
        finally:
            await ss.shutdown()
