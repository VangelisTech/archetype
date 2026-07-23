# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Catalog registration and append contracts for generalized ingestion."""

from pathlib import Path

import daft
import pytest

from archetype.app.container import ServiceContainer
from archetype.core.config import StorageBackend, StorageConfig, WorldConfig

READINGS = "readings"


def _storage(tmp_path: Path) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )


@pytest.mark.contract("ingestion.envelope.append_selection")
@pytest.mark.asyncio
async def test_append_registers_table_in_active_daft_catalog(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)

        rows_written = await container.ingestion_service.append(
            str(world.world_id),
            READINGS,
            daft.from_pydict({"reading_id": ["r1"], "value": [21.5]}),
            key_columns=("reading_id",),
        )

        store = await container.storage_service.get_or_create_store(storage)
        assert store.session.current_catalog().has_table("ns.readings")
        assert rows_written == 1
        assert (
            await container.ingestion_service.read(str(world.world_id), READINGS)
        ).to_pylist() == [
            {
                "world_id": str(world.world_id),
                "run_id": str(world.run_id),
                "reading_id": "r1",
                "value": 21.5,
            }
        ]
    finally:
        await container.shutdown()


@pytest.mark.contract("ingestion.catalog.cold_roundtrip")
@pytest.mark.asyncio
async def test_registered_table_is_queryable_from_fresh_application(tmp_path, monkeypatch):
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "control"))
    storage = _storage(tmp_path)
    writer = ServiceContainer()
    try:
        world = await writer.world_service.create_world(WorldConfig(name="w"), storage)
        world_id = str(world.world_id)
        run_id = str(world.run_id)
        await writer.ingestion_service.append(
            world_id,
            READINGS,
            daft.from_pydict({"reading_id": ["r1"], "value": [21.5]}),
            key_columns=("reading_id",),
        )
    finally:
        await writer.shutdown()

    reader = ServiceContainer()
    try:
        rows = await reader.ingestion_service.read(
            world_id,
            READINGS,
            storage_config=storage,
        )
        assert rows.to_pylist() == [
            {
                "world_id": world_id,
                "run_id": run_id,
                "reading_id": "r1",
                "value": 21.5,
            }
        ]
    finally:
        await reader.shutdown()


@pytest.mark.asyncio
async def test_append_is_idempotent_by_declared_key(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        rows = daft.from_pydict({"reading_id": ["r1"], "value": [21.5]})

        first = await container.ingestion_service.append(
            str(world.world_id), READINGS, rows, key_columns=("reading_id",)
        )
        retry = await container.ingestion_service.append(
            str(world.world_id), READINGS, rows, key_columns=("reading_id",)
        )

        assert first == 1
        assert retry == 0
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_registered_table_rejects_schema_drift(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        await container.ingestion_service.append(
            str(world.world_id),
            READINGS,
            daft.from_pydict({"reading_id": ["r1"], "value": [21.5]}),
            key_columns=("reading_id",),
        )

        with pytest.raises(ValueError, match="different typed schema"):
            await container.ingestion_service.append(
                str(world.world_id),
                READINGS,
                daft.from_pydict({"reading_id": ["r2"], "value": ["hot"]}),
                key_columns=("reading_id",),
            )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_read_preserves_missing_world_error(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)

        with pytest.raises(KeyError, match="world missing-world is not recorded"):
            await container.ingestion_service.read(
                "missing-world",
                READINGS,
                storage_config=storage,
            )
    finally:
        await container.shutdown()
