# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Catalog registration and append contracts for generalized ingestion."""

from pathlib import Path

import daft
import pytest

from archetype.app.container import ServiceContainer
from archetype.core.config import StorageBackend, StorageConfig, WorldConfig
from archetype.ingestion import IngestionTable

READINGS = IngestionTable("readings", key_columns=("reading_id",))


def _storage(tmp_path: Path) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )


@pytest.mark.parametrize(
    ("name", "keys", "message"),
    [
        ("bad-name", ("id",), "table names"),
        ("events", (), "at least one key"),
        ("events", ("id", "id"), "must be unique"),
        ("events", ("bad-key",), "invalid ingestion key"),
        ("events", ("world_id",), "service-owned envelope"),
    ],
)
def test_ingestion_table_rejects_ambiguous_identity(name, keys, message):
    with pytest.raises(ValueError, match=message):
        IngestionTable(name, key_columns=keys)


@pytest.mark.asyncio
async def test_append_registers_table_in_active_daft_catalog(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)

        version = await container.ingestion_service.append(
            str(world.world_id),
            READINGS,
            daft.from_pydict({"reading_id": ["r1"], "value": [21.5]}),
        )

        iceberg = await container.storage_service.get_iceberg_context(storage)
        assert iceberg.catalog.has_table("ns.readings")
        assert version.table_name == "readings"
        assert version.rows_written == 1
        assert version.snapshot_id is not None
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


@pytest.mark.asyncio
async def test_append_is_idempotent_by_declared_key(tmp_path):
    container = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await container.world_service.create_world(WorldConfig(name="w"), storage)
        rows = daft.from_pydict({"reading_id": ["r1"], "value": [21.5]})

        first = await container.ingestion_service.append(str(world.world_id), READINGS, rows)
        retry = await container.ingestion_service.append(str(world.world_id), READINGS, rows)

        assert first.rows_written == 1
        assert retry.rows_written == 0
        assert retry.snapshot_id == first.snapshot_id
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
        )

        with pytest.raises(ValueError, match="different typed schema"):
            await container.ingestion_service.append(
                str(world.world_id),
                READINGS,
                daft.from_pydict({"reading_id": ["r2"], "value": ["hot"]}),
            )
    finally:
        await container.shutdown()
