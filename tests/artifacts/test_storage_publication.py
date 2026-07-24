# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Storage-backed publication contracts consumed by the artifacts family."""

from pathlib import Path

import daft
import pytest

from archetype.core.config import StorageBackend, StorageConfig, WorldConfig
from tests.conftest import make_world_harness

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
    harness = make_world_harness()
    try:
        storage = _storage(tmp_path)
        world = await harness.lifecycle.create_world(WorldConfig(name="w"), storage)

        rows_written = await harness.storage.append_world_rows(
            storage,
            str(world.world_id),
            READINGS,
            daft.from_pydict({"reading_id": ["r1"], "value": [21.5]}),
            key_columns=("reading_id",),
        )

        store = await harness.storage.get_or_create_store(storage)
        assert store.session.current_catalog().has_table("ns.readings")
        assert rows_written == 1
        assert (
            await harness.storage.read_world_rows(
                storage,
                str(world.world_id),
                READINGS,
            )
        ).to_pylist() == [
            {
                "world_id": str(world.world_id),
                "run_id": str(world.run_id),
                "reading_id": "r1",
                "value": 21.5,
            }
        ]
    finally:
        await harness.close()


@pytest.mark.contract("ingestion.catalog.cold_roundtrip")
@pytest.mark.asyncio
async def test_registered_table_is_queryable_from_fresh_application(tmp_path, monkeypatch):
    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "control"))
    storage = _storage(tmp_path)
    writer = make_world_harness()
    try:
        world = await writer.lifecycle.create_world(WorldConfig(name="w"), storage)
        world_id = str(world.world_id)
        run_id = str(world.run_id)
        await writer.storage.append_world_rows(
            storage,
            world_id,
            READINGS,
            daft.from_pydict({"reading_id": ["r1"], "value": [21.5]}),
            key_columns=("reading_id",),
        )
    finally:
        await writer.close()

    reader = make_world_harness()
    try:
        rows = await reader.storage.read_world_rows(
            storage,
            world_id,
            READINGS,
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
        await reader.close()


@pytest.mark.asyncio
async def test_append_is_idempotent_by_declared_key(tmp_path):
    harness = make_world_harness()
    try:
        storage = _storage(tmp_path)
        world = await harness.lifecycle.create_world(WorldConfig(name="w"), storage)
        rows = daft.from_pydict({"reading_id": ["r1"], "value": [21.5]})

        first = await harness.storage.append_world_rows(
            storage,
            str(world.world_id),
            READINGS,
            rows,
            key_columns=("reading_id",),
        )
        retry = await harness.storage.append_world_rows(
            storage,
            str(world.world_id),
            READINGS,
            rows,
            key_columns=("reading_id",),
        )

        assert first == 1
        assert retry == 0
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_registered_table_rejects_schema_drift(tmp_path):
    harness = make_world_harness()
    try:
        storage = _storage(tmp_path)
        world = await harness.lifecycle.create_world(WorldConfig(name="w"), storage)
        await harness.storage.append_world_rows(
            storage,
            str(world.world_id),
            READINGS,
            daft.from_pydict({"reading_id": ["r1"], "value": [21.5]}),
            key_columns=("reading_id",),
        )

        with pytest.raises(ValueError, match="different typed schema"):
            await harness.storage.append_world_rows(
                storage,
                str(world.world_id),
                READINGS,
                daft.from_pydict({"reading_id": ["r2"], "value": ["hot"]}),
                key_columns=("reading_id",),
            )
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_read_preserves_missing_world_error(tmp_path):
    harness = make_world_harness()
    try:
        storage = _storage(tmp_path)

        with pytest.raises(KeyError, match="world missing-world is not recorded"):
            await harness.storage.read_world_rows(
                storage,
                "missing-world",
                READINGS,
            )
    finally:
        await harness.close()
