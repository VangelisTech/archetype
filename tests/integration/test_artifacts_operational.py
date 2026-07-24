# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free source/wheel proof for durable artifact publication."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from uuid_utils import UUID

from archetype import ArchetypeRuntime
from archetype.artifacts.models import ArtifactSource, ArtifactStoreConfig
from archetype.artifacts.pipeline import ARTIFACT_TEXT
from archetype.artifacts.views import read_artifact_index
from archetype.core.config import StorageBackend, StorageConfig
from archetype.runtime_resources import RuntimeCloseState

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("artifacts.ingestion.common_visibility"),
    pytest.mark.integration,
]


async def test_local_artifacts_round_trip_across_a_cold_explicit_handle(
    tmp_path: Path,
) -> None:
    payload = b"durable artifact evidence\n"
    first_source = tmp_path / "first.txt"
    second_source = tmp_path / "second.txt"
    first_source.write_bytes(payload)
    second_source.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    storage = StorageConfig(
        uri=str(tmp_path / "artifact-store"),
        namespace="artifact_operational",
        backend=StorageBackend.ICEBERG,
    )
    object_root = tmp_path / "objects"
    store_config = ArtifactStoreConfig.local(object_root)
    first_resources = None

    async with ArchetypeRuntime(artifact_store=store_config) as runtime:
        first_resources = runtime._resources
        world = runtime.world("artifact-operational", storage=storage)
        await world.step()
        references = await world.ingest_artifacts(
            ArtifactSource(
                source_uri=str(first_source),
                logical_path="evidence/first.txt",
            ),
            ArtifactSource(
                source_uri=str(second_source),
                logical_path="evidence/second.txt",
            ),
        )
        world_id = str(world.world_id)
        catalog = runtime._resources._storage.get_control_catalog(storage)
        durable_world = await catalog.get_world(world_id)
        assert durable_world is not None
        durable_tick = int(durable_world.tick_head)
        assert await catalog.max_manifest_tick(world_id, str(durable_world.run_id)) == durable_tick
        common = (await world.artifacts()).sort("logical_path").to_pylist()
        typed = (
            await read_artifact_index(
                runtime._resources._storage,
                world_id,
                ARTIFACT_TEXT,
                storage_config=storage,
            )
        ).to_pylist()

        assert len(references) == 2
        assert len({reference.artifact_id for reference in references}) == 2
        assert all(UUID(reference.artifact_id).version == 7 for reference in references)
        assert {reference.sha256 for reference in references} == {digest}
        assert len({reference.uri for reference in references}) == 1
        expected_object = (object_root / "objects" / "sha256" / digest[:2] / digest).resolve()
        assert {reference.uri for reference in references} == {expected_object.as_uri()}
        assert expected_object.read_bytes() == payload
        assert [row["logical_path"] for row in common] == [
            "evidence/first.txt",
            "evidence/second.txt",
        ]
        assert {row["artifact_id"] for row in common} == {
            reference.artifact_id for reference in references
        }
        assert {row["artifact_id"] for row in typed} == {row["artifact_id"] for row in common}
        assert {row["tick"] for row in common} == {durable_tick}
        assert {row["source_uri"] for row in common} == {
            str(first_source),
            str(second_source),
        }
        assert first_source.read_bytes() == second_source.read_bytes() == payload

    assert first_resources is not None
    assert first_resources.close_state is RuntimeCloseState.CLOSED

    cold_resources = None
    async with ArchetypeRuntime() as runtime:
        cold_resources = runtime._resources
        cold = runtime.attach(world_id, storage=storage)
        cold_common = (await cold.artifacts()).sort("logical_path").to_pylist()
        cold_typed = (
            await read_artifact_index(
                runtime._resources._storage,
                world_id,
                ARTIFACT_TEXT,
                storage_config=storage,
            )
        ).to_pylist()

        assert cold_common == common
        assert cold_typed == typed

    assert cold_resources is not None
    assert cold_resources.close_state is RuntimeCloseState.CLOSED
