# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical artifact-model contracts."""

from __future__ import annotations

from pathlib import Path

import pytest
from daft.io import IOConfig
from pydantic import ValidationError
from uuid_utils import uuid7

from archetype.core.config import StorageBackend, StorageConfig


def _storage(tmp_path: Path) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )


def test_exact_operations_require_explicit_storage_coordinates(tmp_path: Path) -> None:
    from archetype.artifacts.models import (
        ArtifactSource,
        ArtifactStoreConfig,
        IngestArtifacts,
        QueryArtifacts,
    )

    source = ArtifactSource(source_uri=str(tmp_path / "evidence.txt"))
    storage = _storage(tmp_path)

    with pytest.raises(ValidationError, match="storage_config"):
        IngestArtifacts(world_id="world-1", sources=(source,))
    with pytest.raises(ValidationError, match="storage_config"):
        QueryArtifacts(world_id="world-1")

    assert (
        IngestArtifacts(
            world_id="world-1",
            sources=(source,),
            storage_config=storage,
        ).storage_config
        is storage
    )
    assert (
        QueryArtifacts(
            world_id="world-1",
            storage_config=storage,
        ).storage_config
        is storage
    )
    assert (
        IngestArtifacts(
            world_id="  world-1  ",
            sources=(source,),
            storage_config=storage,
        ).world_id
        == "world-1"
    )
    assert (
        QueryArtifacts(
            world_id="\tworld-1\n",
            storage_config=storage,
        ).world_id
        == "world-1"
    )
    with pytest.raises(ValidationError, match="world_id must not be empty"):
        IngestArtifacts(
            world_id=" \t ",
            sources=(source,),
            storage_config=storage,
        )
    with pytest.raises(ValidationError, match="world_id must not be empty"):
        QueryArtifacts(world_id="\n", storage_config=storage)

    io_config = IOConfig()
    assert ArtifactStoreConfig(io_config=io_config).io_config is io_config
    with pytest.raises(ValidationError, match="Daft IOConfig"):
        ArtifactStoreConfig(io_config=object())
    with pytest.raises(ValidationError, match="StorageConfig"):
        QueryArtifacts(world_id="world-1", storage_config=object())

    world_id = uuid7()
    operation = IngestArtifacts(
        world_id=world_id,
        sources=(source,),
        storage_config=storage,
    )
    encoded = operation.model_dump_json()
    decoded = IngestArtifacts.model_validate_json(encoded)
    assert decoded.world_id == str(world_id)
    assert decoded.sources == operation.sources
    assert decoded.storage_config == storage
    assert decoded.model_dump(mode="json") == operation.model_dump(mode="json")

    query = QueryArtifacts(world_id=world_id, storage_config=storage)
    decoded_query = QueryArtifacts.model_validate_json(query.model_dump_json())
    assert decoded_query.world_id == str(world_id)
    assert decoded_query.storage_config == storage
    assert decoded_query.model_dump(mode="json") == query.model_dump(mode="json")


@pytest.mark.parametrize(
    "invalid_world_id",
    (None, 1, True, object()),
    ids=("none", "integer", "boolean", "object"),
)
def test_exact_operations_reject_non_string_world_ids(
    tmp_path: Path,
    invalid_world_id: object,
) -> None:
    from archetype.artifacts.models import (
        ArtifactSource,
        IngestArtifacts,
        QueryArtifacts,
    )

    source = ArtifactSource(source_uri=str(tmp_path / "evidence.txt"))
    storage = _storage(tmp_path)

    with pytest.raises(ValidationError, match="world_id must be a string or UUID"):
        IngestArtifacts.model_validate(
            {
                "world_id": invalid_world_id,
                "sources": (source,),
                "storage_config": storage,
            },
        )
    with pytest.raises(ValidationError, match="world_id must be a string or UUID"):
        QueryArtifacts.model_validate(
            {
                "world_id": invalid_world_id,
                "storage_config": storage,
            },
        )
