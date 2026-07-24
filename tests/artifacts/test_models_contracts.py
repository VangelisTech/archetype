# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical artifact-model and compatibility-shim contracts."""

from __future__ import annotations

import subprocess
import sys
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


def test_contracts_are_object_identical_model_exports() -> None:
    from archetype.artifacts import contracts, models

    for name in (
        "ArtifactContext",
        "ArtifactRef",
        "ArtifactSource",
        "ArtifactStoreConfig",
    ):
        assert getattr(contracts, name) is getattr(models, name)


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


def test_models_and_contract_shim_are_import_light() -> None:
    root = Path(__file__).resolve().parents[2]
    script = """
import json
import sys
import archetype.artifacts.models
import archetype.artifacts.contracts
archetype.artifacts.models.ArtifactSource(source_uri="evidence.txt")
archetype.artifacts.models.ArtifactStoreConfig.local("./objects")
heavy = sorted(
    name
    for name in sys.modules
    if name == "daft"
    or name.startswith("daft.")
    or name == "pyarrow"
    or name.startswith("pyarrow.")
    or name == "lancedb"
    or name.startswith("lancedb.")
)
print(json.dumps(heavy))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "[]"
