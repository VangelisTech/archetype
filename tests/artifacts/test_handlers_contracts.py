# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Storage-only artifact handler contracts."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

from archetype.core.config import StorageBackend, StorageConfig
from archetype.storage.catalog import WorldRecord
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService

_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M/wHwAF/gL+XfBvAAAAAElFTkSuQmCC"
)


def _storage(tmp_path: Path) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )


def _service(tmp_path: Path) -> StorageService:
    return StorageService(
        control_catalog_config=ControlCatalogConfig(
            catalog_dir=tmp_path / "control",
        )
    )


async def _record_world(
    service: StorageService,
    storage: StorageConfig,
    *,
    world_id: str = "cold-world",
    run_id: str | None = "run-1",
    tick_head: int = 7,
    publish: bool = True,
) -> int:
    catalog = service.get_control_catalog(storage)
    await catalog.register_world(
        WorldRecord(
            world_id=world_id,
            name="cold",
            run_id=run_id,
            parent_world_id=None,
            status="active",
            tick_head=tick_head,
        )
    )
    epoch = await catalog.acquire_fence(world_id, "artifact-handler-test")
    if publish and run_id is not None:
        await catalog.publish_manifest(
            world_id,
            run_id,
            tick_head,
            f"published-{tick_head}",
            epoch,
            [],
        )
    return epoch


@pytest.mark.asyncio
async def test_cold_explicit_coordinates_use_published_tick_without_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from archetype.artifacts.handlers import ingest_artifacts, query_artifacts
    from archetype.artifacts.models import (
        ArtifactSource,
        ArtifactStoreConfig,
        IngestArtifacts,
        QueryArtifacts,
    )
    from archetype.world.registry import WorldRegistry

    async def registry_trap(*_args, **_kwargs):
        raise AssertionError("artifact handlers must not consult the live registry")

    monkeypatch.setattr(WorldRegistry, "storage_record", registry_trap)
    monkeypatch.setattr(WorldRegistry, "live_world", registry_trap)

    storage = _storage(tmp_path)
    service = _service(tmp_path)
    source = tmp_path / "evidence.txt"
    source.write_text("durable evidence", encoding="utf-8")
    try:
        epoch = await _record_world(service, storage, tick_head=7, publish=False)
        operation = IngestArtifacts(
            world_id="cold-world",
            sources=(ArtifactSource(source_uri=str(source)),),
            storage_config=storage,
        )
        with pytest.raises(RuntimeError, match="no published tick head"):
            await ingest_artifacts(
                service,
                operation,
                store_config=ArtifactStoreConfig.local(tmp_path / "objects"),
            )
        assert not (tmp_path / "objects").exists()

        await service.get_control_catalog(storage).publish_manifest(
            "cold-world",
            "run-1",
            7,
            "published-7",
            epoch,
            [],
        )
        (reference,) = await ingest_artifacts(
            service,
            operation,
            store_config=ArtifactStoreConfig.local(tmp_path / "objects"),
        )
        (repeated,) = await ingest_artifacts(
            service,
            operation,
            store_config=ArtifactStoreConfig.local(tmp_path / "objects"),
        )
        rows = await query_artifacts(
            service,
            QueryArtifacts(
                world_id="cold-world",
                storage_config=storage,
            ),
        )

        assert repeated.artifact_id != reference.artifact_id
        assert repeated.uri == reference.uri
        assert rows.select("artifact_id", "tick").sort("artifact_id").to_pylist() == [
            {"artifact_id": artifact_id, "tick": 7}
            for artifact_id in sorted((reference.artifact_id, repeated.artifact_id))
        ]
    finally:
        await service.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("record", "error", "match"),
    [
        (None, KeyError, "is not recorded"),
        (
            WorldRecord(
                world_id="cold-world",
                name="cold",
                run_id=None,
                parent_world_id=None,
                status="active",
                tick_head=0,
            ),
            RuntimeError,
            "has no recorded run",
        ),
    ],
)
async def test_missing_durable_coordinates_reject_before_file_or_index_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record: WorldRecord | None,
    error: type[Exception],
    match: str,
) -> None:
    import archetype.artifacts.handlers as handlers
    from archetype.artifacts.models import (
        ArtifactSource,
        ArtifactStoreConfig,
        IngestArtifacts,
    )

    storage = _storage(tmp_path)
    service = _service(tmp_path)
    source = tmp_path / "evidence.txt"
    source.write_text("must remain unread", encoding="utf-8")
    effects: list[str] = []

    class PipelineTrap:
        def __init__(self, **_kwargs) -> None:
            effects.append("pipeline")

    monkeypatch.setattr(handlers, "FileIngestionPipeline", PipelineTrap)
    try:
        if record is not None:
            await service.get_control_catalog(storage).register_world(record)
        with pytest.raises(error, match=match):
            await handlers.ingest_artifacts(
                service,
                IngestArtifacts(
                    world_id="cold-world",
                    sources=(ArtifactSource(source_uri=str(source)),),
                    storage_config=storage,
                ),
                store_config=ArtifactStoreConfig.local(tmp_path / "objects"),
            )
        assert effects == []
        assert not (tmp_path / "objects").exists()
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_one_discovery_and_one_persistence_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from archetype.artifacts.handlers import ingest_artifacts
    from archetype.artifacts.models import (
        ArtifactSource,
        ArtifactStoreConfig,
        IngestArtifacts,
    )
    from archetype.artifacts.pipeline import ARTIFACT_FILES, ARTIFACT_TEXT

    storage = _storage(tmp_path)
    service = _service(tmp_path)
    source = tmp_path / "evidence.txt"
    source.write_text("one graph per phase", encoding="utf-8")
    materializations: list[tuple[str, ...]] = []
    appends: list[tuple[str, tuple[str, ...]]] = []
    real_materialize = service.materialize
    real_append = service.append_world_rows

    async def observed_materialize(frame):
        materializations.append(tuple(frame.column_names))
        return await real_materialize(frame)

    async def observed_append(
        storage_config,
        world_id,
        table_name,
        rows,
        *,
        key_columns=(),
    ):
        appends.append((table_name, key_columns))
        return await real_append(
            storage_config,
            world_id,
            table_name,
            rows,
            key_columns=key_columns,
        )

    monkeypatch.setattr(service, "materialize", observed_materialize)
    monkeypatch.setattr(service, "append_world_rows", observed_append)
    try:
        await _record_world(service, storage)
        await ingest_artifacts(
            service,
            IngestArtifacts(
                world_id="cold-world",
                sources=(ArtifactSource(source_uri=str(source)),),
                storage_config=storage,
            ),
            store_config=ArtifactStoreConfig.local(tmp_path / "objects"),
        )
        assert len(materializations) == 2
        assert "artifact_id" in materializations[0]
        assert "object_uri" not in materializations[0]
        assert "object_uri" in materializations[1]
        assert appends == [
            (ARTIFACT_TEXT, ("artifact_id",)),
            (ARTIFACT_FILES, ("artifact_id",)),
        ]
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_typed_index_failure_never_publishes_common_visibility(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from archetype.artifacts.handlers import ingest_artifacts
    from archetype.artifacts.models import (
        ArtifactSource,
        ArtifactStoreConfig,
        IngestArtifacts,
    )
    from archetype.artifacts.pipeline import ARTIFACT_FILES, ARTIFACT_IMAGES

    storage = _storage(tmp_path)
    service = _service(tmp_path)
    source = tmp_path / "pixel.png"
    source.write_bytes(_PNG)
    appended: list[tuple[str, tuple[str, ...]]] = []
    real_append = service.append_world_rows

    async def fail_typed(
        storage_config,
        world_id,
        table_name,
        rows,
        *,
        key_columns=(),
    ):
        appended.append((table_name, key_columns))
        if table_name == ARTIFACT_IMAGES:
            await real_append(
                storage_config,
                world_id,
                table_name,
                rows,
                key_columns=key_columns,
            )
            raise RuntimeError("typed index unavailable")
        return await real_append(
            storage_config,
            world_id,
            table_name,
            rows,
            key_columns=key_columns,
        )

    monkeypatch.setattr(service, "append_world_rows", fail_typed)
    try:
        await _record_world(service, storage)
        with pytest.raises(RuntimeError, match="typed index unavailable"):
            await ingest_artifacts(
                service,
                IngestArtifacts(
                    world_id="cold-world",
                    sources=(ArtifactSource(source_uri=str(source)),),
                    storage_config=storage,
                ),
                store_config=ArtifactStoreConfig.local(tmp_path / "objects"),
            )

        store = await service.get_or_create_store(storage)
        catalog = store.session.current_catalog()
        assert appended == [(ARTIFACT_IMAGES, ("artifact_id",))]
        assert catalog.has_table(f"{storage.namespace}.{ARTIFACT_IMAGES}")
        typed_rows = await service.read_world_rows(
            storage,
            "cold-world",
            ARTIFACT_IMAGES,
        )
        assert typed_rows.select("artifact_id").count_rows() == 1
        # Artifact publication is deliberately not cross-table atomic. A
        # durable typed row may remain, but without the common visibility root
        # it is not an observable artifact occurrence.
        assert not catalog.has_table(f"{storage.namespace}.{ARTIFACT_FILES}")
    finally:
        await service.shutdown()
