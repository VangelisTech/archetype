# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest

from archetype.app.storage_service import StorageService
from archetype.core.config import StorageBackend, StorageConfig
from archetype.ledger import StorageRefMismatchError, UnsupportedAtomicInsertError


def test_storage_ref_normalizes_path_spellings(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "durable data"
    relative = StorageService.storage_ref(StorageConfig(uri="durable data", namespace="test"))
    absolute = StorageService.storage_ref(StorageConfig(uri=root, namespace="test"))
    file_uri = StorageService.storage_ref(StorageConfig(uri=root.as_uri(), namespace="test"))

    assert relative == absolute == file_uri
    assert relative.data_uri == root.as_uri()
    assert relative.catalog_uri == (root / "test" / ".archetype" / "catalog-v1.sqlite3").as_uri()
    assert not root.exists(), "constructing a credential-free reference must be pure"


def test_storage_ref_is_portable_across_cwd(tmp_path: Path, monkeypatch) -> None:
    origin = tmp_path / "origin"
    elsewhere = tmp_path / "elsewhere"
    origin.mkdir()
    elsewhere.mkdir()
    monkeypatch.chdir(origin)
    created = StorageService.storage_ref(StorageConfig(uri="store", namespace="test"))

    monkeypatch.chdir(elsewhere)
    reopened = StorageService.storage_ref(
        StorageConfig(uri=created.data_uri, namespace=created.namespace)
    )
    assert reopened == created


def test_storage_identity_includes_namespace_and_catalog(tmp_path: Path) -> None:
    first = StorageService.storage_ref(StorageConfig(uri=tmp_path, namespace="first"))
    second = StorageService.storage_ref(StorageConfig(uri=tmp_path, namespace="second"))

    assert first.storage_id != second.storage_id
    assert set(first.model_dump()) == {
        "schema_version",
        "storage_id",
        "backend",
        "data_uri",
        "namespace",
        "catalog_uri",
    }
    assert "io_config" not in first.model_dump_json()


def test_storage_ref_verification_fails_closed(tmp_path: Path) -> None:
    reference = StorageService.storage_ref(StorageConfig(uri=tmp_path / "one", namespace="test"))
    with pytest.raises(StorageRefMismatchError):
        StorageService.verify_storage_ref(
            reference,
            StorageConfig(uri=tmp_path / "two", namespace="test"),
        )


@pytest.mark.asyncio
async def test_atomic_catalog_is_local_lancedb_only(tmp_path: Path) -> None:
    storage = StorageService()
    iceberg = StorageConfig(
        uri=tmp_path / "iceberg",
        namespace="test",
        backend=StorageBackend.ICEBERG,
    )
    with pytest.raises(UnsupportedAtomicInsertError):
        await storage.get_or_create_atomic_record_store(iceberg)

    remote = StorageConfig(uri="s3://bucket/db", namespace="test")
    with pytest.raises(UnsupportedAtomicInsertError):
        await storage.get_or_create_atomic_record_store(remote)


@pytest.mark.asyncio
async def test_catalog_creation_does_not_create_component_tables(tmp_path: Path) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    storage = StorageService()
    first = await storage.get_or_create_atomic_record_store(config)
    second = await storage.get_or_create_atomic_record_store(config)

    assert first is second
    assert first.database_path.exists()
    assert not (tmp_path / "db" / "test" / "lance").exists()

    await storage.shutdown()
    await storage.shutdown()


@pytest.mark.asyncio
async def test_read_existing_pool_follows_canonical_cwd_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    first_cwd = tmp_path / "first"
    second_cwd = tmp_path / "second"
    first_cwd.mkdir()
    second_cwd.mkdir()
    config = StorageConfig(uri="store", namespace="test")
    storage = StorageService()

    monkeypatch.chdir(first_cwd)
    first_ref = storage.storage_ref(config)
    first_store = await storage.get_read_existing_store(config)
    monkeypatch.chdir(second_cwd)
    second_ref = storage.storage_ref(config)
    second_store = await storage.get_read_existing_store(config)

    assert first_ref != second_ref
    assert first_store is not second_store
    assert Path(first_store.uri) == first_cwd / "store"
    assert Path(second_store.uri) == second_cwd / "store"
    await storage.shutdown()


@pytest.mark.parametrize("namespace", ["a/", "./a", "a/.", "a\\child"])
def test_local_namespace_requires_one_canonical_segment(
    tmp_path: Path,
    namespace: str,
) -> None:
    with pytest.raises(ValueError, match="canonical local path segment"):
        StorageService.storage_ref(StorageConfig(uri=tmp_path, namespace=namespace))


@pytest.mark.parametrize("uri", ["file:relative", "file://"])
def test_local_file_uri_requires_absolute_nonempty_path(uri: str) -> None:
    with pytest.raises(ValueError, match="absolute path"):
        StorageService.storage_ref(StorageConfig(uri=uri, namespace="test"))


def test_durable_profile_rejects_mutable_lancedb_subdir(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ARCT_LANCEDB_SUBDIR", "other")
    with pytest.raises(UnsupportedAtomicInsertError, match="canonical LanceDB subdirectory"):
        StorageService.storage_ref(StorageConfig(uri=tmp_path, namespace="test"))
