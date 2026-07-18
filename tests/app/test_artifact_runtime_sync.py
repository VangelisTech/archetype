# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Synchronous runtime parity for typed artifact tables."""

import hashlib

import daft

from archetype import ArchetypeRuntime
from archetype.core.config import StorageBackend, StorageConfig


@daft.func(return_dtype=daft.DataType.string())
def _read_text(file: daft.File) -> str:
    with file.open() as stream:
        return stream.read().decode("utf-8")


class TextFacts:
    table_name = "documents"

    def process(self, files):
        return files.with_column("text", _read_text(daft.col("file")))


def test_sync_runtime_matches_file_pipeline_and_read_surfaces(tmp_path):
    source = tmp_path / "source.txt"
    source.write_text("sync")
    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="ns",
        backend=StorageBackend.ICEBERG,
    )

    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world("sync-artifacts", storage=storage)
        assert world.ingest_files(source, TextFacts()).rows_written == 1

        direct = daft.from_pydict(
            {
                "source_uri": ["sensor://sync/1"],
                "content_hash": [hashlib.sha256(b"sync-1").hexdigest()],
                "value": [1],
            }
        )
        assert world.write_artifacts("readings", direct).rows_written == 1
        assert world.artifacts("documents").to_pylist()[0]["text"] == "sync"
