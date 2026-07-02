# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Smoke tests for examples/09_cloud_storage.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from archetype.core.config import StorageBackend, StorageConfig

_EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "09_cloud_storage.py"


def _load_example():
    spec = importlib.util.spec_from_file_location("cloud_storage_example", _EXAMPLE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_provider_examples_build_storage_configs() -> None:
    module = _load_example()

    examples = module.provider_examples()

    assert len(examples) == 7
    assert {example.name for example in examples} == {
        "AWS S3",
        "Google Cloud Storage",
        "Azure Blob / ADLS",
        "Cloudflare R2",
        "MinIO",
        "Tencent COS",
        "Volcengine TOS",
    }
    assert all(isinstance(example.storage, StorageConfig) for example in examples)
    assert all(example.storage.backend == StorageBackend.ICEBERG for example in examples)
    assert all(example.storage.io_config is not None for example in examples)


@pytest.mark.asyncio
async def test_local_smoke_uses_runtime_storage_api(capsys) -> None:
    module = _load_example()

    await module._smoke_local()
    out = capsys.readouterr().out

    assert "Local smoke" in out
    assert "spawned_entities=1" in out
    assert "provider=local" in out
