# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical process-owner construction for repository tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from uuid_utils import uuid7

from archetype.runtime_resources import RuntimeResources
from archetype.storage.config import ControlCatalogConfig
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources


def build_test_runtime(
    tmp_path: Path,
    **overrides: Any,
) -> RuntimeResources:
    """Build one isolated resource owner without recreating an app facade."""

    config = RuntimeBootstrapConfig(
        control_catalog_config=ControlCatalogConfig(
            catalog_dir=tmp_path / f"control-{uuid7()}",
        ),
        **overrides,
    )
    return build_runtime_resources(config)


__all__ = ["build_test_runtime"]
