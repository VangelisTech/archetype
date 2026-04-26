# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Configuration coercion helpers for the runtime layer (R9)."""

from pathlib import Path

from archetype.core.config import CacheConfig, StorageConfig


def coerce_storage(value: str | Path | StorageConfig | None) -> StorageConfig:
    """Coerce a user-friendly storage value to StorageConfig."""
    if value is None:
        return StorageConfig()
    if isinstance(value, StorageConfig):
        return value
    return StorageConfig(uri=str(value))


def coerce_cache(value: CacheConfig | None) -> CacheConfig | None:
    """Pass-through for cache config. Exists for symmetry."""
    return value
