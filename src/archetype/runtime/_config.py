# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Configuration coercion helpers for the runtime layer (R9)."""

from pathlib import Path

from archetype.core.config import CacheConfig, StorageConfig


def coerce_storage(value: str | Path | StorageConfig | None) -> StorageConfig | None:
    """Coerce a user-friendly storage value to StorageConfig.

    None passes through unchanged: the service layer owns the default, and
    fork_world inherits the source world's storage when no override is given
    (world-lifecycle.md § 4.5). Manufacturing a default here would defeat
    that inheritance and strand a fork on a store its source never wrote to.
    """
    if value is None:
        return None
    if isinstance(value, StorageConfig):
        return value
    return StorageConfig(uri=str(value))


def coerce_cache(value: CacheConfig | None) -> CacheConfig | None:
    """Pass-through for cache config. Exists for symmetry."""
    return value
