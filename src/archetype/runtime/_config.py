# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
