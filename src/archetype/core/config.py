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

from enum import Enum
from pathlib import Path
from typing import Any

import uuid_utils as uuid
from daft.catalog import Catalog  # noqa: F401
from daft.io import IOConfig
from daft.session import Session  # noqa: F401
from pydantic import BaseModel, Field, field_validator
from uuid_utils import UUID


class StorageBackend(Enum):
    """
    Storage backend engine: 'iceberg' or 'lancedb'
    """

    ICEBERG = "iceberg"  # Iceberg backed by Parquet using SQLite PyIceberg SQL Catalog
    LANCEDB = "lancedb"


class StorageConfig(BaseModel):
    """
    Storage backend configuration for app/runtime storage resolution.

    Includes:
        - uri: str  - The URI location for the storage backend
        - namespace: str - The desired namespace for the catalog
        - io_config: IOConfig - Native Daft I/O configuration for storage reads/writes
    """

    uri: str | Path = Field(
        default="./archetype_db",
        description="The URI location for the storage backend (str or Path)",
    )
    namespace: str = Field(default="ecs")
    backend: StorageBackend = Field(
        default=StorageBackend.LANCEDB,
        description="Storage backend engine: 'iceberg' or 'lancedb (default)'",
    )
    io_config: IOConfig | None = Field(
        default=None,
        description="Native Daft I/O configuration passed to Iceberg storage read/write operations.",
    )
    model_config = dict(arbitrary_types_allowed=True)

    # Coerce Path to str for downstream components
    @field_validator("uri", mode="before")
    def _coerce_uri(cls, v):
        if isinstance(v, Path):
            return str(v)
        return v


class CacheConfig(BaseModel):
    """
    A cache configuration is a container for the cache configuration, including:
      - flush_rows: int - The number of rows to flush to the storage backend
      - flush_mb: int - The number of megabytes to flush to the storage backend
      - global_mb: int - The number of megabytes to use for the cache
      - idle_sec: int - The number of seconds to wait before flushing the cache
    """

    flush_rows: int = Field(
        default=1_000_000, description="The number of rows to flush to the storage backend"
    )
    flush_mb: int = Field(
        default=512, description="The number of megabytes to flush to the storage backend"
    )
    global_mb: int = Field(
        default=1024 * 1024 * 1024, description="The number of megabytes to use for the cache"
    )
    idle_sec: float = Field(
        default=30.0, description="The number of seconds to wait before flushing the cache"
    )

    model_config = dict(arbitrary_types_allowed=True)


class RunConfig(BaseModel):
    """
    A run represents the configuration of a sequence of world.steps, and configures the runtime options for the world.

    Carries configuration for the run, including:
      - run_id: UUID - The unique identifier for the run sequence, a uuid7
      - num_steps: int - The number of steps to execute in the run sequence
      - debug: bool - Whether or not to enable debug mode
      - validate: bool - Whether or not to enable validation mode

    TODO: Add ergonomic named constructors, e.g. RunConfig.dev(steps=1, debug=True)
          and RunConfig.benchmark(steps, explain=False) to reduce call-site verbosity.
    """

    run_id: UUID = Field(
        default_factory=uuid.uuid7,
        description="The unique identifier for the run sequence, a uuid7",
    )
    num_steps: int = Field(
        default=1, description="The number of steps to execute in the run sequence"
    )
    debug: bool = Field(default=False, description="Whether or not to enable debug mode")
    show_rows: int = Field(
        default=8,
        description="Max rows to display for DataFrame snapshots in debug panels (0 disables)",
    )
    metadata: dict[str, Any] | None = Field(
        default=None, description="Arbitrary metadata for experiment tracking"
    )

    model_config = dict(frozen=True, arbitrary_types_allowed=True)

    # Named constructors to reduce call-site verbosity for common scenarios
    @classmethod
    def dev(
        cls,
        *,
        steps: int = 1,
        debug: bool = True,
        show_rows: int = 5,
        metadata: dict[str, Any] | None = None,
    ) -> "RunConfig":
        return cls(
            num_steps=steps,
            debug=debug,
            show_rows=show_rows,
            metadata=metadata,
        )

    @classmethod
    def benchmark(
        cls,
        *,
        steps: int,
        debug: bool = False,
        show_rows: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> "RunConfig":
        return cls(
            num_steps=steps,
            debug=debug,
            show_rows=show_rows,
            metadata=metadata,
        )


class WorldConfig(BaseModel):
    """
    World identity and initial state.

    Carries the world's identity (id, name) and its mutable simulation state
    (tick counter, entity registry, mutation caches).  Passed to the world
    constructor so all state is explicit and serializable.
    """

    world_id: UUID | None = Field(
        default_factory=uuid.uuid7,
        description="Unique identifier for the world. Auto-generated if omitted.",
    )
    name: str | None = Field(default=None, description="Human-readable alias for the world")
    run_id: str | None = Field(default=None, description="Active run identifier")
    tick: int = Field(default=0, description="Current simulation tick")
    next_entity_id: int = Field(default=1, description="Next entity ID to assign")
    entity2sig: dict[int, tuple] = Field(
        default_factory=dict,
        description="Entity ID → archetype signature mapping",
    )
    spawn_cache: dict[tuple, list[dict]] = Field(
        default_factory=dict,
        description="Pending spawn rows keyed by archetype signature",
    )
    despawn_cache: dict[tuple, list[int]] = Field(
        default_factory=dict,
        description="Pending despawn entity IDs keyed by archetype signature",
    )

    model_config = dict(arbitrary_types_allowed=True)
