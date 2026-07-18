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
from typing import Annotated, Any

import uuid_utils as uuid
from daft.catalog import Catalog  # noqa: F401
from daft.io import IOConfig
from daft.session import Session  # noqa: F401
from pydantic import BaseModel, Field, PlainSerializer, WithJsonSchema, field_validator
from uuid_utils import UUID

JsonUUID = Annotated[
    UUID,
    PlainSerializer(lambda value: str(value), return_type=str, when_used="json"),
    WithJsonSchema({"type": "string", "format": "uuid"}),
]
JsonIOConfig = Annotated[
    IOConfig,
    WithJsonSchema({"type": "object", "additionalProperties": True}),
]


class StorageBackend(Enum):
    """Storage engine used for durable world data."""

    ICEBERG = "iceberg"  # Iceberg backed by Parquet using SQLite PyIceberg SQL Catalog
    LANCEDB = "lancedb"


class StorageConfig(BaseModel):
    """Select the location and backend for durable world data."""

    uri: str | Path = Field(
        default="./archetype_db",
        description="Local path or object-store URI for durable data.",
    )
    namespace: str = Field(default="ecs", description="Catalog namespace for world tables.")
    backend: StorageBackend = Field(
        default=StorageBackend.LANCEDB,
        description="Storage engine. Defaults to LanceDB.",
    )
    io_config: JsonIOConfig | None = Field(
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
    """Control when buffered world rows are flushed to durable storage."""

    flush_rows: int = Field(
        default=1_000_000, description="Flush a table after it accumulates this many rows."
    )
    flush_mb: int = Field(
        default=512, description="Flush a table after it reaches this size in megabytes."
    )
    global_mb: int = Field(default=1024, description="Maximum memory budget across cached tables.")
    idle_sec: float = Field(default=30.0, description="Flush a table after this many idle seconds.")

    model_config = dict(arbitrary_types_allowed=True)


class RunConfig(BaseModel):
    """Configure one bounded sequence of world ticks."""

    run_id: str | JsonUUID = Field(
        default_factory=uuid.uuid7,
        description="Stable identifier for this run, generated when omitted.",
    )
    num_steps: int = Field(default=1, ge=0, description="Number of ticks to execute.")
    debug: bool = Field(default=False, description="Emit per-tick diagnostic panels.")
    show_rows: int = Field(
        default=8,
        description="Maximum rows shown in each debug snapshot; zero disables snapshots.",
    )
    metadata: dict[str, Any] | None = Field(
        default=None, description="Optional metadata recorded with the run."
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
        """Create a run configuration with interactive diagnostics enabled."""
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
        """Create a quiet run configuration for performance measurement."""
        return cls(
            num_steps=steps,
            debug=debug,
            show_rows=show_rows,
            metadata=metadata,
        )


class WorldConfig(BaseModel):
    """Describe world identity and serializable engine state.

    Runtime users normally create worlds through `ArchetypeRuntime.world()`.
    Custom hosts use this model when constructing the lower-level engine.
    """

    world_id: str | JsonUUID | None = Field(
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
    lineage: list[tuple[str, str, int]] = Field(
        default_factory=list,
        description=(
            "Ancestor read segments as (world_id, run_id, up_to_tick), ascending. "
            "Reads for ticks <= up_to_tick resolve to that ancestor's rows; later "
            "ticks belong to this world's own run. Populated by fork."
        ),
    )

    model_config = dict(arbitrary_types_allowed=True)
