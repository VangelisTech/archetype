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


class UnityCatalogConfig(BaseModel):
    """
    Unity Catalog configuration. Optional; when provided enables UC-aware behavior.
    """

    enabled: bool = Field(default=False, description="Enable Unity Catalog awareness")
    enforce_permissions: bool = Field(
        default=False, description="Wrap query/update with UC permission enforcement"
    )
    endpoint: str | None = Field(default=None, description="UC REST endpoint base URL")
    token: str | None = Field(
        default=None, description="Bearer token to access UC if not using user tokens"
    )
    uc_catalog: str | None = Field(default=None, description="Default UC catalog to operate in")
    uc_schema: str | None = Field(default=None, description="Default UC schema to operate in")
    default_region: str | None = Field(
        default=None, description="Default region for temporary credentials"
    )


class StorageConfig(BaseModel):
    """
    Storage Backend Context for configuring local or cloud storage access

    Includes:
        - uri: str  - The URI location for the storage backend
        - namespace: str - The desired namespace for the catalog
        - io_config: IOConfig - The access credentials or the daft session/catalog
    """

    uri: str | Path = Field(
        default="./archetype_data",
        description="The URI location for the storage backend (str or Path)",
    )
    namespace: str = Field(default="archetypes")
    backend: StorageBackend = Field(
        default=StorageBackend.ICEBERG,
        description="Storage backend engine: 'iceberg (default)' or 'lancedb'",
    )
    io_config: IOConfig | None = Field(
        default=None,
        description="Configuration for the native I/O layer, e.g. credentials for accessing cloud storage systems.",
    )
    uc_config: UnityCatalogConfig | None = Field(
        default=None,
        description="Optional Unity Catalog configuration. When provided, enables UC integration.",
    )

    model_config = dict(arbitrary_types_allowed=True)

    # Coerce Path to str for downstream components
    @field_validator("uri", mode="before")
    def _coerce_uri(cls, v):
        if isinstance(v, Path):
            return str(v)
        return v

    # Back-compat helpers for legacy callers expecting these flags
    @property
    def is_async(self) -> bool:
        return True

    @property
    def use_lancedb(self) -> bool:
        return self.backend == StorageBackend.LANCEDB


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

    TODO: Add ergonomic named constructors, e.g. RunConfig.dev(steps=1, debug=True, prefer_live_reads=True)
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
    enable_validation: bool = Field(
        default=False, description="Whether or not to enable validation mode"
    )
    show_rows: int = Field(
        default=3,
        description="Max rows to display for DataFrame snapshots in debug panels (0 disables)",
    )
    explain: bool = Field(
        default=False, description="Whether to render DataFrame logical plans in debug panels"
    )
    prefer_live_reads: bool = Field(
        default=False,
        description="When True, step uses in-memory live snapshot for previous-state reads instead of querying the store",
    )
    suite: str | None = Field(
        default=None,
        description="Optional suite/experiment label for grouping runs (e.g., benchmarks, ensembles)",
    )
    trial: int | None = Field(
        default=None, description="Optional trial index for ensemble/grid runs"
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
        prefer_live_reads: bool = True,
        debug: bool = True,
        explain: bool = False,
        show_rows: int = 5,
        metadata: dict[str, Any] | None = None,
    ) -> "RunConfig":
        return cls(
            num_steps=steps,
            prefer_live_reads=prefer_live_reads,
            debug=debug,
            explain=explain,
            show_rows=show_rows,
            metadata=metadata,
        )

    @classmethod
    def benchmark(
        cls,
        *,
        steps: int,
        explain: bool = False,
        debug: bool = False,
        prefer_live_reads: bool = False,
        show_rows: int = 0,
        suite: str | None = "benchmark",
        trial: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "RunConfig":
        return cls(
            num_steps=steps,
            explain=explain,
            debug=debug,
            prefer_live_reads=prefer_live_reads,
            show_rows=show_rows,
            suite=suite,
            trial=trial,
            metadata=metadata,
        )

    @classmethod
    def validate(
        cls,
        *,
        steps: int = 1,
        enable_validation: bool = True,
        debug: bool = True,
        prefer_live_reads: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> "RunConfig":
        return cls(
            num_steps=steps,
            enable_validation=enable_validation,
            debug=debug,
            prefer_live_reads=prefer_live_reads,
            metadata=metadata,
            suite="validate",
        )


class WorldConfig(BaseModel):
    """
    A world configuration is a container for the world configuration, including:
      - world_id: Optional[UUID] - The unique identifier for the world. If not provided, a new one will be generated.
      - name: Optional[str] - A human-readable alias for the world.
    """

    world_id: UUID | None = Field(
        default_factory=uuid.uuid7,
        description="The unique identifier for the world. If not provided, a new one will be generated.",
    )
    name: str | None = Field(default=None, description="A human-readable alias for the world")

    model_config = dict(arbitrary_types_allowed=True)


class SimulationConfig(BaseModel):
    """
    A simulation configuration is a container for the simulation configuration, including:
      - run_configs: List[RunConfig] - The configuration for the run sequence
      - cache_config: CacheConfig - The configuration for the cache
      - storage_config: StorageConfig - The configuration for the storage backend
      - world_configs: List[WorldConfig] - The configuration for the world
    """

    run_config: RunConfig = Field(
        default_factory=RunConfig, description="The configuration for the run sequence"
    )
    cache_config: CacheConfig = Field(
        default_factory=CacheConfig, description="The configuration for the cache"
    )
    storage_config: StorageConfig = Field(
        default_factory=StorageConfig, description="The configuration for the storage backend"
    )
    world_configs: list[WorldConfig] = Field(
        default_factory=list, description="The configuration for the world"
    )

    model_config = dict(arbitrary_types_allowed=True)


class ArchetypeConfig(BaseModel):
    """Top-level application configuration."""

    storage: StorageConfig = Field(default_factory=StorageConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    worlds: list[WorldConfig] = Field(default_factory=list)

    model_config = dict(arbitrary_types_allowed=True)

    @classmethod
    def from_toml(cls, path: str) -> "ArchetypeConfig":
        import tomllib as toml  # type: ignore[import-not-found]  # Python 3.11+

        with open(path) as f:
            data = toml.load(f)
        return cls(**data)
