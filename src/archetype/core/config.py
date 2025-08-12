from typing import List, Dict, Any, Optional
from daft.io import IOConfig
from daft.catalog import Catalog
from pydantic import BaseModel, Field
from uuid_utils import UUID
import uuid_utils as uuid
from daft.session import Session

class StorageConfig(BaseModel):
    """
    Storage Backend Context for configuring local or cloud storage access
    
    Includes: 
        - uri: str  - The URI location for the storage backend
        - namespace: str - The desired namespace for the catalog 
        - io_config: IOConfig - The access credentials or the daft session/catalog
    """
    uri: str = Field(default=".archetype_data/", description="The URI location for the storage backend")
    namespace: str = Field(default="archetypes")
    obs_namespace: str = Field(default="obs", description="Namespace for observability tables (logs/metrics/events)")
    is_async: bool = Field(default=True, description="Whether or not the storage backend is asynchronous") 
    use_lancedb: bool = Field(default=True, description="Whether or not to use lancedb as the storage backend")
    backend: str = Field(default="iceberg", description="Storage backend engine: 'iceberg' or 'lancedb'")
    io_config: Optional[IOConfig] = Field(default=None, description="Configuration for the native I/O layer, e.g. credentials for accessing cloud storage systems.") 
    catalog: Optional[Catalog] = Field(default=None, description="The catalog for the storage backend, feel free to pass in your own, defaults to instatiating a new pyiceberg sql lite catalog if none is provided")
    has_cache: bool = Field(default=False, description="Whether or not the storage backend is supported by a cache")
    session: Optional[Session] = Field(default=None, description="The session for the storage backend, defaults to a new session if none is provided")
    # Unity Catalog configuration (simplified)
    uc_mode: str = Field(default="off", description="'off' | 'governance' | 'attach'")
    uc_endpoint: Optional[str] = Field(default=None, description="Unity Catalog endpoint, e.g. http://localhost:8080/api/2.1/unity-catalog")
    uc_catalog: Optional[str] = Field(default=None, description="Unity Catalog catalog name")
    uc_schema: Optional[str] = Field(default=None, description="Unity Catalog schema name")
    token_source: str = Field(default="request", description="'request' | 'static' – where to source the UC token")
    uc_token: Optional[str] = Field(default=None, description="Fallback/static UC access token when token_source='static' or no request token present")
    # Optional: S3 region for temp creds; ignored for GCS/Azure
    uc_default_region: Optional[str] = Field(default=None, description="Default cloud region for temporary S3 credentials")
    
    model_config = dict(arbitrary_types_allowed=True)

    # Side-effect free: no implicit catalog/session creation here. Use StorageSessionFactory to build runtime context.
    def set_session(self, session: Session) -> None:
        self.session = session

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
    run_id: UUID     = Field(default_factory=uuid.uuid7, description="The unique identifier for the run sequence, a uuid7")
    num_steps: int   = Field(default=1, description="The number of steps to execute in the run sequence")
    debug: bool      = Field(default=False, description="Whether or not to enable debug mode")
    enable_validation: bool   = Field(default=False, description="Whether or not to enable validation mode")
    show_rows: int      = Field(default=3, description="Max rows to display for DataFrame snapshots in debug panels (0 disables)")
    explain: bool     = Field(default=False, description="Whether to render DataFrame logical plans in debug panels")
    prefer_live_reads: bool = Field(default=False, description="When True, step uses in-memory live snapshot for previous-state reads instead of querying the store")
    suite: Optional[str] = Field(default=None, description="Optional suite/experiment label for grouping runs (e.g., benchmarks, ensembles)")
    trial: Optional[int] = Field(default=None, description="Optional trial index for ensemble/grid runs")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Arbitrary metadata for experiment tracking")

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
        metadata: Optional[Dict[str, Any]] = None,
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
        metadata: Optional[Dict[str, Any]] = None,
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
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "RunConfig":
        return cls(
            num_steps=steps,
            enable_validation=enable_validation,
            debug=debug,
            prefer_live_reads=prefer_live_reads,
            metadata=metadata,
            suite="validate",
        )

class CacheConfig(BaseModel):
    """
    A cache configuration is a container for the cache configuration, including:
      - flush_rows: int - The number of rows to flush to the storage backend
      - flush_mb: int - The number of megabytes to flush to the storage backend
      - global_mb: int - The number of megabytes to use for the cache
      - idle_sec: int - The number of seconds to wait before flushing the cache
    """
    flush_rows: int = Field(default=1_000_000, description="The number of rows to flush to the storage backend")
    flush_mb: int = Field(default=512, description="The number of megabytes to flush to the storage backend")
    global_mb: int = Field(default=None, description="The number of megabytes to use for the cache")
    idle_sec: int = Field(default=30, description="The number of seconds to wait before flushing the cache")

    model_config = dict(arbitrary_types_allowed=True)


class WorldConfig(BaseModel):
    """
    A world configuration is a container for the world configuration, including:
      - world_id: Optional[UUID] - The unique identifier for the world. If not provided, a new one will be generated.
      - name: Optional[str] - A human-readable alias for the world.
    """
    world_id: Optional[UUID] = Field(default_factory=uuid.uuid7, description="The unique identifier for the world. If not provided, a new one will be generated.")
    name: Optional[str] = Field(default=None, description="A human-readable alias for the world")

    model_config = dict(arbitrary_types_allowed=True)


class SimulationConfig(BaseModel):
    """
    A simulation configuration is a container for the simulation configuration, including:
      - run_configs: List[RunConfig] - The configuration for the run sequence
      - cache_config: CacheConfig - The configuration for the cache
      - storage_config: StorageConfig - The configuration for the storage backend
      - world_configs: List[WorldConfig] - The configuration for the world
    """
    run_config: RunConfig = Field(default_factory=RunConfig, description="The configuration for the run sequence")
    cache_config: CacheConfig = Field(default_factory=CacheConfig, description="The configuration for the cache")
    storage_config: StorageConfig = Field(default_factory=StorageConfig, description="The configuration for the storage backend")
    world_configs: List[WorldConfig] = Field(default_factory=list, description="The configuration for the world")

    model_config = dict(arbitrary_types_allowed=True)


class ArchetypeConfig(BaseModel):
    """Top-level application configuration."""
    storage: StorageConfig = Field(default_factory=StorageConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    worlds: List[WorldConfig] = Field(default_factory=list)

    model_config = dict(arbitrary_types_allowed=True)

    @classmethod
    def from_toml(cls, path: str) -> "ArchetypeConfig":
        import tomllib as toml  # Python 3.11+
        with open(path, "r") as f:
            data = toml.load(f)
        return cls(**data)
