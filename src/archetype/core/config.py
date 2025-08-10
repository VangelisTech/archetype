from typing import List, Dict, Any, Optional
from daft.io import IOConfig
from daft.catalog import Catalog
from pydantic import BaseModel, Field, model_validator
from uuid_utils import UUID
import uuid_utils as uuid
from pyiceberg.catalog.sql import SqlCatalog
import os
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
    is_async: bool = Field(default=True, description="Whether or not the storage backend is asynchronous") 
    use_lancedb: bool = Field(default=True, description="Whether or not to use lancedb as the storage backend")
    io_config: Optional[IOConfig] = Field(default=None, description="Configuration for the native I/O layer, e.g. credentials for accessing cloud storage systems.") 
    catalog: Optional[Catalog] = Field(default=None, description="The catalog for the storage backend, feel free to pass in your own, defaults to instatiating a new pyiceberg sql lite catalog if none is provided")
    has_cache: bool = Field(default=False, description="Whether or not the storage backend is supported by a cache")
    session: Optional[Session] = Field(default=None, description="The session for the storage backend, defaults to a new session if none is provided")
    
    model_config = dict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def setup_catalog(self) -> "StorageConfig":
        if self.catalog is None:
            # Ensure uri exists
            if not os.path.exists(self.uri):
                os.makedirs(self.uri)

            self.catalog = Catalog.from_iceberg(
                SqlCatalog(
                    "archetype_iceberg_sql_catalog",
                    **{
                        "uri": f"sqlite:///{self.uri}/catalog.db",
                        "warehouse": f"file://{self.uri}",
                    },
                )
            )
        return self
    
    def set_session(self, session: Session) -> None:
        self.session = session

    def get_session(self) -> Session:
        if self.session is None:
            self.session = Session()
            self.session.attach_catalog(self.catalog)
            self.session.create_namespace_if_not_exists(self.namespace)
            self.session.set_namespace(self.namespace)
        return self.session

class RunConfig(BaseModel):
    """
    A run represents the configuration of a sequence of world.steps, and configures the runtime options for the world. 

    Carries configuration for the run, including:
      - run_id: UUID - The unique identifier for the run sequence, a uuid7
      - num_steps: int - The number of steps to execute in the run sequence
      - debug: bool - Whether or not to enable debug mode
      - validate: bool - Whether or not to enable validation mode
    """
    run_id: UUID     = Field(default_factory=uuid.uuid7, description="The unique identifier for the run sequence, a uuid7")
    num_steps: int   = Field(default=1, description="The number of steps to execute in the run sequence")
    debug: bool      = Field(default=False, description="Whether or not to enable debug mode")
    enable_validation: bool   = Field(default=False, description="Whether or not to enable validation mode")
    show_rows: int      = Field(default=3, description="Max rows to display for DataFrame snapshots in debug panels (0 disables)")
    explain: bool     = Field(default=False, description="Whether to render DataFrame logical plans in debug panels")

    model_config = dict(frozen=True, arbitrary_types_allowed=True)

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
        import toml
        with open(path, "r") as f:
            data = toml.load(f)
        return cls(**data)
