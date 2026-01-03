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

"""
Archetype - AI-Native Simulation Engine

A data-centric Entity-Component-System (ECS) runtime for agent simulations.

Architecture (see assets/archetype_diagram.png):

    ┌─────────┐         ┌─────────┐         ┌─────────┐
    │ System  │──runs──▶│  World  │◀───────▶│  Store  │──append──▶ LanceDB
    └─────────┘         │ step()  │         │archetypes│
         │              └─────────┘         └─────────┘
         │ priority          │                   ▲
         ▼                   ▼                   │
    ┌─────────┐         ┌─────────┐         ┌─────────┐
    │Processor│         │ Querier │─query──▶│         │
    │Processor│         │ Updater │─update─▶│         │
    │   ...   │         └─────────┘         └─────────┘
    └─────────┘              │
                          daft df

Quick Start:
    >>> import archetype
    >>> sim = archetype.init("./data")
    >>> world = sim.create_world("demo")
    >>> entity = world.create_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
    >>> world.add_processor(MovementProcessor())
    >>> world.step()
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from archetype.core.component import Component
    from archetype.core.sync.processor import SyncProcessor
    from archetype.core.sync.world import SyncWorld

__version__ = "0.1.0"

__all__ = [
    # Core types
    "Component",
    "Archetype",
    "ArchetypeSignature",
    "Resources",
    # Processor (with alias)
    "Processor",
    "SyncProcessor",
    "AsyncProcessor",
    # World (with alias)
    "World",
    "SyncWorld",
    "AsyncWorld",
    # System (with alias)
    "System",
    "SyncSystem",
    "AsyncSystem",
    # Store (with alias)
    "Store",
    "SyncStore",
    "AsyncStore",
    "AsyncCachedStore",
    "AsyncLancedbStore",
    # Querier/Updater (with aliases)
    "Querier",
    "Updater",
    "QueryManager",
    "UpdateManager",
    "AsyncQueryManager",
    "AsyncUpdateManager",
    # Orchestration (from app layer)
    "WorldOrchestrator",
    "WorldFactory",
    "StorageBackendManager",
    "StorageResourceManager",  # Backwards compat alias
    "ArchetypeApp",
    # Config
    "StorageConfig",
    "CacheConfig",
    "RunConfig",
    "WorldConfig",
    # Managed Storage
    "ManagedStorage",
    "SyncManagedStorage",
    "CatalogEntry",
    # Factory
    "Simulation",
    "init",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    # Core types
    "Component": ("archetype.core", "Component"),
    "Archetype": ("archetype.core", "Archetype"),
    "ArchetypeSignature": ("archetype.core", "ArchetypeSignature"),
    "Resources": ("archetype.core", "Resources"),
    # Processors (and aliases)
    "SyncProcessor": ("archetype.core", "SyncProcessor"),
    "AsyncProcessor": ("archetype.core", "AsyncProcessor"),
    "Processor": ("archetype.core", "SyncProcessor"),
    # Worlds (and aliases)
    "SyncWorld": ("archetype.core", "SyncWorld"),
    "AsyncWorld": ("archetype.core", "AsyncWorld"),
    "World": ("archetype.core", "SyncWorld"),
    # Systems (and aliases)
    "SyncSystem": ("archetype.core", "SyncSystem"),
    "AsyncSystem": ("archetype.core", "AsyncSystem"),
    "System": ("archetype.core", "SyncSystem"),
    # Stores (and aliases)
    "SyncStore": ("archetype.core", "SyncStore"),
    "AsyncStore": ("archetype.core", "AsyncStore"),
    "AsyncCachedStore": ("archetype.core", "AsyncCachedStore"),
    "AsyncLancedbStore": ("archetype.core", "AsyncLancedbStore"),
    "Store": ("archetype.core", "SyncStore"),
    # Query/update managers (and aliases)
    "QueryManager": ("archetype.core", "QueryManager"),
    "UpdateManager": ("archetype.core", "UpdateManager"),
    "AsyncQueryManager": ("archetype.core", "AsyncQueryManager"),
    "AsyncUpdateManager": ("archetype.core", "AsyncUpdateManager"),
    "Querier": ("archetype.core", "QueryManager"),
    "Updater": ("archetype.core", "UpdateManager"),
    # Config
    "StorageConfig": ("archetype.core", "StorageConfig"),
    "CacheConfig": ("archetype.core", "CacheConfig"),
    "RunConfig": ("archetype.core", "RunConfig"),
    "WorldConfig": ("archetype.core", "WorldConfig"),
    # Managed storage
    "ManagedStorage": ("archetype.core", "ManagedStorage"),
    "SyncManagedStorage": ("archetype.core", "SyncManagedStorage"),
    "CatalogEntry": ("archetype.core", "CatalogEntry"),
    # Orchestration (app layer)
    "WorldOrchestrator": ("archetype.app", "WorldOrchestrator"),
    "WorldFactory": ("archetype.app", "WorldFactory"),
    "StorageBackendManager": ("archetype.app", "StorageBackendManager"),
    "StorageResourceManager": ("archetype.app", "StorageResourceManager"),  # Backwards compat
    "ArchetypeApp": ("archetype.app", "ArchetypeApp"),
}


def __getattr__(name: str) -> Any:
    """
    Lazy public API loader.

    Archetype has heavy (and sometimes optional) dependencies. Avoid importing
    the entire world at `import archetype` time so small utilities can run in
    minimal environments.
    """

    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
    module_name, attr_name = target
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(_EXPORTS.keys())))


class Simulation:
    """
    Simple synchronous simulation environment.

    This is a lightweight wrapper matching the diagram flow:
    System → World → Querier/Updater → Store → LanceDB

    For async/distributed workloads, use ArchetypeApp instead.
    """

    def __init__(
        self,
        uri: str,
        namespace: str = "archetypes",
        debug: bool = False,
    ):
        from pathlib import Path

        from archetype.core import StorageConfig
        from archetype.core.runtime.storage import StorageContextFactory

        self.debug = debug

        # Resolve to absolute path
        path = Path(uri).resolve()

        # Build storage context using the existing factory
        storage_config = StorageConfig(uri=str(path), namespace=namespace)
        self._context = StorageContextFactory.build(storage_config)
        self.uri = Path(self._context.uri)
        self.namespace = self._context.namespace

        # World registry
        self._worlds: dict[str, SyncWorld] = {}

    def spawn_world(
        self,
        name: str = "default",
        *,
        is_async: bool = False,
        **kwargs,
    ) -> str:
        """
        Create a new simulation world.

        Args:
            name: Human-readable name for the world
            is_async: Use async world (default False for sync)

        Returns:
            world_id string
        """
        from uuid_utils import uuid7

        from archetype.core import (
            QueryManager,
            SyncStore,
            SyncSystem,
            SyncWorld,
            UpdateManager,
            WorldConfig,
        )

        world_uuid = uuid7()
        world_id_str = str(world_uuid)

        # Create store, querier, updater, system
        store = SyncStore(uri=str(self.uri), session=self._context.session, debug=self.debug)
        querier = QueryManager(store=store, debug=self.debug)
        updater = UpdateManager(store=store)
        system = SyncSystem()

        config = WorldConfig(name=name, world_id=world_uuid)
        world = SyncWorld(
            world_config=config,
            querier=querier,
            updater=updater,
            system=system,
        )

        self._worlds[world_id_str] = world
        return world_id_str

    def get_world(self, world_id: str) -> SyncWorld:
        """Get a world by ID."""
        return self._worlds[world_id]

    def add_processor_to_world(self, world_id: str, processor: SyncProcessor) -> None:
        """Add a processor to a world's system."""
        self._worlds[world_id].add_processor(processor)

    def spawn_entity(self, world_id: str, *components: Component) -> int:
        """Create an entity with the given components in the specified world."""
        return self._worlds[world_id].create_entity(list(components))

    def step_world(self, world_id: str, **kwargs) -> None:
        """Execute one simulation step for the specified world."""
        from archetype.core import RunConfig

        run_config = RunConfig(num_steps=1)
        self._worlds[world_id].step(run_config, **kwargs)


def init(
    uri: str = "./archetype_data",
    *,
    namespace: str = "archetypes",
    debug: bool = False,
) -> Simulation:
    """
    Initialize an Archetype simulation environment.

    This is the recommended entry point for synchronous simulations.
    For async/distributed workloads, use ArchetypeApp.create() instead.

    Args:
        uri: Path to the data directory (created if not exists)
        namespace: Storage namespace for archetype tables
        debug: Enable debug logging and query tracing

    Returns:
        Simulation instance for creating and managing worlds

    Example:
        >>> import archetype
        >>> from archetype import Component, Processor
        >>> from daft import DataFrame, col
        >>>
        >>> class Position(Component):
        ...     x: float
        ...     y: float
        >>>
        >>> class Velocity(Component):
        ...     vx: float
        ...     vy: float
        >>>
        >>> class MovementProcessor(Processor):
        ...     components = (Position, Velocity)
        ...     priority = 1
        ...     def process(self, df: DataFrame, dt: float = 0.1) -> DataFrame:
        ...         return df.with_columns({
        ...             "position__x": col("position__x") + col("velocity__vx") * dt,
        ...             "position__y": col("position__y") + col("velocity__vy") * dt,
        ...         })
        >>>
        >>> sim = archetype.init("./data")
        >>> world_id = sim.spawn_world("physics")
        >>> sim.add_processor_to_world(world_id, MovementProcessor())
        >>> sim.spawn_entity(world_id, Position(x=0, y=0), Velocity(vx=1, vy=1))
        >>> sim.step_world(world_id, dt=0.1)
    """
    return Simulation(uri=uri, namespace=namespace, debug=debug)
