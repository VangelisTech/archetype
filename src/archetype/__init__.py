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

# Core ECS primitives
# Application layer (orchestration, resource management)
from archetype.app import (
    ArchetypeApp,
    StorageResourceManager,
    WorldFactory,
    WorldOrchestrator,
)
from archetype.core import (
    Archetype,
    ArchetypeSignature,
    AsyncCachedStore,
    AsyncLancedbStore,
    AsyncProcessor,
    AsyncQueryManager,
    AsyncStore,
    AsyncSystem,
    AsyncUpdateManager,
    AsyncWorld,
    CacheConfig,
    # Core types
    Component,
    QueryManager,
    RunConfig,
    # Config
    StorageConfig,
    # Processors
    SyncProcessor,
    # Storage & Query
    SyncStore,
    # Systems
    SyncSystem,
    # Worlds
    SyncWorld,
    UpdateManager,
    WorldConfig,
)

# Convenience aliases matching the diagram
Processor = SyncProcessor  # Default to sync for simplicity
World = SyncWorld
System = SyncSystem
Store = SyncStore
Querier = QueryManager
Updater = UpdateManager

__version__ = "0.1.0"

__all__ = [
    # Core types
    "Component",
    "Archetype",
    "ArchetypeSignature",
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
    "StorageResourceManager",
    "ArchetypeApp",
    # Config
    "StorageConfig",
    "CacheConfig",
    "RunConfig",
    "WorldConfig",
    # Factory
    "Simulation",
    "init",
]


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
