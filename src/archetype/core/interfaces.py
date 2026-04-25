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


from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from daft import DataFrame  # type: ignore[import-not-found]

from .component import Component
from .config import RunConfig

if TYPE_CHECKING:
    from archetype.core.component import Component
    from archetype.core.hooks import (
        AsyncHookHandler,
        FireMode,
        HookEvent,
        HookHandle,
        SyncHookHandler,
    )

ArchetypeSignature = tuple[type["Component"], ...]


# ═══════════════════════════════════════════════════════════════════════════════
# SYNCHRONOUS INTERFACES
# ═══════════════════════════════════════════════════════════════════════════════


class iProcessor(Protocol):
    """
    Transforms entity data for a specific archetype.

    Processors are the behavioral building blocks of a simulation.
    Each processor declares which Components it operates on and a priority
    for execution ordering within the System.

    The process() method receives a Daft DataFrame containing all entities
    matching the archetype signature and returns a transformed DataFrame.
    """

    components: tuple[type[Component], ...] = None
    priority: int = 0

    def process(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        """Transform entity data. Pure function: df in → df out."""
        ...


class iSystem(Protocol):
    """
    Orchestrates Processor execution in priority order.

    The System manages a collection of Processors and executes them
    on matching archetypes during each simulation step. Lower priority
    values execute first.
    """

    def add_processor(self, processor: iProcessor) -> None:
        """Register a processor with the system."""
        ...

    def remove_processor(self, processor: iProcessor) -> None:
        """Unregister a processor from the system."""
        ...

    def execute(self, df: DataFrame, sig: ArchetypeSignature, *args, **kwargs) -> DataFrame:
        """Execute all matching processors on the archetype DataFrame."""
        ...


class iStore(Protocol):
    """
    Persistent storage backend for archetype tables.

    The Store manages physical storage of entity data, typically backed
    by LanceDB. It provides append-only semantics for time-travel queries
    and efficient columnar storage.
    """

    def get_archetype_df(self, sig: ArchetypeSignature) -> DataFrame:
        """Get the full DataFrame for an archetype signature."""
        ...

    def list_signatures(self) -> list[ArchetypeSignature]:
        """List archetype signatures registered in the catalog."""
        ...

    def append(
        self, sig: ArchetypeSignature, df: DataFrame, tick: int, world_id: str, run_id: str
    ) -> None:
        """Append new entity states to storage."""
        ...

    def shutdown(self) -> None:
        """Clean up storage resources."""
        ...


class iQueryManager(Protocol):
    """
    Read facade for the Store.

    The Querier abstracts read operations from the underlying storage,
    providing filtered access to entity data by tick, entity_id, or
    component projections.
    """

    def get_archetype(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        """Get all data for an archetype in a world/run."""
        ...

    def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[Component] | None = None,
    ) -> DataFrame:
        """Query archetype data with optional filters."""
        ...

    def list_signatures(self) -> list[ArchetypeSignature]:
        """List archetype signatures known to the underlying store."""
        ...


class iUpdateManager(Protocol):
    """
    Write facade for the Store.

    The Updater handles all write operations, stamping tick/world/run
    metadata and appending to persistent storage.
    """

    def update(
        self, df: DataFrame, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str
    ) -> None:
        """Persist entity state changes to storage."""
        ...


class iWorld(Protocol):
    """
    Central simulation coordinator.

    The World owns entity-to-archetype mappings, live state snapshots,
    and orchestrates the step() cycle:

        1. Query previous state from Querier
        2. Materialize spawn/despawn mutations
        3. Execute processors via System
        4. Persist results via Updater

    Worlds are isolated - each has its own entity namespace and can
    run in parallel via the WorldService.
    """

    def run(self, run_config: RunConfig, **input_kwargs) -> None:
        """Run multiple steps according to config."""
        ...

    def step(self, run_config: RunConfig, **input_kwargs) -> None:
        """Execute one complete simulation tick."""
        ...

    def _run_archetype(
        self, sig: ArchetypeSignature, **input_kwargs
    ) -> tuple[DataFrame, ArchetypeSignature]:
        """Process a single archetype through the full pipeline."""
        ...

    def materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame:
        """Apply pending spawn/despawn operations to the DataFrame."""
        ...

    @property
    def active_signatures(self) -> tuple[type[Component], ...]:
        """Get all archetype signatures with active entities."""
        ...

    def create_entity(self, components: list[Component]) -> int:
        """Create a new entity with the given components."""
        ...

    def spawn_reserved(self, entity_id: int, components: list[Component]) -> None:
        """Create a new entity using a pre-reserved identifier."""
        ...

    def remove_entity(self, entity_id: int) -> None:
        """Mark an entity for removal."""
        ...

    def add_components(self, entity_id: int, components: list[Component]) -> None:
        """Add components to an existing entity (changes archetype)."""
        ...

    def remove_components(self, entity_id: int, component_types: list[type[Component]]) -> None:
        """Remove components from an entity (changes archetype)."""
        ...

    def add_processor(self, proc: iProcessor) -> None:
        """Add a processor to this world's system."""
        ...

    def remove_processor(self, proc_type: type[iProcessor]) -> None:
        """Remove a processor from this world's system."""
        ...

    def add_hook(
        self, event_type: type[HookEvent], fn: SyncHookHandler, /, *args, **kwargs
    ) -> HookHandle:
        """Register a lifecycle hook."""
        ...

    def remove_hook(self, handle: HookHandle) -> None:
        """Unregister a lifecycle hook."""
        ...

    def query_archetype(
        self,
        sig: ArchetypeSignature,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[Component] | None = None,
        run_config: RunConfig | None = None,
    ) -> DataFrame:
        """Query entity data from this world."""
        ...

    def get_components(
        self, components: list[Component], entity_ids: list[int] | None = None
    ) -> DataFrame:
        """Get specific components for entities."""
        ...

    def execute(self, df: DataFrame, sig: ArchetypeSignature, **input_kwargs) -> DataFrame:
        """Execute system processors on the DataFrame."""
        ...

    def update(self, df: DataFrame, sig: ArchetypeSignature, tick: int | None = None) -> None:
        """Persist DataFrame changes via the Updater."""
        ...


# ---------------------------------------
# Asynchronous interfaces
# --------------------------------------
class iAsyncProcessor(Protocol):
    components: tuple[type[Component], ...] = None
    priority: int = 0

    async def process(self, df: DataFrame, **input_kwargs) -> DataFrame: ...


class iAsyncSystem(Protocol):
    async def add_processor(self, proc: iAsyncProcessor) -> None: ...
    async def remove_processor(self, proc_type: type[iAsyncProcessor]) -> None: ...
    async def execute(
        self, df: DataFrame, sig: ArchetypeSignature, **input_kwargs
    ) -> DataFrame: ...


class iAsyncStore(Protocol):
    async def get_archetype_df(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        active_only: bool = False,
    ) -> DataFrame: ...
    async def list_signatures(self) -> list[ArchetypeSignature]: ...
    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> None: ...
    async def shutdown(self) -> None: ...


class iAsyncQueryManager(Protocol):
    async def get_archetype(
        self, sig: ArchetypeSignature, world_id: str, run_id: str
    ) -> DataFrame: ...
    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[Component] | None = None,
    ) -> DataFrame: ...
    async def list_signatures(self) -> list[ArchetypeSignature]: ...


class iAsyncUpdateManager(Protocol):
    async def update(
        self, df: DataFrame, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str
    ) -> DataFrame: ...


class iAsyncWorld(Protocol):
    async def run(self, run_config: RunConfig, **input_kwargs) -> None: ...
    async def step(self, run_config: RunConfig, **input_kwargs) -> None: ...
    async def _run_archetype(
        self, sig: ArchetypeSignature, **input_kwargs
    ) -> tuple[DataFrame, ArchetypeSignature]: ...
    def materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame: ...
    @property
    def active_signatures(self) -> tuple[type[Component], ...]: ...
    async def create_entity(self, components: list[Component]) -> int: ...
    async def spawn_reserved(self, entity_id: int, components: list[Component]) -> None: ...
    async def remove_entity(self, entity_id: int) -> None: ...
    async def add_components(self, entity_id: int, components: list[Component]) -> None: ...
    async def remove_components(
        self, entity_id: int, component_types: list[type[Component]]
    ) -> None: ...
    async def add_processor(self, proc: iAsyncProcessor) -> None: ...
    async def remove_processor(self, proc_type: type[iAsyncProcessor]) -> None: ...
    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[Component] | None = None,
        run_config: RunConfig | None = None,
    ) -> DataFrame: ...
    async def get_components(
        self, components: list[Component], entity_ids: list[int] | None = None
    ) -> DataFrame: ...
    async def execute(
        self, df: DataFrame, sig: ArchetypeSignature, **input_kwargs
    ) -> DataFrame: ...
    async def update(
        self, df: DataFrame, sig: ArchetypeSignature, tick: int | None = None
    ) -> None: ...
    def add_hook(
        self,
        event_type: type[HookEvent],
        fn: AsyncHookHandler,
        *,
        mode: FireMode = "blocking",
    ) -> HookHandle: ...
    def remove_hook(self, handle: HookHandle) -> None: ...
