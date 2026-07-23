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

# These Protocols are the typed surface of the contracts in
# docs/guide/specification.md. Each section cites the normative spec it mirrors;
# signatures are kept in lockstep with the concrete sync/async implementations.

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

import pyarrow as pa
from daft import DataFrame  # type: ignore[import-not-found]
from uuid_utils import UUID

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
T = TypeVar("T")
_HookEventT = TypeVar("_HookEventT", bound="HookEvent")


# ═══════════════════════════════════════════════════════════════════════════════
# WORLD DEPENDENCIES
# ═══════════════════════════════════════════════════════════════════════════════


class iResourceContainer(Protocol):
    """Type-safe DI container for shared world resources (spec §206)."""

    def insert(self, resource: T) -> None: ...
    def get(self, resource_type: type[T]) -> T | None: ...
    def require(self, resource_type: type[T]) -> T: ...
    def remove(self, resource_type: type[T]) -> T | None: ...
    def contains(self, resource_type: type[T]) -> bool: ...
    def items(self) -> Iterable[tuple[type, object]]: ...


# ═══════════════════════════════════════════════════════════════════════════════
# SYNCHRONOUS INTERFACES
# ═══════════════════════════════════════════════════════════════════════════════


class iProcessor(Protocol):
    """
    Transforms entity data for a specific archetype (spec §196–§223).

    Processors are the behavioral building blocks of a simulation.
    Each processor declares which Components it operates on and a priority
    for execution ordering within the System.

    The process() method receives a Daft DataFrame containing all entities
    matching the archetype signature and returns a transformed DataFrame.
    """

    # A processor MUST declare its component set (spec §200). The default is the
    # empty tuple — matches every signature — never None (concrete bases use ()).
    components: tuple[type[Component], ...] = ()
    priority: int = 0

    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        """Transform entity data. Pure function: df in → df out."""
        ...


class iSystem(Protocol):
    """
    Orchestrates Processor execution in priority order (spec §196–§223).

    The System manages a collection of Processors and executes them
    on matching archetypes during each simulation step. Lower priority
    values execute first.
    """

    processors: list[iProcessor]

    def add_processor(self, processor: iProcessor) -> None:
        """Register a processor instance with the system."""
        ...

    def remove_processor(self, proc_type: type[iProcessor]) -> None:
        """Remove all processors of the given type; no-op if none are registered."""
        ...

    def execute(self, df: DataFrame, sig: ArchetypeSignature, *args, **kwargs) -> DataFrame:
        """Execute all matching processors on the archetype DataFrame."""
        ...


class iStore(Protocol):
    """
    Persistent storage backend for archetype tables (spec §132–§157).

    The Store manages physical storage of entity data. It provides
    append-only semantics for time-travel queries and efficient columnar
    storage. Reads are scoped by world_id/run_id; tick/world/run stamping is
    the updater's responsibility, not the store's.
    """

    def get_archetype_df(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        """Get the DataFrame for an archetype, scoped by world_id and run_id (spec §137)."""
        ...

    def list_signatures(self) -> list[ArchetypeSignature]:
        """List archetype signatures registered in the catalog."""
        ...

    def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """Append new entity states. The updater stamps tick/world/run (spec §141, §177)."""
        ...

    def shutdown(self) -> None:
        """Clean up storage resources. Safe to call more than once (spec §144)."""
        ...


class iQueryManager(Protocol):
    """
    Active-state read facade over the Store (spec §159–§173).

    The Querier reads through the store and applies is_active, optional tick
    filters, optional entity filters, and optional component projection. It is
    read-only.
    """

    def get_archetype(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        """Get all data for an archetype in a world/run."""
        ...

    def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type[Component]] | None = None,
        run_config: RunConfig | None = None,
        run_id: str | None = None,
    ) -> DataFrame:
        """Query active archetype data with optional filters.

        ``components`` selects the projection by component type; the canonical
        schema column list is derived from the types (spec §165).
        """
        ...

    def list_signatures(self) -> list[ArchetypeSignature]:
        """List archetype signatures known to the underlying store."""
        ...


class iUpdateManager(Protocol):
    """
    Write facade for the Store (spec §175–§194).

    The Updater normalizes rows, stamps tick/world/run metadata, appends
    through the store, and returns a DataFrame matching the persisted shape.
    """

    def update(
        self, df: DataFrame, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str
    ) -> DataFrame:
        """Persist entity state changes; return the stamped, persisted-shape DataFrame (spec §181)."""
        ...


class iSyncHookBus(Protocol):
    """Synchronous lifecycle hook bus (spec §318–§327)."""

    def add(
        self,
        event_type: type[_HookEventT],
        fn: SyncHookHandler[_HookEventT],
    ) -> HookHandle: ...
    def remove(self, handle: HookHandle) -> None: ...
    def clear(self) -> None: ...
    def items(
        self,
    ) -> Iterable[tuple[type[HookEvent], HookHandle, Callable[..., None], FireMode]]: ...
    def fire(self, event: HookEvent) -> None: ...


class iWorld(Protocol):
    """
    Central simulation coordinator (spec §225–§316).

    The World owns entity-to-archetype mappings, staged mutation caches, and
    orchestrates the step() cycle:

        1. Query previous state from Querier
        2. Materialize spawn/despawn mutations
        3. Execute processors via System
        4. Persist results via Updater

    Worlds are isolated - each has its own entity namespace and can
    run in parallel via the WorldService.
    """

    # ── State ownership (spec §229–§237) ────────────────────────────────────
    world_id: str
    name: str | None

    @property
    def run_id(self) -> UUID: ...

    tick: int
    next_entity_id: int
    entity2sig: dict[int, ArchetypeSignature]
    spawn_cache: dict[ArchetypeSignature, list[dict[str, Any]]]
    despawn_cache: dict[ArchetypeSignature, list[int]]
    # System / querier / updater / resources / hooks integration (spec §237)
    system: iSystem
    querier: iQueryManager
    updater: iUpdateManager
    resources: iResourceContainer
    hooks: iSyncHookBus

    def run(self, run_config: RunConfig, **input_kwargs) -> None:
        """Run multiple steps according to config, preserving run_id (spec §269)."""
        ...

    def step(self, run_config: RunConfig, **input_kwargs) -> CommittedTickReceipt:
        """Execute one complete simulation tick (spec §239)."""
        ...

    def _compute_archetype(
        self, sig: ArchetypeSignature, run_config: RunConfig, **input_kwargs
    ) -> DataFrame:
        """Build one archetype's tick frame without writes or cache consumption."""
        ...

    def materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame:
        """Apply pending spawn/despawn operations to the DataFrame (spec §302)."""
        ...

    @property
    def active_signatures(self) -> set[ArchetypeSignature]:
        """Get all archetype signatures with active entities (spec §244)."""
        ...

    def create_entity(self, components: list[Component]) -> int:
        """Create a new entity with the given components (spec §279)."""
        ...

    def remove_entity(self, entity_id: int) -> None:
        """Mark an entity for removal (spec §287)."""
        ...

    def add_components(self, entity_id: int, components: list[Component]) -> None:
        """Add components to an existing entity (changes archetype) (spec §291)."""
        ...

    def remove_components(self, entity_id: int, component_types: list[type[Component]]) -> None:
        """Remove components from an entity (changes archetype) (spec §291)."""
        ...

    def add_processor(self, processor: iProcessor) -> None:
        """Add a processor instance to this world's system."""
        ...

    def remove_processor(self, processor: type[iProcessor]) -> None:
        """Remove all processors of the given type from this world's system."""
        ...

    def add_hook(
        self, event_type: type[_HookEventT], fn: SyncHookHandler[_HookEventT]
    ) -> HookHandle:
        """Register a lifecycle hook (spec §318)."""
        ...

    def remove_hook(self, handle: HookHandle) -> None:
        """Unregister a lifecycle hook (idempotent, spec §325)."""
        ...

    def query_archetype(
        self,
        sig: ArchetypeSignature,
        run_config: RunConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type[Component]] | None = None,
    ) -> DataFrame:
        """Query entity data from this world."""
        ...

    def get_components(
        self, components: list[type[Component]], entity_ids: list[int] | None = None
    ) -> DataFrame:
        """Get specific components for entities; live-snapshot API (spec §259)."""
        ...

    def execute(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        run_config: RunConfig | None = None,
        **input_kwargs,
    ) -> DataFrame:
        """Execute system processors on the DataFrame."""
        ...

    def update(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        run_config: RunConfig,
        tick: int | None = None,
    ) -> DataFrame:
        """Persist DataFrame changes via the Updater (spec §249)."""
        ...


# ═══════════════════════════════════════════════════════════════════════════════
# COMMIT IDENTITY (issue #273, spec amendment approved 2026-07-14)
# ═══════════════════════════════════════════════════════════════════════════════
# A tick is visible iff its manifest is published. Rows carry a commit token
# and writer epoch; readers admit only rows whose token matches the published
# manifest for that tick, so crashed partial writes and stale-writer appends
# are invisible by construction. The coordinator lives above core (the control
# catalog implements it); core defines the protocol and runs uncoordinated —
# with implicit epoch-0, everything-visible semantics — when none is injected.


@dataclass(frozen=True)
class CommitContext:
    """Identity of one tick-commit attempt by one fenced writer."""

    commit_token: str
    writer_epoch: int


@dataclass(frozen=True, slots=True)
class CommittedTickReceipt:
    """Stable identity of one successfully published world tick.

    ``commands_applied`` is diagnostic and intentionally excluded from
    ``identity``. A bare, uncoordinated world has no manifest visibility token
    and therefore reports ``None``.
    """

    world_id: str
    run_id: str
    committed_tick: int
    visibility_token: str | None
    commands_applied: int

    @property
    def identity(self) -> tuple[str, str, int, str | None]:
        return (
            self.world_id,
            self.run_id,
            self.committed_tick,
            self.visibility_token,
        )


@dataclass(frozen=True)
class AppendReceipt:
    """Durable batch receipt for one append (spec: append returns receipts).

    ``durable`` is False for staged writes (e.g. a caching store's memtable);
    such rows become durable at flush. ``backend_ref`` is an auxiliary
    diagnostic (e.g. a Lance version or Iceberg snapshot id) — never the
    visibility mechanism.
    """

    table_id: str
    rows: int | None
    durable: bool
    backend_ref: str | None = None


class StaleWriterError(RuntimeError):
    """A writer whose epoch lost the fence attempted to publish; fail closed."""


class iCommitCoordinator(Protocol):
    """Fenced tick-commit protocol (spec §273 amendment).

    begin_tick mints the commit identity for a tick attempt; publish_tick
    CAS-publishes the manifest LAST, verifying the writer still holds the
    fence in the same transaction; visible_tokens answers the reader-side
    allowlist (None: no manifests recorded — legacy world, nothing filtered).
    """

    async def begin_tick(self, tick: int) -> CommitContext: ...
    async def publish_tick(
        self,
        tick: int,
        ctx: CommitContext,
        sigs: list[ArchetypeSignature],
    ) -> None: ...
    async def visible_tokens(
        self, world_id: str, run_id: str, ticks: list[int] | None = None
    ) -> dict[int, list[str]] | None: ...


# ═══════════════════════════════════════════════════════════════════════════════
# ASYNCHRONOUS INTERFACES
# ═══════════════════════════════════════════════════════════════════════════════


class iAsyncProcessor(Protocol):
    """Async processor (spec §196–§223)."""

    components: tuple[type[Component], ...] = ()
    priority: int = 0

    async def process(self, df: DataFrame, **input_kwargs) -> DataFrame: ...


class iAsyncSystem(Protocol):
    """Async system; removes processors by type, not identity (spec §196)."""

    processors: list[iAsyncProcessor]

    async def add_processor(self, proc: iAsyncProcessor) -> None: ...
    async def remove_processor(self, proc_type: type[iAsyncProcessor]) -> None: ...
    async def execute(
        self, df: DataFrame, sig: ArchetypeSignature, **input_kwargs
    ) -> DataFrame: ...


class iAsyncStore(Protocol):
    """Async append-only store (spec §132–§157)."""

    async def get_archetype_df(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        active_only: bool = False,
        commit_tokens: list[str] | None = None,
    ) -> DataFrame: ...
    async def list_signatures(self) -> list[ArchetypeSignature]: ...
    async def list_committed_signatures(self) -> list[ArchetypeSignature]:
        """Signatures accepted by a successful append, excluding read-created tables."""
        ...

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> AppendReceipt: ...
    async def flush(self) -> None:
        """Force staged rows durable. No-op for stores that write through;
        caching stores drain their memtables. A commit coordinator's manifest
        head must never be published while its tick's rows are staged-only."""
        ...

    async def shutdown(self) -> None: ...

    # ── Durable-discovery seam (issue #272, approved 2026-07-14) ──────────
    # Reads by persisted physical table identity, for processes that hold a
    # catalog descriptor but not the Python component classes. Both methods
    # are open-never-create: a missing table raises KeyError; no read path
    # may materialize a table (the create-on-read of get_archetype_df is
    # exactly what these exist to avoid).

    async def get_existing_table_schema(self, table_id: str) -> pa.Schema: ...
    async def get_existing_table_df(
        self,
        table_id: str,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        active_only: bool = False,
    ) -> DataFrame: ...


class iAsyncQueryManager(Protocol):
    """Async active-state read facade (spec §159–§173).

    ``commit_tokens`` (issue #273) is the reader-side visibility allowlist.
    An implementation that accepts it MUST filter current-generation rows to
    the allowlisted tokens — accepting and ignoring it silently fails open,
    which the atomic-visibility amendment forbids. Coordinated worlds refuse
    queriers whose signatures cannot accept it (fail closed).
    """

    async def get_archetype(
        self, sig: ArchetypeSignature, world_id: str, run_id: str
    ) -> DataFrame: ...
    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type[Component]] | None = None,
        run_id: str | None = None,
        commit_tokens: list[str] | None = None,
    ) -> DataFrame: ...
    async def query_components(
        self,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        commit_tokens: list[str] | None = None,
    ) -> DataFrame: ...
    async def list_signatures(self) -> list[ArchetypeSignature]: ...
    async def list_committed_signatures(self) -> list[ArchetypeSignature]:
        """List signatures accepted by at least one successful store append."""
        ...


class iAsyncUpdateManager(Protocol):
    """Async write facade; returns the persisted-shape DataFrame (spec §181)."""

    async def update(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        tick: int,
        world_id: str,
        run_id: str,
        commit: CommitContext | None = None,
    ) -> DataFrame: ...


class iAsyncHookBus(Protocol):
    """Async lifecycle hook bus with fire modes (spec §318–§327)."""

    def add(
        self,
        event_type: type[_HookEventT],
        fn: AsyncHookHandler[_HookEventT],
        *,
        mode: FireMode = "blocking",
    ) -> HookHandle: ...
    def remove(self, handle: HookHandle) -> None: ...
    def clear(self) -> None: ...
    def items(
        self,
    ) -> Iterable[tuple[type[HookEvent], HookHandle, Callable[..., Awaitable[None]], FireMode]]: ...
    async def fire(self, event: HookEvent) -> None: ...


class iAsyncWorld(Protocol):
    """Central async simulation coordinator (spec §225–§316)."""

    # ── State ownership (spec §229–§237) ────────────────────────────────────
    world_id: str
    name: str | None

    @property
    def run_id(self) -> UUID: ...

    tick: int
    next_entity_id: int
    entity2sig: dict[int, ArchetypeSignature]
    spawn_cache: dict[ArchetypeSignature, list[dict[str, Any]]]
    despawn_cache: dict[ArchetypeSignature, list[int]]
    # Ancestor read segments (world_id, run_id, up_to_tick) for fork lineage.
    lineage: list[tuple[str, str, int]]
    # System / querier / updater / resources / hooks integration (spec §237)
    system: iAsyncSystem
    querier: iAsyncQueryManager
    updater: iAsyncUpdateManager
    resources: iResourceContainer
    hooks: iAsyncHookBus

    async def run(self, run_config: RunConfig, **input_kwargs) -> None: ...
    async def step(self, run_config: RunConfig, **input_kwargs) -> CommittedTickReceipt: ...
    async def _compute_archetype(
        self, sig: ArchetypeSignature, run_config: RunConfig, **input_kwargs
    ) -> DataFrame: ...
    def materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame: ...
    @property
    def active_signatures(self) -> set[ArchetypeSignature]: ...
    async def create_entity(self, components: list[Component]) -> int: ...
    async def create_entities(self, entities: list[list[Component]]) -> list[int]: ...
    def reserve_entity_ids(self, n: int) -> list[int]: ...
    async def spawn_with_reserved_id(self, entity_id: int, components: list[Component]) -> None: ...
    async def remove_entity(self, entity_id: int) -> None: ...
    async def add_components(self, entity_id: int, components: list[Component]) -> None: ...
    async def remove_components(
        self, entity_id: int, component_types: list[type[Component]]
    ) -> None: ...
    async def add_processor(self, processor: iAsyncProcessor) -> None: ...
    async def remove_processor(self, processor: type[iAsyncProcessor]) -> None: ...
    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        run_config_or_ticks: RunConfig | list[int] | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type[Component]] | None = None,
        run_id: str | None = None,
        run_config: RunConfig | None = None,
    ) -> DataFrame: ...
    async def get_components(
        self, components: list[type[Component]], entity_ids: list[int] | None = None
    ) -> DataFrame: ...
    async def execute(
        self, df: DataFrame, sig: ArchetypeSignature, **input_kwargs
    ) -> DataFrame: ...
    async def update(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        run_config: RunConfig,
        tick: int | None = None,
    ) -> DataFrame: ...
    def add_hook(
        self,
        event_type: type[_HookEventT],
        fn: AsyncHookHandler[_HookEventT],
        *,
        mode: FireMode = "blocking",
    ) -> HookHandle: ...
    def remove_hook(self, handle: HookHandle) -> None: ...
