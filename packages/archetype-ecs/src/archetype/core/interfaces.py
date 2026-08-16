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
# signatures are kept in lockstep with the asynchronous core implementation.

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
    allowlist. ``None`` means pre-coordination history with no writer fence;
    once a fence exists, an absent manifest is the explicit empty mapping.
    Callers reconciling a prepared publication therefore treat ``None`` as
    non-authoritative rather than proof that the attempted write is absent.
    """

    async def begin_tick(self, tick: int) -> CommitContext: ...
    async def publish_tick(
        self,
        tick: int,
        ctx: CommitContext,
        sigs: list[ArchetypeSignature],
    ) -> None: ...
    def acknowledge_published_tick(self, tick: int, ctx: CommitContext) -> None:
        """Release local staging after authoritative exact-token confirmation."""
        ...

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

    @property
    def last_committed_receipt(self) -> CommittedTickReceipt | None:
        """Latest locally completed receipt, including interrupted PostTick."""
        ...

    @property
    def has_prepared_tick_commit(self) -> bool:
        """Whether flushed frames await exact manifest reconciliation."""
        ...

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
    async def reconcile_prepared_tick(self) -> CommittedTickReceipt | None:
        """Finish exact publication without admitting ordinary tick work."""
        ...

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
