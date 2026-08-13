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

"""User-facing asynchronous and synchronous world handles."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, Concatenate

from uuid_utils import UUID

from archetype.artifacts.models import (
    ArtifactRef,
    ArtifactSource,
    IngestArtifacts,
    QueryArtifacts,
)
from archetype.commands.models import GetAuditHistory
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.core.hooks import HookEvent
from archetype.evaluation.models import Evaluate, RunGraders
from archetype.runtime_resources import OperationAdmission
from archetype.world.models import (
    AddComponents,
    AddHook,
    AddProcessor,
    AddResource,
    ComponentTypeRef,
    ComponentValue,
    CreateEntities,
    CreateWorld,
    Despawn,
    DestroyWorld,
    EpisodeConfig,
    EpisodeResult,
    ForkWorld,
    GetWorldInfo,
    HookInfo,
    ListHooks,
    ListProcessors,
    ListResources,
    OpenWorldReadonly,
    ProcessorInfo,
    QueryComponents,
    RemoveComponents,
    RemoveHook,
    RemoveProcessor,
    ReserveEntityIds,
    ResourceInfo,
    RolloutConfig,
    RolloutResult,
    Run,
    RunEpisode,
    RunResult,
    RunRollout,
    Spawn,
    SpawnReserved,
    Step,
    Update,
    WorldInfo,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Protocol

    from daft import DataFrame

    from archetype.core.hooks import HookHandle
    from archetype.evaluation.components import EvalReceipt
    from archetype.evaluation.contracts import (
        FrameGrader,
        GraderContract,
        GraderOutput,
    )
    from archetype.runtime.runtime import SyncArchetypeRuntime

    class _RuntimeHost(Protocol):
        _resources: Any

        def _ensure_open(self) -> None: ...

        def _bind_world_state(self, state: _RuntimeWorldState) -> RuntimeWorld: ...


_FireMode = Any  # Literal["blocking", "spawn"] — kept loose for forward compat


def _admitted_world_operation[**P, R](
    operation: Callable[Concatenate[RuntimeWorld, P], Awaitable[R]],
) -> Callable[Concatenate[RuntimeWorld, P], Awaitable[R]]:
    """Keep one complete public handle call inside process and local admission."""

    @wraps(operation)
    async def admitted(self: RuntimeWorld, *args: P.args, **kwargs: P.kwargs) -> R:
        async with self._state.runtime._resources.admit_operation():
            continuation = self._operation_admission.admitted_by_current_task()
            async with self._operation_admission.admit():
                self._state.runtime._ensure_open()
                if (
                    self._state.destroying or self._state.closing or self._state.closed
                ) and not continuation:
                    raise RuntimeError("World handle is closed")
                return await operation(self, *args, **kwargs)

    return admitted


def _clone_components(components: tuple[Component, ...]) -> list[Component]:
    return [component.model_copy(deep=True) for component in components]


def _parse_spawn_batch_args(
    args: tuple[Component | int, ...],
    count: int | None,
) -> tuple[tuple[Component, ...], int]:
    if count is None:
        if not args or not isinstance(args[-1], int):
            raise TypeError("spawn_batch requires a count, e.g. spawn_batch(component, 10000)")
        count = args[-1]
        components = args[:-1]
    else:
        components = args

    if count < 1:
        raise ValueError("spawn_batch count must be >= 1")
    if not components:
        raise ValueError("spawn_batch requires at least one component template")

    templates: list[Component] = []
    for component in components:
        if not isinstance(component, Component):
            raise TypeError("spawn_batch component templates must be Component instances")
        templates.append(component)

    return tuple(templates), count


# ─────────────────────────────────────────────────────────────────────────────
# Shared state (behind potentially multiple aliased handles)
# ─────────────────────────────────────────────────────────────────────────────


class _RuntimeWorldState:
    """Handle-local activation and close state for one logical world."""

    def __init__(
        self,
        *,
        runtime: _RuntimeHost,
        name: str,
        storage_config: StorageConfig | None,
        cache_config: CacheConfig | None,
        init_processors: list,
        init_resources: list,
        init_hooks: list[tuple[type[HookEvent], Any]],
        # Pre-activated fork state (set when forking from an existing world)
        world_id: str | UUID | None = None,
        # Wiring-owned workflows can pre-bind an exact resumed writer and ask
        # the first handle operation to reinstall its process-local behavior.
        install_initializers: bool = False,
        # Attached handles (runtime.attach) reference a world they did not
        # create; shutdown must not destroy it.
        owns_world: bool = True,
    ) -> None:
        self.runtime = runtime
        self.name = name
        self.storage_config = storage_config
        self.cache_config = cache_config
        self.init_processors = init_processors
        self.init_resources = init_resources
        self.init_hooks = init_hooks
        self.owns_world = owns_world

        self.world_id: str | UUID | None = world_id
        self._create_on_init = world_id is None
        self.initialized = world_id is not None and not install_initializers
        self.init_lock = asyncio.Lock()
        self.close_lock = asyncio.Lock()
        self.destroying = False
        self.closing = False
        self.closed = False

    async def ensure_init(self) -> str | UUID:
        """Single-flight activation. Returns world_id."""
        if self.initialized:
            assert self.world_id is not None  # set before `initialized` flips true
            return self.world_id

        async with self.init_lock:
            if self.initialized:
                assert self.world_id is not None
                return self.world_id

            dispatcher = self.runtime._resources.dispatcher
            effective_storage_config = self.storage_config or StorageConfig()

            try:
                if self.world_id is None:
                    info = await dispatcher.apply(
                        CreateWorld(
                            config=WorldConfig(name=self.name),
                            storage_config=effective_storage_config,
                            cache_config=self.cache_config,
                        )
                    )
                    self.world_id = info.world_id

                for proc in self.init_processors:
                    await dispatcher.apply(AddProcessor(world_id=self.world_id, processor=proc))

                for resource in self.init_resources:
                    await dispatcher.apply(AddResource(world_id=self.world_id, resource=resource))

                for event_type, fn in self.init_hooks:
                    await dispatcher.apply(
                        AddHook(
                            world_id=self.world_id,
                            event_type=event_type,
                            handler=fn,
                        )
                    )

                self.storage_config = effective_storage_config
                self.initialized = True
            except BaseException:
                if self._create_on_init and self.world_id is not None:
                    await dispatcher.apply(DestroyWorld(world_id=self.world_id))
                    self.world_id = None
                self.initialized = False
                raise

        assert self.world_id is not None
        return self.world_id

    def require_storage_config(self, capability: str) -> StorageConfig:
        """Return explicit durable coordinates or fail before capability effects."""

        storage_config = self.storage_config
        if storage_config is None:
            raise ValueError(
                f"{capability} requires explicit storage coordinates; "
                "attach the world with storage=..."
            )
        return storage_config

    async def shutdown(self) -> None:
        """Close this local handle state without destroying durable world state."""
        async with self.close_lock:
            if self.closed:
                return
            self.closing = True
            self.closed = True


# ─────────────────────────────────────────────────────────────────────────────
# RuntimeWorld — the handle
# ─────────────────────────────────────────────────────────────────────────────


class RuntimeWorld:
    """Operate one world through an `ArchetypeRuntime`.

    Handles are lazy and safe to create before the world exists. The first
    operation activates the world. The trusted runtime path is actor-free;
    authorization belongs to remote adapters at the dispatcher ingress
    boundary.
    """

    def __init__(self, *, state: _RuntimeWorldState, reservation: Any | None = None) -> None:
        self._state = state
        self._reservation = reservation
        self._operation_admission = OperationAdmission(closed_message="World handle is closed")

    @property
    def _dispatcher(self):
        return self._state.runtime._resources.dispatcher

    async def _ensure_id(self) -> str | UUID:
        self._state.runtime._ensure_open()
        if (
            self._state.destroying or self._state.closing or self._state.closed
        ) and not self._operation_admission.admitted_by_current_task():
            raise RuntimeError("World handle is closed")
        return await self._state.ensure_init()

    async def _close_owned(self) -> None:
        """Close callback retained by the process lifetime owner."""

        await self._begin_local_close()
        await self._operation_admission.wait_drained()
        await self._state.shutdown()

    async def _begin_local_close(self) -> None:
        """Reject late local calls before any potentially blocking owner join."""

        self._state.closing = True
        await self._operation_admission.stop_admission()

    # ── Properties (sync, no round-trip) ──────────────────────────────────

    @property
    def active_world_id(self) -> str | UUID | None:
        """Return the durable identity without activating this lazy handle."""

        return self._state.world_id

    @property
    def world_id(self) -> str | UUID:
        """Return the durable world identifier after activation."""
        if self._state.world_id is None:
            raise RuntimeError("World has not been activated yet")
        return self._state.world_id

    @property
    def name(self) -> str:
        """Return the handle's local world name."""
        return self._state.name

    def library(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """Construct one installed world-scoped library adapter."""

        self._state.runtime._ensure_open()
        installed = self._state.runtime._resources.world_library(name)
        factory = installed.world_adapter
        if factory is None:
            raise TypeError(f"world library {name!r} has no world adapter")
        return factory(self, *args, **kwargs)

    @_admitted_world_operation
    async def _call_library(
        self,
        callback: Callable[[str | UUID, StorageConfig | None, Any], Awaitable[Any]],
        *,
        capability: str,
        require_storage: bool = False,
    ) -> Any:
        """Run a trusted adapter callback inside this handle's exact gate.

        This is the narrow extension seam used by world-library adapters. It
        preserves lazy activation and close/admission ordering without exposing
        the handle's mutable state object.
        """

        wid = await self._ensure_id()
        storage = (
            self._state.require_storage_config(capability)
            if require_storage
            else self._state.storage_config
        )
        result = callback(wid, storage, self._dispatcher)
        if not inspect.isawaitable(result):
            raise TypeError("world-library callback must return an awaitable")
        return await result

    # ── Mutations ─────────────────────────────────────────────────────────

    @_admitted_world_operation
    async def spawn(self, *components: Component) -> int:
        """Create an entity and return its reserved identifier."""
        wid = await self._ensure_id()
        return await self._dispatcher.apply(
            Spawn(
                world_id=wid,
                components=tuple(ComponentValue.from_component(value) for value in components),
            )
        )

    @_admitted_world_operation
    async def ingest_artifacts(self, *sources: ArtifactSource) -> tuple[ArtifactRef, ...]:
        """Copy files into the artifact store and index their metadata."""

        wid = str(await self._ensure_id())
        storage_config = self._state.require_storage_config("ingest_artifacts")
        return await self._dispatcher.apply(
            IngestArtifacts(
                world_id=wid,
                sources=tuple(sources),
                storage_config=storage_config,
            )
        )

    @_admitted_world_operation
    async def spawn_many(self, entities: list[list[Component]]) -> list[int]:
        """Create several entities in one batch.

        Each entity's first persisted row contains its supplied components.
        Processors first apply on the following tick.

        Args:
            entities: Component lists, one for each entity.

        Returns:
            A list of entity IDs in the same order as ``entities``.
        """
        wid = await self._ensure_id()
        return await self._dispatcher.apply(
            CreateEntities.from_entities(world_id=wid, entities=entities)
        )

    @_admitted_world_operation
    async def spawn_batch(
        self, *components_or_count: Component | int, count: int | None = None
    ) -> list[int]:
        """Spawn many copies of one component template.

        ``spawn_batch(foo, 10000)`` is shorthand for building 10,000
        component lists and sending them through the existing gated
        ``spawn_many`` batch path. The template components are deep-copied
        per entity so later mutation of one component instance cannot alias
        another spawned row.

        Args:
            *components_or_count: Component templates, followed by a positional
                count; e.g. ``spawn_batch(Position(x=1), 10000)``.
            count: Keyword count for multi-component archetypes; e.g.
                ``spawn_batch(Position(), Velocity(), count=10000)``.

        Returns:
            A list of entity IDs in spawn order.
        """
        components, batch_count = _parse_spawn_batch_args(components_or_count, count)
        entities = [_clone_components(components) for _ in range(batch_count)]
        return await self.spawn_many(entities)

    @_admitted_world_operation
    async def reserve_ids(self, n: int) -> list[int]:
        """Reserve entity identifiers without creating entities.

        The returned IDs are drawn from the same monotonic counter as
        ``spawn`` / ``spawn_many``, so interleaved calls produce disjoint
        ranges. Use `spawn_reserved()` to materialize a reserved ID.

        Args:
            n: Number of identifiers to reserve. Must be at least one.

        Returns:
            Reserved identifiers in ascending order.
        """
        wid = await self._ensure_id()
        return await self._dispatcher.apply(ReserveEntityIds(world_id=wid, count=n))

    @_admitted_world_operation
    async def spawn_reserved(self, entity_id: int, *components: Component) -> None:
        """Create an entity with a previously reserved identifier.

        Args:
            entity_id: A previously reserved ID (from ``reserve_ids``).
            *components: Initial component values.

        Raises:
            ValueError: If *entity_id* is already registered.
        """
        wid = await self._ensure_id()
        await self._dispatcher.apply(
            SpawnReserved(
                world_id=wid,
                entity_id=entity_id,
                components=tuple(ComponentValue.from_component(value) for value in components),
            )
        )

    @_admitted_world_operation
    async def despawn(self, entity_id: int) -> None:
        """Remove an entity."""
        wid = await self._ensure_id()
        await self._dispatcher.apply(Despawn(world_id=wid, entity_id=entity_id))

    @_admitted_world_operation
    async def update(self, entity_id: int, *components: Component) -> None:
        """Replace values on component types already held by an entity."""
        wid = await self._ensure_id()
        await self._dispatcher.apply(
            Update(
                world_id=wid,
                entity_id=entity_id,
                components=tuple(ComponentValue.from_component(value) for value in components),
            )
        )

    @_admitted_world_operation
    async def add_components(self, entity_id: int, *components: Component) -> None:
        """Add component types to an entity."""
        wid = await self._ensure_id()
        await self._dispatcher.apply(
            AddComponents(
                world_id=wid,
                entity_id=entity_id,
                components=tuple(ComponentValue.from_component(value) for value in components),
            )
        )

    @_admitted_world_operation
    async def remove_components(self, entity_id: int, *component_types: type[Component]) -> None:
        """Remove component types from an entity."""
        wid = await self._ensure_id()
        await self._dispatcher.apply(
            RemoveComponents(
                world_id=wid,
                entity_id=entity_id,
                component_types=tuple(
                    ComponentTypeRef.from_type(value) for value in component_types
                ),
            )
        )

    @_admitted_world_operation
    async def add_processor(self, processor) -> None:
        """Install a processor on this world."""
        wid = await self._ensure_id()
        await self._dispatcher.apply(AddProcessor(world_id=wid, processor=processor))

    @_admitted_world_operation
    async def remove_processor(self, proc_type) -> None:
        """Remove every installed processor of a type."""
        wid = await self._ensure_id()
        await self._dispatcher.apply(RemoveProcessor(world_id=wid, processor_type=proc_type))

    # ── Simulation ────────────────────────────────────────────────────────

    @_admitted_world_operation
    async def step(self, *, debug: bool = False, config: RunConfig | None = None, **kw) -> None:
        """Advance one tick."""
        wid = await self._ensure_id()
        rc = config or RunConfig(num_steps=1, debug=debug)
        await self._dispatcher.apply(Step(world_id=wid, run_config=rc, input_kwargs=kw))

    @_admitted_world_operation
    async def run(
        self, steps: int = 1, *, debug: bool = False, config: RunConfig | None = None, **kw
    ) -> RunResult:
        """Advance the world by a number of ticks and return the run result."""
        wid = await self._ensure_id()
        rc = config or RunConfig(num_steps=steps, debug=debug)
        return await self._dispatcher.apply(Run(world_id=wid, run_config=rc, input_kwargs=kw))

    @_admitted_world_operation
    async def run_episode(self, config: EpisodeConfig, **kw) -> EpisodeResult:
        """Run until an episode termination condition or step limit is reached."""
        wid = await self._ensure_id()
        return await self._dispatcher.apply(
            RunEpisode(world_id=wid, config=config, input_kwargs=kw)
        )

    @_admitted_world_operation
    async def run_rollout(self, config: RolloutConfig, **kw) -> RolloutResult:
        """Run several episodes on forks of this world."""
        wid = await self._ensure_id()
        return await self._dispatcher.apply(
            RunRollout(world_id=wid, config=config, input_kwargs=kw)
        )

    @_admitted_world_operation
    async def grade(
        self,
        *component_types: type[Component],
        graders: Sequence[FrameGrader],
        entity_ids: list[int] | None = None,
    ) -> list[GraderOutput]:
        """Run graders against this world's append-only history.

        Graders receive one lazy Daft DataFrame. Returned values are ephemeral;
        use `evaluate()` when the outcome needs a durable receipt.
        """
        df = await self.query(*component_types, entity_ids=entity_ids)
        return await self._dispatcher.apply(RunGraders(df=df, graders=tuple(graders)))

    @_admitted_world_operation
    async def evaluate(
        self,
        *component_types: type[Component],
        contract: GraderContract,
        grader: FrameGrader,
        evaluation_id: str,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> EvalReceipt:
        """Persist one evaluation result for an evaluation identity.

        The receipt is pinned to the current snapshot and grader contract.
        Repeating an evaluation identity returns its original receipt without
        grading again. Use a new identity for another nondeterministic trial.
        Durable receipts require an Iceberg-backed world; use ``grade()`` for
        ephemeral scoring on other storage backends.
        """
        wid = await self._ensure_id()
        storage_config = self._state.require_storage_config("evaluate")
        return await self._dispatcher.apply(
            Evaluate(
                world_id=wid,
                components=tuple(component_types),
                contract=contract,
                grader=grader,
                evaluation_id=evaluation_id,
                storage_config=storage_config,
                ticks=tuple(ticks) if ticks is not None else None,
                entity_ids=tuple(entity_ids) if entity_ids is not None else None,
            )
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def _resolve_info(self, wid) -> WorldInfo:
        """World descriptor for live OR cold worlds.

        Live worlds answer from the registry. A handle attached with an
        explicit storage config can also describe a COLD world (durable
        discovery, issue #272) through the catalog — reads never require the
        world to be live.
        """
        try:
            return await self._dispatcher.apply(GetWorldInfo(world_id=wid))
        except Exception:
            if self._state.storage_config is None:
                raise
            return await self._dispatcher.apply(
                OpenWorldReadonly(
                    storage_config=self._state.storage_config,
                    world_id=wid,
                )
            )

    @_admitted_world_operation
    async def info(self) -> WorldInfo:
        """Get an immutable snapshot of world state (live or cold)."""
        wid = await self._ensure_id()
        return await self._resolve_info(wid)

    @_admitted_world_operation
    async def fork(
        self,
        name: str | None = None,
        *,
        storage: str | StorageConfig | None = None,
        cache: CacheConfig | None = None,
    ) -> RuntimeWorld:
        """Create a copy-on-write fork and return its handle."""
        from archetype.runtime._config import coerce_cache, coerce_storage

        # None means "inherit the source's storage" (world-lifecycle.md § 4.5):
        # the gate resolves it to the source's store, and the fork handle keeps
        # the source handle's config so its own reads hit the same store.
        fork_storage = coerce_storage(storage)
        fork_cache = coerce_cache(cache)

        wid = await self._ensure_id()
        info = await self._dispatcher.apply(
            ForkWorld(
                source_world_id=wid,
                name=name,
                storage_config=fork_storage,
                cache_config=fork_cache,
            )
        )

        fork_state = _RuntimeWorldState(
            runtime=self._state.runtime,
            name=info.name or name or "fork",
            storage_config=fork_storage if storage is not None else self._state.storage_config,
            cache_config=fork_cache if cache is not None else self._state.cache_config,
            init_processors=[],
            init_resources=[],
            init_hooks=[],
            world_id=info.world_id,
        )
        return self._state.runtime._bind_world_state(fork_state)

    async def destroy(self) -> None:
        """Destroy the live world while retaining its durable rows."""
        if self._operation_admission.admitted_by_current_task():
            raise RuntimeError("World handle cannot destroy from an admitted operation")
        if self._reservation is not None:
            self._reservation.ensure_close_allowed()
        if self._state.destroying or self._state.closing or self._state.closed:
            raise RuntimeError("World handle is closed")
        async with self._state.runtime._resources.admit_operation():
            self._state.runtime._ensure_open()
            async with self._state.close_lock:
                if self._state.destroying or self._state.closing or self._state.closed:
                    raise RuntimeError("World handle is closed")
                self._state.destroying = True
                try:
                    await self._operation_admission.wait_drained()
                    if self._state.closing or self._state.closed:
                        raise RuntimeError("World handle is closed")
                    wid = await self._state.ensure_init()
                    await self._dispatcher.apply(DestroyWorld(world_id=wid))
                    await self._begin_local_close()
                finally:
                    self._state.destroying = False
            await self._shutdown_internal(from_runtime=False)

    async def shutdown(self) -> None:
        """Close this handle without destroying the world."""
        if self._operation_admission.admitted_by_current_task():
            raise RuntimeError("World handle cannot close from an admitted operation")
        if self._reservation is not None:
            self._reservation.ensure_close_allowed()
        if self._state.destroying:
            raise RuntimeError("World handle is closed")
        await self._shutdown_internal(from_runtime=False)

    async def _shutdown_internal(self, *, from_runtime: bool) -> None:
        del from_runtime
        async with self._state.close_lock:
            await self._begin_local_close()
        if self._reservation is None:
            await self._close_owned()
            return
        await self._reservation.aclose()

    # ── Queries ───────────────────────────────────────────────────────────

    @_admitted_world_operation
    async def query(
        self, *component_types: type[Component], entity_ids: list[int] | None = None
    ) -> DataFrame:
        """Return append-only history for entities with the requested components.

        The result contains every matching tick, not only current state.
        """
        wid = await self._ensure_id()
        info = await self._resolve_info(wid)
        return await self._dispatcher.apply(
            QueryComponents(
                components=tuple(
                    ComponentTypeRef.from_type(component_type) for component_type in component_types
                ),
                world_id=wid,
                run_id=info.run_id,
                storage_config=self._state.storage_config,
                entity_ids=tuple(entity_ids) if entity_ids is not None else None,
            )
        )

    @_admitted_world_operation
    async def history(self, *, limit: int = 100, **filters: Any) -> DataFrame:
        """Return recent audit-log rows for this world."""
        wid = await self._ensure_id()
        return await self._dispatcher.apply(GetAuditHistory(world_id=wid, limit=limit, **filters))

    @_admitted_world_operation
    async def artifacts(self) -> DataFrame:
        """Return this run's common file-artifact index."""
        wid = str(await self._ensure_id())
        storage_config = self._state.require_storage_config("query_artifacts")
        return await self._dispatcher.apply(
            QueryArtifacts(
                world_id=wid,
                storage_config=storage_config,
            )
        )

    @_admitted_world_operation
    async def list_processors(self) -> list[ProcessorInfo]:
        """Return summaries of installed processors."""
        wid = await self._ensure_id()
        return await self._dispatcher.apply(ListProcessors(world_id=wid))

    @_admitted_world_operation
    async def list_hooks(self) -> list[HookInfo]:
        """Return summaries of installed hooks."""
        wid = await self._ensure_id()
        return await self._dispatcher.apply(ListHooks(world_id=wid))

    @_admitted_world_operation
    async def add_hook(
        self,
        event_type: type[HookEvent],
        fn: Callable,
        *,
        mode: _FireMode = "blocking",
    ) -> HookHandle:
        """Install a hook on an active world.

        Hooks needed during activation should be passed to `runtime.world()`.
        """
        if not self._state.initialized:
            raise RuntimeError(
                "Cannot add_hook before activation. Pass hooks via runtime.world(..., hooks=[...])."
            )
        wid = await self._ensure_id()
        return await self._dispatcher.apply(
            AddHook(
                world_id=wid,
                event_type=event_type,
                handler=fn,
                mode=mode,
            )
        )

    @_admitted_world_operation
    async def remove_hook(self, handle: HookHandle) -> None:
        """Remove a hook by handle."""
        wid = await self._ensure_id()
        await self._dispatcher.apply(RemoveHook(world_id=wid, handle=handle))

    @_admitted_world_operation
    async def list_resources(self) -> list[ResourceInfo]:
        """Return summaries of installed resources."""
        wid = await self._ensure_id()
        return await self._dispatcher.apply(ListResources(world_id=wid))

    # ── Aliasing ──────────────────────────────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        resources = self._state.runtime._resources
        for manifest in getattr(resources, "world_library_manifests", ()):
            target = manifest.world_method_aliases.get(name)
            if target is None:
                continue

            async def compatibility_alias(
                *args: Any,
                _library: str = manifest.name,
                _target: str = target,
                **kwargs: Any,
            ) -> Any:
                adapter = self.library(_library)
                method = getattr(adapter, _target)
                result = method(*args, **kwargs)
                if not inspect.isawaitable(result):
                    raise TypeError(
                        f"world-library compatibility method {_target!r} must return an awaitable"
                    )
                return await result

            return compatibility_alias
        raise AttributeError(f"{type(self).__name__!s} has no attribute {name!r}")


# ─────────────────────────────────────────────────────────────────────────────
# SyncRuntimeWorld
# ─────────────────────────────────────────────────────────────────────────────


class SyncRuntimeWorld:
    """Synchronous compatibility facade over `RuntimeWorld`."""

    def __init__(self, world: RuntimeWorld, runtime: SyncArchetypeRuntime) -> None:
        self._world = world
        self._runtime = runtime

    def _run(self, factory) -> Any:
        return self._runtime._dispatch(factory())

    @property
    def world_id(self):
        return self._world.world_id

    @property
    def name(self):
        return self._world.name

    def spawn(self, *components: Component) -> int:
        return self._run(lambda: self._world.spawn(*components))

    def spawn_many(self, entities: list[list[Component]]) -> list[int]:
        return self._run(lambda: self._world.spawn_many(entities))

    def ingest_artifacts(self, *sources: ArtifactSource) -> tuple[ArtifactRef, ...]:
        return self._run(lambda: self._world.ingest_artifacts(*sources))

    def spawn_batch(
        self, *components_or_count: Component | int, count: int | None = None
    ) -> list[int]:
        return self._run(lambda: self._world.spawn_batch(*components_or_count, count=count))

    def reserve_ids(self, n: int) -> list[int]:
        return self._run(lambda: self._world.reserve_ids(n))

    def spawn_reserved(self, entity_id: int, *components: Component) -> None:
        self._run(lambda: self._world.spawn_reserved(entity_id, *components))

    def despawn(self, entity_id: int) -> None:
        self._run(lambda: self._world.despawn(entity_id))

    def update(self, entity_id: int, *components: Component) -> None:
        self._run(lambda: self._world.update(entity_id, *components))

    def add_components(self, entity_id: int, *components: Component) -> None:
        self._run(lambda: self._world.add_components(entity_id, *components))

    def remove_components(self, entity_id: int, *component_types: type[Component]) -> None:
        self._run(lambda: self._world.remove_components(entity_id, *component_types))

    def add_processor(self, processor) -> None:
        self._run(lambda: self._world.add_processor(processor))

    def remove_processor(self, proc_type) -> None:
        self._run(lambda: self._world.remove_processor(proc_type))

    def step(self, *, debug: bool = False, config: RunConfig | None = None, **kw) -> None:
        self._run(lambda: self._world.step(debug=debug, config=config, **kw))

    def run(
        self, steps: int = 1, *, debug: bool = False, config: RunConfig | None = None, **kw
    ) -> RunResult:
        return self._run(lambda: self._world.run(steps=steps, debug=debug, config=config, **kw))

    def run_episode(self, config: EpisodeConfig, **kw) -> EpisodeResult:
        return self._run(lambda: self._world.run_episode(config, **kw))

    def run_rollout(self, config: RolloutConfig, **kw) -> RolloutResult:
        return self._run(lambda: self._world.run_rollout(config, **kw))

    def grade(
        self,
        *component_types: type[Component],
        graders: Sequence[FrameGrader],
        entity_ids: list[int] | None = None,
    ) -> list[GraderOutput]:
        return self._run(
            lambda: self._world.grade(*component_types, graders=graders, entity_ids=entity_ids)
        )

    def evaluate(
        self,
        *component_types: type[Component],
        contract: GraderContract,
        grader: FrameGrader,
        evaluation_id: str,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> EvalReceipt:
        """Persist one evaluation result for an evaluation identity."""
        return self._run(
            lambda: self._world.evaluate(
                *component_types,
                contract=contract,
                grader=grader,
                evaluation_id=evaluation_id,
                ticks=ticks,
                entity_ids=entity_ids,
            )
        )

    def info(self) -> WorldInfo:
        return self._run(lambda: self._world.info())

    def fork(self, name: str | None = None, *, storage=None, cache=None) -> SyncRuntimeWorld:
        rw = self._run(lambda: self._world.fork(name, storage=storage, cache=cache))
        return SyncRuntimeWorld(rw, self._runtime)

    def destroy(self) -> None:
        self._run(lambda: self._world.destroy())

    def query(
        self, *component_types: type[Component], entity_ids: list[int] | None = None
    ) -> DataFrame:
        return self._run(lambda: self._world.query(*component_types, entity_ids=entity_ids))

    def history(self, *, limit: int = 100, **filters: Any) -> DataFrame:
        return self._run(lambda: self._world.history(limit=limit, **filters))

    def artifacts(self) -> DataFrame:
        return self._run(self._world.artifacts)

    def list_processors(self) -> list[ProcessorInfo]:
        return self._run(lambda: self._world.list_processors())

    def list_hooks(self) -> list[HookInfo]:
        return self._run(lambda: self._world.list_hooks())

    def list_resources(self) -> list[ResourceInfo]:
        return self._run(lambda: self._world.list_resources())

    def add_hook(
        self,
        event_type: type[HookEvent],
        fn: Callable,
        *,
        mode: _FireMode = "blocking",
    ) -> HookHandle:
        return self._run(lambda: self._world.add_hook(event_type, fn, mode=mode))

    def remove_hook(self, handle: HookHandle) -> None:
        self._run(lambda: self._world.remove_hook(handle))

    def library(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """Return the library's typed adapter over the asynchronous world."""

        return self._world.library(name, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        resources = self._world._state.runtime._resources
        for manifest in getattr(resources, "world_library_manifests", ()):
            target = manifest.world_method_aliases.get(name)
            if target is None:
                continue
            sync_target = manifest.sync_world_method_aliases.get(name, target)
            adapter = self._world.library(manifest.name)
            alias = getattr(adapter, sync_target)

            def synchronous_alias(
                *args: Any,
                _alias: Callable[..., Any] = alias,
                **kwargs: Any,
            ) -> Any:
                return self._runtime._dispatch(_alias(*args, **kwargs))

            return synchronous_alias

        raise AttributeError(f"{type(self).__name__!s} has no attribute {name!r}")

    def shutdown(self) -> None:
        self._run(lambda: self._world.shutdown())
