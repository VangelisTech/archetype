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

"""RuntimeWorld — the user-facing world handle.

Holds world_id (not iWorld). Routes every operation through iCommandService.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from weakref import WeakSet

from uuid_utils import UUID

from archetype.app.auth.models import ActorCtx
from archetype.app.models import (
    EpisodeConfig,
    EpisodeResult,
    RolloutConfig,
    RolloutResult,
    RunResult,
    WorldInfo,
)
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.core.hooks import HookEvent

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from archetype.app.autoresearch_service import (
        AutoResearchConfig,
        AutoResearchResult,
        CandidatePreparer,
        Evaluator,
        IterationResult,
    )
    from archetype.app.eval_service import GraderOutput, TrajectoryGrader
    from archetype.runtime.runtime import ArchetypeRuntime, SyncArchetypeRuntime

_FireMode = Any  # Literal["blocking", "spawn"] — kept loose for forward compat


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
    """Shared activation state. One per logical world, N handles (via as_actor)."""

    def __init__(
        self,
        *,
        runtime: ArchetypeRuntime,
        name: str,
        storage_config: StorageConfig | None,
        cache_config: CacheConfig | None,
        init_processors: list,
        init_resources: list,
        init_hooks: list[tuple[type[HookEvent], Any]],
        # Pre-activated fork state (set when forking from an existing world)
        world_id: str | UUID | None = None,
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
        self.initialized: bool = world_id is not None
        self.init_lock = asyncio.Lock()
        self.op_lock = asyncio.Lock()
        self.closed = False
        self.aliases: WeakSet[RuntimeWorld] = WeakSet()

    async def ensure_init(self, ctx: ActorCtx) -> str | UUID:
        """Single-flight activation. Returns world_id."""
        if self.initialized:
            assert self.world_id is not None  # set before `initialized` flips true
            return self.world_id

        async with self.init_lock:
            if self.initialized:
                assert self.world_id is not None
                return self.world_id

            gate = self.runtime._container.command_service

            # Create the world (serializable config only)
            info = await gate.create_world(
                ctx,
                WorldConfig(name=self.name),
                self.storage_config,
                self.cache_config,
            )
            self.world_id = info.world_id

            # Wire non-serializable config through dedicated gate methods
            # Order: processors → resources → hooks (hooks may depend on resources)
            for proc in self.init_processors:
                await gate.add_processor(ctx, self.world_id, proc)

            for resource in self.init_resources:
                await gate.add_resource(ctx, self.world_id, resource)

            for event_type, fn in self.init_hooks:
                await gate.add_hook(ctx, self.world_id, event_type, fn)

            self.initialized = True

        assert self.world_id is not None
        return self.world_id

    async def shutdown(self, *, from_runtime: bool) -> None:
        """Shut down this world state. Idempotent."""
        if self.closed:
            return
        self.closed = True

        if not from_runtime and self.owns_world and self.initialized and self.world_id is not None:
            gate = self.runtime._container.command_service
            # Use a default admin ctx for shutdown
            from archetype.runtime._actor import default_actor_ctx

            await gate.destroy_world(default_actor_ctx(), self.world_id)

        for alias in list(self.aliases):
            self.runtime._unregister_handle(alias)


# ─────────────────────────────────────────────────────────────────────────────
# RuntimeWorld — the handle
# ─────────────────────────────────────────────────────────────────────────────


class RuntimeWorld:
    """User-facing world handle. Holds world_id, NOT iWorld.

    Every operation routes through CommandService with the bound ActorCtx.
    """

    def __init__(self, *, state: _RuntimeWorldState, actor_ctx: ActorCtx) -> None:
        self._state = state
        self._ctx = actor_ctx

    @property
    def _gate(self):
        return self._state.runtime._container.command_service

    async def _ensure_id(self) -> str | UUID:
        self._state.runtime._ensure_open()
        if self._state.closed:
            raise RuntimeError("World handle is closed")
        return await self._state.ensure_init(self._ctx)

    # ── Properties (sync, no round-trip) ──────────────────────────────────

    @property
    def world_id(self) -> str | UUID:
        if not self._state.initialized or self._state.world_id is None:
            raise RuntimeError("World has not been activated yet")
        return self._state.world_id

    @property
    def name(self) -> str:
        return self._state.name

    # ── Mutations ─────────────────────────────────────────────────────────

    async def spawn(self, *components: Component) -> int:
        """Create an entity. Returns entity_id immediately."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return await self._gate.create_entity(self._ctx, wid, list(components))

    async def spawn_many(self, entities: list[list[Component]]) -> list[int]:
        """Batch-spawn entities. Fires one OnSpawn per entity, one batch per archetype.

        All entities obey the initial-conditions contract: each entity's first
        persisted row is its raw spawn values at the materialization tick;
        processors first apply on the following tick (x_0 is given,
        x_{t+1} = f(x_t)).

        Args:
            entities: A list of component lists, one per entity to spawn.

        Returns:
            A list of entity IDs in the same order as ``entities``.
        """
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return await self._gate.create_entities(self._ctx, wid, entities)

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

    async def reserve_ids(self, n: int) -> list[int]:
        """Reserve *n* entity IDs without spawning.

        The returned IDs are drawn from the same monotonic counter as
        ``spawn`` / ``spawn_many``, so interleaved calls produce disjoint
        ranges. Use ``spawn_reserved`` to materialise a reserved ID.

        Args:
            n: Number of IDs to reserve (must be >= 1).

        Returns:
            A sorted list of *n* reserved entity IDs.
        """
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return self._gate.reserve_entity_ids(self._ctx, wid, n)

    async def spawn_reserved(self, entity_id: int, *components: Component) -> None:
        """Materialise a previously reserved entity ID.

        Args:
            entity_id: A previously reserved ID (from ``reserve_ids``).
            *components: Initial component values.

        Raises:
            ValueError: If *entity_id* is already registered.
        """
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.spawn_with_reserved_id(self._ctx, wid, entity_id, list(components))

    async def despawn(self, entity_id: int) -> None:
        """Remove an entity."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.remove_entity(self._ctx, wid, entity_id)

    async def update(self, entity_id: int, *components: Component) -> None:
        """Overlay values on existing components (same archetype)."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.update_entity(self._ctx, wid, entity_id, list(components))

    async def add_components(self, entity_id: int, *components: Component) -> None:
        """Extend entity's archetype with new component types."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.add_components(self._ctx, wid, entity_id, list(components))

    async def remove_components(self, entity_id: int, *component_types: type[Component]) -> None:
        """Remove component types from an entity."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.remove_components(self._ctx, wid, entity_id, list(component_types))

    async def add_processor(self, processor) -> None:
        """Add a processor to this world's system."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.add_processor(self._ctx, wid, processor)

    async def remove_processor(self, proc_type) -> None:
        """Remove a processor from this world's system."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.remove_processor(self._ctx, wid, proc_type)

    # ── Simulation ────────────────────────────────────────────────────────

    async def step(self, *, debug: bool = False, config: RunConfig | None = None, **kw) -> None:
        """Advance one tick."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            rc = config or RunConfig(num_steps=1, debug=debug)
            await self._gate.step(self._ctx, wid, rc, **kw)

    async def run(
        self, steps: int = 1, *, debug: bool = False, config: RunConfig | None = None, **kw
    ) -> RunResult:
        """Run N ticks."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            rc = config or RunConfig(num_steps=steps, debug=debug)
            return await self._gate.run(self._ctx, wid, rc, **kw)

    async def run_episode(self, config: EpisodeConfig, **kw) -> EpisodeResult:
        """Run a bounded episode."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return await self._gate.run_episode(self._ctx, wid, config, **kw)

    async def run_rollout(self, config: RolloutConfig, **kw) -> RolloutResult:
        """Run N forked episodes."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return await self._gate.run_rollout(self._ctx, wid, config, **kw)

    async def autoresearch(
        self,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        *,
        prepare_candidate: CandidatePreparer | None = None,
        lab_world_id: str | UUID | None = None,
        on_iteration: Callable[[IterationResult], Any] | None = None,
    ) -> AutoResearchResult:
        """Run an autoresearch loop with this world as the base save state.

        Each iteration optionally prepares a candidate world (a fork of this
        one), runs a rollout, scores it with ``evaluator``, and advances the
        experiment's BranchHead on improvement. This world is never mutated.
        Loop state lives on the experiment's lab world, so rerunning with the
        same ``config.experiment_id`` resumes from the persisted incumbent.
        Episode worlds are kept by default — load them with
        ``runtime.attach`` to inspect or grade what happened.

        The op lock is held only for activation: ``evaluator``,
        ``prepare_candidate``, and ``on_iteration`` may call back into this
        handle (e.g. ``query``) without deadlocking.
        """
        async with self._state.op_lock:
            wid = await self._ensure_id()
        return await self._gate.autoresearch(
            self._ctx,
            wid,
            config,
            evaluator,
            prepare_candidate=prepare_candidate,
            lab_world_id=lab_world_id,
            on_iteration=on_iteration,
        )

    async def grade(
        self,
        *component_types: type[Component],
        graders: Sequence[TrajectoryGrader],
        entity_ids: list[int] | None = None,
    ) -> list[GraderOutput]:
        """Query this world's rows and execute graders over the result.

        The query is gated and lineage-resolved exactly like ``query``;
        graders receive one lazy Daft DataFrame of the full append-only
        history and decide what to compute. Durable scores remain components
        the caller writes back.
        """
        df = await self.query(*component_types, entity_ids=entity_ids)
        return await self._state.runtime._container.eval_service.run_graders(df, graders)

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def info(self) -> WorldInfo:
        """Get an immutable snapshot of world state."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return await self._gate.get_world_info(self._ctx, wid)

    async def fork(
        self,
        name: str | None = None,
        *,
        storage: str | StorageConfig | None = None,
        cache: CacheConfig | None = None,
    ) -> RuntimeWorld:
        """Fork this world. Returns a new handle."""
        from archetype.runtime._config import coerce_cache, coerce_storage

        # None means "inherit the source's storage" (world-lifecycle.md § 4.5):
        # the gate resolves it to the source's store, and the fork handle keeps
        # the source handle's config so its own reads hit the same store.
        fork_storage = coerce_storage(storage)
        fork_cache = coerce_cache(cache)

        async with self._state.op_lock:
            wid = await self._ensure_id()
            info = await self._gate.fork_world(
                self._ctx,
                wid,
                name,
                storage_config=fork_storage,
                cache_config=fork_cache,
            )

            # Build a pre-activated state for the fork
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
            fork_handle = RuntimeWorld(state=fork_state, actor_ctx=self._ctx)
            fork_state.aliases.add(fork_handle)
            self._state.runtime._register_handle(fork_handle)
            return fork_handle

    async def destroy(self) -> None:
        """Destroy this world. In-memory cleanup; storage retained."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.destroy_world(self._ctx, wid)
            self._state.closed = True
            for alias in list(self._state.aliases):
                self._state.runtime._unregister_handle(alias)

    async def shutdown(self) -> None:
        """Shut down this handle."""
        await self._shutdown_internal(from_runtime=False)

    async def _shutdown_internal(self, *, from_runtime: bool) -> None:
        await self._state.shutdown(from_runtime=from_runtime)

    # ── Queries ───────────────────────────────────────────────────────────

    async def query(self, *component_types: type[Component], entity_ids: list[int] | None = None):
        """Query all entities that have the requested component types.

        Returns the full append-only history (all ticks) for matching entities.
        To get current state only, filter the result:

            df = await world.query(Position)
            info = await world.info()
            current = df.where(col("tick") == info.tick - 1)
        """
        async with self._state.op_lock:
            wid = await self._ensure_id()
            info = await self._gate.get_world_info(self._ctx, wid)
            return await self._gate.query_components(
                self._ctx,
                list(component_types),
                str(wid),
                str(info.run_id or ""),
                storage_config=self._state.storage_config,
                entity_ids=entity_ids,
            )

    async def history(self, *, limit: int = 100, **filters):
        """Read the audit log for this world. Returns raw DataFrame."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return await self._gate.get_audit_history(self._ctx, wid, limit=limit, **filters)

    async def list_processors(self):
        """List processor summaries (ProcessorInfo)."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return await self._gate.list_processors(self._ctx, wid)

    async def list_hooks(self):
        """List hook summaries (HookInfo)."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return await self._gate.list_hooks(self._ctx, wid)

    async def add_hook(self, event_type, fn, *, mode: str = "blocking"):
        """Add a hook. Raises if world not yet activated (use runtime.world(..., hooks=[...]) instead)."""
        if not self._state.initialized:
            raise RuntimeError(
                "Cannot add_hook before activation. Pass hooks via runtime.world(..., hooks=[...])."
            )
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return await self._gate.add_hook(self._ctx, wid, event_type, fn, mode=mode)

    async def remove_hook(self, handle) -> None:
        """Remove a hook by handle."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.remove_hook(self._ctx, wid, handle)

    async def list_resources(self):
        """List resource summaries (ResourceInfo)."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            return await self._gate.list_resources(self._ctx, wid)

    # ── Aliasing ──────────────────────────────────────────────────────────

    def as_actor(self, actor_ctx: ActorCtx) -> RuntimeWorld:
        """Return a sibling handle with a different ActorCtx."""
        sibling = RuntimeWorld(state=self._state, actor_ctx=actor_ctx)
        self._state.aliases.add(sibling)
        self._state.runtime._register_handle(sibling)
        return sibling


# ─────────────────────────────────────────────────────────────────────────────
# SyncRuntimeWorld
# ─────────────────────────────────────────────────────────────────────────────


class SyncRuntimeWorld:
    """Synchronous facade. Mirrors RuntimeWorld without await."""

    def __init__(self, world: RuntimeWorld, runtime: SyncArchetypeRuntime) -> None:
        self._world = world
        self._runtime = runtime

    def _run(self, factory) -> Any:
        return self._runtime._require_runner().run(factory())

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

    def autoresearch(
        self,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        *,
        prepare_candidate: CandidatePreparer | None = None,
        lab_world_id: str | UUID | None = None,
        on_iteration: Callable[[IterationResult], Any] | None = None,
    ) -> AutoResearchResult:
        return self._run(
            lambda: self._world.autoresearch(
                config,
                evaluator,
                prepare_candidate=prepare_candidate,
                lab_world_id=lab_world_id,
                on_iteration=on_iteration,
            )
        )

    def grade(
        self,
        *component_types: type[Component],
        graders: Sequence[TrajectoryGrader],
        entity_ids: list[int] | None = None,
    ) -> list[GraderOutput]:
        return self._run(
            lambda: self._world.grade(*component_types, graders=graders, entity_ids=entity_ids)
        )

    def info(self) -> WorldInfo:
        return self._run(lambda: self._world.info())

    def fork(self, name: str | None = None, *, storage=None, cache=None) -> SyncRuntimeWorld:
        rw = self._run(lambda: self._world.fork(name, storage=storage, cache=cache))
        return SyncRuntimeWorld(rw, self._runtime)

    def destroy(self) -> None:
        self._run(lambda: self._world.destroy())

    def query(self, *component_types: type[Component], entity_ids: list[int] | None = None):
        return self._run(lambda: self._world.query(*component_types, entity_ids=entity_ids))

    def history(self, *, limit: int = 100, **filters):
        return self._run(lambda: self._world.history(limit=limit, **filters))

    def list_processors(self):
        return self._run(lambda: self._world.list_processors())

    def list_hooks(self):
        return self._run(lambda: self._world.list_hooks())

    def list_resources(self):
        return self._run(lambda: self._world.list_resources())

    def add_hook(self, event_type, fn, *, mode: str = "blocking"):
        return self._run(lambda: self._world.add_hook(event_type, fn, mode=mode))

    def remove_hook(self, handle) -> None:
        self._run(lambda: self._world.remove_hook(handle))

    def as_actor(self, actor_ctx: ActorCtx) -> SyncRuntimeWorld:
        return SyncRuntimeWorld(self._world.as_actor(actor_ctx), self._runtime)

    def shutdown(self) -> None:
        self._run(lambda: self._world.shutdown())
