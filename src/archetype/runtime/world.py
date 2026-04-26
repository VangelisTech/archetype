# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RuntimeWorld — the user-facing world handle.

Holds world_id (not iWorld). Routes every operation through iCommandService.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from weakref import WeakSet

from daft import DataFrame
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
    from archetype.runtime.runtime import ArchetypeRuntime, SyncArchetypeRuntime

_FireMode = Any  # Literal["blocking", "spawn"] — kept loose for forward compat


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
        storage_config: StorageConfig,
        cache_config: CacheConfig | None,
        init_processors: list,
        init_resources: list,
        init_hooks: list[tuple[type[HookEvent], Any]],
        # Pre-activated fork state (set when forking from an existing world)
        world_id: str | UUID | None = None,
    ) -> None:
        self.runtime = runtime
        self.name = name
        self.storage_config = storage_config
        self.cache_config = cache_config
        self.init_processors = init_processors
        self.init_resources = init_resources
        self.init_hooks = init_hooks

        self.world_id: str | UUID | None = world_id
        self.initialized: bool = world_id is not None
        self.init_lock = asyncio.Lock()
        self.op_lock = asyncio.Lock()
        self.closed = False
        self.aliases: WeakSet[RuntimeWorld] = WeakSet()

    async def ensure_init(self, ctx: ActorCtx) -> str | UUID:
        """Single-flight activation. Returns world_id."""
        if self.initialized:
            return self.world_id

        async with self.init_lock:
            if self.initialized:
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
            for proc in self.init_processors:
                await gate.add_processor(ctx, self.world_id, proc)

            for event_type, fn in self.init_hooks:
                # Hooks are non-serializable; wire directly on the world
                # (escape hatch until gate.add_hook is implemented)
                world = self.runtime._container.world_service.get_world(
                    UUID(str(self.world_id))
                )
                world.add_hook(event_type, fn)

            for resource in self.init_resources:
                # TODO: gate.add_resource when implemented
                world = self.runtime._container.world_service.get_world(
                    UUID(str(self.world_id))
                )
                world.resources.insert(resource)

            self.initialized = True

        return self.world_id

    async def shutdown(self, *, from_runtime: bool) -> None:
        """Shut down this world state. Idempotent."""
        if self.closed:
            return
        self.closed = True

        if not from_runtime and self.initialized and self.world_id is not None:
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

    async def despawn(self, entity_id: int) -> None:
        """Remove an entity."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.remove_entity(self._ctx, wid, entity_id)

    async def update(self, entity_id: int, *components: Component) -> None:
        """Overlay values on existing components."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            await self._gate.add_components(self._ctx, wid, entity_id, list(components))

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
        from archetype.runtime._config import coerce_storage, coerce_cache

        async with self._state.op_lock:
            wid = await self._ensure_id()
            info = await self._gate.fork_world(
                self._ctx, wid, name,
                storage_config=coerce_storage(storage),
                cache_config=coerce_cache(cache),
            )

            # Build a pre-activated state for the fork
            fork_state = _RuntimeWorldState(
                runtime=self._state.runtime,
                name=info.name or name or "fork",
                storage_config=coerce_storage(storage),
                cache_config=coerce_cache(cache),
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
        """Query components at the current tick."""
        async with self._state.op_lock:
            wid = await self._ensure_id()
            sig = tuple(sorted(component_types, key=lambda t: t.__name__))
            info = await self._gate.get_world_info(self._ctx, wid)
            return await self._gate.query_archetype(
                self._ctx, sig, str(wid), str(info.run_id or ""),
                entity_ids=entity_ids,
            )

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

    def run(self, steps: int = 1, *, debug: bool = False, config: RunConfig | None = None, **kw) -> RunResult:
        return self._run(lambda: self._world.run(steps=steps, debug=debug, config=config, **kw))

    def run_episode(self, config: EpisodeConfig, **kw) -> EpisodeResult:
        return self._run(lambda: self._world.run_episode(config, **kw))

    def run_rollout(self, config: RolloutConfig, **kw) -> RolloutResult:
        return self._run(lambda: self._world.run_rollout(config, **kw))

    def info(self) -> WorldInfo:
        return self._run(lambda: self._world.info())

    def fork(self, name: str | None = None, *, storage=None, cache=None) -> SyncRuntimeWorld:
        rw = self._run(lambda: self._world.fork(name, storage=storage, cache=cache))
        return SyncRuntimeWorld(rw, self._runtime)

    def destroy(self) -> None:
        self._run(lambda: self._world.destroy())

    def query(self, *component_types: type[Component], entity_ids: list[int] | None = None):
        return self._run(lambda: self._world.query(*component_types, entity_ids=entity_ids))

    def as_actor(self, actor_ctx: ActorCtx) -> SyncRuntimeWorld:
        return SyncRuntimeWorld(self._world.as_actor(actor_ctx), self._runtime)

    def shutdown(self) -> None:
        self._run(lambda: self._world.shutdown())
