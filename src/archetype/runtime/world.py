# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runtime world handles — lazy, actor-scoped world wrappers."""

from __future__ import annotations

import asyncio
import itertools
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from weakref import WeakSet

from uuid_utils import UUID

from archetype.app.models import Command, CommandType, RunResult
from archetype.core.aio import AsyncProcessor, AsyncWorld
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.core.hooks import AsyncHookHandler, HookEvent, HookHandle
from archetype.core.resources import Resources

if TYPE_CHECKING:
    from archetype.app.auth.models import ActorCtx
    from archetype.runtime.runtime import ArchetypeRuntime, SyncArchetypeRuntime

_HookEventT = Any  # TypeVar not needed at runtime
_FireMode = Literal["blocking", "spawn"]


def _coerce_storage_config(storage: str | Path | StorageConfig | None) -> StorageConfig:
    if storage is None:
        return StorageConfig()
    if isinstance(storage, StorageConfig):
        return storage
    return StorageConfig(uri=str(storage))


class _RuntimeWorldState:
    """Shared lifecycle state for actor-bound aliases of one logical world."""

    def __init__(
        self,
        *,
        runtime: ArchetypeRuntime,
        name: str = "world",
        storage: str | Path | StorageConfig | None = None,
        cache: CacheConfig | None = None,
        processors: list[AsyncProcessor] | None = None,
        resources: list[Any] | None = None,
        existing_world: AsyncWorld | None = None,
    ) -> None:
        self.runtime = runtime
        self.name = name
        self.storage_config = _coerce_storage_config(storage)
        self.cache_config = cache
        self.init_processors = list(processors or [])
        self.init_resources = list(resources or [])
        self.pending_hooks: list[
            tuple[HookHandle, type[HookEvent], AsyncHookHandler, _FireMode]
        ] = []
        self.handle_map: dict[HookHandle, HookHandle] = {}
        self.sugar_hook_ids = itertools.count(1)
        self.sugar_hook_token = object()
        self.world = existing_world
        self.initialized = existing_world is not None
        self.init_lock = asyncio.Lock()
        self.op_lock = asyncio.Lock()
        self.closed = False
        self.aliases: WeakSet[RuntimeWorld] = WeakSet()

    def bind(self, actor_ctx: ActorCtx) -> RuntimeWorld:
        handle = RuntimeWorld(state=self, actor_ctx=actor_ctx)
        self.aliases.add(handle)
        self.runtime._register_handle(handle)
        return handle

    def ensure_usable(self) -> None:
        self.runtime._ensure_open()
        if self.closed:
            raise RuntimeError("World handle is closed")

    async def ensure_init(self) -> AsyncWorld:
        self.ensure_usable()
        if self.initialized:
            assert self.world is not None
            return self.world

        async with self.init_lock:
            self.ensure_usable()
            if self.initialized:
                assert self.world is not None
                return self.world

            world = await self.runtime._container.world_service.create_world(
                WorldConfig(name=self.name),
                self.storage_config,
                self.cache_config,
            )
            if not isinstance(world, AsyncWorld):
                raise TypeError("ArchetypeRuntime only supports AsyncWorld instances")

            for proc in self.init_processors:
                await world.add_processor(proc)
            for resource in self.init_resources:
                world.resources.insert(resource)
            for sugar_handle, event_type, fn, mode in self.pending_hooks:
                self.handle_map[sugar_handle] = world.add_hook(event_type, fn, mode=mode)
            self.pending_hooks.clear()

            self.world = world
            self.initialized = True
            self.name = world.name or self.name

        assert self.world is not None
        return self.world

    def require_initialized_world(self) -> AsyncWorld:
        self.ensure_usable()
        if not self.initialized or self.world is None:
            raise RuntimeError("World has not been activated yet")
        return self.world

    async def shutdown(self, *, from_runtime: bool) -> None:
        async with self.op_lock:
            if self.closed:
                return
            if (
                self.initialized
                and self.world is not None
                and (from_runtime or not self.runtime._closed)
            ):
                await self.runtime._container.broker.clear(self.world.world_id)
                await self.runtime._container.world_service.remove_world(self.world.world_id)
            self.closed = True
            self.world = None
            self.initialized = False
            for alias in list(self.aliases):
                self.runtime._unregister_handle(alias)


class RuntimeWorld:
    """Lazy world handle bound to an ``ArchetypeRuntime``."""

    def __init__(
        self,
        *,
        state: _RuntimeWorldState,
        actor_ctx: ActorCtx,
    ) -> None:
        self._state = state
        self._actor_ctx = actor_ctx

    def _ensure_usable(self) -> None:
        self._state.ensure_usable()

    def _require_initialized_world(self) -> AsyncWorld:
        return self._state.require_initialized_world()

    async def _submit(self, cmd: Command) -> UUID:
        async with self._state.op_lock:
            world = await self._state.ensure_init()
            return await self._state.runtime._container.command_service.submit(
                world.world_id,
                cmd,
                self._actor_ctx,
            )

    async def spawn(
        self,
        *components: Component,
        tick: int = 0,
        priority: int = 0,
    ) -> int:
        async with self._state.op_lock:
            world = await self._state.ensure_init()
            return await self._state.runtime._container.command_service.submit_spawn(
                world.world_id,
                list(components),
                self._actor_ctx,
                tick=tick,
                priority=priority,
            )

    async def despawn(self, entity_id: int, *, tick: int = 0, priority: int = 0) -> UUID:
        return await self._submit(
            Command(
                type=CommandType.DESPAWN,
                tick=tick,
                priority=priority,
                payload={"entity_id": entity_id},
            )
        )

    async def update(
        self,
        entity_id: int,
        *components: Component,
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        return await self._submit(
            Command(
                type=CommandType.UPDATE,
                tick=tick,
                priority=priority,
                payload={"entity_id": entity_id, "components": list(components)},
            )
        )

    async def add_components(
        self,
        entity_id: int,
        *components: Component,
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        return await self._submit(
            Command(
                type=CommandType.ADD_COMPONENT,
                tick=tick,
                priority=priority,
                payload={"entity_id": entity_id, "components": list(components)},
            )
        )

    async def remove_components(
        self,
        entity_id: int,
        *component_types: type[Component],
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        return await self._submit(
            Command(
                type=CommandType.REMOVE_COMPONENT,
                tick=tick,
                priority=priority,
                payload={"entity_id": entity_id, "component_types": list(component_types)},
            )
        )

    async def add_processor(
        self,
        processor: AsyncProcessor,
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        return await self._submit(
            Command(
                type=CommandType.ADD_PROCESSOR,
                tick=tick,
                priority=priority,
                payload={"processor": processor},
            )
        )

    async def remove_processor(
        self,
        processor_type: type[AsyncProcessor],
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        return await self._submit(
            Command(
                type=CommandType.REMOVE_PROCESSOR,
                tick=tick,
                priority=priority,
                payload={"processor_type": processor_type},
            )
        )

    async def step(
        self,
        *,
        debug: bool = False,
        config: RunConfig | None = None,
        **input_kwargs: Any,
    ) -> int:
        async with self._state.op_lock:
            world = await self._state.ensure_init()
            run_config = config or RunConfig(num_steps=1, debug=debug)
            return await self._state.runtime._container.simulation_service.step(
                world.world_id,
                run_config,
                **input_kwargs,
            )

    async def run(
        self,
        steps: int = 1,
        *,
        debug: bool = False,
        config: RunConfig | None = None,
        **input_kwargs: Any,
    ) -> RunResult:
        async with self._state.op_lock:
            world = await self._state.ensure_init()
            run_config = config or RunConfig(num_steps=steps, debug=debug)
            return await self._state.runtime._container.simulation_service.run(
                world.world_id,
                run_config,
                **input_kwargs,
            )

    async def query(
        self,
        *component_types: type[Component],
        entity_ids: list[int] | None = None,
    ):
        async with self._state.op_lock:
            world = await self._state.ensure_init()
            return await world.get_components(list(component_types), entity_ids=entity_ids)

    async def command_history(self, *, limit: int = 100) -> list[Command]:
        async with self._state.op_lock:
            world = await self._state.ensure_init()
            return await self._state.runtime._container.query_service.get_command_history(
                world.world_id,
                limit=limit,
            )

    async def fork(
        self,
        name: str | None = None,
        *,
        storage: str | Path | StorageConfig | None = None,
        cache: CacheConfig | None = None,
    ) -> RuntimeWorld:
        """Fork this world: create a new world with a snapshot of the current state.

        The fork gets a new world_id but inherits tick, entity state, and
        mutation caches from the source. Source and fork diverge independently.
        """
        async with self._state.op_lock:
            world = await self._state.ensure_init()

            fork_config = WorldConfig(
                name=name or "fork",
                tick=world.tick,
                next_entity_id=world.next_entity_id,
                entity2sig=dict(world.entity2sig),
                spawn_cache=dict(world.spawn_cache),
                despawn_cache=dict(world.despawn_cache),
                run_id=world.run_id,
            )

            forked = await self._state.runtime._container.world_service.create_world(
                fork_config,
                _coerce_storage_config(storage),
                cache,
            )
            if not isinstance(forked, AsyncWorld):
                raise TypeError("ArchetypeRuntime only supports AsyncWorld forks")

            state = _RuntimeWorldState(
                runtime=self._state.runtime,
                name=name or "fork",
                storage=storage,
                cache=cache,
                existing_world=forked,
            )
            return state.bind(self._actor_ctx)

    def as_actor(self, actor_ctx: ActorCtx) -> RuntimeWorld:
        self._ensure_usable()
        return self._state.bind(actor_ctx)

    def add_hook(
        self,
        event_type: type,
        fn: AsyncHookHandler,
        *,
        mode: _FireMode = "blocking",
    ) -> HookHandle:
        self._ensure_usable()
        world = self._state.world
        if self._state.initialized and world is not None:
            return world.add_hook(event_type, fn, mode=mode)
        sugar_handle = HookHandle(
            _id=next(self._state.sugar_hook_ids),
            _event_type=event_type,
            _registry_token=self._state.sugar_hook_token,
        )
        self._state.pending_hooks.append((sugar_handle, event_type, fn, mode))
        return sugar_handle

    def remove_hook(self, handle: HookHandle) -> None:
        self._ensure_usable()
        world = self._state.world
        if self._state.initialized and world is not None:
            world_handle = self._state.handle_map.pop(handle, handle)
            world.remove_hook(world_handle)
            return
        self._state.pending_hooks = [row for row in self._state.pending_hooks if row[0] != handle]

    async def shutdown(self) -> None:
        await self._shutdown_internal(from_runtime=False)

    async def _shutdown_internal(self, *, from_runtime: bool) -> None:
        await self._state.shutdown(from_runtime=from_runtime)

    @property
    def world_id(self) -> UUID:
        return self._require_initialized_world().world_id

    @property
    def tick(self) -> int:
        return self._require_initialized_world().tick

    @property
    def name(self) -> str | None:
        if self._state.initialized and self._state.world is not None:
            return self._state.world.name
        return self._state.name

    @property
    def resources(self) -> Resources:
        return self._require_initialized_world().resources


class SyncRuntimeWorld:
    """Synchronous facade over ``RuntimeWorld``."""

    def __init__(self, world: RuntimeWorld, runtime: SyncArchetypeRuntime) -> None:
        self._world = world
        self._runtime = runtime

    def _run(self, factory) -> Any:
        runner = self._runtime._require_runner()
        return runner.run(factory())

    def spawn(self, *components: Component, tick: int = 0, priority: int = 0) -> int:
        return self._run(lambda: self._world.spawn(*components, tick=tick, priority=priority))

    def despawn(self, entity_id: int, *, tick: int = 0, priority: int = 0) -> UUID:
        return self._run(lambda: self._world.despawn(entity_id, tick=tick, priority=priority))

    def update(
        self,
        entity_id: int,
        *components: Component,
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        return self._run(
            lambda: self._world.update(entity_id, *components, tick=tick, priority=priority)
        )

    def add_components(
        self,
        entity_id: int,
        *components: Component,
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        return self._run(
            lambda: self._world.add_components(entity_id, *components, tick=tick, priority=priority)
        )

    def remove_components(
        self,
        entity_id: int,
        *component_types: type[Component],
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        return self._run(
            lambda: self._world.remove_components(
                entity_id,
                *component_types,
                tick=tick,
                priority=priority,
            )
        )

    def add_processor(
        self,
        processor: AsyncProcessor,
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        return self._run(lambda: self._world.add_processor(processor, tick=tick, priority=priority))

    def remove_processor(
        self,
        processor_type: type[AsyncProcessor],
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        return self._run(
            lambda: self._world.remove_processor(processor_type, tick=tick, priority=priority)
        )

    def step(self, *, debug: bool = False, config: RunConfig | None = None, **kwargs: Any) -> int:
        return self._run(lambda: self._world.step(debug=debug, config=config, **kwargs))

    def run(
        self,
        steps: int = 1,
        *,
        debug: bool = False,
        config: RunConfig | None = None,
        **kwargs: Any,
    ) -> RunResult:
        return self._run(lambda: self._world.run(steps=steps, debug=debug, config=config, **kwargs))

    def query(self, *component_types: type[Component], entity_ids: list[int] | None = None):
        return self._run(lambda: self._world.query(*component_types, entity_ids=entity_ids))

    def command_history(self, *, limit: int = 100) -> list[Command]:
        return self._run(lambda: self._world.command_history(limit=limit))

    def fork(
        self,
        name: str | None = None,
        *,
        storage: str | Path | StorageConfig | None = None,
        cache: CacheConfig | None = None,
    ) -> SyncRuntimeWorld:
        return SyncRuntimeWorld(
            self._run(lambda: self._world.fork(name, storage=storage, cache=cache)),
            self._runtime,
        )

    def as_actor(self, actor_ctx: ActorCtx) -> SyncRuntimeWorld:
        return SyncRuntimeWorld(self._world.as_actor(actor_ctx), self._runtime)

    def add_hook(
        self,
        event_type: type,
        fn: AsyncHookHandler,
        *,
        mode: _FireMode = "blocking",
    ) -> HookHandle:
        return self._world.add_hook(event_type, fn, mode=mode)

    def remove_hook(self, handle: HookHandle) -> None:
        self._world.remove_hook(handle)

    def shutdown(self) -> None:
        self._run(lambda: self._world.shutdown())

    @property
    def world_id(self) -> UUID:
        return self._world.world_id

    @property
    def tick(self) -> int:
        return self._world.tick

    @property
    def name(self) -> str | None:
        return self._world.name

    @property
    def resources(self) -> Resources:
        return self._world.resources
