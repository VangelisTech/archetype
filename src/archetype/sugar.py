# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Top-level scripting runtime for Archetype.

This module provides an additive sugar layer around the service stack without
changing the existing top-level ``World`` and ``Processor`` exports.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from weakref import WeakSet

from uuid_utils import UUID, uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType, RunResult
from archetype.core.aio import AsyncProcessor, AsyncWorld
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.core.resources import Resources

HookFn = Callable[..., Awaitable[None]]


def _default_actor_ctx() -> ActorCtx:
    # Trusted local script runtime: broad enough for mutation + query + processor
    # changes, but narrower than the service layer's all-powerful "admin".
    return ActorCtx(id=uuid7(), roles={"viewer", "operator", "maintainer"})


def _coerce_storage_config(storage: str | Path | StorageConfig | None) -> StorageConfig:
    if storage is None:
        return StorageConfig()
    if isinstance(storage, StorageConfig):
        return storage
    return StorageConfig(uri=str(storage))


class ArchetypeRuntime:
    """Explicit runtime owner for script-friendly worlds."""

    def __init__(
        self,
        *,
        registry_path: str | Path | None = None,
        actor_ctx: ActorCtx | None = None,
    ) -> None:
        self._container = ServiceContainer(registry_path=registry_path)
        self._actor_ctx = actor_ctx or _default_actor_ctx()
        self._handles: WeakSet[RuntimeWorld] = WeakSet()
        self._closed = False

    async def __aenter__(self) -> ArchetypeRuntime:
        self._ensure_open()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.shutdown()

    @classmethod
    def sync(
        cls,
        *,
        registry_path: str | Path | None = None,
        actor_ctx: ActorCtx | None = None,
    ) -> SyncArchetypeRuntime:
        return SyncArchetypeRuntime(registry_path=registry_path, actor_ctx=actor_ctx)

    def world(
        self,
        name: str = "world",
        *,
        storage: str | Path | StorageConfig | None = None,
        cache: CacheConfig | None = None,
        processors: list[AsyncProcessor] | None = None,
        resources: list[Any] | None = None,
    ) -> RuntimeWorld:
        self._ensure_open()
        handle = RuntimeWorld(
            runtime=self,
            name=name,
            storage=storage,
            cache=cache,
            processors=processors,
            resources=resources,
        )
        self._register_handle(handle)
        return handle

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        for handle in list(self._handles):
            await handle._shutdown_internal(from_runtime=True)
        await self._container.shutdown()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("ArchetypeRuntime is closed")

    def _register_handle(self, handle: RuntimeWorld) -> None:
        self._handles.add(handle)

    def _unregister_handle(self, handle: RuntimeWorld) -> None:
        self._handles.discard(handle)


class RuntimeWorld:
    """Lazy world handle bound to an ``ArchetypeRuntime``."""

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
        self._runtime = runtime
        self._name = name
        self._storage_config = _coerce_storage_config(storage)
        self._cache_config = cache
        self._init_processors = list(processors or [])
        self._init_resources = list(resources or [])
        self._pending_hooks: list[tuple[str, HookFn]] = []
        self._world = existing_world
        self._initialized = existing_world is not None
        self._init_lock = asyncio.Lock()
        self._op_lock = asyncio.Lock()
        self._closed = False

    async def _ensure_init(self) -> AsyncWorld:
        self._ensure_usable()
        if self._initialized:
            assert self._world is not None
            return self._world

        async with self._init_lock:
            self._ensure_usable()
            if self._initialized:
                assert self._world is not None
                return self._world

            world = await self._runtime._container.world_service.create_world(
                WorldConfig(name=self._name),
                self._storage_config,
                self._cache_config,
            )
            if not isinstance(world, AsyncWorld):
                raise TypeError("ArchetypeRuntime only supports AsyncWorld instances")

            for proc in self._init_processors:
                await world.add_processor(proc)
            for resource in self._init_resources:
                world.resources.insert(resource)
            for event, fn in self._pending_hooks:
                world.add_hook(event, fn)

            self._world = world
            self._initialized = True

        assert self._world is not None
        return self._world

    def _ensure_usable(self) -> None:
        self._runtime._ensure_open()
        if self._closed:
            raise RuntimeError("World handle is closed")

    def _require_initialized_world(self) -> AsyncWorld:
        self._ensure_usable()
        if not self._initialized or self._world is None:
            raise RuntimeError("World has not been activated yet")
        return self._world

    async def spawn(
        self,
        *components: Component,
        tick: int = 0,
        priority: int = 0,
    ) -> int:
        async with self._op_lock:
            world = await self._ensure_init()
            return await self._runtime._container.command_service.submit_spawn(
                world.world_id,
                list(components),
                self._runtime._actor_ctx,
                tick=tick,
                priority=priority,
            )

    async def despawn(self, entity_id: int, *, tick: int = 0, priority: int = 0) -> UUID:
        async with self._op_lock:
            world = await self._ensure_init()
            cmd = Command(
                type=CommandType.DESPAWN,
                tick=tick,
                priority=priority,
                payload={"entity_id": entity_id},
            )
            return await self._runtime._container.command_service.submit(
                world.world_id,
                cmd,
                self._runtime._actor_ctx,
            )

    async def add_processor(
        self,
        processor: AsyncProcessor,
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        async with self._op_lock:
            world = await self._ensure_init()
            cmd = Command(
                type=CommandType.ADD_PROCESSOR,
                tick=tick,
                priority=priority,
                payload={"processor": processor},
            )
            return await self._runtime._container.command_service.submit(
                world.world_id,
                cmd,
                self._runtime._actor_ctx,
            )

    async def remove_processor(
        self,
        processor_type: type[AsyncProcessor],
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> UUID:
        async with self._op_lock:
            world = await self._ensure_init()
            cmd = Command(
                type=CommandType.REMOVE_PROCESSOR,
                tick=tick,
                priority=priority,
                payload={"processor_type": processor_type},
            )
            return await self._runtime._container.command_service.submit(
                world.world_id,
                cmd,
                self._runtime._actor_ctx,
            )

    async def step(
        self,
        *,
        debug: bool = False,
        config: RunConfig | None = None,
        **input_kwargs: Any,
    ) -> int:
        async with self._op_lock:
            world = await self._ensure_init()
            run_config = config or RunConfig(num_steps=1, debug=debug)
            return await self._runtime._container.simulation_service.step(
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
        async with self._op_lock:
            world = await self._ensure_init()
            run_config = config or RunConfig(num_steps=steps, debug=debug)
            return await self._runtime._container.simulation_service.run(
                world.world_id,
                run_config,
                **input_kwargs,
            )

    async def query(
        self,
        *component_types: type[Component],
        entity_ids: list[int] | None = None,
    ):
        async with self._op_lock:
            world = await self._ensure_init()
            return await world.get_components(list(component_types), entity_ids=entity_ids)

    async def fork(
        self,
        name: str | None = None,
        *,
        storage: str | Path | StorageConfig | None = None,
        cache: CacheConfig | None = None,
    ) -> RuntimeWorld:
        async with self._op_lock:
            world = await self._ensure_init()
            forked = await self._runtime._container.world_service.fork_world(
                world.world_id,
                name,
                _coerce_storage_config(storage),
                cache,
            )
            if not isinstance(forked, AsyncWorld):
                raise TypeError("ArchetypeRuntime only supports AsyncWorld forks")
            handle = RuntimeWorld(
                runtime=self._runtime,
                name=name or forked.name or "fork",
                storage=storage,
                cache=cache,
                existing_world=forked,
            )
            self._runtime._register_handle(handle)
            return handle

    def add_hook(self, event: str, fn: HookFn) -> None:
        world = self._world
        if self._initialized and world is not None:
            world.add_hook(event, fn)
            return
        self._pending_hooks.append((event, fn))

    def remove_hook(self, event: str, fn: HookFn) -> None:
        world = self._world
        if self._initialized and world is not None:
            world.remove_hook(event, fn)
            return
        self._pending_hooks = [
            (pending_event, pending_fn)
            for pending_event, pending_fn in self._pending_hooks
            if pending_event != event or pending_fn is not fn
        ]

    async def shutdown(self) -> None:
        await self._shutdown_internal(from_runtime=False)

    async def _shutdown_internal(self, *, from_runtime: bool) -> None:
        async with self._op_lock:
            if self._closed:
                return
            if (
                self._initialized
                and self._world is not None
                and (from_runtime or not self._runtime._closed)
            ):
                await self._runtime._container.broker.clear(self._world.world_id)
                self._runtime._container.world_service.remove_world(self._world.world_id)
            self._closed = True
            self._world = None
            self._initialized = False
            self._runtime._unregister_handle(self)

    @property
    def world_id(self) -> UUID:
        return self._require_initialized_world().world_id

    @property
    def tick(self) -> int:
        return self._require_initialized_world().tick

    @property
    def name(self) -> str | None:
        if self._initialized and self._world is not None:
            return self._world.name
        return self._name

    @property
    def resources(self) -> Resources:
        return self._require_initialized_world().resources


class SyncArchetypeRuntime:
    """Synchronous wrapper around ``ArchetypeRuntime`` for scripts."""

    def __init__(
        self,
        *,
        registry_path: str | Path | None = None,
        actor_ctx: ActorCtx | None = None,
    ) -> None:
        self._runtime = ArchetypeRuntime(registry_path=registry_path, actor_ctx=actor_ctx)
        self._runner: asyncio.Runner | None = None

    def __enter__(self) -> SyncArchetypeRuntime:
        self._runner = asyncio.Runner()
        self._runner.run(self._runtime.__aenter__())
        return self

    def __exit__(self, *exc_info: object) -> None:
        assert self._runner is not None
        try:
            self._runner.run(self._runtime.__aexit__(*exc_info))
        finally:
            self._runner.close()
            self._runner = None

    def _require_runner(self) -> asyncio.Runner:
        if self._runner is None:
            raise RuntimeError("SyncArchetypeRuntime is not active")
        return self._runner

    def run(self, awaitable: Awaitable[Any]) -> Any:
        return self._require_runner().run(awaitable)

    def world(
        self,
        name: str = "world",
        *,
        storage: str | Path | StorageConfig | None = None,
        cache: CacheConfig | None = None,
        processors: list[AsyncProcessor] | None = None,
        resources: list[Any] | None = None,
    ) -> SyncRuntimeWorld:
        return SyncRuntimeWorld(
            self._runtime.world(
                name,
                storage=storage,
                cache=cache,
                processors=processors,
                resources=resources,
            ),
            self,
        )


class SyncRuntimeWorld:
    """Synchronous facade over ``RuntimeWorld``."""

    def __init__(self, world: RuntimeWorld, runtime: SyncArchetypeRuntime) -> None:
        self._world = world
        self._runtime = runtime

    def _run(self, factory: Callable[[], Awaitable[Any]]) -> Any:
        runner = self._runtime._require_runner()
        return runner.run(factory())

    def spawn(self, *components: Component, tick: int = 0, priority: int = 0) -> int:
        return self._run(lambda: self._world.spawn(*components, tick=tick, priority=priority))

    def despawn(self, entity_id: int, *, tick: int = 0, priority: int = 0) -> UUID:
        return self._run(lambda: self._world.despawn(entity_id, tick=tick, priority=priority))

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

    def add_hook(self, event: str, fn: HookFn) -> None:
        self._world.add_hook(event, fn)

    def remove_hook(self, event: str, fn: HookFn) -> None:
        self._world.remove_hook(event, fn)

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


def run_sync(coro: Awaitable[Any]) -> Any:
    """Run an async coroutine synchronously."""

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("run_sync() cannot be used from within a running event loop")
