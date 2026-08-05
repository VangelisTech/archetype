from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archetype.core.aio import AsyncSystem, AsyncWorld
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.storage.service import StorageService
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.registry import WorldRegistry

__all__ = [
    "BenchResult",
    "BenchWorldHarness",
    "CacheConfig",
    "RunConfig",
    "StorageConfig",
    "Timer",
    "make_world",
]


def _default_storage() -> StorageConfig:
    uri = Path(os.environ.get("ARCHETYPE_DATA_URI", "./archetype_data")).resolve()
    return StorageConfig(
        uri=uri,
        namespace=os.environ.get("ARCHETYPE_BENCH_NS", "benchmarks"),
    )


@dataclass
class BenchResult:
    name: str
    entities: int
    steps: int
    elapsed_s: float
    extras: dict[str, Any]

    @property
    def steps_per_sec(self) -> float:
        return self.steps / self.elapsed_s if self.elapsed_s > 0 else 0.0

    @property
    def entities_per_sec(self) -> float:
        total = self.entities * self.steps
        return total / self.elapsed_s if self.elapsed_s > 0 else 0.0


@dataclass
class BenchWorldHarness:
    """Own the canonical low-level world resources used by core benchmarks."""

    storage: StorageService
    registry: WorldRegistry
    lifecycle: WorldLifecycle

    @classmethod
    def create(cls) -> BenchWorldHarness:
        storage = StorageService()
        registry = WorldRegistry()
        return cls(
            storage=storage,
            registry=registry,
            lifecycle=WorldLifecycle(storage, registry),
        )

    async def shutdown(self) -> None:
        for world in await self.registry.list_worlds():
            await self.lifecycle.destroy_world(world.world_id)
        await self.storage.shutdown()


async def make_world(
    name: str,
    system: AsyncSystem | None = None,
    storage: StorageConfig | None = None,
    cache_config: CacheConfig | None = None,
    harness: BenchWorldHarness | None = None,
) -> tuple[AsyncWorld, BenchWorldHarness]:
    """
    Create a world for a given (storage, cache) configuration.

    - Accepts an optional existing harness so suites can reuse one canonical
      registry/lifecycle/storage graph across many runs.
    - Defaults to a sane local StorageConfig if none is provided.
    """
    worlds = harness or BenchWorldHarness.create()
    world = await worlds.lifecycle.create_world(
        config=WorldConfig(name=name),
        storage_config=storage or _default_storage(),
        cache_config=cache_config,
        system=system or AsyncSystem(),
    )
    return world, worlds


class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.t1 = time.perf_counter()
        self.elapsed = self.t1 - self.t0
