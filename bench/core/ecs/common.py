from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archetype.app.storage.service import StorageService
from archetype.app.world.service import WorldService
from archetype.core.aio import AsyncSystem
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig

__all__ = [
    "BenchResult",
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


async def make_world(
    name: str,
    system: AsyncSystem | None = None,
    storage: StorageConfig | None = None,
    cache_config: CacheConfig | None = None,
    orchestrator: WorldService | None = None,
) -> tuple[object, WorldService]:
    """
    Create a world for a given (storage, cache) configuration.

    - Accepts an optional existing orchestrator so suites can reuse a single orchestrator across many runs
      (this shares the StorageService and reduces setup overhead).
    - Defaults to a sane local StorageConfig if none is provided.
    """
    orch = orchestrator or WorldService(StorageService())
    world = await orch.create_world(
        config=WorldConfig(name=name),
        storage_config=storage or _default_storage(),
        cache_config=cache_config,
        system=system or AsyncSystem(),
    )
    return world, orch


class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.t1 = time.perf_counter()
        self.elapsed = self.t1 - self.t0
