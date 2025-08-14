from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict

from archetype.core.config import StorageConfig, WorldConfig, RunConfig
from archetype.core.orchestrator import WorldOrchestrator
from archetype.core.aio import AsyncSystem


DEFAULT_STORAGE = StorageConfig(uri=".data", namespace="benchmarks", use_lancedb=False)


@dataclass
class BenchResult:
    name: str
    entities: int
    steps: int
    elapsed_s: float
    extras: Dict[str, Any]

    @property
    def steps_per_sec(self) -> float:
        return self.steps / self.elapsed_s if self.elapsed_s > 0 else 0.0

    @property
    def entities_per_sec(self) -> float:
        total = self.entities * self.steps
        return total / self.elapsed_s if self.elapsed_s > 0 else 0.0


async def make_world(name: str, storage: StorageConfig | None = None, *, instrumented: bool | None = None):
    orchestrator = WorldOrchestrator()
    world = await orchestrator.create_world(
        config=WorldConfig(name=name),
        system=AsyncSystem(),
        storage_config=storage or DEFAULT_STORAGE,
        instrumented=instrumented,
    )
    return world, orchestrator


class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.t1 = time.perf_counter()
        self.elapsed = self.t1 - self.t0


