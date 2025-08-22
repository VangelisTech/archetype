from __future__ import annotations

import asyncio
import json
import argparse
from dataclasses import asdict
from typing import Sequence, Optional

from . import packed_iteration, simple_iteration, fragmented_iteration, entity_cycle
from . import add_remove
from archetype.core.config import StorageConfig, CacheConfig, StorageBackend
from archetype.core.orchestrator import WorldOrchestrator


async def run_all(
    *,
    steps: int = 1,
    storage: Optional[StorageConfig] = None,
    cache: Optional[CacheConfig] = None,
    instrumented: Optional[bool] = None,
) -> list[dict]:
    results = []
    benches: Sequence[tuple[str, callable]] = [
        ("packed_iteration", packed_iteration.run),
        ("simple_iteration", simple_iteration.run),
        ("fragmented_iteration", fragmented_iteration.run),
        ("entity_cycle", entity_cycle.run),
        ("add_remove", add_remove.run),
    ]

    orch = WorldOrchestrator()
    try:
        for name, fn in benches:
            res, ids = await fn(
                steps=steps,
                orchestrator=orch,
                storage=storage,
                cache_config=cache,
                instrumented=instrumented,
            )
            rec = asdict(res)
            rec.update({"world_id": str(ids[0]), "run_id": str(ids[1])})
            results.append(rec)
    finally:
        await orch.shutdown()

    return results


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run ECS microbenchmarks")
    p.add_argument("--steps", type=int, default=1)
    p.add_argument("--use-cache", action="store_true")
    p.add_argument("--backend", choices=["iceberg", "lancedb"], default="iceberg")
    p.add_argument("--uri", default=None, help="Override storage uri (absolute path recommended)")
    p.add_argument("--namespace", default="benchmarks")
    p.add_argument("--instrumented", action="store_true")
    p.add_argument("--out", default=None, help="Write JSON results to this file")
    return p.parse_args()


def main():
    args = parse_args()
    storage = None
    if args.uri:
        backend = StorageBackend.ICEBERG if args.backend == "iceberg" else StorageBackend.LANCEDB
        storage = StorageConfig(uri=args.uri, namespace=args.namespace, backend=backend)
    cache = CacheConfig() if args.use_cache else None
    out = asyncio.run(run_all(steps=args.steps, storage=storage, cache=cache, instrumented=args.instrumented))
    if args.out:
        import os
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(out, f, indent=2)
    else:
        for r in out:
            print(r)


if __name__ == "__main__":
    main()


