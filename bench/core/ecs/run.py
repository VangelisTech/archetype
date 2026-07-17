from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence
from dataclasses import asdict

from archetype.core.config import CacheConfig, StorageBackend, StorageConfig
from bench.core.report import build_report, capture_environment, write_report

from . import add_remove, entity_cycle, fragmented_iteration, packed_iteration, simple_iteration
from .common import _default_storage


def _storage_for_bench(storage: StorageConfig | None, bench_name: str) -> StorageConfig:
    """Give each bench its own namespace so sibling benches cannot see one
    another's tables when they happen to share component class names."""
    storage = storage or _default_storage()
    suffix = bench_name.replace("-", "_")
    namespace = f"{storage.namespace}__{suffix}" if storage.namespace else suffix
    return storage.model_copy(update={"namespace": namespace})


def _storage_from_args(args: argparse.Namespace) -> StorageConfig:
    """Resolve CLI overrides against the same defaults used by ``run_all``."""
    storage = _default_storage()
    updates = {}
    if args.uri is not None:
        updates["uri"] = args.uri
    if args.namespace is not None:
        updates["namespace"] = args.namespace
    if args.backend is not None:
        updates["backend"] = StorageBackend(args.backend)
    return storage.model_copy(update=updates)


async def run_all(
    *,
    steps: int = 1,
    storage: StorageConfig | None = None,
    cache: CacheConfig | None = None,
) -> list[dict]:
    results = []
    benches: Sequence[tuple[str, callable]] = [
        ("packed_iteration", packed_iteration.run),
        ("simple_iteration", simple_iteration.run),
        ("fragmented_iteration", fragmented_iteration.run),
        ("entity_cycle", entity_cycle.run),
        ("add_remove", add_remove.run),
    ]

    for name, fn in benches:
        res, ids = await fn(
            steps=steps,
            orchestrator=None,
            storage=_storage_for_bench(storage, name),
            cache_config=cache,
        )
        rec = asdict(res)
        rec.update({"world_id": str(ids[0]), "run_id": str(ids[1]), "bench_name": name})
        results.append(rec)

    return results


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run ECS microbenchmarks")
    p.add_argument("--steps", type=int, default=1)
    p.add_argument("--use-cache", action="store_true")
    p.add_argument(
        "--backend",
        choices=["iceberg", "lancedb"],
        default=None,
        help="Override StorageConfig's backend",
    )
    p.add_argument("--uri", default=None, help="Override storage uri (absolute path recommended)")
    p.add_argument("--namespace", default=None, help="Override ARCHETYPE_BENCH_NS")
    p.add_argument("--out", default=None, help="Write the current schema-v1 report here")
    p.add_argument(
        "--history-dir",
        default=None,
        help="Also archive the report in this history directory",
    )
    p.add_argument(
        "--runner-id",
        default=None,
        help="Stable machine identity; defaults to ARCHETYPE_BENCH_RUNNER or hostname",
    )
    return p.parse_args()


def main():
    args = parse_args()
    storage = _storage_from_args(args)
    cache = CacheConfig() if args.use_cache else None
    results = asyncio.run(run_all(steps=args.steps, storage=storage, cache=cache))
    report = build_report(
        results,
        suite="ecs",
        config={
            "steps": args.steps,
            "storage_backend": storage.backend.value,
            "cache": args.use_cache,
        },
        environment=capture_environment(runner_id=args.runner_id),
    )
    if args.out is not None or args.history_dir is not None:
        write_report(report, current_path=args.out, history_dir=args.history_dir)
    if args.out is None:
        print(json.dumps(report, allow_nan=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
