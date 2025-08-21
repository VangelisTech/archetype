#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
import time
import json
import argparse
import asyncio
from typing import Optional, Dict, Any

import daft
from daft import col, DataType

from archetype.core.config import StorageConfig, CacheConfig
from archetype.core.runtime.storage import StorageContextFactory
from archetype.core.component import Component
from archetype.core.archetype import Archetype
from archetype.core.aio.async_store import AsyncStore
from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_updater import AsyncUpdateManager


class Position(Component):
    x: int
    y: int


def build_rows(start_id: int, count: int, tick: int, world_id: str, run_id: str):
    rows = [
        {
            "entity_id": start_id + i,
            "position__x": start_id + i,
            "position__y": -(start_id + i),
            "is_active": True,
            "tick": tick,
            "world_id": world_id,
            "run_id": run_id,
        }
        for i in range(count)
    ]
    df = daft.from_pylist(rows).collect()
    # Normalize types for cache memtable stability
    try:
        df = df.with_columns({
            "entity_id": col("entity_id").cast(DataType.uint32()),
            "tick": col("tick").cast(DataType.uint32()),
        })
    except Exception:
        pass
    return df


async def make_store(uri: str, namespace: str, use_cache: bool, cache_cfg: Optional[CacheConfig]):
    storage = StorageConfig(uri=uri, namespace=namespace)
    context = StorageContextFactory.build(storage)
    base = AsyncStore(context)
    if not use_cache:
        return base, AsyncQueryManager(base), AsyncUpdateManager(base)
    cached = AsyncCachedStore(async_store=base, cache_config=cache_cfg or CacheConfig())
    return cached, AsyncQueryManager(cached), AsyncUpdateManager(cached)


async def bench_append_only(store, num_entities: int, batch_size: int, steps: int, world_id: str, run_id: str) -> Dict[str, Any]:
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    _ = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)

    t0 = time.perf_counter()
    appended = 0
    for step in range(steps):
        remaining = num_entities
        start_id = step * num_entities
        while remaining > 0:
            n = min(batch_size, remaining)
            df = build_rows(start_id, n, tick=step, world_id=world_id, run_id=run_id)
            await store.append(sig, df)
            appended += n
            start_id += n
            remaining -= n
    dur = time.perf_counter() - t0
    return {
        "mode": "append_only",
        "appended": appended,
        "duration_s": dur,
        "throughput_rows_s": appended / max(dur, 1e-9),
    }


async def bench_updater(updater: AsyncUpdateManager, store, num_entities: int, batch_size: int, steps: int, world_id: str, run_id: str) -> Dict[str, Any]:
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    _ = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)

    t0 = time.perf_counter()
    appended = 0
    for step in range(steps):
        remaining = num_entities
        start_id = step * num_entities
        while remaining > 0:
            n = min(batch_size, remaining)
            rows = [
                {
                    "entity_id": start_id + i,
                    "position__x": start_id + i,
                    "position__y": -(start_id + i),
                    "is_active": True,
                }
                for i in range(n)
            ]
            df = daft.from_pylist(rows)
            await updater.update(df, sig, tick=step, world_id=world_id, run_id=run_id)
            appended += n
            start_id += n
            remaining -= n
    dur = time.perf_counter() - t0
    return {
        "mode": "updater",
        "appended": appended,
        "duration_s": dur,
        "throughput_rows_s": appended / max(dur, 1e-9),
    }


async def bench_query(querier: AsyncQueryManager, num_entities: int, world_id: str, run_id: str, ticks, iters: int) -> Dict[str, Any]:
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    t0 = time.perf_counter()
    scanned = 0
    for _ in range(iters):
        df = await querier.query_archetype(sig, world_id=world_id, ticks=ticks, entity_ids=None, components=[Position(x=0, y=0)], run_id=run_id)
        scanned += df.collect().count_rows()
    dur = time.perf_counter() - t0
    return {
        "mode": "query",
        "rows_scanned": scanned,
        "duration_s": dur,
        "throughput_rows_s": scanned / max(dur, 1e-9),
    }


async def run_one(use_cache: bool, args) -> Dict[str, Any]:
    uri = os.path.abspath(args.tmp_dir)
    os.makedirs(uri, exist_ok=True)
    cache_cfg = CacheConfig(
        flush_rows=args.flush_rows,
        flush_mb=args.flush_mb,
        global_mb=args.global_mb,
        idle_sec=args.idle_sec,
    ) if use_cache else None

    world_id = "w"
    run_id = "r"

    results = {"variant": "async_cached" if use_cache else "async"}
    try:
        # Fresh store per phase to avoid cache memtable schema mixing
        store, querier, updater = await make_store(uri=uri, namespace=args.namespace, use_cache=use_cache, cache_cfg=cache_cfg)
        a = await bench_append_only(store, args.num_entities, args.batch_size, args.steps, world_id, run_id)
        await store.shutdown()

        store, querier, updater = await make_store(uri=uri, namespace=args.namespace, use_cache=use_cache, cache_cfg=cache_cfg)
        u = await bench_updater(updater, store, args.num_entities, args.batch_size, args.steps, world_id, run_id)
        await store.shutdown()

        store, querier, updater = await make_store(uri=uri, namespace=args.namespace, use_cache=use_cache, cache_cfg=cache_cfg)
        q = await bench_query(querier, args.num_entities, world_id, run_id, ticks=list(range(args.steps)), iters=args.q_iters)
        await store.shutdown()
        results.update({
            "append_only": a,
            "updater": u,
            "query": q,
        })
    finally:
        pass
    return results


async def main_async(args):
    out = []
    if args.variant in ("plain", "both"):
        out.append(await run_one(use_cache=False, args=args))
    if args.variant in ("cached", "both"):
        out.append(await run_one(use_cache=True, args=args))

    print(json.dumps(out, indent=2))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(out, f, indent=2)


def parse_args():
    p = argparse.ArgumentParser(description="Benchmark AsyncStore vs AsyncCachedStore (Iceberg)")
    p.add_argument("--variant", choices=["plain", "cached", "both"], default="both")
    p.add_argument("--num-entities", type=int, default=20000)
    p.add_argument("--batch-size", type=int, default=2000)
    p.add_argument("--steps", type=int, default=3)
    p.add_argument("--q-iters", type=int, default=5)
    p.add_argument("--tmp-dir", type=str, default=".archetype_bench_iceberg")
    p.add_argument("--namespace", type=str, default="bench")
    # cache config
    p.add_argument("--flush-rows", type=int, default=500_000)
    p.add_argument("--flush-mb", type=int, default=256)
    p.add_argument("--global-mb", type=int, default=2048)
    p.add_argument("--idle-sec", type=int, default=30)
    p.add_argument("--out", type=str, default="")
    return p.parse_args()


def main():
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()


