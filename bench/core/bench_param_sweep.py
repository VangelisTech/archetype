#!/usr/bin/env python3
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

import argparse
import asyncio
import json
import time
from statistics import median

import daft
from daft import DataType, col

from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_store import AsyncStore
from archetype.core.aio.async_updater import AsyncUpdateManager
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, StorageConfig
from archetype.core.runtime.storage import StorageContextFactory


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
    try:
        df = df.with_columns(
            {
                "entity_id": col("entity_id").cast(DataType.uint32()),
                "tick": col("tick").cast(DataType.uint32()),
            }
        )
    except Exception:
        pass
    return df


async def make_store(uri: str, namespace: str, use_cache: bool, cache_cfg: CacheConfig | None):
    storage = StorageConfig(uri=uri, namespace=namespace)
    context = StorageContextFactory.build(storage)
    base = AsyncStore(context)
    if not use_cache:
        return base, AsyncQueryManager(base), AsyncUpdateManager(base)
    cached = AsyncCachedStore(async_store=base, cache_config=cache_cfg or CacheConfig())
    return cached, AsyncQueryManager(cached), AsyncUpdateManager(cached)


async def bench_append_only(
    store,
    num_entities: int,
    batch_size: int,
    steps: int,
    world_id: str,
    run_id: str,
    include_flush: bool,
):
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    _ = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)

    t0 = time.perf_counter()
    latencies = []
    appended = 0
    for step in range(steps):
        remaining = num_entities
        start_id = step * num_entities
        while remaining > 0:
            n = min(batch_size, remaining)
            df = build_rows(start_id, n, tick=step, world_id=world_id, run_id=run_id)
            s = time.perf_counter()
            await store.append(sig, df)
            latencies.append(time.perf_counter() - s)
            appended += n
            start_id += n
            remaining -= n
    dur = time.perf_counter() - t0
    if include_flush:
        s2 = time.perf_counter()
        # Ensure all buffered data is fully persisted (captures cached flush time)
        await store.shutdown()
        dur += time.perf_counter() - s2
    latencies.sort()
    p50 = median(latencies) if latencies else 0.0
    p95 = latencies[int(len(latencies) * 0.95) - 1] if latencies else 0.0
    return {
        "mode": "append_only",
        "appended": appended,
        "duration_s": dur,
        "throughput_rows_s": appended / max(dur, 1e-9),
        "latency_p50_ms": p50 * 1000.0,
        "latency_p95_ms": p95 * 1000.0,
        "includes_flush": include_flush,
    }


async def bench_updater(
    updater: AsyncUpdateManager,
    store,
    num_entities: int,
    batch_size: int,
    steps: int,
    world_id: str,
    run_id: str,
    include_flush: bool,
):
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    _ = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)

    t0 = time.perf_counter()
    latencies = []
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
            s = time.perf_counter()
            await updater.update(df, sig, tick=step, world_id=world_id, run_id=run_id)
            latencies.append(time.perf_counter() - s)
            appended += n
            start_id += n
            remaining -= n
    dur = time.perf_counter() - t0
    if include_flush:
        s2 = time.perf_counter()
        await store.shutdown()
        dur += time.perf_counter() - s2
    latencies.sort()
    p50 = median(latencies) if latencies else 0.0
    p95 = latencies[int(len(latencies) * 0.95) - 1] if latencies else 0.0
    return {
        "mode": "updater",
        "appended": appended,
        "duration_s": dur,
        "throughput_rows_s": appended / max(dur, 1e-9),
        "latency_p50_ms": p50 * 1000.0,
        "latency_p95_ms": p95 * 1000.0,
        "includes_flush": include_flush,
    }


async def bench_query(querier: AsyncQueryManager, world_id: str, run_id: str, ticks, iters: int):
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    latencies = []
    scanned = 0
    for _ in range(iters):
        s = time.perf_counter()
        df = await querier.query_archetype(
            sig,
            world_id=world_id,
            ticks=ticks,
            entity_ids=None,
            components=[Position(x=0, y=0)],
            run_id=run_id,
        )
        scanned += df.collect().count_rows()
        latencies.append(time.perf_counter() - s)
    latencies.sort()
    p50 = median(latencies) if latencies else 0.0
    p95 = latencies[int(len(latencies) * 0.95) - 1] if latencies else 0.0
    dur = sum(latencies)
    return {
        "mode": "query",
        "rows_scanned": scanned,
        "duration_s": dur,
        "throughput_rows_s": scanned / max(dur, 1e-9),
        "latency_p50_ms": p50 * 1000.0,
        "latency_p95_ms": p95 * 1000.0,
    }


async def run_case(
    use_cache: bool,
    num_entities: int,
    batch_size: int,
    steps: int,
    q_iters: int,
    uri: str,
    namespace: str,
    cache_cfg: CacheConfig | None,
    include_flush: bool,
):
    world_id = "w"
    run_id = "r"
    # Each phase gets its own storage directory to avoid data accumulation
    # across phases (append → updater → query).
    variant_tag = "cached" if use_cache else "plain"

    append_uri = os.path.join(uri, f"{variant_tag}_append")
    os.makedirs(append_uri, exist_ok=True)
    store, querier, updater = await make_store(
        uri=append_uri, namespace=namespace, use_cache=use_cache, cache_cfg=cache_cfg
    )
    a = await bench_append_only(
        store, num_entities, batch_size, steps, world_id, run_id, include_flush
    )

    updater_uri = os.path.join(uri, f"{variant_tag}_updater")
    os.makedirs(updater_uri, exist_ok=True)
    store, querier, updater = await make_store(
        uri=updater_uri, namespace=namespace, use_cache=use_cache, cache_cfg=cache_cfg
    )
    u = await bench_updater(
        updater, store, num_entities, batch_size, steps, world_id, run_id, include_flush
    )

    # Query phase: seed data first, then benchmark reads in isolation.
    query_uri = os.path.join(uri, f"{variant_tag}_query")
    os.makedirs(query_uri, exist_ok=True)
    store, querier, updater = await make_store(
        uri=query_uri, namespace=namespace, use_cache=use_cache, cache_cfg=cache_cfg
    )
    await bench_append_only(
        store, num_entities, batch_size, steps, world_id, run_id, include_flush=False
    )
    await store.shutdown()
    store, querier, updater = await make_store(
        uri=query_uri, namespace=namespace, use_cache=use_cache, cache_cfg=cache_cfg
    )
    try:
        q = await bench_query(querier, world_id, run_id, ticks=list(range(steps)), iters=q_iters)
    finally:
        await store.shutdown()

    return {
        "store": "iceberg",
        "cache": bool(use_cache),
        "variant": "cached" if use_cache else "plain",
        "variant_label": (
            f"iceberg-cached(fr={getattr(cache_cfg, 'flush_rows', None)},fm={getattr(cache_cfg, 'flush_mb', None)}MB,gm={getattr(cache_cfg, 'global_mb', None)}MB,idle={getattr(cache_cfg, 'idle_sec', None)}s)"
            if use_cache
            else "iceberg-plain"
        ),
        "num_entities": num_entities,
        "batch_size": batch_size,
        "steps": steps,
        "q_iters": q_iters,
        "flush_rows": getattr(cache_cfg, "flush_rows", None) if use_cache else None,
        "flush_mb": getattr(cache_cfg, "flush_mb", None) if use_cache else None,
        "global_mb": getattr(cache_cfg, "global_mb", None) if use_cache else None,
        "idle_sec": getattr(cache_cfg, "idle_sec", None) if use_cache else None,
        "append_only": a,
        "updater": u,
        "query": q,
    }


async def main_async(args):
    os.makedirs(args.tmp_dir, exist_ok=True)
    cache_cfg = CacheConfig(
        flush_rows=args.flush_rows,
        flush_mb=args.flush_mb,
        global_mb=args.global_mb,
        idle_sec=args.idle_sec,
    )
    results = []
    for ne in args.num_entities_list:
        bs_list = [ne] if getattr(args, "no_batch", False) else args.batch_sizes
        for bs in bs_list:
            case_uri = os.path.abspath(os.path.join(args.tmp_dir, f"ne{ne}_bs{bs}"))
            os.makedirs(case_uri, exist_ok=True)
            # plain
            results.append(
                await run_case(
                    False,
                    ne,
                    bs,
                    args.steps,
                    args.q_iters,
                    case_uri,
                    args.namespace,
                    None,
                    args.include_flush,
                )
            )
            # cached
            results.append(
                await run_case(
                    True,
                    ne,
                    bs,
                    args.steps,
                    args.q_iters,
                    case_uri,
                    args.namespace,
                    cache_cfg,
                    args.include_flush,
                )
            )

    print(json.dumps(results, indent=2))

    # Plot
    try:
        import matplotlib.pyplot as plt

        # Bar chart: throughput (rows/s) for append and updater at max load
        max_ne = max(args.num_entities_list)
        max_bs = max(args.batch_sizes)
        subset = [r for r in results if r["num_entities"] == max_ne and r["batch_size"] == max_bs]
        labels = []
        x = []
        y = []
        idx = 0
        for rec in subset:
            labels.extend([f"{rec['variant']}-append", f"{rec['variant']}-updater"])
            x.extend([idx, idx + 1])
            y.extend([rec["append_only"]["throughput_rows_s"], rec["updater"]["throughput_rows_s"]])
            idx += 2
        plt.figure(figsize=(10, 5))
        plt.bar(x, y)
        plt.xticks(x, labels, rotation=20)
        plt.ylabel("Throughput (rows/s)")
        plt.title(f"Throughput at max load (ne={max_ne}, bs={max_bs})")
        png1 = os.path.join(args.tmp_dir, "sweep_throughput.png")
        plt.tight_layout()
        plt.savefig(png1)

        # Heatmaps: for append and updater throughput across (num_entities x batch_size)
        import numpy as np

        entities_sorted = sorted(set(r["num_entities"] for r in results))
        batches_sorted = sorted(set(r["batch_size"] for r in results))
        for mode in ("append_only", "updater"):
            for var in ("plain", "cached"):
                grid = np.zeros((len(entities_sorted), len(batches_sorted)))
                for i, ne in enumerate(entities_sorted):
                    for j, bs in enumerate(batches_sorted):
                        recs = [
                            r
                            for r in results
                            if r["variant"] == var
                            and r["num_entities"] == ne
                            and r["batch_size"] == bs
                        ]
                        grid[i, j] = recs[0][mode]["throughput_rows_s"] if recs else 0.0
                plt.figure(figsize=(8, 6))
                im = plt.imshow(grid, aspect="auto", origin="lower", cmap="viridis")
                plt.colorbar(im, label="Throughput (rows/s)")
                plt.xticks(range(len(batches_sorted)), batches_sorted)
                plt.yticks(range(len(entities_sorted)), entities_sorted)
                plt.xlabel("batch_size")
                plt.ylabel("num_entities")
                plt.title(f"Heatmap: {mode} throughput ({var})")
                png_hm = os.path.join(args.tmp_dir, f"heatmap_{mode}_{var}.png")
                plt.tight_layout()
                plt.savefig(png_hm)

        # Table of testcases (with config flags)
        plt.figure(figsize=(14, max(4, len(results) * 0.25)))
        plt.axis("off")
        cols = [
            "variant_label",
            "store",
            "cache",
            "flush_rows",
            "flush_mb",
            "global_mb",
            "idle_sec",
            "num_entities",
            "batch_size",
            "steps",
            "append_tps",
            "append_p95_ms",
            "update_tps",
            "update_p95_ms",
            "query_tps",
            "query_p95_ms",
        ]
        rows = []
        for rec in results:
            rows.append(
                [
                    rec.get("variant_label"),
                    rec.get("store"),
                    rec.get("cache"),
                    rec.get("flush_rows"),
                    rec.get("flush_mb"),
                    rec.get("global_mb"),
                    rec.get("idle_sec"),
                    rec["num_entities"],
                    rec["batch_size"],
                    rec["steps"],
                    round(rec["append_only"]["throughput_rows_s"], 1),
                    round(rec["append_only"]["latency_p95_ms"], 2),
                    round(rec["updater"]["throughput_rows_s"], 1),
                    round(rec["updater"]["latency_p95_ms"], 2),
                    round(rec["query"]["throughput_rows_s"], 1),
                    round(rec["query"]["latency_p95_ms"], 2),
                ]
            )
        import matplotlib.pyplot as plt  # ensure in this scope

        table = plt.table(cellText=rows, colLabels=cols, loc="center")
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1, 1.2)
        png2 = os.path.join(args.tmp_dir, "sweep_matrix.png")
        plt.tight_layout()
        plt.savefig(png2)
        print(
            json.dumps({"plots": {"throughput": png1, "matrix": png2, "heatmaps": True}}, indent=2)
        )
    except Exception as e:
        print(json.dumps({"plot_error": str(e)}))


def parse_args():
    p = argparse.ArgumentParser(
        description="Parameter sweep for AsyncStore vs AsyncCachedStore (Iceberg)"
    )
    p.add_argument("--num-entities-list", type=int, nargs="+", default=[1000, 10000])
    p.add_argument(
        "--entities-powers",
        type=int,
        nargs="*",
        default=[],
        help="If provided, overrides num-entities-list with [10**p for p in powers]",
    )
    p.add_argument("--batch-sizes", type=int, nargs="+", default=[1000, 2000])
    p.add_argument(
        "--no-batch",
        action="store_true",
        help="If set, perform a single append per step (batch_size = num_entities)",
    )
    p.add_argument("--steps", type=int, default=3)
    p.add_argument("--q-iters", type=int, default=3)
    p.add_argument("--tmp-dir", type=str, default=".archetype_param_sweep")
    p.add_argument("--namespace", type=str, default="bench")
    # cache config
    p.add_argument("--flush-rows", type=int, default=500_000)
    p.add_argument("--flush-mb", type=int, default=256)
    p.add_argument("--global-mb", type=int, default=2048)
    p.add_argument("--idle-sec", type=int, default=30)
    p.add_argument(
        "--include-flush",
        action="store_true",
        default=True,
        help="Include store.flush/shutdown time in write durations (recommended)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    # If powers provided, override list
    if args.entities_powers:
        args.num_entities_list = [10**p for p in args.entities_powers]
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
