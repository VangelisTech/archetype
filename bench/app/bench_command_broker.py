#!/usr/bin/env python3
import asyncio
import time
import statistics
from typing import List

from archetype.app.broker import CommandBroker
from archetype.app.models import Command
from archetype.app.auth.models import ActorCtx
from archetype.app.auth import guard as guard_mod


async def producer(broker: CommandBroker, world_id: str, actor: ActorCtx, n: int, *, tick: int = 0) -> float:
    t0 = time.perf_counter()
    for i in range(n):
        cmd = Command(op="custom", payload={"i": i}, tick=tick)
        await broker.enqueue(world_id, cmd, actor)
    return time.perf_counter() - t0


async def consumer(broker: CommandBroker, world_id: str, batch: int, total: int) -> float:
    t0 = time.perf_counter()
    seen = 0
    while seen < total:
        got: List[Command] = await broker.dequeue(world_id, max_items=batch)
        seen += len(got)
        if not got:
            await asyncio.sleep(0)
    return time.perf_counter() - t0


async def main():
    # Relax guardrails for benchmarking
    guard_mod.DAILY_TOKEN_BUDGET = 10**12
    guard_mod.PER_TICK_CMD_BUDGET = 10**9
    guard_mod._TOKENS_USED_TODAY.clear()
    guard_mod._CMD_COUNT_TICK.clear()

    broker = CommandBroker()
    actor = ActorCtx(roles={"admin"})
    world = "w"

    sizes = [10_000, 50_000, 100_000]
    batch_sizes = [100, 1000, 5000]
    for n in sizes:
        for b in batch_sizes:
            # Single-producer single-consumer
            t_prod = await producer(broker, world, actor, n)
            t_cons = await consumer(broker, world, b, n)
            print({
                "mode": "spsc",
                "n": n,
                "batch": b,
                "enqueue_tps": n / max(t_prod, 1e-9),
                "dequeue_tps": n / max(t_cons, 1e-9),
            })

    # Multi-producer multi-consumer
    n = 100_000
    prod_tasks = [producer(broker, world, actor, n // 4) for _ in range(4)]
    tps = await asyncio.gather(*prod_tasks)
    t_prod = max(tps)
    t_cons = await consumer(broker, world, 2000, n)
    print({
        "mode": "mpmc",
        "n": n,
        "batch": 2000,
        "enqueue_tps": n / max(t_prod, 1e-9),
        "dequeue_tps": n / max(t_cons, 1e-9),
    })


if __name__ == "__main__":
    asyncio.run(main())


