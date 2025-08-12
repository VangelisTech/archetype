import asyncio
import os
import time
from itertools import product

# Ensure src is importable like toy_async_example does
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from archetype import StorageConfig, CacheConfig, WorldConfig, RunConfig
import toy_async_example as toy


async def run_case(case_id: int, use_lancedb: bool, cache_on: bool, debug: bool, show_rows: int, show_plan: bool) -> dict:
    storage_uri = ".archetype_data/"
    namespace = f"examples_matrix_{case_id}"
    storage_config = StorageConfig(uri=storage_uri, namespace=namespace, use_lancedb=use_lancedb)
    cache_config = CacheConfig() if cache_on else None

    world_config = WorldConfig(name=f"toy_async_world_{case_id}")
    run_config = RunConfig(
        num_steps=3,
        debug=debug,
        show_rows=show_rows,
        explain=show_plan,
    )

    t0 = time.perf_counter()
    # Delegate to the example's orchestration helper
    world = await toy.single_world_simulation(storage_config, world_config, run_config)
    duration_ms = (time.perf_counter() - t0) * 1000

    return {
        "lance": use_lancedb,
        "cache": cache_on,
        "debug": debug,
        "rows": show_rows,
        "plan": show_plan,
        "duration_ms": round(duration_ms, 2),
        "world_id": str(world.world_id),
        "tick": world.tick,
    }


async def main():
    cases = []
    case_id = 0
    for use_lancedb, cache_on, debug, show_rows, show_plan in product(
        [True, False],            # lance on/off
        [True, False],            # cache on/off
        [True, False],            # debug on/off
        [0, 3],                   # snapshot rows
        [False, True],            # plan
    ):
        # Skip meaningless: debug=False but rows>0/plan True
        if not debug and (show_rows > 0 or show_plan):
            continue
        case_id += 1
        result = await run_case(case_id, use_lancedb, cache_on, debug, show_rows, show_plan)
        cases.append(result)
        print(
            f"case={case_id} lance={result['lance']} cache={result['cache']} "
            f"debug={result['debug']} rows={result['rows']} plan={result['plan']} "
            f"tick={result['tick']} duration_ms={result['duration_ms']} world_id={result['world_id'][:12]}"
        )

    # Simple summary
    print(f"\nCompleted {len(cases)} cases.")


if __name__ == "__main__":
    asyncio.run(main())


