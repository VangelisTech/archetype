from __future__ import annotations

import asyncio
from dataclasses import asdict
from typing import Sequence

from . import packed_iteration, simple_iteration, fragmented_iteration, entity_cycle, add_remove


async def run_all() -> list[dict]:
    results = []
    # Use 1 step for mutation correctness and overhead visibility; callers can increase
    benches: Sequence[tuple[str, callable]] = [
        ("packed_iteration", packed_iteration.run),
        ("simple_iteration", simple_iteration.run),
        ("fragmented_iteration", fragmented_iteration.run),
        ("entity_cycle", entity_cycle.run),
        ("add_remove", add_remove.run),
    ]

    for name, fn in benches:
        res, ids = await fn()
        rec = asdict(res)
        rec.update({"world_id": str(ids[0]), "run_id": str(ids[1])})
        results.append(rec)

    return results


def main():
    out = asyncio.run(run_all())
    for r in out:
        print(r)


if __name__ == "__main__":
    main()


