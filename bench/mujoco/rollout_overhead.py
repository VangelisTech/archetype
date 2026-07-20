# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ledger overhead per recorded MuJoCo step.

Compares a raw MuJoCo cartpole rollout against the same rollout recorded
through Archetype (one tick = ``substeps`` physics steps, every tick
persisted to the append-only store). The interesting number is
``overhead_per_tick_ms``: what one ledgered control step costs over bare
physics. Later stages put a VLA policy inference (tens of ms) inside each
tick, so this number certifies how far below the policy budget the
ledger sits.

Usage:
    uv run python bench/mujoco/rollout_overhead.py --case 1x20 --case 64x20
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
from pathlib import Path
from statistics import median
from typing import Any

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")

from archetype import ArchetypeRuntime
from archetype.core.config import StorageConfig
from archetype.physical_ai.mujoco_cartpole import (
    DEFAULT_SUBSTEPS,
    CartpoleState,
    CartpoleStepProcessor,
    raw_rollout,
)


def initial_states(envs: int) -> list[tuple[float, float, float, float]]:
    return [(0.01 * i, 0.1 + 0.001 * i, 0.0, 0.0) for i in range(envs)]


def run_raw_trial(envs: int, ticks: int, substeps: int) -> float:
    states = initial_states(envs)
    start = time.perf_counter()
    raw_rollout(states, ticks=ticks, substeps=substeps)
    return time.perf_counter() - start


async def run_ledger_trial(envs: int, ticks: int, substeps: int, root: Path) -> dict[str, float]:
    storage = StorageConfig(uri=str(root), namespace="mujoco_bench")
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "mujoco-bench",
            storage=storage,
            processors=[CartpoleStepProcessor(substeps=substeps)],
        )

        setup_start = time.perf_counter()
        for cart_pos, pole_angle, cart_vel, pole_vel in initial_states(envs):
            await world.spawn(
                CartpoleState(
                    cart_pos=cart_pos,
                    pole_angle=pole_angle,
                    cart_vel=cart_vel,
                    pole_vel=pole_vel,
                )
            )
        setup_sec = time.perf_counter() - setup_start

        run_start = time.perf_counter()
        await world.run(steps=ticks)
        run_sec = time.perf_counter() - run_start

        return {"setup_sec": setup_sec, "run_sec": run_sec}


async def run_case(
    envs: int, ticks: int, substeps: int, trials: int, warmups: int
) -> dict[str, Any]:
    for _ in range(warmups):
        run_raw_trial(envs, ticks, substeps)
        with tempfile.TemporaryDirectory() as tmp:
            await run_ledger_trial(envs, ticks, substeps, Path(tmp) / "store")

    raw_secs = []
    ledger_records = []
    for _ in range(trials):
        raw_secs.append(run_raw_trial(envs, ticks, substeps))
        with tempfile.TemporaryDirectory() as tmp:
            ledger_records.append(
                await run_ledger_trial(envs, ticks, substeps, Path(tmp) / "store")
            )

    raw_sec = median(raw_secs)
    ledger_sec = median(r["run_sec"] for r in ledger_records)
    physics_steps = envs * ticks * substeps
    return {
        "envs": envs,
        "ticks": ticks,
        "substeps": substeps,
        "trials": trials,
        "raw_sec": raw_sec,
        "ledger_run_sec": ledger_sec,
        "ledger_setup_sec": median(r["setup_sec"] for r in ledger_records),
        "overhead_per_tick_ms": (ledger_sec - raw_sec) / ticks * 1000.0,
        "raw_steps_per_sec": physics_steps / raw_sec,
        "ledger_steps_per_sec": physics_steps / ledger_sec,
        "ledger_overhead_factor": ledger_sec / raw_sec,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="Benchmark case as ENVSxTICKS, e.g. 64x20. Repeatable.",
    )
    parser.add_argument("--substeps", type=int, default=DEFAULT_SUBSTEPS)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--out", default=None)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    cases = []
    for value in args.case or ["1x20", "64x20", "256x20"]:
        envs, ticks = value.split("x", 1)
        cases.append((int(envs), int(ticks)))

    results = [
        await run_case(envs, ticks, args.substeps, args.trials, args.warmups)
        for envs, ticks in cases
    ]

    payload = json.dumps(results, indent=2, sort_keys=True)
    if args.out:
        path = Path(args.out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload)
    else:
        print(payload)


if __name__ == "__main__":
    asyncio.run(main())
