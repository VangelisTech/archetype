# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Control Sander Mertens' actual Biome game with an Archetype agent.

The Biome process owns its real prefab library and simulation systems.
Archetype owns the goal, policy decision, and append-only execution evidence.

Run an already-started Biome process:
    uv run python examples/14_biome_agent.py --require-live

Prepare, launch, act, verify, and keep the game open:
    uv run python examples/14_biome_agent.py --launch --keep-open
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from biome_agent import (
    BiomeClient,
    ExtractionGoal,
    FlecsRemoteError,
    run_durable_episode,
    wait_until_ready,
)
from biome_agent.bootstrap import (
    BIOME_REVISION,
    BIOME_URL,
    DEFAULT_CHECKOUT_ROOT,
    FLECS_REVISION,
    launch,
    prepare,
    terminate,
)

from archetype import StorageConfig


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=BIOME_URL)
    parser.add_argument("--resource", default="Copper")
    parser.add_argument("--amount", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--require-live", action="store_true")
    parser.add_argument(
        "--launch",
        action="store_true",
        help="clone/build the pinned upstream repositories and launch Biome",
    )
    parser.add_argument(
        "--keep-open",
        action="store_true",
        help="leave a process started by --launch running after the mission",
    )
    parser.add_argument("--checkout-root", type=Path, default=DEFAULT_CHECKOUT_ROOT)
    parser.add_argument("--jobs", type=int)
    parser.add_argument("--storage-uri", default="./archetype_data")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    process = None
    launched_from_pins = False

    try:
        if args.launch:
            if args.url.rstrip("/") != BIOME_URL:
                raise ValueError(f"--launch owns the exact local endpoint {BIOME_URL}")
            print("Preparing pinned upstream Biome and Flecs revisions...")
            print(
                "WARNING: upstream Flecs REST binds 0.0.0.0:27750 without authentication; "
                "run only on a trusted local network."
            )
            checkout = prepare(args.checkout_root, jobs=args.jobs)
            process = launch(checkout)
            launched_from_pins = True

        with BiomeClient(args.url) as client:
            if not wait_until_ready(client, process, timeout=30.0 if process else 0.5):
                message = (
                    "Biome is not listening at "
                    f"{args.url}. Run with --launch, or start the pinned game separately."
                )
                if args.require_live or args.launch:
                    print(message, file=sys.stderr)
                    return 2
                print(f"SKIP: {message}")
                return 0

            goal = ExtractionGoal(resource=args.resource, amount=args.amount)
            storage = StorageConfig(uri=args.storage_uri, namespace="biome_agent")
            result = run_durable_episode(
                client,
                goal,
                storage=storage,
                biome_revision=BIOME_REVISION if launched_from_pins else "external-unverified",
                flecs_revision=FLECS_REVISION if launched_from_pins else "external-unverified",
                timeout=args.timeout,
                poll_interval=args.poll_interval,
            )
            trace = result.trace
            plan = trace.plan
            final_sample = trace.final_sample
            drill = final_sample.drill if final_sample else None
            print(f"Goal: extract {goal.amount} {goal.resource}")
            print(
                "Decision:",
                f"{plan.action.target_path} -> {plan.action.drill_path}",
                f"powered by {plan.action.power_path}",
            )
            print(
                "Native result:",
                f"extracted={trace.extracted}",
                f"powered={drill.powered if drill else False}",
                f"stored={drill.stored_amount if drill else 0}",
            )
            print(
                "Archetype evidence:",
                f"world_id={result.world_id}",
                f"run_id={result.run_id}",
                f"committed_tick={result.committed_tick}",
            )
            print(trace.reason)
            return 0 if trace.success else 1
    except (FlecsRemoteError, LookupError, RuntimeError, ValueError) as exc:
        print(f"Biome mission failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if process is not None:
            if args.keep_open and process.poll() is None:
                print(f"Biome remains open (pid={process.pid}).")
            else:
                terminate(process)


if __name__ == "__main__":
    raise SystemExit(main())
