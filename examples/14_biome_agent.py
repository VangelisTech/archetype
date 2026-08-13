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
import subprocess
import sys
import time
from pathlib import Path

from biome_agent import (
    BiomeAgentDecision,
    BiomeClient,
    BiomeEpisodeState,
    BiomeMission,
    BiomeMissionOutcome,
    ExtractionGoal,
    FlecsRemoteError,
    GoalDirectedDrillPolicy,
    monitor_mission,
    plan_mission,
)
from biome_agent.bootstrap import (
    BIOME_REVISION,
    DEFAULT_CHECKOUT_ROOT,
    FLECS_REVISION,
    launch,
    prepare,
)

from archetype import ArchetypeRuntime, StorageConfig


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:27750")
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


def _wait_until_ready(client: BiomeClient, process, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if client.is_ready():
            return True
        if process is not None and process.poll() is not None:
            return False
        time.sleep(0.1)
    return False


def _state_from_trace(trace) -> BiomeEpisodeState:
    sample = trace.final_sample
    drill = sample.drill if sample else None
    return BiomeEpisodeState(
        phase="succeeded" if trace.success else "failed",
        target_entity=trace.plan.action.target_path,
        deposit_amount=sample.deposit_amount if sample else trace.plan.target.amount,
        extracted=trace.extracted,
        drill_entity=trace.plan.action.drill_path,
        powered=drill.powered if drill else False,
        stored_amount=drill.stored_amount if drill else 0,
    )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    process = None
    launched_from_pins = False

    try:
        if args.launch:
            print("Preparing pinned upstream Biome and Flecs revisions...")
            print(
                "WARNING: upstream Flecs REST binds 0.0.0.0:27750 without authentication; "
                "run only on a trusted local network."
            )
            checkout = prepare(args.checkout_root, jobs=args.jobs)
            process = launch(checkout)
            launched_from_pins = True

        with BiomeClient(args.url) as client:
            if not _wait_until_ready(client, process, timeout=30.0 if process else 0.5):
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
            policy = GoalDirectedDrillPolicy()
            storage = StorageConfig(uri=args.storage_uri, namespace="biome_agent")

            with ArchetypeRuntime.sync() as runtime:
                world = runtime.world("live-biome-agent", storage=storage)
                episode = world.spawn(
                    BiomeMission(
                        environment_uri=args.url,
                        resource=goal.resource,
                        target_amount=goal.amount,
                        biome_revision=BIOME_REVISION
                        if launched_from_pins
                        else "external-unverified",
                        flecs_revision=FLECS_REVISION
                        if launched_from_pins
                        else "external-unverified",
                    ),
                    BiomeEpisodeState(),
                )
                world.step()

                plan = plan_mission(client, policy, goal)
                world.update(
                    episode,
                    BiomeEpisodeState(
                        phase="observed",
                        target_entity=plan.action.target_path,
                        deposit_amount=plan.target.amount,
                    ),
                )
                world.step()

                client.deploy(plan.action)
                world.add_components(
                    episode,
                    BiomeAgentDecision(
                        target_entity=plan.action.target_path,
                        drill_x=plan.action.drill_cell.x,
                        drill_y=plan.action.drill_cell.y,
                        power_x=plan.action.power_cell.x,
                        power_y=plan.action.power_cell.y,
                    ),
                )
                world.update(
                    episode,
                    BiomeEpisodeState(
                        phase="action_applied",
                        target_entity=plan.action.target_path,
                        deposit_amount=plan.target.amount,
                        drill_entity=plan.action.drill_path,
                    ),
                )
                world.step()

                trace = monitor_mission(
                    client,
                    plan,
                    timeout=args.timeout,
                    poll_interval=args.poll_interval,
                )
                final_sample = trace.final_sample
                elapsed = final_sample.elapsed_seconds if final_sample else 0.0
                world.update(episode, _state_from_trace(trace))
                world.add_components(
                    episode,
                    BiomeMissionOutcome(
                        success=trace.success,
                        extracted=trace.extracted,
                        reason=trace.reason,
                        elapsed_seconds=elapsed,
                    ),
                )
                world.step()
                info = world.info()

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
                    f"world_id={info.world_id}",
                    f"run_id={info.run_id}",
                    f"ticks={info.tick}",
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
            elif process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
