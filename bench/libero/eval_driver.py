# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""LIBERO eval driver — suites × tasks × trials on the Archetype ledger.

Orchestrates LIBERO benchmark episodes following the experiments vocabulary:

- One ``Experiment`` entity per run at tick-0 of the lab world (genesis
  at the initial-conditions tick, processors first apply at tick 1).
- One ``Run`` entity per (suite, task_id) batch, also at tick-0.
- One ``EvalTrialResult`` entity per completed trial appended after the
  episode episode world completes.

The driver is designed for **CI-safe scripted use** (no Modal, no GPU)
and **smoke/live use** (real Modal env + VLA-JEPA workers).  In CI, pass
``--env-client scripted`` to drive with ``ScriptedReachEnv``; in production
use the default Modal env client.

Live deployment prereqs:
    modal deploy bench/libero/modal_worker.py
    modal deploy bench/libero/vla_jepa_worker.py

Usage (smoke — 1 task, 2 trials, max 80 steps):
    uv run python bench/libero/eval_driver.py \\
        --suite libero_spatial --task-ids 0 \\
        --trials 2 --max-steps 80 \\
        --out /tmp/libero_eval

Usage (full benchmark — USER TRIGGERED, GPU-cost):
    uv run python bench/libero/eval_driver.py \\
        --suite libero_spatial \\
        --trials 50 --max-steps 600 \\
        --out /tmp/libero_eval_full

Protocol:
    For each (suite, task_id):
      For each trial in range(trials):
        seed  = task_id * 1000 + trial_idx
        env_key = trial_idx          # per-trial env instance
        1. env.reset(env_key, seed)  → obs (raw tick-0 initial conditions)
        2. spawn episode entity in episode world
        3. run episode world until done or max_steps ticks
        4. query ledger for final ManipStatus row
        5. record EvalTrialResult in lab world
    After all trials: step lab world once (persist)

Lab-world genesis (mirrors autoresearch_service._attach_ledger):
    tick 0: Experiment + Run entities as initial conditions (raw, processors
            first apply at tick 1); step once to persist genesis.
    tick 1+: EvalTrialResult entities appended; step after each trial.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")

# Allow ``bench/libero`` helpers to be imported without package install.
sys.path.insert(0, str(Path(__file__).parent))

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.app.auth.guard import reset_tick_counters  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.experiments.components import (  # noqa: E402
    EvalTrialResult,
    Experiment,
    Run,
    RunStatus,
)
from archetype.experiments.manipulation import (  # noqa: E402
    ACTION_DIM,
    EnvStepProcessor,
    FramedEnvStepProcessor,
    ManipAction,
    ManipFrameRef,
    ManipProprio,
    ManipStatus,
    ManipTask,
    ScriptedReachEnv,
)

# ---------------------------------------------------------------------------
# Policy client imports — lazy to keep scripted path Modal-free
# ---------------------------------------------------------------------------


def _make_modal_env_client(suite: str, task_id: int, with_frames: bool = False) -> Any:
    """Build a ModalEnvClient; deferred so scripted path avoids the import.

    ``with_frames`` must be True whenever the VLA policy is in the loop: that
    policy consumes ``agentview_ref``/``wrist_ref`` from the ledger, which only
    exist when the env returns frame refs and the framed step processor carries
    them.
    """
    from modal_worker import ModalEnvClient  # type: ignore[import-untyped]  # noqa: PLC0415

    frame_commit_every = 7 if with_frames else 1
    return ModalEnvClient(
        suite=suite,
        task_id=task_id,
        with_frames=with_frames,
        frame_commit_every=frame_commit_every,
    )


def _make_vla_policy_client(suite: str, task_id: int) -> Any:
    """Build a VlaJepaPolicyClient; deferred so scripted path avoids Modal."""
    from archetype.experiments.policy import VlaJepaPolicyClient  # noqa: PLC0415

    return VlaJepaPolicyClient(suite=suite, task_id=task_id)


def _make_scripted_env(task_id: int) -> ScriptedReachEnv:
    """Build the in-process scripted env for CI / unit-test paths."""
    # One fixed target per task_id — enough to make success deterministic.
    target: tuple[float, float, float] = (0.5 + 0.05 * task_id, 0.0, 0.5)
    return ScriptedReachEnv(targets={i: target for i in range(64)}, tolerance=0.5)


def _task_language(env_client: Any) -> str:
    """Return the env's task instruction when the client exposes it."""
    task_language = getattr(env_client, "task_language", None)
    if callable(task_language):
        return str(task_language())
    return ""


async def _task_language_async(env_client: Any) -> str:
    """Return task language via an async client hook when available."""
    task_language_async = getattr(env_client, "task_language_async", None)
    if callable(task_language_async):
        return str(await task_language_async())
    return _task_language(env_client)


async def _reset_many_async(
    env_client: Any,
    env_keys: list[int],
    seeds: list[int],
) -> list[dict[str, Any]]:
    """Reset many env instances through a batched async hook when available."""
    reset_many_async = getattr(env_client, "reset_many_async", None)
    if callable(reset_many_async):
        return list(await reset_many_async(env_keys, seeds))
    return [env_client.reset(env_key, seed) for env_key, seed in zip(env_keys, seeds, strict=True)]


async def _latest_status_rows(world: Any, *, use_frames: bool) -> list[dict[str, Any]]:
    """Read only the latest ManipStatus tick for the episode world."""
    info = await world.info()
    latest_tick = info.tick - 1
    sig = (
        (ManipAction, ManipFrameRef, ManipProprio, ManipStatus, ManipTask)
        if use_frames
        else (ManipAction, ManipProprio, ManipStatus, ManipTask)
    )
    df = await world._gate.query_archetype(  # noqa: SLF001 - bench hot-path escape hatch
        world._ctx,  # noqa: SLF001
        sig,
        str(info.world_id),
        str(info.run_id or ""),
        storage_config=world._state.storage_config,  # noqa: SLF001
        ticks=[latest_tick],
        components=[ManipStatus],
    )
    return df.to_pylist()


def _episode_components(
    *,
    obs: dict[str, Any],
    suite: str,
    task_id: int,
    instruction: str,
    seed: int,
    env_key: int,
    use_frames: bool,
) -> list[Any]:
    """Build the initial components for one manipulation episode entity."""
    spawn_action = [0.0] * ACTION_DIM
    components: list[Any] = [
        ManipProprio(
            eef_pos=list(obs.get("eef_pos", [0.0, 0.0, 0.0])),
            eef_quat=list(obs.get("eef_quat", [1.0, 0.0, 0.0, 0.0])),
            gripper=float(obs.get("gripper", 0.0)),
            gripper_qpos=list(obs.get("gripper_qpos", [0.0, 0.0])),
        ),
        ManipAction(values=list(spawn_action)),
        ManipStatus(),
        ManipTask(
            suite=suite,
            task_id=task_id,
            instruction=instruction,
            seed=seed,
            env_key=env_key,
        ),
    ]
    if use_frames:
        components.append(
            ManipFrameRef(
                agentview_ref=str(obs.get("agentview_ref", "")),
                wrist_ref=str(obs.get("wrist_ref", "")),
            )
        )
    return components


def _flush_frames(env_client: Any, *, use_frames: bool) -> None:
    """Commit deferred frame sidecars if the env client supports it."""
    if not use_frames:
        return
    commit_frames = getattr(env_client, "commit_frames", None)
    if callable(commit_frames):
        commit_frames()


async def _flush_frames_async(env_client: Any, *, use_frames: bool) -> None:
    """Commit deferred frame sidecars through async hook when available."""
    if not use_frames:
        return
    commit_frames_async = getattr(env_client, "commit_frames_async", None)
    if callable(commit_frames_async):
        await commit_frames_async()
        return
    _flush_frames(env_client, use_frames=use_frames)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass
class DriverConfig:
    """Configuration for one eval driver run."""

    suite: str = "libero_spatial"
    task_ids: list[int] = field(default_factory=lambda: [0])
    trials: int = 3
    max_steps: int = 600
    out: str = ""  # output directory for lab-world parquet
    run_id: str = ""  # free-form run label
    env_client_type: str = "modal"  # "modal" or "scripted"
    use_policy: bool = True  # False → zero-action policy (scripted path)


# ---------------------------------------------------------------------------
# Episode driver
# ---------------------------------------------------------------------------


async def run_episode(
    *,
    env_client: Any,
    policy_client: Any | None,
    suite: str,
    task_id: int,
    trial_idx: int,
    seed: int,
    env_key: int,
    max_steps: int,
    storage: StorageConfig,
    runtime: ArchetypeRuntime,
    use_frames: bool = False,
) -> tuple[bool, int]:
    """Drive one episode on an isolated world.

    Returns (success, episode_length_steps).

    The episode world has no lab-world processors — only the env step (and
    optionally a policy) processor.  The lab world carries the experiment
    metadata; episode worlds are ephemeral and destroyed after each trial.

    Initial-conditions contract:
        - env.reset() obs lands as the raw tick-0 row (x_0 is given).
        - Processors (EnvStepProcessor) first apply at tick 1.
        - ManipStatus.env_step reaches max_steps at the latest at tick max_steps.
    """
    # Reset per-tick RBAC quota counter at the start of each episode.
    # Each episode world is an independent execution context; the quota
    # accumulates across command-service calls in the same process and must
    # be reset between episodes to avoid hitting the 500-command per-tick limit.
    reset_tick_counters()

    obs = env_client.reset(env_key, seed)
    instruction = await _task_language_async(env_client)

    processors: list[Any] = []
    if policy_client is not None:
        from archetype.experiments.policy import PolicyActionProcessor  # noqa: PLC0415

        processors.append(PolicyActionProcessor(policy_client))
    # The VLA policy consumes frame refs; carry them with the framed step
    # processor and a ManipFrameRef on the archetype so the policy's ref columns
    # exist from tick 0 (reset refs land raw, like every other initial value).
    processors.append(
        FramedEnvStepProcessor(env_client) if use_frames else EnvStepProcessor(env_client)
    )

    world = runtime.world(
        f"ep-{suite}-t{task_id}-trial{trial_idx}",
        storage=storage,
        processors=processors,
    )

    await world.spawn(
        *_episode_components(
            obs=obs,
            suite=suite,
            task_id=task_id,
            instruction=instruction,
            seed=seed,
            env_key=env_key,
            use_frames=use_frames,
        )
    )

    # Run until done or max_steps.  We step one tick at a time so we can
    # break early when all entities are done (single entity here).
    success = False
    episode_length = 0
    for _tick in range(max_steps):
        # Reset per-tick counter before each step so the quota window aligns
        # with one episode control step rather than the entire episode duration.
        reset_tick_counters()
        await world.step()
        # lazy_audit: single-row done-check at episode-loop boundary. Query
        # only the latest archetype tick; RuntimeWorld.query() returns the
        # full append-only history, which is quadratic inside long rollouts.
        rows = await _latest_status_rows(world, use_frames=use_frames)
        if not rows:
            break
        row = rows[0]
        episode_length = int(row.get("manipstatus__env_step", _tick + 1))
        if row.get("manipstatus__done", False):
            success = bool(row.get("manipstatus__success", False))
            break

    _flush_frames(env_client, use_frames=use_frames)
    await world.destroy()
    return success, episode_length


async def run_task_batch(
    *,
    env_client: Any,
    policy_client: Any | None,
    suite: str,
    task_id: int,
    trials: int,
    max_steps: int,
    storage: StorageConfig,
    runtime: ArchetypeRuntime,
    use_frames: bool = False,
) -> list[tuple[int, int, int, int, bool, int, float]]:
    """Drive all trials for one task in one batched episode world.

    Returns tuples of ``(trial_idx, seed, env_key, entity_id, success,
    episode_length, wall_s)``.
    """
    reset_tick_counters()
    instruction = await _task_language_async(env_client)

    processors: list[Any] = []
    if policy_client is not None:
        from archetype.experiments.policy import PolicyActionProcessor  # noqa: PLC0415

        processors.append(PolicyActionProcessor(policy_client))
    processors.append(
        FramedEnvStepProcessor(env_client) if use_frames else EnvStepProcessor(env_client)
    )

    world = runtime.world(
        f"ep-{suite}-t{task_id}-batch",
        storage=storage,
        processors=processors,
    )

    trial_meta: list[tuple[int, int, int]] = []
    entities: list[list[Any]] = []
    env_keys: list[int] = []
    seeds: list[int] = []
    for trial_idx in range(trials):
        seed = task_id * 1000 + trial_idx
        env_key = trial_idx
        trial_meta.append((trial_idx, seed, env_key))
        env_keys.append(env_key)
        seeds.append(seed)

    reset_observations = await _reset_many_async(env_client, env_keys, seeds)
    for (_trial_idx, seed, env_key), obs in zip(
        trial_meta,
        reset_observations,
        strict=True,
    ):
        entities.append(
            _episode_components(
                obs=obs,
                suite=suite,
                task_id=task_id,
                instruction=instruction,
                seed=seed,
                env_key=env_key,
                use_frames=use_frames,
            )
        )

    entity_ids = await world.spawn_many(entities)
    started_at = {entity_id: time.perf_counter() for entity_id in entity_ids}
    latest_by_entity: dict[int, dict[str, Any]] = {}
    finished: set[int] = set()

    for _tick in range(max_steps):
        reset_tick_counters()
        await world.step()
        rows = await _latest_status_rows(world, use_frames=use_frames)
        for row in rows:
            entity_id = int(row["entity_id"])
            latest_by_entity[entity_id] = row
            if row.get("manipstatus__done", False):
                finished.add(entity_id)
        if len(finished) == len(entity_ids):
            break

    await _flush_frames_async(env_client, use_frames=use_frames)

    out: list[tuple[int, int, int, int, bool, int, float]] = []
    now = time.perf_counter()
    for (trial_idx, seed, env_key), entity_id in zip(trial_meta, entity_ids, strict=True):
        row = latest_by_entity.get(entity_id, {})
        out.append(
            (
                trial_idx,
                seed,
                env_key,
                entity_id,
                bool(row.get("manipstatus__success", False)),
                int(row.get("manipstatus__env_step", 0)),
                now - started_at[entity_id],
            )
        )

    await world.destroy()
    return out


# ---------------------------------------------------------------------------
# Top-level driver
# ---------------------------------------------------------------------------


async def run_eval(config: DriverConfig) -> list[EvalTrialResult]:
    """Run the full eval: suites × tasks × trials → EvalTrialResult rows.

    Records results into a lab world following the genesis-at-tick-0 pattern.
    Returns the list of EvalTrialResult component instances for callers
    (the report generator reads from the lab-world parquet; the list is
    a convenience return for tests).
    """
    run_id = config.run_id or f"eval-{config.suite}-{int(time.time())}"
    out_dir = config.out or tempfile.mkdtemp(prefix="libero_eval_")
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    lab_storage = StorageConfig(
        uri=str(Path(out_dir) / "lab"),
        namespace=f"eval_{config.suite}",
    )
    # Episode worlds each get their own temp store so they don't pollute the lab.
    ep_storage = StorageConfig(
        uri=str(Path(out_dir) / "episodes"),
        namespace=f"episodes_{config.suite}",
    )

    results: list[EvalTrialResult] = []

    async with ArchetypeRuntime() as runtime:
        lab = runtime.world("eval-lab", storage=lab_storage)

        # Genesis: Experiment + one Run entity per suite/task combination.
        # Following the initial-conditions contract: entities spawned here land
        # as raw tick-0 rows; processors first apply at tick 1.
        await lab.spawn(
            Experiment.make(
                name=run_id,
                repo_url="",
                metadata={
                    "suite": config.suite,
                    "task_ids": config.task_ids,
                    "trials": config.trials,
                    "max_steps": config.max_steps,
                },
            )
        )
        for task_id in config.task_ids:
            await lab.spawn(
                Run(
                    run_id=f"{run_id}:task{task_id}",
                    experiment_name=run_id,
                    status=RunStatus.RUNNING.value,
                    task=f"{config.suite}:task{task_id}",
                )
            )
        # Step once: genesis tick lands initial conditions.
        await lab.step()

        for task_id in config.task_ids:
            # Build per-task clients (Modal creates per-task containers).
            # The VLA policy on the Modal env needs frame refs on the ledger;
            # the scripted env and the no-policy path do not.
            use_frames = config.env_client_type != "scripted" and config.use_policy
            if config.env_client_type == "scripted":
                env_client = _make_scripted_env(task_id)
                policy_client = None
            else:
                env_client = _make_modal_env_client(config.suite, task_id, with_frames=use_frames)
                policy_client = (
                    _make_vla_policy_client(config.suite, task_id) if config.use_policy else None
                )

            batch_results = await run_task_batch(
                env_client=env_client,
                policy_client=policy_client,
                suite=config.suite,
                task_id=task_id,
                trials=config.trials,
                max_steps=config.max_steps,
                storage=ep_storage,
                runtime=runtime,
                use_frames=use_frames,
            )

            trial_results: list[EvalTrialResult] = []
            for (
                trial_idx,
                seed,
                env_key,
                _entity_id,
                success,
                episode_length,
                wall_s,
            ) in batch_results:
                trial_result = EvalTrialResult.make(
                    suite=config.suite,
                    task_id=task_id,
                    trial_idx=trial_idx,
                    seed=seed,
                    env_key=env_key,
                    success=success,
                    episode_length=episode_length,
                    wall_s=wall_s,
                    run_id=run_id,
                )
                results.append(trial_result)
                trial_results.append(trial_result)

                print(
                    f"  [{config.suite}] task={task_id} trial={trial_idx} "
                    f"success={success} steps={episode_length} "
                    f"wall={wall_s:.1f}s seed={seed}"
                )

            # Append task result rows as one batch; step once to persist.
            reset_tick_counters()
            await lab.spawn_many([[trial_result] for trial_result in trial_results])
            await lab.step()

        # Mark runs as stopped.
        print(f"Eval complete. Lab world at {out_dir!r}")

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> DriverConfig:
    parser = argparse.ArgumentParser(description="LIBERO eval driver")
    parser.add_argument("--suite", default="libero_spatial", help="LIBERO suite name")
    parser.add_argument(
        "--task-ids",
        type=lambda s: [int(x) for x in s.split(",")],
        default=None,
        help="Comma-separated task IDs (default: all tasks for suite, or just 0 if unknown)",
    )
    parser.add_argument("--trials", type=int, default=3, help="Trials per task")
    parser.add_argument("--max-steps", type=int, default=600, help="Max steps per episode")
    parser.add_argument("--out", default="", help="Output directory for lab-world parquet")
    parser.add_argument("--run-id", default="", help="Run identifier label")
    parser.add_argument(
        "--env-client",
        choices=["modal", "scripted"],
        default="modal",
        help="'modal' uses the deployed Modal env; 'scripted' uses the in-process scripted env",
    )
    parser.add_argument(
        "--no-policy",
        action="store_true",
        help="Disable VLA-JEPA policy (use zero-action policy instead)",
    )
    args = parser.parse_args()

    # Default suite task lists (approximate; LIBERO has 10 tasks per suite).
    all_tasks = list(range(10))
    task_ids = args.task_ids if args.task_ids is not None else all_tasks

    return DriverConfig(
        suite=args.suite,
        task_ids=task_ids,
        trials=args.trials,
        max_steps=args.max_steps,
        out=args.out,
        run_id=args.run_id,
        env_client_type=args.env_client,
        use_policy=not args.no_policy,
    )


if __name__ == "__main__":
    config = _parse_args()
    print(
        f"Eval driver: suite={config.suite} tasks={config.task_ids} "
        f"trials={config.trials} max_steps={config.max_steps}"
    )
    asyncio.run(run_eval(config))
