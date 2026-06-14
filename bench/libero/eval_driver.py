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

    return ModalEnvClient(suite=suite, task_id=task_id, with_frames=with_frames)


def _make_libero_plus_env_client(suite: str, task_id: int, with_frames: bool = False) -> Any:
    """Build a LiberoPlusEnvClient for the LANGUAGE-perturbation dimension.

    Drop-in for ``ModalEnvClient`` (same reset/step/task_language surface); here
    ``task_id`` is the *ordinal within the suite's Language Instructions list*,
    and ``task_language()`` returns the PERTURBED instruction — the honest
    LIBERO-Plus baseline. Deferred import so the scripted/standard paths never
    import the LIBERO-Plus worker module.
    """
    from libero_plus_worker import (
        LiberoPlusEnvClient,  # type: ignore[import-untyped]  # noqa: PLC0415
    )

    return LiberoPlusEnvClient(suite=suite, task_id=task_id, with_frames=with_frames)


def _resolve_instruction(
    *,
    suite: str,
    task_id: int,
    env_client: Any,
    override_map: dict[tuple[str, int], str] | None,
) -> str:
    """Resolve the conditioning instruction for one (suite, task_id).

    Fairness rule (core-loop §2): the baseline is the *raw* LIBERO instruction,
    never the empty string.  Resolution order:

    1. If ``override_map`` supplies a paraphrase for ``(suite, task_id)``, use it.
       (This is the GEPA hook — a strategy can swap the conditioning text later.)
    2. Otherwise, default to the env's native ``task_language()`` (the canonical
       LIBERO instruction for the task), queried over the EnvClient boundary.
    3. If neither is available (e.g. a scripted env with no ``task_language``),
       fall back to the empty string — scripted reach envs ignore the
       instruction entirely, so this only ever happens off the real-policy path.

    The resolved string flows through ``ManipTask.instruction`` →
    ``PolicyActionProcessor`` → ``VlaJepaPolicyClient.infer_refs(instruction=...)``.
    """
    if override_map is not None:
        override = override_map.get((suite, task_id))
        if override:
            return override
    task_language = getattr(env_client, "task_language", None)
    if callable(task_language):
        try:
            return str(task_language())
        except Exception:  # noqa: BLE001 - scripted/test envs may not implement it
            return ""
    return ""


def _make_vla_policy_client(suite: str, task_id: int) -> Any:
    """Build a VlaJepaPolicyClient; deferred so scripted path avoids Modal."""
    from archetype.experiments.policy import VlaJepaPolicyClient  # noqa: PLC0415

    return VlaJepaPolicyClient(suite=suite, task_id=task_id)


def _make_scripted_env(task_id: int) -> ScriptedReachEnv:
    """Build the in-process scripted env for CI / unit-test paths."""
    # One fixed target per task_id — enough to make success deterministic.
    target: tuple[float, float, float] = (0.5 + 0.05 * task_id, 0.0, 0.5)
    return ScriptedReachEnv(targets={i: target for i in range(64)}, tolerance=0.5)


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
    env_client_type: str = "modal"  # "modal", "modal_plus" (LIBERO-Plus LANGUAGE), or "scripted"
    use_policy: bool = True  # False → zero-action policy (scripted path)
    # Optional per-(suite, task_id) instruction override map. When a key is
    # present, its paraphrase replaces the env's native task_language() for that
    # task — the GEPA hook (core-loop §3). Absent keys fall back to the raw
    # LIBERO instruction (the honest baseline, core-loop §2).
    instruction_overrides: dict[tuple[str, int], str] = field(default_factory=dict)


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
    instruction: str = "",
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

    spawn_action = [0.0] * ACTION_DIM
    spawn_components: list[Any] = [
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
        spawn_components.append(
            ManipFrameRef(
                agentview_ref=str(obs.get("agentview_ref", "")),
                wrist_ref=str(obs.get("wrist_ref", "")),
            )
        )
    await world.spawn(*spawn_components)

    # Run until done or max_steps.  We step one tick at a time so we can
    # break early when all entities are done (single entity here).
    from daft import col as _col  # noqa: PLC0415

    success = False
    episode_length = 0
    for _tick in range(max_steps):
        # Reset per-tick counter before each step so the quota window aligns
        # with one episode control step rather than the entire episode duration.
        reset_tick_counters()
        await world.step()
        info = await world.info()
        # Filter to latest tick (tick = info.tick - 1 after step).
        latest_tick = info.tick - 1
        df = await world.query(ManipStatus)
        # lazy_audit: single-row done-check at episode-loop boundary — the Python bool
        # drives the break decision; cannot remain lazy.
        rows = df.where(_col("tick") == latest_tick).to_pylist()
        if not rows:
            break
        row = rows[0]
        episode_length = int(row.get("manipstatus__env_step", _tick + 1))
        if row.get("manipstatus__done", False):
            success = bool(row.get("manipstatus__success", False))
            break

    await world.destroy()
    return success, episode_length


async def run_episode_batched(
    *,
    env_client: Any,
    policy_client: Any | None,
    suite: str,
    task_id: int,
    seeds: list[int],
    env_keys: list[int],
    max_steps: int,
    storage: StorageConfig,
    runtime: ArchetypeRuntime,
    use_frames: bool = False,
    instruction: str = "",
) -> list[tuple[bool, int]]:
    """Drive ALL trials of one task as N entities in ONE world — batched.

    The proven processors (``PolicyActionProcessor`` / ``FramedEnvStepProcessor``)
    already issue one ``env.step([live env_keys], [live actions])`` per tick over
    the whole DataFrame, so N seeds advance together each tick instead of in N
    serial single-entity worlds. Within an episode the tick sequence stays coupled
    (step t+1 needs t); across seeds everything is data → one batched tick.

    Returns one ``(success, episode_length)`` per seed, ordered by ``env_key``
    (== trial index). Success latches, so the per-entity ``max`` over its rows is
    the verdict even for entities that finished early (mirrors ``monitor.py``).
    """
    import daft  # noqa: PLC0415
    from daft import col as _col  # noqa: PLC0415

    reset_tick_counters()
    # Per-env reset (one-time); the worker holds N MuJoCo envs keyed by env_id.
    obs_list = [env_client.reset(env_keys[i], seeds[i]) for i in range(len(seeds))]

    processors: list[Any] = []
    if policy_client is not None:
        from archetype.experiments.policy import PolicyActionProcessor  # noqa: PLC0415

        processors.append(PolicyActionProcessor(policy_client))
    processors.append(
        FramedEnvStepProcessor(env_client) if use_frames else EnvStepProcessor(env_client)
    )

    world = runtime.world(
        f"ep-{suite}-t{task_id}-batch{len(seeds)}",
        storage=storage,
        processors=processors,
    )

    spawn_action = [0.0] * ACTION_DIM
    for i, obs in enumerate(obs_list):
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
                seed=seeds[i],
                env_key=env_keys[i],
            ),
        ]
        if use_frames:
            components.append(
                ManipFrameRef(
                    agentview_ref=str(obs.get("agentview_ref", "")),
                    wrist_ref=str(obs.get("wrist_ref", "")),
                )
            )
        await world.spawn(*components)

    # Service-layer orchestration: delegate the episode loop to
    # SimulationService.run_episode (gate.run_episode), which owns the per-tick
    # quota reset and steps to max_steps through the command broker — no
    # hand-rolled tick loop. Done entities are skipped by the step processor, so
    # spinning to max_steps costs no extra env calls. The terminal verdict is
    # read from the ledger below (success latches → max).
    from archetype.app.models import EpisodeConfig  # noqa: PLC0415

    await world.run_episode(EpisodeConfig(max_steps=max_steps))

    # Per-entity terminal verdict (success latches → max), joined to env_key
    # (constant per entity) so results come back in trial order.
    status = (
        (await world.query(ManipStatus))
        .groupby("entity_id")
        .agg(
            _col("manipstatus__success").cast(daft.DataType.int64()).max().alias("success"),
            _col("manipstatus__env_step").max().alias("steps"),
        )
        .to_pylist()
    )
    tasks = (
        (await world.query(ManipTask))
        .groupby("entity_id")
        .agg(_col("maniptask__env_key").max().alias("env_key"))
        .to_pylist()
    )
    await world.destroy()

    key_by_entity = {r["entity_id"]: int(r["env_key"]) for r in tasks}
    by_env_key: dict[int, tuple[bool, int]] = {}
    for r in status:
        ek = key_by_entity.get(r["entity_id"])
        if ek is not None:
            by_env_key[ek] = (bool(r["success"]), int(r["steps"] or 0))
    return [by_env_key.get(ek, (False, 0)) for ek in env_keys]


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
                if config.env_client_type == "modal_plus":
                    env_client = _make_libero_plus_env_client(
                        config.suite, task_id, with_frames=use_frames
                    )
                else:
                    env_client = _make_modal_env_client(
                        config.suite, task_id, with_frames=use_frames
                    )
                policy_client = (
                    _make_vla_policy_client(config.suite, task_id) if config.use_policy else None
                )

            # Resolve the conditioning instruction once per task: paraphrase
            # override if supplied, else the env's native task_language() (the
            # honest baseline). Empty string only on the scripted path.
            instruction = _resolve_instruction(
                suite=config.suite,
                task_id=task_id,
                env_client=env_client,
                override_map=config.instruction_overrides,
            )
            print(f"  [{config.suite}] task={task_id} instruction={instruction!r}")

            # Batch ALL trials of this task into ONE world: N seed-entities
            # stepped by a single batched env.step per tick — not N serial
            # single-entity episode worlds. Across-task parallelism stays at the
            # .starmap layer (one Function/container per task).
            seeds = [task_id * 1000 + i for i in range(config.trials)]
            env_keys = list(range(config.trials))

            t0 = time.perf_counter()
            per_trial = await run_episode_batched(
                env_client=env_client,
                policy_client=policy_client,
                suite=config.suite,
                task_id=task_id,
                seeds=seeds,
                env_keys=env_keys,
                max_steps=config.max_steps,
                storage=ep_storage,
                runtime=runtime,
                use_frames=use_frames,
                instruction=instruction,
            )
            wall_total = time.perf_counter() - t0
            wall_each = wall_total / max(1, len(per_trial))

            for trial_idx, (success, episode_length) in enumerate(per_trial):
                seed = seeds[trial_idx]
                env_key = env_keys[trial_idx]
                trial_result = EvalTrialResult.make(
                    suite=config.suite,
                    task_id=task_id,
                    trial_idx=trial_idx,
                    seed=seed,
                    env_key=env_key,
                    success=success,
                    episode_length=episode_length,
                    wall_s=wall_each,
                    run_id=run_id,
                )
                results.append(trial_result)

                # Append to lab world; step to persist. Reset quota counter to
                # avoid the 500-command-per-tick limit from accumulated ops.
                reset_tick_counters()
                await lab.spawn(trial_result)
                await lab.step()

                print(
                    f"  [{config.suite}] task={task_id} trial={trial_idx} "
                    f"success={success} steps={episode_length} "
                    f"seed={seed} (batched task wall={wall_total:.1f}s)"
                )

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
        choices=["modal", "modal_plus", "scripted"],
        default="modal",
        help=(
            "'modal' uses the deployed standard-LIBERO env; 'modal_plus' uses the "
            "LIBERO-Plus LANGUAGE-perturbation env (task-id = language ordinal); "
            "'scripted' uses the in-process scripted env"
        ),
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
