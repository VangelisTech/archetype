# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Batched control-plane LIBERO eval — the clean replacement for eval_driver.

What the old ``eval_driver.run_episode`` got wrong (and why the paper numbers
were untrustworthy):

- one **ephemeral world per trial**, ``destroy()``d after grading → the raw
  ``ManipStatus`` rows were orphaned under a ``(world_id, run_id)`` nobody
  recorded, so the eval service could never re-grade them (the A2 blocker);
- a **hand-rolled episode loop** with a per-tick ``ManipStatus`` materialization
  to decide termination — reinventing what ``run_episode`` owns;
- **manual ``reset_tick_counters()``** to paper over the RBAC quota never
  resetting (bug B1);
- ``instruction=""`` handed to the policy (failure mode 4d).

This module does it the Archetype-native way:

- **one control-plane world per task, N trial entities** keyed by ``env_key``.
  The env client batches by ``env_key``, so one tick steps all live trials at
  once; finished trials freeze on the ledger (``done`` latches). Every trial's
  trajectory is addressable by the single ``(world_id, run_id)`` and sliced by
  ``ManipTask`` — so the eval service grades it natively (A2 unblocked).
- termination is the **value-based "all entities done" contract** (B2:
  ``terminal_field="done"``), no per-tick hand-roll.
- the RBAC quota resets inside ``SimulationService.step`` (B1), no manual call.
- the **real instruction** is wired from ``env_client.task_language()`` (4d).
- success/length are **graded from raw ``ManipStatus``** by the eval service,
  not computed in the driver — so there is no ``EvalTrialResult`` summary to
  drift (E1: it is recomputable, hence legacy).

The env client is injected, so this is identical for the in-process LIBERO
client (``InProcessLiberoEnvClient``), the Modal client, or the scripted
contract env — the orchestration never learns which.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import uuid_utils as uuid

from archetype.app.models import EpisodeConfig
from archetype.core.config import StorageConfig, WorldConfig
from archetype.experiments.manipulation import (
    ACTION_DIM,
    EnvStepProcessor,
    FramedEnvStepProcessor,
    ManipAction,
    ManipFrameRef,
    ManipProprio,
    ManipStatus,
    ManipTask,
)


@dataclass
class TrialOutcome:
    trial_idx: int
    env_key: int
    seed: int
    success: bool
    episode_length: int


@dataclass
class TaskEvalReport:
    """Graded result for one task's batched trials — all derived from the
    persisted ledger, addressable by ``(world_id, run_id)``."""

    suite: str
    task_id: int
    instruction: str
    world_id: str
    run_id: str
    trials: list[TrialOutcome] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return (sum(t.success for t in self.trials) / len(self.trials)) if self.trials else 0.0

    @property
    def mean_length(self) -> float:
        return (
            (sum(t.episode_length for t in self.trials) / len(self.trials)) if self.trials else 0.0
        )


def _instruction_for(env_client: Any, fallback: str) -> str:
    """Real task language when the env exposes it (LIBERO), else a fallback."""
    getter = getattr(env_client, "task_language", None)
    if callable(getter):
        return str(getter())
    return fallback


async def run_task_eval(
    *,
    world_service: Any,
    simulation_service: Any,
    eval_service: Any,
    env_client: Any,
    policy_client: Any | None = None,
    suite: str,
    task_id: int,
    trials: int,
    max_steps: int,
    storage: StorageConfig,
    with_frames: bool = False,
    instruction: str = "reach",
) -> TaskEvalReport:
    """Run ``trials`` trials of one task in a single batched world and grade them.

    The control plane is one world; each trial is one entity carrying the
    manipulation archetype. The policy (priority 1) writes each tick's action
    from the previous observation; ``EnvStepProcessor`` (priority 10) consumes
    it. ``run_episode`` batch-steps them all until every trial's
    ``ManipStatus.done`` latches (or ``max_steps``), then the eval service
    grades the persisted rows.
    """
    # Unique per call: the registry enforces name uniqueness, so a retry / a
    # second variance run on the same (suite, task_id) would otherwise crash.
    world = await world_service.create_world(
        WorldConfig(name=f"eval:{suite}:t{task_id}:{uuid.uuid7()}"), storage
    )
    if policy_client is not None:
        from archetype.experiments.policy import PolicyActionProcessor  # noqa: PLC0415

        await world.add_processor(PolicyActionProcessor(policy_client))
    processor = FramedEnvStepProcessor(env_client) if with_frames else EnvStepProcessor(env_client)
    await world.add_processor(processor)

    task_instruction = _instruction_for(env_client, instruction)

    # Reset-then-spawn: each trial's reset obs lands as its raw tick-0 row.
    env_key_by_entity: dict[int, int] = {}
    seed_by_entity: dict[int, int] = {}
    for trial_idx in range(trials):
        env_key = trial_idx
        seed = task_id * 1000 + trial_idx
        obs = env_client.reset(env_key, seed)
        components: list[Any] = [
            ManipProprio(
                eef_pos=list(obs.get("eef_pos", [0.0, 0.0, 0.0])),
                eef_quat=list(obs.get("eef_quat", [1.0, 0.0, 0.0, 0.0])),
                gripper=float(obs.get("gripper", 0.0)),
                gripper_qpos=list(obs.get("gripper_qpos", [0.0, 0.0])),
            ),
            ManipAction(values=[0.0] * ACTION_DIM),
            ManipStatus(),
            ManipTask(
                suite=suite,
                task_id=task_id,
                instruction=task_instruction,
                seed=seed,
                env_key=env_key,
            ),
        ]
        if with_frames:
            components.append(
                ManipFrameRef(
                    agentview_ref=str(obs.get("agentview_ref", "")),
                    wrist_ref=str(obs.get("wrist_ref", "")),
                )
            )
        entity_id = await world.create_entity(components)
        env_key_by_entity[entity_id] = env_key
        seed_by_entity[entity_id] = seed

    # Batch-step all trials until every one is done (B2) or max_steps.
    episode = await simulation_service.run_episode(
        world.world_id,
        EpisodeConfig(
            max_steps=max_steps,
            terminal_component=ManipStatus,
            terminal_field="done",
            terminal_all=True,
        ),
    )

    # Grade from the persisted ledger — the eval service, not the driver.
    df = await eval_service.query_components(
        [ManipStatus, ManipTask],
        world_id=world.world_id,
        run_id=episode.run_id,
        storage_config=storage,
    )
    final = _final_row_per_entity(df)

    report = TaskEvalReport(
        suite=suite,
        task_id=task_id,
        instruction=task_instruction,
        world_id=str(world.world_id),
        run_id=str(episode.run_id),
    )
    for entity_id, env_key in sorted(env_key_by_entity.items(), key=lambda kv: kv[1]):
        row = final.get(entity_id, {})
        report.trials.append(
            TrialOutcome(
                trial_idx=env_key,
                env_key=env_key,
                seed=seed_by_entity[entity_id],
                success=bool(row.get("manipstatus__success", False)),
                episode_length=int(row.get("manipstatus__env_step", 0)),
            )
        )
    return report


def _final_row_per_entity(df: Any) -> dict[int, dict]:
    """Latest-tick row per entity (``success`` latched, ``env_step`` frozen).

    Terminal materialization at the grading boundary — the same place the eval
    graders collect; out of the lazy-execution contract's scope (analysis edge).
    """
    by_eid: dict[int, dict] = {}
    for r in df.collect().to_pylist():
        eid = r["entity_id"]
        if eid not in by_eid or r["tick"] > by_eid[eid]["tick"]:
            by_eid[eid] = r
    return by_eid
