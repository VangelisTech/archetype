# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Archetype-shaped RoboSemanticBench batched rollout primitives.

The upstream RSB evaluator is intentionally serial: one task env, one model,
one episode loop. These helpers split the hot loop into the primitives the rest
of Archetype expects:

- one World per benchmark cell;
- one Entity per episode seed;
- one batched policy call over live entities;
- one batched env step over live entities;
- queryable component fields for arm identity and summary state.

Raw RGB observations stay inside the colocated env/policy client. The ledger
records lightweight state, actions, status, and task metadata so high-bandwidth
image tensors do not become the transport bottleneck.
"""

from __future__ import annotations

from typing import Any, Protocol

from archetype import ArchetypeRuntime
from archetype.core.component import Component
from bench.robosemantic.protocol import RsbSuite, normalized_semantic_grounding

RSB_ACTION_DIM = 14


class RsbTask(Component):
    """RSB task identity and GEPA/eval arm label."""

    suite: str = ""
    task_name: str = ""
    task_config: str = ""
    instruction: str = ""
    seed: int = 0
    env_key: int = 0
    episode_index: int = 0
    run_name: str = ""


class RsbState(Component):
    """Lightweight robot state used for provenance and debugging."""

    values: list[float] = [0.0] * RSB_ACTION_DIM


class RsbAction(Component):
    """Last action applied to the RSB env."""

    values: list[float] = [0.0] * RSB_ACTION_DIM


class RsbStatus(Component):
    """Episode status, latched once success/done occurs."""

    done: bool = False
    success: bool = False
    grasp_success: bool = False
    env_step: int = 0


class RsbEnvBatch(Protocol):
    """Batched RSB env boundary used by the Archetype cell loop."""

    def reset_batch(self, env_keys: list[int], seeds: list[int]) -> list[dict[str, Any]]:
        """Reset envs and return one observation dict per env key."""
        ...

    def step_batch(
        self,
        env_keys: list[int],
        actions: list[list[float]],
    ) -> list[dict[str, Any]]:
        """Step envs with one action per live env key."""
        ...


class RsbPolicyBatch(Protocol):
    """Batched policy boundary used by the Archetype cell loop."""

    def infer_batch(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[list[float]]]:
        """Return one action chunk per live env key."""
        ...


def _state_values(obs: dict[str, Any]) -> list[float]:
    values = obs.get("state", [])
    return [float(value) for value in values][:RSB_ACTION_DIM]


def _action_values(action: list[float]) -> list[float]:
    values = [float(value) for value in action][:RSB_ACTION_DIM]
    if len(values) < RSB_ACTION_DIM:
        values.extend([0.0] * (RSB_ACTION_DIM - len(values)))
    return values


async def run_batched_cell(
    *,
    runtime: ArchetypeRuntime,
    suite: RsbSuite,
    run_name: str,
    seeds: list[int],
    env: RsbEnvBatch,
    policy: RsbPolicyBatch,
    max_steps: int,
    storage: str | None = None,
    ledger_interval: int = 25,
) -> dict[str, Any]:
    """Run one RSB suite cell with one entity per seed.

    This is the reusable primitive for baseline sweeps and prompt-optimization
    arms. It deliberately keeps RGB observations in ``env``/``policy`` memory
    and writes compact per-tick components to Archetype.
    """
    if not seeds:
        raise ValueError("run_batched_cell requires at least one seed")
    if max_steps < 1:
        raise ValueError("max_steps must be >= 1")
    if ledger_interval < 1:
        raise ValueError("ledger_interval must be >= 1")

    world = runtime.world(
        name=f"{suite.name}-{run_name}",
        storage=storage,
        processors=[],
    )
    env_keys = list(range(len(seeds)))
    reset_obs = env.reset_batch(env_keys, seeds)
    if len(reset_obs) != len(seeds):
        raise RuntimeError(
            f"reset_batch returned {len(reset_obs)} observations for {len(seeds)} seeds"
        )

    entity_ids = await world.spawn_many(
        [
            [
                RsbTask(
                    suite=suite.name,
                    task_name=suite.task_name,
                    task_config=suite.eval_config,
                    instruction=str(obs.get("instruction", "")),
                    seed=int(seed),
                    env_key=int(env_key),
                    episode_index=int(idx),
                    run_name=run_name,
                ),
                RsbState(values=_state_values(obs)),
                RsbAction(),
                RsbStatus(),
            ]
            for idx, (env_key, seed, obs) in enumerate(zip(env_keys, seeds, reset_obs, strict=True))
        ]
    )
    await world.step()

    live: dict[int, dict[str, Any]] = {
        env_key: {
            "entity_id": entity_id,
            "instruction": str(obs.get("instruction", "")),
            "observation": obs,
            "env_step": 0,
            "success": False,
            "grasp_success": False,
        }
        for env_key, entity_id, obs in zip(env_keys, entity_ids, reset_obs, strict=True)
    }
    finished: dict[int, dict[str, Any]] = {}

    step_idx = 0
    while live and step_idx < max_steps:
        batch_keys = list(live)
        chunks = policy.infer_batch(
            batch_keys,
            [live[key]["instruction"] for key in batch_keys],
            [live[key]["observation"] for key in batch_keys],
        )
        if len(chunks) != len(batch_keys):
            raise RuntimeError(
                f"infer_batch returned {len(chunks)} chunks for {len(batch_keys)} live envs"
            )
        chunk_by_key = dict(zip(batch_keys, chunks, strict=True))
        chunk_len = max((len(chunk) for chunk in chunks), default=0)
        if chunk_len == 0:
            raise RuntimeError("infer_batch returned empty action chunks")

        for chunk_pos in range(chunk_len):
            if not live or step_idx >= max_steps:
                break
            active_keys = [
                key
                for key in batch_keys
                if key in live and chunk_pos < len(chunk_by_key[key])
            ]
            if not active_keys:
                break
            actions = [_action_values(chunk_by_key[key][chunk_pos]) for key in active_keys]
            next_obs = env.step_batch(active_keys, actions)
            if len(next_obs) != len(active_keys):
                raise RuntimeError(
                    f"step_batch returned {len(next_obs)} observations for {len(active_keys)} envs"
                )
            step_idx += 1

            flush_ledger = False
            for key, action, obs in zip(active_keys, actions, next_obs, strict=True):
                row = live[key]
                env_step = int(row["env_step"]) + 1
                success = bool(row["success"]) or bool(obs.get("success", False))
                grasp_success = bool(row["grasp_success"]) or bool(obs.get("grasp_success", False))
                done = bool(obs.get("done", False)) or success
                row.update(
                    {
                        "observation": obs,
                        "env_step": env_step,
                        "success": success,
                        "grasp_success": grasp_success,
                        "last_action": action,
                    }
                )
                if done or env_step % ledger_interval == 0:
                    flush_ledger = True
                    await world.update(
                        int(row["entity_id"]),
                        RsbState(values=_state_values(obs)),
                        RsbAction(values=action),
                        RsbStatus(
                            done=done,
                            success=success,
                            grasp_success=grasp_success,
                            env_step=env_step,
                        ),
                    )
                if done:
                    finished[key] = row
                    del live[key]
            if flush_ledger:
                await world.step()

    if live:
        for key, row in list(live.items()):
            await world.update(
                int(row["entity_id"]),
                RsbState(values=_state_values(row["observation"])),
                RsbAction(values=row.get("last_action", [0.0] * RSB_ACTION_DIM)),
                RsbStatus(
                    done=True,
                    success=bool(row["success"]),
                    grasp_success=bool(row["grasp_success"]),
                    env_step=int(row["env_step"]),
                ),
            )
            finished[key] = row
            del live[key]
        await world.step()

    episodes = len(seeds)
    successes = sum(1 for row in finished.values() if row["success"])
    grasp_successes = sum(1 for row in finished.values() if row["grasp_success"])
    tsr = successes / episodes
    gsr = grasp_successes / episodes
    info = await world.info()
    return {
        "suite": suite.name,
        "task_name": suite.task_name,
        "task_config": suite.eval_config,
        "choices": suite.choices,
        "run_name": run_name,
        "run_id": str(info.run_id),
        "world_id": str(info.world_id),
        "world": world,
        "episodes": episodes,
        "successes": successes,
        "grasp_successes": grasp_successes,
        "task_success_rate": tsr,
        "grasp_success_rate": gsr,
        "normalized_semantic_grounding": normalized_semantic_grounding(
            tsr=tsr,
            gsr=gsr,
            choices=suite.choices,
        ),
        "ticks": int(info.tick),
    }
