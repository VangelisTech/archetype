# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Manipulation episodes on the ledger: external envs behind a step boundary.

Stage 2 of the LIBERO/VLA-JEPA ladder. LIBERO pins an incompatible Python
(3.8-3.10 lineage, old torch), so the simulator can never live in the
Archetype process: it runs behind ``EnvClient`` — in-process for the
scripted contract env, out-of-process (subprocess, Docker, or a Modal
container) for robosuite/LIBERO.

The episode contract mirrors Stage 1's physics contract:

- ``env.reset()`` produces the initial observation, which the driver spawns
  as raw component values — reset obs land on the ledger at tick 0
  untouched (x_0 is given).
- Each tick, ``EnvStepProcessor`` sends the current action to the env and
  records the returned observation: row at tick t+1 is exactly
  ``env.step(action_t)``.
- ``done`` rows are frozen: the env is not stepped again and the terminal
  state persists unchanged, so success is latched on the ledger.

One Archetype tick = one control step. Action *chunks* (Stage 3) execute
inside the env worker between control steps, exactly like Stage 1's
physics substeps.
"""

from __future__ import annotations

from typing import Any, Protocol

import daft
from daft import DataType, Series, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component

ACTION_DIM = 7  # 6-DoF delta pose + gripper, the LIBERO/OSC convention


class ManipProprio(Component):
    """Proprioceptive observation: end-effector pose and gripper opening."""

    eef_pos: list[float] = [0.0, 0.0, 0.0]
    eef_quat: list[float] = [1.0, 0.0, 0.0, 0.0]
    gripper: float = 0.0


class ManipAction(Component):
    """The action applied during the next env step (delta pose + gripper)."""

    values: list[float] = [0.0] * ACTION_DIM


class ManipTask(Component):
    """Task identity. ``env_key`` routes rows to the env instance: the driver
    chooses it at ``reset`` time, before the entity id exists, so the raw
    reset observation can be spawned as the tick-0 row."""

    suite: str = ""
    task_id: int = 0
    instruction: str = ""
    seed: int = 0
    env_key: int = 0


class ManipStatus(Component):
    """Episode bookkeeping. ``success`` latches; ``done`` freezes the row."""

    reward: float = 0.0
    done: bool = False
    success: bool = False
    env_step: int = 0


class EnvClient(Protocol):
    """Boundary to an external manipulation simulator.

    Implementations own env instances keyed by ``env_id`` (the entity id).
    ``reset`` is called by the episode driver *before* spawning, so the
    returned observation becomes the entity's raw tick-0 row. ``step``
    advances each env one control step.
    """

    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        """Create/reset the env and return the initial observation dict with
        keys: eef_pos, eef_quat, gripper."""
        ...

    def step(self, env_ids: list[int], actions: list[list[float]]) -> list[dict[str, Any]]:
        """Step each env with its action. Returns one dict per env with keys:
        eef_pos, eef_quat, gripper, reward, done, success."""
        ...


_STEP_STRUCT = DataType.struct(
    {
        "eef_pos": DataType.list(DataType.float64()),
        "eef_quat": DataType.list(DataType.float64()),
        "gripper": DataType.float64(),
        "reward": DataType.float64(),
        "done": DataType.bool(),
        "success": DataType.bool(),
        "env_step": DataType.int64(),
    }
)


@daft.cls()
class _EnvStepper:
    """The env RPC boundary as a batch UDF: one client call per batch.

    The simulator is an external stateful process; its step cannot be a
    lazy Daft expression. Same sanctioned escape hatch as Stage 1's
    MuJoCo boundary."""

    def __init__(self, client: EnvClient):
        self._client = client

    @daft.method.batch(return_dtype=_STEP_STRUCT)
    def step(
        self,
        env_key: Series,
        action: Series,
        eef_pos: Series,
        eef_quat: Series,
        gripper: Series,
        reward: Series,
        done: Series,
        success: Series,
        env_step: Series,
    ) -> Series:
        ids = env_key.to_pylist()
        actions = action.to_pylist()
        prev = {
            "eef_pos": eef_pos.to_pylist(),
            "eef_quat": eef_quat.to_pylist(),
            "gripper": gripper.to_pylist(),
            "reward": reward.to_pylist(),
            "done": done.to_pylist(),
            "success": success.to_pylist(),
            "env_step": env_step.to_pylist(),
        }

        # Done rows are frozen: never step a finished episode.
        live = [i for i in range(len(ids)) if not prev["done"][i]]
        stepped: dict[int, dict[str, Any]] = {}
        if live:
            results = self._client.step([ids[i] for i in live], [actions[i] for i in live])
            stepped = dict(zip(live, results, strict=True))

        out: list[dict[str, Any]] = []
        for i in range(len(ids)):
            if i in stepped:
                obs = stepped[i]
                out.append(
                    {
                        "eef_pos": [float(v) for v in obs["eef_pos"]],
                        "eef_quat": [float(v) for v in obs["eef_quat"]],
                        "gripper": float(obs["gripper"]),
                        "reward": float(obs["reward"]),
                        "done": bool(obs["done"]),
                        # Success latches even if the env reports a
                        # post-success regression.
                        "success": bool(obs["success"]) or bool(prev["success"][i]),
                        "env_step": int(prev["env_step"][i]) + 1,
                    }
                )
            else:
                out.append({key: prev[key][i] for key in prev})
        return Series.from_pylist(out)


class EnvStepProcessor(AsyncProcessor):
    components = (ManipProprio, ManipAction, ManipStatus, ManipTask)
    priority = 10  # after any policy processor writes actions

    def __init__(self, client: EnvClient):
        self._stepper = _EnvStepper(client)

    async def process(self, df, **kwargs):
        nxt = self._stepper.step(
            col("maniptask__env_key"),
            col("manipaction__values"),
            col("manipproprio__eef_pos"),
            col("manipproprio__eef_quat"),
            col("manipproprio__gripper"),
            col("manipstatus__reward"),
            col("manipstatus__done"),
            col("manipstatus__success"),
            col("manipstatus__env_step"),
        )
        return (
            df.with_column("_env_next", nxt)
            .with_columns(
                {
                    "manipproprio__eef_pos": col("_env_next")["eef_pos"],
                    "manipproprio__eef_quat": col("_env_next")["eef_quat"],
                    "manipproprio__gripper": col("_env_next")["gripper"],
                    "manipstatus__reward": col("_env_next")["reward"],
                    "manipstatus__done": col("_env_next")["done"],
                    "manipstatus__success": col("_env_next")["success"],
                    "manipstatus__env_step": col("_env_next")["env_step"],
                }
            )
            .exclude("_env_next")
        )


class ScriptedReachEnv:
    """Deterministic in-process EnvClient for contract tests.

    A point end-effector integrates the first three action components;
    success when within ``tolerance`` of the per-env target. Pure float
    arithmetic, so tests can assert ledger rows with strict equality.
    """

    def __init__(self, targets: dict[int, tuple[float, float, float]], tolerance: float = 0.05):
        self._targets = targets
        self._tolerance = tolerance
        self._state: dict[int, list[float]] = {}

    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        # Deterministic seed-derived start, no RNG: contract tests replay it.
        start = [0.001 * seed, -0.001 * seed, 0.5]
        self._state[env_id] = list(start)
        return {"eef_pos": list(start), "eef_quat": [1.0, 0.0, 0.0, 0.0], "gripper": 0.0}

    def step(self, env_ids: list[int], actions: list[list[float]]) -> list[dict[str, Any]]:
        results = []
        for env_id, action in zip(env_ids, actions, strict=True):
            pos = self._state[env_id]
            for axis in range(3):
                pos[axis] += action[axis]
            target = self._targets[env_id]
            dist = sum((pos[i] - target[i]) ** 2 for i in range(3)) ** 0.5
            success = dist < self._tolerance
            results.append(
                {
                    "eef_pos": list(pos),
                    "eef_quat": [1.0, 0.0, 0.0, 0.0],
                    "gripper": 0.0,
                    "reward": -dist,
                    "done": success,
                    "success": success,
                }
            )
        return results
