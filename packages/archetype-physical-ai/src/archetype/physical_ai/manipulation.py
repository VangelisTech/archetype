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
- Each tick, the internal environment-step processor sends the current action to the env and
  records the returned observation: row at tick t+1 is exactly
  ``env.step(action_t)``.
- ``done`` or inactive rows are frozen: the env is not stepped again and the
  terminal state persists unchanged, so success is latched on the ledger.

One Archetype tick = one control step. Action *chunks* (Stage 3) execute
inside the env worker between control steps, exactly like Stage 1's
physics substeps.
"""

from __future__ import annotations

from typing import Any

import daft
from daft import DataType, Series, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.physical_ai._dataframe import (
    external_call_indices,
    series_to_rows,
    unpack_struct,
)
from archetype.physical_ai.interfaces import EnvClient

ACTION_DIM = 7  # 6-DoF delta pose + gripper, the LIBERO/OSC convention


class ManipProprio(Component):
    """Proprioceptive observation: end-effector pose and gripper opening.

    ``gripper_qpos`` carries both finger joint values (the VLA policy's 8-dim
    state vector needs the full ``robot0_gripper_qpos``, not just the scalar
    opening). Defaults to ``[0.0, 0.0]`` for backwards compatibility.
    """

    eef_pos: list[float] = [0.0, 0.0, 0.0]
    eef_quat: list[float] = [1.0, 0.0, 0.0, 0.0]
    gripper: float = 0.0
    gripper_qpos: list[float] = [0.0, 0.0]


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


class ManipFrameRef(Component):
    """Volume refs to the agentview and wrist camera PNGs for a tick.

    Written by the driver when frames are stored in the shared Modal
    ``libero-frames`` volume.  Empty strings signal "no frame sidecar"
    (e.g. in tests that do not mount the volume).

    Ledger contract (mirrors the initial-conditions invariant):
    - Tick-0 refs are the refs returned by ``env.reset(…, with_frames=True)``
      — they land raw on the spawn row.
    - Tick t refs are the refs returned by ``env.step(…)`` — written by
      the framed environment-step processor immediately after the env call.
    """

    agentview_ref: str = ""
    wrist_ref: str = ""


_STEP_STRUCT = DataType.struct(
    {
        "eef_pos": DataType.list(DataType.float64()),
        "eef_quat": DataType.list(DataType.float64()),
        "gripper": DataType.float64(),
        "gripper_qpos": DataType.list(DataType.float64()),
        "reward": DataType.float64(),
        "done": DataType.bool(),
        "success": DataType.bool(),
        "env_step": DataType.int64(),
    }
)

_FRAMED_STEP_STRUCT = DataType.struct(
    {
        "eef_pos": DataType.list(DataType.float64()),
        "eef_quat": DataType.list(DataType.float64()),
        "gripper": DataType.float64(),
        "gripper_qpos": DataType.list(DataType.float64()),
        "reward": DataType.float64(),
        "done": DataType.bool(),
        "success": DataType.bool(),
        "env_step": DataType.int64(),
        "agentview_ref": DataType.string(),
        "wrist_ref": DataType.string(),
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
        gripper_qpos: Series,
        reward: Series,
        done: Series,
        success: Series,
        env_step: Series,
        is_active: Series,
    ) -> Series:
        rows = series_to_rows(
            [
                "env_key",
                "action",
                "eef_pos",
                "eef_quat",
                "gripper",
                "gripper_qpos",
                "reward",
                "done",
                "success",
                "env_step",
                "is_active",
            ],
            env_key,
            action,
            eef_pos,
            eef_quat,
            gripper,
            gripper_qpos,
            reward,
            done,
            success,
            env_step,
            is_active,
        )

        live = external_call_indices(rows)
        stepped: dict[int, dict[str, Any]] = {}
        if live:
            results = self._client.step(
                [rows[i]["env_key"] for i in live],
                [rows[i]["action"] for i in live],
            )
            stepped = dict(zip(live, results, strict=True))

        out: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            if i in stepped:
                obs = stepped[i]
                out.append(
                    {
                        "eef_pos": [float(v) for v in obs["eef_pos"]],
                        "eef_quat": [float(v) for v in obs["eef_quat"]],
                        "gripper": float(obs["gripper"]),
                        "gripper_qpos": [
                            float(v)
                            for v in obs.get("gripper_qpos", [obs["gripper"], obs["gripper"]])
                        ],
                        "reward": float(obs["reward"]),
                        "done": bool(obs["done"]),
                        # Success latches even if the env reports a
                        # post-success regression.
                        "success": bool(obs["success"]) or bool(row["success"]),
                        "env_step": int(row["env_step"]) + 1,
                    }
                )
            else:
                out.append(
                    {
                        k: row[k]
                        for k in (
                            "eef_pos",
                            "eef_quat",
                            "gripper",
                            "gripper_qpos",
                            "reward",
                            "done",
                            "success",
                            "env_step",
                        )
                    }
                )
        return Series.from_pylist(out)


class _EnvStepProcessor(AsyncProcessor):
    """Step the env and write observations back to the ledger.

    The explicit client is registered with the runtime owner before this
    processor is constructed. Its ``aclose()`` owns every backing resource
    reached by the serialized Daft handle.
    """

    components = (ManipProprio, ManipAction, ManipStatus, ManipTask)
    priority = 10  # after any policy processor writes actions

    def __init__(self, client: EnvClient):
        # @daft.cls() instances aren't statically typeable (the decorator returns
        # a UDF wrapper, not the class); annotate the slot as Any.
        self._stepper = _EnvStepper(client)

    async def process(self, df, **kwargs):
        nxt = self._stepper.step(
            col("maniptask__env_key"),
            col("manipaction__values"),
            col("manipproprio__eef_pos"),
            col("manipproprio__eef_quat"),
            col("manipproprio__gripper"),
            col("manipproprio__gripper_qpos"),
            col("manipstatus__reward"),
            col("manipstatus__done"),
            col("manipstatus__success"),
            col("manipstatus__env_step"),
            col("is_active"),
        )
        return unpack_struct(
            df.with_column("_env_next", nxt),
            "_env_next",
            {
                "eef_pos": "manipproprio__eef_pos",
                "eef_quat": "manipproprio__eef_quat",
                "gripper": "manipproprio__gripper",
                "gripper_qpos": "manipproprio__gripper_qpos",
                "reward": "manipstatus__reward",
                "done": "manipstatus__done",
                "success": "manipstatus__success",
                "env_step": "manipstatus__env_step",
            },
        )


@daft.cls()
class _FramedEnvStepper:
    """Framed variant of _EnvStepper: expects the client to return
    ``agentview_ref`` and ``wrist_ref`` keys in each obs dict alongside
    the standard proprio fields.  Volume refs are strings (empty string
    means no frame for that row).
    """

    def __init__(self, client: EnvClient):
        self._client = client

    @daft.method.batch(return_dtype=_FRAMED_STEP_STRUCT)
    def step(
        self,
        env_key: Series,
        action: Series,
        eef_pos: Series,
        eef_quat: Series,
        gripper: Series,
        gripper_qpos: Series,
        reward: Series,
        done: Series,
        success: Series,
        env_step: Series,
        agentview_ref: Series,
        wrist_ref: Series,
        is_active: Series,
    ) -> Series:
        rows = series_to_rows(
            [
                "env_key",
                "action",
                "eef_pos",
                "eef_quat",
                "gripper",
                "gripper_qpos",
                "reward",
                "done",
                "success",
                "env_step",
                "agentview_ref",
                "wrist_ref",
                "is_active",
            ],
            env_key,
            action,
            eef_pos,
            eef_quat,
            gripper,
            gripper_qpos,
            reward,
            done,
            success,
            env_step,
            agentview_ref,
            wrist_ref,
            is_active,
        )

        live = external_call_indices(rows)
        stepped: dict[int, dict[str, Any]] = {}
        if live:
            results = self._client.step(
                [rows[i]["env_key"] for i in live],
                [rows[i]["action"] for i in live],
            )
            stepped = dict(zip(live, results, strict=True))

        out: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            if i in stepped:
                obs = stepped[i]
                out.append(
                    {
                        "eef_pos": [float(v) for v in obs["eef_pos"]],
                        "eef_quat": [float(v) for v in obs["eef_quat"]],
                        "gripper": float(obs["gripper"]),
                        "gripper_qpos": [
                            float(v)
                            for v in obs.get("gripper_qpos", [obs["gripper"], obs["gripper"]])
                        ],
                        "reward": float(obs["reward"]),
                        "done": bool(obs["done"]),
                        "success": bool(obs["success"]) or bool(row["success"]),
                        "env_step": int(row["env_step"]) + 1,
                        "agentview_ref": str(obs.get("agentview_ref", "")),
                        "wrist_ref": str(obs.get("wrist_ref", "")),
                    }
                )
            else:
                out.append(
                    {
                        k: row[k]
                        for k in (
                            "eef_pos",
                            "eef_quat",
                            "gripper",
                            "gripper_qpos",
                            "reward",
                            "done",
                            "success",
                            "env_step",
                            "agentview_ref",
                            "wrist_ref",
                        )
                    }
                )
        return Series.from_pylist(out)


class _FramedEnvStepProcessor(AsyncProcessor):
    """Like the environment-step processor but carry frame refs on the ledger.

    Use this when the env client returns ``agentview_ref`` and ``wrist_ref``
    alongside proprio (e.g. the ModalEnvClient with ``with_frames=True`` and
    a shared volume mounted at ``/frames``).  The existing
    The non-framed processor is unchanged — tests that do not use the volume
    continue to work against it.

    The client must be an already-constructed, runtime-owned provider.
    """

    components = (ManipProprio, ManipAction, ManipStatus, ManipTask, ManipFrameRef)
    priority = 10

    def __init__(self, client: EnvClient):
        # @daft.cls() instances aren't statically typeable; annotate as Any.
        self._stepper = _FramedEnvStepper(client)

    async def process(self, df, **kwargs):
        nxt = self._stepper.step(
            col("maniptask__env_key"),
            col("manipaction__values"),
            col("manipproprio__eef_pos"),
            col("manipproprio__eef_quat"),
            col("manipproprio__gripper"),
            col("manipproprio__gripper_qpos"),
            col("manipstatus__reward"),
            col("manipstatus__done"),
            col("manipstatus__success"),
            col("manipstatus__env_step"),
            col("manipframeref__agentview_ref"),
            col("manipframeref__wrist_ref"),
            col("is_active"),
        )
        return unpack_struct(
            df.with_column("_env_next", nxt),
            "_env_next",
            {
                "eef_pos": "manipproprio__eef_pos",
                "eef_quat": "manipproprio__eef_quat",
                "gripper": "manipproprio__gripper",
                "gripper_qpos": "manipproprio__gripper_qpos",
                "reward": "manipstatus__reward",
                "done": "manipstatus__done",
                "success": "manipstatus__success",
                "env_step": "manipstatus__env_step",
                "agentview_ref": "manipframeref__agentview_ref",
                "wrist_ref": "manipframeref__wrist_ref",
            },
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
        return {
            "eef_pos": list(start),
            "eef_quat": [1.0, 0.0, 0.0, 0.0],
            "gripper": 0.0,
            "gripper_qpos": [0.0, 0.0],
        }

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
                    "gripper_qpos": [0.0, 0.0],
                    "reward": -dist,
                    "done": success,
                    "success": success,
                }
            )
        return results

    async def aclose(self) -> None:
        """Release this dependency-free provider (a deliberate no-op)."""


class ScriptedFramedReachEnv(ScriptedReachEnv):
    """ScriptedReachEnv extended with deterministic fake volume refs.

    Used by contract tests to assert that ``ManipFrameRef`` refs are
    written to the ledger at every tick without requiring a Modal volume.
    Refs are deterministic strings: ``<session>/<env_id>/reset-agentview``
    at reset and ``<session>/<env_id>/step<N>-agentview`` at each step,
    where ``session`` is a fixed tag for reproducibility.
    """

    SESSION = "test-session"

    def __init__(
        self,
        targets: dict[int, tuple[float, float, float]],
        tolerance: float = 0.05,
    ):
        super().__init__(targets=targets, tolerance=tolerance)
        self._step_counts: dict[int, int] = {}

    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        obs = super().reset(env_id, seed)
        self._step_counts[env_id] = 0
        obs["agentview_ref"] = f"{self.SESSION}/{env_id}/reset-agentview.png"
        obs["wrist_ref"] = f"{self.SESSION}/{env_id}/reset-wrist.png"
        return obs

    def step(self, env_ids: list[int], actions: list[list[float]]) -> list[dict[str, Any]]:
        results = super().step(env_ids, actions)
        for obs, env_id in zip(results, env_ids, strict=True):
            self._step_counts[env_id] = self._step_counts.get(env_id, 0) + 1
            n = self._step_counts[env_id]
            obs["agentview_ref"] = f"{self.SESSION}/{env_id}/step{n:05d}-agentview.png"
            obs["wrist_ref"] = f"{self.SESSION}/{env_id}/step{n:05d}-wrist.png"
        return results
