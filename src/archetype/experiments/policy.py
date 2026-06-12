# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Policy-in-the-loop: actions on the ledger with full provenance.

Stage 3 of the LIBERO/VLA-JEPA ladder. A ``PolicyActionProcessor``
(priority 1) writes the action *before* ``EnvStepProcessor`` (priority 10)
consumes it, so each ledger row t >= 1 carries both the action chosen from
the previous observation and the observation that action produced:

    a_t = pi(obs_{t-1});  obs_t = env.step(a_t);  row_t = (a_t, obs_t)

That makes every rollout self-documenting: replaying the policy against
the ledger's own observation column must reproduce the action column
exactly (the provenance contract tested in
``tests/experiments/test_policy_loop.py``).

``PolicyClient`` mirrors ``EnvClient``: in-process scripted policies for
contract tests, a remote GPU worker (VLA-JEPA behind a Modal/websocket
server) for the real thing. The tick-0 row keeps its spawn action
untouched — like the reset observation, the initial action slot is given,
not computed.
"""

from __future__ import annotations

from typing import Any, Protocol

import daft
from daft import DataType, Series, col

from archetype.core.aio.async_processor import AsyncProcessor

from .manipulation import (
    ACTION_DIM,
    ManipAction,
    ManipProprio,
    ManipStatus,
    ManipTask,
)


class PolicyClient(Protocol):
    """Boundary to a policy. Implementations may hold per-env recurrent
    state (action chunks, image history) keyed by ``env_key``."""

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        """Return one action per env given its latest observation dict
        (keys: eef_pos, eef_quat, gripper)."""
        ...


_ACTION_TYPE = DataType.list(DataType.float64())


@daft.cls()
class _PolicyCaller:
    """Policy inference as a batch UDF: one client call per batch, the
    same sanctioned boundary pattern as the env and MuJoCo crossings."""

    def __init__(self, client: PolicyClient):
        self._client = client

    @daft.method.batch(return_dtype=_ACTION_TYPE)
    def act(
        self,
        env_key: Series,
        instruction: Series,
        eef_pos: Series,
        eef_quat: Series,
        gripper: Series,
        done: Series,
        prev_action: Series,
    ) -> Series:
        keys = env_key.to_pylist()
        instructions = instruction.to_pylist()
        pos = eef_pos.to_pylist()
        quat = eef_quat.to_pylist()
        grip = gripper.to_pylist()
        finished = done.to_pylist()
        actions = prev_action.to_pylist()

        # Done rows are frozen: keep the terminal action unchanged.
        live = [i for i in range(len(keys)) if not finished[i]]
        if live:
            chosen = self._client.act(
                [keys[i] for i in live],
                [instructions[i] for i in live],
                [{"eef_pos": pos[i], "eef_quat": quat[i], "gripper": grip[i]} for i in live],
            )
            for i, action in zip(live, chosen, strict=True):
                actions[i] = [float(v) for v in action]
        return Series.from_pylist(actions)


class PolicyActionProcessor(AsyncProcessor):
    # Same archetype as the env step; must run BEFORE EnvStepProcessor so
    # the env consumes this tick's action, not last tick's.
    components = (ManipProprio, ManipAction, ManipStatus, ManipTask)
    priority = 1

    def __init__(self, client: PolicyClient):
        self._caller = _PolicyCaller(client)

    async def process(self, df, **kwargs):
        return df.with_column(
            "manipaction__values",
            self._caller.act(
                col("maniptask__env_key"),
                col("maniptask__instruction"),
                col("manipproprio__eef_pos"),
                col("manipproprio__eef_quat"),
                col("manipproprio__gripper"),
                col("manipstatus__done"),
                col("manipaction__values"),
            ),
        )


class ScriptedReachPolicy:
    """Deterministic proportional controller toward a per-env target.

    Pure float arithmetic so the provenance contract can be asserted with
    strict equality: action = clip(gain * (target - eef_pos)) per axis,
    zeros for the remaining action dims.
    """

    def __init__(
        self,
        targets: dict[int, tuple[float, float, float]],
        gain: float = 0.5,
        max_step: float = 0.05,
    ):
        self._targets = targets
        self._gain = gain
        self._max_step = max_step

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        actions = []
        for env_key, obs in zip(env_keys, observations, strict=True):
            target = self._targets[env_key]
            action = [0.0] * ACTION_DIM
            for axis in range(3):
                delta = self._gain * (target[axis] - obs["eef_pos"][axis])
                action[axis] = max(-self._max_step, min(self._max_step, delta))
            actions.append(action)
        return actions
