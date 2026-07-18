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
contract tests and direct in-interpreter GPU policies such as VLA-JEPA for the
real thing. The tick-0 row keeps its spawn action
untouched — like the reset observation, the initial action slot is given,
not computed.

Resources reconciliation
------------------------
``PolicyActionProcessor`` and ``EnvStepProcessor`` accept either a client
instance (constructor injection, existing tests continue to work) OR, when
constructed with ``client=None``, pull a ``PolicyClientSpec`` /
``EnvClientSpec`` from the Resources container on the first ``process()``
call and build the client + UDF wrapper lazily (cached on the processor).

Why live handles cannot live in Resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Daft batch UDFs (``@daft.cls``) run on Daft workers — they may execute in
threads, sub-processes, or remote Ray/Modal workers depending on the cluster.
The ``@daft.cls`` class is **serialized per worker**: pickle is the boundary.
Resources lives in the driver process; you cannot put a non-picklable live
handle (a Modal stub, a network socket, an OS-level thread) in Resources and
expect it to cross the Daft worker boundary.  The spec dataclasses
(``PolicyClientSpec``, ``EnvClientSpec``) store only plain scalars that
**do** pickle.  The actual client is built inside ``_PolicyCaller.__init__``
(which runs once per worker, the same pattern as every other ``@daft.cls``
in this module).
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, Protocol

import daft
from daft import DataType, Series, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.resources import Resources
from archetype.experiments.boundary import external_call_indices, series_to_rows

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
        (keys: eef_pos, eef_quat, gripper, gripper_qpos, agentview_ref,
        wrist_ref)."""
        ...


# ---------------------------------------------------------------------------
# Resources-spec contract
# ---------------------------------------------------------------------------


class PolicyClientSpec(ABC):
    """Picklable, benchmark-supplied recipe for building a ``PolicyClient``.

    Registered in a world's ``Resources``; ``PolicyActionProcessor`` pulls it
    and calls ``build()`` to construct the live client — in the driver process
    or, for ``@daft.cls`` UDFs, once per Daft worker after the spec's scalars
    cross the pickle boundary (a live Modal handle / socket does not pickle;
    the scalars in a concrete spec do).

    The framework defines only this contract. Concrete specs — and the models,
    GPUs, and dependencies they pull in (VLA-JEPA/Modal) — live in the
    benchmark, so nothing under ``src/archetype`` imports a policy model.
    Register a concrete spec under this base type::

        world.resources.insert_as(MyPolicySpec(checkpoint="..."), PolicyClientSpec)
    """

    @abstractmethod
    def build(self) -> PolicyClient:
        """Construct the live ``PolicyClient`` from this spec's scalar config."""
        ...


# ---------------------------------------------------------------------------
# _PolicyCaller — Daft batch UDF wrapping any PolicyClient
# ---------------------------------------------------------------------------

_ACTION_TYPE = DataType.list(DataType.float64())


@daft.cls()
class _PolicyCaller:
    """Policy inference as a batch UDF: one client call per batch, the
    same sanctioned boundary pattern as the env and MuJoCo crossings.

    Accepts either a live ``PolicyClient`` (for constructor-injection) or
    a ``PolicyClientSpec`` (for Resources-spec construction).  The live
    client is built in ``__init__`` — once per Daft worker — by hydrating
    the spec.
    """

    def __init__(self, client: PolicyClient | PolicyClientSpec):
        if isinstance(client, PolicyClientSpec):
            # Hydrate from spec: delegate to spec.build() so subclasses can
            # override (e.g., return a test double via resources.insert_as).
            self._client: PolicyClient = client.build()
        else:
            self._client = client

    @daft.method.batch(return_dtype=_ACTION_TYPE)
    def act(
        self,
        env_key: Series,
        instruction: Series,
        eef_pos: Series,
        eef_quat: Series,
        gripper: Series,
        gripper_qpos: Series,
        done: Series,
        is_active: Series,
        prev_action: Series,
        agentview_ref: Series,
        wrist_ref: Series,
    ) -> Series:
        rows = series_to_rows(
            [
                "env_key",
                "instruction",
                "eef_pos",
                "eef_quat",
                "gripper",
                "gripper_qpos",
                "done",
                "is_active",
                "prev_action",
                "agentview_ref",
                "wrist_ref",
            ],
            env_key,
            instruction,
            eef_pos,
            eef_quat,
            gripper,
            gripper_qpos,
            done,
            is_active,
            prev_action,
            agentview_ref,
            wrist_ref,
        )

        # Done or inactive rows are frozen: keep the prior action unchanged.
        actions = [row["prev_action"] for row in rows]
        live = external_call_indices(rows)
        if live:
            chosen = self._client.act(
                [rows[i]["env_key"] for i in live],
                [rows[i]["instruction"] for i in live],
                [
                    {
                        "eef_pos": rows[i]["eef_pos"],
                        "eef_quat": rows[i]["eef_quat"],
                        "gripper": rows[i]["gripper"],
                        "gripper_qpos": rows[i]["gripper_qpos"],
                        "agentview_ref": rows[i]["agentview_ref"],
                        "wrist_ref": rows[i]["wrist_ref"],
                    }
                    for i in live
                ],
            )
            for i, action in zip(live, chosen, strict=True):
                actions[i] = [float(v) for v in action]
        return Series.from_pylist(actions)


@daft.cls()
class _PolicyCallerNoRefs:
    """Legacy variant without frame refs — used by PolicyActionProcessor
    when the archetype does not include ManipFrameRef (backwards compat)."""

    def __init__(self, client: PolicyClient | PolicyClientSpec):
        if isinstance(client, PolicyClientSpec):
            # Delegate to spec.build() so subclasses can return a test double.
            self._client: PolicyClient = client.build()
        else:
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
        is_active: Series,
        prev_action: Series,
    ) -> Series:
        rows = series_to_rows(
            [
                "env_key",
                "instruction",
                "eef_pos",
                "eef_quat",
                "gripper",
                "done",
                "is_active",
                "prev_action",
            ],
            env_key,
            instruction,
            eef_pos,
            eef_quat,
            gripper,
            done,
            is_active,
            prev_action,
        )

        # Done or inactive rows are frozen: keep the prior action unchanged.
        actions = [row["prev_action"] for row in rows]
        live = external_call_indices(rows)
        if live:
            chosen = self._client.act(
                [rows[i]["env_key"] for i in live],
                [rows[i]["instruction"] for i in live],
                [
                    {
                        "eef_pos": rows[i]["eef_pos"],
                        "eef_quat": rows[i]["eef_quat"],
                        "gripper": rows[i]["gripper"],
                    }
                    for i in live
                ],
            )
            for i, action in zip(live, chosen, strict=True):
                actions[i] = [float(v) for v in action]
        return Series.from_pylist(actions)


# ---------------------------------------------------------------------------
# PolicyActionProcessor
# ---------------------------------------------------------------------------


class PolicyActionProcessor(AsyncProcessor):
    """Write the policy action before EnvStepProcessor consumes it.

    Same archetype as the env step; must run BEFORE EnvStepProcessor so
    the env consumes this tick's action, not last tick's.

    Supports two construction modes:

    1. Constructor injection (existing tests)::

           PolicyActionProcessor(client=my_policy)

    2. Resources-spec construction (no live handle at construction time)::

           PolicyActionProcessor()  # client=None
           # Resources must carry a PolicyClientSpec at process() time.

    In mode 2 the processor builds the ``_PolicyCaller`` UDF wrapper on the
    first ``process()`` call and caches it.  Because ``_PolicyCaller`` is a
    ``@daft.cls``, it is serialized per Daft worker on UDF execution — the
    live ``PolicyClient`` is reconstructed inside ``_PolicyCaller.__init__``
    from the spec scalars.

    With ManipFrameRef
    ~~~~~~~~~~~~~~~~~~
    When the archetype includes ``ManipFrameRef`` (i.e. the framed episode
    path), ``_PolicyCaller`` receives ``agentview_ref`` and ``wrist_ref``
    columns and forwards them to the client.  Clients that do not use refs
    (e.g. ``ScriptedReachPolicy``) can ignore extra observation keys.

    Without ManipFrameRef (legacy path)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ``_PolicyCallerNoRefs`` is used, keeping the 7-arg signature that
    ``ScriptedReachPolicy``-based tests rely on.
    """

    components = (ManipProprio, ManipAction, ManipStatus, ManipTask)
    priority = 1

    def __init__(self, client: PolicyClient | PolicyClientSpec | None = None):
        self._client_or_spec = client
        # Lazily built on first process() when client is None. @daft.cls()
        # instances aren't statically typeable (decorator returns a UDF
        # wrapper, not the class), so the slot is annotated Any.
        self._caller: Any = None
        if client is not None:
            self._caller_no_refs = _PolicyCallerNoRefs(client)
            self._caller = _PolicyCaller(client)

    def _ensure_callers(self, resources: Resources | None) -> None:
        """Build callers from Resources on first call if not already built."""
        if self._caller is not None:
            return
        if resources is None:
            raise RuntimeError(
                "PolicyActionProcessor has no client and no Resources were passed. "
                "Either pass a client to the constructor or register a PolicyClientSpec "
                "in the world's Resources."
            )
        spec: PolicyClientSpec = resources.require(PolicyClientSpec)
        self._caller_no_refs = _PolicyCallerNoRefs(spec)
        self._caller = _PolicyCaller(spec)

    async def process(self, df, resources: Resources | None = None, **kwargs):
        self._ensure_callers(resources)

        # Detect whether the archetype has ManipFrameRef columns.
        schema_names = set(df.schema().column_names())
        has_refs = (
            "manipframeref__agentview_ref" in schema_names
            and "manipframeref__wrist_ref" in schema_names
        )

        if has_refs:
            return df.with_column(
                "manipaction__values",
                self._caller.act(
                    col("maniptask__env_key"),
                    col("maniptask__instruction"),
                    col("manipproprio__eef_pos"),
                    col("manipproprio__eef_quat"),
                    col("manipproprio__gripper"),
                    col("manipproprio__gripper_qpos"),
                    col("manipstatus__done"),
                    col("is_active"),
                    col("manipaction__values"),
                    col("manipframeref__agentview_ref"),
                    col("manipframeref__wrist_ref"),
                ),
            )
        else:
            return df.with_column(
                "manipaction__values",
                self._caller_no_refs.act(
                    col("maniptask__env_key"),
                    col("maniptask__instruction"),
                    col("manipproprio__eef_pos"),
                    col("manipproprio__eef_quat"),
                    col("manipproprio__gripper"),
                    col("manipstatus__done"),
                    col("is_active"),
                    col("manipaction__values"),
                ),
            )


# ---------------------------------------------------------------------------
# ScriptedReachPolicy (unchanged)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# InstructionConditionedReachPolicy — competence scales with the instruction
# ---------------------------------------------------------------------------

_TOKEN = re.compile(r"[a-z0-9]+")


def instruction_quality(instruction: str, required: Iterable[str]) -> float:
    """Fraction of ``required`` keywords present in ``instruction``, in [0, 1].

    A deterministic, dependency-free analog of "a policy follows the
    instruction better when it names the right verb/object." Tokenization is a
    lowercase alphanumeric split; distractor words are neutral, so the optimum
    is exactly "name every required keyword." Empty ``required`` returns 1.0.

    Exposed at module scope so an independent in-Python replay (and the
    instruction optimizer's stop test) can recompute the same gradient the
    policy acts on — no hidden state.
    """
    req = {w.lower() for w in required}
    if not req:
        return 1.0
    tokens = set(_TOKEN.findall(instruction.lower()))
    return len(req & tokens) / len(req)


class InstructionConditionedReachPolicy:
    """Proportional reach controller whose gain scales with instruction quality.

    A pure-Python stand-in for an instruction-conditioned VLA: the step it
    takes toward the per-env target is ``instruction_quality`` times the gain a
    perfectly-instructed policy would use. A vague instruction under-actuates
    and the episode times out; a precise one reaches. That gives the
    instruction-optimization loop a real, replayable success gradient with no
    GPU, no model weights, and no RNG — the same role ``ScriptedReachPolicy``
    plays for the provenance contract, extended along the instruction axis.

    Conforms to the ``PolicyClient`` protocol, so it drops into
    ``PolicyActionProcessor`` and ``run_instruction_sweep`` exactly where a
    direct model policy such as ``InProcessVlaJepaPolicy`` goes.
    """

    def __init__(
        self,
        targets: dict[int, tuple[float, float, float]],
        required_keywords: Iterable[str],
        gain: float = 0.6,
        max_step: float = 0.05,
    ):
        self._targets = targets
        self._required = frozenset(w.lower() for w in required_keywords)
        self._gain = gain
        self._max_step = max_step

    def quality(self, instruction: str) -> float:
        """How well ``instruction`` names this task — the gain multiplier."""
        return instruction_quality(instruction, self._required)

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        actions = []
        for env_key, instruction, obs in zip(env_keys, instructions, observations, strict=True):
            target = self._targets[env_key]
            eff_gain = self._gain * self.quality(instruction)
            action = [0.0] * ACTION_DIM
            for axis in range(3):
                delta = eff_gain * (target[axis] - obs["eef_pos"][axis])
                action[axis] = max(-self._max_step, min(self._max_step, delta))
            actions.append(action)
        return actions
