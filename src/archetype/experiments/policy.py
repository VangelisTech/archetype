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
contract tests, a remote GPU worker (VLA-JEPA behind a Modal/websocket
server) for the real thing. The tick-0 row keeps its spawn action
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

import math
from dataclasses import dataclass
from typing import Any, Protocol

import daft
from daft import DataType, Series, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.resources import Resources

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
# Resources-spec dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PolicyClientSpec:
    """Picklable config for a ``PolicyClient``.

    Stored in Resources (driver-side) and passed to processors that
    want lazy construction.  The actual client is built in the processor
    (or ``_PolicyCaller.__init__`` on each Daft worker) from these scalars.

    Subclasses may override ``build()`` to return a different client
    (e.g., a test double).  ``_PolicyCallerNoRefs`` and ``_PolicyCaller``
    call ``spec.build()`` instead of instantiating ``VlaJepaPolicyClient``
    directly, so subclasses get the override for free.
    """

    suite: str
    task_id: int
    app_name: str = "archetype-vla-jepa"

    def build(self) -> PolicyClient:
        """Build the live ``PolicyClient`` from this spec's scalar config."""
        return VlaJepaPolicyClient(
            suite=self.suite,
            task_id=self.task_id,
            app_name=self.app_name,
        )


@dataclass
class EnvClientSpec:
    """Picklable config for an ``EnvClient``.

    Same driver/worker boundary rationale as ``PolicyClientSpec``: only
    plain scalars that survive pickle.

    Subclasses may override ``build()`` to return a different client
    (e.g., a test double registered via ``resources.insert_as``).
    """

    suite: str
    task_id: int
    app_name: str = "archetype-libero-env"
    with_frames: bool = False

    def build(self) -> Any:
        """Build the live ``EnvClient`` from this spec's scalar config.

        Delegates to ``_build_env_client_from_spec`` in manipulation.py
        so all Modal-import logic stays in one place.  Subclasses override
        this to return a test double without touching production import paths.
        """
        from archetype.experiments.manipulation import (  # noqa: PLC0415
            _build_env_client_from_spec,
        )

        return _build_env_client_from_spec(self)


# ---------------------------------------------------------------------------
# Quaternion helper (robosuite convention)
# ---------------------------------------------------------------------------


def _quat_to_axis_angle(quat: list[float]) -> list[float]:
    """Convert a robosuite quaternion (x, y, z, w) to axis-angle (3-vector).

    Upstream VLA-JEPA state vector convention (from ``eval_libero.py``)::

        state = [eef_pos(3), axis_angle(3), gripper_qpos(2)]

    Robosuite returns quaternions as (x, y, z, w).  The axis-angle
    representation is ``axis * angle`` where ``angle = 2 * acos(w)``
    (clamped to [0, pi]) and ``axis = q[:3] / sin(angle/2)``.

    Small-denominator guard: when ``|sin(angle/2)| < 1e-8`` (rotation is
    near-zero), the axis is ill-defined; we return the zero vector, which
    corresponds to the identity rotation.
    """
    x, y, z, w = quat
    # Clamp w to [-1, 1] to protect acos against floating-point overshoot.
    w_c = max(-1.0, min(1.0, w))
    half_angle = math.acos(w_c)
    sin_ha = math.sqrt(max(0.0, 1.0 - w_c * w_c))
    if sin_ha < 1e-8:
        return [0.0, 0.0, 0.0]
    scale = (2.0 * half_angle) / sin_ha
    return [x * scale, y * scale, z * scale]


# ---------------------------------------------------------------------------
# VlaJepaPolicyClient: chunk-buffered, ref-consuming
# ---------------------------------------------------------------------------

# Chunk length produced by VLA-JEPA (matches server response; currently 7).
_CHUNK_LEN = 7


class VlaJepaPolicyClient:
    """``PolicyClient`` wrapping the deployed VLA-JEPA Modal worker.

    Chunk-buffered: ``VlaJepaPolicy.infer_refs`` returns a full action chunk
    (``_CHUNK_LEN`` steps) per inference call.  This client maintains a
    per-``env_key`` buffer and pops one action per ``act()`` call, refreshing
    when the buffer is empty.

    State vector (8-dim, matches upstream eval convention)::

        [eef_pos(3), axis_angle(3), gripper_qpos(2)]

    where ``eef_pos`` and ``eef_quat`` (x,y,z,w robosuite) come from
    ``ManipProprio`` and ``gripper_qpos`` is the 2-element joint-position
    array (also from ``ManipProprio``).

    Gripper output convention:
        The VLA-JEPA model emits gripper **open** value in {0, 1} (binary,
        after dataset-statistics binarization inside the worker's
        ``_unnormalize``).  Robosuite expects ``{-1: open, +1: close}``.
        Upstream reference (``bench/libero/video_rollout.py``,
        ``_binarize_gripper_open``)::

            gripper_robosuite = 1 - 2 * (gripper_model > 0.5)

        Verification::

            model 1 (open)  → 1 - 2*1 = -1  (open in robosuite)  ✓
            model 0 (close) → 1 - 2*0 = +1  (close in robosuite) ✓

    Pickling:
        Stored as ``(suite, task_id, app_name)``; the live Modal handle
        (``self._worker``) is excluded from pickle so the client can cross
        Daft's worker boundary and reconnect lazily.
    """

    def __init__(
        self,
        suite: str = "libero_spatial",
        task_id: int = 0,
        app_name: str = "archetype-vla-jepa",
    ) -> None:
        self._suite = suite
        self._task_id = task_id
        self._app_name = app_name
        # Live handle — not picklable; excluded from __getstate__.
        self._worker = None
        # Per-env chunk buffers: env_key -> list of remaining actions.
        self._buffers: dict[int, list[list[float]]] = {}

    # --- Pickle protocol: store only plain config, reconnect lazily -------

    def __getstate__(self) -> dict[str, Any]:
        return {
            "suite": self._suite,
            "task_id": self._task_id,
            "app_name": self._app_name,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._suite = state["suite"]
        self._task_id = state["task_id"]
        self._app_name = state.get("app_name", "archetype-vla-jepa")
        self._worker = None
        self._buffers = {}

    def _get_worker(self):
        """Lazy Modal handle construction (reconnects after unpickling)."""
        if self._worker is None:
            import modal

            policy_cls = modal.Cls.from_name(self._app_name, "VlaJepaPolicy")
            self._worker = policy_cls()
        return self._worker

    @staticmethod
    def _build_state(obs: dict[str, Any]) -> list[float]:
        """Compose the 8-dim state vector from an observation dict.

        State = eef_pos(3) + axis_angle(eef_quat)(3) + gripper_qpos(2).
        ``eef_quat`` is robosuite (x, y, z, w); converted to axis-angle via
        ``_quat_to_axis_angle``.
        """
        eef_pos: list[float] = list(obs["eef_pos"])
        eef_quat: list[float] = list(obs["eef_quat"])
        gripper_qpos: list[float] = list(obs.get("gripper_qpos", [0.0, 0.0]))
        axis_angle = _quat_to_axis_angle(eef_quat)
        return eef_pos + axis_angle + gripper_qpos

    @staticmethod
    def _convert_gripper(action_row: list[float]) -> list[float]:
        """Convert gripper dim from model space to robosuite space (in-place copy).

        The model emits a binarized **open** value in {0, 1}:
          - 1 means "open"  → robosuite -1
          - 0 means "close" → robosuite +1

        Formula (applied to action dim index 6, matching upstream
        ``bench/libero/video_rollout.py`` / ``_binarize_gripper_open``)::

            gripper_robosuite = 1 - 2 * (gripper_model > 0.5)

        The remaining 6 dims (delta pose) are passed through unchanged.
        """
        out = list(action_row)
        out[6] = 1.0 - 2.0 * (1.0 if out[6] > 0.5 else 0.0)
        return out

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        """Return one action per env, popping from the chunk buffer.

        When a buffer is empty, calls ``infer_refs`` to refresh it with a
        new chunk.  The chunk length equals the server's response length
        (currently ``_CHUNK_LEN = 7``).

        Frame refs (``agentview_ref``, ``wrist_ref``) must be present in
        the observation dicts; they are forwarded to ``infer_refs`` for
        volume-based inference.
        """
        worker = self._get_worker()
        actions = []
        for env_key, instruction, obs in zip(env_keys, instructions, observations, strict=True):
            buf = self._buffers.get(env_key, [])
            if not buf:
                state = self._build_state(obs)
                chunk: list[list[float]] = worker.infer_refs.remote(
                    agentview_ref=obs["agentview_ref"],
                    wrist_ref=obs["wrist_ref"],
                    instruction=instruction,
                    state=state,
                )
                # Convert gripper dim for every action in the chunk before
                # buffering.  See docstring for the model→robosuite mapping.
                buf = [self._convert_gripper(a) for a in chunk]
            action = buf.pop(0)
            self._buffers[env_key] = buf
            actions.append(action)
        return actions


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
        prev_action: Series,
        agentview_ref: Series,
        wrist_ref: Series,
    ) -> Series:
        keys = env_key.to_pylist()
        instructions = instruction.to_pylist()
        pos = eef_pos.to_pylist()
        quat = eef_quat.to_pylist()
        grip = gripper.to_pylist()
        grip_qpos = gripper_qpos.to_pylist()
        finished = done.to_pylist()
        actions = prev_action.to_pylist()
        av_refs = agentview_ref.to_pylist()
        wr_refs = wrist_ref.to_pylist()

        # Done rows are frozen: keep the terminal action unchanged.
        live = [i for i in range(len(keys)) if not finished[i]]
        if live:
            chosen = self._client.act(
                [keys[i] for i in live],
                [instructions[i] for i in live],
                [
                    {
                        "eef_pos": pos[i],
                        "eef_quat": quat[i],
                        "gripper": grip[i],
                        "gripper_qpos": grip_qpos[i],
                        "agentview_ref": av_refs[i],
                        "wrist_ref": wr_refs[i],
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
        # Lazily built on first process() when client is None.
        self._caller: _PolicyCaller | _PolicyCallerNoRefs | None = None
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
