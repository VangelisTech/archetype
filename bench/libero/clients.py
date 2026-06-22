# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Harness-side LIBERO/VLA-JEPA clients and their Resources specs.

This is the benchmark half of the env/policy boundary. The framework
(``archetype.experiments.manipulation`` / ``.policy``) defines the
``EnvClient`` / ``PolicyClient`` protocols, the processors that drive them,
and the *abstract* ``EnvClientSpec`` / ``PolicyClientSpec`` Resources keys.
Everything LIBERO/robosuite/VLA-JEPA/Modal-specific — the things that need a
GPU or pin incompatible dependencies — lives here, so nothing under
``src/archetype`` imports a simulator or a model.

Wiring (Resources-spec path)::

    world.resources.insert_as(LiberoEnvSpec(suite="libero_spatial"), EnvClientSpec)
    world.resources.insert_as(LiberoVlaPolicySpec(suite="libero_spatial"), PolicyClientSpec)
    # processors built with client=None then build() these on first use.

Or pass the clients directly (constructor injection), as ``eval_driver`` does.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from archetype.experiments.manipulation import EnvClient, EnvClientSpec
from archetype.experiments.policy import PolicyClient, PolicyClientSpec

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

            try:
                policy_cls = modal.Cls.from_name(self._app_name, "VlaJepaPolicy")
                self._worker = policy_cls()
            except Exception as e:
                # This runs deep inside a Daft worker mid-sweep; a bare Modal
                # lookup error there is opaque. Say what's actually wrong.
                raise RuntimeError(
                    f"could not reach the VLA-JEPA Modal worker "
                    f"(app={self._app_name!r}, class='VlaJepaPolicy') — is it deployed? "
                    f"underlying error: {e}"
                ) from e
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
# Concrete Resources specs (the inversion point)
# ---------------------------------------------------------------------------


@dataclass
class LiberoEnvSpec(EnvClientSpec):
    """Picklable recipe for the LIBERO env worker behind ``EnvClient``.

    ``build()`` constructs a ``ModalEnvClient`` (harness-side adapter over the
    deployed ``LiberoEnvBatch``). Only plain scalars are stored, so the spec
    survives pickle to a Daft worker; the live Modal handle is built lazily by
    ``ModalEnvClient`` after unpickling.
    """

    suite: str = "libero_spatial"
    task_id: int = 0
    with_frames: bool = False

    def build(self) -> EnvClient:
        # Lazy so importing this module never requires `modal` to be installed;
        # the Modal dependency is paid only when a live client is built.
        try:
            from bench.libero.modal_worker import ModalEnvClient  # noqa: PLC0415
        except ImportError:  # bench/libero on sys.path directly (script context)
            from modal_worker import (
                ModalEnvClient,  # type: ignore[import-untyped,no-redef]  # noqa: PLC0415
            )

        return ModalEnvClient(
            suite=self.suite,
            task_id=self.task_id,
            with_frames=self.with_frames,
        )


@dataclass
class LiberoVlaPolicySpec(PolicyClientSpec):
    """Picklable recipe for the VLA-JEPA policy behind ``PolicyClient``."""

    suite: str = "libero_spatial"
    task_id: int = 0
    app_name: str = "archetype-vla-jepa"

    def build(self) -> PolicyClient:
        return VlaJepaPolicyClient(
            suite=self.suite,
            task_id=self.task_id,
            app_name=self.app_name,
        )
