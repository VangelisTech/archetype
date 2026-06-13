# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic LIBERO replay + probe helpers — the GEPA scorer's substrate.

The scorer (gepa-core-loop.md §5) is an agentic REPL that, for any rollout,
must answer *exactly where it failed, why, and the root cause*. That requires
restoring MuJoCo state at any tick and querying ground-truth subgoal predicates
there. This module is that substrate.

Why a separate Modal app: LIBERO pins a Python-3.10 / EGL container that cannot
coexist with Archetype's 3.12 process (see modal_worker.py). We reuse that exact
image — ``image.add_local_python_source("modal_worker")`` makes the remote
container importable so we inherit the SHA-pinned LIBERO + robosuite + EGL recipe
without duplicating it. We do **not** edit modal_worker.py.

Determinism contract (verified, gepa-core-loop.md §8): a rollout is fully
reproducible from ``(suite, task_id, seed → init_state, action_sequence)``.
Replay is::

    env.seed(seed); env.reset()
    env.set_init_state(init_states[seed % n_init_states])   # x_0
    for a in action_sequence: env.step(a)                    # x_1 .. x_T

We replay once, caching the flattened MuJoCo state + obs at every tick, so
``replay_to(t)`` is an O(1) ``sim.set_state_from_flattened`` restore rather than
a re-rollout. tick 0 = the reset/init state (before any action); tick t (1..T)
= the state after applying ``action_sequence[t-1]``.

Probe helpers (all callable at any tick t), mirroring the core-loop REPL list:

    replay_to(t)        object_states(t)    contacts(t)     eef_to(obj, t)
    eval_subgoals(t)    frame(t, cam)       find_grasp_attempts()

Ground-truth predicates (verified on a live env):
  - goal: each entry of ``parsed_problem["goal_state"]`` via ``env.env._eval_predicate``.
  - constructed intermediates, evaluated by the *same* native machinery:
      grasped(obj)         — gripper finger geoms in contact with the object
                             (robosuite ``check_contact``); both-finger pinch.
      lifted(obj)          — native ``up`` predicate (registered: on/in/up/...).
      in(obj, region)      — native ``in`` predicate over a named region.
  obj_of_interest + the suite's named regions come from ``parsed_problem``.

Batch design: a scorer asks for a *set* of ticks in ONE Modal call
(``ProbeReplay.probe_batch``) — replay happens once, all requested probes for all
requested ticks come back together as JSON (+ base64 key frames). No per-tick
round trips.

Self-test (the gate)::

    modal run bench/libero/libero_replay.py

replays libero_spatial / task 0 / seed 0 with a zero action sequence and dumps
``eval_subgoals`` + ``object_states`` at the final tick — proving predicates and
poses come back from a real env.
"""

# NOTE: no `from __future__ import annotations` — modal.parameter() validates
# real annotation objects, and PEP 563 string annotations break it.
import sys
from pathlib import Path
from typing import Any

# Make ``modal_worker`` importable both locally (for the image recipe) and
# inside the remote container (added via add_local_python_source below).
sys.path.insert(0, str(Path(__file__).parent))

import modal
from modal_worker import FRAMES_MOUNT, frames_volume, image

# Reuse the SHA-pinned LIBERO/EGL image from modal_worker, and bundle this
# module's local dependency so the remote container can import it at top level.
image = image.add_local_python_source("modal_worker")

app = modal.App("archetype-libero-replay", image=image)


# ---------------------------------------------------------------------------
# Probe constants
# ---------------------------------------------------------------------------

# LIBERO/OpenVLA action convention: a 7-DoF action where the last element is the
# gripper command. Positive (≈ +1) = close, negative (≈ -1) = open. A "grasp
# attempt" is a tick whose action commands a close.
_GRIPPER_CLOSE_THRESHOLD = 0.0

# Cameras the env renders (matches modal_worker frame keys).
_CAM_OBS_KEY = {
    "agentview": "agentview_image",
    "wrist": "robot0_eye_in_hand_image",
}


# ---------------------------------------------------------------------------
# Replay class
# ---------------------------------------------------------------------------


@app.cls(
    cpu=4,
    memory=8192,
    timeout=1800,
    scaledown_window=300,
    volumes={FRAMES_MOUNT: frames_volume},
)
class ProbeReplay:
    """Deterministic replay + ground-truth probes for one (suite, task_id).

    A container instance is keyed by (suite, task_id, camera_size). Each
    ``probe_batch`` call is self-contained: it replays the supplied
    action_sequence from the seeded init state, caches per-tick MuJoCo state,
    then gathers the requested probes for the requested ticks in one shot.
    """

    suite: str = modal.parameter(default="libero_spatial")
    task_id: int = modal.parameter(default=0)
    camera_size: int = modal.parameter(default=128)

    @modal.enter()
    def load_suite(self):
        import os

        from libero.libero import benchmark, get_libero_path

        self._bm = benchmark.get_benchmark_dict()[self.suite]()
        self._task = self._bm.get_task(self.task_id)
        self._init_states = self._bm.get_task_init_states(self.task_id)
        self._bddl = os.path.join(
            get_libero_path("bddl_files"),
            self._task.problem_folder,
            self._task.bddl_file,
        )
        self._env = None  # built lazily; offscreen EGL ctx is expensive

    # -- env lifecycle -----------------------------------------------------

    def _get_env(self):
        if self._env is None:
            from libero.libero.envs import OffScreenRenderEnv

            self._env = OffScreenRenderEnv(
                bddl_file_name=self._bddl,
                camera_heights=self.camera_size,
                camera_widths=self.camera_size,
            )
        return self._env

    # -- deterministic replay ---------------------------------------------

    def _replay(self, seed: int, action_sequence: list[list[float]]) -> dict[str, Any]:
        """Replay the rollout, caching per-tick flat sim state + obs.

        Returns a dict with:
          states[t]   : flattened MjSimState (np.ndarray) restorable via
                        sim.set_state_from_flattened
          obs[t]      : the obs dict at tick t
          done_at     : first tick where the sim reports success, or None
          n_ticks     : T (number of actions applied)
        tick 0 is the reset/init state; tick t is post action_sequence[t-1].
        """
        import numpy as np

        env = self._get_env()
        env.seed(seed)
        env.reset()
        n = len(self._init_states)
        obs0 = env.set_init_state(self._init_states[seed % n])
        sim = env.env.sim

        states: dict[int, np.ndarray] = {0: np.asarray(sim.get_state().flatten()).copy()}
        obses: dict[int, dict] = {0: obs0}
        done_at: int | None = None

        for i, action in enumerate(action_sequence):
            obs, _reward, _done, _info = env.step(list(action))
            t = i + 1
            states[t] = np.asarray(sim.get_state().flatten()).copy()
            obses[t] = obs
            if done_at is None and bool(env.check_success()):
                done_at = t

        return {
            "states": states,
            "obs": obses,
            "done_at": done_at,
            "n_ticks": len(action_sequence),
        }

    def _restore(self, replay: dict[str, Any], t: int) -> dict:
        """Restore the sim to tick t and return the cached obs at that tick.

        replay_to(t): O(1) MuJoCo restore (no re-rollout). After this the env's
        underlying sim is positioned at tick t, so _eval_predicate / contacts /
        rendering all reflect that exact state.
        """
        env = self._get_env()
        sim = env.env.sim
        t = self._clamp_tick(replay, t)
        sim.set_state_from_flattened(replay["states"][t])
        sim.forward()
        return replay["obs"][t]

    @staticmethod
    def _clamp_tick(replay: dict[str, Any], t: int) -> int:
        max_t = replay["n_ticks"]
        if t < 0:  # negative indexing from the end, like Python lists
            t = max_t + 1 + t
        return max(0, min(t, max_t))

    # -- probe helpers (operate on the *currently restored* state) ---------

    def _object_states(self) -> dict[str, Any]:
        """Poses (pos+quat) for every object and named region at the restored state."""
        e = self._get_env().env
        out: dict[str, Any] = {}
        for name, ostate in e.object_states_dict.items():
            try:
                gs = ostate.get_geom_state()
                out[name] = {
                    "pos": [float(v) for v in gs["pos"]],
                    "quat": [float(v) for v in gs["quat"]],
                    "is_region": bool(getattr(ostate, "is_fixture", lambda: False)())
                    if callable(getattr(ostate, "is_fixture", None))
                    else False,
                }
            except Exception as exc:  # noqa: BLE001 — never let one bad object kill the batch
                out[name] = {"error": str(exc)}
        return out

    def _gripper_geoms(self) -> list[str]:
        e = self._get_env().env
        return list(e.robots[0].gripper.contact_geoms)

    def _finger_geom_groups(self) -> tuple[list[str], list[str]]:
        """(left_finger_geoms, right_finger_geoms) for both-finger pinch checks."""
        e = self._get_env().env
        ig = getattr(e.robots[0].gripper, "important_geoms", {})
        left = list(ig.get("left_finger", [])) or list(ig.get("left_fingerpad", []))
        right = list(ig.get("right_finger", [])) or list(ig.get("right_fingerpad", []))
        return left, right

    def _object_model(self, obj: str):
        """The robosuite MujocoModel for an object (so check_contact uses its geoms)."""
        e = self._get_env().env
        return e.get_object(obj)

    def _grasped(self, obj: str) -> bool:
        """grasped(obj): both gripper fingers in contact with the object.

        A single-finger touch is a graze, not a grasp; a stable grasp pinches the
        object between both fingers. We use robosuite's ``check_contact`` between
        each finger group and the object's collision geoms.
        """
        e = self._get_env().env
        try:
            obj_model = self._object_model(obj)
        except Exception:  # noqa: BLE001
            return False
        left, right = self._finger_geom_groups()
        if not left or not right:
            # Fall back to any-gripper-geom contact if finger groups are absent.
            return bool(e.check_contact(self._gripper_geoms(), obj_model))
        left_touch = bool(e.check_contact(left, obj_model))
        right_touch = bool(e.check_contact(right, obj_model))
        return left_touch and right_touch

    def _contacts(self) -> dict[str, Any]:
        """Gripper-object contact at the restored state, per object of interest + goal objs."""
        e = self._get_env().env
        names = self._relevant_objects()
        per_obj: dict[str, dict[str, bool]] = {}
        left, right = self._finger_geom_groups()
        for name in names:
            try:
                model = self._object_model(name)
            except Exception:  # noqa: BLE001
                continue
            entry = {
                "any_gripper_contact": bool(e.check_contact(self._gripper_geoms(), model)),
                "left_finger": bool(e.check_contact(left, model)) if left else False,
                "right_finger": bool(e.check_contact(right, model)) if right else False,
            }
            entry["grasped"] = entry["left_finger"] and entry["right_finger"]
            per_obj[name] = entry
        return per_obj

    def _eef_to(self, obj: str, obs: dict) -> dict[str, Any]:
        """Distance from eef to obj via the obs ``<obj>_to_robot0_eef_pos`` vector."""
        import numpy as np

        key = f"{obj}_to_robot0_eef_pos"
        if key not in obs:
            return {"error": f"no obs key {key!r}", "available": self._eef_rel_objects(obs)}
        vec = np.asarray(obs[key], dtype=float)
        return {
            "vector": [float(v) for v in vec],
            "distance": float(np.linalg.norm(vec)),
        }

    @staticmethod
    def _eef_rel_objects(obs: dict) -> list[str]:
        suffix = "_to_robot0_eef_pos"
        return sorted(k[: -len(suffix)] for k in obs if k.endswith(suffix))

    def _eval_subgoals(self, obs: dict) -> list[dict[str, Any]]:
        """Goal predicate(s) + constructed intermediate subgoals at the restored state.

        ``pass``/goal truth is sim ground truth (``_eval_predicate``), never an
        opinion. Intermediate subgoals (grasped/lifted/in) use the same native
        predicate machinery (``up``/``in``) or robosuite contact for grasp.
        """
        e = self._get_env().env
        pp = e.parsed_problem
        out: list[dict[str, Any]] = []

        # 1. Native goal predicates (the success conjunction).
        for pred in pp.get("goal_state", []):
            out.append(
                {
                    "predicate": list(pred),
                    "kind": "goal",
                    "satisfied": self._safe_eval(e, pred),
                }
            )

        ooi = pp.get("obj_of_interest", []) or []
        regions = list(pp.get("regions", {}).keys())

        # 2. grasped(obj_of_interest) — robosuite contact (no native 'grasp' pred).
        for obj in ooi:
            if obj in e.object_states_dict and not self._is_region(e, obj):
                out.append(
                    {
                        "predicate": ["grasped", obj],
                        "kind": "intermediate",
                        "satisfied": self._grasped(obj),
                    }
                )

        # 3. lifted(obj_of_interest) — native 'up' predicate.
        for obj in ooi:
            if obj in e.object_states_dict and not self._is_region(e, obj):
                out.append(
                    {
                        "predicate": ["lifted", obj],
                        "kind": "intermediate",
                        "satisfied": self._safe_eval(e, ["up", obj]),
                    }
                )

        # 4. in(obj_of_interest, region) — native 'in' predicate over goal-relevant
        #    regions. We surface regions named in the goal_state, plus any region
        #    explicitly referenced by obj_of_interest, so the scorer sees the
        #    placement subgoal flip without guessing the target region.
        goal_regions = self._goal_regions(pp)
        target_regions = goal_regions or regions
        movable = [o for o in ooi if o in e.object_states_dict and not self._is_region(e, o)]
        for obj in movable:
            for region in target_regions:
                if region in e.object_states_dict:
                    out.append(
                        {
                            "predicate": ["in", obj, region],
                            "kind": "intermediate",
                            "satisfied": self._safe_eval(e, ["in", obj, region]),
                        }
                    )
        return out

    @staticmethod
    def _goal_regions(pp: dict) -> list[str]:
        """Region names that appear as the second operand of a goal predicate."""
        regions = set(pp.get("regions", {}).keys())
        out: list[str] = []
        for pred in pp.get("goal_state", []):
            for token in pred[1:]:
                if token in regions and token not in out:
                    out.append(token)
        return out

    @staticmethod
    def _is_region(e, name: str) -> bool:
        ostate = e.object_states_dict.get(name)
        is_fixture = getattr(ostate, "is_fixture", None)
        try:
            return bool(is_fixture()) if callable(is_fixture) else False
        except Exception:  # noqa: BLE001
            return False

    @staticmethod
    def _safe_eval(e, pred: list) -> Any:
        """Evaluate a predicate; return bool, or {"error": ...} so one bad pred
        never aborts a batch the scorer depends on."""
        try:
            return bool(e._eval_predicate(list(pred)))
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    def _relevant_objects(self) -> list[str]:
        """obj_of_interest + every object named in the goal_state (deduped, movable)."""
        e = self._get_env().env
        pp = e.parsed_problem
        names: list[str] = []
        for n in pp.get("obj_of_interest", []) or []:
            if n in e.object_states_dict and n not in names and not self._is_region(e, n):
                names.append(n)
        for pred in pp.get("goal_state", []):
            for token in pred[1:]:
                if (
                    token in e.object_states_dict
                    and token not in names
                    and not self._is_region(e, token)
                ):
                    names.append(token)
        return names

    def _render_frame_b64(self, cam: str) -> str:
        """Render the current restored state on a camera and return base64 PNG."""
        import base64

        import cv2

        env = self._get_env()
        e = env.env
        obs_key = _CAM_OBS_KEY.get(cam)
        if obs_key is None:
            raise ValueError(f"unknown camera {cam!r}; choose from {sorted(_CAM_OBS_KEY)}")
        # Regenerate observations at the restored state to get fresh pixels.
        obs = e._get_observations(force_update=True)
        rgb = obs[obs_key]
        ok, buf = cv2.imencode(".png", cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR))
        if not ok:
            raise RuntimeError("png encode failed")
        return base64.b64encode(buf.tobytes()).decode()

    def _find_grasp_attempts(
        self, replay: dict[str, Any], action_sequence: list[list[float]]
    ) -> list[int]:
        """Ticks where the policy commands the gripper to close.

        A grasp *attempt* is a control intent — the last action element crossing
        into close territory — independent of whether contact was actually made.
        The scorer pairs these with ``contacts(t)`` to tell "tried and missed"
        from "tried and held".
        """
        attempts: list[int] = []
        prev_close = False
        for i, action in enumerate(action_sequence):
            if not action:
                continue
            close = float(action[-1]) > _GRIPPER_CLOSE_THRESHOLD
            # Rising edge: an *attempt* begins when the command transitions to close.
            if close and not prev_close:
                attempts.append(i + 1)  # tick after this action
            prev_close = close
        return attempts

    # -- the batched entry point ------------------------------------------

    @modal.method()
    def probe_batch(
        self,
        seed: int,
        action_sequence: list[list[float]],
        ticks: list[int] | None = None,
        probes: list[str] | None = None,
        frame_ticks: list[int] | None = None,
        cams: list[str] | None = None,
        eef_objects: list[str] | None = None,
    ) -> dict[str, Any]:
        """Replay once, then gather every requested probe for every requested tick.

        Args:
            seed: rollout seed (selects init_states[seed % n] — the x_0).
            action_sequence: the exact actions that produced the rollout.
            ticks: ticks to probe (default: just the final tick). Negative
                indexes from the end (-1 = final). Out-of-range ticks are clamped.
            probes: which probes per tick — subset of
                {"object_states","contacts","eef_to","eval_subgoals"}.
                Default: all of them.
            frame_ticks: ticks to render key frames for (default: none — frames
                are the heavy payload, so they're opt-in).
            cams: cameras to render at frame_ticks (default ["agentview"]).
            eef_objects: objects for eef_to (default: obj_of_interest + goal objs).

        Returns a JSON-serialisable dict::

            {
              "meta": {suite, task_id, seed, language, n_ticks, done_at,
                       obj_of_interest, regions, goal_state, success},
              "ticks": { "<t>": {object_states, contacts, eef_to, subgoals} },
              "grasp_attempts": [t, ...],
              "frames": { "<t>": { "<cam>": "<base64 png>" } }
            }
        """
        replay = self._replay(seed, action_sequence)
        e = self._get_env().env
        pp = e.parsed_problem

        n_ticks = replay["n_ticks"]
        if ticks is None:
            ticks = [n_ticks]  # final tick by default
        if probes is None:
            probes = ["object_states", "contacts", "eef_to", "eval_subgoals"]
        if cams is None:
            cams = ["agentview"]
        probe_set = set(probes)

        eef_targets = eef_objects if eef_objects is not None else self._relevant_objects()

        # Final-state success (sim ground truth) — restore the last tick for it.
        self._restore(replay, n_ticks)
        final_success = bool(self._get_env().check_success())

        out_ticks: dict[str, Any] = {}
        for raw_t in ticks:
            t = self._clamp_tick(replay, raw_t)
            obs = self._restore(replay, t)
            entry: dict[str, Any] = {}
            if "object_states" in probe_set:
                entry["object_states"] = self._object_states()
            if "contacts" in probe_set:
                entry["contacts"] = self._contacts()
            if "eef_to" in probe_set:
                entry["eef_to"] = {obj: self._eef_to(obj, obs) for obj in eef_targets}
            if "eval_subgoals" in probe_set:
                entry["subgoals"] = self._eval_subgoals(obs)
            out_ticks[str(t)] = entry

        frames: dict[str, Any] = {}
        for raw_t in frame_ticks or []:
            t = self._clamp_tick(replay, raw_t)
            self._restore(replay, t)
            frames[str(t)] = {cam: self._render_frame_b64(cam) for cam in cams}

        return {
            "meta": {
                "suite": self.suite,
                "task_id": self.task_id,
                "seed": seed,
                "language": str(self._task.language),
                "n_ticks": n_ticks,
                "n_init_states": len(self._init_states),
                "done_at": replay["done_at"],
                "success": final_success,
                "obj_of_interest": list(pp.get("obj_of_interest", []) or []),
                "regions": list(pp.get("regions", {}).keys()),
                "goal_state": [list(p) for p in pp.get("goal_state", [])],
            },
            "ticks": out_ticks,
            "grasp_attempts": self._find_grasp_attempts(replay, action_sequence),
            "frames": frames,
        }

    @modal.method()
    def task_language(self) -> str:
        return str(self._task.language)


# ---------------------------------------------------------------------------
# Harness-side client (py3.12, modal package only — no LIBERO import)
# ---------------------------------------------------------------------------


class ReplayClient:
    """Harness-side adapter over the deployed ProbeReplay class.

    Import from the Archetype (py3.12) process: it needs only the ``modal``
    client package, not LIBERO. Like ModalEnvClient, it pickles as plain config
    and reconnects lazily so it can ride into a Daft batch UDF (a live Modal
    handle is not serialisable).
    """

    def __init__(self, suite: str = "libero_spatial", task_id: int = 0, camera_size: int = 128):
        self._suite = suite
        self._task_id = task_id
        self._camera_size = camera_size
        self._worker = None

    def _get_worker(self):
        if self._worker is None:
            cls = modal.Cls.from_name("archetype-libero-replay", "ProbeReplay")
            self._worker = cls(
                suite=self._suite, task_id=self._task_id, camera_size=self._camera_size
            )
        return self._worker

    def __getstate__(self) -> dict[str, Any]:
        return {
            "suite": self._suite,
            "task_id": self._task_id,
            "camera_size": self._camera_size,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._suite = state["suite"]
        self._task_id = state["task_id"]
        self._camera_size = state.get("camera_size", 128)
        self._worker = None

    def probe_batch(
        self,
        seed: int,
        action_sequence: list[list[float]],
        ticks: list[int] | None = None,
        probes: list[str] | None = None,
        frame_ticks: list[int] | None = None,
        cams: list[str] | None = None,
        eef_objects: list[str] | None = None,
    ) -> dict[str, Any]:
        return self._get_worker().probe_batch.remote(
            seed,
            action_sequence,
            ticks=ticks,
            probes=probes,
            frame_ticks=frame_ticks,
            cams=cams,
            eef_objects=eef_objects,
        )


# ---------------------------------------------------------------------------
# Self-test (the gate)
# ---------------------------------------------------------------------------


@app.local_entrypoint()
def selftest(suite: str = "libero_spatial", task_id: int = 0, seed: int = 0, steps: int = 20):
    """Replay a fixed rollout with a zero/dummy action sequence and dump
    eval_subgoals + object_states at the final tick.

    Proves the predicates and poses come back from a real env. This is the GATE.
    """
    import json

    worker = ProbeReplay(suite=suite, task_id=task_id)
    zero_action = [0.0] * 7
    action_sequence = [zero_action for _ in range(steps)]

    result = worker.probe_batch.remote(
        seed=seed,
        action_sequence=action_sequence,
        ticks=[0, steps],  # init state and final state
        probes=["object_states", "contacts", "eef_to", "eval_subgoals"],
        frame_ticks=[steps],
        cams=["agentview"],
    )

    meta = result["meta"]
    print("=" * 70)
    print(f"suite={meta['suite']} task={meta['task_id']} seed={meta['seed']}")
    print(f"language: {meta['language']!r}")
    print(f"n_ticks={meta['n_ticks']} done_at={meta['done_at']} success={meta['success']}")
    print(f"obj_of_interest: {meta['obj_of_interest']}")
    print(f"goal_state: {meta['goal_state']}")
    print(f"grasp_attempts: {result['grasp_attempts']}")
    print("=" * 70)

    final = result["ticks"][str(meta["n_ticks"])]

    print("\n--- eval_subgoals @ final tick ---")
    print(json.dumps(final["subgoals"], indent=2))

    print("\n--- object_states @ final tick (poses) ---")
    print(json.dumps(final["object_states"], indent=2))

    print("\n--- contacts @ final tick ---")
    print(json.dumps(final["contacts"], indent=2))

    print("\n--- eef_to @ final tick ---")
    print(json.dumps(final["eef_to"], indent=2))

    # Gate assertions: real predicate + pose data must come back.
    assert final["subgoals"], "no subgoals returned — predicate machinery failed"
    assert any(s["kind"] == "goal" for s in final["subgoals"]), "no native goal predicate"
    assert final["object_states"], "no object poses returned"
    sample = next(iter(final["object_states"].values()))
    assert "pos" in sample, f"object state missing pose: {sample}"
    frame_b64 = result["frames"][str(meta["n_ticks"])]["agentview"]
    assert len(frame_b64) > 100, "key frame did not render"

    print("\nSELFTEST OK — predicates, poses, contacts, eef, and a key frame all returned.")
