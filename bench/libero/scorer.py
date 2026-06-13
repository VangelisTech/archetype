# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The agentic REPL scorer — the make-or-break GEPA feedback stage.

This is core-loop §5: per rollout, localize *exactly where and why* the robot
failed, and emit the locked scorer contract::

    {
      "pass": bool,                 # GROUND TRUTH from sim _check_success
      "subgoals": [{"predicate", "satisfied", "flipped_at_tick"}],
      "failure_phase": "approach|grasp|lift|transport|place|none",
      "root_cause": str,
      "key_frames": [tick, ...],
    }

``pass`` is sim ground truth (``probe_batch`` ``meta.success``), *never* the
LLM's opinion. The LLM is a multi-step tool-use agent (Claude Sonnet) that
actively investigates the rollout — replaying any tick, evaluating constructed
subgoals, reading gripper contacts and eef-relative distances, rendering frames —
to produce the *diagnosis* (failure_phase, root_cause, key_frames) and the
constructed-subgoal reasoning. The verdict is the sim's; the explanation is the
agent's.

Composition (these are the INTERFACES this file is built on — owned by sibling
workstreams, not edited here):

- ``ledger_query.read_subgoal_inputs(store_uri, namespace, world_id, run_id)``
  → ``{suite, task_id, seed, instruction, actions, ...}`` — the deterministic
  replay substrate (seed → init_state, ordered action sequence). [bench/libero/ledger_query.py]
- ``libero_replay.ReplayClient(suite, task_id).probe_batch(seed, actions, ticks,
  probes, frame_ticks, cams, eef_objects)`` → per-tick probes + ``meta.success``
  (the ground-truth verdict) over the deployed ``archetype-libero-replay`` app.
  [bench/libero/libero_replay.py]

The scorer's *tools* (the Claude tool-use surface) wrap slices of ``probe_batch``:

    replay_to(t)          → object_states/contacts/eef_to at tick t
    eval_subgoals(t)      → goal + constructed intermediate predicates at tick t
    object_states(t)      → object/region poses at tick t
    contacts(t)           → per-object gripper contact at tick t
    eef_to(obj, t)        → eef-relative distance to obj at tick t
    frame(t, cam)         → base64 PNG of the restored state
    find_grasp_attempts() → ticks where the policy commanded a gripper close

GATE (structural dry-run): if the ``anthropic-api-key`` secret is absent, the
scorer runs with a deterministic MOCK LLM that drives a fixed, representative
tool sequence and proves the loop + contract end to end on a smoke episode.
The live Sonnet run is gated on the secret. Run:

    python bench/libero/scorer.py                       # local mock dry-run
    modal run bench/libero/scorer.py                    # remote mock dry-run
    modal run bench/libero/scorer.py --live             # remote Sonnet (needs secret)
"""

# NOTE: no ``from __future__ import annotations`` — modal.parameter() validates
# real annotation objects, and PEP 563 string annotations break it. We keep this
# module consistent with the rest of bench/libero even though no modal.parameter
# is used here, so a future @app.cls refactor stays safe.

import json
import sys
from pathlib import Path
from typing import Any, Protocol

sys.path.insert(0, str(Path(__file__).parent))

# claude-sonnet → claude-sonnet-4-6 (the locked scorer model; see core-loop §5).
SCORER_MODEL = "claude-sonnet-4-6"
ANTHROPIC_SECRET_NAME = "anthropic-api-key"

# Modal is optional for the pure-Python structural gate: the dispatch loop,
# contract assembly, mock probe, and mock LLM all run with no Modal at all
# (``python bench/libero/scorer.py``). Modal is only needed to deploy/run the
# remote scorer (``modal run …``). When absent we install a tiny no-op shim so
# the @app.* decorators below are inert and the module still imports.
try:
    import modal  # type: ignore[import-untyped]

    _HAS_MODAL = True
except ImportError:  # pragma: no cover - exercised only without the modal client
    _HAS_MODAL = False


if _HAS_MODAL:
    from modal_worker import image as _libero_image  # noqa: E402

    # The scorer image: LIBERO (to replay) + the anthropic SDK (to reason). It
    # also carries this module and its sibling interfaces so the remote
    # container can import them.
    image = (
        _libero_image.pip_install("anthropic>=0.40")
        .add_local_python_source("modal_worker")
        .add_local_python_source("libero_replay")
        .add_local_python_source("ledger_query")
        .add_local_python_source("scorer")
    )
    app = modal.App("archetype-rollout-scorer", image=image)
else:

    class _NoModalApp:
        """Inert stand-in so @app.function / @app.local_entrypoint no-op when the
        modal client isn't installed (pure-Python local gate)."""

        def function(self, *_a: Any, **_k: Any):
            def _wrap(fn):
                return fn

            return _wrap

        def local_entrypoint(self, *_a: Any, **_k: Any):
            def _wrap(fn):
                return fn

            return _wrap

    app = _NoModalApp()  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Failure phases & contract types
# ---------------------------------------------------------------------------

FAILURE_PHASES = ("approach", "grasp", "lift", "transport", "place", "none")

# Maps a constructed-subgoal predicate name to the manipulation phase whose
# completion that subgoal witnesses. The scorer uses this to derive the
# failure_phase deterministically from the *last subgoal that flipped true*
# (so the phase boundary is grounded in sim, not the LLM's prose).
_SUBGOAL_PHASE = {
    "grasped": "grasp",
    "lifted": "lift",
    "up": "lift",
    "in": "transport",
    "on": "place",  # the goal predicate (place) — see _phase_for below
}


# ---------------------------------------------------------------------------
# The probe adapter — wraps the replay + ledger interfaces (or a mock)
# ---------------------------------------------------------------------------


class RolloutSource(Protocol):
    """What the scorer needs from a rollout to investigate it.

    The two production implementations differ only in *where* the substrate
    comes from: ``LedgerRolloutProbe`` reads it back from the append-only
    ledger by (world_id, run_id); ``DirectRolloutProbe`` takes
    (suite, task_id, seed, actions) inline. Both end up calling the same
    ``ReplayClient.probe_batch``.
    """

    def meta(self) -> dict[str, Any]:
        """Rollout metadata incl. ``success`` (sim ground truth), ``n_ticks``,
        ``goal_state``, ``obj_of_interest``, ``language``, ``instruction``."""
        ...

    def probe(
        self,
        ticks: list[int],
        probes: list[str] | None = None,
        frame_ticks: list[int] | None = None,
        cams: list[str] | None = None,
        eef_objects: list[str] | None = None,
    ) -> dict[str, Any]:
        """Return ``probe_batch``-shaped output for the requested ticks/probes."""
        ...


class _ReplayProbeBase:
    """Shared replay-backed probe over libero_replay.ReplayClient.

    Subclasses provide (suite, task_id, seed, action_sequence, instruction);
    this base owns the single replay round trip and a metadata cache so the
    ground-truth ``success`` is read exactly once.
    """

    def __init__(
        self,
        *,
        suite: str,
        task_id: int,
        seed: int,
        actions: list[list[float]],
        instruction: str,
        camera_size: int = 128,
    ) -> None:
        self._suite = suite
        self._task_id = task_id
        self._seed = seed
        self._actions = actions
        self._instruction = instruction
        self._camera_size = camera_size
        self._client = None
        self._meta_cache: dict[str, Any] | None = None

    def _replay_client(self):
        if self._client is None:
            from libero_replay import ReplayClient  # noqa: PLC0415

            self._client = ReplayClient(
                suite=self._suite, task_id=self._task_id, camera_size=self._camera_size
            )
        return self._client

    def meta(self) -> dict[str, Any]:
        if self._meta_cache is None:
            # One cheap final-tick probe fixes meta incl. ground-truth success.
            out = self._replay_client().probe_batch(
                self._seed, self._actions, ticks=[-1], probes=["eval_subgoals"]
            )
            meta = dict(out["meta"])
            meta["instruction"] = self._instruction
            self._meta_cache = meta
        return self._meta_cache

    def probe(
        self,
        ticks: list[int],
        probes: list[str] | None = None,
        frame_ticks: list[int] | None = None,
        cams: list[str] | None = None,
        eef_objects: list[str] | None = None,
    ) -> dict[str, Any]:
        return self._replay_client().probe_batch(
            self._seed,
            self._actions,
            ticks=ticks,
            probes=probes,
            frame_ticks=frame_ticks,
            cams=cams,
            eef_objects=eef_objects,
        )


class DirectRolloutProbe(_ReplayProbeBase):
    """Probe a rollout given (suite, task_id, seed, actions) inline.

    Use when the caller already holds the replay substrate (e.g. immediately
    after a rollout) and does not need a ledger round trip.
    """


class LedgerRolloutProbe(_ReplayProbeBase):
    """Probe a rollout addressed by (world_id, run_id) on the ledger.

    Reads the deterministic replay substrate via
    ``ledger_query.read_subgoal_inputs`` (seed + ordered actions), then replays.
    Construct with the async factory ``from_ledger`` since the ledger read is
    async.
    """

    @classmethod
    async def from_ledger(
        cls,
        *,
        store_uri: str,
        namespace: str,
        world_id: str,
        run_id: str,
        camera_size: int = 128,
    ) -> "LedgerRolloutProbe":
        from ledger_query import read_subgoal_inputs  # noqa: PLC0415

        sub = await read_subgoal_inputs(store_uri, namespace, world_id, run_id)
        if not sub.get("actions"):
            raise ValueError(
                f"no rollout found on ledger for world_id={world_id} run_id={run_id} "
                f"(store_uri={store_uri!r} namespace={namespace!r})"
            )
        return cls(
            suite=sub["suite"],
            task_id=sub["task_id"],
            seed=sub["seed"],
            actions=sub["actions"],
            instruction=sub.get("instruction", ""),
            camera_size=camera_size,
        )


# ---------------------------------------------------------------------------
# Mock probe — deterministic substrate for the structural dry-run gate
# ---------------------------------------------------------------------------


class MockRolloutProbe:
    """A deterministic, replay-free probe for the GATE dry-run.

    Models a representative *grasp failure*: the policy approaches the bowl,
    commands a gripper close at tick 12, but never achieves a two-finger grasp,
    so ``grasped`` / ``lifted`` / the goal predicate never flip true. This lets
    the tool loop + contract assembly run end to end with no Modal env and no
    LLM, proving the wiring before either is in play. Shapes mirror
    ``probe_batch`` exactly so swapping in the real probe is a no-op.
    """

    GOAL = ["on", "akita_black_bowl_1", "plate_1"]
    OOI = ["akita_black_bowl_1", "plate_1"]
    N_TICKS = 60
    GRASP_TICK = 12

    def meta(self) -> dict[str, Any]:
        return {
            "suite": "libero_spatial",
            "task_id": 0,
            "seed": 0,
            "language": "pick up the black bowl and place it on the plate",
            "instruction": "pick up the black bowl and place it on the plate",
            "n_ticks": self.N_TICKS,
            "n_init_states": 50,
            "done_at": self.N_TICKS,
            "success": False,  # sim ground truth: this rollout failed
            "obj_of_interest": list(self.OOI),
            "regions": ["main_table_between_plate_ramekin_region"],
            "goal_state": [list(self.GOAL)],
        }

    def _subgoals_at(self, t: int) -> list[dict[str, Any]]:
        # Grasp is attempted at GRASP_TICK but never holds → nothing flips true.
        grasped = False  # the failure: two-finger grasp never achieved
        lifted = False
        placed = False
        return [
            {"predicate": list(self.GOAL), "kind": "goal", "satisfied": placed},
            {
                "predicate": ["grasped", "akita_black_bowl_1"],
                "kind": "intermediate",
                "satisfied": grasped,
            },
            {
                "predicate": ["lifted", "akita_black_bowl_1"],
                "kind": "intermediate",
                "satisfied": lifted,
            },
        ]

    def _contacts_at(self, t: int) -> dict[str, Any]:
        # After the close command, one finger grazes the bowl but not both.
        touching = t >= self.GRASP_TICK
        return {
            "akita_black_bowl_1": {
                "any_gripper_contact": touching,
                "left_finger": touching,
                "right_finger": False,  # the miss: right finger never contacts
                "grasped": False,
            }
        }

    def _eef_to_at(self, t: int) -> dict[str, Any]:
        # eef descends toward the bowl, closes the gap by GRASP_TICK, then stalls.
        frac = min(t / self.GRASP_TICK, 1.0)
        dist = 0.25 * (1.0 - frac) + 0.02
        return {"akita_black_bowl_1": {"vector": [0.0, 0.0, -dist], "distance": dist}}

    def probe(
        self,
        ticks: list[int],
        probes: list[str] | None = None,
        frame_ticks: list[int] | None = None,
        cams: list[str] | None = None,
        eef_objects: list[str] | None = None,
    ) -> dict[str, Any]:
        probe_set = set(probes or ["object_states", "contacts", "eef_to", "eval_subgoals"])
        n = self.N_TICKS

        def _clamp(t: int) -> int:
            # Mirror ProbeReplay._clamp_tick exactly: -1 -> final tick (n).
            if t < 0:
                t = n + 1 + t
            return max(0, min(t, n))

        out_ticks: dict[str, Any] = {}
        for raw_t in ticks:
            t = _clamp(raw_t)
            entry: dict[str, Any] = {}
            if "object_states" in probe_set:
                entry["object_states"] = {
                    "akita_black_bowl_1": {"pos": [0.0, 0.0, 0.90]},
                    "plate_1": {"pos": [0.1, 0.0, 0.90]},
                }
            if "contacts" in probe_set:
                entry["contacts"] = self._contacts_at(t)
            if "eef_to" in probe_set:
                entry["eef_to"] = self._eef_to_at(t)
            if "eval_subgoals" in probe_set:
                entry["subgoals"] = self._subgoals_at(t)
            out_ticks[str(t)] = entry

        frames: dict[str, Any] = {}
        for raw_t in frame_ticks or []:
            t = _clamp(raw_t)
            frames[str(t)] = {cam: f"<mock-png:tick{t}:{cam}>" for cam in (cams or ["agentview"])}

        return {
            "meta": self.meta(),
            "ticks": out_ticks,
            "grasp_attempts": [self.GRASP_TICK],
            "frames": frames,
        }


# ---------------------------------------------------------------------------
# Tool surface (the Claude tool-use schema) + dispatch
# ---------------------------------------------------------------------------


def _tool_defs() -> list[dict[str, Any]]:
    """The investigation tools Claude can call. Each wraps a probe slice.

    Descriptions are prescriptive about *when* to call (per the Claude tool-use
    guidance) so the agent localizes failures efficiently instead of dumping
    every tick.
    """
    return [
        {
            "name": "rollout_meta",
            "description": (
                "Get the rollout's metadata: task language, instruction under test, "
                "goal predicate(s), objects of interest, named regions, total tick "
                "count, and the SIM GROUND-TRUTH success bool. Call this FIRST to "
                "orient before investigating."
            ),
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "eval_subgoals",
            "description": (
                "Evaluate the goal predicate plus constructed intermediate subgoals "
                "(grasped/lifted/in-region) at one or more ticks, via the native sim "
                "predicate evaluator. This is your primary tool: scan ticks to find "
                "WHERE a subgoal should have flipped true and didn't. Pass several "
                "ticks at once (e.g. [0, 15, 30, 45, -1]) to localize the phase, then "
                "narrow."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "ticks": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "Ticks to evaluate. -1 = final tick. Out-of-range is clamped.",
                    }
                },
                "required": ["ticks"],
                "additionalProperties": False,
            },
        },
        {
            "name": "contacts",
            "description": (
                "Per-object gripper contact at given ticks: any_gripper_contact, "
                "left_finger, right_finger, and grasped (both fingers). Call this "
                "around a grasp attempt to tell 'tried and missed' (one finger or no "
                "contact) from 'tried and held' (both fingers)."
            ),
            "input_schema": {
                "type": "object",
                "properties": {"ticks": {"type": "array", "items": {"type": "integer"}}},
                "required": ["ticks"],
                "additionalProperties": False,
            },
        },
        {
            "name": "eef_to",
            "description": (
                "End-effector-relative distance/vector to objects at given ticks "
                "(approach signal — 'grasp'/'approach' are not sim predicates, so use "
                "this to judge whether the gripper actually reached the object). "
                "Optionally restrict to specific objects."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "ticks": {"type": "array", "items": {"type": "integer"}},
                    "objects": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Objects to measure to (default: task-relevant objects).",
                    },
                },
                "required": ["ticks"],
                "additionalProperties": False,
            },
        },
        {
            "name": "object_states",
            "description": (
                "Object and named-region poses at given ticks. Use to check whether an "
                "object moved at all, drifted, or where it ended up relative to the goal."
            ),
            "input_schema": {
                "type": "object",
                "properties": {"ticks": {"type": "array", "items": {"type": "integer"}}},
                "required": ["ticks"],
                "additionalProperties": False,
            },
        },
        {
            "name": "find_grasp_attempts",
            "description": (
                "Return the ticks where the policy COMMANDED a gripper close (control "
                "intent), independent of whether contact was made. Pair with contacts() "
                "to diagnose grasp failures. Takes no arguments."
            ),
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "frame",
            "description": (
                "Render a camera frame of the restored state at a tick (base64 PNG). "
                "Heavy — call only for the 1-3 ticks you want to visually confirm the "
                "failure at (e.g. the grasp-attempt tick). camera defaults to agentview."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "tick": {"type": "integer"},
                    "camera": {"type": "string", "enum": ["agentview", "wrist"]},
                },
                "required": ["tick"],
                "additionalProperties": False,
            },
        },
        {
            "name": "submit_diagnosis",
            "description": (
                "Submit the final diagnosis once you have localized the failure. "
                "failure_phase is the phase where progress stopped; root_cause is a "
                "concise NL explanation of what happened and why; key_frames are the "
                "1-4 ticks a human should render to see it. Do NOT set pass — the sim "
                "provides ground truth; you provide diagnosis only."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "failure_phase": {"type": "string", "enum": list(FAILURE_PHASES)},
                    "root_cause": {"type": "string"},
                    "key_frames": {"type": "array", "items": {"type": "integer"}},
                },
                "required": ["failure_phase", "root_cause", "key_frames"],
                "additionalProperties": False,
            },
        },
    ]


def _dispatch_tool(source: RolloutSource, name: str, args: dict[str, Any]) -> dict[str, Any]:
    """Execute one tool call against the probe source. Returns a JSON-able dict.

    Pure data access — every branch is a slice of the same deterministic replay,
    so the agent's investigation is reproducible.
    """
    if name == "rollout_meta":
        return source.meta()

    if name == "eval_subgoals":
        out = source.probe(args["ticks"], probes=["eval_subgoals"])
        return {t: e.get("subgoals", []) for t, e in out["ticks"].items()}

    if name == "contacts":
        out = source.probe(args["ticks"], probes=["contacts"])
        return {t: e.get("contacts", {}) for t, e in out["ticks"].items()}

    if name == "eef_to":
        out = source.probe(args["ticks"], probes=["eef_to"], eef_objects=args.get("objects"))
        return {t: e.get("eef_to", {}) for t, e in out["ticks"].items()}

    if name == "object_states":
        out = source.probe(args["ticks"], probes=["object_states"])
        return {t: e.get("object_states", {}) for t, e in out["ticks"].items()}

    if name == "find_grasp_attempts":
        out = source.probe([-1], probes=["eval_subgoals"])
        return {"grasp_attempt_ticks": out.get("grasp_attempts", [])}

    if name == "frame":
        cam = args.get("camera", "agentview")
        out = source.probe([], frame_ticks=[args["tick"]], cams=[cam])
        return {"frames": out.get("frames", {})}

    return {"error": f"unknown tool {name!r}"}


# ---------------------------------------------------------------------------
# Ground-truth subgoal trace (independent of the LLM)
# ---------------------------------------------------------------------------


def _phase_for(predicate: list[str]) -> str:
    """Map a (goal or constructed) predicate to its manipulation phase."""
    head = predicate[0] if predicate else ""
    if head in _SUBGOAL_PHASE:
        # The goal 'on(obj, plate)' is the place phase; 'on(obj, region)' is a
        # transport waypoint. We treat any 'on' against the goal as place; the
        # subgoal-builder already names region waypoints with 'in', so 'on' here
        # is the goal predicate.
        return _SUBGOAL_PHASE[head]
    return "none"


def _ground_truth_subgoals(source: RolloutSource) -> tuple[list[dict[str, Any]], list[int]]:
    """Compute the subgoal trace + flipped_at_tick directly from the sim.

    Scans a coarse grid of ticks, evaluates every subgoal at each, and records
    the FIRST tick each subgoal becomes true (``flipped_at_tick``). This is the
    sim-derived backbone of the contract — the LLM never sets ``satisfied`` or
    ``flipped_at_tick``; it only explains them. Returns (subgoals, scanned_ticks).
    """
    meta = source.meta()
    n = int(meta.get("n_ticks", 0))
    # A coarse grid + the final tick; dense enough to localize a flip to the
    # nearest grid step, cheap enough to be one replay's worth of restores.
    step = max(1, n // 12)
    grid = sorted(set(list(range(0, n + 1, step)) + [n]))

    out = source.probe(grid, probes=["eval_subgoals"])
    # probe_batch returns ticks[<t>] = {object_states, contacts, eef_to, subgoals};
    # extract the subgoal list and key by the *actual* (clamped) restored tick.
    per_tick: dict[int, list[dict[str, Any]]] = {
        int(t): entry.get("subgoals", []) for t, entry in out["ticks"].items()
    }

    # key a subgoal by its predicate tuple
    def _key(sg: dict[str, Any]) -> tuple:
        return tuple(sg["predicate"])

    flipped: dict[tuple, int | None] = {}
    satisfied: dict[tuple, bool] = {}
    order: list[tuple] = []
    kinds: dict[tuple, str] = {}

    # Scan in ascending tick order so flipped_at_tick is the FIRST true tick.
    for t in sorted(per_tick):
        for sg in per_tick[t]:
            k = _key(sg)
            if k not in flipped:
                flipped[k] = None
                satisfied[k] = False
                kinds[k] = sg.get("kind", "intermediate")
                order.append(k)
            if sg.get("satisfied") is True:
                satisfied[k] = True
                if flipped[k] is None:
                    flipped[k] = t

    subgoals = [
        {
            "predicate": list(k),
            "kind": kinds[k],
            "satisfied": satisfied[k],
            "flipped_at_tick": flipped[k],
        }
        for k in order
    ]
    return subgoals, grid


def _derive_failure_phase(subgoals: list[dict[str, Any]], passed: bool) -> str:
    """Sim-grounded failure phase: the first phase whose subgoal never flipped.

    Phase order: grasp → lift → transport → place. On success → 'none'.
    """
    if passed:
        return "none"
    phase_order = ["grasp", "lift", "transport", "place"]
    # Build per-phase "did any subgoal of this phase ever satisfy?"
    phase_satisfied: dict[str, bool] = {p: False for p in phase_order}
    phase_present: dict[str, bool] = {p: False for p in phase_order}
    for sg in subgoals:
        phase = _phase_for(sg["predicate"])
        if phase in phase_satisfied:
            phase_present[phase] = True
            if sg["satisfied"]:
                phase_satisfied[phase] = True
    for phase in phase_order:
        if phase_present[phase] and not phase_satisfied[phase]:
            return phase
    # No constructed subgoal witnessed a stall (e.g. only the goal predicate
    # exists, or the failure is purely in approach with everything else absent).
    return "approach"


# ---------------------------------------------------------------------------
# The agentic loop
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are a robotics manipulation failure analyst. You diagnose WHY a "
    "Vision-Language-Action policy rollout failed (or, on success, confirm the "
    "trajectory). You investigate by calling tools that replay the rollout "
    "deterministically and probe the simulator at any tick: evaluate native "
    "subgoal predicates, read gripper contacts, measure end-effector distances, "
    "and render frames.\n\n"
    "The manipulation phases, in order, are: approach (reach the object), grasp "
    "(close both fingers on it), lift (raise it off the table), transport (move "
    "it over the target region), place (satisfy the goal predicate).\n\n"
    "Investigate efficiently: call rollout_meta first, then eval_subgoals across "
    "a coarse grid of ticks to find where a subgoal SHOULD have flipped true and "
    "didn't. Narrow to the failing phase, confirm with contacts/eef_to, and "
    "render at most a couple of frames at the decisive tick. Then call "
    "submit_diagnosis exactly once.\n\n"
    "IMPORTANT: the pass/fail verdict is sim ground truth and is set for you — "
    "your job is the diagnosis (failure_phase, root_cause, key_frames) and the "
    "reasoning about the constructed subgoals, NOT to judge success."
)


class MockLLM:
    """A deterministic stand-in for Claude that drives a fixed, representative
    tool sequence. Used by the GATE when the anthropic secret is absent.

    It mirrors what a competent agent does on a grasp failure: orient → scan
    subgoals → find grasp attempts → inspect contacts at the attempt → render
    a key frame → submit a grounded diagnosis. The sequence exercises every
    tool and every dispatch branch, proving the loop end to end.
    """

    def __init__(self) -> None:
        self._step = 0

    def turn(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        """Return a synthetic assistant turn: either tool_use blocks or a final
        submit_diagnosis. Mirrors the Anthropic content-block shape so the loop
        treats mock and live identically."""
        # Pull what the agent has learned so far out of the most recent tool
        # results so the mock's diagnosis is *derived*, not hardcoded.
        latest = _last_tool_results(messages)

        script: list[dict[str, Any]] = [
            {"id": "t0", "name": "rollout_meta", "input": {}},
            {"id": "t1", "name": "eval_subgoals", "input": {"ticks": [0, 15, 30, 45, -1]}},
            {"id": "t2", "name": "find_grasp_attempts", "input": {}},
            {"id": "t3", "name": "contacts", "input": {"ticks": [12, 13, 14]}},
            {"id": "t4", "name": "eef_to", "input": {"ticks": [12]}},
            {"id": "t5", "name": "frame", "input": {"tick": 12, "camera": "agentview"}},
        ]

        if self._step < len(script):
            call = script[self._step]
            self._step += 1
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": call["id"],
                        "name": call["name"],
                        "input": call["input"],
                    }
                ],
            }

        # Final turn: synthesize a diagnosis grounded in the gathered facts.
        diagnosis = _mock_diagnose(latest)
        return {
            "stop_reason": "tool_use",
            "content": [
                {
                    "type": "tool_use",
                    "id": "submit",
                    "name": "submit_diagnosis",
                    "input": diagnosis,
                }
            ],
        }


def _last_tool_results(messages: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Collect the most recent tool_result for each tool name from the running
    transcript, keyed by the tool name (decoded from the originating tool_use)."""
    # Map tool_use_id -> tool name from assistant turns.
    id_to_name: dict[str, str] = {}
    for msg in messages:
        if msg["role"] != "assistant":
            continue
        for block in msg["content"]:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                id_to_name[block["id"]] = block["name"]
    results: dict[str, dict[str, Any]] = {}
    for msg in messages:
        if msg["role"] != "user":
            continue
        content = msg["content"]
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                name = id_to_name.get(block["tool_use_id"], "?")
                try:
                    results[name] = json.loads(block["content"])
                except (TypeError, ValueError, KeyError):
                    results[name] = {}
    return results


def _mock_diagnose(facts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Derive a diagnosis from the gathered tool results (mock LLM brain).

    Reads the contacts probe to tell apart a missed grasp (one finger) from no
    contact at all, and the grasp-attempt tick for the key frame. This is the
    deterministic analogue of Claude's reasoning over the same evidence.
    """
    contacts = facts.get("contacts", {})
    attempts = facts.get("find_grasp_attempts", {}).get("grasp_attempt_ticks", [])
    attempt_tick = attempts[0] if attempts else 12

    # Inspect the contact record at the attempt tick (string keys).
    one_finger = False
    no_contact = True
    for _t, per_obj in contacts.items():
        for _obj, c in per_obj.items():
            if c.get("any_gripper_contact"):
                no_contact = False
            if c.get("left_finger") != c.get("right_finger"):
                one_finger = True

    if no_contact:
        root = (
            f"The policy commanded a gripper close at tick {attempt_tick} but the "
            f"end-effector never made contact with the bowl — the grasp closed on "
            f"empty space. Approach under-reached the object."
        )
        phase = "grasp"
    elif one_finger:
        root = (
            f"At the grasp attempt (tick {attempt_tick}) only one finger contacted "
            f"the bowl; the two-finger grasp never closed, so 'grasped' and "
            f"'lifted' never became true and the bowl was never moved to the plate. "
            f"The gripper was mis-aligned at closure."
        )
        phase = "grasp"
    else:
        root = (
            f"The grasp held at tick {attempt_tick} but the goal predicate "
            f"on(bowl, plate) never satisfied — the failure is downstream of grasp."
        )
        phase = "transport"

    return {
        "failure_phase": phase,
        "root_cause": root,
        "key_frames": [attempt_tick],
    }


def _anthropic_turn(client: Any, messages: list[dict[str, Any]]) -> dict[str, Any]:
    """One real Claude turn over the tool surface. Returns a normalized dict
    with ``stop_reason`` and ``content`` (list of content-block dicts)."""
    resp = client.messages.create(
        model=SCORER_MODEL,
        max_tokens=4096,
        thinking={"type": "adaptive"},
        system=_SYSTEM_PROMPT,
        tools=_tool_defs(),
        messages=messages,
    )
    # Normalize SDK blocks to plain dicts so the loop is identical to the mock.
    content: list[dict[str, Any]] = []
    for block in resp.content:
        if block.type == "tool_use":
            content.append(
                {"type": "tool_use", "id": block.id, "name": block.name, "input": block.input}
            )
        elif block.type == "text":
            content.append({"type": "text", "text": block.text})
        # thinking blocks are not replayed back into the transcript for this
        # short loop; the diagnosis arrives via the submit_diagnosis tool.
    return {"stop_reason": resp.stop_reason, "content": content}


def score_rollout(
    source: RolloutSource,
    *,
    llm: Any | None = None,
    max_turns: int = 12,
) -> dict[str, Any]:
    """Run the agentic scorer over one rollout and return the locked contract.

    Args:
        source: a RolloutSource (DirectRolloutProbe / LedgerRolloutProbe / mock).
        llm: an object with ``.turn(messages) -> {stop_reason, content}``. If
            None, a MockLLM is used (the deterministic dry-run brain). A real
            Anthropic client is wrapped by ``score_rollout_live`` instead.
        max_turns: safety bound on the tool-use loop.

    Returns the contract dict. ``pass``/``subgoals``/``flipped_at_tick`` come
    from the sim; ``failure_phase``/``root_cause``/``key_frames`` from the agent
    (with a sim-grounded ``failure_phase`` fallback if the agent's choice is
    inconsistent with the trace).
    """
    if llm is None:
        llm = MockLLM()

    # 1. Sim ground truth — computed up front, independent of the LLM.
    meta = source.meta()
    passed = bool(meta.get("success", False))
    gt_subgoals, _grid = _ground_truth_subgoals(source)

    # 2. Agentic investigation: drive the tool loop until submit_diagnosis.
    user_kickoff = (
        "Diagnose this rollout. The instruction under test was: "
        f"{meta.get('instruction') or meta.get('language')!r}. "
        "Start with rollout_meta, then investigate."
    )
    messages: list[dict[str, Any]] = [{"role": "user", "content": user_kickoff}]

    diagnosis: dict[str, Any] | None = None
    turns = 0
    while turns < max_turns:
        turns += 1
        turn = llm.turn(messages)
        content = turn["content"]
        messages.append({"role": "assistant", "content": content})

        tool_uses = [b for b in content if b.get("type") == "tool_use"]
        if not tool_uses:
            # Agent emitted only text with no tool call — nudge once, then stop.
            if turn.get("stop_reason") == "end_turn":
                break
            continue

        tool_results: list[dict[str, Any]] = []
        for tu in tool_uses:
            if tu["name"] == "submit_diagnosis":
                diagnosis = dict(tu["input"])
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tu["id"],
                        "content": json.dumps({"received": True}),
                    }
                )
            else:
                try:
                    result = _dispatch_tool(source, tu["name"], tu["input"])
                except Exception as exc:  # noqa: BLE001
                    result = {"error": str(exc)}
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tu["id"],
                        "content": json.dumps(result, default=str),
                    }
                )

        messages.append({"role": "user", "content": tool_results})
        if diagnosis is not None:
            break

    # 3. Assemble the locked contract. The agent supplies diagnosis fields; the
    #    sim supplies the verdict + subgoal trace.
    if diagnosis is None:
        diagnosis = {
            "failure_phase": _derive_failure_phase(gt_subgoals, passed),
            "root_cause": "agent did not submit a diagnosis within the turn budget",
            "key_frames": [],
        }

    failure_phase = diagnosis.get("failure_phase", "none")
    if passed:
        # On success the phase is always 'none' regardless of the agent's prose.
        failure_phase = "none"
    elif failure_phase not in FAILURE_PHASES:
        failure_phase = _derive_failure_phase(gt_subgoals, passed)

    return {
        "pass": passed,  # SIM GROUND TRUTH — never the LLM's opinion
        "subgoals": [
            {
                "predicate": sg["predicate"],
                "satisfied": sg["satisfied"],
                "flipped_at_tick": sg["flipped_at_tick"],
            }
            for sg in gt_subgoals
        ],
        "failure_phase": failure_phase,
        "root_cause": str(diagnosis.get("root_cause", "")),
        "key_frames": [
            int(t) for t in diagnosis.get("key_frames", []) if isinstance(t, (int, float))
        ],
        # Provenance: who produced the diagnosis + how many investigation steps.
        "diagnosed_by": "mock" if isinstance(llm, MockLLM) else SCORER_MODEL,
        "investigation_turns": turns,
    }


def score_rollout_live(source: RolloutSource, **kw: Any) -> dict[str, Any]:
    """Score with the real Claude Sonnet agent (requires ANTHROPIC_API_KEY)."""
    import anthropic  # noqa: PLC0415

    client = anthropic.Anthropic()

    class _LiveLLM:
        def turn(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
            return _anthropic_turn(client, messages)

    return score_rollout(source, llm=_LiveLLM(), **kw)


# ---------------------------------------------------------------------------
# Modal entry points — the deployed scorer + the dry-run gate
# ---------------------------------------------------------------------------

# The anthropic-api-key secret is attached only when modal is present. Its
# absence is the GATE trigger: a deploy without the secret runs the mock path.
_SCORER_SECRETS = [modal.Secret.from_name(ANTHROPIC_SECRET_NAME)] if _HAS_MODAL else []


@app.function(timeout=900, secrets=_SCORER_SECRETS)
def score_ledger_rollout(
    store_uri: str,
    namespace: str,
    world_id: str,
    run_id: str,
    live: bool = True,
) -> dict[str, Any]:
    """Score one persisted rollout by (world_id, run_id) — the production path.

    Reads the deterministic substrate from the ledger, replays via the deployed
    archetype-libero-replay app, and runs the live Sonnet agent. This is the
    function the GEPA loop calls per val rollout.
    """
    import asyncio

    source = asyncio.run(
        LedgerRolloutProbe.from_ledger(
            store_uri=store_uri, namespace=namespace, world_id=world_id, run_id=run_id
        )
    )
    return score_rollout_live(source) if live else score_rollout(source)


@app.function(timeout=600)
def dry_run() -> dict[str, Any]:
    """The structural GATE, run remotely. Mock probe + mock LLM — no env, no
    secret. Proves the tool loop + contract output end to end."""
    return _run_dry_run()


def _run_dry_run() -> dict[str, Any]:
    """Mock probe + mock LLM dry run. Returns the contract and a self-check."""
    source = MockRolloutProbe()
    report = score_rollout(source, llm=MockLLM())
    checks = _self_check(report)
    return {"report": report, "self_check": checks, "ok": all(checks.values())}


def _self_check(report: dict[str, Any]) -> dict[str, bool]:
    """Structural assertions the contract must satisfy."""
    keys_ok = {"pass", "subgoals", "failure_phase", "root_cause", "key_frames"} <= set(report)
    pass_is_bool = isinstance(report.get("pass"), bool)
    phase_ok = report.get("failure_phase") in FAILURE_PHASES
    subgoals = report.get("subgoals", [])
    subgoals_shaped = isinstance(subgoals, list) and all(
        {"predicate", "satisfied", "flipped_at_tick"} <= set(sg) for sg in subgoals
    )
    # The mock is a known FAILURE → pass must be False and phase must be grasp.
    verdict_ok = report.get("pass") is False
    phase_is_grasp = report.get("failure_phase") == "grasp"
    root_cause_nonempty = bool(report.get("root_cause"))
    key_frames_ok = isinstance(report.get("key_frames"), list) and len(report["key_frames"]) >= 1
    return {
        "contract_keys_present": keys_ok,
        "pass_is_bool": pass_is_bool,
        "failure_phase_valid": phase_ok,
        "subgoals_shaped": subgoals_shaped,
        "verdict_is_ground_truth_fail": verdict_ok,
        "failure_phase_localized_to_grasp": phase_is_grasp,
        "root_cause_nonempty": root_cause_nonempty,
        "key_frames_present": key_frames_ok,
    }


@app.local_entrypoint()
def main(
    live: bool = False,
    store_uri: str = "",
    namespace: str = "",
    world_id: str = "",
    run_id: str = "",
) -> None:
    """Modal entry. Default: remote MOCK dry-run (the gate). With --live and a
    (store_uri, namespace, world_id, run_id) it scores a real ledger rollout
    with Sonnet (requires the anthropic-api-key secret)."""
    if live and world_id and run_id:
        report = score_ledger_rollout.remote(store_uri, namespace, world_id, run_id, live=True)
        print(json.dumps(report, indent=2, sort_keys=True))
        return

    out = dry_run.remote()
    print(json.dumps(out, indent=2, sort_keys=True))
    if not out["ok"]:
        raise SystemExit("dry-run self-check FAILED")
    print("\nGATE PASSED (remote mock): tool loop + contract output verified.")
    print("Live Sonnet run is gated on the anthropic-api-key secret.")


if __name__ == "__main__":
    # Local (no Modal) structural dry-run — the cheapest gate.
    out = _run_dry_run()
    print(json.dumps(out, indent=2, sort_keys=True))
    if not out["ok"]:
        raise SystemExit("dry-run self-check FAILED")
    print("\nGATE PASSED (local mock): tool loop + contract output verified.")
    print("Live Sonnet run is gated on the anthropic-api-key secret.")
