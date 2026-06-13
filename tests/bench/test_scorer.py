"""Contract + tool-loop tests for the agentic rollout scorer.

These run with no Modal, no anthropic secret, and no LIBERO env: the scorer's
dispatch loop, contract assembly, and sim-grounded subgoal trace are exercised
against the in-process ``MockRolloutProbe`` and ``MockLLM``. This is the CI-safe
half of the structural dry-run gate (the remote half lives in
``bench/libero/scorer.py::dry_run``).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# bench/libero is a flat script dir (no __init__.py); put it on the path so the
# scorer's bare sibling imports resolve, then import the module by name.
_LIBERO = Path(__file__).resolve().parents[2] / "bench" / "libero"
if str(_LIBERO) not in sys.path:
    sys.path.insert(0, str(_LIBERO))

import scorer  # noqa: E402


def test_dry_run_gate_passes_and_self_check_green():
    """The full pure-Python gate: mock probe + mock LLM → valid contract."""
    out = scorer._run_dry_run()
    assert out["ok"] is True
    assert all(out["self_check"].values()), out["self_check"]


def test_contract_shape_is_locked():
    out = scorer._run_dry_run()
    report = out["report"]
    assert {"pass", "subgoals", "failure_phase", "root_cause", "key_frames"} <= set(report)
    assert isinstance(report["pass"], bool)
    assert report["failure_phase"] in scorer.FAILURE_PHASES
    for sg in report["subgoals"]:
        assert {"predicate", "satisfied", "flipped_at_tick"} <= set(sg)
        assert isinstance(sg["satisfied"], bool)
        assert sg["flipped_at_tick"] is None or isinstance(sg["flipped_at_tick"], int)


def test_verdict_is_sim_ground_truth_not_llm():
    """The mock rollout is a known failure; pass must be False regardless of any
    diagnosis the (mock) LLM produces — pass comes from the sim."""
    report = scorer.score_rollout(scorer.MockRolloutProbe(), llm=scorer.MockLLM())
    assert report["pass"] is False
    assert report["diagnosed_by"] == "mock"


def test_failure_phase_localized_to_grasp():
    report = scorer.score_rollout(scorer.MockRolloutProbe(), llm=scorer.MockLLM())
    assert report["failure_phase"] == "grasp"
    assert report["key_frames"] == [scorer.MockRolloutProbe.GRASP_TICK]


def test_tool_loop_invokes_every_tool():
    """The mock LLM script must drive a multi-step investigation, not a one-shot."""
    report = scorer.score_rollout(scorer.MockRolloutProbe(), llm=scorer.MockLLM())
    # orient + 5 probes + submit = 7 agent turns.
    assert report["investigation_turns"] == 7


def test_dispatch_tool_branches_return_probe_shaped_data():
    """Every tool dispatch branch returns JSON-able, probe-shaped output."""
    src = scorer.MockRolloutProbe()
    assert src.meta()["success"] is False
    subg = scorer._dispatch_tool(src, "eval_subgoals", {"ticks": [0, -1]})
    assert set(subg) == {"0", "60"}
    assert all(isinstance(v, list) for v in subg.values())
    contacts = scorer._dispatch_tool(src, "contacts", {"ticks": [12]})
    assert contacts["12"]["akita_black_bowl_1"]["grasped"] is False
    eef = scorer._dispatch_tool(src, "eef_to", {"ticks": [12]})
    assert "distance" in eef["12"]["akita_black_bowl_1"]
    grasp = scorer._dispatch_tool(src, "find_grasp_attempts", {})
    assert grasp["grasp_attempt_ticks"] == [scorer.MockRolloutProbe.GRASP_TICK]
    frame = scorer._dispatch_tool(src, "frame", {"tick": 12, "camera": "agentview"})
    assert "12" in frame["frames"]


def _success_probe() -> Any:
    """A mock probe that reports a SUCCESSFUL rollout (subgoals all flip true)."""

    class SuccessProbe(scorer.MockRolloutProbe):
        def meta(self) -> dict[str, Any]:
            m = super().meta()
            m["success"] = True
            return m

        def _subgoals_at(self, t: int) -> list[dict[str, Any]]:
            grasped = t >= 12
            lifted = t >= 20
            placed = t >= 40
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

    return SuccessProbe()


def test_success_rollout_phase_is_none_and_flips_recorded():
    """On a passing rollout: pass=True, failure_phase=none, and each subgoal's
    flipped_at_tick is the first scanned tick where it became true."""
    report = scorer.score_rollout(_success_probe(), llm=scorer.MockLLM())
    assert report["pass"] is True
    assert report["failure_phase"] == "none"
    flips = {tuple(sg["predicate"]): sg["flipped_at_tick"] for sg in report["subgoals"]}
    # grasped flips at >=12, lifted at >=20, goal at >=40 (snapped to the scan grid).
    assert flips[("grasped", "akita_black_bowl_1")] is not None
    assert flips[("lifted", "akita_black_bowl_1")] is not None
    assert flips[("on", "akita_black_bowl_1", "plate_1")] is not None
    assert all(sg["satisfied"] for sg in report["subgoals"])
