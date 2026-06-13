# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit gate for bench/libero/replay_determinism.py — the pure local logic.

The full proof (query a persisted rollout, replay it through a fresh LIBERO env,
assert ``check_success() == ledger success`` and object_states reproduce) needs
Modal + LIBERO and runs as the ``modal run`` gate. These tests cover the two
load-bearing *local* helpers that the gate's verdict hinges on — action
alignment and the object_states byte-identity check — with no Modal/LIBERO/GPU.

``replay_determinism`` builds ``modal.App``/``modal.Volume`` and imports the
deployed ``ReplayClient`` at module load, so the whole module needs the ``modal``
client package. ``modal`` is a separately-installed CLI tool, not a project
dependency (same posture as the rest of the bench/libero Modal layer), so these
tests ``importorskip("modal")`` and skip in a bare CI env.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# modal is a separately-installed CLI tool, not a project dep — skip cleanly
# where it is absent (bare CI), run where it is present (the dev box).
pytest.importorskip("modal")

# bench/libero is a directory of loose scripts (no __init__.py); import by path.
_BENCH_LIBERO = Path(__file__).resolve().parents[2] / "bench" / "libero"
if str(_BENCH_LIBERO) not in sys.path:
    sys.path.insert(0, str(_BENCH_LIBERO))

from replay_determinism import (  # type: ignore[import-not-found]  # noqa: E402
    _applied_action_sequence,
    _object_states_identical,
)

# ---------------------------------------------------------------------------
# Action alignment — the subtle "which actions were actually stepped" contract
# ---------------------------------------------------------------------------


def test_applied_sequence_drops_the_unstepped_final_action() -> None:
    """Ledger tick t carries (a_t, p_t) with p_t = step(a_{t-1}); the final
    recorded action a_T is never stepped, so the applied sequence is actions[:-1]."""
    actions = [[0.0] * 7, [1.0] * 7, [2.0] * 7, [3.0] * 7]
    applied = _applied_action_sequence(actions)
    assert applied == [[0.0] * 7, [1.0] * 7, [2.0] * 7]
    # Replaying `applied` (3 steps) reproduces ledger ticks 0..3 (reset + 3 steps).
    assert len(applied) == len(actions) - 1


def test_applied_sequence_degenerate_single_row() -> None:
    """A one-row rollout (only the reset tick) has no applied action."""
    assert _applied_action_sequence([[0.0] * 7]) == []
    assert _applied_action_sequence([]) == []


# ---------------------------------------------------------------------------
# object_states byte-identity — "object_states reproduce" determinism check
# ---------------------------------------------------------------------------


def _scene(z: float) -> dict:
    return {
        "akita_black_bowl_1": {
            "pos": [0.1, 0.2, z],
            "quat": [1.0, 0.0, 0.0, 0.0],
            "is_region": False,
        },
        "plate_1": {"pos": [0.3, 0.4, 0.5], "quat": [1.0, 0.0, 0.0, 0.0], "is_region": False},
        "table_region": {"pos": [0.0, 0.0, 0.0], "quat": [1.0, 0.0, 0.0, 0.0], "is_region": True},
    }


def test_object_states_identical_when_equal() -> None:
    ok, msg = _object_states_identical(_scene(0.9), _scene(0.9))
    assert ok, msg
    assert "identical" in msg


def test_object_states_differ_on_pose() -> None:
    ok, msg = _object_states_identical(_scene(0.9), _scene(0.9001))
    assert not ok
    # The diff message must pinpoint the object that moved.
    assert "akita_black_bowl_1" in msg


def test_object_states_differ_on_key_set() -> None:
    a = _scene(0.9)
    b = _scene(0.9)
    del b["plate_1"]
    ok, msg = _object_states_identical(a, b)
    assert not ok
    assert "plate_1" in msg
