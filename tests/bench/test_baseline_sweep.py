# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the baseline-sweep split logic and instruction threading.

CI-safe (no Modal, no GPU): exercises the pure ``build_split`` policy and the
``_resolve_instruction`` resolution order. Both are the load-bearing decisions
of WS2 — the split determines what GEPA optimizes against, and the instruction
fix is the honest baseline (core-loop §2, §4).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make bench/libero importable.
_BENCH_LIBERO = str(Path(__file__).resolve().parents[2] / "bench" / "libero")
if _BENCH_LIBERO not in sys.path:
    sys.path.insert(0, _BENCH_LIBERO)

from baseline_sweep import build_split  # type: ignore[import-not-found]  # noqa: E402
from eval_driver import _resolve_instruction  # type: ignore[import-not-found]  # noqa: E402


def _mk(task_id: int, success_rate: float, n: int = 3):
    return {
        "task_id": task_id,
        "n": n,
        "successes": round(success_rate * n),
        "success_rate": success_rate,
    }


# ---------------------------------------------------------------------------
# build_split
# ---------------------------------------------------------------------------


def test_val_is_hardest_tasks():
    """val must be the lowest-success tasks (most room to climb)."""
    per_task = [
        _mk(0, 0.9),
        _mk(1, 0.1),
        _mk(2, 0.5),
        _mk(3, 0.0),
        _mk(4, 0.7),
    ]
    split = build_split(per_task, suite="libero_spatial", val_size=2, test_size=1)
    # Hardest two: task 3 (0.0) and task 1 (0.1).
    assert split["val_task_ids"] == [1, 3]


def test_val_and_test_are_disjoint():
    per_task = [_mk(i, i / 10.0) for i in range(10)]
    split = build_split(per_task, suite="s", val_size=4, test_size=3)
    val = set(split["val_task_ids"])
    test = set(split["test_task_ids"])
    assert val.isdisjoint(test), f"val {val} and test {test} overlap"
    assert len(val) == 4
    assert len(test) == 3


def test_test_drawn_from_non_val_tasks():
    """test tasks must never be the val (hardest) tasks — no leakage."""
    per_task = [_mk(i, i / 10.0) for i in range(10)]
    split = build_split(per_task, suite="s", val_size=4, test_size=3)
    hardest = {0, 1, 2, 3}  # rates 0.0..0.3
    assert set(split["val_task_ids"]) == hardest
    assert set(split["test_task_ids"]).isdisjoint(hardest)


def test_test_is_distribution_matched_not_all_easiest():
    """test should spread across remaining difficulty, not cluster at the top."""
    per_task = [_mk(i, i / 10.0) for i in range(10)]
    split = build_split(per_task, suite="s", val_size=4, test_size=3)
    # Remaining after val = tasks 4..9 (rates 0.4..0.9). Even strides over 6
    # remaining → indices {0, 2 or 3, 5} → tasks {4, 6/7, 9}. The key property:
    # test spans both ends of the remaining range, not three adjacent easies.
    test_ids = split["test_task_ids"]
    assert min(test_ids) <= 5, f"test should include a harder remaining task: {test_ids}"
    assert max(test_ids) >= 8, f"test should include an easy remaining task: {test_ids}"


def test_deterministic_tie_break_by_task_id():
    """Equal success rates break ties by task_id, deterministically."""
    per_task = [_mk(2, 0.0), _mk(0, 0.0), _mk(1, 0.0), _mk(3, 0.9)]
    split = build_split(per_task, suite="s", val_size=2, test_size=1)
    # All three hardest tie at 0.0; lowest task_ids win val slots.
    assert split["val_task_ids"] == [0, 1]


def test_per_task_and_overall_recorded():
    per_task = [_mk(0, 1.0, n=2), _mk(1, 0.0, n=2)]
    split = build_split(per_task, suite="libero_spatial", val_size=1, test_size=1)
    assert split["per_task"]["0"]["success_rate"] == 1.0
    assert split["per_task"]["1"]["success_rate"] == 0.0
    # 2 successes out of 4 trials = 0.5 overall.
    assert split["baseline"]["overall_success_rate"] == 0.5
    assert split["suite"] == "libero_spatial"


def test_split_sizes_clamped_to_available():
    """Requesting more val/test than tasks must not error or overlap."""
    per_task = [_mk(0, 0.5), _mk(1, 0.5)]
    split = build_split(per_task, suite="s", val_size=5, test_size=5)
    val = set(split["val_task_ids"])
    test = set(split["test_task_ids"])
    assert val.isdisjoint(test)
    assert len(val) <= 2
    assert len(val) + len(test) <= 2


# ---------------------------------------------------------------------------
# _resolve_instruction
# ---------------------------------------------------------------------------


class _FakeEnv:
    def __init__(self, language: str):
        self._language = language

    def task_language(self) -> str:
        return self._language


class _NoLanguageEnv:
    pass


def test_resolve_defaults_to_task_language():
    """Default baseline = the env's raw task_language (never empty)."""
    env = _FakeEnv("pick up the black bowl and place it on the plate")
    instr = _resolve_instruction(
        suite="libero_spatial", task_id=0, env_client=env, override_map=None
    )
    assert instr == "pick up the black bowl and place it on the plate"


def test_resolve_override_takes_precedence():
    """A per-(suite, task_id) override (the GEPA paraphrase) wins over the raw."""
    env = _FakeEnv("raw libero instruction")
    overrides = {("libero_spatial", 0): "grasp the dark bowl, set it on the dish"}
    instr = _resolve_instruction(
        suite="libero_spatial", task_id=0, env_client=env, override_map=overrides
    )
    assert instr == "grasp the dark bowl, set it on the dish"


def test_resolve_override_miss_falls_back_to_task_language():
    """An override map without this task's key falls back to the raw instruction."""
    env = _FakeEnv("raw libero instruction")
    overrides = {("libero_spatial", 5): "some other paraphrase"}
    instr = _resolve_instruction(
        suite="libero_spatial", task_id=0, env_client=env, override_map=overrides
    )
    assert instr == "raw libero instruction"


def test_resolve_no_task_language_returns_empty():
    """Scripted envs without task_language fall back to empty (off the policy path)."""
    instr = _resolve_instruction(
        suite="scripted", task_id=0, env_client=_NoLanguageEnv(), override_map=None
    )
    assert instr == ""
