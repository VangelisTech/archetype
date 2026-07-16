# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure-python contract tests for the LIBERO-Pro loader (no libero import).

The load-bearing contract is THE LANGUAGE TRAP: perturbed BDDLs keep original
filenames but carry the perturbed instruction in ``(:language ...)`` — a
filename-derived loader evaluates the wrong instruction on perturbed scenes.
"""

from __future__ import annotations

import pytest

from bench.libero.pro_suite import ProTask, _discover_tasks, parse_language

_BDDL = """(define (problem LIBERO_Kitchen_Tabletop_Manipulation)
  (:domain robosuite)
  (:language lift the black bowl between the plate and ramekin and set it on the plate)
  (:objects obj1)
)"""


def test_parse_language_prefers_bddl_field_over_filename():
    lang = parse_language(_BDDL, "pick_up_the_black_bowl_and_place_it.bddl")
    assert lang == "lift the black bowl between the plate and ramekin and set it on the plate"


def test_parse_language_falls_back_to_filename_rules():
    assert (
        parse_language("(define (problem X))", "pick_up_the_black_bowl.bddl")
        == "pick up the black bowl"
    )
    # LIBERO-100-style uppercase SCENE names strip the scene prefix.
    assert (
        parse_language("(define (problem X))", "KITCHEN_SCENE3_turn_on_the_stove.bddl")
        == "turn on the stove"
    )


def test_discover_tasks_pairs_and_parses(tmp_path):
    variant = "libero_spatial_lan"
    (tmp_path / "bddl_files" / variant).mkdir(parents=True)
    (tmp_path / "init_files" / variant).mkdir(parents=True)
    (tmp_path / "bddl_files" / variant / "pick_up_the_bowl.bddl").write_text(_BDDL)
    (tmp_path / "init_files" / variant / "pick_up_the_bowl.pruned_init").write_bytes(b"x")

    variants = _discover_tasks(tmp_path)
    assert list(variants) == [variant]
    (task,) = variants[variant]
    assert task == ProTask(
        name="pick_up_the_bowl",
        language="lift the black bowl between the plate and ramekin and set it on the plate",
        bddl_file="pick_up_the_bowl.bddl",
        init_states_file="pick_up_the_bowl.pruned_init",
    )


def test_discover_tasks_fails_loudly_on_unpaired_bddl(tmp_path):
    variant = "libero_goal_swap"
    (tmp_path / "bddl_files" / variant).mkdir(parents=True)
    (tmp_path / "init_files" / variant).mkdir(parents=True)
    (tmp_path / "bddl_files" / variant / "orphan.bddl").write_text(_BDDL)

    with pytest.raises(FileNotFoundError, match="unpaired"):
        _discover_tasks(tmp_path)
