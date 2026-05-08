# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the dry4clj-port oracle used by evals.suites.dry_detection."""

from __future__ import annotations

from pathlib import Path

from evals.dry import find_duplicates, fingerprint_module, fingerprint_paths
from evals.dry.oracle import jaccard

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "evals" / "dry" / "fixtures"


def test_jaccard_self_is_one():
    a = fingerprint_module("def f(x):\n    return x + 1\n", path="m.py")[0]
    assert jaccard(a.features, a.features) == 1.0


def test_jaccard_empty_returns_zero():
    from collections import Counter

    assert jaccard(Counter(), Counter()) == 0.0


def test_rename_only_scores_one():
    a = fingerprint_module("def add(x, y):\n    z = x + y\n    return z\n", path="a.py")[0]
    b = fingerprint_module("def sum2(p, q):\n    r = p + q\n    return r\n", path="b.py")[0]
    assert jaccard(a.features, b.features) == 1.0


def test_literal_change_scores_one():
    a = fingerprint_module("def f(x):\n    return x + 1\n", path="a.py")[0]
    b = fingerprint_module("def g(y):\n    return y + 999\n", path="b.py")[0]
    assert jaccard(a.features, b.features) == 1.0


def test_structural_change_drops_score():
    a = fingerprint_module("def f(x):\n    if x:\n        return 1\n    return 0\n", path="a.py")[0]
    b = fingerprint_module(
        "def g(x):\n    for i in x:\n        print(i)\n    return 0\n", path="b.py"
    )[0]
    assert jaccard(a.features, b.features) < 0.6


def test_fingerprint_module_picks_up_methods_and_nested():
    src = (
        "def outer():\n"
        "    def inner():\n"
        "        return 1\n"
        "    return inner\n"
        "\n"
        "class C:\n"
        "    def method(self):\n"
        "        return 2\n"
        "    async def amethod(self):\n"
        "        return 3\n"
    )
    forms = fingerprint_module(src, path="x.py")
    names = {f.name for f in forms}
    assert names == {"outer", "inner", "method", "amethod"}


def test_fixture_has_five_ground_truth_pairs():
    paths = sorted(p for p in FIXTURE_DIR.glob("*.py") if p.name != "__init__.py")
    forms = fingerprint_paths(paths)
    pairs = find_duplicates(forms, threshold=0.82, min_nodes=12)

    pair_names = {frozenset({p.a.name, p.b.name}) for p in pairs}
    expected = {
        frozenset({"restock_item", "fulfill_order"}),
        frozenset({"format_summary", "render_receipt"}),
        frozenset({"format_summary", "format_label"}),
        frozenset({"render_receipt", "format_label"}),
        frozenset({"deactivate", "cancel"}),
    }
    assert pair_names == expected
    assert all(p.score >= 0.99 for p in pairs)


def test_min_nodes_filters_short_forms():
    src = "def tiny():\n    return 1\n" * 3 + "def longer(x, y):\n    z = x + y\n    return z\n"
    forms = fingerprint_module(src, path="x.py")
    pairs = find_duplicates(forms, threshold=0.5, min_nodes=12)
    # Tiny forms are filtered out, so no pairs survive even at low threshold.
    assert pairs == []
