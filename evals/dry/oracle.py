# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Python port of unclebob/dry4clj's structural duplication scorer.

Each function-level form (def / async def, including nested) is reduced
to a multiset of structural edges in its AST.  Identifiers and literals
are replaced with generic markers so that two forms with different names
and constants but identical structure score 1.0.  Pairs of forms are
scored with multiset Jaccard similarity; pairs at or above ``threshold``
are reported.
"""

from __future__ import annotations

import ast
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

# Defaults track dry4clj.  ``threshold`` is the Jaccard floor for
# reporting a pair; ``min_nodes`` filters trivially small forms whose
# fingerprints would over-match by accident.
DEFAULT_THRESHOLD = 0.82
DEFAULT_MIN_NODES = 12


@dataclass(frozen=True)
class FormFingerprint:
    """Structural fingerprint of a single function-level form.

    ``form_id`` is a human-readable handle (``path:lineno:name``) used
    in graded output.  ``features`` is the multiset of normalized AST
    edges that participates in the Jaccard score.
    """

    form_id: str
    path: str
    name: str
    lineno: int
    end_lineno: int
    node_count: int
    features: Counter[tuple[str, str, str]] = field(default_factory=Counter)


def _node_label(node: ast.AST) -> str:
    """Normalized label for an AST node.

    Identifier-bearing and literal-bearing nodes collapse to a generic
    marker so that renames and literal substitutions are invisible.
    """
    cls = type(node).__name__
    if isinstance(node, ast.Name):
        return "NAME"
    if isinstance(node, ast.arg):
        return "ARG"
    if isinstance(node, ast.Constant):
        return f"CONST_{type(node.value).__name__}"
    if isinstance(node, ast.Attribute):
        return "ATTR"
    return cls


def _walk_edges(
    node: ast.AST,
) -> Iterable[tuple[str, str, str]]:
    """Yield (parent_label, field_name, child_label) for every parent→child edge.

    Counting edges (rather than nodes) makes the fingerprint sensitive
    to structural composition while still independent of names.
    """
    parent_label = _node_label(node)
    for field_name, value in ast.iter_fields(node):
        if isinstance(value, list):
            for item in value:
                if isinstance(item, ast.AST):
                    yield (parent_label, field_name, _node_label(item))
                    yield from _walk_edges(item)
        elif isinstance(value, ast.AST):
            yield (parent_label, field_name, _node_label(value))
            yield from _walk_edges(value)


def _count_nodes(node: ast.AST) -> int:
    return sum(1 for _ in ast.walk(node))


def _form_fingerprint(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    path: str,
) -> FormFingerprint:
    edges = Counter(_walk_edges(node))
    return FormFingerprint(
        form_id=f"{path}:{node.lineno}:{node.name}",
        path=path,
        name=node.name,
        lineno=node.lineno,
        end_lineno=node.end_lineno or node.lineno,
        node_count=_count_nodes(node),
        features=edges,
    )


def fingerprint_module(source: str, *, path: str) -> list[FormFingerprint]:
    """Fingerprint every function-level form in ``source``.

    Nested functions are included; classes are descended into so that
    methods are scored as forms.
    """
    tree = ast.parse(source)
    out: list[FormFingerprint] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            out.append(_form_fingerprint(node, path=path))
    return out


def fingerprint_paths(paths: Iterable[Path]) -> list[FormFingerprint]:
    """Fingerprint every form found across ``paths`` (.py files only)."""
    out: list[FormFingerprint] = []
    for p in paths:
        if p.suffix != ".py":
            continue
        out.extend(fingerprint_module(p.read_text(), path=str(p)))
    return out


def jaccard(a: Counter, b: Counter) -> float:
    """Multiset Jaccard: sum(min) / sum(max) over the union of keys.

    Returns 0.0 when both counters are empty (rather than NaN), which
    keeps callers from having to special-case the trivial case.
    """
    if not a and not b:
        return 0.0
    keys = set(a) | set(b)
    inter = sum(min(a.get(k, 0), b.get(k, 0)) for k in keys)
    union = sum(max(a.get(k, 0), b.get(k, 0)) for k in keys)
    if union == 0:
        return 0.0
    return inter / union


@dataclass(frozen=True)
class DuplicatePair:
    """A scored pair of structurally-similar forms."""

    a: FormFingerprint
    b: FormFingerprint
    score: float

    def as_dict(self) -> dict[str, object]:
        return {
            "a": self.a.form_id,
            "b": self.b.form_id,
            "score": round(self.score, 4),
        }


def find_duplicates(
    forms: list[FormFingerprint],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    min_nodes: int = DEFAULT_MIN_NODES,
) -> list[DuplicatePair]:
    """Return all form pairs whose Jaccard score meets ``threshold``.

    Forms with fewer than ``min_nodes`` AST nodes are excluded — short
    forms produce small fingerprints whose matches are noisy.  Pairs
    where both forms are the same form are excluded.  Output is sorted
    by descending score, then by form_id for stability.
    """
    eligible = [f for f in forms if f.node_count >= min_nodes]
    pairs: list[DuplicatePair] = []
    for i in range(len(eligible)):
        for j in range(i + 1, len(eligible)):
            a, b = eligible[i], eligible[j]
            if a.form_id == b.form_id:
                continue
            score = jaccard(a.features, b.features)
            if score >= threshold:
                pairs.append(DuplicatePair(a=a, b=b, score=score))
    pairs.sort(key=lambda p: (-p.score, p.a.form_id, p.b.form_id))
    return pairs
