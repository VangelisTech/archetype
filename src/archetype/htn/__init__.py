# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype HTN — Hierarchical Task Network plan resolution as a formal ECS pattern.

Plan resolution is a single-world, ``plan_id``-tagged AND/OR forest. Every live
branch is ONE row of ONE ``(Branch,)`` archetype carrying, co-located:

  (i)   ``atoms_json``   — the progressed STRIPS state (closed-world ground atoms),
  (ii)  ``network_json`` — the entire remaining task network,
  (iii) op-spec columns  — the ready node's denormalized operator / method spec,
  (iv)  ``plan_json``    — the accumulated, self-contained, append-only plan.

Because all four live on ONE archetype, a priority-ordered processor chain
(``Frontier(10) -> Applicability(20) -> Effect(30) -> Termination(40)``) evaluates
preconditions and applies STRIPS effects *same-tick, same-row* — giving genuine
SHOP "state-known-at-precondition-time" by construction, with no cross-archetype
staleness window. Fan-out lives on two axes:

  * AXIS 1 (in-tick, vectorized across branches): every live branch advances one
    micro-step in ONE DataFrame pass — N branches cost one tick regardless of N.
  * AXIS 2 (between-ticks, structural OR-spawn): the driver ``world.spawn``s one
    child branch per applicable method (copy-on-write ``plan_id`` multiplication)
    and retires the parent to ``superseded``.

The driver (``resolve_htn``) is structural-only and lives in ``examples/`` because
it must materialize rows (eager collection) at the spawn-decision boundary; the
processors and UDFs here are pure ``DataFrame -> DataFrame`` and lazy-audit clean.

See ``.context/htn/PATTERN.md`` for the full formal pattern and numbered invariants.
"""

from __future__ import annotations

from archetype.htn.components import Branch, BranchStatus, Solved, TaskKind, TaskStatus
from archetype.htn.domain import (
    HtnDomain,
    MethodSpec,
    OperatorSpec,
    build_initial_network,
    splice_method,
)
from archetype.htn.processors import (
    ApplicabilityProcessor,
    EffectProcessor,
    FrontierProcessor,
    TerminationProcessor,
    htn_processors,
)

__all__ = [
    "Branch",
    "BranchStatus",
    "Solved",
    "TaskKind",
    "TaskStatus",
    "HtnDomain",
    "OperatorSpec",
    "MethodSpec",
    "build_initial_network",
    "splice_method",
    "FrontierProcessor",
    "ApplicabilityProcessor",
    "EffectProcessor",
    "TerminationProcessor",
    "htn_processors",
]
