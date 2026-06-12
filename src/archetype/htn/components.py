# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
HTN Components
==============

``Branch`` is the single live-branch archetype — one row per branch in the AND/OR
forest, co-locating state + remaining network + ready op-spec + accumulated plan so
the processor chain can evaluate preconditions and apply effects same-tick, same-row
(Inv HTN-V2.1 / HTN-V2.2). ``Solved`` is the only marker, attached once by the driver
when a branch's network has zero open nodes.

House idiom (mirrors ``src/archetype/experiments/components.py``): subclass
``Component`` (Pydantic via LanceModel, never dataclass); every field defaulted;
scalars stored directly; complex data JSON-encoded into ``*_json``; ``StrEnum``-as-
string for status with a helper class beside it (stored as ``.value``, never the enum
— enums are not Arrow-serializable); timestamps as ``*_at_ms`` ints; ``@classmethod
make(...)`` for type-safe construction; ``get_*()`` decoders for the JSON fields.
"""

from __future__ import annotations

import json
import time
from enum import StrEnum

from archetype.core.component import Component


def _now_ms() -> int:
    """Current time as Unix milliseconds (driver/seed side — never inside a UDF)."""
    return int(time.time() * 1000)


# ============================================================================
# Helper StrEnums — NOT components. Stored on rows as their ``.value`` string.
# ============================================================================


class BranchStatus(StrEnum):
    """Lifecycle of one branch in the forest.

    A branch is LIVE while it is still being decomposed/progressed. It becomes
    SOLVED (zero open nodes), FAILED (precondition unsat / no applicable method /
    depth exceeded), PRUNED (memoized duplicate frontier), or SUPERSEDED (a parent
    retired after OR fan-out — its children carry the live continuation).
    """

    LIVE = "live"
    SOLVED = "solved"
    FAILED = "failed"
    PRUNED = "pruned"
    SUPERSEDED = "superseded"  # retired parent after OR fan-out (Inv HTN-V2.17)

    @classmethod
    def is_terminal(cls, s: str) -> bool:
        """Non-live = finished being processed.

        SUPERSEDED is a structural retirement, NOT a failure — its children carry
        the live continuation, but the parent row itself is done.
        """
        return s in (cls.SOLVED.value, cls.FAILED.value, cls.PRUNED.value, cls.SUPERSEDED.value)

    @classmethod
    def is_live(cls, s: str) -> bool:
        return s == cls.LIVE.value


class TaskKind(StrEnum):
    COMPOUND = "compound"
    PRIMITIVE = "primitive"


class TaskStatus(StrEnum):
    OPEN = "open"
    RESOLVED = "resolved"
    FAILED = "failed"


# ============================================================================
# Components
# ============================================================================


class Branch(Component):
    """ONE row per live branch. Carries, co-located on a single archetype:

       (i)   ``atoms_json``   — progressed STRIPS state (sorted ground atoms),
       (ii)  ``network_json`` — the ENTIRE remaining task network,
       (iii) op-spec cols     — the ready node's denormalized operator/method spec,
       (iv)  ``plan_json``    — the accumulated, self-contained, append-only plan.

    Because all four are on ONE archetype, ``Frontier/Applicability/Effect/
    Termination`` are same-tick priority-chained column transforms — no
    cross-archetype read is ever required (Inv HTN-V2.1 / HTN-V2.2).
    """

    plan_id: str = "r"
    parent_branch_id: str = ""

    # (i) state — sorted ["pred(a,b)", ...] = the closed-world TRUE set
    atoms_json: str = "[]"

    # (ii) the entire remaining task network:
    #   [{node_id, name, args, kind, preds, order_index, depth, status,
    #     chosen_method, op, candidate_methods}, ...]
    network_json: str = "[]"

    # (iv) accumulated plan (self-contained per branch, copied COW to children)
    plan_json: str = "[]"  # [{seq, op, args, pre_sig, node_id}, ...]
    seq_next: int = 0

    # depth / termination
    depth: int = 0
    max_depth: int = 64  # MANDATORY-on cap (Inv HTN-V2.11)
    status: str = "live"  # BranchStatus value
    fail_reason: str = ""  # no_applicable_method | precond_unsat | depth_exceeded | pruned

    # (iii) ready-node denormalized op-spec columns (stamped by FrontierProcessor)
    ready_node_id: int = -1
    ready_kind: str = ""  # compound | primitive | none
    ready_op_name: str = ""
    ready_args_json: str = "[]"
    pre_pos_json: str = "[]"
    pre_neg_json: str = "[]"
    add_json: str = "[]"
    del_json: str = "[]"
    candidate_methods_json: str = "[]"  # for a ready compound: embedded per-node by driver

    # applicability / expansion / witness (written by Applicability / Effect)
    applicable: bool = False
    applicable_methods_json: str = "[]"  # COMPLETE applicable set vs CURRENT atoms
    needs_expansion: bool = False
    pre_state_sig: str = ""  # written by EffectProcessor (separate with_column, Inv HTN-V2.4a)

    # same-row derived columns (NOT cross-archetype folds)
    open_count: int = 0  # recomputed post-Effect in TerminationProcessor (Inv HTN-V2.4b)
    frontier_signature: str = ""
    updated_at_ms: int = 0

    @classmethod
    def make(
        cls,
        plan_id: str,
        atoms: list[str],
        network: list[dict],
        *,
        parent_branch_id: str = "",
        depth: int = 0,
        max_depth: int = 64,
        plan: list[dict] | None = None,
        seq_next: int = 0,
    ) -> Branch:
        """Construct a branch row. ``atoms`` are sorted for a canonical state encoding."""
        return cls(
            plan_id=plan_id,
            parent_branch_id=parent_branch_id,
            atoms_json=json.dumps(sorted(atoms)),
            network_json=json.dumps(network),
            plan_json=json.dumps(plan or []),
            seq_next=seq_next,
            depth=depth,
            max_depth=max_depth,
            updated_at_ms=_now_ms(),
        )

    def get_atoms(self) -> list[str]:
        return json.loads(self.atoms_json)

    def get_network(self) -> list[dict]:
        return json.loads(self.network_json)

    def get_plan(self) -> list[dict]:
        return json.loads(self.plan_json)

    def get_applicable_methods(self) -> list[dict]:
        return json.loads(self.applicable_methods_json)


class Solved(Component):
    """Attached by the DRIVER via ``add_components`` EXACTLY ONCE when a branch's
    network has zero open nodes. Keys ``EpisodeConfig.terminal_component``. One
    terminal migration per solved branch (Inv HTN-V2.9 / HTN-V2.16)."""

    plan_id: str = "r"
    plan_length: int = 0
    solved_at_ms: int = 0

    @classmethod
    def make(cls, plan_id: str, plan_length: int) -> Solved:
        return cls(plan_id=plan_id, plan_length=plan_length, solved_at_ms=_now_ms())
