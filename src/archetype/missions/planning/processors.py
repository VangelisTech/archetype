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
HTN Processors
==============

Four pure ``DataFrame -> DataFrame`` processors, all ``components = (Branch,)``, run
in ascending priority within ONE tick on ONE archetype — so the engine threads a
single immutable df proc-to-proc (``async_system.py``) and each reads exactly the
columns the previous wrote THIS tick:

    Frontier(10) -> Applicability(20) -> Effect(30) -> Termination(40)

That same-tick, same-row chain IS SHOP "state-known-at-precondition-time": the atoms
``ApplicabilityProcessor`` tests are exactly the atoms ``EffectProcessor`` is about to
mutate (Inv HTN-V2.2 / HTN-V2.2b). None of these forces eager materialization — all
structural fan-out is the driver's job — so the module is lazy-audit clean in ``src/``.
"""

from __future__ import annotations

from typing import cast

from daft import Expression, col, lit
from daft.functions import when

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.missions.planning import udfs

_LIVE = lit("live")
_READY_FIELDS = (
    "ready_node_id",
    "ready_kind",
    "ready_op_name",
    "ready_args_json",
    "pre_pos_json",
    "pre_neg_json",
    "add_json",
    "del_json",
    "candidate_methods_json",
)


class FrontierProcessor(AsyncProcessor):
    """Priority 10. Pick the ready node and denormalize its op-spec into in-row
    columns. Pure value columns — zero archetype migration per step (Inv HTN-V2.9)."""

    # Rebound to (Branch,) after the class bodies (keeps imports linear).
    components: tuple[type[Component], ...] = ()
    priority = 10

    async def process(self, df, **kw):
        df = df.with_column("branch__ready", udfs.pick_ready(col("branch__network_json")))
        for k in _READY_FIELDS:
            df = df.with_column(f"branch__{k}", col("branch__ready")[k])
        return df.exclude("branch__ready")


class ApplicabilityProcessor(AsyncProcessor):
    """Priority 20. Reads the SAME-ROW op-spec columns and ``atoms_json``. Primitive:
    ``applicable = pre+ ⊆ atoms ∧ pre- ∩ atoms = ∅``. Compound: the COMPLETE set of
    methods whose precondition holds in CURRENT atoms (Inv HTN-V2.7)."""

    components: tuple[type[Component], ...] = ()
    priority = 20

    async def process(self, df, **kw):
        prim = col("branch__ready_kind") == "primitive"
        # Daft stubs type str-literal == as bool; runtime returns Expression.
        comp = cast(Expression, col("branch__ready_kind") == "compound")
        df = df.with_column(
            "branch__applicable",
            when(
                prim,
                then=udfs.precond_holds(
                    col("branch__atoms_json"),
                    col("branch__pre_pos_json"),
                    col("branch__pre_neg_json"),
                ),
            ).otherwise(lit(False)),
        )
        df = df.with_column(
            "branch__applicable_methods_json",
            when(
                comp,
                then=udfs.methods_applicable(
                    col("branch__atoms_json"), col("branch__candidate_methods_json")
                ),
            ).otherwise(lit("[]")),
        )
        df = df.with_column(
            "branch__needs_expansion",
            comp & udfs.json_list_nonempty(col("branch__applicable_methods_json")),
        )
        return df


_EFFECT_FIELDS = (
    "atoms_json",
    "network_json",
    "plan_json",
    "pre_state_sig",
    "seq_next",
    "status",
    "fail_reason",
    "applicable",
)


class EffectProcessor(AsyncProcessor):
    """Priority 30. Guarded, self-recording STRIPS application via ONE consolidated UDF.

    Applies ``gamma`` ONLY where the ready node is a primitive that re-checks as applicable
    in CURRENT atoms and the branch is live (Inv HTN-V2.3); records the executability
    witness ``pre_state_sig`` from the genuine pre-mutation atoms and the plan step at
    application time (Inv HTN-V2.4). A single struct-returning UDF is mandatory here: Daft
    folds a UDF's column args to the column's final definition, so splitting the effect
    across multiple UDFs that read-then-overwrite ``atoms_json`` / ``seq_next`` corrupts the
    witness and the seq (see ``udfs.apply_effect``). Reading every input once and splitting
    the struct back out is aliasing-proof (cycle-prevention reads the pre-mutation values)."""

    components: tuple[type[Component], ...] = ()
    priority = 30

    async def process(self, df, **kw):
        df = df.with_column(
            "branch__eff",
            udfs.apply_effect(
                col("branch__atoms_json"),
                col("branch__network_json"),
                col("branch__plan_json"),
                col("branch__seq_next"),
                col("branch__status"),
                col("branch__fail_reason"),
                col("branch__pre_state_sig"),
                col("branch__depth"),
                col("branch__max_depth"),
                col("branch__ready_kind"),
                col("branch__ready_node_id"),
                col("branch__ready_op_name"),
                col("branch__ready_args_json"),
                col("branch__pre_pos_json"),
                col("branch__pre_neg_json"),
                col("branch__add_json"),
                col("branch__del_json"),
            ),
        )
        for f in _EFFECT_FIELDS:
            df = df.with_column(f"branch__{f}", col("branch__eff")[f])
        return df.exclude("branch__eff")


class TerminationProcessor(AsyncProcessor):
    """Priority 40. Pure in-row marking, no structural change. Recomputes ``open_count``
    from the POST-Effect network (Inv HTN-V2.4b), then flips status: depth cap is
    MANDATORY every tick (Inv HTN-V2.11); ``open_count == 0`` => solved; a ready compound
    with no applicable method, or a stalled branch with no ready node, => failed."""

    components: tuple[type[Component], ...] = ()
    priority = 40

    async def process(self, df, **kw):
        # Daft stubs type Expression.__eq__ as bool; cast records the real
        # Expression type so downstream &-chains type-check.
        live = col("branch__status") == _LIVE
        over_depth = col("branch__depth") > col("branch__max_depth")
        # ty: int-literal comparison hits no stub overload; runtime returns Expression.
        open_count = col("branch__open_count")
        open_positive = cast(Expression, open_count > 0)  # ty: ignore[unsupported-operator]
        none_ready = cast(Expression, col("branch__ready_kind") == "none")
        dead_compound = cast(Expression, col("branch__ready_kind") == "compound") & ~col(
            "branch__needs_expansion"
        )

        # (a) authoritative post-resolution open count, same row.
        df = df.with_column("branch__open_count", udfs.count_open(col("branch__network_json")))
        # (b) status flips — guarded on still-live so retired/terminal rows never change.
        df = df.with_column(
            "branch__status",
            when(over_depth & live, then=lit("failed"))
            .when(cast(Expression, col("branch__open_count") == 0) & live, then=lit("solved"))
            .when(dead_compound & live, then=lit("failed"))
            .when(none_ready & open_positive & live, then=lit("failed"))
            .otherwise(col("branch__status")),
        )
        # (c) fail reason for the transition that just fired.
        failed = cast(Expression, col("branch__status") == "failed")
        df = df.with_column(
            "branch__fail_reason",
            when(over_depth & failed, then=lit("depth_exceeded"))
            .when(
                dead_compound & failed & cast(Expression, col("branch__fail_reason") == ""),
                then=lit("no_applicable_method"),
            )
            .when(
                none_ready & failed & cast(Expression, col("branch__fail_reason") == ""),
                then=lit("stalled"),
            )
            .otherwise(col("branch__fail_reason")),
        )
        # (d) memo key — state ++ remaining-open multiset, in-row.
        df = df.with_column(
            "branch__frontier_signature",
            udfs.frontier_sig(col("branch__atoms_json"), col("branch__network_json")),
        )
        return df


# Bind the (Branch,) signature after the class bodies to keep imports linear.
from archetype.missions.planning.components import Branch  # noqa: E402

FrontierProcessor.components = (Branch,)
ApplicabilityProcessor.components = (Branch,)
EffectProcessor.components = (Branch,)
TerminationProcessor.components = (Branch,)


def htn_processors() -> list[AsyncProcessor]:
    """The priority-ordered processor chain for HTN resolution."""
    return [
        FrontierProcessor(),
        ApplicabilityProcessor(),
        EffectProcessor(),
        TerminationProcessor(),
    ]
