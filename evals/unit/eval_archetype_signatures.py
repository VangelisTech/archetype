# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit evals: Archetype signature operations.

Tests that signatures are stable (sorted by class name), add/remove
operations produce correct results, and naming is deterministic.
"""

from __future__ import annotations

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from evals.types import EvalResult, Tier, binary


class A(Component):
    value: int


class B(Component):
    value: int


class C(Component):
    value: int


def run() -> list[EvalResult]:
    results = []

    # --- signature is sorted by class name ---
    sig_ab = Archetype.sig_from_components([B(value=1), A(value=2)])
    results.append(
        binary(
            [t.__name__ for t in sig_ab],
            ["A", "B"],
            case_name="sig_sorted_order",
            tier=Tier.UNIT,
        )
    )

    # --- same components in different order produce same signature ---
    sig_ba = Archetype.sig_from_components([A(value=2), B(value=1)])
    results.append(
        binary(
            sig_ab,
            sig_ba,
            case_name="sig_order_invariant",
            tier=Tier.UNIT,
        )
    )

    # --- add_components union ---
    sig_abc = Archetype.add_components(sig_ab, [C])
    results.append(
        binary(
            [t.__name__ for t in sig_abc],
            ["A", "B", "C"],
            case_name="sig_add_components",
            tier=Tier.UNIT,
        )
    )

    # --- add existing component is no-op ---
    sig_same = Archetype.add_components(sig_abc, [A])
    results.append(
        binary(
            sig_same,
            sig_abc,
            case_name="sig_add_existing_noop",
            tier=Tier.UNIT,
        )
    )

    # --- remove_components difference ---
    sig_a = Archetype.remove_components(sig_abc, [B, C])
    results.append(
        binary(
            [t.__name__ for t in sig_a],
            ["A"],
            case_name="sig_remove_components",
            tier=Tier.UNIT,
        )
    )

    # --- deterministic naming ---
    name1 = Archetype.get_name(sig_ab)
    name2 = Archetype.get_name(sig_ab)
    results.append(
        binary(
            name1,
            name2,
            case_name="sig_name_deterministic",
            tier=Tier.UNIT,
        )
    )

    # --- different signatures produce different names ---
    name_abc = Archetype.get_name(sig_abc)
    results.append(
        binary(
            name1 != name_abc,
            True,
            case_name="sig_name_unique",
            tier=Tier.UNIT,
        )
    )

    # --- schema includes base fields + component fields ---
    schema = Archetype.get_archetype_schema(sig_ab)
    field_names = {f.name for f in schema}
    expected_fields = {
        "entity_id",
        "tick",
        "world_id",
        "run_id",
        "is_active",
        "a__value",
        "b__value",
    }
    results.append(
        binary(
            expected_fields.issubset(field_names),
            True,
            case_name="sig_schema_fields",
            tier=Tier.UNIT,
        )
    )

    # --- to_row_dict includes all fields ---
    row = Archetype.to_row_dict(
        entity_id=0,
        tick=0,
        components=[A(value=42), B(value=99)],
        world_id="test",
        run_id="run1",
    )
    results.append(
        binary(
            (row["a__value"], row["b__value"]),
            (42, 99),
            case_name="archetype_row_dict_values",
            tier=Tier.UNIT,
        )
    )
    results.append(
        binary(
            (row["entity_id"], row["world_id"]),
            (0, "test"),
            case_name="archetype_row_dict_base_fields",
            tier=Tier.UNIT,
        )
    )

    return results
