# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fail-closed guards the Mission projections rely on before settlement.

Both invariants are enforced by module-private helpers and were previously
unexercised, so a refactor could weaken them without any test noticing. These
cover the exact shapes that pass a naive aggregate check but are not complete.
"""

from __future__ import annotations

import daft
import pytest

from archetype.missions.activities import AuthorActivityEntityFact
from archetype.missions.components import Commit
from archetype.missions.projection_bundles import _select_author_provenance
from archetype.missions.projections import _daft_latest
from archetype.missions.relations import ProducedBy

_EXECUTION_ID = 7


def _output(entity_id: int) -> AuthorActivityEntityFact:
    return AuthorActivityEntityFact(entity_id=entity_id, component=Commit(sha=f"sha-{entity_id}"))


def _edge(entity_id: int, source: int, target: int) -> AuthorActivityEntityFact:
    return AuthorActivityEntityFact(
        entity_id=entity_id,
        component=ProducedBy(source=source, target=target),
    )


def test_provenance_requires_one_edge_per_output_not_a_matching_total() -> None:
    """Two edges on one output and none on another must not settle.

    The edge count equals the output count here, so any check that compares
    totals rather than per-output cardinality admits an incomplete bundle.
    """

    outputs = [_output(10), _output(11)]
    facts_by_type = {
        ProducedBy: [
            _edge(20, source=10, target=_EXECUTION_ID),
            _edge(21, source=10, target=_EXECUTION_ID),
        ]
    }

    assert _select_author_provenance(facts_by_type, outputs, _EXECUTION_ID) is None


def test_provenance_selects_the_edge_naming_each_output() -> None:
    outputs = [_output(10), _output(11)]
    facts_by_type = {
        ProducedBy: [
            _edge(20, source=10, target=_EXECUTION_ID),
            _edge(21, source=11, target=_EXECUTION_ID),
            _edge(22, source=11, target=_EXECUTION_ID + 1),
        ]
    }

    selected = _select_author_provenance(facts_by_type, outputs, _EXECUTION_ID)

    assert selected is not None
    assert [fact.entity_id for fact in selected] == [20, 21]


def test_immutable_component_rejects_two_committed_values() -> None:
    frame = daft.from_pydict({"entity_id": [1, 1], "tick": [3, 4], "value": ["a", "b"]})

    with pytest.raises(ValueError, match="conflicting committed rows"):
        _daft_latest(frame, label="candidate")


def test_immutable_component_accepts_an_unchanged_history() -> None:
    frame = daft.from_pydict({"entity_id": [1, 1], "tick": [3, 4], "value": ["a", "a"]})

    latest = _daft_latest(frame, label="candidate").to_pylist()

    assert latest == [{"entity_id": 1, "tick": 4, "value": "a"}]


def test_mutable_component_rejects_two_values_at_one_tick() -> None:
    frame = daft.from_pydict({"entity_id": [1, 1], "tick": [4, 4], "value": ["a", "b"]})

    with pytest.raises(ValueError, match="conflicting committed rows"):
        _daft_latest(frame, label="candidate task", allow_updates=True)


def test_mutable_component_keeps_the_newest_committed_value() -> None:
    frame = daft.from_pydict({"entity_id": [1, 1], "tick": [3, 4], "value": ["a", "b"]})

    latest = _daft_latest(frame, label="candidate task", allow_updates=True).to_pylist()

    assert latest == [{"entity_id": 1, "tick": 4, "value": "b"}]
