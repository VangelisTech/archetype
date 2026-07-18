# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import daft
import pytest

from archetype.api.query_filter import parse_where


@pytest.mark.parametrize(
    ("source", "expected"),
    (
        ("score__value > 0.5", [0.75, 1.0]),
        ("score__value >= 0.75", [0.75, 1.0]),
        ("score__value < 0.75", [0.25]),
        ("score__value <= 0.75", [0.25, 0.75]),
        ("score__value == 0.75", [0.75]),
        ("score__value != 0.75", [0.25, 1.0]),
    ),
)
def test_parse_where_builds_a_daft_comparison(source: str, expected: list[float]) -> None:
    parsed = parse_where(source)
    frame = daft.from_pydict({"score__value": [0.25, 0.75, 1.0]})
    rows = frame.where(parsed.expression).collect().to_pylist()
    assert parsed.column == "score__value"
    assert [row["score__value"] for row in rows] == expected


def test_parse_where_supports_quoted_and_bare_strings() -> None:
    frame = daft.from_pydict({"agent__status": ["ready", "waiting"]})
    for source in ('agent__status == "ready"', "agent__status == ready"):
        parsed = parse_where(source)
        assert frame.where(parsed.expression).collect().to_pylist() == [{"agent__status": "ready"}]


def test_parse_where_supports_negative_numeric_literals() -> None:
    parsed = parse_where("metric__value >= -1.5")
    frame = daft.from_pydict({"metric__value": [-2.0, -1.5, 0.0]})
    assert frame.where(parsed.expression).collect().to_pylist() == [
        {"metric__value": -1.5},
        {"metric__value": 0.0},
    ]


@pytest.mark.parametrize(
    "source",
    (
        "",
        "score__value",
        "score__value > 0 and score__value < 1",
        "0 < score__value < 1",
        "score__value + 1 > 2",
        "score__value in [1, 2]",
        "score__value > dangerous()",
        "record.score > 1",
        "score__value > 1e309",
    ),
)
def test_parse_where_rejects_executable_or_ambiguous_syntax(source: str) -> None:
    with pytest.raises(ValueError, match="Invalid where expression"):
        parse_where(source)
