# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the generated CLI reference."""

from __future__ import annotations

import pytest

from scripts.generate_cli_docs import OUTPUT, generate


@pytest.fixture(scope="module")
def cli_reference() -> str:
    return generate()


def _command(reference: str, heading: str) -> str:
    start = reference.index(f"### `{heading}`\n")
    end = reference.index("\n---\n", start)
    return reference[start:end]


def test_committed_cli_reference_matches_click_app(cli_reference: str) -> None:
    assert OUTPUT.read_text(encoding="utf-8") == cli_reference


def test_query_reference_exposes_component_terminals(cli_reference: str) -> None:
    query = _command(cli_reference, "archetype query")
    assert "[COMPONENT_TYPES]" in query
    assert "`--show`" in query
    assert "`--count`" in query
    assert "`--where`" in query
