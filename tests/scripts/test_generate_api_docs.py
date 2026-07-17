# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the generated REST reference."""

from __future__ import annotations

import pytest

from scripts.generate_api_docs import OUTPUT, generate


@pytest.fixture(scope="module")
def rest_reference() -> str:
    return generate()


def _operation(reference: str, heading: str) -> str:
    start = reference.index(f"### {heading}\n")
    end = reference.index("\n---\n", start)
    return reference[start:end]


def test_committed_rest_reference_matches_openapi(rest_reference: str) -> None:
    assert OUTPUT.read_text(encoding="utf-8") == rest_reference


def test_nullable_query_parameter_keeps_its_integer_type(rest_reference: str) -> None:
    state = _operation(rest_reference, "Get World State")
    assert "| `tick` | integer \\| null |" in state


def test_optional_step_body_renders_its_fields(rest_reference: str) -> None:
    step = _operation(rest_reference, "Step World")
    assert "**Request body:**" in step
    assert "| `run_config` | RunConfig \\| null |" in step
    assert "| `num_steps` | integer |" in step
    assert "| `debug` | boolean |" in step
