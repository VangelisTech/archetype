# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the generated REST reference."""

from __future__ import annotations

from copy import deepcopy

import pytest

from scripts.generate_api_docs import (
    MISSIONS_OUTPUT,
    OUTPUT,
    _validate_extension_composition,
    generate,
    generate_references,
    get_openapi_schema,
)


@pytest.fixture(scope="module")
def rest_references() -> dict:
    return generate_references()


@pytest.fixture(scope="module")
def rest_reference(rest_references: dict) -> str:
    return rest_references[OUTPUT]


@pytest.fixture(scope="module")
def missions_rest_reference(rest_references: dict) -> str:
    return rest_references[MISSIONS_OUTPUT]


def _operation(reference: str, heading: str) -> str:
    start = reference.index(f"### {heading}\n")
    end = reference.index("\n---\n", start)
    return reference[start:end]


def test_committed_rest_references_match_openapi(rest_references: dict) -> None:
    assert OUTPUT.read_text(encoding="utf-8") == rest_references[OUTPUT]
    assert MISSIONS_OUTPUT.read_text(encoding="utf-8") == rest_references[MISSIONS_OUTPUT]
    assert generate() == rest_references[OUTPUT]


def test_framework_and_missions_routes_use_explicit_compositions(
    rest_reference: str,
    missions_rest_reference: str,
) -> None:
    from archetype.missions._extension import get_manifest

    base_paths = get_openapi_schema(world_libraries=())["paths"]
    missions_paths = get_openapi_schema(world_libraries=(get_manifest(),))["paths"]

    assert "/worlds/{world_id}/missions" not in base_paths
    assert "/worlds/{world_id}/missions" in missions_paths
    assert "# Framework REST API Reference" in rest_reference
    assert "| Distribution | `archetype-ecs` |" in rest_reference
    assert "GET /worlds/{world_id}/missions" not in rest_reference
    assert "# Agent Missions REST API Reference" in missions_rest_reference
    assert "| Distribution | `archetype-missions` |" in missions_rest_reference
    assert "GET /worlds/{world_id}/missions" in missions_rest_reference
    assert "POST /v1/mission-control/runs" in missions_rest_reference
    assert "GET /healthz" not in missions_rest_reference


def test_extension_coordinate_collision_fails_closed() -> None:
    base = {"paths": {"/healthz": {"get": {"summary": "Healthz"}}}}

    with pytest.raises(RuntimeError, match="collides with framework REST operations"):
        _validate_extension_composition(
            base,
            deepcopy(base),
            {("/healthz", "get")},
            owner="Test extension",
        )


def test_extension_framework_mutation_fails_closed() -> None:
    base = {
        "paths": {"/healthz": {"get": {"summary": "Healthz"}}},
        "components": {"schemas": {"Health": {"type": "object"}}},
    }
    composed = deepcopy(base)
    composed["paths"]["/extension"] = {"get": {"summary": "Extension"}}
    composed["paths"]["/healthz"]["get"]["summary"] = "Changed"

    with pytest.raises(RuntimeError, match="mutates framework REST contracts"):
        _validate_extension_composition(
            base,
            composed,
            {("/extension", "get")},
            owner="Test extension",
        )


def test_extension_framework_component_mutation_fails_closed() -> None:
    base = {
        "paths": {"/healthz": {"get": {"summary": "Healthz"}}},
        "components": {"schemas": {"Health": {"type": "object"}}},
    }
    composed = deepcopy(base)
    composed["paths"]["/extension"] = {"get": {"summary": "Extension"}}
    composed["components"]["schemas"]["Health"]["type"] = "string"

    with pytest.raises(RuntimeError, match="components/schemas/Health"):
        _validate_extension_composition(
            base,
            composed,
            {("/extension", "get")},
            owner="Test extension",
        )


def test_nullable_query_parameter_keeps_its_integer_type(rest_reference: str) -> None:
    state = _operation(rest_reference, "Get World State")
    assert "| `tick` | integer \\| null |" in state


def test_optional_step_body_renders_its_fields(rest_reference: str) -> None:
    step = _operation(rest_reference, "Step World")
    assert "**Request body:**" in step
    assert "| `run_config` | RunConfig \\| null |" in step
    assert "| `num_steps` | integer |" in step
    assert "| `debug` | boolean |" in step
