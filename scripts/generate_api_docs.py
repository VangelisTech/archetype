#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Generate owner-scoped REST API references from explicit OpenAPI schemas.

Usage:
    python scripts/generate_api_docs.py

Writes the framework reference and one reference per documented extension.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

DOCS_DIR = Path(__file__).resolve().parent.parent / "docs" / "reference"
OUTPUT = DOCS_DIR / "rest-api.md"
MISSIONS_OUTPUT = DOCS_DIR / "rest-api-missions.md"
HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete"})


@dataclass(frozen=True, slots=True)
class RestExtension:
    """One explicitly documented world-library REST surface."""

    title: str
    distribution: str
    module_name: str
    factory_name: str
    output: Path


REST_EXTENSIONS: tuple[RestExtension, ...] = (
    RestExtension(
        "Agent Missions",
        "archetype-missions",
        "archetype.missions._extension",
        "get_manifest",
        MISSIONS_OUTPUT,
    ),
)


def get_openapi_schema(*, world_libraries: tuple[Any, ...] = ()) -> dict[str, Any]:
    """Extract one explicit API surface without installed-package discovery."""
    from archetype.api.app import create_app

    app = create_app(world_libraries=world_libraries)
    return app.openapi()


def _extension_coordinates(manifest: Any, *, owner: str) -> set[tuple[str, str]]:
    """Return coordinates contributed by one manifest, rejecting duplicates."""
    from fastapi import APIRouter

    coordinates: set[tuple[str, str]] = set()
    for router_factory in manifest.api_router_factories:
        router = router_factory()
        if not isinstance(router, APIRouter):
            raise TypeError(f"{owner} API factory did not return APIRouter")
        for route in router.routes:
            path = getattr(route, "path", None)
            methods = getattr(route, "methods", None) or ()
            if not isinstance(path, str):
                continue
            for method in methods:
                normalized = str(method).lower()
                if normalized not in HTTP_METHODS:
                    continue
                coordinate = (path, normalized)
                if coordinate in coordinates:
                    raise RuntimeError(
                        f"{owner} declares duplicate REST operation {normalized.upper()} {path}"
                    )
                coordinates.add(coordinate)
    if not coordinates:
        raise RuntimeError(f"{owner} contributes no documentable REST operations")
    return coordinates


def _component_mutations(
    base_schema: dict[str, Any],
    composed_schema: dict[str, Any],
) -> list[str]:
    """Return framework component definitions changed by composition."""
    mutations: list[str] = []
    for section, definitions in base_schema.get("components", {}).items():
        if not isinstance(definitions, dict):
            continue
        composed_definitions = composed_schema.get("components", {}).get(section, {})
        for name, definition in definitions.items():
            if composed_definitions.get(name) != definition:
                mutations.append(f"components/{section}/{name}")
    return sorted(mutations)


def _validate_extension_composition(
    base_schema: dict[str, Any],
    composed_schema: dict[str, Any],
    extension_coordinates: set[tuple[str, str]],
    *,
    owner: str,
) -> None:
    """Fail when an extension collides with or mutates the framework API."""
    base_operations = _operations(base_schema)
    composed_operations = _operations(composed_schema)
    base_coordinates = set(base_operations)
    collisions = sorted(base_coordinates & extension_coordinates)
    if collisions:
        rendered = ", ".join(f"{method.upper()} {path}" for path, method in collisions)
        raise RuntimeError(f"{owner} collides with framework REST operations: {rendered}")

    expected_coordinates = base_coordinates | extension_coordinates
    actual_coordinates = set(composed_operations)
    if actual_coordinates != expected_coordinates:
        missing = sorted(expected_coordinates - actual_coordinates)
        unexpected = sorted(actual_coordinates - expected_coordinates)
        details: list[str] = []
        if missing:
            details.append(
                "missing " + ", ".join(f"{method.upper()} {path}" for path, method in missing)
            )
        if unexpected:
            details.append(
                "unexpected " + ", ".join(f"{method.upper()} {path}" for path, method in unexpected)
            )
        raise RuntimeError(f"{owner} REST composition changed coordinates: {'; '.join(details)}")

    operation_mutations = sorted(
        coordinate
        for coordinate, operation in base_operations.items()
        if composed_operations[coordinate] != operation
    )
    component_mutations = _component_mutations(base_schema, composed_schema)
    if operation_mutations or component_mutations:
        details = [
            *(f"{method.upper()} {path}" for path, method in operation_mutations),
            *component_mutations,
        ]
        raise RuntimeError(f"{owner} mutates framework REST contracts: {', '.join(details)}")


def get_extension_openapi_schemas(
    base_schema: dict[str, Any],
) -> tuple[tuple[RestExtension, dict[str, Any], set[tuple[str, str]]], ...]:
    """Build and validate each explicit extension composition."""
    schemas: list[tuple[RestExtension, dict[str, Any], set[tuple[str, str]]]] = []
    for extension in REST_EXTENSIONS:
        factory = getattr(import_module(extension.module_name), extension.factory_name)
        manifest = factory()
        coordinates = _extension_coordinates(manifest, owner=extension.title)
        schema = get_openapi_schema(world_libraries=(manifest,))
        _validate_extension_composition(
            base_schema,
            schema,
            coordinates,
            owner=extension.title,
        )
        schemas.append(
            (
                extension,
                schema,
                coordinates,
            )
        )
    return tuple(schemas)


def resolve_ref(schema: dict[str, Any], root: dict[str, Any]) -> dict[str, Any]:
    """Resolve a $ref pointer in an OpenAPI schema."""
    if "$ref" not in schema:
        return schema
    ref_path = schema["$ref"]  # e.g. "#/components/schemas/CreateWorldRequest"
    parts = ref_path.lstrip("#/").split("/")
    node = root
    for part in parts:
        node = node[part]
    return node


def _unwrap_optional(schema: dict[str, Any], root: dict[str, Any]) -> dict[str, Any]:
    """Unwrap a top-level ``anyOf``/``oneOf`` that only adds nullability.

    Pydantic renders ``Foo | None`` as ``anyOf: [{$ref: Foo}, {type: "null"}]``.
    Pick the first non-null option and resolve ``$ref`` so downstream code
    sees the underlying object schema instead of a wrapper with no properties.
    """
    schema = resolve_ref(schema, root)
    for key in ("anyOf", "oneOf"):
        options = schema.get(key)
        if not options:
            continue
        non_null = [o for o in options if o.get("type") != "null"]
        if len(non_null) == 1:
            return resolve_ref(non_null[0], root)
    return schema


def schema_to_table(schema: dict[str, Any], root: dict[str, Any], indent: int = 0) -> list[str]:
    """Render a JSON Schema object as a markdown table of fields."""
    schema = resolve_ref(schema, root)
    properties = schema.get("properties", {})
    required = set(schema.get("required", []))
    if not properties:
        return []

    lines: list[str] = []
    if indent == 0:
        lines.append("| Field | Type | Required | Default | Description |")
        lines.append("|-------|------|----------|---------|-------------|")

    for name, prop in properties.items():
        prop = resolve_ref(prop, root)
        typ = _type_label(prop, root)
        req = "Yes" if name in required else "No"
        default = prop.get("default", "—")
        if default is None:
            default = "`null`"
        elif default != "—":
            default = f"`{json.dumps(default)}`"
        desc = _table_cell(prop.get("description", prop.get("title", "")))
        prefix = "&nbsp;" * (indent * 4)
        lines.append(f"| {prefix}`{name}` | {typ} | {req} | {default} | {desc} |")

    return lines


def _table_cell(text: str) -> str:
    """Flatten text for a Markdown table cell: collapse newlines/whitespace
    and escape pipes so multi-line docstrings cannot break the table."""
    return " ".join(str(text).split()).replace("|", "\\|")


def _type_label(prop: dict[str, Any], root: dict[str, Any]) -> str:
    """Return a human-readable type label for a schema property."""
    if "$ref" in prop:
        resolved = resolve_ref(prop, root)
        return resolved.get("title", "object")
    if "anyOf" in prop:
        types = []
        for option in prop["anyOf"]:
            if option.get("type") == "null":
                types.append("null")
            elif "$ref" in option:
                resolved = resolve_ref(option, root)
                types.append(resolved.get("title", "object"))
            else:
                types.append(option.get("type", "any"))
        return " \\| ".join(types)
    typ = prop.get("type", "any")
    if typ == "array":
        items = prop.get("items", {})
        item_type = _type_label(items, root)
        return f"array[{item_type}]"
    return typ


def render_operation(
    method: str,
    path: str,
    operation: dict[str, Any],
    root: dict[str, Any],
) -> list[str]:
    """Render a single API operation as markdown."""
    lines: list[str] = []
    summary = operation.get("summary", operation.get("operationId", ""))
    lines.append(f"### {summary}")
    lines.append("")
    lines.append("```text")
    lines.append(f"{method.upper()} {path}")
    lines.append("```")
    lines.append("")

    desc = operation.get("description", "")
    if desc:
        lines.append(desc)
        lines.append("")

    # Path and query parameters
    params = operation.get("parameters", [])
    path_params = [p for p in params if p.get("in") == "path"]
    query_params = [p for p in params if p.get("in") == "query"]

    if path_params:
        lines.append("**Path parameters:**")
        lines.append("")
        lines.append("| Parameter | Type | Description |")
        lines.append("|-----------|------|-------------|")
        for p in path_params:
            schema = p.get("schema", {})
            typ = _type_label(schema, root) if schema else "string"
            desc = _table_cell(p.get("description", ""))
            lines.append(f"| `{p['name']}` | {typ} | {desc} |")
        lines.append("")

    if query_params:
        lines.append("**Query parameters:**")
        lines.append("")
        lines.append("| Parameter | Type | Default | Description |")
        lines.append("|-----------|------|---------|-------------|")
        for p in query_params:
            schema = p.get("schema", {})
            typ = _type_label(schema, root) if schema else "string"
            default = schema.get("default", "—")
            if default != "—":
                default = f"`{json.dumps(default)}`"
            desc = _table_cell(p.get("description", ""))
            lines.append(f"| `{p['name']}` | {typ} | {default} | {desc} |")
        lines.append("")

    # Request body
    request_body = operation.get("requestBody", {})
    if request_body:
        content = request_body.get("content", {})
        json_content = content.get("application/json", {})
        body_schema = json_content.get("schema", {})
        if body_schema:
            resolved = _unwrap_optional(body_schema, root)
            table = schema_to_table(resolved, root)
            if table:
                lines.append("**Request body:**")
                lines.append("")
                lines.extend(table)
                lines.append("")

    # Responses — prefer 200, else lowest numeric 2xx for deterministic output
    responses = operation.get("responses", {})
    numeric_success = sorted(
        (int(c), c) for c in responses if len(c) == 3 and c.isdigit() and c.startswith("2")
    )
    if numeric_success:
        code = "200" if any(c == "200" for _, c in numeric_success) else numeric_success[0][1]
        resp = responses[code]
        resp_content = resp.get("content", {})
        json_resp = resp_content.get("application/json", {})
        resp_schema = json_resp.get("schema", {})
        if resp_schema:
            resolved = _unwrap_optional(resp_schema, root)
            table = schema_to_table(resolved, root)
            if table:
                lines.append(f"**Response** (`{code}`):")
                lines.append("")
                lines.extend(table)
                lines.append("")

    # Error codes
    error_codes = [c for c in responses if not c.startswith("2")]
    if error_codes:
        lines.append("**Error codes:** " + ", ".join(f"`{c}`" for c in sorted(error_codes)))
        lines.append("")

    lines.append("---")
    lines.append("")
    return lines


def _operations(schema: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    """Index documented HTTP operations by stable path/method coordinates."""
    return {
        (path, method): operation
        for path, methods in schema.get("paths", {}).items()
        for method, operation in methods.items()
        if method in HTTP_METHODS
    }


def _render_surface(
    lines: list[str],
    schema: dict[str, Any],
    coordinates: set[tuple[str, str]],
) -> None:
    """Render one owner-scoped set of operations grouped by OpenAPI tag."""
    operations = _operations(schema)
    tag_groups: dict[str, list[tuple[str, str, dict[str, Any]]]] = {}
    for coordinate in sorted(coordinates):
        path, method = coordinate
        operation = operations[coordinate]
        tags = operation.get("tags", ["Other"])
        tag = tags[0] if tags else "Other"
        tag_groups.setdefault(tag, []).append((method, path, operation))

    for tag in sorted(tag_groups, key=str.casefold):
        tagged = sorted(tag_groups[tag], key=lambda item: (item[1], item[0]))
        lines.append(f"## {tag.title()}")
        lines.append("")
        for method, path, operation in tagged:
            lines.extend(render_operation(method, path, operation, schema))


def _render_reference(
    *,
    title: str,
    distribution: str,
    introduction: str,
    schema: dict[str, Any],
    coordinates: set[tuple[str, str]],
) -> str:
    """Render one distribution-owned REST reference page."""
    lines = [
        "<!-- Auto-generated by scripts/generate_api_docs.py — do not edit -->",
        "",
        f"# {title} REST API Reference",
        "",
        "| Package | Value |",
        "| --- | --- |",
        f"| Distribution | `{distribution}` |",
        "",
        introduction,
        "",
    ]
    _render_surface(lines, schema, coordinates)
    return "\n".join(lines)


def _render_framework_reference(base_schema: dict[str, Any]) -> str:
    """Render the framework page from an explicitly extension-free schema."""
    return _render_reference(
        title="Framework",
        distribution="archetype-ecs",
        introduction=(
            "This domain-free reference is generated with no world libraries installed. "
            "Start the server with `archetype serve` (default: "
            "`http://localhost:8000`)."
        ),
        schema=base_schema,
        coordinates=set(_operations(base_schema)),
    )


def generate_references() -> dict[Path, str]:
    """Generate deterministic owner-scoped REST reference pages."""
    base_schema = get_openapi_schema(world_libraries=())
    outputs = {OUTPUT: _render_framework_reference(base_schema)}
    for extension, schema, coordinates in get_extension_openapi_schemas(base_schema):
        outputs[extension.output] = _render_reference(
            title=extension.title,
            distribution=extension.distribution,
            introduction=(
                "These routes are contributed only when this trusted world-library "
                "manifest is explicitly installed in the API host."
            ),
            schema=schema,
            coordinates=coordinates,
        )
    return outputs


def generate() -> str:
    """Generate the framework-only REST reference."""
    return _render_framework_reference(get_openapi_schema(world_libraries=()))


def main() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    for output, content in generate_references().items():
        output.write_text(content, encoding="utf-8", newline="\n")
        print(f"Generated {output} ({len(content)} bytes)")


if __name__ == "__main__":
    main()
