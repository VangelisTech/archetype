#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Generate REST API reference markdown from FastAPI's OpenAPI schema.

Usage:
    python scripts/generate_api_docs.py

Writes docs/reference/rest-api.md.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DOCS_DIR = Path(__file__).resolve().parent.parent / "docs" / "reference"
OUTPUT = DOCS_DIR / "rest-api.md"


def get_openapi_schema() -> dict[str, Any]:
    """Import the FastAPI app and extract its OpenAPI schema."""
    from archetype.api.app import create_app

    app = create_app()
    return app.openapi()


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


def generate() -> str:
    """Generate the full REST API reference markdown."""
    schema = get_openapi_schema()
    lines: list[str] = []

    lines.append("<!-- Auto-generated by scripts/generate_api_docs.py — do not edit -->")
    lines.append("")
    lines.append("# REST API Reference")
    lines.append("")
    lines.append(
        "This reference is auto-generated from the FastAPI application's OpenAPI schema. "
        "Start the server with `archetype serve` (default: `http://localhost:8000`)."
    )
    lines.append("")

    # Group by tag
    paths = schema.get("paths", {})
    tag_groups: dict[str, list[tuple[str, str, dict]]] = {}
    for path, methods in paths.items():
        for method, operation in methods.items():
            if method in ("get", "post", "put", "patch", "delete"):
                tags = operation.get("tags", ["Other"])
                tag = tags[0] if tags else "Other"
                tag_groups.setdefault(tag, []).append((method, path, operation))

    for tag in sorted(tag_groups, key=str.casefold):
        operations = sorted(tag_groups[tag], key=lambda item: (item[1], item[0]))
        lines.append(f"## {tag.title()}")
        lines.append("")
        for method, path, operation in operations:
            lines.extend(render_operation(method, path, operation, schema))

    return "\n".join(lines)


def main() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    content = generate()
    OUTPUT.write_text(content, encoding="utf-8", newline="\n")
    print(f"Generated {OUTPUT} ({len(content)} bytes)")


if __name__ == "__main__":
    main()
