#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Audit the exact supported wire gate, grant ordering and error/outcome taxonomy."""

from __future__ import annotations

import ast
import dataclasses
import inspect
import json
import textwrap
from pathlib import Path
from typing import get_args

from archetype_native import ingress, wire

ROOT = Path(__file__).resolve().parents[1]
CODES = frozenset(
    {
        "unavailable",
        "unauthenticated",
        "forbidden",
        "invalid_request",
        "busy",
        "operation_failed",
        "conflict",
        "corrupt_data",
        "resource_limit",
        "unsupported_format",
    }
)


def main() -> int:
    operations = set(get_args(wire.Operation.__value__))
    if not operations or operations != set(wire.CAPABILITIES):
        raise ValueError("Closed operation/capability inventory differs")
    names = [operation.name for operation in operations]
    if len(set(names)) != len(names):
        raise ValueError("Duplicate public operation selector")
    for operation in operations:
        if not dataclasses.is_dataclass(operation) or not operation.__dataclass_params__.frozen:
            raise ValueError("Public operation is not immutable")
        if not wire.CAPABILITIES[operation]:
            raise ValueError("Operation has no explicit capability")
    source = inspect.getsource(ingress.Ingress.invoke)
    # This AST check preserves the accepted authority-before-binding order.
    tree = ast.parse(textwrap.dedent(source))
    positions = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if isinstance(function, ast.Attribute) and function.attr in {
            "authenticate",
            "decode",
            "requirements",
            "_call",
        }:
            positions.setdefault(function.attr, node.lineno)
        if isinstance(function, ast.Name) and function.id == "_validate_request":
            positions[function.id] = node.lineno
    ordered = [
        positions[name]
        for name in ("authenticate", "decode", "requirements", "_validate_request", "_call")
    ]
    if ordered != sorted(ordered) or len(set(ordered)) != len(ordered):
        raise ValueError("Authentication/complete grants must precede binding/native resolution")
    docs = (ROOT / "docs/reference/native-operations.md").read_text()
    for operation in operations:
        if f"| `{operation.name}` |" not in docs:
            raise ValueError("Native operation missing from public reference")
    request = wire.Request("example", wire.Status())
    for code in CODES:
        for outcome in ("not_dispatched", "unknown"):
            response = {"version": 1, "ok": False, "error": {"code": code, "outcome": outcome}}
            if wire.decode_response(json.dumps(response).encode(), request) != response:
                raise ValueError("Error taxonomy differs")
    for invalid in (
        {"code": "raw_backend_error", "outcome": "unknown"},
        {"code": "conflict", "outcome": "rolled_back"},
        {"code": "operation_failed", "outcome": "unknown", "message": "/physical/path"},
    ):
        try:
            wire.decode_response(
                json.dumps({"version": 1, "ok": False, "error": invalid}).encode(), request
            )
        except ValueError:
            continue
        raise ValueError("Public error gate admits an unknown/unsafe field or outcome")
    # Every wire operation is referenced by both closed decoding and ingress
    # handling; runtime tests separately own effects and multi-resource grants.
    decoder = inspect.getsource(wire.Request.decode)
    handler = inspect.getsource(ingress)
    for operation in operations:
        if operation.__name__ not in decoder or "w." + operation.__name__ not in handler:
            raise ValueError("Operation lacks a closed decoder or ingress handler")
    print(
        f"Supported gate audit passed: {len(operations)} operations, complete grant ordering, {len(CODES)} bounded error codes"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
