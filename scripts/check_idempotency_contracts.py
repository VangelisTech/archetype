#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Check the supported 0.7 request-identity matrix and executable oracles."""

from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    manifest = json.loads((ROOT / "quality/native_idempotency.json").read_text())
    rows = manifest["rows"]
    if manifest["version"] != 1 or manifest["scope"] != "0.7" or len(rows) != 6:
        raise ValueError("Incomplete supported idempotency inventory")
    normative = (ROOT / "docs/guide/specification.md").read_text()
    identities = set()
    for row in rows:
        if row["id"] in identities:
            raise ValueError("Duplicate idempotency scope")
        identities.add(row["id"])
        expected = f"| `{row['id']}` | {row['contract']} | `{row['oracle']}` |"
        if expected not in normative:
            raise ValueError("Normative idempotency row differs: " + row["id"])
        filename, class_name, method = row["oracle"].split("::")
        tree = ast.parse((ROOT / filename).read_text())
        cls = next(
            (
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == class_name
            ),
            None,
        )
        if cls is None or not any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == method
            for node in cls.body
        ):
            raise ValueError("Missing executable idempotency oracle: " + row["oracle"])
    if normative.count("| Executable oracle |") != 1:
        raise ValueError("Missing or duplicate supported idempotency matrix")
    print("Supported 0.7 idempotency audit passed: six exact identity/retry oracles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
