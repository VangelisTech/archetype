#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Generate the native operation reference without opening a server or owner."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

from archetype_native import wire


def main():
    tree = ast.parse(inspect.getsource(wire))
    arguments = {
        name: ""
        for name in ("status", "start", "stop", "resolve", "program_resolve", "program_describe")
    }
    for node in ast.walk(tree):
        if not isinstance(node, ast.If) or not isinstance(node.test, ast.Compare):
            continue
        compare = node.test
        if not isinstance(compare.left, ast.Name) or compare.left.id != "name":
            continue
        if len(compare.comparators) != 1:
            continue
        selector = compare.comparators[0]
        if isinstance(selector, ast.Constant):
            names = (selector.value,)
        elif isinstance(selector, ast.Tuple) and all(
            isinstance(n, ast.Constant) for n in selector.elts
        ):
            names = tuple(n.value for n in selector.elts)
        else:
            continue
        for statement in node.body:
            if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call):
                call = statement.value
                if (
                    isinstance(call.func, ast.Name)
                    and call.func.id == "fields"
                    and len(call.args) == 2
                ):
                    if (
                        isinstance(call.args[0], ast.Name)
                        and call.args[0].id == "args"
                        and isinstance(call.args[1], ast.Constant)
                    ):
                        for name in names:
                            arguments[name] = call.args[1].value
                        break
    arguments.update(history="offset limit", read="receipt component offset limit")
    expected = {operation.name for operation in wire.CAPABILITIES}
    if set(arguments) != expected:
        raise RuntimeError(f"Incomplete operation reference: {expected ^ set(arguments)}")
    lines = [
        "# Native operations",
        "",
        "Generated from the closed version 1 request decoder and capability map.",
        "",
        "HTTP uses `POST /invoke`; MCP exposes the same operations through the installed server. CLI `invoke` sends the same JSON request. All paths authenticate and authorize the complete resource grant set before native lookup.",
        "",
        "```json",
        '{"version":1,"operation":"history","resource":"experiment","arguments":{"offset":"0","limit":"32"}}',
        "```",
        "",
        "Requests contain exactly `version`, `operation`, `resource`, and `arguments`. Duplicate or unknown fields fail validation. Requests are bounded to 64 KiB and responses to 16 KiB. Pages contain at most 32 rows. Native Int64 and counters use canonical decimal strings; Float64 cells use finite IEEE-754 bit strings with positive zero; Bool cells use JSON booleans. Artifact metadata may independently contain null values.",
        "",
        "| Operation | Required argument fields | Capability |",
        "|---|---|---|",
    ]
    for operation, capability in sorted(wire.CAPABILITIES.items(), key=lambda item: item[0].name):
        fields = arguments[operation.name]
        lines.append(
            f"| `{operation.name}` | {', '.join(f'`{field}`' for field in fields.split()) or 'None'} | `{capability}` |"
        )
    lines.extend(
        [
            "",
            "Composition additionally requires read grants on every referenced program. Create requires a read grant on its pinned program. Fork and hosted context publication require the same capability on their source resource. Context scopes and all native paths are configured by the operator; clients cannot select physical storage or compiler paths.",
            "",
            "Inline artifact uploads contain base64 bytes, a canonical UUIDv7, and a relative logical path. Decoded content is limited to 32 KiB. Larger local batches use the Python artifact preparation and publication workflow described in [Artifacts](../guide/artifacts.md).",
            "",
            "Errors report a bounded public code and outcome. `not_dispatched` means admission did not occur; `unknown` requires checking the recorded admission identity before retry. Cancelling a caller does not release ownership of admitted native work.",
            "",
        ]
    )
    reference = Path(__file__).resolve().parents[1] / "docs/reference"
    (reference / "native-operations.md").write_text("\n".join(lines))
    (reference / "rest-api.md").write_text(
        "# HTTP API\n\nThe current API uses the shared [native operation contract](native-operations.md).\n"
    )


if __name__ == "__main__":
    main()
