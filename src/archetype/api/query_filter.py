# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Safe parsing for the CLI/REST component-query filter grammar."""

from __future__ import annotations

import ast
import math
from dataclasses import dataclass
from typing import cast

from daft import Expression, col

_EXPECTED = "expected one comparison such as score__value > 0.5"


@dataclass(frozen=True, slots=True)
class ParsedWhere:
    """One validated column comparison and its Daft expression."""

    column: str
    expression: Expression


def _invalid(source: str) -> ValueError:
    return ValueError(f"Invalid where expression {source!r}; {_EXPECTED}")


def _literal(node: ast.expr, source: str) -> str | int | float | bool:
    if isinstance(node, ast.Constant):
        value = node.value
        if isinstance(value, str | int | float | bool) and value is not None:
            if isinstance(value, float) and not math.isfinite(value):
                raise _invalid(source)
            return value

    if isinstance(node, ast.Name):
        # Preserve the original CLI's ergonomic bare-string form while calls,
        # attributes, containers, and all other executable syntax stay invalid.
        return node.id

    if (
        isinstance(node, ast.UnaryOp)
        and isinstance(node.op, ast.UAdd | ast.USub)
        and isinstance(node.operand, ast.Constant)
        and isinstance(node.operand.value, int | float)
        and not isinstance(node.operand.value, bool)
    ):
        value = node.operand.value if isinstance(node.op, ast.UAdd) else -node.operand.value
        if isinstance(value, float) and not math.isfinite(value):
            raise _invalid(source)
        return value

    raise _invalid(source)


def parse_where(source: str) -> ParsedWhere:
    """Parse one inert ``column operator literal`` comparison.

    The grammar intentionally excludes calls, attribute access, Boolean
    composition, arithmetic, containers, and chained comparisons. The parsed
    tree is translated to a Daft expression; user input is never evaluated.
    """
    try:
        body = ast.parse(source, mode="eval").body
    except (SyntaxError, ValueError):
        raise _invalid(source) from None

    if (
        not isinstance(body, ast.Compare)
        or not isinstance(body.left, ast.Name)
        or len(body.ops) != 1
        or len(body.comparators) != 1
    ):
        raise _invalid(source)

    value = _literal(body.comparators[0], source)
    column = body.left.id
    left = col(column)
    operator = body.ops[0]

    # Daft's type stubs describe comparison dunders as bool even though the
    # runtime returns Expression objects.
    if isinstance(operator, ast.Gt):
        expression = left > value  # ty: ignore[unsupported-operator]
    elif isinstance(operator, ast.GtE):
        expression = left >= value  # ty: ignore[unsupported-operator]
    elif isinstance(operator, ast.Lt):
        expression = left < value  # ty: ignore[unsupported-operator]
    elif isinstance(operator, ast.LtE):
        expression = left <= value  # ty: ignore[unsupported-operator]
    elif isinstance(operator, ast.Eq):
        expression = cast("Expression", left == value)
    elif isinstance(operator, ast.NotEq):
        expression = cast("Expression", left != value)
    else:
        raise _invalid(source)

    return ParsedWhere(column=column, expression=expression)


__all__ = ["ParsedWhere", "parse_where"]
