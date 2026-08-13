# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure grader execution over one caller-owned lazy frame."""

from __future__ import annotations

from collections.abc import Sequence
from inspect import isawaitable

from daft import DataFrame

from archetype.evaluation.models import FrameGrader, GraderOutput


async def run_graders(
    frame: DataFrame,
    graders: Sequence[FrameGrader],
) -> list[GraderOutput]:
    """Execute callbacks and flatten their non-empty outputs."""

    if not graders:
        raise ValueError("run_graders requires at least one grader")

    results: list[GraderOutput] = []
    for grader in graders:
        raw = grader(frame)
        output = await raw if isawaitable(raw) else raw
        if isinstance(output, Sequence) and not isinstance(output, str | bytes):
            if not output:
                name = getattr(grader, "__name__", type(grader).__name__)
                raise ValueError(f"grader {name!r} returned no outputs")
            results.extend(output)
        else:
            results.append(output)
    return results


__all__ = ["run_graders"]
