# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""DataFrame processor contract for the Smol teaching engine."""

from __future__ import annotations

from daft import DataFrame

from .component import Component


class Processor:
    """Transform every matching archetype table once per step.

    A processor runs when its declared Components are a subset of a table's
    signature. Lower priorities run first. Implementations must preserve the
    input entity IDs, metadata, and columns; Smol validates that invariant
    before publishing the step.
    """

    components: tuple[type[Component], ...] = ()
    priority: int = 10

    def process(self, df: DataFrame, *, tick: int) -> DataFrame:
        """Return a lazy DataFrame transform for one matching table."""

        del tick
        return df
