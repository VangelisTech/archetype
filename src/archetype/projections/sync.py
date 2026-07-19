# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Handle-level projection sugar, sync flavor (runtime.md R5 parity).

Blocking counterpart of :mod:`archetype.projections.worlds` for
``SyncRuntimeWorld``-shaped handles. Passing an async handle here raises
before any query.
"""

from __future__ import annotations

import inspect
from typing import Protocol

from daft import DataFrame

from archetype.core.component import Component
from archetype.projections.frames import overview


class SyncQueriesLike(Protocol):
    """Structural surface the sync projection helpers need from a handle."""

    def query(self, *components: type[Component]) -> DataFrame: ...


def _require_sync(world: SyncQueriesLike) -> None:
    if inspect.iscoroutinefunction(world.query):
        raise TypeError(
            "world.query is async; for async world handles use archetype.projections.worlds"
        )


def world_overview(world: SyncQueriesLike, *components: type[Component]) -> DataFrame:
    """Per-tick population series for each component type, labeled by name."""
    if not components:
        raise ValueError("world_overview requires at least one component type")
    _require_sync(world)
    frames = {comp.__name__.lower(): world.query(comp) for comp in components}
    return overview(**frames)
