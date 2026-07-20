# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Handle-level projection sugar, async flavor.

Sync-parity counterparts live in :mod:`archetype.projections.sync`
(runtime.md R5). Passing a sync handle here raises before any query.
"""

from __future__ import annotations

import inspect
from typing import Protocol

from daft import DataFrame

from archetype.core.component import Component
from archetype.projections.frames import overview


def require_unique_labels(components: tuple[type[Component], ...]) -> None:
    """Reject distinct component types whose lowercased names collide.

    A duplicate label would silently drop one component's series from the
    overview; failing loud preserves the read model's completeness. Shared
    by the async and sync ``world_overview``.
    """
    labels: dict[str, type[Component]] = {}
    for comp in components:
        label = comp.__name__.lower()
        other = labels.get(label)
        if other is not None and other is not comp:
            raise ValueError(
                f"components {other!r} and {comp!r} both label as '{label}'; "
                "call archetype.projections.overview with explicit labels"
            )
        labels[label] = comp


class QueriesLike(Protocol):
    """Structural surface the async projection helpers need from a handle."""

    async def query(self, *components: type[Component]) -> DataFrame: ...


def _require_async(world: QueriesLike) -> None:
    if not inspect.iscoroutinefunction(world.query):
        raise TypeError(
            "world.query is not async; for SyncRuntimeWorld-shaped handles "
            "use archetype.projections.sync"
        )


async def world_overview(world: QueriesLike, *components: type[Component]) -> DataFrame:
    """Per-tick population series for each component type, labeled by name.

    Labels are lowercased class names. Two distinct components sharing a
    name would silently shadow each other, so collisions raise instead —
    call :func:`archetype.projections.overview` with explicit labels to
    disambiguate.
    """
    if not components:
        raise ValueError("world_overview requires at least one component type")
    _require_async(world)
    require_unique_labels(components)
    frames = {comp.__name__.lower(): await world.query(comp) for comp in components}
    return overview(**frames)
