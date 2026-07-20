# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""GraphView: previous-tick frames for cross-archetype reads in processors.

Design D3 (docs/design/graph-system.md). ``PostTick`` delivers every
just-persisted frame (``results: dict[ArchetypeSignature, DataFrame]``), so a
``GraphView`` registered as a resource and a ``PostTick`` hook gives
processors at tick N read-only, lazy frames of tick N−1 state — strictly
previous-tick visibility, no core changes, no query-service reentrancy.

Wiring is the declarative world config::

    view = GraphView()
    world = runtime.world(
        "sim",
        processors=[MyEdgeProcessor()],
        resources=[view],
        hooks=[(PostTick, view.on_post_tick)],
    )

Sync worlds register ``view.on_post_tick_sync`` instead. Inside a processor::

    view = resources.require(GraphView)
    nodes = view.frame(Node)   # tick N-1 rows, or None before the first tick
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft import DataFrame, col

from archetype.core.component import Component
from archetype.core.hooks import PostTick

if TYPE_CHECKING:
    from archetype.core.interfaces import ArchetypeSignature


def _same_component(a: type[Component], b: type[Component]) -> bool:
    """Schema identity, as the query path defines it.

    A resumed durable world may register a different but schema-identical
    class object, so class identity alone under-matches; name plus prefixed
    schema is the identity the engine keys tables by.
    """
    return a is b or (
        a.__name__ == b.__name__ and a.get_prefixed_schema() == b.get_prefixed_schema()
    )


class GraphView:
    """Read-only window onto the last persisted tick's frames.

    Frames stay lazy; this class never materializes. The view is empty
    (``tick == -1``, ``frame(...) is None``) until the first tick persists.
    """

    def __init__(self) -> None:
        self._frames: dict[ArchetypeSignature, DataFrame] = {}
        self.tick: int = -1

    async def on_post_tick(self, event: PostTick) -> None:
        """Async hook handler: capture the tick's persisted frames."""
        self._capture(event)

    def on_post_tick_sync(self, event: PostTick) -> None:
        """Sync hook handler for sync worlds (runtime.md R5 parity)."""
        self._capture(event)

    def _capture(self, event: PostTick) -> None:
        self._frames = dict(event.results)
        self.tick = event.tick - 1  # PostTick.tick is the next tick

    def frames(self) -> list[tuple[ArchetypeSignature, DataFrame]]:
        """The captured (signature, frame) pairs — read-only iteration order."""
        return list(self._frames.items())

    def population(self) -> DataFrame | None:
        """Distinct ids of every entity alive at the captured tick.

        The lazy union of ``entity_id`` across all captured frames — the
        liveness authority for cascade's dangling-edge anti-join. ``None``
        before the first tick.
        """
        parts: list[DataFrame] = []
        for frame in self._frames.values():
            part = frame
            if "is_active" in part.column_names:
                part = part.where(col("is_active"))
            parts.append(part.select("entity_id"))
        if not parts:
            return None
        out = parts[0]
        for part in parts[1:]:
            out = out.concat(part)
        return out.distinct()

    def frame(self, *components: type[Component]) -> DataFrame | None:
        """Concat the frames whose archetype contains all ``components``.

        Membership uses schema identity — name plus prefixed schema — not
        Python class identity, matching the engine's query path: a resumed
        durable world may hold a different but schema-identical class object
        in its signatures. Each matching frame is projected to ``entity_id``,
        ``tick``, and the requested components' prefixed columns, so
        heterogeneous archetypes concat cleanly. Rows despawned at capture
        time are filtered out. Returns ``None`` before the first tick or when
        nothing matches.
        """
        if not components:
            raise ValueError("frame requires at least one component type")
        columns = ["entity_id", "tick"]
        for comp in components:
            prefix = comp.get_prefix()
            columns.extend(f"{prefix}{field}" for field in comp.model_fields)

        matches: list[DataFrame] = []
        for signature, frame in self._frames.items():
            if all(
                any(_same_component(comp, member) for member in signature) for comp in components
            ):
                part = frame
                if "is_active" in part.column_names:
                    part = part.where(col("is_active"))
                matches.append(part.select(*columns))
        if not matches:
            return None
        out = matches[0]
        for part in matches[1:]:
            out = out.concat(part)
        return out
