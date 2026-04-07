"""World — the ergonomic entry point for archetype simulations."""

from __future__ import annotations

from typing import TYPE_CHECKING

import daft
from daft import DataFrame

from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig
from archetype.dsl.proxy import AgentProxy

if TYPE_CHECKING:
    from archetype.core.archetype import Archetype


# ── In-memory storage (no LanceDB needed) ───────────────────────────────────


class _InMemoryQuerier:
    def __init__(self) -> None:
        self._store: dict[tuple, DataFrame] = {}

    async def query_archetype(self, **kwargs):
        import pyarrow as pa

        from archetype.core.archetype import Archetype as Arch

        sig = kwargs["sig"]
        ticks = kwargs.get("ticks", [0])
        key = (sig, ticks[0] if ticks else 0)
        if key in self._store:
            return self._store[key]
        schema = Arch.get_archetype_schema(sig)
        return daft.from_arrow(pa.Table.from_batches([], schema=schema))

    def store(self, sig, tick, df):
        self._store[(sig, tick)] = df


class _InMemoryUpdater:
    def __init__(self, querier: _InMemoryQuerier) -> None:
        self._querier = querier

    async def update(self, df, sig, tick, world_id, run_id):
        df = (
            df.with_column("tick", daft.lit(tick))
            .with_column("world_id", daft.lit(str(world_id)))
            .with_column("run_id", daft.lit(str(run_id)))
        )
        df_mat = df.collect()
        self._querier.store(sig, tick, df_mat)
        return df_mat


# ── World ────────────────────────────────────────────────────────────────────


class World:
    """Ergonomic simulation world. Async context manager.

    Usage::

        async with World("my-sim") as world:
            world.add_behavior(MyBehavior)
            await world.spawn(Agent(name="Alice"))
            await world.run(ticks=10)

            for agent in world.agents:
                print(agent.name)
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._world: AsyncWorld | None = None
        self._system: AsyncSystem | None = None

    async def __aenter__(self) -> World:
        querier = _InMemoryQuerier()
        updater = _InMemoryUpdater(querier)
        self._system = AsyncSystem()
        self._world = AsyncWorld(
            world_config=WorldConfig(name=self.name),
            querier=querier,
            updater=updater,
            system=self._system,
        )
        return self

    async def __aexit__(self, *exc) -> None:
        self._world = None
        self._system = None

    def _require_world(self) -> AsyncWorld:
        if self._world is None:
            raise RuntimeError("World must be used as 'async with World(...) as world:'")
        return self._world

    # ── Public API ───────────────────────────────────────────────────────

    def add_behavior(self, behavior_cls) -> None:
        """Register a @behavior-decorated class as a processor."""
        w = self._require_world()
        # behavior_cls is already an AsyncProcessor subclass (from @behavior).
        # AsyncSystem.add_processor is async but only does list.append,
        # so we access the processor list directly to avoid event loop issues.
        w.system.processors.append(behavior_cls())

    async def spawn(self, *components: Component) -> int:
        """Create an entity with the given components. Returns entity_id."""
        w = self._require_world()
        return await w.create_entity(list(components))

    async def run(self, ticks: int = 1) -> None:
        """Run the simulation for N ticks."""
        w = self._require_world()
        await w.run(RunConfig(num_steps=ticks))

    @property
    def agents(self) -> list[AgentProxy]:
        """Iterate all live entities as AgentProxy objects."""
        w = self._require_world()
        result = []
        for sig, df in w._live.items():
            rows = df.collect().to_pylist()
            # Build prefix list from component types in this signature
            prefixes = [c.__name__.lower() for c in sig]
            for row in rows:
                result.append(AgentProxy(row, prefixes))
        return result

    @property
    def tick(self) -> int:
        return self._require_world().tick

    @property
    def world_id(self):
        return self._require_world().world_id
