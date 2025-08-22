from __future__ import annotations

import time
from typing import List, Tuple, Optional, Type

from daft import DataFrame

from archetype.core.aio.async_world import AsyncWorld
from archetype.core.aio.async_interfaces import iAsyncWorld
from archetype.core import Archetype, ArchetypeSignature, Component
from archetype.core.config import RunConfig, WorldConfig
from .profiling_shim import zone, frame_mark, plot, set_thread_name
from .logging_shim import (
    tick_header,
    tick_footer,
    show_plan,
    archetype_panel,
    dbg_sig,
    start_live_dashboard,
    update_live_dashboard,
    stop_live_dashboard,
)
from .validation_shim import validate_materialized, validate_pre_update, validate_post_update
from archetype.core.aio.async_interfaces import (
    iAsyncQueryManager,
    iAsyncUpdateManager,
    iAsyncSystem,
)
from archetype.core.aio.async_processor import AsyncProcessor


class InstrumentedAsyncWorld(iAsyncWorld):
    """
    A composition-based wrapper that adds logging/profiling to the async world.

    Notes:
    - Mirrors the public interface of `AsyncWorld` and delegates to an inner instance
    - Intercepts `run`, `step`, and per-archetype execution to add instrumentation
    - Keeps the underlying world unmodified and focused on correctness and performance
    """

    def __init__(
        self,
        world_config: WorldConfig,
        querier: iAsyncQueryManager,
        updater: iAsyncUpdateManager,
        system: iAsyncSystem,
    ) -> None:
        self.world = AsyncWorld(world_config, querier, updater, system)
        self._run_config: Optional[RunConfig] = None

    # ---------------------------
    # Lifecycle
    # ---------------------------
    async def run(self, run_config: RunConfig, **input_kwargs) -> None:  # type: ignore[override]
        self._run_config = run_config
        set_thread_name(f"world:{self.world_id}")
        with zone("world.run"):
            # Mirror AsyncWorld.run bookkeeping
            self.world.run_id = run_config.run_id
            if run_config.debug:
                # Prepare per-world file logging and live dashboard
                from .logging_shim import setup_world_file_logging
                setup_world_file_logging(self.world)
                start_live_dashboard(self.world)
            for _ in range(run_config.num_steps):
                await self.step(**input_kwargs)
            if run_config.debug:
                stop_live_dashboard(self.world)

    async def step(self, **input_kwargs) -> None:  # type: ignore[override]
        run_config = self._run_config or RunConfig()
        start_time = time.time()

        # Optional pretty header for the tick
        if run_config.debug:
            # Only emit header/footer in non-rich mode; rich live dashboard replaces them
            try:
                from .logging_shim import is_rich_tty
                if not is_rich_tty():
                    tick_header(self.world, num_sigs=len(self.world.active_signatures))
            except Exception:
                tick_header(self.world, num_sigs=len(self.world.active_signatures))

        frame_mark(f"tick:{self.tick}")
        with zone("world.step"):
            try:
                # Cheap metric: number of active signatures
                plot("active_signatures", float(len(self.world.active_signatures)))
            except Exception:
                pass

            # Execute all active archetypes in parallel, instrumenting each
            tasks = [
                self._run_archetype(sig, **input_kwargs)
                for sig in sorted(self.world.active_signatures, key=Archetype.get_name)
            ]
            results = await __import__("asyncio").gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    raise result
                df_mat, sig = result
                self.world._live[sig] = df_mat

            # Finalize tick, mirroring AsyncWorld.step
            self.world._clear_caches()
            self.world.tick += 1

        if run_config.debug:
            try:
                from .logging_shim import is_rich_tty
                if is_rich_tty():
                    counts = {Archetype.get_name(sig): self.world._live[sig].count_rows() for sig in self.world._live.keys()}
                    # Prepare richer stats snapshot for the renderer
                    world_stats = {
                        "tick": self.world.tick,
                        "active_archetypes": len(self.world.active_signatures),
                        "entities": sum(counts.values()),
                        # Future: spawns/despawns per tick tracked in world
                        "spawns": sum(len(v) for v in getattr(self.world, "_spawn_cache", {}).values()),
                        "despawns": sum(len(v) for v in getattr(self.world, "_despawn_cache", {}).values()),
                        "step_ms": (time.time() - start_time) * 1000.0,
                    }
                    self.world._dash_world_stats = world_stats  # type: ignore[attr-defined]
                    self.world._dash_archetype_stats = {
                        Archetype.get_name(sig): {
                            "entities": self.world._live[sig].count_rows(),
                            # Placeholders; can be refined with per-processor timing
                            "spawns": 0,
                            "despawns": 0,
                            "procs": getattr(self.world.system, "processors", None) and len(self.world.system.processors) or "-",
                            "step_ms": 0.0,
                        }
                        for sig in self.world._live.keys()
                    }  # type: ignore[attr-defined]
                    update_live_dashboard(self.world, self.world.tick, counts)
                else:
                    tick_footer(self.world, duration_ms=round((time.time() - start_time) * 1000, 3))
            except Exception:
                tick_footer(self.world, duration_ms=round((time.time() - start_time) * 1000, 3))

    # ---------------------------
    # Instrumented helpers
    # ---------------------------
    async def _run_archetype(self, sig: ArchetypeSignature, **input_kwargs) -> Tuple[DataFrame, ArchetypeSignature]:  # type: ignore[override]
        run_config = self._run_config or RunConfig()
        name = Archetype.get_name(sig)
        if run_config.debug:
            try:
                from .logging_shim import is_rich_tty
                if not is_rich_tty():
                    archetype_panel(self.world, sig, run_config=run_config)
            except Exception:
                archetype_panel(self.world, sig, run_config=run_config)
        with zone(f"archetype[{name}]"):
            # 1) Query previous state
            with zone("archetype.query"):
                df = await self.world.query_archetype(sig)
            if run_config.debug:
                try:
                    dbg_sig(self.world, sig, "query_result", rows=df.count_rows())
                except Exception:
                    pass

            # 2) Materialize mutations (spawns/despawns)
            df = self.materialize_mutations(df, sig)
            if getattr(run_config, "enable_validation", False):
                validate_materialized(df, sig)
            if run_config.debug:
                try:
                    spawn_len = len(self.world._spawn_cache.get(sig, []))  # type: ignore[attr-defined]
                    despawn_len = len(self.world._despawn_cache.get(sig, []))  # type: ignore[attr-defined]
                    dbg_sig(self.world, sig, "after_materialize", rows=df.count_rows(), spawns=spawn_len, despawns=despawn_len)
                except Exception:
                    pass

            # 3) Execute processors
            with zone("archetype.execute"):
                df = await self.world.execute(df, sig, run_config=run_config, world=self.world, **input_kwargs)
            if run_config.debug:
                try:
                    dbg_sig(self.world, sig, "after_execute", rows=df.count_rows())
                except Exception:
                    pass
            if getattr(run_config, "enable_validation", False):
                validate_pre_update(df, sig)

            # 4) Update store and return materialized frame
            with zone("archetype.update"):
                df_mat = await self.world.update(df, sig)
            if run_config.debug:
                try:
                    dbg_sig(self.world, sig, "after_update", rows=df_mat.count_rows())
                except Exception:
                    pass
            if getattr(run_config, "enable_validation", False):
                validate_post_update(df_mat, sig)
            return df_mat, sig

    def materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame:  # type: ignore[override]
        run_config = self._run_config or RunConfig()
        with zone("world.materialize"):
            df2 = self.world.materialize_mutations(df, sig)
        if run_config.debug:
            # Introspection opportunity to understand how mutations impact performance.
            show_plan(self.world, f"world.materialize[{Archetype.get_name(sig)}]", df2, run_config)
        if getattr(run_config, "enable_validation", False):
            # Reserved for future validation hooks
            pass
        return df2

    # ---------------------------
    # Delegation (public surface)
    # ---------------------------
    @property
    def active_signatures(self):  # type: ignore[override]
        return self.world.active_signatures

    @property
    def tick(self) -> int:
        return self.world.tick

    @property
    def world_id(self):
        return self.world.world_id

    @property
    def run_id(self):
        return self.world.run_id

    async def create_entity(self, components: List[Component]) -> int:  # type: ignore[override]
        return await self.world.create_entity(components)

    async def remove_entity(self, entity_id: int) -> None:  # type: ignore[override]
        return await self.world.remove_entity(entity_id)

    async def add_components(self, entity_id: int, components: List[Component]) -> None:  # type: ignore[override]
        return await self.world.add_components(entity_id, components)

    async def remove_components(self, entity_id: int, component_types: List[type[Component]]) -> None:  # type: ignore[override]
        return await self.world.remove_components(entity_id, component_types)

    async def add_processor(self, processor: AsyncProcessor):  # type: ignore[override]
        return await self.world.add_processor(processor)

    async def remove_processor(self, processor: Type[AsyncProcessor]):  # type: ignore[override]
        return await self.world.remove_processor(processor)

    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        ticks: Optional[List[int]] = None,
        entity_ids: Optional[List[int]] = None,
        components: Optional[List[Component]] = None,
        run_config: Optional[RunConfig] = None,
    ) -> DataFrame:  # type: ignore[override]
        return await self.world.query_archetype(sig, ticks, entity_ids, components, run_config)

    async def get_components(
        self,
        components: List[type[Component]] | List[Component],
        entity_ids: Optional[List[int]] = None,
    ) -> DataFrame:  # type: ignore[override]
        return await self.world.get_components(components, entity_ids)

    async def execute(self, df: DataFrame, sig: ArchetypeSignature, **input_kwargs) -> DataFrame:  # type: ignore[override]
        return await self.world.execute(df, sig, **input_kwargs)

    async def update(self, df: DataFrame, sig: ArchetypeSignature, tick: int | None = None) -> DataFrame:  # type: ignore[override]
        return await self.world.update(df, sig, tick)



