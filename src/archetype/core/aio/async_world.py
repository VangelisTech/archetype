# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from logging import getLogger
from typing import Any

import daft
import pyarrow as pa
from daft import DataFrame, col
from daft.functions import when
from uuid_utils import UUID  # noqa: F401 imported for type hints

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig
from archetype.core.interfaces import (
    ArchetypeSignature,
    iAsyncQueryManager,
    iAsyncSystem,
    iAsyncUpdateManager,
    iAsyncWorld,
)
from archetype.core.resources import Resources

# Type alias for hook functions
HookFn = Callable[..., Awaitable[None]]

logger = getLogger(__name__)


class AsyncWorld(iAsyncWorld):
    def __init__(
        self,
        world_config: WorldConfig,
        querier: iAsyncQueryManager,
        updater: iAsyncUpdateManager,
        system: iAsyncSystem,
    ):
        """
        Initialize the fully parallel async world.
        """
        # World Properties
        self.name = world_config.name
        self.world_id = world_config.world_id

        # Dependencies
        self.querier = querier
        self.updater = updater
        self.system = system

        # Resources: type-safe DI container for shared state
        self.resources = Resources()

        # Hooks: lifecycle callbacks for observability
        self._hooks: dict[str, list[HookFn]] = defaultdict(list)

        # Internal State
        self.tick = 0
        self.run_id: str | None = None
        self._next_entity_id = 1
        self._entity2sig: dict[int, ArchetypeSignature] = {}
        self._spawn_cache: dict[ArchetypeSignature, list[dict[str, Any]]] = {}
        self._despawn_cache: dict[ArchetypeSignature, list[int]] = {}

    # -------------------------------------------------------------------------
    # Hooks: Lifecycle callbacks for observability
    # -------------------------------------------------------------------------

    def add_hook(self, event: str, fn: HookFn) -> None:
        """
        Register a hook for lifecycle events.

        Supported events:
            - "pre_tick": Before any processing (world, tick)
            - "post_tick": After all processing (world, tick, results)
            - "on_spawn": When entity created (world, entity_id, components)
            - "on_despawn": When entity removed (world, entity_id)

        Example:
            world.add_hook("post_tick", lambda world, tick, **kw: print(f"Tick {tick} done"))
        """
        self._hooks[event].append(fn)

    def remove_hook(self, event: str, fn: HookFn) -> None:
        """Unregister a hook."""
        self._hooks[event] = [h for h in self._hooks[event] if h != fn]

    async def _fire_hooks(self, event: str, **kwargs) -> None:
        """Fire all hooks for an event, logging but not raising on errors."""
        for hook in self._hooks[event]:
            try:
                await hook(**kwargs)
            except Exception as e:
                logger.warning(f"Hook {getattr(hook, '__name__', hook)} failed on {event}: {e}")

    async def run(self, run_config: RunConfig, **input_kwargs) -> None:
        """
        Runs the world for the given run configuration.
        """

        # Pin run_id on first invocation; subsequent calls keep the existing
        # run_id so cross-step reads/writes remain continuous.
        if self.run_id is None:
            self.run_id = str(run_config.run_id)
        for _ in range(run_config.num_steps):
            await self.step(run_config, **input_kwargs)

    async def step(self, run_config: RunConfig, **input_kwargs) -> None:
        """
        Executes one full, parallel simulation tick.

        Tick Lifecycle:
            1. pre_tick hook fires
            2. For each archetype (parallel):
               a. Query previous state (tick N-1)
               b. Materialize mutations (spawn/despawn caches)
               c. Execute processors (priority order)
               d. Persist to store
            3. Increment tick
            4. post_tick hook fires (tick is now N+1)

        Note: Messages enqueued in tick N are dequeued in tick N+1.
        """
        # Pin run_id on first step so cross-step reads/writes share the same run.
        # run() pre-pins this; calling step() directly initializes it here.
        if self.run_id is None:
            self.run_id = str(run_config.run_id)

        debug = run_config.debug

        if debug:
            self._debug_log(
                "tick_start",
                tick=self.tick,
                active_entities=len(self._entity2sig),
                spawn_pending=sum(len(v) for v in self._spawn_cache.values()),
                despawn_pending=sum(len(v) for v in self._despawn_cache.values()),
            )

        # Fire pre-tick hooks
        await self._fire_hooks("pre_tick", world=self, tick=self.tick)

        sigs = sorted(self.active_signatures, key=Archetype.get_name)

        if debug:
            self._debug_log("archetypes_processing", tick=self.tick, count=len(sigs))

        futures = [self._run_archetype(sig, run_config, **input_kwargs) for sig in sigs]
        results = await asyncio.gather(*futures, return_exceptions=True)
        errors = {
            sig: r for sig, r in zip(sigs, results, strict=False) if isinstance(r, Exception)
        }  # from asyncio.gather docs: The order of result values corresponds to the order of awaitables in aws.

        if errors:
            raise RuntimeError(
                "; ".join(f"{Archetype.get_name(sig)}: {e}" for sig, e in errors.items())
            )

        self.tick += 1

        if debug:
            self._debug_log("tick_end", tick=self.tick, archetypes_processed=len(sigs))

        # Fire post-tick hooks
        await self._fire_hooks("post_tick", world=self, tick=self.tick, results=results)

    def _debug_log(self, event: str, **data) -> None:
        """Emit structured debug event."""
        import json

        payload = {"event": event, "world_id": str(self.world_id), **data}
        logger.debug(f"[archetype] {json.dumps(payload)}")

    async def _run_archetype(
        self, sig: ArchetypeSignature, run_config: RunConfig, **input_kwargs
    ) -> DataFrame:
        "Atomic sequence of a world step with a dedicated execution and materialization helper for future remote operators."

        df = await self.query_archetype(
            sig=sig,
            run_id=self.run_id,
            ticks=[self.tick - 1],
            entity_ids=None,
            components=None,
        )

        # 2. Materialize Mutations (Spawns/Despawns)
        df = self.materialize_mutations(df, sig)

        # 3. Execute Processors for this archetype via system
        df = await self.execute(df, sig, tick=self.tick, debug=run_config.debug, **input_kwargs)

        # 4. Update (returns materialized df with tick/world/run/entity_id set)
        df_mat = await self.update(df, sig, run_config)

        return df_mat

    # ---------------------------------------------------------------------
    #  Step Planning
    # ---------------------------------------------------------------------

    @property
    def active_signatures(self) -> set[ArchetypeSignature]:
        """Get the union of all archetypes that need processing this tick."""
        active_sigs = set(self._entity2sig.values())
        spawned_sigs = set(self._spawn_cache.keys())
        despawn_sigs = set(self._despawn_cache.keys())
        return active_sigs | spawned_sigs | despawn_sigs

    def materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame:
        # Handle Despawns
        if self._despawn_cache.get(sig):
            entities_to_despawn = list(
                set(self._despawn_cache[sig])
            )  # Dedupe and convert to list for Daft
            df = df.with_column(
                "is_active",
                when(col("entity_id").is_in(entities_to_despawn), then=False).otherwise(
                    col("is_active")
                ),
            )
            # Clear Cache
            self._despawn_cache[sig] = []

        # Handle Spawns
        if self._spawn_cache.get(sig):
            # Dedupe duplicate spawns, prioritizing "most recent cmd" (last write wins)
            # Dict keeps last value per key, so iterate forward to keep latest
            rows = list({row["entity_id"]: row for row in self._spawn_cache[sig]}.values())

            # Convert list of dicts to arrow table and eventually daft df
            pyarrow_schema = Archetype.get_archetype_schema(sig)
            arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
            spawns_df = daft.from_arrow(arrow_table)

            df = df.concat(spawns_df)
            self._spawn_cache[sig] = []

        return df

    async def _move_entity(
        self,
        entity_id: int,
        old_sig: ArchetypeSignature,
        new_sig: ArchetypeSignature,
        mutated_components: list[Component],
    ) -> dict[str, Any]:
        """
        Returns a row dict that is valid for the NEW archetype.
        Any field that is NOT in `mutated_components` is read from the
        previous most-recent row in the OLD archetype.
        """

        # 1) fetch *only* the single entity from old archetype's previous tick
        df = await self.query_archetype(
            sig=old_sig,
            run_id=self.run_id,
            ticks=[self.tick - 1],
            entity_ids=[entity_id],
            components=None,
        )

        row_list = df.to_pylist()

        if len(row_list) == 0:
            logger.warning(
                f"World {self.name} ({self.world_id}): Entity Migration Failed: No entity: {entity_id}"
            )
            return {}
        elif len(row_list) > 1:
            logger.warning(
                f"World {self.name} ({self.world_id}): Entity Migration Failed: Multiple entities: {entity_id}"
            )
            return {}
        else:
            # We should never have multiple entities with the same entity_id in the same tick
            row_dict = row_list[0]

        # 3) overlay components that change with the new ones (skips for remove component with 0 member list)
        for c in mutated_components:
            row_dict.update(c.to_row_dict())

        # _spawn_cache rows must already match the full archetype schema for
        # materialization; the updater will stamp canonical tick/world/run values.
        row_dict.update(
            {
                "entity_id": entity_id,
                "tick": self.tick,
                "world_id": str(self.world_id),
                "run_id": self.run_id,
                "is_active": True,
            }
        )
        return row_dict

    def _clear_caches(self, sig: ArchetypeSignature):
        self._spawn_cache.pop(sig, None)
        self._despawn_cache.pop(sig, None)

    # ---------------------------------------------------------------------
    # World Mutation Commands
    # ---------------------------------------------------------------------

    async def create_entity(self, components: list[Component]) -> int:
        entity_id = self._next_entity_id
        self._next_entity_id += 1
        sig = Archetype.sig_from_components(components)
        self._entity2sig[entity_id] = sig

        # Placeholder run_id; updater will stamp correct run_id on update
        row_dict = Archetype.to_row_dict(entity_id, self.tick, components, self.world_id, run_id="")
        self._spawn_cache.setdefault(sig, []).append(row_dict)
        await self._fire_hooks("on_spawn", world=self, entity_id=entity_id, components=components)
        return entity_id

    async def remove_entity(self, entity_id: int):
        sig = self._entity2sig.pop(entity_id, None)
        if sig is None:
            logger.warning(
                f"World {self.name} ({self.world_id}): Entity Removal Failed: No entity: {entity_id}"
            )
            return

        pending = self._spawn_cache.get(sig)
        if pending:
            remaining = [row for row in pending if row["entity_id"] != entity_id]
            if len(remaining) != len(pending):
                if remaining:
                    self._spawn_cache[sig] = remaining
                else:
                    del self._spawn_cache[sig]
                await self._fire_hooks("on_despawn", world=self, entity_id=entity_id)
                return

        self._despawn_cache.setdefault(sig, []).append(entity_id)
        await self._fire_hooks("on_despawn", world=self, entity_id=entity_id)

    async def add_components(self, entity_id: int, components: list[Component]) -> None:
        old_sig = self._entity2sig.get(entity_id)
        if not old_sig:
            logger.warning("add_components: entity %s not found", entity_id)
            return

        new_sig = Archetype.add_components(old_sig, [type(c) for c in components])
        if new_sig == old_sig:
            logger.debug("add_components: no-op; entity %s already has components", entity_id)
            return

        row = await self._move_entity(entity_id, old_sig, new_sig, components)

        # 1) mark *old row* inactive
        self._despawn_cache.setdefault(old_sig, []).append(entity_id)

        # 2) row to *insert* under new signature
        self._spawn_cache.setdefault(new_sig, []).append(row)

        # 3) update bookkeeping – atomically
        self._entity2sig[entity_id] = new_sig

    async def remove_components(
        self, entity_id: int, component_types: list[type[Component]]
    ) -> None:
        old_sig = self._entity2sig.get(entity_id)
        if old_sig is None:
            return

        new_sig = Archetype.remove_components(old_sig, component_types)
        if new_sig == old_sig:
            return

        row = await self._move_entity(
            entity_id, old_sig, new_sig, []
        )  # remove ≡ keep remaining columns

        self._despawn_cache.setdefault(old_sig, []).append(entity_id)
        self._spawn_cache.setdefault(new_sig, []).append(row)
        self._entity2sig[entity_id] = new_sig

    async def add_processor(self, processor: "AsyncProcessor"):
        await self.system.add_processor(processor)

    async def remove_processor(self, processor: type["AsyncProcessor"]):
        await self.system.remove_processor(processor)

    # ---------------------------------------------------------------------
    # Updater, Querier, System Facade methods
    # ---------------------------------------------------------------------
    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        run_config_or_ticks=None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[Component] | None = None,
        run_id: str | None = None,
        run_config: RunConfig | None = None,
    ) -> DataFrame:
        """Facade Method to query an archetype table by signature.

        Defaults to the world's most recent run_id when not provided. Accepts an optional
        run_config for instrumentation; ignored by the base querier.
        """

        # Back-compat: tests sometimes pass RunConfig as the 2nd positional arg
        if run_config_or_ticks is not None and not isinstance(run_config_or_ticks, list):
            run_config = run_config_or_ticks  # type: ignore[assignment]
        elif isinstance(run_config_or_ticks, list) and ticks is None:
            ticks = run_config_or_ticks

        effective_run_id = (
            run_id
            or (self.run_id and str(self.run_id))
            or (run_config and str(run_config.run_id))
            or ""
        )
        # Prefer to pass run_config if the querier supports it (instrumented); otherwise omit.
        try:
            return await self.querier.query_archetype(
                sig=sig,
                world_id=self.world_id,
                run_id=effective_run_id,
                ticks=ticks or [self.tick],
                entity_ids=entity_ids,
                components=components,
                run_config=run_config,
            )  # type: ignore[call-arg]
        except TypeError:
            return await self.querier.query_archetype(
                sig=sig,
                world_id=self.world_id,
                run_id=effective_run_id,
                ticks=ticks or [self.tick],
                entity_ids=entity_ids,
                components=components,
            )

    async def get_components(
        self,
        components: list[type[Component]],
        entity_ids: list[int] | None = None,
    ) -> DataFrame:
        """
        Query all active entities that contain at least the provided component types,
        unioning rows across all matching archetype signatures.

        Returns a DataFrame projected to the provided components' schema.
        """
        required_types = set(components)

        # Build output schema directly from component types
        temp_sig = tuple(sorted(components, key=lambda t: t.__name__))
        schema = Archetype.get_archetype_schema(temp_sig)
        df = daft.from_arrow(pa.Table.from_batches([], schema=schema))

        # Discover all archetype signatures that contain the requested components.
        all_sigs = await self.querier.list_signatures()
        matching_sigs = [sig for sig in all_sigs if required_types.issubset(set(sig))]

        # Read state at the latest committed tick (the previous tick to the one being processed).
        read_tick = max(self.tick - 1, 0)

        # Project each matching archetype to the requested schema and union.
        proj_cols = schema.names
        for sig in matching_sigs:
            sig_df = await self.querier.query_archetype(
                sig=sig,
                world_id=self.world_id,
                run_id=self.run_id,
                ticks=[read_tick],
            )
            df = df.concat(sig_df.select(*proj_cols))

        if entity_ids:
            df = df.where(col("entity_id").is_in(entity_ids))

        return df

    async def execute(self, df: DataFrame, sig: ArchetypeSignature, **input_kwargs) -> DataFrame:
        """
        Execute system processors, passing resources for type-safe dependency injection.
        """
        return await self.system.execute(df, sig, resources=self.resources, **input_kwargs)

    async def update(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        run_config: RunConfig,
        tick: int | None = None,
    ) -> DataFrame:
        """Update the store with the given archetypes."""
        df = await self.updater.update(df, sig, tick or self.tick, self.world_id, self.run_id)
        return df
