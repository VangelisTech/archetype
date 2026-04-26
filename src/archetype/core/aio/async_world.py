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
import json
from logging import getLogger
from typing import Any, TypeVar

import daft
import pyarrow as pa
from daft import DataFrame, col
from daft.functions import when
from uuid_utils import UUID, uuid7  # noqa: F401 imported for type hints

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig
from archetype.core.hooks import (
    AsyncHookHandler,
    FireMode,
    HookEvent,
    HookHandle,
    OnComponentAdded,
    OnComponentRemoved,
    OnDespawn,
    OnSpawn,
    PostTick,
    PreTick,
)
from archetype.core.interfaces import (
    ArchetypeSignature,
    iAsyncHookBus,
    iAsyncQueryManager,
    iAsyncSystem,
    iAsyncUpdateManager,
    iAsyncWorld,
    iResourceContainer,
)

logger = getLogger(__name__)

_HookEventT = TypeVar("_HookEventT", bound=HookEvent)


class AsyncWorld(iAsyncWorld):
    def __init__(
        self,
        *,
        world_id: str,
        name: str,
        querier: iAsyncQueryManager,
        updater: iAsyncUpdateManager,
        system: iAsyncSystem,
        resources: iResourceContainer,
        hooks: iAsyncHookBus,
        run_id: str | None = None,
        tick: int = 0,
        next_entity_id: int = 1,
        entity2sig: dict[int, ArchetypeSignature] | None = None,
        spawn_cache: dict[ArchetypeSignature, list[dict[str, Any]]] | None = None,
        despawn_cache: dict[ArchetypeSignature, list[int]] | None = None,
    ):
        """
        Initialize the fully parallel async world.
        """
        # World Properties
        self.name = name
        self.world_id = world_id

        # Dependencies
        self.querier = querier       # Querier: read-only data access
        self.updater = updater       # Updater: write-only data access
        self.system = system         # System: processor executor
        self.resources = resources   # Resources: type-safe DI container for shared state
        self.hooks = hooks           # Hooks: typed lifecycle callbacks

        # State
        self.run_id = run_id or str(uuid7())
        self.tick = tick
        self.next_entity_id = next_entity_id
        self.entity2sig = entity2sig if entity2sig is not None else {}
        self.spawn_cache = spawn_cache if spawn_cache is not None else {}
        self.despawn_cache = despawn_cache if despawn_cache is not None else {}

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

        debug_handles = self._install_step_debug_hooks() if run_config.debug else ()
        try:
            # Fire pre-tick hooks
            await self.hooks.fire(PreTick(world_id=self.world_id, tick=self.tick))

            sigs = sorted(self.active_signatures, key=Archetype.get_name)

            futures = [self._run_archetype(sig, run_config, **input_kwargs) for sig in sigs]
            results = await asyncio.gather(*futures, return_exceptions=True)
            errors = {
                sig: r for sig, r in zip(sigs, results, strict=False) if isinstance(r, Exception)
            }  # from asyncio.gather docs: The order of result values corresponds to the order of awaitables in aws.

            if errors:
                raise RuntimeError(
                    "; ".join(f"{Archetype.get_name(sig)}: {e}" for sig, e in errors.items())
                )

            result_frames = dict(zip(sigs, results, strict=False))

            self.tick += 1

            # Fire post-tick hooks
            await self.hooks.fire(
                PostTick(world_id=self.world_id, tick=self.tick, results=result_frames)
            )
        finally:
            for handle in debug_handles:
                self.remove_hook(handle)

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
        active_sigs = set(self.entity2sig.values())
        spawned_sigs = set(self.spawn_cache.keys())
        despawn_sigs = set(self.despawn_cache.keys())
        return active_sigs | spawned_sigs | despawn_sigs

    def materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame:
        # Handle Despawns
        if self.despawn_cache.get(sig):
            entities_to_despawn = list(
                set(self.despawn_cache[sig])
            )  # Dedupe and convert to list for Daft
            df = df.with_column(
                "is_active",
                when(col("entity_id").is_in(entities_to_despawn), then=False).otherwise(
                    col("is_active")
                ),
            )
            # Clear Cache
            self.despawn_cache[sig] = []

        # Handle Spawns
        if self.spawn_cache.get(sig):
            # Dedupe duplicate spawns, prioritizing "most recent cmd" (last write wins)
            # Dict keeps last value per key, so iterate forward to keep latest
            rows = list({row["entity_id"]: row for row in self.spawn_cache[sig]}.values())

            # Convert list of dicts to arrow table and eventually daft df
            pyarrow_schema = Archetype.get_archetype_schema(sig)
            arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
            spawns_df = daft.from_arrow(arrow_table)

            df = df.concat(spawns_df)
            self.spawn_cache[sig] = []

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
        if len(row_list) > 1:
            logger.warning(
                f"World {self.name} ({self.world_id}): Entity Migration Failed: Multiple entities: {entity_id}"
            )
            return {}

        # We should never have multiple entities with the same entity_id in the same tick.
        row_dict = row_list[0]

        # 3) overlay components that change with the new ones (skips for remove component with 0 member list)
        for c in mutated_components:
            row_dict.update(c.to_row_dict())

        # 4) stamp housekeeping columns for new location
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
        self.spawn_cache.pop(sig, None)
        self.despawn_cache.pop(sig, None)

    # ---------------------------------------------------------------------
    # World Mutation Commands
    # ---------------------------------------------------------------------

    async def create_entity(self, components: list[Component]) -> int:
        """Spawn a new entity with an auto-assigned id. Fires ``OnSpawn``."""
        entity_id = self.next_entity_id
        self.next_entity_id += 1
        await self._register_entity(entity_id, components)
        return entity_id

    async def _register_entity(self, entity_id: int, components: list[Component]) -> None:
        """Single source of truth for entity spawn. Every path that makes a
        new entity observable to the world MUST go through this method so
        ``OnSpawn`` is always fired exactly once with the correct payload."""
        sig = Archetype.sig_from_components(components)
        self.entity2sig[entity_id] = sig
        row_dict = Archetype.to_row_dict(entity_id, self.tick, components, self.world_id, run_id=self.run_id)
        self.spawn_cache.setdefault(sig, []).append(row_dict)
        await self.hooks.fire(
            OnSpawn(world_id=self.world_id, entity_id=entity_id, components=list(components))
        )

    async def remove_entity(self, entity_id: int) -> None:
        """Despawn an entity. Cancels a pending same-tick spawn if present,
        otherwise queues a despawn row for the current tick. Fires
        ``OnDespawn`` iff the entity existed."""
        sig = self.entity2sig.pop(entity_id, None)
        if sig is None:
            logger.warning(
                "World %s (%s): Entity Removal Failed: No entity: %s",
                self.name,
                self.world_id,
                entity_id,
            )
            return

        pending = self.spawn_cache.get(sig)
        if pending:
            remaining = [row for row in pending if row["entity_id"] != entity_id]
            if len(remaining) != len(pending):
                if remaining:
                    self.spawn_cache[sig] = remaining
                else:
                    del self.spawn_cache[sig]
                await self.hooks.fire(OnDespawn(world_id=self.world_id, entity_id=entity_id))
                return

        self.despawn_cache.setdefault(sig, []).append(entity_id)
        await self.hooks.fire(OnDespawn(world_id=self.world_id, entity_id=entity_id))

    async def update_entity(self, entity_id: int, components: list[Component]) -> None:
        """Overlay component values on an existing entity without changing its archetype.

        The entity keeps its current signature. Only the supplied component
        fields are overwritten. Used for value mutations (e.g., Position.x += 1)
        as distinct from add_components which extends the signature.
        """
        sig = self.entity2sig.get(entity_id)
        if sig is None:
            logger.warning("update_entity: entity %s not found", entity_id)
            return

        row = await self._move_entity(entity_id, sig, sig, components)
        if not row:
            logger.warning("update_entity: entity %s has no prior row", entity_id)
            return

        # Mark prior row inactive, insert updated row under same sig
        self.despawn_cache.setdefault(sig, []).append(entity_id)
        self.spawn_cache.setdefault(sig, []).append(row)

    async def add_components(self, entity_id: int, components: list[Component]) -> None:
        """Attach additional components to an existing entity. Fires
        ``OnComponentAdded`` iff the signature actually changes."""
        old_sig = self.entity2sig.get(entity_id)
        if not old_sig:
            logger.warning("add_components: entity %s not found", entity_id)
            return

        new_sig = Archetype.add_components(old_sig, [type(c) for c in components])
        if new_sig == old_sig:
            logger.debug("add_components: no-op; entity %s already has components", entity_id)
            return

        row = await self._move_entity(entity_id, old_sig, new_sig, components)

        # 1) mark *old row* inactive
        self.despawn_cache.setdefault(old_sig, []).append(entity_id)

        # 2) row to *insert* under new signature
        self.spawn_cache.setdefault(new_sig, []).append(row)

        # 3) update bookkeeping – atomically
        self.entity2sig[entity_id] = new_sig

        await self.hooks.fire(
            OnComponentAdded(
                world_id=self.world_id,
                entity_id=entity_id,
                components=list(components),
            )
        )

    async def remove_components(
        self, entity_id: int, component_types: list[type[Component]]
    ) -> None:
        """Detach components from an existing entity. Fires
        ``OnComponentRemoved`` iff the signature actually changes."""
        old_sig = self.entity2sig.get(entity_id)
        if old_sig is None:
            return

        new_sig = Archetype.remove_components(old_sig, component_types)
        if new_sig == old_sig:
            return

        row = await self._move_entity(
            entity_id, old_sig, new_sig, []
        )  # remove ≡ keep remaining columns

        self.despawn_cache.setdefault(old_sig, []).append(entity_id)
        self.spawn_cache.setdefault(new_sig, []).append(row)
        self.entity2sig[entity_id] = new_sig

        await self.hooks.fire(
            OnComponentRemoved(
                world_id=self.world_id,
                entity_id=entity_id,
                component_types=list(component_types),
            )
        )

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

    # -------------------------------------------------------------------------
    # Hooks: Typed lifecycle callbacks for observability
    # -------------------------------------------------------------------------

    def add_hook(
        self,
        event_type: type[_HookEventT],
        fn: AsyncHookHandler[_HookEventT],
        *,
        mode: FireMode = "blocking",
    ) -> HookHandle:
        """Register a handler for lifecycle events.

        Args:
            event_type: Event dataclass to listen for: PreTick, PostTick,
                OnSpawn, OnDespawn, OnComponentAdded, or OnComponentRemoved.
            fn: Async callable that accepts the matching event instance.
            mode: "blocking" awaits the handler inline. "spawn" schedules it
                with asyncio.create_task so the tick does not wait.

        Returns:
            HookHandle to pass back to remove_hook when the handler should be
            unregistered.

        Example:
            from archetype.core.hooks import PostTick

            async def log_tick(event: PostTick) -> None:
                print(f"tick {event.tick} done, {len(event.results)} archetypes")

            handle = world.add_hook(PostTick, log_tick)
            # ... later ...
            world.remove_hook(handle)
        """
        return self.hooks.add(event_type, fn, mode=mode)

    def remove_hook(self, handle: HookHandle) -> None:
        """Unregister a hook by handle.

        The operation is idempotent. Passing a handle that was already removed,
        or a handle minted by another world, is a no-op.
        """
        self.hooks.remove(handle)

    def _install_step_debug_hooks(self) -> tuple[HookHandle, HookHandle]:
        """Install temporary hooks for RunConfig(debug=True) step logging."""

        async def log_tick_start(event: PreTick) -> None:
            self._debug_log(
                "tick_start",
                tick=event.tick,
                active_entities=len(self.entity2sig),
                spawn_pending=sum(len(v) for v in self.spawn_cache.values()),
                despawn_pending=sum(len(v) for v in self.despawn_cache.values()),
            )
            self._debug_log(
                "archetypes_processing",
                tick=event.tick,
                count=len(self.active_signatures),
            )

        async def log_tick_end(event: PostTick) -> None:
            total_live = sum(
                df.count_rows() if hasattr(df, "count_rows") else 0 for df in event.results.values()
            )
            self._debug_log("tick_end", tick=event.tick, live_entities=total_live)

        return (
            self.add_hook(PreTick, log_tick_start),
            self.add_hook(PostTick, log_tick_end),
        )

    def _debug_log(self, event: str, **data) -> None:
        """Emit structured debug event."""
        payload = {"event": event, "world_id": str(self.world_id), **data}
        logger.debug(f"[archetype] {json.dumps(payload)}")
