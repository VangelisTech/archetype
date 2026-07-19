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

from logging import getLogger
from typing import Any, TypeVar

import daft
import pyarrow as pa
from daft import DataFrame, col
from daft.functions import when
from uuid_utils import uuid7

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig
from archetype.core.errors import TickExecutionError, TickFailure
from archetype.core.hooks import (
    HookEvent,
    HookHandle,
    OnComponentAdded,
    OnComponentRemoved,
    OnDespawn,
    OnSpawn,
    PostTick,
    PreTick,
    SyncHookHandler,
)
from archetype.core.interfaces import (
    ArchetypeSignature,
    iProcessor,
    iQueryManager,
    iResourceContainer,
    iSyncHookBus,
    iSystem,
    iUpdateManager,
    iWorld,
)

logger = getLogger(__name__)

_HookEventT = TypeVar("_HookEventT", bound=HookEvent)


class SyncWorld(iWorld):
    def __init__(
        self,
        *,
        world_id: str,
        name: str | None,
        querier: iQueryManager,
        updater: iUpdateManager,
        system: iSystem,
        resources: iResourceContainer,
        hooks: iSyncHookBus,
        run_id: str | None = None,
        tick: int = 0,
        next_entity_id: int = 1,
        entity2sig: dict[int, ArchetypeSignature] | None = None,
        spawn_cache: dict[ArchetypeSignature, list[dict[str, Any]]] | None = None,
        despawn_cache: dict[ArchetypeSignature, list[int]] | None = None,
    ):
        """
        Initialize the synchronous world.
        """
        # World Properties
        self.name = name
        self.world_id = world_id

        # Dependencies
        self.querier = querier  # Querier: read-only data access
        self.updater = updater  # Updater: write-only data access
        self.system = system  # System: processor executor
        self.resources = resources  # Resources: type-safe DI container for shared state
        self.hooks = hooks  # Hooks: typed lifecycle callbacks

        # State
        self.run_id = run_id or str(uuid7())
        self.tick = tick
        self.next_entity_id = next_entity_id
        self.entity2sig = entity2sig if entity2sig is not None else {}
        self.spawn_cache = spawn_cache if spawn_cache is not None else {}
        self.despawn_cache = despawn_cache if despawn_cache is not None else {}

    def run(self, run_config: RunConfig, **input_kwargs):
        """
        Runs the world for the given run configuration.
        """
        # Pin run_id on first invocation; subsequent calls keep the existing
        # run_id so cross-step reads/writes remain continuous.
        if self.run_id is None:
            self.run_id = str(run_config.run_id)
        for _ in range(run_config.num_steps):
            self.step(run_config, **input_kwargs)

    def step(self, run_config: RunConfig, **input_kwargs) -> None:
        """
        Executes one full simulation tick.
        """
        # Ensure run_id is set for entity creation during step
        if self.run_id is None:
            self.run_id = str(run_config.run_id)

        self.hooks.fire(PreTick(world_id=self.world_id, tick=self.tick))

        sigs = sorted(self.active_signatures, key=Archetype.get_name)

        # Phase 1 — compute. No store writes, no cache consumption: a
        # processor failure leaves the world untouched and the tick retryable.
        # Sequential execution fails fast, so the public TickExecutionError
        # carries exactly one failure — same contract as the async stack, with
        # the single original chained directly as __cause__ (#444).
        computed: dict[ArchetypeSignature, DataFrame] = {}
        for sig in sigs:
            try:
                computed[sig] = self._compute_archetype(sig, run_config, **input_kwargs)
            except Exception as e:
                raise TickExecutionError(
                    phase="compute",
                    failures=[TickFailure(table_id=Archetype.get_name(sig), error=e)],
                ) from e

        # Phase 2 — commit. Each archetype's caches are consumed only after
        # its rows are appended; a store failure preserves exactly the
        # uncommitted mutations.
        results: dict[ArchetypeSignature, DataFrame] = {}
        for sig, df in computed.items():
            try:
                results[sig] = self.update(df, sig, run_config, self.tick)
            except Exception as e:
                raise TickExecutionError(
                    phase="commit",
                    failures=[TickFailure(table_id=Archetype.get_name(sig), error=e)],
                ) from e
            self.spawn_cache.pop(sig, None)
            self.despawn_cache.pop(sig, None)

        self.tick += 1

        self.hooks.fire(PostTick(world_id=self.world_id, tick=self.tick, results=results))

    def _compute_archetype(
        self, sig: ArchetypeSignature, run_config: RunConfig, **input_kwargs
    ) -> DataFrame:
        """Build one archetype's tick-N frame. Pure with respect to world
        state: reads the caches without consuming them and writes nothing.
        """
        df = self.query_archetype(
            sig=sig,
            run_config=run_config,
            ticks=[self.tick - 1],
            entity_ids=None,
            components=None,
        )

        # 2. Despawns apply to the existing population; spawns are held aside.
        df = self._apply_despawns(df, sig)
        spawns_df = self._spawn_frame(df, sig)

        # 3. Execute Processors for this archetype via system
        df = self.execute(df, sig, run_config, **input_kwargs)

        # Initial conditions are part of the ledger: a newborn's first row is
        # its raw spawn values at this tick; processors first apply on the
        # next tick (x_0 is given, x_{t+1} = f(x_t)).
        if spawns_df is not None:
            df = df.concat(spawns_df)

        return df

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

    def _apply_despawns(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame:
        """Flip is_active for pending despawns. Non-destructive apart from
        idempotent dedupe; caches are consumed in step's commit phase."""
        if self.despawn_cache.get(sig):
            # Grab despawn list of dicts and dedupe by most recent mutation command
            self.despawn_cache[sig] = list(dict.fromkeys(reversed(self.despawn_cache[sig])))
            entities_to_despawn = self.despawn_cache[sig]

            # Left Join is O(n+m), better than df['entity_id'].is_in(entities_to_despawn) -> O(n*m)
            mask_df = daft.from_pydict(
                {"entity_id": entities_to_despawn, "is_active": [False] * len(entities_to_despawn)}
            )

            df = (
                df.join(
                    mask_df, left_on="entity_id", right_on="entity_id", how="left", suffix="_right"
                )
                .with_column(
                    "is_active",
                    when(col("is_active_right").is_null(), then=col("is_active")).otherwise(
                        col("is_active_right")
                    ),
                )
                .select(*df.column_names)
            )
        return df

    def _spawn_frame(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame | None:
        """Pending spawns as a raw frame (initial conditions), cast to df's dtypes.

        Non-destructive: caches are consumed in step's commit phase.
        """
        if not self.spawn_cache.get(sig):
            return None
        # Dedupe duplicate spawns, prioritizing "most recent cmd" for easy user overwrite
        rows = list({row["entity_id"]: row for row in self.spawn_cache[sig]}.values())

        # Convert list of dicts to arrow table and eventually daft df
        pyarrow_schema = Archetype.get_archetype_schema(sig)
        arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
        spawns_df = daft.from_arrow(arrow_table)
        target_schema = df.schema()
        return spawns_df.with_columns(
            {
                "entity_id": col("entity_id").cast(target_schema["entity_id"].dtype),
                "tick": col("tick").cast(target_schema["tick"].dtype),
            }
        )

    def _materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature, run_config: RunConfig):
        """Apply pending despawns and concat pending spawns onto df.

        Diagnostic/compatibility surface (exercised directly by sync contract
        tests). The step path applies despawns before processors and concats
        spawns after them, so newborn rows persist their initial conditions
        (see _compute_archetype and step's commit phase).
        """
        df = self._apply_despawns(df, sig)
        spawns_df = self._spawn_frame(df, sig)
        return df.concat(spawns_df) if spawns_df is not None else df

    def _pop_staged_spawn(self, sig: ArchetypeSignature, entity_id: int) -> dict[str, Any] | None:
        """Remove and return the entity's staged spawn row under ``sig``.

        Multiple staged rows for one entity collapse last-write-wins,
        matching ``_spawn_frame``'s dedupe rule. Returns ``None`` when
        nothing is staged.
        """
        pending = self.spawn_cache.get(sig)
        if not pending:
            return None
        remaining = [row for row in pending if row["entity_id"] != entity_id]
        if len(remaining) == len(pending):
            return None
        staged = next(row for row in reversed(pending) if row["entity_id"] == entity_id)
        if remaining:
            self.spawn_cache[sig] = remaining
        else:
            del self.spawn_cache[sig]
        return staged

    def _move_entity(
        self,
        entity_id: int,
        old_sig: ArchetypeSignature,
        new_sig: ArchetypeSignature,
        mutated_components: list[Component],
    ) -> dict[str, Any]:
        """
        Returns a row dict that is valid for the NEW archetype, or an empty
        dict when the entity has no visible state to move (callers treat
        that as a logged no-op).

        The base row is the entity's latest visible state: a same-tick
        staged spawn row when one exists (spec C7 — later commands observe
        earlier staged mutations), otherwise the previous most-recent row in
        the OLD archetype. A staged row is consumed here so it cannot also
        materialize under the old signature after the move.
        """
        row_dict: dict[str, Any] | None = None

        staged = self._pop_staged_spawn(old_sig, entity_id)
        if staged is not None:
            row_dict = dict(staged)
        else:
            df = self.query_archetype(
                sig=old_sig,
                run_config=None,
                ticks=[self.tick - 1],
                entity_ids=[entity_id],
                components=None,
            )
            rows = df.to_pylist()
            if len(rows) == 1:
                row_dict = rows[0]
            elif len(rows) > 1:
                logger.warning(
                    f"World {self.name} ({self.world_id}): Entity Migration ambiguous: "
                    f"{len(rows)} rows for entity {entity_id} at tick {self.tick - 1}"
                )
                row_dict = rows[0]

        if row_dict is None:
            return {}  # entity vanished, caller decides

        # 3) overlay components that change with the new ones (skips for remove component with 0 member list)
        for c in mutated_components:
            row_dict.update(c.to_row_dict())

        # 4) stamp housekeeping columns for new location
        row_dict.update(
            {
                "entity_id": entity_id,
                "tick": self.tick,
                "world_id": str(self.world_id),
                "run_id": self.run_id or "",  # Placeholder; updater stamps correct run_id
                "is_active": True,
            }
        )
        return row_dict

    def _clear_caches(self):
        self.spawn_cache.clear()
        self.despawn_cache.clear()

    # ---------------------------------------------------------------------
    # World Mutation Commands
    # ---------------------------------------------------------------------

    def create_entity(self, components: list[Component]) -> int:
        """Spawn a new entity with an auto-assigned id. Fires ``OnSpawn``."""
        entity_id = self.next_entity_id
        self.next_entity_id += 1
        self._register_entity(entity_id, components)
        return entity_id

    def create_entities(self, entities: list[list[Component]]) -> list[int]:
        """Spawn multiple entities in one batch. Fires one ``OnSpawn`` per entity.

        Sync mirror of ``AsyncWorld.create_entities``. All entities share the
        same materialization tick and obey the initial-conditions contract:
        each entity's first row is its raw spawn values; processors first apply
        on the following tick.

        Args:
            entities: A list of component lists, one per entity to spawn.

        Returns:
            A list of entity IDs in the same order as ``entities``.
        """
        ids: list[int] = []
        for components in entities:
            entity_id = self.next_entity_id
            self.next_entity_id += 1
            ids.append(entity_id)
            self._register_entity(entity_id, components)
        return ids

    def reserve_entity_ids(self, n: int) -> list[int]:
        """Reserve *n* entity IDs without spawning.

        Sync mirror of ``AsyncWorld.reserve_entity_ids``.

        Args:
            n: Number of IDs to reserve (must be >= 1).

        Returns:
            A sorted list of *n* reserved entity IDs.

        Raises:
            ValueError: If *n* is less than 1.
        """
        if n < 1:
            raise ValueError(f"reserve_entity_ids requires n >= 1, got {n}")
        start = self.next_entity_id
        self.next_entity_id += n
        return list(range(start, start + n))

    def spawn_with_reserved_id(self, entity_id: int, components: list[Component]) -> None:
        """Materialise a previously reserved entity ID.

        Sync mirror of ``AsyncWorld.spawn_with_reserved_id``.

        Args:
            entity_id: A previously reserved ID (from ``reserve_entity_ids``).
            components: Initial component values.

        Raises:
            ValueError: If *entity_id* is already registered.
        """
        if entity_id in self.entity2sig:
            raise ValueError(
                f"Entity {entity_id} is already registered. "
                "Use update_entity to change component values on a live entity."
            )
        self._register_entity(entity_id, components)

    def _register_entity(self, entity_id: int, components: list[Component]) -> None:
        """Single source of truth for entity spawn. Every path that makes a
        new entity observable to the world MUST go through this method so
        ``OnSpawn`` is always fired exactly once with the correct payload."""
        sig = Archetype.sig_from_components(components)
        self.entity2sig[entity_id] = sig
        row_dict = Archetype.to_row_dict(
            entity_id, self.tick, components, self.world_id, self.run_id
        )
        self.spawn_cache.setdefault(sig, []).append(row_dict)
        self.hooks.fire(
            OnSpawn(world_id=self.world_id, entity_id=entity_id, components=list(components))
        )

    def remove_entity(self, entity_id: int):
        """Despawn an entity. Cancels a pending same-tick spawn if present,
        otherwise queues a despawn row for the current tick. Fires
        ``OnDespawn`` iff the entity existed."""
        sig = self.entity2sig.pop(entity_id, None)
        if sig is None:
            logger.warning(
                f"World {self.name} ({self.world_id}): Entity Removal Failed: No entity: {entity_id}"
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
                self.hooks.fire(OnDespawn(world_id=self.world_id, entity_id=entity_id))
                return

        self.despawn_cache.setdefault(sig, []).append(entity_id)
        self.hooks.fire(OnDespawn(world_id=self.world_id, entity_id=entity_id))

    def add_components(self, entity_id: int, components: list[Component]) -> None:
        """Attach additional components to an existing entity. Fires
        ``OnComponentAdded`` iff the signature actually changes."""
        old_sig = self.entity2sig.get(entity_id)
        if old_sig is None:
            logger.warning(f"add_components: entity {entity_id} not found")
            return

        new_sig = Archetype.add_components(old_sig, [type(c) for c in components])
        if new_sig == old_sig:
            logger.debug(f"add_components: components already in archetype for entity {entity_id}")
            return

        row = self._move_entity(entity_id, old_sig, new_sig, components)
        if not row:
            logger.warning("add_components: entity %s has no prior row; no-op", entity_id)
            return

        # 1) mark *old row* inactive
        self.despawn_cache.setdefault(old_sig, []).append(entity_id)

        # 2) row to *insert* under new signature
        self.spawn_cache.setdefault(new_sig, []).append(row)

        # 3) update bookkeeping – atomically
        self.entity2sig[entity_id] = new_sig

        self.hooks.fire(
            OnComponentAdded(
                world_id=self.world_id,
                entity_id=entity_id,
                components=list(components),
            )
        )

    def remove_components(self, entity_id: int, component_types: list[type[Component]]) -> None:
        """Detach components from an existing entity. Fires
        ``OnComponentRemoved`` iff the signature actually changes."""
        old_sig = self.entity2sig.get(entity_id)
        if old_sig is None:
            return

        new_sig = Archetype.remove_components(old_sig, component_types)
        if new_sig == old_sig:
            return

        row = self._move_entity(entity_id, old_sig, new_sig, [])  # remove ≡ keep remaining columns
        if not row:
            logger.warning("remove_components: entity %s has no prior row; no-op", entity_id)
            return

        self.despawn_cache.setdefault(old_sig, []).append(entity_id)
        self.spawn_cache.setdefault(new_sig, []).append(row)
        self.entity2sig[entity_id] = new_sig

        self.hooks.fire(
            OnComponentRemoved(
                world_id=self.world_id,
                entity_id=entity_id,
                component_types=list(component_types),
            )
        )

    def add_processor(self, processor: iProcessor):
        self.system.add_processor(processor)

    def remove_processor(self, processor: type[iProcessor]):
        self.system.remove_processor(processor)

    # ---------------------------------------------------------------------
    # Updater, Querier, System Facade methods
    # ---------------------------------------------------------------------

    def query_archetype(
        self,
        sig: ArchetypeSignature,
        run_config: RunConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type[Component]] | None = None,
    ) -> DataFrame:
        """Get an archetype by signature and tick. Esper equivalent of get_component for Archetype"""
        return self.querier.query_archetype(
            sig=sig,
            world_id=str(self.world_id),
            ticks=ticks or [self.tick],
            entity_ids=entity_ids,
            components=components,
            run_config=run_config,
            run_id=self.run_id,
        )

    def execute(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        run_config: RunConfig | None = None,
        **input_kwargs,
    ) -> DataFrame:
        """
        Execute system processors, passing resources for type-safe dependency injection.
        """
        return self.system.execute(df, sig, run_config, resources=self.resources, **input_kwargs)

    def update(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        run_config: RunConfig,
        tick: int | None = None,
    ) -> DataFrame:
        """Update the store with the given archetypes. Returns the stamped DataFrame."""
        return self.updater.update(df, sig, tick or self.tick, str(self.world_id), self.run_id)

    # -------------------------------------------------------------------------
    # Hooks: Typed lifecycle callbacks for observability
    # -------------------------------------------------------------------------

    def add_hook(
        self,
        event_type: type[_HookEventT],
        fn: SyncHookHandler[_HookEventT],
    ) -> HookHandle:
        """Register a handler for lifecycle events.

        Args:
            event_type: Event dataclass to listen for: PreTick, PostTick,
                OnSpawn, OnDespawn, OnComponentAdded, or OnComponentRemoved.
            fn: Plain callable that accepts the matching event instance.
                Handlers run inline on the firing thread.

        Returns:
            HookHandle to pass back to remove_hook when the handler should be
            unregistered.

        Example:
            from archetype.core.hooks import PostTick

            def log_tick(event: PostTick) -> None:
                print(f"tick {event.tick} done, {len(event.results)} archetypes")

            handle = world.add_hook(PostTick, log_tick)
            # ... later ...
            world.remove_hook(handle)
        """
        return self.hooks.add(event_type, fn)

    def remove_hook(self, handle: HookHandle) -> None:
        """Unregister a hook by handle.

        The operation is idempotent. Passing a handle that was already removed,
        or a handle minted by another world, is a no-op.
        """
        self.hooks.remove(handle)
