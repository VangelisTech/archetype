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
import inspect
import json
from logging import getLogger
from typing import Any, TypeVar, cast

import daft
import pyarrow as pa
from daft import DataFrame, col
from daft.functions import when
from uuid_utils import UUID, uuid7  # noqa: F401 imported for type hints

from archetype import _obs
from archetype.core.aio.async_querier import _canonicalize, _unknown_signature_error
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig
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
    CommitContext,
    iAsyncHookBus,
    iAsyncProcessor,
    iAsyncQueryManager,
    iAsyncSystem,
    iAsyncUpdateManager,
    iAsyncWorld,
    iCommitCoordinator,
    iResourceContainer,
)

logger = getLogger(__name__)

_HookEventT = TypeVar("_HookEventT", bound=HookEvent)


class AsyncWorld(iAsyncWorld):
    def __init__(
        self,
        *,
        world_id: str,
        name: str | None,
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
        lineage: list[tuple[str, str, int]] | None = None,
        commit_coordinator: iCommitCoordinator | None = None,
    ):
        """
        Initialize the fully parallel async world.
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
        # Ancestor read segments (world_id, run_id, up_to_tick), ascending.
        # Rows for ticks <= up_to_tick live under that ancestor's run; the
        # store is append-only, so those rows are immutable history.
        self.lineage = list(lineage) if lineage else []

        # Commit identity (issue #273). Coordinated worlds mint a commit
        # context per tick attempt and publish the tick's manifest LAST;
        # readers admit only manifest-matched rows. Uncoordinated worlds
        # (coordinator=None) keep implicit epoch-0 semantics: rows stamp
        # token ""/epoch 0 and nothing is filtered.
        self.commit_coordinator = commit_coordinator
        self._commit_ctx: CommitContext | None = None
        self._updater_takes_commit: bool | None = None
        self._querier_caps: dict[str, dict[str, bool]] | None = None
        self._sig_intern: dict[str, ArchetypeSignature] | None = None

    @property
    def _entity2sig(self):
        """Compatibility alias for pre-refactor evals and diagnostics."""
        return self.entity2sig

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

            # Phase 1 — compute. No store writes, no cache consumption: a
            # processor failure here leaves the world exactly as it was, so
            # the failed tick is retryable and nothing half-commits.
            futures = [self._compute_archetype(sig, run_config, **input_kwargs) for sig in sigs]
            results = await asyncio.gather(*futures, return_exceptions=True)
            # Order corresponds to `sigs` (per asyncio.gather docs). Collect ordinary
            # Exceptions to surface together; propagate non-Exception BaseExceptions
            # (e.g. CancelledError) directly so task cancellation is never masked.
            errors: dict[ArchetypeSignature, Exception] = {}
            for sig, r in zip(sigs, results, strict=False):
                if isinstance(r, Exception):
                    errors[sig] = r
                elif isinstance(r, BaseException):
                    raise r

            if errors:
                raise RuntimeError(
                    "; ".join(f"{Archetype.get_name(sig)}: {e}" for sig, e in errors.items())
                )

            # Phase 2 — commit every computed frame concurrently.
            #
            # Coordinated worlds (issue #273) mint one commit context for the
            # whole tick; every append this tick stamps it. The manifest is
            # published LAST, after a forced flush — a crash or stale-writer
            # failure anywhere before publish leaves this tick's rows
            # unmanifested and therefore invisible to every reader.
            if self.commit_coordinator is not None:
                self._commit_ctx = await self.commit_coordinator.begin_tick(
                    str(self.world_id), str(self.run_id), self.tick
                )
            frames = cast("list[DataFrame]", results)
            commits = [
                self._commit_archetype(sig, df, run_config)
                for sig, df in zip(sigs, frames, strict=False)
            ]
            committed = await asyncio.gather(*commits, return_exceptions=True)
            errors = {}
            for sig, r in zip(sigs, committed, strict=False):
                if isinstance(r, Exception):
                    errors[sig] = r
                elif isinstance(r, BaseException):
                    # Never mask cancellation behind the aggregate RuntimeError.
                    raise r
            if errors:
                raise RuntimeError(
                    "; ".join(f"{Archetype.get_name(sig)}: {e}" for sig, e in errors.items())
                )

            # Every remaining commit result is a DataFrame, keeping
            # PostTick.results exception-free per its contract (spec §320).
            result_frames: dict[ArchetypeSignature, DataFrame] = {
                sig: r
                for sig, r in zip(sigs, committed, strict=False)
                if not isinstance(r, BaseException)
            }

            if self.commit_coordinator is not None and self._commit_ctx is not None:
                # Visibility must never outrun durability: staged (cached)
                # rows flush before the head claims them.
                store = getattr(self.updater, "store", None)
                if store is not None:
                    await store.flush()
                await self.commit_coordinator.publish_tick(
                    str(self.world_id),
                    str(self.run_id),
                    self.tick,
                    self._commit_ctx,
                    sigs,
                )
                # Mutations are consumed only now, after the manifest exists.
                # A crash or stale-writer failure at ANY earlier point leaves
                # the caches intact, so the retried tick recomputes and
                # re-appends under a fresh token — the failed attempt's rows
                # stay unmanifested and invisible; exactly one attempt per
                # tick ever becomes visible.
                for sig in sigs:
                    self.spawn_cache.pop(sig, None)
                    self.despawn_cache.pop(sig, None)
            self._commit_ctx = None

            self.tick += 1

            # Fire post-tick hooks
            await self.hooks.fire(
                PostTick(world_id=self.world_id, tick=self.tick, results=result_frames)
            )
        finally:
            for handle in debug_handles:
                self.remove_hook(handle)

    async def _compute_archetype(
        self, sig: ArchetypeSignature, run_config: RunConfig, **input_kwargs
    ) -> DataFrame:
        """Build one archetype's tick-N frame. Pure with respect to world
        state: reads the caches without consuming them and writes nothing."""
        sig_name = Archetype.get_name(sig)

        with _obs.span("world.query", sig=sig_name, tick=self.tick):
            df = await self.query_archetype(
                sig=sig,
                run_id=self.run_id,
                ticks=[self.tick - 1],
                entity_ids=None,
                components=None,
            )

        with _obs.span("world.materialize", sig=sig_name, tick=self.tick):
            df = self._apply_despawns(df, sig)
            spawns_df = self._spawn_frame(sig)

        with _obs.span("world.execute", sig=sig_name, tick=self.tick):
            df = await self.execute(df, sig, tick=self.tick, debug=run_config.debug, **input_kwargs)

        # Initial conditions are part of the ledger: a newborn's first row is
        # its raw spawn values at this tick; processors first apply on the
        # next tick (x_0 is given, x_{t+1} = f(x_t)).
        if spawns_df is not None:
            df = df.concat(spawns_df)

        return df

    async def _commit_archetype(
        self, sig: ArchetypeSignature, df: DataFrame, run_config: RunConfig
    ) -> DataFrame:
        """Append one archetype's computed frame; consume caches (uncoordinated).

        The updater raises on failed persistence, in which case the caches
        survive for retry. Coordinated worlds defer cache consumption to
        after manifest publish (see step): durability alone is not enough
        once visibility requires the published head."""
        with _obs.span("world.update", sig=Archetype.get_name(sig), tick=self.tick):
            df_mat = await self.update(df, sig, run_config)

        if self.commit_coordinator is None:
            self.spawn_cache.pop(sig, None)
            self.despawn_cache.pop(sig, None)
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

    def _apply_despawns(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame:
        """Flip is_active for pending despawns. Non-destructive: the cache is
        consumed by _commit_archetype after a successful append."""
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
        return df

    def _spawn_frame(self, sig: ArchetypeSignature) -> DataFrame | None:
        """Pending spawns as a raw frame (initial conditions). Non-destructive:
        the cache is consumed by _commit_archetype after a successful append."""
        if not self.spawn_cache.get(sig):
            return None
        # Dedupe duplicate spawns, prioritizing "most recent cmd" (last write wins)
        # Dict keeps last value per key, so iterate forward to keep latest
        rows = list({row["entity_id"]: row for row in self.spawn_cache[sig]}.values())

        # Convert list of dicts to arrow table and eventually daft df
        pyarrow_schema = Archetype.get_archetype_schema(sig)
        arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
        return daft.from_arrow(arrow_table)

    def materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame:
        """Apply pending despawns and concat pending spawns onto df, consuming
        both caches.

        Diagnostic/compatibility surface. The step path does NOT use this
        combined form: it applies despawns before processors, concats spawns
        after them (initial conditions), and consumes the caches only after
        the append commits (see _compute_archetype/_commit_archetype).
        """
        df = self._apply_despawns(df, sig)
        spawns_df = self._spawn_frame(sig)
        self.spawn_cache.pop(sig, None)
        self.despawn_cache.pop(sig, None)
        return df.concat(spawns_df) if spawns_df is not None else df

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

    async def create_entities(self, entities: list[list[Component]]) -> list[int]:
        """Spawn multiple entities in one batch. Fires one ``OnSpawn`` per entity.

        All spawn rows are appended in a single cache operation per archetype
        signature, preserving the initial-conditions contract: every entity's
        first row is its raw spawn values at the materialization tick;
        processors first apply on the following tick (x_0 is given,
        x_{t+1} = f(x_t)).

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
            await self._register_entity(entity_id, components)
        return ids

    def reserve_entity_ids(self, n: int) -> list[int]:
        """Reserve *n* entity IDs without spawning.

        The returned IDs are guaranteed never to collide with auto-assigned
        ones: they are drawn from the same monotonic counter as
        ``create_entity`` / ``create_entities``, so interleaved calls produce
        disjoint ranges.

        Reserved IDs must be materialised via ``spawn_with_reserved_id``
        before the next ``step()`` that processes their archetype, otherwise
        they remain invisible (no spawn_cache row, no entity2sig entry).

        Use this when external systems need to reference an entity by ID
        before the spawn lands on the ledger (e.g. to break a circular
        dependency between two entities that reference each other).

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

    async def spawn_with_reserved_id(self, entity_id: int, components: list[Component]) -> None:
        """Materialise a previously reserved entity ID.

        Registers the entity in ``entity2sig`` and ``spawn_cache`` exactly as
        ``create_entity`` would, then fires ``OnSpawn``. The entity will appear
        as a raw spawn row at the next materialization tick.

        Cancellation contract: if you want to cancel a reserved spawn before
        it materialises, call ``remove_entity(entity_id)`` — it will find the
        pending spawn_cache row and remove it cleanly, matching the semantics
        of cancelling a same-tick spawn from ``create_entity``.

        Args:
            entity_id: A previously reserved ID (from ``reserve_entity_ids``).
            components: Initial component values.

        Raises:
            ValueError: If *entity_id* is already registered (double-spawn
                guard — prevents silent overwrites of live entities).
        """
        if entity_id in self.entity2sig:
            raise ValueError(
                f"Entity {entity_id} is already registered. "
                "Use update_entity to change component values on a live entity."
            )
        await self._register_entity(entity_id, components)

    def _intern_sig(self, sig: ArchetypeSignature) -> ArchetypeSignature:
        """One canonical signature tuple per table id.

        Schema-identical twin classes exist legitimately: a resumed world's
        fingerprint-resolved signature and the caller's own classes hash to
        the same table. They must share ONE key in entity2sig and the
        mutation caches — as distinct dict keys, the step loop would process
        their shared table once per alias and double-append every row.
        """
        if self._sig_intern is None:
            self._sig_intern = {}
            for existing in (
                *self.entity2sig.values(),
                *self.spawn_cache,
                *self.despawn_cache,
            ):
                self._sig_intern.setdefault(Archetype.get_name(existing), existing)
        return self._sig_intern.setdefault(Archetype.get_name(sig), sig)

    async def _register_entity(self, entity_id: int, components: list[Component]) -> None:
        """Single source of truth for entity spawn. Every path that makes a
        new entity observable to the world MUST go through this method so
        ``OnSpawn`` is always fired exactly once with the correct payload."""
        sig = self._intern_sig(Archetype.sig_from_components(components))
        self.entity2sig[entity_id] = sig
        row_dict = Archetype.to_row_dict(
            entity_id, self.tick, components, self.world_id, run_id=self.run_id
        )
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
        if old_sig is None:
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
        new_sig = self._intern_sig(new_sig)
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
        new_sig = self._intern_sig(new_sig)
        self.spawn_cache.setdefault(new_sig, []).append(row)
        self.entity2sig[entity_id] = new_sig

        await self.hooks.fire(
            OnComponentRemoved(
                world_id=self.world_id,
                entity_id=entity_id,
                component_types=list(component_types),
            )
        )

    async def add_processor(self, processor: iAsyncProcessor):
        await self.system.add_processor(processor)

    async def remove_processor(self, processor: type[iAsyncProcessor]):
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
        components: list[type[Component]] | None = None,
        run_id: str | None = None,
        run_config: RunConfig | None = None,
    ) -> DataFrame:
        """Facade Method to query an archetype table by signature.

        The signature is canonicalized (sorted by class name) before lookup,
        so callers may supply types in any order.  If the canonical signature
        has never been active in this world (not in the live registry AND not
        accepted by the store) an ``UnknownSignatureError`` is raised —
        a distinct failure from 'the signature exists but has zero rows at
        this tick' which returns an empty DataFrame.

        Defaults to the world's most recent run_id when not provided. Accepts an optional
        run_config for instrumentation; ignored by the base querier.
        """
        # Canonicalize the input sig so unsorted tuples resolve correctly.
        sig = _canonicalize(sig)

        # Back-compat: tests sometimes pass RunConfig as the 2nd positional arg
        if run_config_or_ticks is not None and not isinstance(run_config_or_ticks, list):
            run_config = run_config_or_ticks  # type: ignore[assignment]
        elif isinstance(run_config_or_ticks, list) and ticks is None:
            ticks = run_config_or_ticks

        if run_id:
            effective_run_id = run_id
        elif self.run_id:
            effective_run_id = str(self.run_id)
        elif run_config is not None:
            effective_run_id = str(run_config.run_id)
        else:
            effective_run_id = ""

        # Exact-signature existence is the union of accepted writes and live state.
        signature_source = getattr(self.querier, "list_committed_signatures", None)
        if signature_source is None:
            signature_source = getattr(self.querier, "list_signatures", None)
        if signature_source is not None:
            committed_sigs = {_canonicalize(s) for s in await signature_source()}
            live_sigs = {_canonicalize(s) for s in self.active_signatures}
            all_known = live_sigs | committed_sigs
            known_ids = {Archetype.get_name(s) for s in all_known}
            if all_known and Archetype.get_name(sig) not in known_ids:
                raise _unknown_signature_error(sig)

        async def _query_one(target_world: str, target_run: str, target_ticks: list[int]):
            # Querier capability is resolved by signature inspection, NOT by
            # TypeError probing: a retry-on-TypeError fallback that omits
            # commit_tokens would silently fail OPEN — surfacing rows no
            # manifest authorized — the exact failure the atomic-visibility
            # amendment forbids (same reasoning as update()'s commit param).
            tokens = await self._visible_tokens(target_world, target_run, target_ticks)
            caps = self._querier_caps_for("query_archetype")
            kwargs: dict[str, Any] = {}
            if caps["_world_validated"]:
                # Bypass the querier's own existence check — the world-level
                # guard above already verified the sig against live + store
                # sigs, including uncommitted spawn-cache sigs.
                kwargs["_world_validated"] = True
            if caps["run_config"] and run_config is not None:
                kwargs["run_config"] = run_config
            if tokens is not None:
                if not caps["commit_tokens"]:
                    raise RuntimeError(
                        f"querier {type(self.querier).__name__} does not accept "
                        "commit_tokens; refusing an unfiltered read of a "
                        "coordinated world (fail closed)"
                    )
                kwargs["commit_tokens"] = tokens
            return await self.querier.query_archetype(
                sig=sig,
                world_id=target_world,
                run_id=target_run,
                ticks=target_ticks,
                entity_ids=entity_ids,
                components=components,
                **kwargs,
            )

        requested_ticks = ticks or [self.tick]
        if not self.lineage:
            return await _query_one(self.world_id, effective_run_id, requested_ticks)

        # Group ticks by the lineage segment that owns them, preserving
        # segment order so pre-fork history reads from the ancestor's run.
        groups: dict[tuple[str, str], list[int]] = {}
        for t in requested_ticks:
            groups.setdefault(self._tick_source(t, effective_run_id), []).append(t)

        frames = [
            await _query_one(target_world, target_run, group_ticks)
            for (target_world, target_run), group_ticks in groups.items()
        ]
        result = frames[0]
        for frame in frames[1:]:
            result = result.concat(frame)
        return result

    def _tick_source(self, tick: int, own_run_id: str | None = None) -> tuple[str, str]:
        """Resolve which (world_id, run_id) owns the rows for a tick.

        Forks own ticks after their fork point; earlier ticks live under the
        ancestor that wrote them. Lineage segments ascend by up_to_tick.
        """
        for ancestor_world, ancestor_run, up_to_tick in self.lineage:
            if tick <= up_to_tick:
                return str(ancestor_world), str(ancestor_run)
        return str(self.world_id), str(own_run_id or self.run_id or "")

    def _querier_caps_for(self, method: str) -> dict[str, bool]:
        """Which optional kwargs a querier method accepts (memoized per method).

        A parameter is accepted if it is named in the signature or the
        signature takes **kwargs (the core querier absorbs world-internal
        keys that way). Per the iAsyncQueryManager contract, a querier that
        accepts commit_tokens MUST filter by it — coordinated worlds refuse
        queriers that cannot accept it at all (fail closed).
        """
        if self._querier_caps is None:
            self._querier_caps = {}
        caps = self._querier_caps.get(method)
        if caps is None:
            params = inspect.signature(getattr(self.querier, method)).parameters
            var_kw = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())
            caps = {
                name: var_kw or name in params
                for name in ("commit_tokens", "run_config", "_world_validated")
            }
            self._querier_caps[method] = caps
        return caps

    async def _visible_tokens(
        self, world_id: str, run_id: str, ticks: list[int] | None
    ) -> list[str] | None:
        """Reader-side commit-token allowlist for one (world, run) segment.

        None means unfiltered: either this world is uncoordinated, or the
        target segment has no manifests at all (pre-#273 legacy history).
        An empty list means manifests exist but none cover the requested
        ticks — nothing current-generation is visible there.
        """
        if self.commit_coordinator is None:
            return None
        visible = await self.commit_coordinator.visible_tokens(str(world_id), str(run_id), ticks)
        if visible is None:
            return None
        if ticks is None:
            return sorted({token for tokens in visible.values() for token in tokens})
        return sorted({token for t in ticks if t in visible for token in visible[t]})

    async def get_components(
        self,
        components: list[type[Component]],
        entity_ids: list[int] | None = None,
    ) -> DataFrame:
        """Query all active entities that contain the requested component types.

        Passthrough to the querier's query_components method.
        """
        read_tick = max(self.tick - 1, 0)
        source_world, source_run = self._tick_source(read_tick)
        tokens = await self._visible_tokens(source_world, source_run, [read_tick])
        kwargs: dict[str, Any] = {}
        if tokens is not None:
            # Same fail-closed guard as query_archetype: a querier that
            # cannot accept the allowlist must not serve a coordinated read.
            if not self._querier_caps_for("query_components")["commit_tokens"]:
                raise RuntimeError(
                    f"querier {type(self.querier).__name__} does not accept "
                    "commit_tokens; refusing an unfiltered read of a "
                    "coordinated world (fail closed)"
                )
            kwargs["commit_tokens"] = tokens
        return await self.querier.query_components(
            components=components,
            world_id=source_world,
            run_id=source_run,
            ticks=[read_tick],
            entity_ids=entity_ids,
            **kwargs,
        )

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
        """Update the store with the given archetypes.

        When coordinated persistence is active, the update carries the tick's
        commit identity. Capability is determined before writing so an error
        cannot cause the same rows to be appended twice.
        """
        if self._updater_takes_commit is None:
            params = inspect.signature(self.updater.update).parameters
            self._updater_takes_commit = "commit" in params
        if self._updater_takes_commit:
            return await self.updater.update(
                df,
                sig,
                tick or self.tick,
                self.world_id,
                self.run_id,
                commit=self._commit_ctx,
            )
        return await self.updater.update(df, sig, tick or self.tick, self.world_id, self.run_id)

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
