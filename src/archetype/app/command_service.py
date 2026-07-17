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

"""
Command Service — the gate.

Every external operation flows through here. The gate runs:
  1. guardrail_allow(cmd, ctx) — RBAC + quotas
  2. delegate to the underlying service
  3. emit an AuditRow via iAuditLog.record

Two paths: direct (sync semantics) and tick-deferred (queued via broker).
"""

from __future__ import annotations

import json
import logging
import math
from typing import TYPE_CHECKING, Any

from uuid_utils import UUID

from archetype._obs import instrument
from archetype.app.auth.guard import guardrail_allow
from archetype.app.models import (
    Command,
    CommandType,
    HookInfo,
    ProcessorInfo,
    ResourceInfo,
    WorldInfo,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from daft import DataFrame

    from archetype.app.audit_log import AuditLog
    from archetype.app.auth.models import ActorCtx
    from archetype.app.autoresearch_service import (
        AutoResearchConfig,
        AutoResearchResult,
        AutoResearchService,
        CandidatePreparer,
        Evaluator,
        IterationResult,
    )
    from archetype.app.broker import CommandBroker
    from archetype.app.eval_service import EvalService
    from archetype.app.fact_service import FactService
    from archetype.app.facts import FactProcessor, FactReceipt, FactWriteReceipt
    from archetype.app.ingestion_service import IngestionService
    from archetype.app.models import (
        EpisodeConfig,
        EpisodeResult,
        RolloutConfig,
        RolloutResult,
        RunResult,
        WorldInfo,
    )
    from archetype.app.mutation_service import MutationService
    from archetype.app.query_service import QueryService
    from archetype.app.simulation_service import SimulationService
    from archetype.app.world_service import WorldService
    from archetype.core.component import Component
    from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
    from archetype.core.interfaces import ArchetypeSignature
    from archetype.experiments.receipts import GraderContract

logger = logging.getLogger(__name__)


def _parse_entity_id(value: object) -> int:
    """Decode an entity ID without lossy numeric coercion."""
    if type(value) is int:
        return value

    if isinstance(value, str):
        digits = value[1:] if value[:1] in {"+", "-"} else value
        if digits and digits.isascii() and digits.isdecimal():
            return int(value)

    raise TypeError("entity_id must be an integer or decimal-integer string")


class CommandService:
    """Policy enforcement point.

    Every external mutation, lifecycle operation, and read flows through
    here. The only service that sees ActorCtx.
    """

    def __init__(
        self,
        mutations: MutationService,
        worlds: WorldService,
        simulation: SimulationService,
        queries: QueryService,
        broker: CommandBroker,
        audit: AuditLog | None = None,
        autoresearch: AutoResearchService | None = None,
        facts: FactService | None = None,
        ingestion: IngestionService | None = None,
        evals: EvalService | None = None,
    ) -> None:
        self._mutations = mutations
        self._worlds = worlds
        self._simulation = simulation
        self._queries = queries
        self._broker = broker
        self._audit = audit
        self._autoresearch = autoresearch
        self._facts = facts
        self._ingestion = ingestion
        self._evals = evals

    def _gate(self, cmd: Command, ctx: ActorCtx) -> None:
        """RBAC + quota check. Raises GuardrailError if denied."""
        guardrail_allow(cmd, ctx)

    def _require_world(self, world_id: str | UUID) -> None:
        """Reject submissions to worlds not in the registry.

        Per ``docs/guide/specification.md`` "Required Hardening Work" item 3
        and the "CommandService" CURRENT GAPS list, ``submit*`` MUST reject
        commands targeted at unknown ``world_id`` before quota debit, broker
        enqueue, or audit emit.
        """
        from archetype.app.errors import WorldNotFoundError

        if not self._worlds.has_world(world_id):
            raise WorldNotFoundError(world_id)

    async def _emit(self, ctx: ActorCtx, command_type: str, world_id=None, **kw) -> None:
        """Emit one audit row. Best-effort — never raises."""
        if self._audit is None:
            return
        try:
            from archetype.app.audit_log import make_audit_row

            row = make_audit_row(ctx, command_type, world_id, **kw)
            await self._audit.record(row)
        except Exception:
            logger.warning("audit emission failed", exc_info=True)

    # ── Mutations (gated, direct) ─────────────────────────────────────────

    @instrument("gate.create_entity")
    async def create_entity(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        components: list[Component],
    ) -> int:
        self._gate(Command(type=CommandType.SPAWN), ctx)
        result = await self._mutations.create_entity(world_id, components)
        await self._emit(ctx, "spawn", world_id)
        return result

    @instrument("gate.create_entities")
    async def create_entities(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entities: list[list[Component]],
    ) -> list[int]:
        """Batch-spawn entities. Applies one RBAC gate check for the batch."""
        self._gate(Command(type=CommandType.SPAWN), ctx)
        result = await self._mutations.create_entities(world_id, entities)
        await self._emit(ctx, "spawn_batch", world_id, count=len(entities))
        return result

    @instrument("gate.reserve_entity_ids")
    def reserve_entity_ids(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        n: int,
    ) -> list[int]:
        """Reserve *n* entity IDs without spawning. Synchronous — no round-trip."""
        self._gate(Command(type=CommandType.SPAWN), ctx)
        return self._mutations.reserve_entity_ids(world_id, n)

    @instrument("gate.spawn_with_reserved_id")
    async def spawn_with_reserved_id(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        components: list[Component],
    ) -> None:
        """Materialise a previously reserved entity ID."""
        self._gate(Command(type=CommandType.SPAWN), ctx)
        await self._mutations.spawn_with_reserved_id(world_id, entity_id, components)
        await self._emit(ctx, "spawn_reserved", world_id, entity_id=entity_id)

    @instrument("gate.remove_entity")
    async def remove_entity(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
    ) -> None:
        self._gate(Command(type=CommandType.DESPAWN), ctx)
        await self._mutations.remove_entity(world_id, entity_id)
        await self._emit(ctx, "despawn", world_id)

    @instrument("gate.update_entity")
    async def update_entity(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        components: list[Component],
    ) -> None:
        """Overlay values on existing components (same archetype)."""
        self._gate(Command(type=CommandType.UPDATE), ctx)
        await self._mutations.update_entity(world_id, entity_id, components)
        await self._emit(ctx, "update", world_id)

    @instrument("gate.add_components")
    async def add_components(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        components: list[Component],
    ) -> None:
        self._gate(Command(type=CommandType.ADD_COMPONENT), ctx)
        await self._mutations.add_components(world_id, entity_id, components)
        await self._emit(ctx, "add_component", world_id)

    @instrument("gate.remove_components")
    async def remove_components(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        entity_id: int,
        component_types: list[type[Component]],
    ) -> None:
        self._gate(Command(type=CommandType.REMOVE_COMPONENT), ctx)
        await self._mutations.remove_components(world_id, entity_id, component_types)
        await self._emit(ctx, "remove_component", world_id)

    @instrument("gate.add_processor")
    async def add_processor(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        processor,
    ) -> None:
        self._gate(Command(type=CommandType.ADD_PROCESSOR), ctx)
        await self._mutations.add_processor(world_id, processor)
        await self._emit(ctx, "add_processor", world_id)

    @instrument("gate.remove_processor")
    async def remove_processor(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        proc_type,
    ) -> None:
        self._gate(Command(type=CommandType.REMOVE_PROCESSOR), ctx)
        await self._mutations.remove_processor(world_id, proc_type)
        await self._emit(ctx, "remove_processor", world_id)

    # ── Lifecycle (gated, direct) ─────────────────────────────────────────

    @instrument("gate.create_world")
    async def create_world(
        self,
        ctx: ActorCtx,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> WorldInfo:
        self._gate(Command(type=CommandType.CREATE_WORLD), ctx)
        world = await self._worlds.create_world(config, storage_config, cache_config)
        info = WorldInfo(
            world_id=world.world_id,
            name=world.name,
            tick=getattr(world, "tick", 0),
            run_id=getattr(world, "run_id", None),
        )
        await self._emit(ctx, "create_world", info.world_id)
        return info

    @instrument("gate.fork_world")
    async def fork_world(
        self,
        ctx: ActorCtx,
        source_world_id: str | UUID,
        name: str | None = None,
        *,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
    ) -> WorldInfo:
        self._gate(Command(type=CommandType.FORK_WORLD), ctx)
        world = await self._worlds.fork_world(source_world_id, name, storage_config, cache_config)
        info = WorldInfo(
            world_id=world.world_id,
            name=world.name,
            tick=getattr(world, "tick", 0),
            run_id=getattr(world, "run_id", None),
        )
        await self._emit(ctx, "fork_world", info.world_id)
        return info

    @instrument("gate.destroy_world")
    async def destroy_world(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> None:
        self._gate(Command(type=CommandType.DESTROY_WORLD), ctx)
        if self._audit:
            await self._audit.flush()
        await self._broker.clear(world_id)
        await self._worlds.destroy_world(world_id)
        await self._emit(ctx, "destroy_world", world_id)

    @instrument("gate.get_world_info")
    async def get_world_info(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> WorldInfo:
        self._gate(Command(type=CommandType.GET_WORLD_INFO), ctx)
        world = self._worlds.get_world(UUID(str(world_id)))
        info = WorldInfo(
            world_id=world.world_id,
            name=world.name,
            tick=getattr(world, "tick", 0),
            run_id=getattr(world, "run_id", None),
        )
        await self._emit(ctx, "get_world_info", world_id)
        return info

    async def list_worlds(self, ctx: ActorCtx) -> list[WorldInfo]:
        self._gate(Command(type=CommandType.LIST_WORLDS), ctx)
        worlds = [
            WorldInfo(
                world_id=world.world_id,
                name=world.name,
                tick=getattr(world, "tick", 0),
                run_id=getattr(world, "run_id", None),
            )
            for world in self._worlds.list_worlds()
        ]
        await self._emit(ctx, "list_worlds")
        return worlds

    @instrument("gate.discover_worlds")
    async def discover_worlds(
        self, ctx: ActorCtx, storage_config: StorageConfig
    ) -> list[WorldInfo]:
        """Return worlds recorded in a storage catalog.

        Unlike `list_worlds()`, this works in a fresh process and includes
        worlds that are not currently live.
        """
        self._gate(Command(type=CommandType.LIST_WORLDS), ctx)
        infos = await self._worlds.discover_worlds(storage_config)
        await self._emit(ctx, "discover_worlds")
        return infos

    @instrument("gate.open_world_readonly")
    async def open_world_readonly(
        self, ctx: ActorCtx, storage_config: StorageConfig, world_id: str | UUID
    ) -> WorldInfo:
        """Return the durable descriptor for a world without activating it.

        The returned descriptor can be used with read paths but cannot mutate
        the world.
        """
        self._gate(Command(type=CommandType.GET_WORLD_INFO), ctx)
        info = await self._worlds.open_world_readonly(storage_config, world_id)
        await self._emit(ctx, "open_world_readonly", world_id)
        return info

    @instrument("gate.ingest_fact")
    async def ingest_fact(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        components: list,
        *,
        external_id: str,
        producer: str = "default",
        storage_config: StorageConfig | None = None,
    ) -> FactReceipt:
        """Persist an external fact exactly once per external identity.

        This operation completes directly rather than entering the deferred
        command queue so the caller always receives its durable receipt.
        """
        if self._ingestion is None:
            raise RuntimeError("ingestion service is not wired")
        self._gate(Command(type=CommandType.INGEST_FACT), ctx)
        receipt = await self._ingestion.ingest_fact(
            str(world_id),
            components,
            external_id=external_id,
            producer=producer,
            storage_config=storage_config,
        )
        await self._emit(ctx, "ingest_fact", world_id)
        return receipt

    @instrument("gate.ingest_files")
    async def ingest_files(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        paths: str | Path | list[str | Path],
        processor: FactProcessor,
        *,
        storage_config: StorageConfig | None = None,
    ) -> FactWriteReceipt:
        """Run a ``daft.File`` processor into its typed Iceberg fact table."""
        if self._facts is None:
            raise RuntimeError("fact service is not wired")
        self._gate(Command(type=CommandType.INGEST_FACT), ctx)
        receipt = await self._facts.ingest_files(
            str(world_id),
            paths,
            processor,
            storage_config=storage_config,
        )
        await self._emit(ctx, "ingest_files", world_id)
        return receipt

    @instrument("gate.write_facts")
    async def write_facts(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        table_name: str,
        facts: DataFrame,
        *,
        storage_config: StorageConfig | None = None,
    ) -> FactWriteReceipt:
        """Write an existing Daft pipeline to a typed Iceberg fact table."""
        if self._facts is None:
            raise RuntimeError("fact service is not wired")
        self._gate(Command(type=CommandType.INGEST_FACT), ctx)
        receipt = await self._facts.write_facts(
            str(world_id),
            table_name,
            facts,
            storage_config=storage_config,
        )
        await self._emit(ctx, "write_facts", world_id)
        return receipt

    @instrument("gate.query_facts")
    async def query_facts(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        table_name: str,
        *,
        storage_config: StorageConfig | None = None,
    ) -> DataFrame:
        """Read a typed fact table through the standard viewer gate."""
        if self._facts is None:
            raise RuntimeError("fact service is not wired")
        self._gate(Command(type=CommandType.QUERY_WORLD), ctx)
        facts = await self._facts.read_facts(
            str(world_id),
            table_name,
            storage_config=storage_config,
        )
        await self._emit(ctx, "query_facts", world_id)
        return facts

    @instrument("gate.evaluate")
    async def evaluate(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        components: list[type[Component]],
        *,
        contract: GraderContract,
        grader,
        evaluation_id: str,
        producer: str = "evals",
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> FactReceipt:
        """Persist one evaluation receipt for an evaluation identity.

        The identity is claimed before grading. A completed matching claim
        returns its receipt without running the grader again. The receipt is
        pinned to a snapshot and requires a versioned `GraderContract`.
        """
        import time as _time

        from pydantic_core import to_jsonable_python as _to_jsonable

        from archetype.experiments.receipts import (
            EvalReceipt,
            GraderContract,
            Outcome,
            evaluation_identity_digest,
            subject_digest,
        )

        self._gate(Command(type=CommandType.EVALUATE), ctx)
        if self._ingestion is None or self._evals is None:
            raise RuntimeError("evaluation requires the ingestion and eval services wired")
        ingestion, evals = self._ingestion, self._evals
        if not isinstance(contract, GraderContract):
            raise ValueError(
                "persisted receipts require a GraderContract descriptor — bare "
                "callables get no digest (use world.grade for ephemeral scoring)"
            )

        wid = str(world_id)
        snapshot = await ingestion.snapshot_ref(wid, storage_config)
        subject = subject_digest(
            wid,
            snapshot.run_id,
            snapshot_tick=snapshot.tick,
            snapshot_tokens=list(snapshot.head_tokens),
            component_names=[c.__name__ for c in components],
            ticks=ticks,
            entity_ids=entity_ids,
        )
        contract_digest = contract.digest()
        identity = evaluation_identity_digest(subject, contract_digest)
        snapshot_ticks = (
            [tick for tick in ticks if tick <= snapshot.tick]
            if ticks is not None
            else list(range(snapshot.tick + 1))
        )

        async def _grade_and_build(_claim) -> list:
            df = await self._queries.query_components(
                components,
                wid,
                snapshot.run_id,
                snapshot.storage_config,
                ticks=snapshot_ticks,
                entity_ids=entity_ids,
                visibility_tokens=list(snapshot.visibility_tokens),
            )
            outputs = await evals.run_graders(df, [grader])
            typed = [o for o in outputs if isinstance(o, Outcome)]
            if len(typed) != len(outputs):
                raise ValueError(
                    "persisted receipts require typed Outcome results "
                    "(pass/fail/invalid/inconclusive + optional finite score)"
                )
            if len(typed) != 1:
                raise ValueError(
                    "a persisted evaluation is one trial: the grader must return "
                    "exactly one Outcome (run more trials under new evaluation_ids)"
                )
            outcome = typed[0]
            return [
                EvalReceipt(
                    evaluation_id=evaluation_id,
                    subject_digest=subject,
                    contract_digest=contract_digest,
                    grader_id=contract.grader_id,
                    outcome=outcome.status,
                    score=outcome.score,
                    graded_at_ms=int(_time.time() * 1000),
                    evidence_json=json.dumps(_to_jsonable(outcome.evidence)),
                )
            ]

        receipt = await ingestion.ingest_evaluated(
            wid,
            evaluation_id=evaluation_id,
            producer=producer,
            identity_digest=identity,
            component_types=[EvalReceipt],
            build_components=_grade_and_build,
            storage_config=snapshot.storage_config,
        )
        await self._emit(
            ctx,
            "evaluate",
            world_id,
            payload_json=json.dumps(
                {
                    "evaluation_id": evaluation_id,
                    "duplicate": receipt.duplicate,
                    "grader_id": contract.grader_id,
                }
            ),
        )
        return receipt

    @instrument("gate.resume_world")
    async def resume_world(
        self, ctx: ActorCtx, storage_config: StorageConfig, world_id: str | UUID
    ) -> WorldInfo:
        """Resume a durable world as the active writer.

        Resuming reconstructs live state from durable rows and invalidates
        the previous writer. The caller receives a descriptor and continues
        through the normal command interface.
        """
        self._gate(Command(type=CommandType.CREATE_WORLD), ctx)
        world = await self._worlds.open_world_mutable(storage_config, world_id)
        await self._emit(ctx, "resume_world", world.world_id)
        return WorldInfo(
            world_id=str(world.world_id),
            name=world.name,
            tick=world.tick,
            run_id=str(world.run_id) if world.run_id else None,
        )

    # ── Simulation (gated, direct) ────────────────────────────────────────

    @instrument("gate.step")
    async def step(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> int:
        self._gate(Command(type=CommandType.STEP), ctx)
        commands_applied = await self._simulation.step(world_id, run_config, **input_kwargs)
        await self._emit(ctx, "step", world_id)
        return commands_applied

    @instrument("gate.run")
    async def run(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        run_config: RunConfig,
        **input_kwargs,
    ) -> RunResult:
        self._gate(Command(type=CommandType.RUN), ctx)
        result = await self._simulation.run(world_id, run_config, **input_kwargs)
        await self._emit(ctx, "run", world_id)
        return result

    @instrument("gate.run_episode")
    async def run_episode(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        config: EpisodeConfig,
        **input_kwargs,
    ) -> EpisodeResult:
        """Gate, then delegate to SimulationService.run_episode."""
        self._gate(Command(type=CommandType.RUN_EPISODE), ctx)
        result = await self._simulation.run_episode(world_id, config, **input_kwargs)
        await self._emit(
            ctx,
            "run_episode",
            world_id,
            payload_json=json.dumps(
                {
                    "episode_id": str(result.episode_id),
                    "run_id": str(result.run_id) if result.run_id is not None else None,
                    "start_tick": result.start_tick,
                    "final_tick": result.final_tick,
                    "terminated": result.terminated,
                    "duration_steps": result.duration_steps,
                }
            ),
        )
        return result

    @instrument("gate.run_rollout")
    async def run_rollout(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        config: RolloutConfig,
        **input_kwargs,
    ) -> RolloutResult:
        """Gate, then delegate to SimulationService.run_rollout.

        Emits ONE rollout-level audit row, not one per fork.
        Internal forks are SimulationService's mechanics.
        """
        self._gate(Command(type=CommandType.RUN_ROLLOUT), ctx)
        result = await self._simulation.run_rollout(world_id, config, **input_kwargs)
        await self._emit(
            ctx,
            "run_rollout",
            world_id,
            payload_json=json.dumps(
                {
                    "num_episodes": result.num_episodes,
                    "total_duration_steps": result.total_duration_steps,
                    "episode_world_ids": [str(ep.world_id) for ep in result.episodes],
                }
            ),
        )
        return result

    @instrument("gate.autoresearch")
    async def autoresearch(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        *,
        prepare_candidate: CandidatePreparer | None = None,
        lab_world_id: str | UUID | None = None,
        on_iteration: Callable[[IterationResult], Any] | None = None,
    ) -> AutoResearchResult:
        """Gate, then delegate to AutoResearchService.run.

        Emits ONE loop-level audit row. Candidate forks, rollouts, and
        lab-world lifecycle ticks are AutoResearchService's mechanics; the
        experiment's own ledger is the fine-grained record of every attempt.
        """
        if self._autoresearch is None:
            raise RuntimeError("CommandService was constructed without an AutoResearchService")
        # The payload drives quota accounting: the loop is charged per
        # iteration at the rollout rate (see guard.estimate_token_cost).
        self._gate(
            Command(
                type=CommandType.AUTORESEARCH,
                payload={
                    "max_iterations": config.max_iterations,
                    "num_episodes": config.num_episodes,
                },
            ),
            ctx,
        )
        result = await self._autoresearch.run(
            world_id,
            config,
            evaluator,
            prepare_candidate=prepare_candidate,
            lab_world_id=lab_world_id,
            on_iteration=on_iteration,
        )
        await self._emit(
            ctx,
            "autoresearch",
            world_id,
            payload_json=json.dumps(
                {
                    "experiment_id": config.experiment_id,
                    "iterations_completed": result.iterations_completed,
                    # -inf baselines (fresh ledger) are not JSON values.
                    "initial_score": (
                        result.initial_score if math.isfinite(result.initial_score) else None
                    ),
                    "final_score": (
                        result.final_score if math.isfinite(result.final_score) else None
                    ),
                    "improved": result.improved,
                    "lab_world_id": result.lab_world_id,
                }
            ),
        )
        return result

    # ── Queries (gated reads) ─────────────────────────────────────────────

    @instrument("gate.query_components")
    async def query_components(
        self,
        ctx: ActorCtx,
        components: list[type[Component]],
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ) -> DataFrame:
        """Query entities by component subset. Gated read."""
        self._gate(Command(type=CommandType.QUERY_WORLD), ctx)
        storage_config = self._resolve_storage(world_id, storage_config)
        result = await self._queries.query_components(
            components=components,
            world_id=world_id,
            run_id=run_id,
            storage_config=storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            lineage=await self._resolve_lineage(world_id, run_id, storage_config),
        )
        await self._emit(ctx, "query_world", world_id)
        return result

    def _resolve_storage(
        self,
        world_id: str | UUID,
        storage_config: StorageConfig | None,
    ) -> StorageConfig | None:
        """Resolve which store holds a world's rows.

        An explicit storage_config is an override and wins. Otherwise the
        world's recorded storage is used, so readers find the rows wherever
        the world actually wrote them — forks included.
        """
        if storage_config is not None:
            return storage_config
        record = self._worlds.storage_record(world_id)
        return record[0] if record is not None else None

    async def _resolve_lineage(
        self,
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None,
    ) -> list[tuple[str, str, int]] | None:
        """Fork ancestry for a world, so reads cover pre-fork ticks.

        Live worlds carry lineage in memory; destroyed worlds fall back to
        the lineage rows persisted at fork time (append-only, never lost).
        """
        try:
            world = self._worlds.get_world(UUID(str(world_id)))
        except Exception:
            world = None
        if world is not None:
            lineage = getattr(world, "lineage", None)
            return list(lineage) if lineage else None
        return await self._queries.get_lineage(world_id, run_id, storage_config)

    @instrument("gate.query_archetype")
    async def query_archetype(
        self,
        ctx: ActorCtx,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        storage_config: StorageConfig | None = None,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list[type[Component]] | None = None,
    ) -> DataFrame:
        self._gate(Command(type=CommandType.QUERY_WORLD), ctx)
        storage_config = self._resolve_storage(world_id, storage_config)
        result = await self._queries.query_archetype(
            sig,
            world_id,
            run_id,
            storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            components=components,
            lineage=await self._resolve_lineage(world_id, run_id, storage_config),
        )
        await self._emit(ctx, "query_world", world_id)
        return result

    async def list_signatures(
        self,
        ctx: ActorCtx,
        storage_config: StorageConfig | None = None,
    ) -> list[ArchetypeSignature]:
        self._gate(Command(type=CommandType.LIST_SIGNATURES), ctx)
        result = await self._queries.list_signatures(storage_config)
        await self._emit(ctx, "list_signatures")
        return result

    # ── Resource attachment (gated) ────────────────────────────────────────

    async def add_resource(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        resource: object,
    ) -> None:
        self._gate(Command(type=CommandType.ADD_RESOURCE), ctx)
        await self._worlds.add_resource(world_id, resource)
        await self._emit(ctx, "add_resource", world_id)

    async def add_hook(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        event_type,
        fn,
        *,
        mode: str = "blocking",
    ):
        self._gate(Command(type=CommandType.ADD_HOOK), ctx)
        handle = self._worlds.add_hook(world_id, event_type, fn, mode=mode)
        await self._emit(ctx, "add_hook", world_id)
        return handle

    async def remove_hook(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        handle,
    ) -> None:
        self._gate(Command(type=CommandType.REMOVE_HOOK), ctx)
        self._worlds.remove_hook(world_id, handle)
        await self._emit(ctx, "remove_hook", world_id)

    # ── Read introspection (gated) ─────────────────────────────────────────

    async def list_processors(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> list[ProcessorInfo]:
        self._gate(Command(type=CommandType.LIST_PROCESSORS), ctx)
        procs = self._worlds.list_processors(world_id)
        result = [
            ProcessorInfo(
                qualname=f"{type(p).__module__}.{type(p).__qualname__}",
                priority=getattr(p, "priority", 0),
                components=tuple(
                    f"{c.__module__}.{c.__qualname__}" for c in getattr(p, "components", ())
                ),
            )
            for p in procs
        ]
        await self._emit(ctx, "list_processors", world_id)
        return result

    async def list_hooks(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> list[HookInfo]:
        self._gate(Command(type=CommandType.LIST_HOOKS), ctx)
        entries = self._worlds.list_hooks(world_id)
        result: list[HookInfo] = []
        # Hook bus items() yields uniform (event_type, handle, fn, mode) rows.
        for event_type, handle, fn, mode in entries:
            result.append(
                HookInfo(
                    event_type=event_type.__name__,
                    handler_qualname=getattr(fn, "__qualname__", str(fn)),
                    mode=mode,
                    handle_id=handle.id,
                )
            )
        await self._emit(ctx, "list_hooks", world_id)
        return result

    async def list_resources(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
    ) -> list[ResourceInfo]:
        self._gate(Command(type=CommandType.LIST_RESOURCES), ctx)
        items = self._worlds.list_resources(world_id)
        result = [ResourceInfo(qualname=f"{t.__module__}.{t.__qualname__}") for t, _ in items]
        await self._emit(ctx, "list_resources", world_id)
        return result

    async def get_audit_history(
        self,
        ctx: ActorCtx,
        world_id: str | UUID | None = None,
        *,
        tick_range: tuple[int, int] | None = None,
        actor_id: str | UUID | None = None,
        idempotency_key: str | None = None,
        limit: int | None = None,
    ):
        self._gate(Command(type=CommandType.GET_AUDIT_HISTORY), ctx)
        if self._audit is None:
            return []
        result = await self._audit.query(
            world_id=world_id,
            tick_range=tick_range,
            actor_id=actor_id,
            idempotency_key=idempotency_key,
            limit=limit,
        )
        await self._emit(ctx, "get_audit_history", world_id)
        return result

    # ── Tick-deferred path (queued) ───────────────────────────────────────

    @staticmethod
    def _normalize_submit_args(ctx, world_id, command_or_commands):
        """Accept canonical and pre-refactor positional submit ordering."""
        from archetype.app.auth.models import ActorCtx

        if isinstance(ctx, ActorCtx):
            return ctx, world_id, command_or_commands
        if isinstance(command_or_commands, ActorCtx):
            return command_or_commands, ctx, world_id
        raise TypeError("submit expects (ctx, world_id, command) or (world_id, command, ctx)")

    async def submit(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        cmd: Command,
    ) -> UUID:
        """Gate, then enqueue for application at cmd.tick."""
        ctx, world_id, cmd = self._normalize_submit_args(ctx, world_id, cmd)
        self._require_world(world_id)
        self._gate(cmd, ctx)
        await self._broker.enqueue(world_id, cmd)
        await self._emit(ctx, cmd.type.value, world_id, command_id=cmd.id, status="queued")
        return cmd.id

    async def submit_batch(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        cmds: list[Command],
    ) -> list[UUID]:
        """Gate all-or-nothing, then enqueue atomically."""
        ctx, world_id, cmds = self._normalize_submit_args(ctx, world_id, cmds)
        self._require_world(world_id)
        for cmd in cmds:
            self._gate(cmd, ctx)
        await self._broker.enqueue_bulk(world_id, cmds)
        for cmd in cmds:
            await self._emit(ctx, cmd.type.value, world_id, command_id=cmd.id, status="queued")
        return [cmd.id for cmd in cmds]

    async def submit_spawn(
        self,
        ctx: ActorCtx,
        world_id: str | UUID,
        components: list[Component],
        *,
        tick: int = 0,
        priority: int = 0,
    ) -> int:
        """Reserve entity_id, gate, enqueue spawn, return id immediately."""
        from archetype.core.aio import AsyncWorld

        self._require_world(world_id)
        world = self._worlds.get_world(UUID(str(world_id)))
        if not isinstance(world, AsyncWorld):
            raise TypeError("submit_spawn requires AsyncWorld")

        entity_id = world.next_entity_id
        world.next_entity_id += 1

        cmd = Command(
            type=CommandType.SPAWN,
            tick=tick,
            priority=priority,
            payload={"entity_id": entity_id, "components": list(components)},
        )

        try:
            self._gate(cmd, ctx)
            await self._broker.enqueue(world_id, cmd)
            await self._emit(ctx, "spawn", world_id, command_id=cmd.id, status="queued")
        except Exception:
            # Roll back the reserved id
            if world.next_entity_id == entity_id + 1:
                world.next_entity_id = entity_id
            raise

        return entity_id

    async def drain_and_apply(
        self,
        world_id: str | UUID,
        tick: int,
    ) -> list[Command]:
        """Drain commands due at tick, apply them via MutationService.

        No ActorCtx — commands carry their own context validated at submit.
        """
        commands = await self._broker.dequeue_due(world_id, tick)
        if not commands:
            return []

        applied: list[Command] = []
        for cmd in commands:
            try:
                await self._apply(world_id, cmd)
                applied.append(cmd)
            except Exception:
                logger.exception("Failed to apply command %s (%s)", cmd.id, cmd.type.value)

        if applied:
            await self._broker.ack([cmd.id for cmd in applied])

        return applied

    @staticmethod
    def _hydrate_components(payload_components) -> list[Component]:
        from archetype.core.component import Component

        return [
            component if isinstance(component, Component) else Component.from_dict(component)
            for component in payload_components
        ]

    @staticmethod
    def _hydrate_component_types(payload_types) -> list[type[Component]]:
        from archetype.core.component import Component

        return [
            component_type
            if isinstance(component_type, type) and issubclass(component_type, Component)
            else Component.get_type_by_name(str(component_type))
            for component_type in payload_types
        ]

    async def _apply(self, world_id: str | UUID, cmd: Command) -> None:
        """Dispatch a single command to the appropriate mutation."""
        payload = cmd.payload

        match cmd.type:
            case CommandType.SPAWN:
                components = self._hydrate_components(payload.get("components", []))
                entity_id = payload.get("entity_id")
                if entity_id is not None:
                    # Deferred spawn with reserved id — register directly
                    # on the world so the pre-reserved ID is honored.
                    from archetype.core.aio import AsyncWorld

                    world = self._worlds.get_world(UUID(str(world_id)))
                    if isinstance(world, AsyncWorld):
                        await world._register_entity(_parse_entity_id(entity_id), components)
                else:
                    await self._mutations.create_entity(world_id, components)

            case CommandType.UPDATE:
                components = self._hydrate_components(payload.get("components", []))
                await self._mutations.update_entity(
                    world_id, _parse_entity_id(payload["entity_id"]), components
                )

            case CommandType.DESPAWN:
                await self._mutations.remove_entity(
                    world_id, _parse_entity_id(payload["entity_id"])
                )

            case CommandType.ADD_COMPONENT:
                components = self._hydrate_components(payload.get("components", []))
                await self._mutations.add_components(
                    world_id, _parse_entity_id(payload["entity_id"]), components
                )

            case CommandType.REMOVE_COMPONENT:
                component_types = self._hydrate_component_types(payload.get("component_types", []))
                await self._mutations.remove_components(
                    world_id, _parse_entity_id(payload["entity_id"]), component_types
                )

            case CommandType.ADD_PROCESSOR:
                await self._mutations.add_processor(world_id, payload["processor"])

            case CommandType.REMOVE_PROCESSOR:
                await self._mutations.remove_processor(world_id, payload["processor_type"])

            case _:
                logger.warning("Unhandled command type in drain: %s", cmd.type.value)
