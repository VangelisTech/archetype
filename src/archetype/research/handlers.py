# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Free AutoResearch workflow handlers and experiment-scoped admission."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, cast

from pydantic_core import to_jsonable_python
from uuid_utils import UUID

from archetype.core.config import RunConfig, WorldConfig
from archetype.research.components import BranchHead, Experiment, Result, Run, RunStatus
from archetype.research.models import (
    AutoResearch,
    AutoResearchConfig,
    AutoResearchResult,
    CandidatePreparer,
    Evaluation,
    EvaluationResult,
    Evaluator,
    IterationResult,
    ResearchCandidateContext,
)
from archetype.research.views import next_iteration, read_experiment, read_head
from archetype.storage.interfaces import iStorageService
from archetype.world import mutation, simulation
from archetype.world.interfaces import iWorldLifecycle, iWorldRegistry
from archetype.world.models import RolloutConfig, RolloutResult

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _AdmissionEntry:
    lock: asyncio.Lock
    users: int = 0


class AutoResearchAdmissions:
    """Composition-owned keyed locks for durable experiment ledgers."""

    def __init__(self) -> None:
        self._guard = asyncio.Lock()
        self._entries: dict[str, _AdmissionEntry] = {}

    @staticmethod
    def key(experiment_id: str) -> str:
        """Return the exact process-local admission key for an experiment."""

        return f"autoresearch:{experiment_id}"

    @asynccontextmanager
    async def admit(self, experiment_id: str) -> AsyncIterator[str]:
        """Serialize one experiment and release ownership on every exit path."""

        key = self.key(experiment_id)
        async with self._guard:
            entry = self._entries.setdefault(key, _AdmissionEntry(lock=asyncio.Lock()))
            entry.users += 1

        acquired = False
        try:
            await entry.lock.acquire()
            acquired = True
            yield key
        finally:
            if acquired:
                entry.lock.release()
            async with self._guard:
                entry.users -= 1
                if entry.users == 0 and self._entries.get(key) is entry:
                    self._entries.pop(key)


def _normalize_evaluation(value: Evaluation, evaluator_id: str) -> EvaluationResult:
    if isinstance(value, EvaluationResult):
        if value.evaluator != evaluator_id:
            raise ValueError(
                "evaluation evaluator does not match the configured evaluator_id: "
                f"{value.evaluator!r} != {evaluator_id!r}"
            )
        return value
    return EvaluationResult(score=float(value), evaluator=evaluator_id)


def _callable_identity(value: Any) -> str | None:
    """Return a diagnostic name for configured callable/type boundaries."""

    if value is None:
        return None
    module = getattr(value, "__module__", type(value).__module__)
    qualname = getattr(value, "__qualname__", type(value).__qualname__)
    return f"{module}.{qualname}"


def _jsonable_run_metadata(metadata: dict[str, Any] | None) -> Any:
    """Normalize run metadata to deterministic JSON-native identity values."""

    if metadata is None:
        return None

    def reject_unknown(value: Any) -> str:
        if isinstance(value, UUID):
            return str(value)
        raise ValueError(
            "run metadata is part of the experiment identity and must be "
            f"JSON-encodable; got {type(value).__module__}.{type(value).__qualname__}"
        )

    return to_jsonable_python(metadata, fallback=reject_unknown)


def _config_identity(config: AutoResearchConfig) -> tuple[str, dict[str, Any]]:
    """Hash semantic loop configuration, excluding invocation-only fields."""

    episode = config.episode_config
    run = episode.run_config
    payload = {
        "schema_version": 2,
        "experiment_name": config.experiment_name,
        "evaluator_id": config.evaluator_id,
        "rollout_contract_id": config.rollout_contract_id,
        "episode": {
            "max_steps": episode.max_steps,
            "terminal_component": _callable_identity(episode.terminal_component),
            "terminal_field": episode.terminal_field,
            "terminal_all": episode.terminal_all,
            "termination": _callable_identity(episode.termination),
            "run": {
                "num_steps": run.num_steps,
                "debug": run.debug,
                "show_rows": run.show_rows,
                "metadata": _jsonable_run_metadata(run.metadata),
            },
        },
        "num_episodes": config.num_episodes,
        "parallel": config.parallel,
        "improvement_threshold": config.improvement_threshold,
        "destroy_forks_on_complete": config.destroy_forks_on_complete,
    }
    try:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except ValueError as exc:
        raise ValueError(
            "run metadata is part of the experiment identity and must be "
            f"strict-JSON encodable: {exc}"
        ) from exc
    return hashlib.sha256(encoded.encode()).hexdigest(), payload


async def handle_autoresearch(
    admissions: AutoResearchAdmissions,
    world_registry: iWorldRegistry,
    world_lifecycle: iWorldLifecycle,
    storage: iStorageService,
    destroy_world: simulation.DestroyWorldCallable,
    operation: AutoResearch,
) -> AutoResearchResult:
    """Translate the sole research operation into one directly awaited workflow."""

    return await run_autoresearch(
        admissions,
        world_registry,
        world_lifecycle,
        storage,
        destroy_world,
        operation.world_id,
        operation.config,
        operation.evaluator,
        prepare_candidate=operation.prepare_candidate,
        lab_world_id=operation.lab_world_id,
        on_iteration=operation.on_iteration,
    )


async def run_autoresearch(
    admissions: AutoResearchAdmissions,
    world_registry: iWorldRegistry,
    world_lifecycle: iWorldLifecycle,
    storage: iStorageService,
    destroy_world: simulation.DestroyWorldCallable,
    world_id: str | UUID,
    config: AutoResearchConfig,
    evaluator: Evaluator,
    *,
    prepare_candidate: CandidatePreparer | None = None,
    lab_world_id: str | UUID | None = None,
    on_iteration: Callable[[IterationResult], Any] | None = None,
) -> AutoResearchResult:
    """Run one workflow, serializing only experiments with durable ledgers."""

    if not config.record_to_ledger:
        return await _run_autoresearch(
            world_registry,
            world_lifecycle,
            storage,
            destroy_world,
            world_id,
            config,
            evaluator,
            prepare_candidate=prepare_candidate,
            lab_world_id=lab_world_id,
            on_iteration=on_iteration,
        )

    async with admissions.admit(config.experiment_id):
        return await _run_autoresearch(
            world_registry,
            world_lifecycle,
            storage,
            destroy_world,
            world_id,
            config,
            evaluator,
            prepare_candidate=prepare_candidate,
            lab_world_id=lab_world_id,
            on_iteration=on_iteration,
        )


async def _run_autoresearch(
    world_registry: iWorldRegistry,
    world_lifecycle: iWorldLifecycle,
    storage: iStorageService,
    destroy_world: simulation.DestroyWorldCallable,
    world_id: str | UUID,
    config: AutoResearchConfig,
    evaluator: Evaluator,
    *,
    prepare_candidate: CandidatePreparer | None = None,
    lab_world_id: str | UUID | None = None,
    on_iteration: Callable[[IterationResult], Any] | None = None,
) -> AutoResearchResult:
    async with world_registry.operation(str(world_id)):
        pass

    lab_id: str | None = None
    head_entity_id: int | None = None
    incumbent_score = float("-inf")
    start_iteration = 0
    if config.record_to_ledger:
        (
            lab_id,
            head_entity_id,
            incumbent_score,
            start_iteration,
        ) = await _attach_ledger(
            world_registry,
            world_lifecycle,
            storage,
            world_id,
            config,
            lab_world_id=lab_world_id,
        )

    initial_score = incumbent_score
    iterations: list[IterationResult] = []

    for i in range(start_iteration, start_iteration + config.max_iterations):
        run_id = f"{config.experiment_id}:iter{i}"
        # The display name is intentionally not experiment identity. Derive
        # fork names from the stable id so unrelated experiments may share it.
        rollout_config = RolloutConfig(
            episode_config=config.episode_config,
            num_episodes=config.num_episodes,
            parallel=config.parallel,
            destroy_forks_on_complete=config.destroy_forks_on_complete,
            name_prefix=f"autoresearch:{config.experiment_id}:iter{i}",
        )

        started_at_ms = int(time.time() * 1000)
        run_entity_id: int | None = None
        if lab_id is not None:
            run_entity_id = await _record_running(
                world_registry,
                lab_id,
                config,
                run_id=run_id,
                started_at_ms=started_at_ms,
            )

        candidate_world_id: str | UUID = world_id
        try:
            if prepare_candidate is not None:
                prepared_value = prepare_candidate(
                    ResearchCandidateContext(
                        experiment_id=config.experiment_id,
                        experiment_name=config.experiment_name,
                        iteration=i,
                        run_id=run_id,
                        base_world_id=str(world_id),
                    )
                )
                if inspect.isawaitable(prepared_value):
                    prepared_value = await prepared_value
                prepared = cast(str | UUID | None, prepared_value)
                if prepared is not None:
                    candidate_world_id = prepared

            rollout_result = await simulation.run_rollout(
                world_registry,
                storage,
                world_lifecycle.fork_world,
                destroy_world,
                candidate_world_id,
                rollout_config,
            )

            evaluation_value = evaluator(rollout_result)
            if inspect.isawaitable(evaluation_value):
                evaluation_value = await evaluation_value
            evaluation = _normalize_evaluation(
                cast(Evaluation, evaluation_value),
                config.evaluator_id,
            )
        except BaseException as exc:
            if lab_id is not None and run_entity_id is not None:
                try:
                    await _record_terminal(
                        world_registry,
                        lab_id,
                        run_entity_id,
                        head_entity_id,
                        config,
                        run_id=run_id,
                        iteration=i,
                        status=RunStatus.CRASHED,
                        started_at_ms=started_at_ms,
                        candidate_world_id=candidate_world_id,
                        error=exc,
                    )
                except Exception:
                    logger.exception(
                        "failed to persist autoresearch crash transition for %s",
                        run_id,
                    )
            raise

        score = evaluation.score
        if lab_id is None and i == start_iteration:
            initial_score = score

        improved = score > incumbent_score + config.improvement_threshold
        if improved:
            incumbent_score = score

        if lab_id is not None:
            assert run_entity_id is not None
            await _record_terminal(
                world_registry,
                lab_id,
                run_entity_id,
                head_entity_id,
                config,
                run_id=run_id,
                iteration=i,
                status=RunStatus.STOPPED,
                rollout=rollout_result,
                evaluation=evaluation,
                improved=improved,
                started_at_ms=started_at_ms,
                candidate_world_id=candidate_world_id,
            )

        iteration_result = IterationResult(
            iteration=i,
            rollout=rollout_result,
            score=score,
            evaluation=evaluation,
            improved=improved,
            incumbent_score=incumbent_score,
        )
        iterations.append(iteration_result)

        logger.info(
            "autoresearch %s iter=%d score=%.4f incumbent=%.4f improved=%s",
            config.experiment_name,
            i,
            score,
            incumbent_score,
            improved,
        )

        if on_iteration is not None:
            result = on_iteration(iteration_result)
            if inspect.isawaitable(result):
                await result

    return AutoResearchResult(
        experiment_name=config.experiment_name,
        iterations_completed=len(iterations),
        final_score=incumbent_score,
        initial_score=initial_score,
        iterations=iterations,
        lab_world_id=lab_id or "",
    )


async def _attach_ledger(
    world_registry: iWorldRegistry,
    world_lifecycle: iWorldLifecycle,
    storage: iStorageService,
    base_world_id: str | UUID,
    config: AutoResearchConfig,
    *,
    lab_world_id: str | UUID | None = None,
) -> tuple[str, int, float, int]:
    config_digest, config_payload = _config_identity(config)
    expected_metadata = {
        "experiment_id": config.experiment_id,
        "base_world_id": str(base_world_id),
        "config_digest": config_digest,
        "config": config_payload,
    }

    name = f"autoresearch:{config.experiment_id}"
    if lab_world_id is not None:
        lab_id = str(lab_world_id)
    else:
        try:
            lab_id = await world_registry.world_id_for_name(name)
        except KeyError:
            lab_id = None

    if lab_id is None:
        record = await world_registry.storage_record(str(base_world_id))
        storage_config = record[0] if record is not None else None
        cache_config = record[1] if record is not None else None
        lab = await world_lifecycle.create_world(
            WorldConfig(name=name),
            storage_config,
            cache_config,
        )
        lab_id = str(lab.world_id)

    async with world_registry.operation(lab_id) as lab:
        if lab.tick == 0:
            await mutation._create_entity_locked(
                lab,
                [
                    Experiment.make(
                        config.experiment_name,
                        "",
                        metadata=expected_metadata,
                    )
                ],
            )
            head_entity_id = await mutation._create_entity_locked(
                lab,
                [
                    BranchHead.make(
                        config.experiment_name,
                        "",
                        descriptor={"score": None},
                    )
                ],
            )
            await simulation._step_locked(
                world_registry,
                lab_id,
                lab,
                RunConfig(),
            )
            return lab_id, head_entity_id, float("-inf"), 0

        experiment_row = await read_experiment(lab, storage)
        if experiment_row is None:
            raise ValueError(
                f"experiment identity collision: lab world {lab.world_id} has no Experiment row"
            )
        actual_metadata = json.loads(experiment_row["experiment__metadata_json"])
        if (
            experiment_row["experiment__name"] != config.experiment_name
            or actual_metadata != expected_metadata
        ):
            raise ValueError(
                "experiment identity collision: the requested experiment id, "
                "base world, or semantic configuration does not match the attached lab"
            )

        head_row = await read_head(lab, storage)
        if head_row is None:
            raise ValueError(
                f"experiment identity collision: lab world {lab.world_id} has no BranchHead row"
            )
        descriptor = json.loads(head_row["branchhead__descriptor_json"])
        score = descriptor.get("score")
        incumbent = float(score) if score is not None else float("-inf")
        return (
            lab_id,
            int(head_row["entity_id"]),
            incumbent,
            await next_iteration(lab, storage, config),
        )


async def _record_running(
    world_registry: iWorldRegistry,
    lab_world_id: str,
    config: AutoResearchConfig,
    *,
    run_id: str,
    started_at_ms: int,
) -> int:
    async with world_registry.operation(lab_world_id) as lab:
        run_entity_id = await mutation._create_entity_locked(
            lab,
            [
                Run(
                    run_id=run_id,
                    experiment_name=config.experiment_name,
                    status=RunStatus.RUNNING.value,
                    task="rollout",
                    started_at_ms=started_at_ms,
                )
            ],
        )
        await simulation._step_locked(
            world_registry,
            lab_world_id,
            lab,
            RunConfig(),
        )
        return run_entity_id


async def _record_terminal(
    world_registry: iWorldRegistry,
    lab_world_id: str,
    run_entity_id: int,
    head_entity_id: int | None,
    config: AutoResearchConfig,
    *,
    run_id: str,
    iteration: int,
    status: RunStatus,
    started_at_ms: int,
    rollout: RolloutResult | None = None,
    evaluation: EvaluationResult | None = None,
    improved: bool = False,
    candidate_world_id: str | UUID | None = None,
    error: BaseException | None = None,
) -> None:
    if not RunStatus.is_terminal(status.value):
        raise ValueError(f"terminal status required, got {status.value}")
    if status is RunStatus.STOPPED and (rollout is None or evaluation is None):
        raise ValueError("STOPPED requires rollout and evaluation")

    async with world_registry.operation(lab_world_id) as lab:
        await mutation._update_entity_locked(
            lab,
            run_entity_id,
            [
                Run(
                    run_id=run_id,
                    experiment_name=config.experiment_name,
                    status=status.value,
                    task="rollout",
                    started_at_ms=started_at_ms,
                    finished_at_ms=int(time.time() * 1000),
                )
            ],
        )

        if status is RunStatus.STOPPED:
            assert rollout is not None
            assert evaluation is not None
            await mutation._create_entity_locked(
                lab,
                [
                    Result.make(
                        run_id,
                        outputs={
                            "score": evaluation.score,
                            "improved": improved,
                            "iteration": iteration,
                            "candidate_world_id": str(candidate_world_id),
                            "num_episodes": rollout.num_episodes,
                            "total_duration_steps": rollout.total_duration_steps,
                            "episode_world_ids": [
                                str(episode.world_id) for episode in rollout.episodes
                            ],
                            "evidence": evaluation.evidence,
                            "metadata": evaluation.metadata,
                        },
                        evaluator=evaluation.evaluator,
                    )
                ],
            )
        elif error is not None:
            await mutation._create_entity_locked(
                lab,
                [
                    Result.make(
                        run_id,
                        outputs={
                            "iteration": iteration,
                            "status": RunStatus.CRASHED.value,
                            "candidate_world_id": str(candidate_world_id),
                            "error_type": f"{type(error).__module__}.{type(error).__qualname__}",
                        },
                        evaluator="autoresearch:lifecycle",
                    )
                ],
            )

        if status is RunStatus.STOPPED and improved and head_entity_id is not None:
            assert evaluation is not None
            await mutation._update_entity_locked(
                lab,
                head_entity_id,
                [
                    BranchHead.make(
                        config.experiment_name,
                        "",
                        run_id=run_id,
                        descriptor={
                            "score": evaluation.score,
                            "iteration": iteration,
                            "evaluator": evaluation.evaluator,
                            "evidence": evaluation.evidence,
                        },
                    )
                ],
            )
        await simulation._step_locked(
            world_registry,
            lab_world_id,
            lab,
            RunConfig(),
        )


__all__ = [
    "AutoResearchAdmissions",
    "handle_autoresearch",
    "run_autoresearch",
]
