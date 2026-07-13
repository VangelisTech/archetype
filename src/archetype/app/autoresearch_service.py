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
AutoResearch Service

The loop controller. Reads the current BranchHead, launches a rollout,
evaluates results, and advances the head on improvement.

The service is deliberately scoring-agnostic: the user provides an
evaluator callable that takes rollout results and returns a typed evaluation
(or a compatibility float). The service compares finite scores and advances
the BranchHead when the new score beats the incumbent.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from uuid_utils import UUID

from archetype.app.models import EpisodeConfig, RolloutConfig, RolloutResult
from archetype.core.config import RunConfig, WorldConfig
from archetype.experiments.components import BranchHead, Experiment, Result, Run, RunStatus

if TYPE_CHECKING:
    from archetype.app.simulation_service import SimulationService
    from archetype.app.world_service import WorldService

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AutoResearchConfig:
    """Configuration for one autoresearch loop.

    Higher scores are better. The loop advances the BranchHead when a finite
    evaluation score exceeds the incumbent by at least
    ``improvement_threshold``.
    """

    experiment_name: str
    # Stable caller-supplied identity.  The display name is deliberately not
    # used as the resume/idempotency key.
    experiment_id: str
    # Stable caller-supplied scoring contract. Scores are comparable only
    # while this identity remains unchanged.
    evaluator_id: str
    # Stable caller-supplied identity for episode dynamics, termination, and
    # rollout interpretation. Callable qualnames alone are not semantic IDs.
    rollout_contract_id: str
    episode_config: EpisodeConfig = field(default_factory=EpisodeConfig)
    num_episodes: int = 10
    parallel: bool = False
    max_iterations: int = 100
    improvement_threshold: float = 0.0
    destroy_forks_on_complete: bool = True
    # The loop's own state lives on the ledger: a lab world named
    # "autoresearch:{experiment_id}" with explicit attempt lifecycle ticks.
    record_to_ledger: bool = True

    def __post_init__(self) -> None:
        if not self.experiment_id.strip():
            raise ValueError("experiment_id must be a non-empty caller-supplied identity")
        if not self.experiment_name.strip():
            raise ValueError("experiment_name must be non-empty")
        if not self.evaluator_id.strip():
            raise ValueError("evaluator_id must be a non-empty scoring contract identity")
        if not self.rollout_contract_id.strip():
            raise ValueError("rollout_contract_id must be a non-empty rollout contract identity")
        if self.num_episodes < 1:
            raise ValueError("num_episodes must be at least 1")
        if self.max_iterations < 1:
            raise ValueError("max_iterations must be at least 1")
        if self.episode_config.max_steps < 1:
            raise ValueError("episode_config.max_steps must be at least 1")
        if not math.isfinite(self.improvement_threshold):
            raise ValueError("improvement_threshold must be finite")
        if self.improvement_threshold < 0:
            raise ValueError("improvement_threshold must be non-negative")


@dataclass(frozen=True)
class CandidateContext:
    """Stable context passed to a per-iteration candidate preparer."""

    experiment_id: str
    experiment_name: str
    iteration: int
    run_id: str
    base_world_id: str


@dataclass(frozen=True)
class EvaluationResult:
    """Typed evaluator output with explicit evaluator and evidence metadata."""

    score: float
    evaluator: str
    evidence: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        score = float(self.score)
        if not math.isfinite(score):
            raise ValueError("evaluation score must be finite")
        if not self.evaluator.strip():
            raise ValueError("evaluation evaluator must be non-empty")
        # Result.make persists these values as JSON.  Validate at the typed
        # boundary so a terminal STOPPED row cannot be paired with an
        # unpersistable evaluation.
        json.dumps(self.evidence)
        json.dumps(self.metadata)
        object.__setattr__(self, "score", score)


@dataclass(frozen=True)
class IterationResult:
    """Result of one autoresearch iteration."""

    iteration: int
    rollout: RolloutResult
    score: float
    evaluation: EvaluationResult
    improved: bool
    incumbent_score: float


@dataclass(frozen=True)
class AutoResearchResult:
    """Result of the full autoresearch loop."""

    experiment_name: str
    iterations_completed: int
    final_score: float
    initial_score: float
    iterations: list[IterationResult] = field(default_factory=list)
    # World id of the experiment's ledger (lab world); "" when recording
    # was disabled. Query it like any other world: Experiment at tick 0,
    # then RUNNING and terminal lifecycle ticks for every attempt.
    lab_world_id: str = ""

    @property
    def improved(self) -> bool:
        return self.final_score > self.initial_score


# ─────────────────────────────────────────────────────────────────────────────
# Evaluator protocol
# ─────────────────────────────────────────────────────────────────────────────

# Float evaluators remain supported for compatibility.  Typed evaluators are
# preferred because their identity and evidence metadata are persisted beside
# the score.
Evaluation = float | EvaluationResult
Evaluator = Callable[[RolloutResult], Evaluation | Awaitable[Evaluation]]
CandidatePreparer = Callable[
    [CandidateContext],
    str | UUID | None | Awaitable[str | UUID | None],
]


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
    """Return a diagnostic name for configured callable/type boundaries.

    This is persisted for inspection, not trusted as the semantic contract;
    callers supply ``rollout_contract_id`` for that purpose.
    """
    if value is None:
        return None
    module = getattr(value, "__module__", type(value).__module__)
    qualname = getattr(value, "__qualname__", type(value).__qualname__)
    return f"{module}.{qualname}"


def _config_identity(config: AutoResearchConfig) -> tuple[str, dict[str, Any]]:
    """Hash semantic loop configuration, excluding invocation-only fields.

    ``max_iterations`` may change when extending a run.  Generated episode and
    run UUIDs are also excluded; they identify one invocation, not the
    experiment contract.
    """
    episode = config.episode_config
    run = episode.run_config
    payload = {
        "schema_version": 1,
        "experiment_name": config.experiment_name,
        "evaluator_id": config.evaluator_id,
        "rollout_contract_id": config.rollout_contract_id,
        "episode": {
            "max_steps": episode.max_steps,
            "terminal_component": _callable_identity(episode.terminal_component),
            "termination": _callable_identity(episode.termination),
            "run": {
                "num_steps": run.num_steps,
                "debug": run.debug,
                "show_rows": run.show_rows,
                "metadata": run.metadata,
            },
        },
        "num_episodes": config.num_episodes,
        "parallel": config.parallel,
        "improvement_threshold": config.improvement_threshold,
        "destroy_forks_on_complete": config.destroy_forks_on_complete,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest(), payload


# ─────────────────────────────────────────────────────────────────────────────
# Service
# ─────────────────────────────────────────────────────────────────────────────


class AutoResearchService:
    """Loop controller for autoresearch.

    Depends on:
      - WorldService for world lifecycle (fork, destroy, get_world)
      - SimulationService for run_rollout execution
    """

    def __init__(
        self,
        world_service: WorldService,
        simulation_service: SimulationService,
    ) -> None:
        self._world_service = world_service
        self._simulation_service = simulation_service

    async def run(
        self,
        world_id: str | UUID,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        *,
        prepare_candidate: CandidatePreparer | None = None,
        lab_world_id: str | UUID | None = None,
        on_iteration: Callable[[IterationResult], Any] | None = None,
    ) -> AutoResearchResult:
        """Run the autoresearch loop.

        1. Get the base world state
        2. For each iteration:
           a. Optionally prepare a distinct candidate world
           b. Run a rollout (N episodes on forks of the candidate)
           c. Evaluate the rollout result → typed evaluation
           d. If score > incumbent + threshold → advance (record improvement)
           e. Call on_iteration callback if provided
        3. Return the aggregate result

        The service never mutates the base world. Without ``prepare_candidate``,
        every iteration evaluates the same base seed. A preparer may return a
        caller-owned world id so successive iterations evaluate distinct
        candidates; returning ``None`` selects the base world.

        The loop's own state lives on the ledger (unless
        config.record_to_ledger is False): a lab world named
        "autoresearch:{experiment_id}" whose tick 0 is the genesis. Every
        attempt first appends a RUNNING Run row, then appends a STOPPED or
        CRASHED transition. Successful attempts also append a Result and any
        BranchHead advance. ``lab_world_id`` explicitly reattaches a live lab
        world instead of relying on its name index. Stored experiment identity
        and configuration must match before resume.
        """
        # Validate the base world exists (raises if not); the loop forks
        # from world_id rather than mutating this instance.
        self._world_service.get_world(UUID(str(world_id)))

        lab = None
        head_entity_id: int | None = None
        incumbent_score = float("-inf")
        start_iteration = 0
        if config.record_to_ledger:
            lab, head_entity_id, incumbent_score, start_iteration = await self._attach_ledger(
                world_id, config, lab_world_id=lab_world_id
            )

        # The result baseline is the incumbent at invocation start. This keeps
        # aggregate ``improved`` semantics correct for both fresh and resumed
        # runs; the first candidate score is not itself the baseline.
        initial_score = incumbent_score
        iterations: list[IterationResult] = []

        for i in range(start_iteration, start_iteration + config.max_iterations):
            run_id = f"{config.experiment_id}:iter{i}"
            # Build rollout config
            rollout_config = RolloutConfig(
                episode_config=config.episode_config,
                num_episodes=config.num_episodes,
                parallel=config.parallel,
                destroy_forks_on_complete=config.destroy_forks_on_complete,
                name_prefix=f"{config.experiment_name}:iter{i}",
            )

            started_at_ms = int(time.time() * 1000)
            run_entity_id: int | None = None
            if lab is not None:
                run_entity_id = await self._record_running(
                    lab,
                    config,
                    run_id=run_id,
                    started_at_ms=started_at_ms,
                )

            candidate_world_id: str | UUID = world_id
            try:
                if prepare_candidate is not None:
                    prepared_value = prepare_candidate(
                        CandidateContext(
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

                rollout_result = await self._simulation_service.run_rollout(
                    candidate_world_id, rollout_config
                )

                evaluation_value = evaluator(rollout_result)
                if inspect.isawaitable(evaluation_value):
                    evaluation_value = await evaluation_value
                evaluation = _normalize_evaluation(
                    cast(Evaluation, evaluation_value), config.evaluator_id
                )
            except BaseException as exc:
                if lab is not None and run_entity_id is not None:
                    try:
                        await self._record_terminal(
                            lab,
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
                            "failed to persist autoresearch crash transition for %s", run_id
                        )
                raise

            score = evaluation.score

            # Compare to incumbent
            improved = score > incumbent_score + config.improvement_threshold
            if improved:
                incumbent_score = score

            if lab is not None:
                assert run_entity_id is not None
                await self._record_terminal(
                    lab,
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

            # Callback
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
            lab_world_id=str(lab.world_id) if lab is not None else "",
        )

    # ── Ledger: the loop's own state as world rows ─────────────────────────

    async def _attach_ledger(
        self,
        base_world_id: str | UUID,
        config: AutoResearchConfig,
        *,
        lab_world_id: str | UUID | None = None,
    ) -> tuple[Any, int, float, int]:
        """Create or resume the experiment's lab world.

        Returns (lab_world, head_entity_id, incumbent_score, start_iteration).

        Genesis (new lab world): spawn Experiment + a seed BranchHead and
        step once, so tick 0 holds the experiment's initial conditions. The
        lab world inherits the base world's storage — the record lives next
        to the data it is about.

        Resume (existing lab world): validate the stored experiment contract,
        read the incumbent BranchHead, and derive the next index from recorded
        Run ids.  A caller may attach by explicit ``lab_world_id`` instead of
        relying on the process-local name index.
        """
        name = f"autoresearch:{config.experiment_id}"
        if lab_world_id is not None:
            lab = self._world_service.get_world(UUID(str(lab_world_id)))
        else:
            try:
                lab = self._world_service.get_world_by_name(name)
            except KeyError:
                lab = None

        if lab is None:
            record = self._world_service.storage_record(base_world_id)
            storage_config = record[0] if record is not None else None
            cache_config = record[1] if record is not None else None
            lab = await self._world_service.create_world(
                WorldConfig(name=name), storage_config, cache_config
            )

        config_digest, config_payload = _config_identity(config)
        expected_metadata = {
            "experiment_id": config.experiment_id,
            "base_world_id": str(base_world_id),
            "config_digest": config_digest,
            "config": config_payload,
        }

        if lab.tick == 0:
            await lab.create_entity(
                [
                    Experiment.make(
                        config.experiment_name,
                        "",
                        metadata=expected_metadata,
                    )
                ]
            )
            head_entity_id = await lab.create_entity(
                [BranchHead.make(config.experiment_name, "", descriptor={"score": None})]
            )
            await lab.step(RunConfig())  # genesis: initial conditions at tick 0
            return lab, head_entity_id, float("-inf"), 0

        experiment_row = await self._read_experiment(lab)
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
                "experiment identity collision: the requested experiment id, base world, or "
                "semantic configuration does not match the attached lab"
            )

        head_row = await self._read_head(lab)
        if head_row is None:
            raise ValueError(
                f"experiment identity collision: lab world {lab.world_id} has no BranchHead row"
            )
        descriptor = json.loads(head_row["branchhead__descriptor_json"])
        score = descriptor.get("score")
        incumbent = float(score) if score is not None else float("-inf")
        return lab, int(head_row["entity_id"]), incumbent, await self._next_iteration(lab, config)

    async def _read_experiment(self, lab) -> dict | None:
        """The active Experiment row, or None when the lab has no genesis."""
        if lab.tick == 0:
            return None
        df = await lab.query_archetype(sig=(Experiment,), ticks=[lab.tick - 1])
        rows = df.to_pylist()
        return rows[0] if rows else None

    async def _read_head(self, lab) -> dict | None:
        """The latest persisted BranchHead row, or None for a fresh world."""
        if lab.tick == 0:
            return None
        df = await lab.query_archetype(sig=(BranchHead,), ticks=[lab.tick - 1])
        rows = df.to_pylist()
        return rows[0] if rows else None

    async def _next_iteration(self, lab, config: AutoResearchConfig) -> int:
        """Return one past the greatest terminal iteration Run id.

        An active row is not silently abandoned: without an explicit lease or
        reconciliation protocol, starting another attempt could duplicate
        work and compare a result against an ambiguous incumbent.
        """
        if lab.tick == 0:
            return 0
        rows = (await lab.query_archetype(sig=(Run,), ticks=[lab.tick - 1])).to_pylist()
        prefix = f"{config.experiment_id}:iter"
        indices: list[int] = []
        for row in rows:
            run_id = row["run__run_id"]
            if not run_id.startswith(prefix):
                raise ValueError(
                    "experiment identity collision: attached lab contains an unexpected Run id"
                )
            suffix = run_id[len(prefix) :]
            if not suffix.isdigit():
                raise ValueError(
                    "experiment identity collision: attached lab contains a malformed Run id"
                )
            status = row["run__status"]
            if RunStatus.is_active(status):
                raise RuntimeError(
                    f"experiment has an active attempt {run_id!r}; reconcile it before resuming"
                )
            if not RunStatus.is_terminal(status):
                raise ValueError(
                    f"experiment history contains an unknown Run status {status!r} for {run_id!r}"
                )
            indices.append(int(suffix))
        ordered_indices = sorted(indices)
        if ordered_indices != list(range(len(ordered_indices))):
            raise ValueError(
                "experiment history contains duplicate or non-contiguous iteration ids"
            )
        return len(ordered_indices)

    async def _record_running(
        self,
        lab,
        config: AutoResearchConfig,
        *,
        run_id: str,
        started_at_ms: int,
    ) -> int:
        """Persist RUNNING before candidate preparation or rollout begins."""
        run_entity_id = await lab.create_entity(
            [
                Run(
                    run_id=run_id,
                    experiment_name=config.experiment_name,
                    status=RunStatus.RUNNING.value,
                    task="rollout",
                    started_at_ms=started_at_ms,
                )
            ]
        )
        await lab.step(RunConfig())
        return run_entity_id

    async def _record_terminal(
        self,
        lab,
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
        """Persist a terminal Run transition and its result envelope."""
        if not RunStatus.is_terminal(status.value):
            raise ValueError(f"terminal status required, got {status.value}")
        if status is RunStatus.STOPPED and (rollout is None or evaluation is None):
            raise ValueError("STOPPED requires rollout and evaluation")

        await lab.update_entity(
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
            await lab.create_entity(
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
                            "episode_world_ids": [str(ep.world_id) for ep in rollout.episodes],
                            "evidence": evaluation.evidence,
                            "metadata": evaluation.metadata,
                        },
                        evaluator=evaluation.evaluator,
                    )
                ]
            )
        elif error is not None:
            await lab.create_entity(
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
                ]
            )

        if status is RunStatus.STOPPED and improved and head_entity_id is not None:
            assert evaluation is not None
            await lab.update_entity(
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
        await lab.step(RunConfig())

    async def sweep(
        self,
        world_id: str | UUID,
        config: AutoResearchConfig,
        evaluator: Evaluator,
        param_grid: dict[str, list[Any]],
        *,
        setup_fn: Callable | None = None,
    ) -> dict[tuple, AutoResearchResult]:
        """Parameter sweep: run the autoresearch loop for each point in a grid.

        For each combination of parameters in param_grid:
        1. Fork the base world
        2. Call setup_fn(fork_world_id, params) to configure the fork
        3. Run the autoresearch loop on the fork
        4. Collect results keyed by parameter tuple

        Returns a dict mapping param tuples to AutoResearchResults.
        """
        import itertools

        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        results: dict[tuple, AutoResearchResult] = {}

        for combo in itertools.product(*param_values):
            params = dict(zip(param_names, combo, strict=False))

            # Fork the base world for this parameter point
            fork = await self._world_service.fork_world(
                world_id,
                name=f"{config.experiment_name}:sweep:{combo}",
            )

            # Let the caller configure the fork with these params
            if setup_fn is not None:
                result = setup_fn(fork.world_id, params)
                if inspect.isawaitable(result):
                    await result

            # Run the loop on this fork
            sweep_config = AutoResearchConfig(
                experiment_name=f"{config.experiment_name}:{combo}",
                experiment_id=f"{config.experiment_id}:{combo}",
                evaluator_id=config.evaluator_id,
                rollout_contract_id=config.rollout_contract_id,
                episode_config=config.episode_config,
                num_episodes=config.num_episodes,
                parallel=config.parallel,
                max_iterations=config.max_iterations,
                improvement_threshold=config.improvement_threshold,
                destroy_forks_on_complete=config.destroy_forks_on_complete,
                record_to_ledger=config.record_to_ledger,
            )

            result = await self.run(fork.world_id, sweep_config, evaluator)
            results[combo] = result

            logger.info(
                "autoresearch sweep %s params=%s final_score=%.4f",
                config.experiment_name,
                params,
                result.final_score,
            )

        return results
