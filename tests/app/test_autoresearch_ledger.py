# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""AutoResearch ledger contracts.

The loop's own state lives on the ledger: a lab world whose tick 0 is the
genesis (Experiment + seed BranchHead as initial conditions) and whose
every subsequent tick is one iteration. The incumbent is read from the
ledger, never from an in-memory float — a second run of the same
experiment resumes from the last declared best.
"""

import asyncio
import json
from pathlib import Path

import pytest
from uuid_utils import UUID

from archetype.app.container import ServiceContainer
from archetype.app.research.contracts import AutoResearchConfig, EvaluationResult
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.research import BranchHead, Experiment, Result, Run, RunStatus
from archetype.world.models import EpisodeConfig


class Tag(Component):
    label: str = ""


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


def _config(
    name: str,
    max_iterations: int,
    *,
    experiment_id: str | None = None,
    evaluator_id: str = "scripted-score-v1",
    rollout_contract_id: str = "single-step-rollout-v1",
    num_episodes: int = 1,
) -> AutoResearchConfig:
    return AutoResearchConfig(
        experiment_name=name,
        experiment_id=f"{name}-id" if experiment_id is None else experiment_id,
        evaluator_id=evaluator_id,
        rollout_contract_id=rollout_contract_id,
        episode_config=EpisodeConfig(max_steps=1),
        num_episodes=num_episodes,
        max_iterations=max_iterations,
    )


def _scripted_evaluator(scores: list[float]):
    """Evaluator returning pre-scripted scores in call order."""
    calls = iter(scores)

    def evaluate(_rollout) -> float:
        return next(calls)

    return evaluate


async def _base_world(c: ServiceContainer, tmp_path):
    base = await c.world_service.create_world(WorldConfig(name="base"), _storage(tmp_path))
    await c.mutation_service.create_entity(base.world_id, [Tag(label="seed")])
    await c.simulation_service.step(base.world_id, RunConfig())
    return base


@pytest.mark.asyncio
async def test_loop_records_genesis_and_iterations(tmp_path):
    """Each attempt records RUNNING before its terminal lifecycle tick."""
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)
        result = await c.autoresearch_service.run(
            base.world_id,
            _config("exp", max_iterations=3),
            _scripted_evaluator([1.0, 0.5, 2.0]),
        )

        assert result.lab_world_id, "loop must report its lab world"
        lab = c.world_service.get_world(result.lab_world_id)
        # genesis + RUNNING/terminal ticks for each iteration
        assert lab.tick == 7

        # Genesis: Experiment row at tick 0, raw initial conditions
        exp_rows = (await lab.query_archetype(sig=(Experiment,), ticks=[0])).to_pylist()
        assert len(exp_rows) == 1
        assert exp_rows[0]["experiment__name"] == "exp"
        metadata = json.loads(exp_rows[0]["experiment__metadata_json"])
        assert metadata["base_world_id"] == str(base.world_id)
        assert metadata["experiment_id"] == "exp-id"
        assert metadata["config_digest"]

        # RUNNING never advances the head; only a successful terminal tick can.
        head_scores = []
        for t in range(7):
            rows = (await lab.query_archetype(sig=(BranchHead,), ticks=[t])).to_pylist()
            assert len(rows) == 1, f"exactly one active head at tick {t}"
            head_scores.append(json.loads(rows[0]["branchhead__descriptor_json"]).get("score"))
        assert head_scores == [None, None, 1.0, 1.0, 1.0, 1.0, 2.0], (
            f"head advances on improvement, holds otherwise: {head_scores}"
        )

        # Run rows transition in place from RUNNING to STOPPED.
        for iteration in range(3):
            run_id = f"exp-id:iter{iteration}"
            running_rows = (
                await lab.query_archetype(sig=(Run,), ticks=[1 + 2 * iteration])
            ).to_pylist()
            running = next(r for r in running_rows if r["run__run_id"] == run_id)
            assert running["run__status"] == RunStatus.RUNNING.value

        run_rows = (await lab.query_archetype(sig=(Run,), ticks=[lab.tick - 1])).to_pylist()
        assert sorted(r["run__run_id"] for r in run_rows) == [
            "exp-id:iter0",
            "exp-id:iter1",
            "exp-id:iter2",
        ]
        assert all(r["run__status"] == RunStatus.STOPPED.value for r in run_rows)

        # Result rows preserve scores and use the configured evaluator id.
        result_rows = (await lab.query_archetype(sig=(Result,), ticks=[lab.tick - 1])).to_pylist()
        assert {r["result__evaluator"] for r in result_rows} == {"scripted-score-v1"}
        outputs = sorted(
            (json.loads(r["result__outputs_json"]) for r in result_rows),
            key=lambda o: o["iteration"],
        )
        assert [o["score"] for o in outputs] == [1.0, 0.5, 2.0]
        assert [o["improved"] for o in outputs] == [True, False, True]
        assert all(o["episode_world_ids"] for o in outputs), "provenance: episode worlds recorded"
        assert all(step.evaluation.evaluator == "scripted-score-v1" for step in result.iterations)
        assert result.initial_score == float("-inf")
        assert result.final_score == 2.0
        assert result.improved is True
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_loop_resumes_incumbent_from_ledger(tmp_path):
    """A second run of the same experiment reads the incumbent from the
    BranchHead row and continues iteration numbering from the lab tick."""
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)
        await c.autoresearch_service.run(
            base.world_id,
            _config("exp", max_iterations=3),
            _scripted_evaluator([1.0, 0.5, 2.0]),
        )

        resumed = await c.autoresearch_service.run(
            base.world_id,
            _config("exp", max_iterations=1),
            _scripted_evaluator([1.5]),
            lab_world_id=(c.world_service.get_world_by_name("autoresearch:exp-id").world_id),
        )

        step = resumed.iterations[0]
        assert step.iteration == 3, "iteration numbering continues from the ledger"
        assert step.incumbent_score == 2.0, "incumbent read from the BranchHead row"
        assert step.improved is False, "1.5 does not beat the persisted 2.0"
        assert resumed.initial_score == 2.0
        assert resumed.final_score == 2.0
        assert resumed.improved is False

        lab = c.world_service.get_world(resumed.lab_world_id)
        rows = (await lab.query_archetype(sig=(BranchHead,), ticks=[lab.tick - 1])).to_pylist()
        assert json.loads(rows[0]["branchhead__descriptor_json"])["score"] == 2.0
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_record_to_ledger_opt_out(tmp_path):
    """record_to_ledger=False keeps the old in-memory behavior."""
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)
        config = AutoResearchConfig(
            experiment_name="ephemeral",
            experiment_id="ephemeral-id",
            evaluator_id="scripted-score-v1",
            rollout_contract_id="single-step-rollout-v1",
            episode_config=EpisodeConfig(max_steps=1),
            num_episodes=1,
            max_iterations=1,
            record_to_ledger=False,
        )
        result = await c.autoresearch_service.run(base.world_id, config, _scripted_evaluator([1.0]))
        assert result.lab_world_id == ""
        with pytest.raises(KeyError):
            c.world_service.get_world_by_name("autoresearch:ephemeral-id")
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_no_ledger_baseline_is_first_score_not_neg_inf(tmp_path):
    """Without a ledger the baseline is the first evaluated score: a flat or
    declining search must not report aggregate improvement."""
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)

        def config(name: str) -> AutoResearchConfig:
            return AutoResearchConfig(
                experiment_name=name,
                experiment_id=f"{name}-id",
                evaluator_id="scripted-score-v1",
                rollout_contract_id="single-step-rollout-v1",
                episode_config=EpisodeConfig(max_steps=1),
                max_iterations=2,
                record_to_ledger=False,
            )

        flat = await c.autoresearch_service.run(
            base.world_id, config("flat"), _scripted_evaluator([1.0, 0.5])
        )
        assert flat.initial_score == 1.0
        assert flat.final_score == 1.0
        assert flat.improved is False, "nothing improved after the first candidate"

        rising = await c.autoresearch_service.run(
            base.world_id, config("rising"), _scripted_evaluator([1.0, 2.0])
        )
        assert rising.initial_score == 1.0
        assert rising.final_score == 2.0
        assert rising.improved is True
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_run_metadata_with_uuid_and_path_hashes_and_resumes(tmp_path):
    """RunConfig metadata with non-JSON-native but value-encodable types
    (uuid_utils UUID, Path) must hash deterministically and round-trip the
    persisted identity so an equal-valued config can resume."""
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)

        def config(max_iterations: int) -> AutoResearchConfig:
            # Fresh objects each call: identity must be value-based.
            return AutoResearchConfig(
                experiment_name="meta",
                experiment_id="meta-id",
                evaluator_id="scripted-score-v1",
                rollout_contract_id="single-step-rollout-v1",
                episode_config=EpisodeConfig(
                    max_steps=1,
                    run_config=RunConfig(
                        metadata={
                            "trace_id": UUID("019625d2-6e70-7000-8000-000000000000"),
                            "artifacts": Path("/tmp/meta-artifacts"),
                        }
                    ),
                ),
                max_iterations=max_iterations,
            )

        first = await c.autoresearch_service.run(
            base.world_id, config(max_iterations=1), _scripted_evaluator([1.0])
        )
        lab = c.world_service.get_world(first.lab_world_id)
        exp_rows = (await lab.query_archetype(sig=(Experiment,), ticks=[0])).to_pylist()
        stored = json.loads(exp_rows[0]["experiment__metadata_json"])
        assert stored["config"]["episode"]["run"]["metadata"] == {
            "trace_id": "019625d2-6e70-7000-8000-000000000000",
            "artifacts": "/tmp/meta-artifacts",
        }

        resumed = await c.autoresearch_service.run(
            base.world_id,
            config(max_iterations=1),
            _scripted_evaluator([2.0]),
            lab_world_id=first.lab_world_id,
        )
        assert resumed.iterations[0].iteration == 1, "equal-valued config resumes"
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_run_metadata_that_cannot_identify_the_experiment_fails_closed(tmp_path):
    """Unknown metadata types and non-finite floats cannot serve as a stable
    identity; the loop must fail before creating a lab world."""
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)

        def config(name: str, metadata: dict) -> AutoResearchConfig:
            return AutoResearchConfig(
                experiment_name=name,
                experiment_id=f"{name}-id",
                evaluator_id="scripted-score-v1",
                rollout_contract_id="single-step-rollout-v1",
                episode_config=EpisodeConfig(max_steps=1, run_config=RunConfig(metadata=metadata)),
                max_iterations=1,
            )

        with pytest.raises(ValueError, match="JSON-encodable"):
            await c.autoresearch_service.run(
                base.world_id, config("opaque", {"handle": object()}), _scripted_evaluator([1.0])
            )
        with pytest.raises(ValueError, match="strict-JSON"):
            await c.autoresearch_service.run(
                base.world_id, config("nonjson", {"x": float("nan")}), _scripted_evaluator([1.0])
            )
        for name in ("opaque", "nonjson"):
            with pytest.raises(KeyError):
                c.world_service.get_world_by_name(f"autoresearch:{name}-id")
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_candidate_preparer_can_select_a_distinct_world_per_iteration(tmp_path):
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)
        candidate_ids: list[str] = []

        async def prepare(context):
            candidate = await c.world_service.fork_world(
                base.world_id,
                name=f"candidate-{context.iteration}",
            )
            candidate_ids.append(str(candidate.world_id))
            return candidate.world_id

        scores = iter([1.0, 2.0])

        def evaluate(rollout) -> EvaluationResult:
            return EvaluationResult(
                score=next(scores),
                evaluator="candidate-score-v1",
                evidence={"candidate_world_id": str(rollout.base_world_id)},
                metadata={"contract": "candidate-world"},
            )

        result = await c.autoresearch_service.run(
            base.world_id,
            _config(
                "candidate-exp",
                max_iterations=2,
                evaluator_id="candidate-score-v1",
            ),
            evaluate,
            prepare_candidate=prepare,
        )

        assert len(set(candidate_ids)) == 2
        assert [str(step.rollout.base_world_id) for step in result.iterations] == candidate_ids
        assert all(step.evaluation.evaluator == "candidate-score-v1" for step in result.iterations)

        lab = c.world_service.get_world(result.lab_world_id)
        rows = (await lab.query_archetype(sig=(Result,), ticks=[lab.tick - 1])).to_pylist()
        outputs = [json.loads(row["result__outputs_json"]) for row in rows]
        assert {row["result__evaluator"] for row in rows} == {"candidate-score-v1"}
        assert {output["candidate_world_id"] for output in outputs} == set(candidate_ids)
        assert all(output["evidence"]["candidate_world_id"] in candidate_ids for output in outputs)
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_candidate_preparation_failure_records_crashed_terminal_state(tmp_path):
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)

        async def fail_preparation(_context):
            raise RuntimeError("candidate construction failed")

        config = _config("crash-exp", max_iterations=1)
        with pytest.raises(RuntimeError, match="candidate construction failed"):
            await c.autoresearch_service.run(
                base.world_id,
                config,
                _scripted_evaluator([1.0]),
                prepare_candidate=fail_preparation,
            )

        lab = c.world_service.get_world_by_name("autoresearch:crash-exp-id")
        assert lab.tick == 3  # genesis, RUNNING, CRASHED
        running_rows = (await lab.query_archetype(sig=(Run,), ticks=[1])).to_pylist()
        assert running_rows[0]["run__status"] == RunStatus.RUNNING.value
        terminal_rows = (await lab.query_archetype(sig=(Run,), ticks=[2])).to_pylist()
        assert terminal_rows[0]["run__status"] == RunStatus.CRASHED.value
        result_rows = (await lab.query_archetype(sig=(Result,), ticks=[2])).to_pylist()
        assert result_rows[0]["result__evaluator"] == "autoresearch:lifecycle"
        crash = json.loads(result_rows[0]["result__outputs_json"])
        assert crash["error_type"] == "builtins.RuntimeError"
        assert crash["candidate_world_id"] == str(base.world_id)
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_failure_after_candidate_selection_records_candidate_provenance(tmp_path):
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)
        candidate = await c.world_service.fork_world(base.world_id, name="failing-candidate")

        async def prepare(_context):
            return candidate.world_id

        def fail_evaluation(_rollout):
            raise RuntimeError("evaluation failed")

        with pytest.raises(RuntimeError, match="evaluation failed"):
            await c.autoresearch_service.run(
                base.world_id,
                _config("candidate-crash", max_iterations=1),
                fail_evaluation,
                prepare_candidate=prepare,
            )

        lab = c.world_service.get_world_by_name("autoresearch:candidate-crash-id")
        result_rows = (await lab.query_archetype(sig=(Result,), ticks=[lab.tick - 1])).to_pylist()
        crash = json.loads(result_rows[0]["result__outputs_json"])
        assert crash["candidate_world_id"] == str(candidate.world_id)
    finally:
        await c.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("score", [float("nan"), float("inf"), float("-inf")])
async def test_non_finite_evaluation_is_rejected_and_recorded_as_crashed(tmp_path, score):
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)
        config = _config("nonfinite", max_iterations=1)

        with pytest.raises(ValueError, match="score must be finite"):
            await c.autoresearch_service.run(base.world_id, config, _scripted_evaluator([score]))

        lab = c.world_service.get_world_by_name("autoresearch:nonfinite-id")
        rows = (await lab.query_archetype(sig=(Run,), ticks=[lab.tick - 1])).to_pylist()
        assert rows[0]["run__status"] == RunStatus.CRASHED.value
        heads = (await lab.query_archetype(sig=(BranchHead,), ticks=[lab.tick - 1])).to_pylist()
        assert json.loads(heads[0]["branchhead__descriptor_json"])["score"] is None
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_resume_rejects_experiment_config_identity_collision(tmp_path):
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)
        first = await c.autoresearch_service.run(
            base.world_id,
            _config("collision", max_iterations=1, num_episodes=1),
            _scripted_evaluator([1.0]),
        )

        with pytest.raises(ValueError, match="experiment identity collision"):
            await c.autoresearch_service.run(
                base.world_id,
                _config("collision", max_iterations=1, num_episodes=2),
                _scripted_evaluator([2.0]),
                lab_world_id=first.lab_world_id,
            )

        lab = c.world_service.get_world(first.lab_world_id)
        rows = (await lab.query_archetype(sig=(Run,), ticks=[lab.tick - 1])).to_pylist()
        assert len(rows) == 1, "collision must fail before a new attempt is recorded"
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_evaluator_contract_mismatch_crashes_without_advancing_head(tmp_path):
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)

        def mismatched_evaluator(_rollout) -> EvaluationResult:
            return EvaluationResult(score=2.0, evaluator="different-score-v2")

        with pytest.raises(ValueError, match="configured evaluator_id"):
            await c.autoresearch_service.run(
                base.world_id,
                _config("evaluator-mismatch", max_iterations=1),
                mismatched_evaluator,
            )

        lab = c.world_service.get_world_by_name("autoresearch:evaluator-mismatch-id")
        runs = (await lab.query_archetype(sig=(Run,), ticks=[lab.tick - 1])).to_pylist()
        assert runs[0]["run__status"] == RunStatus.CRASHED.value
        heads = (await lab.query_archetype(sig=(BranchHead,), ticks=[lab.tick - 1])).to_pylist()
        assert json.loads(heads[0]["branchhead__descriptor_json"])["score"] is None
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_resume_fails_closed_while_an_attempt_is_running(tmp_path):
    c = ServiceContainer()
    first_task = None
    release = asyncio.Event()
    try:
        base = await _base_world(c, tmp_path)
        entered = asyncio.Event()
        config = _config("active-attempt", max_iterations=1)

        async def block_candidate(_context):
            entered.set()
            await release.wait()

        first_task = asyncio.create_task(
            c.autoresearch_service.run(
                base.world_id,
                config,
                _scripted_evaluator([1.0]),
                prepare_candidate=block_candidate,
            )
        )
        await entered.wait()
        lab = c.world_service.get_world_by_name("autoresearch:active-attempt-id")

        with pytest.raises(RuntimeError, match="active attempt"):
            await c.autoresearch_service.run(
                base.world_id,
                config,
                _scripted_evaluator([2.0]),
                lab_world_id=lab.world_id,
            )

        release.set()
        await first_task
    finally:
        release.set()
        if first_task is not None and not first_task.done():
            await first_task
        await c.shutdown()


@pytest.mark.asyncio
async def test_resume_rejects_unknown_run_status(tmp_path):
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)
        completed = await c.autoresearch_service.run(
            base.world_id,
            _config("unknown-status", max_iterations=1),
            _scripted_evaluator([1.0]),
        )
        lab = c.world_service.get_world(completed.lab_world_id)
        rows = (await lab.query_archetype(sig=(Run,), ticks=[lab.tick - 1])).to_pylist()
        await lab.update_entity(
            int(rows[0]["entity_id"]),
            [
                Run(
                    run_id="unknown-status-id:iter0",
                    experiment_name="unknown-status",
                    status="foreign-status",
                    task="rollout",
                )
            ],
        )
        await lab.step(RunConfig())

        with pytest.raises(ValueError, match="unknown Run status"):
            await c.autoresearch_service.run(
                base.world_id,
                _config("unknown-status", max_iterations=1),
                _scripted_evaluator([2.0]),
                lab_world_id=lab.world_id,
            )
    finally:
        await c.shutdown()


@pytest.mark.asyncio
async def test_resume_rejects_non_contiguous_iteration_history(tmp_path):
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)
        completed = await c.autoresearch_service.run(
            base.world_id,
            _config("gapped-history", max_iterations=1),
            _scripted_evaluator([1.0]),
        )
        lab = c.world_service.get_world(completed.lab_world_id)
        await lab.create_entity(
            [
                Run(
                    run_id="gapped-history-id:iter2",
                    experiment_name="gapped-history",
                    status=RunStatus.CRASHED.value,
                    task="rollout",
                )
            ]
        )
        await lab.step(RunConfig())

        with pytest.raises(ValueError, match="duplicate or non-contiguous"):
            await c.autoresearch_service.run(
                base.world_id,
                _config("gapped-history", max_iterations=1),
                _scripted_evaluator([2.0]),
                lab_world_id=lab.world_id,
            )
    finally:
        await c.shutdown()


def test_experiment_identity_and_threshold_fail_closed():
    with pytest.raises(ValueError, match="experiment_id"):
        _config("invalid", max_iterations=1, experiment_id="")

    with pytest.raises(ValueError, match="improvement_threshold"):
        AutoResearchConfig(
            experiment_name="invalid",
            experiment_id="invalid-id",
            evaluator_id="scripted-score-v1",
            rollout_contract_id="single-step-rollout-v1",
            improvement_threshold=float("nan"),
        )

    with pytest.raises(ValueError, match="evaluator_id"):
        AutoResearchConfig(
            experiment_name="invalid",
            experiment_id="invalid-id",
            evaluator_id="",
            rollout_contract_id="single-step-rollout-v1",
        )

    with pytest.raises(ValueError, match="rollout_contract_id"):
        AutoResearchConfig(
            experiment_name="invalid",
            experiment_id="invalid-id",
            evaluator_id="scripted-score-v1",
            rollout_contract_id="",
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"num_episodes": 0}, "num_episodes"),
        ({"max_iterations": 0}, "max_iterations"),
        ({"improvement_threshold": -0.1}, "improvement_threshold"),
    ],
)
def test_empty_or_regressive_search_config_fails_closed(overrides, message):
    values = {
        "experiment_name": "invalid",
        "experiment_id": "invalid-id",
        "evaluator_id": "scripted-score-v1",
        "rollout_contract_id": "single-step-rollout-v1",
    }
    values.update(overrides)
    with pytest.raises(ValueError, match=message):
        AutoResearchConfig(**values)


@pytest.mark.asyncio
async def test_termination_contract_is_part_of_experiment_identity(tmp_path):
    """terminal_field/terminal_all are semantic episode-termination fields:
    an experiment resumed with a different value-based termination contract
    must fail closed instead of comparing scores across different rollout
    semantics."""
    c = ServiceContainer()
    try:
        base = await _base_world(c, tmp_path)

        def config(terminal_field: str | None) -> AutoResearchConfig:
            return AutoResearchConfig(
                experiment_name="term",
                experiment_id="term-id",
                evaluator_id="scripted-score-v1",
                rollout_contract_id="single-step-rollout-v1",
                episode_config=EpisodeConfig(
                    max_steps=1, terminal_component=Tag, terminal_field=terminal_field
                ),
                max_iterations=1,
            )

        first = await c.autoresearch_service.run(
            base.world_id, config(terminal_field=None), _scripted_evaluator([1.0])
        )

        with pytest.raises(ValueError, match="experiment identity collision"):
            await c.autoresearch_service.run(
                base.world_id,
                config(terminal_field="label"),
                _scripted_evaluator([2.0]),
                lab_world_id=first.lab_world_id,
            )
    finally:
        await c.shutdown()
