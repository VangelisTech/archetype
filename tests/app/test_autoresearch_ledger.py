# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""AutoResearch ledger contracts.

The loop's own state lives on the ledger: a lab world whose tick 0 is the
genesis (Experiment + seed BranchHead as initial conditions) and whose
every subsequent tick is one iteration. The incumbent is read from the
ledger, never from an in-memory float — a second run of the same
experiment resumes from the last declared best.
"""

import json

import pytest

from archetype.app.autoresearch_service import AutoResearchConfig
from archetype.app.container import ServiceContainer
from archetype.app.models import EpisodeConfig
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.experiments.components import BranchHead, Experiment, Result, Run


class Tag(Component):
    label: str = ""


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


def _config(name: str, max_iterations: int) -> AutoResearchConfig:
    return AutoResearchConfig(
        experiment_name=name,
        episode_config=EpisodeConfig(max_steps=1),
        num_episodes=1,
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
    """Tick 0 = Experiment + seed BranchHead (initial conditions); each
    iteration appends one tick: Run + Result rows and any head advance."""
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
        # genesis tick + one tick per iteration
        assert lab.tick == 4

        # Genesis: Experiment row at tick 0, raw initial conditions
        exp_rows = (await lab.query_archetype(sig=(Experiment,), ticks=[0])).to_pylist()
        assert len(exp_rows) == 1
        assert exp_rows[0]["experiment__name"] == "exp"
        metadata = json.loads(exp_rows[0]["experiment__metadata_json"])
        assert metadata["base_world_id"] == str(base.world_id)

        # BranchHead history: seed at tick 0, advance on improvement only
        head_scores = []
        for t in range(4):
            rows = (await lab.query_archetype(sig=(BranchHead,), ticks=[t])).to_pylist()
            assert len(rows) == 1, f"exactly one active head at tick {t}"
            head_scores.append(json.loads(rows[0]["branchhead__descriptor_json"]).get("score"))
        assert head_scores == [None, 1.0, 1.0, 2.0], (
            f"head advances on improvement, holds otherwise: {head_scores}"
        )

        # Run + Result rows: one per iteration, scores preserved
        run_rows = (await lab.query_archetype(sig=(Run,), ticks=[lab.tick - 1])).to_pylist()
        assert sorted(r["run__run_id"] for r in run_rows) == [
            "exp:iter0",
            "exp:iter1",
            "exp:iter2",
        ]
        result_rows = (await lab.query_archetype(sig=(Result,), ticks=[lab.tick - 1])).to_pylist()
        outputs = sorted(
            (json.loads(r["result__outputs_json"]) for r in result_rows),
            key=lambda o: o["iteration"],
        )
        assert [o["score"] for o in outputs] == [1.0, 0.5, 2.0]
        assert [o["improved"] for o in outputs] == [True, False, True]
        assert all(o["episode_world_ids"] for o in outputs), "provenance: episode worlds recorded"
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
        )

        step = resumed.iterations[0]
        assert step.iteration == 3, "iteration numbering continues from the ledger"
        assert step.incumbent_score == 2.0, "incumbent read from the BranchHead row"
        assert step.improved is False, "1.5 does not beat the persisted 2.0"

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
            episode_config=EpisodeConfig(max_steps=1),
            num_episodes=1,
            max_iterations=1,
            record_to_ledger=False,
        )
        result = await c.autoresearch_service.run(base.world_id, config, _scripted_evaluator([1.0]))
        assert result.lab_world_id == ""
        with pytest.raises(KeyError):
            c.world_service.get_world_by_name("autoresearch:ephemeral")
    finally:
        await c.shutdown()
