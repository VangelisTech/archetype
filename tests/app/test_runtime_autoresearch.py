# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Runtime exposure of autoresearch and evals.

The beginner path: ``world.autoresearch`` runs the gated optimization loop,
``runtime.attach`` loads existing worlds (episode saves, the lab ledger),
and ``world.grade`` composes the gated query with grader execution. All of
it routes through the command gate with the handle's ActorCtx.
"""

import pytest
from daft import col
from uuid_utils import uuid7

from archetype import ArchetypeRuntime, AutoResearchConfig, EvaluationResult
from archetype.app.auth.errors import GuardrailError
from archetype.app.auth.models import ActorCtx
from archetype.app.models import EpisodeConfig
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.experiments.components import Run, RunStatus

TARGET = 3.0


class Knob(Component):
    x: float = 0.0


def _config(name: str, max_iterations: int) -> AutoResearchConfig:
    return AutoResearchConfig(
        experiment_name=name,
        experiment_id=f"{name}-id",
        evaluator_id="knob-distance-v1",
        rollout_contract_id="knob-1ep-1step-v1",
        episode_config=EpisodeConfig(max_steps=1),
        num_episodes=1,
        max_iterations=max_iterations,
    )


@pytest.mark.asyncio
async def test_world_autoresearch_optimizes_and_ledgers(tmp_path):
    """Full runtime path: prepare forks candidates, evaluate loads episode
    saves via attach and grades their final state, the loop advances the
    head, and the lab world is attachable for audit."""
    async with ArchetypeRuntime() as runtime:
        base = runtime.world(
            "base", storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        )
        await base.spawn(Knob(x=0.0))
        await base.run(steps=1)

        async def prepare(ctx):
            fork = await base.fork(f"candidate-{ctx.iteration}")
            rows = (await fork.query(Knob)).to_pylist()
            await fork.update(int(rows[0]["entity_id"]), Knob(x=float(ctx.iteration)))
            await fork.run(steps=1)
            return fork.world_id

        async def evaluate(rollout) -> EvaluationResult:
            xs = []
            for ep in rollout.episodes:
                episode = runtime.attach(ep.world_id)
                final_tick = (await episode.info()).tick - 1

                def final_x(df, t=final_tick):
                    latest = df.where(col("tick") == t)
                    return latest.agg(col("knob__x").mean().alias("x")).to_pylist()[0]["x"]

                xs.append((await episode.grade(Knob, graders=[final_x]))[0])
            score = -sum((x - TARGET) ** 2 for x in xs) / len(xs)
            return EvaluationResult(score=score, evaluator="knob-distance-v1", evidence={"xs": xs})

        result = await base.autoresearch(
            _config("runtime-exp", max_iterations=4), evaluate, prepare_candidate=prepare
        )

        assert [it.score for it in result.iterations] == [-9.0, -4.0, -1.0, 0.0]
        assert result.final_score == 0.0
        assert result.improved

        # Episode worlds are kept by default (destroy_forks_on_complete=False):
        # the evaluator above could only have graded live saves.
        lab = runtime.attach(result.lab_world_id, name="lab")
        info = await lab.info()
        assert info.tick == 9, "genesis + RUNNING/terminal ticks for 4 attempts"
        attempts = (await lab.query(Run)).where(col("tick") == info.tick - 1).to_pylist()
        assert sorted(r["run__run_id"] for r in attempts) == [
            f"runtime-exp-id:iter{i}" for i in range(4)
        ]
        assert all(r["run__status"] == RunStatus.STOPPED.value for r in attempts)


@pytest.mark.asyncio
async def test_world_autoresearch_denied_below_operator(tmp_path):
    async with ArchetypeRuntime() as runtime:
        base = runtime.world(
            "base", storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        )
        await base.spawn(Knob(x=0.0))
        await base.run(steps=1)

        for role in ("viewer", "player"):
            handle = base.as_actor(ActorCtx(id=uuid7(), roles={role}))
            with pytest.raises(GuardrailError):
                await handle.autoresearch(
                    _config(f"denied-{role}", max_iterations=1), lambda _r: 1.0
                )


@pytest.mark.asyncio
async def test_attach_shutdown_does_not_destroy_the_world(tmp_path):
    async with ArchetypeRuntime() as runtime:
        base = runtime.world(
            "base", storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        )
        await base.spawn(Knob(x=1.0))
        await base.run(steps=1)

        attached = runtime.attach(base.world_id, name="alias")
        assert (await attached.info()).tick == 1
        await attached.shutdown()

        # The world survives: the attached handle never owned it.
        assert (await base.info()).tick == 1
        again = runtime.attach(base.world_id)
        assert (await again.info()).tick == 1


@pytest.mark.asyncio
async def test_grade_rejects_empty_graders(tmp_path):
    async with ArchetypeRuntime() as runtime:
        base = runtime.world(
            "base", storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        )
        await base.spawn(Knob(x=1.0))
        await base.run(steps=1)

        with pytest.raises(ValueError, match="at least one grader"):
            await base.grade(Knob, graders=[])


@pytest.mark.asyncio
async def test_attach_unknown_world_fails_on_first_operation():
    async with ArchetypeRuntime() as runtime:
        ghost = runtime.attach(uuid7(), name="ghost")
        with pytest.raises(LookupError):
            await ghost.info()
