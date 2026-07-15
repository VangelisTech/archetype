# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fenced mutable cold resume (issue #273, A1-resume).

A live, writable world is reconstructed from rows + catalog in a process
sharing nothing with the previous writer but the storage config. Resume
restores what rows can prove (tick head, entity directory, lineage), fences
out the previous writer, and refuses loudly whenever it cannot reconstruct
faithfully. Code is not rows: component classes must be imported;
processors/resources/hooks reattach explicitly.
"""

import json
import subprocess
import sys
import textwrap

import pytest
from uuid_utils import uuid7

from archetype.app._catalog import SqliteControlCatalog, WorldRecord
from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import StaleWriterError

pytestmark = pytest.mark.asyncio


class Score(Component):
    points: float = 0.0


class Flag(Component):
    label: str = ""


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(uri=str(tmp_path / "store"), namespace="ns")


async def _seed_world(c: ServiceContainer, storage) -> tuple[str, str, int, int]:
    """Create a world with two archetypes, three ticks, one despawn.

    Returns (world_id, run_id, live_entity_id, despawned_entity_id).
    """
    world = await c.world_service.create_world(WorldConfig(name="seed"), storage)
    e1 = await c.mutation_service.create_entity(world.world_id, [Score(points=1.0)])
    e2 = await c.mutation_service.create_entity(
        world.world_id, [Score(points=2.0), Flag(label="x")]
    )
    await c.simulation_service.step(world.world_id, RunConfig())
    await c.simulation_service.step(world.world_id, RunConfig())
    await c.mutation_service.remove_entity(world.world_id, e2)
    await c.simulation_service.step(world.world_id, RunConfig())
    return str(world.world_id), str(world.run_id), int(e1), int(e2)


async def test_resume_restores_tick_entities_and_continues(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        wid, rid, e1, e2 = await _seed_world(c, storage)
    finally:
        await c.shutdown()

    fresh = ServiceContainer()
    try:
        world = await fresh.world_service.open_world_mutable(storage, wid)
        assert world.tick == 3, "resume tick = last published tick + 1"
        assert str(world.run_id) == rid, "run identity continues"
        assert set(world.entity2sig) == {e1}, "despawned entities stay dead"
        # Identity is the schema: the resolved class may be a fingerprint-
        # identical twin from another imported module; the table id is the
        # honest comparison.
        from archetype.core.archetype import Archetype

        assert Archetype.get_name(world.entity2sig[e1]) == Archetype.get_name((Score,))
        assert world.next_entity_id > max(e1, e2), "entity ids are never reused"
        assert world.commit_coordinator is not None and world.commit_coordinator.epoch == 2

        # The resumed world is live and steps under the new epoch.
        e3 = await fresh.mutation_service.create_entity(wid, [Score(points=9.0)])
        assert e3 > max(e1, e2)
        await fresh.simulation_service.step(wid, RunConfig())
        df = await fresh.query_service.query_components([Score], wid, rid, storage, ticks=[3])
        points = sorted(r["score__points"] for r in df.to_pylist())
        assert points == [1.0, 9.0], "continued history unions old and new entities"
    finally:
        await fresh.shutdown()


async def test_resume_fences_out_previous_writer(tmp_path):
    a = ServiceContainer()
    b = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world_a = await a.world_service.create_world(WorldConfig(name="w"), storage)
        await a.mutation_service.create_entity(world_a.world_id, [Score(points=1.0)])
        await a.simulation_service.step(world_a.world_id, RunConfig())
        wid = str(world_a.world_id)

        # A second container (a second process, logically) resumes the world.
        world_b = await b.world_service.open_world_mutable(storage, wid)
        assert world_b.commit_coordinator.epoch == 2

        # The original writer is now stale: its publish fails closed.
        with pytest.raises((StaleWriterError, RuntimeError)):
            await a.simulation_service.step(wid, RunConfig())
        assert world_a.tick == 1, "stale writer must not advance"

        # The new writer owns the world.
        await b.simulation_service.step(wid, RunConfig())
        assert world_b.tick == 2
    finally:
        await a.shutdown()
        await b.shutdown()


async def test_resume_after_crash_resumes_at_last_visible_tick(tmp_path, monkeypatch):
    """Rows from a crashed, unpublished attempt must not advance the resume
    point: manifests, never rows, decide the tick head."""
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage)
        await c.mutation_service.create_entity(world.world_id, [Score(points=1.0)])
        await c.simulation_service.step(world.world_id, RunConfig())  # tick 0 published
        wid, rid = str(world.world_id), str(world.run_id)

        async def _crash(self, *args, **kwargs):
            raise RuntimeError("injected crash before head publish")

        monkeypatch.setattr(SqliteControlCatalog, "publish_manifest", _crash)
        with pytest.raises(RuntimeError, match="injected crash"):
            await c.simulation_service.step(wid, RunConfig())  # tick 1 rows, no manifest
        monkeypatch.undo()
    finally:
        await c.shutdown()

    fresh = ServiceContainer()
    try:
        resumed = await fresh.world_service.open_world_mutable(storage, wid)
        assert resumed.tick == 1, "unpublished tick-1 rows must not advance the head"
        await fresh.simulation_service.step(wid, RunConfig())
        df = await fresh.query_service.query_components([Score], wid, rid, storage, ticks=[1])
        assert len(df.to_pylist()) == 1, "exactly one visible attempt at the retried tick"
    finally:
        await fresh.shutdown()


async def test_resume_restores_fork_lineage(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        base = await c.world_service.create_world(WorldConfig(name="base"), storage)
        await c.mutation_service.create_entity(base.world_id, [Score(points=5.0)])
        await c.simulation_service.step(base.world_id, RunConfig())
        fork = await c.world_service.fork_world(base.world_id, name="branch")
        await c.simulation_service.step(fork.world_id, RunConfig())
        fid = str(fork.world_id)
        expected_lineage = list(fork.lineage)
        assert expected_lineage, "fork must carry lineage"
    finally:
        await c.shutdown()

    fresh = ServiceContainer()
    try:
        resumed = await fresh.world_service.open_world_mutable(storage, fid)
        assert resumed.lineage == expected_lineage, "ancestor segments restored"
        # Pre-fork history still resolves through the ancestor's run.
        df = await resumed.query_archetype((Score,), ticks=[0])
        rows = df.to_pylist()
        assert len(rows) == 1 and rows[0]["score__points"] == 5.0
    finally:
        await fresh.shutdown()


async def test_resume_refusals(tmp_path):
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world = await c.world_service.create_world(WorldConfig(name="w"), storage)
        await c.mutation_service.create_entity(world.world_id, [Score(points=1.0)])
        await c.simulation_service.step(world.world_id, RunConfig())
        wid = str(world.world_id)

        # Already live in this process.
        with pytest.raises(RuntimeError, match="already live"):
            await c.world_service.open_world_mutable(storage, wid)

        # Unrecorded world.
        with pytest.raises(KeyError):
            await c.world_service.open_world_mutable(storage, str(uuid7()))

        # Destroyed world: queryable, not resumable.
        await c.world_service.destroy_world(wid)
    finally:
        await c.shutdown()

    fresh = ServiceContainer()
    try:
        with pytest.raises(RuntimeError, match="destroyed"):
            await fresh.world_service.open_world_mutable(storage, wid)

        # Fork record without lineage rows = detectable corruption.
        catalog = fresh.storage_service.get_control_catalog(storage)
        orphan = str(uuid7())
        await catalog.register_world(
            WorldRecord(
                world_id=orphan,
                name=None,
                run_id=str(uuid7()),
                parent_world_id=str(uuid7()),
                status="active",
                tick_head=0,
            )
        )
        with pytest.raises(RuntimeError, match="lineage"):
            await fresh.world_service.open_world_mutable(storage, orphan)
    finally:
        await fresh.shutdown()


async def test_resume_requires_component_classes(tmp_path):
    """A child process writes an archetype whose class the parent never
    defines; resume must refuse loudly, naming the missing class."""
    script = textwrap.dedent(
        """
        import asyncio, json, sys

        from archetype.app.container import ServiceContainer
        from archetype.core.component import Component
        from archetype.core.config import RunConfig, StorageConfig, WorldConfig

        class Score(Component):
            points: float = 0.0

        class OnlyInChild(Component):
            secret: str = ""

        async def main(uri):
            c = ServiceContainer()
            try:
                storage = StorageConfig(uri=uri, namespace="ns")
                world = await c.world_service.create_world(WorldConfig(name="child"), storage)
                await c.mutation_service.create_entity(
                    world.world_id, [Score(points=1.0), OnlyInChild(secret="s")]
                )
                await c.simulation_service.step(world.world_id, RunConfig())
                print(json.dumps({"world_id": str(world.world_id)}))
            finally:
                await c.shutdown()

        asyncio.run(main(sys.argv[1]))
        """
    )
    uri = str(tmp_path / "store")
    proc = subprocess.run(
        [sys.executable, "-c", script, uri],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert proc.returncode == 0, proc.stderr
    wid = json.loads(proc.stdout.strip().splitlines()[-1])["world_id"]

    c = ServiceContainer()
    try:
        storage = StorageConfig(uri=uri, namespace="ns")
        with pytest.raises(RuntimeError, match="OnlyInChild"):
            await c.world_service.open_world_mutable(storage, wid)
    finally:
        await c.shutdown()


async def test_autoresearch_continues_across_processes(tmp_path):
    """Flagship acceptance (issue #273): a child process runs an AutoResearch
    experiment; a fresh process resumes base + lab from rows alone and the
    loop CONTINUES — attempt indices contiguous, incumbent preserved."""
    script = textwrap.dedent(
        """
        import asyncio, json, sys

        from daft import col

        from archetype import ArchetypeRuntime, AutoResearchConfig, EvaluationResult
        from archetype.app.models import EpisodeConfig
        from archetype.core.component import Component
        from archetype.core.config import StorageConfig

        TARGET = 3.0

        class Knob(Component):
            x: float = 0.0

        def config(max_iterations):
            return AutoResearchConfig(
                experiment_name="resume-exp",
                experiment_id="resume-exp-id",
                evaluator_id="knob-distance-v1",
                rollout_contract_id="knob-1ep-1step-v1",
                episode_config=EpisodeConfig(max_steps=1),
                num_episodes=1,
                max_iterations=max_iterations,
            )

        async def main(uri):
            async with ArchetypeRuntime() as runtime:
                base = runtime.world(
                    "base", storage=StorageConfig(uri=uri, namespace="ns")
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
                    return EvaluationResult(
                        score=score, evaluator="knob-distance-v1", evidence={"xs": xs}
                    )

                result = await base.autoresearch(
                    config(2), evaluate, prepare_candidate=prepare
                )
                print(json.dumps({
                    "base_world_id": base.world_id,
                    "lab_world_id": result.lab_world_id,
                    "scores": [it.score for it in result.iterations],
                }))

        asyncio.run(main(sys.argv[1]))
        """
    )
    uri = str(tmp_path / "store")
    proc = subprocess.run(
        [sys.executable, "-c", script, uri],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr
    child = json.loads(proc.stdout.strip().splitlines()[-1])
    assert child["scores"] == [-9.0, -4.0], "child ran iterations 0 and 1"

    from daft import col

    from archetype import ArchetypeRuntime, AutoResearchConfig, EvaluationResult
    from archetype.app.models import EpisodeConfig

    class Knob(Component):
        x: float = 0.0

    target = 3.0
    async with ArchetypeRuntime() as runtime:
        storage = StorageConfig(uri=uri, namespace="ns")
        base = await runtime.resume(child["base_world_id"], storage=storage)
        await runtime.resume(child["lab_world_id"], storage=storage, name="lab")

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
            score = -sum((x - target) ** 2 for x in xs) / len(xs)
            return EvaluationResult(score=score, evaluator="knob-distance-v1", evidence={"xs": xs})

        config = AutoResearchConfig(
            experiment_name="resume-exp",
            experiment_id="resume-exp-id",
            evaluator_id="knob-distance-v1",
            rollout_contract_id="knob-1ep-1step-v1",
            episode_config=EpisodeConfig(max_steps=1),
            num_episodes=1,
            max_iterations=2,  # the per-invocation budget: two MORE attempts
        )
        result = await base.autoresearch(
            config,
            evaluate,
            prepare_candidate=prepare,
            lab_world_id=child["lab_world_id"],
        )
        assert [it.iteration for it in result.iterations] == [2, 3], (
            "the resumed loop continues where the child stopped"
        )
        assert [it.score for it in result.iterations] == [-1.0, 0.0]
        assert result.final_score == 0.0 and result.improved, "incumbent carried across processes"


async def test_fork_resumed_before_first_step_inherits_snapshot(tmp_path):
    """A fork registers and persists lineage at creation, possibly before
    writing any rows of its own. Resume must seed the entity directory from
    the ancestor segments — an unstepped fork is its snapshot (Codex P1)."""
    c = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        base = await c.world_service.create_world(WorldConfig(name="base"), storage)
        e1 = await c.mutation_service.create_entity(base.world_id, [Score(points=5.0)])
        await c.simulation_service.step(base.world_id, RunConfig())
        fork = await c.world_service.fork_world(base.world_id, name="unstepped")
        fid, fork_tick = str(fork.world_id), int(fork.tick)
    finally:
        await c.shutdown()

    fresh = ServiceContainer()
    try:
        resumed = await fresh.world_service.open_world_mutable(storage, fid)
        assert resumed.tick == fork_tick, "unstepped fork resumes at the fork point"
        assert set(resumed.entity2sig) == {e1}, "inherited entities restored from lineage"
        assert resumed.next_entity_id > e1, "counter accounts for inherited ids"

        # The resumed fork is a working world: step and read continuity.
        await fresh.simulation_service.step(fid, RunConfig())
        df = await resumed.query_archetype((Score,), ticks=[fork_tick])
        rows = df.to_pylist()
        assert len(rows) == 1 and rows[0]["score__points"] == 5.0
    finally:
        await fresh.shutdown()


async def test_resume_snapshot_taken_after_fence_beats_racing_publisher(tmp_path):
    """A publish that lands between a pre-fence read and the fence must not
    strand the resumed world behind the true head: the authoritative
    snapshot is taken AFTER fencing, when the head can no longer move
    (Codex P1)."""
    a = ServiceContainer()
    b = ServiceContainer()
    try:
        storage = _storage(tmp_path)
        world_a = await a.world_service.create_world(WorldConfig(name="w"), storage)
        await a.mutation_service.create_entity(world_a.world_id, [Score(points=1.0)])
        await a.simulation_service.step(world_a.world_id, RunConfig())  # tick 0
        wid = str(world_a.world_id)

        # Interleave: the incumbent publishes tick 1 exactly between B's
        # validation pass and its fence acquisition.
        real_acquire = SqliteControlCatalog.acquire_fence
        fired = False

        async def racing_acquire(self, world_id, holder):
            nonlocal fired
            if not fired and world_id == wid:
                fired = True
                await a.simulation_service.step(wid, RunConfig())  # tick 1 lands
            return await real_acquire(self, world_id, holder)

        SqliteControlCatalog.acquire_fence = racing_acquire
        try:
            resumed = await b.world_service.open_world_mutable(storage, wid)
        finally:
            SqliteControlCatalog.acquire_fence = real_acquire

        assert resumed.tick == 2, (
            f"snapshot must include the racing publish (tick 1); resumed at {resumed.tick}"
        )
        await b.simulation_service.step(wid, RunConfig())  # tick 2 under epoch 2
        df = await b.query_service.query_components([Score], wid, str(resumed.run_id), storage)
        assert {r["tick"] for r in df.to_pylist()} == {0, 1, 2}, "no tick lost, none doubled"
    finally:
        await a.shutdown()
        await b.shutdown()
