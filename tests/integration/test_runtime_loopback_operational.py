# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Installed-source and installed-wheel runtime loopback evidence."""

from __future__ import annotations

import asyncio
import gc
import json
import os
import weakref
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import BaseModel
from uuid_utils import uuid7

from archetype import ArchetypeRuntime
from archetype.api.app import create_app
from archetype.api.deps import get_dispatcher
from archetype.artifacts.models import ArtifactSource, IngestArtifacts, QueryArtifacts
from archetype.commands.models import AccessSummary, ActorCtx, DeferredItem, DurableOptions
from archetype.core.config import StorageConfig
from archetype.errors import RuntimeShutdownError
from archetype.evaluation.contracts import GraderContract
from archetype.evaluation.models import Evaluate, RunGraders
from archetype.missions.components import Mission
from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    CommandValidator,
    SubmittedMission,
)
from archetype.missions.models import RestoreMissionSandbox, RunMission, SubmitMission
from archetype.missions.sandboxes import CheckpointRef
from archetype.missions.trajectories import ClaudeTranscriptSource, TrajectorySelection
from archetype.missions.trajectories.models import (
    GradeTrajectory,
    IngestClaudeTranscript,
    QueryTrajectory,
    QueryTranscriptRows,
)
from archetype.physical_ai.models import (
    HostedEpisodeRequest,
    ModalHostedEpisodeConfig,
    RunHostedEpisode,
)
from archetype.research.models import AutoResearch, AutoResearchConfig
from archetype.runtime_resources import RuntimeCloseState, RuntimeResources
from scripts.run_runtime_loopback import (
    RECEIPT_FILENAME,
    RECEIPT_SCHEMA,
    run_runtime_loopback,
)

pytestmark = pytest.mark.integration

_PULL_FORWARD_MODELS: tuple[type[BaseModel], ...] = (
    IngestArtifacts,
    QueryArtifacts,
    RunGraders,
    Evaluate,
    AutoResearch,
    RunHostedEpisode,
    IngestClaudeTranscript,
    QueryTranscriptRows,
    QueryTrajectory,
    GradeTrajectory,
    SubmitMission,
    RunMission,
    RestoreMissionSandbox,
)
_ACTOR_AWARE_NAMES = frozenset(
    {
        "autoresearch",
        "evaluate",
        "ingest_artifacts",
        "query_artifacts",
    }
)


def _operation(model: type[BaseModel], *, world_id: object) -> BaseModel:
    name = str(model.model_fields["operation"].default)
    return model.model_construct(operation=name, world_id=world_id)


def _receipt_workspace(tmp_path: Path) -> tuple[Path, bool]:
    storage_uri = os.environ.get("ARCHETYPE_OPERATIONAL_STORAGE_URI", "").strip()
    if storage_uri:
        return Path(storage_uri).resolve().parent, True
    return tmp_path / "runtime-loopback", False


def test_shipped_server_cli_receipt_is_complete_bounded_and_redacted(tmp_path: Path) -> None:
    """The source process leaves one semantic receipt for the oracle process."""

    workspace, externally_run = _receipt_workspace(tmp_path)
    path = workspace / RECEIPT_FILENAME
    if externally_run:
        assert path.is_file(), "runtime loopback source command did not retain its receipt"
        receipt = json.loads(path.read_text(encoding="utf-8"))
    else:
        receipt = run_runtime_loopback(workspace)

    routes = cast(dict[str, object], receipt["routes"])
    spawned_entity_id = routes["spawned_entity_id"]
    query_rows = routes["query_rows"]
    history_rows = routes["history_rows"]
    assert isinstance(spawned_entity_id, int)
    assert isinstance(query_rows, int)
    assert isinstance(history_rows, int)
    assert receipt == {
        "schema": RECEIPT_SCHEMA,
        "transport": {
            "host": "127.0.0.1",
            "dynamic_port": True,
            "shipped_server": True,
            "shipped_cli_http": True,
        },
        "routes": {
            "create": True,
            "entity_spawn": True,
            "step": True,
            "run": True,
            "query": True,
            "history": True,
            "fork": True,
            "fork_readable": True,
            "destroy": True,
            "post_destroy_not_found": True,
            "history_semantic": True,
            "spawned_entity_id": spawned_entity_id,
            "query_rows": query_rows,
            "history_rows": history_rows,
        },
        "cleanup": {
            "server_process": "closed",
            "server_exit": "graceful",
            "listening_socket": "closed",
            "provider_children": 0,
            "workspace": "runner_owned",
        },
    }
    assert query_rows >= 1
    assert history_rows >= 1

    encoded = json.dumps(receipt, allow_nan=False, separators=(",", ":"), sort_keys=True)
    assert len(encoded.encode("utf-8")) <= 4096
    assert str(workspace) not in encoded
    for forbidden in (
        "provider close unavailable",
        "private provider",
        "secret",
        "traceback",
        "exception",
    ):
        assert forbidden not in encoded.lower()


@pytest.mark.asyncio
async def test_trusted_runtime_reaches_all_thirteen_models_and_reserved_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The public runtime constructs every pull-forward plus real reserved spawn."""

    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "catalog"))
    runtime = ArchetypeRuntime()
    resources = runtime._resources
    world = runtime.world("runtime-loopback-reserved", storage=tmp_path / "reserved-store")
    reservation = world._reservation
    assert reservation is not None
    captured: list[BaseModel] = []
    actor_calls: list[ActorCtx] = []
    try:
        (entity_id,) = await world.reserve_ids(1)
        await world.spawn_reserved(
            entity_id,
            Mission(
                name="runtime-loopback",
                repository="local",
                branch="main",
            ),
        )
        await world.step()
        rows = (await world.query(Mission, entity_ids=[entity_id])).collect().to_pylist()
        assert len(rows) == 1
        assert rows[0]["entity_id"] == entity_id

        dispatcher = resources.dispatcher
        registry = dispatcher._registry
        results: dict[str, object] = {
            str(model.model_fields["operation"].default): object() for model in _PULL_FORWARD_MODELS
        }
        submitted = SubmittedMission(
            mission_id=17,
            task_ids=(("implementation", 18),),
            episode_id="mission-episode-loopback",
            repository="repo",
            branch="branch",
        )
        results["submit_mission"] = submitted

        def handler_for(name: str):
            async def handler(operation: BaseModel) -> object:
                captured.append(operation)
                return results[name]

            return handler

        for model in _PULL_FORWARD_MODELS:
            name = str(model.model_fields["operation"].default)
            spec = registry.resolve_name(name)
            replacement = replace(spec, handler=handler_for(name))
            monkeypatch.setitem(registry._by_name, name, replacement)
            monkeypatch.setitem(registry._by_model, model, replacement)

        async def forbidden_apply_as(actor: ActorCtx, _operation: BaseModel) -> None:
            actor_calls.append(actor)
            raise AssertionError("trusted runtime invented actor-aware dispatch")

        monkeypatch.setattr(dispatcher, "apply_as", forbidden_apply_as)

        artifact = ArtifactSource(source_uri=str(tmp_path / "artifact.txt"))
        transcript = ClaudeTranscriptSource(
            tmp_path / "session.jsonl",
            project="runtime-loopback",
            session_id="session",
        )
        storage = StorageConfig(
            uri=str(tmp_path / "pull-forward-store"),
            namespace="runtime_loopback_pull_forward",
        )

        def grader(_frame: object) -> object:
            return object()

        def evaluator(_rollout: object) -> float:
            return 1.0

        def prepare_candidate(_world: object) -> object:
            return object()

        contract = GraderContract(
            grader_id="runtime-loopback",
            implementation_version="1",
        )
        research = AutoResearchConfig(
            experiment_name="runtime-loopback",
            experiment_id="runtime-loopback-1",
            evaluator_id="evaluator-1",
            rollout_contract_id="rollout-1",
            max_iterations=1,
            num_episodes=1,
        )
        physical = HostedEpisodeRequest(
            trial_id=0,
            suite="suite",
            task_id=1,
            seed=1,
            instruction="reach",
            max_transitions=1,
            environment_id="environment@v1",
            policy_id="policy@v1",
        )
        provider = ModalHostedEpisodeConfig(
            workspace_name="workspace",
            environment_name="environment",
            app_name="app",
            function_name="function",
            result_dict_name="results",
            result_volume_name="values",
        )
        selection = TrajectorySelection(episode_ids=("episode-1",))
        await world.ingest_artifacts(artifact)
        await world.artifacts()
        await world.grade(Mission, graders=(grader,))
        await world.evaluate(
            Mission,
            contract=contract,
            grader=grader,
            evaluation_id="evaluation-1",
        )
        await world.autoresearch(
            research,
            evaluator,
            prepare_candidate=prepare_candidate,
            lab_world_id="lab-world",
        )
        await world.run_hosted_episode(
            [physical],
            provider=provider,
            activity_id="activity-1",
        )
        await world.ingest_claude_transcript(transcript)
        await world.transcript_rows()
        await world.query_trajectory(Mission, selection=selection)
        await world.grade_trajectory(
            Mission,
            graders=(grader,),
            selection=selection,
        )

        mission_config = AgentMissionConfig(
            sandbox_backend=cast(Any, object()),
            sandbox_environment="runtime-loopback:v1",
        )
        missions = runtime.missions(
            "runtime-loopback-mission",
            config=mission_config,
            storage=storage,
        )
        task = AgentTask(
            "implementation",
            "Implement the requested change.",
            (
                CommandValidator(
                    "tests",
                    ("pytest", "-q"),
                ),
            ),
        )
        checkpoint = CheckpointRef(
            provider="runtime-loopback",
            checkpoint_id="checkpoint-1",
            uri="runtime-loopback://checkpoint-1",
            created_at_ms=1,
        )
        assert (
            await missions.submit(
                repository="repo",
                branch="branch",
                tasks=(task,),
            )
            is submitted
        )
        await missions.run(submitted, max_ticks=1)
        await missions.restore_sandbox(submitted, checkpoint)

        assert len(captured) == 13
        assert {type(operation) for operation in captured} == set(_PULL_FORWARD_MODELS)
        assert all(
            sum(type(operation) is model for operation in captured) == 1
            for model in _PULL_FORWARD_MODELS
        )
        assert {cast(Any, operation).operation for operation in captured} == {
            str(model.model_fields["operation"].default) for model in _PULL_FORWARD_MODELS
        }
        assert actor_calls == []
    finally:
        await runtime.shutdown()

    assert resources.close_state is RuntimeCloseState.CLOSED
    assert reservation.released
    assert resources._owners == {}


@pytest.mark.asyncio
async def test_actor_aware_matrix_and_deferred_paths_reject_before_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lifespan dispatcher allows exactly four and defers none."""

    monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "api-catalog"))
    app = create_app()
    effects: list[str] = []
    scheduler_effects: list[str] = []
    evidence: list[AccessSummary] = []

    async def record_handler(operation: BaseModel) -> str:
        name = str(cast(Any, operation).operation)
        effects.append(name)
        return name

    async def forbidden_scheduler(*_args: object, **_kwargs: object) -> None:
        scheduler_effects.append("scheduler")
        raise AssertionError("direct-only operation reached durable admission")

    async def record_access(row: AccessSummary) -> None:
        evidence.append(row)

    async with app.router.lifespan_context(app):
        resources = app.state.resources
        request = SimpleNamespace(app=app)
        dispatcher = await get_dispatcher(cast(Any, request))
        assert dispatcher is resources.dispatcher
        registry = dispatcher._registry
        specs = {
            model: registry.resolve_name(str(model.model_fields["operation"].default))
            for model in _PULL_FORWARD_MODELS
        }
        assert {spec.name for spec in specs.values() if spec.untrusted} == _ACTOR_AWARE_NAMES
        assert all(spec.trusted and spec.durable is None for spec in specs.values())

        monkeypatch.setattr(dispatcher, "_record_access", record_access)
        monkeypatch.setattr(dispatcher, "_target_tick_for_world", lambda _world_id: 0)
        monkeypatch.setattr(dispatcher._scheduler, "admit", forbidden_scheduler)
        monkeypatch.setattr(dispatcher._scheduler, "admit_batch", forbidden_scheduler)
        for model, spec in specs.items():
            replacement = replace(
                spec,
                handler=record_handler,
                summarize=lambda operation: {
                    "operation": str(cast(Any, operation).operation),
                    "world_id": str(cast(Any, operation).world_id),
                },
                token_cost=0,
            )
            monkeypatch.setitem(registry._by_name, spec.name, replacement)
            monkeypatch.setitem(registry._by_model, model, replacement)
            specs[model] = replacement

        operations = {
            model: _operation(model, world_id="runtime-loopback-world")
            for model in _PULL_FORWARD_MODELS
        }
        allowed_roles = {
            "autoresearch": "operator",
            "evaluate": "operator",
            "ingest_artifacts": "operator",
            "query_artifacts": "viewer",
        }
        for model, spec in specs.items():
            if spec.name not in _ACTOR_AWARE_NAMES:
                continue
            actor = ActorCtx(id=uuid7(), roles={allowed_roles[spec.name]})
            assert await dispatcher.apply_as(actor, operations[model]) == spec.name

        assert sorted(effects) == sorted(_ACTOR_AWARE_NAMES)
        successful_evidence = [row for row in evidence if row.outcome == "succeeded"]
        assert len(successful_evidence) == 4
        assert {row.operation for row in successful_evidence} == _ACTOR_AWARE_NAMES

        effects_before_rejection = tuple(effects)
        evidence_before_role_denial = tuple(evidence)
        for model, spec in specs.items():
            if spec.name not in _ACTOR_AWARE_NAMES:
                continue
            denied_role = "unknown" if spec.name == "query_artifacts" else "viewer"
            denied = ActorCtx(id=uuid7(), roles={denied_role})
            with pytest.raises(PermissionError):
                await dispatcher.apply_as(denied, operations[model])
        assert tuple(evidence) == evidence_before_role_denial

        actor = ActorCtx(id=uuid7(), roles={"admin"})
        options = DurableOptions(target_tick=0)
        for model, spec in specs.items():
            operation = operations[model]
            if spec.name not in _ACTOR_AWARE_NAMES:
                with pytest.raises(PermissionError):
                    await dispatcher.apply_as(actor, operation)
            with pytest.raises(ValueError, match="direct-only"):
                await dispatcher.defer(operation, options)
            with pytest.raises((PermissionError, ValueError)):
                await dispatcher.defer_as(actor, operation, options)
            item = DeferredItem(operation=operation, options=options)
            with pytest.raises(ValueError, match="direct-only"):
                await dispatcher.defer_batch((item,))
            with pytest.raises((PermissionError, ValueError)):
                await dispatcher.defer_batch_as(actor, (item,))

        assert tuple(effects) == effects_before_rejection
        assert scheduler_effects == []

        monkeypatch.setenv("ARCHETYPE_CATALOG_DIR", str(tmp_path / "runtime-catalog"))
        runtime = ArchetypeRuntime()
        try:
            assert runtime._resources is not resources
        finally:
            await runtime.shutdown()
        assert runtime._resources.close_state is RuntimeCloseState.CLOSED

    assert resources.close_state is RuntimeCloseState.CLOSED
    assert resources._owners == {}
    assert not hasattr(app.state, "resources")


class _AdmissionProbe:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    @asynccontextmanager
    async def _admit_runtime_operation(
        self,
        continuation: Callable[[], bool],
    ) -> AsyncIterator[None]:
        del continuation
        yield

    def request_stop(self) -> None:
        pass

    async def stop_admission(self) -> None:
        self._events.append("admission")

    async def wait_drained(self) -> None:
        self._events.append("drain")


class _DependencyProbe:
    def __init__(self, label: str, events: list[str]) -> None:
        self._label = label
        self._events = events

    async def shutdown(self) -> None:
        self._events.append(self._label)


class _RetainedAnchor:
    pass


@pytest.mark.asyncio
async def test_injected_mission_failure_retains_world_and_storage_until_retry() -> None:
    """A redacted author cleanup failure cannot overtake world or storage close."""

    events: list[str] = []
    private_failure = RuntimeError("private provider teardown details")
    mission_attempts = 0
    before_tasks = {
        task.get_name()
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and task.get_name().startswith("archetype-runtime-")
    }

    async def close_mission() -> None:
        nonlocal mission_attempts
        mission_attempts += 1
        events.append(f"mission-author:{mission_attempts}")
        if mission_attempts == 1:
            raise private_failure

    async def close_world() -> None:
        events.append("world")

    resources = RuntimeResources(
        dispatcher=_AdmissionProbe(events),
        audit=_DependencyProbe("audit", events),
        storage=_DependencyProbe("storage", events),
        owns_storage=True,
    )
    mission = resources.reserve_owner(
        "mission:author-loopback",
        phase="workflow-handles",
    )
    world = resources.reserve_owner(
        "world:runtime-loopback",
        phase="world-handles",
    )
    anchor = mission.retain_anchor(_RetainedAnchor())
    anchor_ref = weakref.ref(anchor)
    mission.bind(object(), close=close_mission)
    world.bind(object(), close=close_world)
    del anchor
    gc.collect()

    with pytest.raises(RuntimeShutdownError) as raised:
        await resources.aclose()

    assert raised.value.phase == "workflow-handles"
    assert raised.value.failures[0].owner == "mission:author-loopback"
    assert raised.value.failures[0].cause is private_failure
    assert "private provider teardown details" not in str(raised.value)
    assert resources.close_state is RuntimeCloseState.CLOSING_RETRYABLE
    assert events == ["admission", "drain", "mission-author:1"]
    assert not mission.released
    assert not world.released
    assert anchor_ref() is not None
    assert resources._storage is not None

    await resources.aclose()
    await resources.aclose()

    assert events == [
        "admission",
        "drain",
        "mission-author:1",
        "mission-author:2",
        "world",
        "audit",
        "storage",
    ]
    assert mission.released
    assert world.released
    assert resources.close_state is RuntimeCloseState.CLOSED
    assert resources._owners == {}
    assert resources._storage is None
    gc.collect()
    assert anchor_ref() is None
    after_tasks = {
        task.get_name()
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and task.get_name().startswith("archetype-runtime-")
    }
    assert after_tasks == before_tasks
