# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RED contracts for the PR-4 trusted-runtime dispatcher boundary.

Future family modules are loaded inside test bodies so this module still
collects on the PR-3 base.  The probes deliberately expose both the intended
dispatcher seam and traps for the application facade, actor-aware entry, and
runtime-owned world locks that PR-4 removes.
"""

from __future__ import annotations

import ast
import inspect
from collections.abc import Sequence
from contextlib import asynccontextmanager
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from weakref import WeakSet

import pytest

from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig
from archetype.runtime_resources import OperationAdmission

_PULL_FORWARD_MODELS = (
    ("archetype.artifacts.models", "IngestArtifacts"),
    ("archetype.artifacts.models", "QueryArtifacts"),
    ("archetype.evaluation.models", "RunGraders"),
    ("archetype.evaluation.models", "Evaluate"),
    ("archetype.research.models", "AutoResearch"),
)


class DispatchMetric(Component):
    value: int = 0


class _EffectTrap:
    """Fail if a PR-3 facade or another unintended effect is reached."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"trusted runtime reached forbidden effect port {name!r}")


class _ForbiddenAsyncContext:
    def __init__(self, label: str) -> None:
        self.label = label
        self.entries = 0

    async def __aenter__(self) -> None:
        self.entries += 1
        raise AssertionError(f"runtime entered duplicate {self.label}")

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _DispatchProbe:
    def __init__(self) -> None:
        self.trusted: list[Any] = []
        self.actor_aware: list[tuple[object, Any]] = []
        self.results: dict[str, object] = {}

    async def apply(self, operation: Any) -> Any:
        self.trusted.append(operation)
        return self.results.get(operation.operation)

    async def apply_as(self, actor: object, operation: Any) -> Any:
        self.actor_aware.append((actor, operation))
        raise AssertionError("trusted runtime must never call dispatcher.apply_as")


class _AdmissionResources:
    def __init__(self, dispatcher: _DispatchProbe) -> None:
        from archetype.research._extension import get_manifest as research_manifest
        from archetype.research.runtime import Research

        self.dispatcher = dispatcher
        self.world_library_manifests = (research_manifest(),)
        self._world_libraries = {
            "research": SimpleNamespace(runtime_adapter=None, world_adapter=Research),
        }
        self._operations = OperationAdmission(closed_message="runtime is closed")

    def world_library(self, name: str) -> object:
        return self._world_libraries[name]

    def admit_operation(self):
        return self._operations.admit()

    def operation_admitted(self) -> bool:
        return self._operations.admitted_by_current_task()


class _RuntimeProbe:
    def __init__(self, dispatcher: _DispatchProbe) -> None:
        resources = _AdmissionResources(dispatcher)
        self._resources = resources
        self.resources = resources
        self._dispatcher = dispatcher
        self.dispatcher = dispatcher
        self._application = _EffectTrap()
        self._container = _EffectTrap()
        self._shutdown_started = False
        self._closed = False

    def _ensure_open(self) -> None:
        if self._shutdown_started or self._closed:
            raise RuntimeError("runtime is closed")

    def _register_handle(self, _handle: object) -> None:
        return None

    def _unregister_handle(self, _handle: object) -> None:
        return None


class _WorldStateProbe:
    """The retained handle-local state plus traps for mirrors that must die."""

    def __init__(
        self,
        dispatcher: _DispatchProbe,
        *,
        storage: StorageConfig | None = None,
    ) -> None:
        self.runtime = _RuntimeProbe(dispatcher)
        self.name = "dispatch-probe"
        self.storage_config = storage
        self.cache_config = None
        self.world_id = "world-1"
        self.initialized = True
        self.owns_world = True
        self.destroying = False
        self.closing = False
        self.closed = False
        self.aliases: WeakSet[object] = WeakSet()
        self.op_lock = _ForbiddenAsyncContext("per-world operation lock")
        self.admission_lock = _ForbiddenAsyncContext("per-world admission lock")

    async def ensure_init(self) -> str:
        return "world-1"

    def require_storage_config(self, capability: str) -> StorageConfig:
        if self.storage_config is None:
            raise ValueError(
                f"{capability} requires explicit storage coordinates; "
                "attach the world with storage=..."
            )
        return self.storage_config

    @asynccontextmanager
    async def admit(self):
        raise AssertionError("runtime entered duplicate per-world admission state")
        yield  # pragma: no cover - makes this an async context manager


def _canonical_pull_forward_models() -> dict[str, type[Any]]:
    loaded: dict[str, type[Any]] = {}
    errors: list[str] = []
    for module_name, model_name in _PULL_FORWARD_MODELS:
        try:
            model = getattr(import_module(module_name), model_name)
        except (AttributeError, ImportError) as error:
            errors.append(f"{module_name}.{model_name}: {type(error).__name__}")
            continue
        loaded[model_name] = model
    if errors:
        pytest.fail(
            "PR-4 canonical runtime operation models are incomplete:\n- " + "\n- ".join(errors),
            pytrace=False,
        )
    return loaded


def _runtime_world(
    dispatcher: _DispatchProbe,
    *,
    storage: StorageConfig | None = None,
) -> tuple[Any, _WorldStateProbe]:
    runtime_world = import_module("archetype.runtime.world").RuntimeWorld
    state = _WorldStateProbe(dispatcher, storage=storage)
    world = runtime_world(state=cast("Any", state))
    state.aliases.add(world)
    return world, state


def _runtime_shell(dispatcher: _DispatchProbe) -> Any:
    runtime_type = import_module("archetype.runtime.runtime").ArchetypeRuntime
    runtime = object.__new__(runtime_type)
    resources = _AdmissionResources(dispatcher)
    runtime._resources = resources
    runtime.resources = resources
    runtime._dispatcher = dispatcher
    runtime.dispatcher = dispatcher
    runtime._application = _EffectTrap()
    runtime._container = _EffectTrap()
    runtime._shutdown_started = False
    runtime._closed = False
    runtime._handles = WeakSet()
    return runtime


def test_generic_library_lookup_constructs_installed_typed_adapters() -> None:
    from archetype.research import Research

    dispatcher = _DispatchProbe()
    runtime = _runtime_shell(dispatcher)
    world, _state = _runtime_world(dispatcher)

    assert isinstance(world.library("research"), Research)
    with pytest.raises(TypeError, match="has no runtime adapter"):
        runtime.library("research")


@pytest.mark.asyncio
async def test_lazy_world_activation_retains_the_effective_default_storage() -> None:
    """A default-created handle must remember the coordinates creation used."""

    runtime_world = import_module("archetype.runtime.world")
    dispatcher = _DispatchProbe()
    dispatcher.results["create_world"] = SimpleNamespace(world_id="default-world")
    state = runtime_world._RuntimeWorldState(
        runtime=_RuntimeProbe(dispatcher),
        name="default-storage",
        storage_config=None,
        cache_config=None,
        init_processors=[],
        init_resources=[],
        init_hooks=[],
    )

    assert await state.ensure_init() == "default-world"
    assert isinstance(state.storage_config, StorageConfig)
    assert len(dispatcher.trusted) == 1
    create = dispatcher.trusted[0]
    assert create.operation == "create_world"
    assert create.storage_config is state.storage_config


@pytest.mark.asyncio
async def test_cold_attached_evaluation_without_storage_fails_before_dispatch() -> None:
    """A cold handle cannot recover coordinates through a live-registry fallback."""

    from archetype.evaluation.contracts import GraderContract, Outcome

    dispatcher = _DispatchProbe()
    world, _state = _runtime_world(dispatcher)

    def grader(_frame: object) -> Outcome:
        raise AssertionError("missing coordinates reached the grader")

    with pytest.raises(ValueError, match="evaluate requires explicit storage coordinates"):
        await world.evaluate(
            DispatchMetric,
            contract=GraderContract(
                grader_id="cold-fail-closed",
                implementation_version="v1",
            ),
            grader=grader,
            evaluation_id="cold-without-storage",
        )

    assert dispatcher.trusted == []


@pytest.mark.asyncio
async def test_cold_attached_artifacts_without_storage_fail_before_dispatch(
    tmp_path: Path,
) -> None:
    """Artifact capabilities cannot recover coordinates through live state."""

    from archetype.artifacts.models import ArtifactSource

    dispatcher = _DispatchProbe()
    world, _state = _runtime_world(dispatcher)
    source = ArtifactSource(source_uri=str(tmp_path / "never-scanned.txt"))

    with pytest.raises(
        ValueError,
        match="ingest_artifacts requires explicit storage coordinates",
    ):
        await world.ingest_artifacts(source)
    with pytest.raises(
        ValueError,
        match="query_artifacts requires explicit storage coordinates",
    ):
        await world.artifacts()

    assert dispatcher.trusted == []
    assert not Path(source.source_uri).exists()


@pytest.mark.asyncio
async def test_resume_retains_the_exact_explicit_storage_on_its_handle(
    tmp_path: Path,
) -> None:
    """Resume and the returned handle must share one effective coordinate value."""

    dispatcher = _DispatchProbe()
    dispatcher.results["resume_world"] = SimpleNamespace(world_id="resumed-world")
    runtime = _runtime_shell(dispatcher)
    runtime._bind_world_state = lambda state: state
    storage = StorageConfig(
        uri=str(tmp_path / "resume-store"),
        namespace="resume-explicit",
    )

    state = await runtime.resume("durable-world", storage=storage)

    assert len(dispatcher.trusted) == 1
    resume = dispatcher.trusted[0]
    assert resume.storage_config is storage
    assert state.storage_config is storage


@pytest.mark.asyncio
async def test_resume_retains_its_effective_default_storage_on_the_handle() -> None:
    """Default resume must not discard the coordinates used for admission."""

    dispatcher = _DispatchProbe()
    dispatcher.results["resume_world"] = SimpleNamespace(world_id="resumed-default")
    runtime = _runtime_shell(dispatcher)
    runtime._bind_world_state = lambda state: state

    state = await runtime.resume("durable-world")

    assert len(dispatcher.trusted) == 1
    resume = dispatcher.trusted[0]
    assert isinstance(resume.storage_config, StorageConfig)
    assert state.storage_config is resume.storage_config


def _assert_exact_pull_forward_dispatch(
    operations: Sequence[object],
    models: dict[str, type[Any]],
    *,
    get_world_info: type[Any],
    query_components: type[Any],
) -> dict[str, Any]:
    expected_types = (
        models["IngestArtifacts"],
        models["QueryArtifacts"],
        get_world_info,
        query_components,
        models["RunGraders"],
        models["Evaluate"],
        models["AutoResearch"],
    )
    actual_types = tuple(type(operation) for operation in operations)
    assert actual_types == expected_types, (
        "runtime must dispatch the exact canonical sequence: one of each "
        "pull-forward plus only GetWorldInfo/QueryComponents support calls"
    )

    by_model = {type(operation): operation for operation in operations}
    return {model_name: by_model[model] for model_name, model in models.items()}


@pytest.mark.asyncio
async def test_runtime_world_constructs_exact_registered_family_models() -> None:
    """Stable world methods construct exact values and use trusted dispatch."""

    from archetype.world.models import ComponentValue, Run, Spawn

    dispatcher = _DispatchProbe()
    run_result = object()
    dispatcher.results.update({"spawn": 41, "run": run_result})
    world, state = _runtime_world(dispatcher)
    component = DispatchMetric(value=7)
    capability = object()
    config = RunConfig(num_steps=3, debug=True)

    # Counterfactual: the test state really would catch retaining the PR-3 lock.
    with pytest.raises(AssertionError, match="per-world operation lock"):
        async with state.op_lock:
            pass

    assert await world.spawn(component) == 41
    assert await world.run(config=config, capability=capability) is run_result

    assert [type(operation) for operation in dispatcher.trusted] == [Spawn, Run]
    spawn, run = dispatcher.trusted
    assert spawn == Spawn(
        world_id="world-1",
        components=(ComponentValue.from_component(component),),
    )
    assert run.world_id == "world-1"
    assert run.run_config is config
    assert run.input_kwargs["capability"] is capability
    assert dispatcher.actor_aware == []
    assert state.op_lock.entries == 1, "only the counterfactual may enter the lock trap"


@pytest.mark.asyncio
async def test_pull_forward_runtime_methods_reach_exact_nondurable_specs(
    tmp_path: Path,
) -> None:
    """All five supported workflow methods construct their canonical operation."""

    from daft import from_pydict

    from archetype.artifacts.models import ArtifactSource
    from archetype.evaluation.contracts import GraderContract
    from archetype.research.models import AutoResearchConfig
    from archetype.research.runtime import Research
    from archetype.world.models import GetWorldInfo, QueryComponents

    models = _canonical_pull_forward_models()
    dispatcher = _DispatchProbe()
    results = {str(model.model_fields["operation"].default): object() for model in models.values()}
    dispatcher.results.update(results)
    frame = from_pydict({"value": [1]})
    dispatcher.results["query_components"] = frame
    dispatcher.results["get_world_info"] = SimpleNamespace(
        world_id="world-1",
        run_id="run-1",
    )

    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="runtime-dispatch-red",
    )
    world, _state = _runtime_world(dispatcher, storage=storage)

    artifact = ArtifactSource(source_uri=str(tmp_path / "artifact.txt"))

    def grader(_df: object) -> object:
        return object()

    def evaluator(_rollout: object) -> float:
        return 1.0

    def prepare_candidate(_world: object) -> object:
        return object()

    def on_iteration(_iteration: object) -> None:
        return None

    contract = GraderContract(
        grader_id="runtime-dispatch",
        implementation_version="1",
    )
    research_config = AutoResearchConfig(
        experiment_name="runtime-dispatch",
        experiment_id="runtime-dispatch-1",
        evaluator_id="eval-1",
        rollout_contract_id="rollout-1",
        max_iterations=1,
        num_episodes=1,
    )
    assert await world.ingest_artifacts(artifact) is results["ingest_artifacts"]
    assert await world.artifacts() is results["query_artifacts"]
    assert await world.grade(DispatchMetric, graders=(grader,)) is results["run_graders"]
    assert (
        await world.evaluate(
            DispatchMetric,
            contract=contract,
            grader=grader,
            evaluation_id="evaluation-1",
            ticks=[2],
            entity_ids=[3],
        )
        is results["evaluate"]
    )
    assert (
        await Research(world).autoresearch(
            research_config,
            evaluator,
            prepare_candidate=prepare_candidate,
            lab_world_id="lab-world-1",
            on_iteration=on_iteration,
        )
        is results["autoresearch"]
    )
    operations = _assert_exact_pull_forward_dispatch(
        dispatcher.trusted,
        models,
        get_world_info=GetWorldInfo,
        query_components=QueryComponents,
    )
    assert len(operations) == 5
    assert all(
        model.model_fields["operation"].default == operations[model_name].operation
        for model_name, model in models.items()
    )
    with pytest.raises(AssertionError, match="exact canonical sequence"):
        _assert_exact_pull_forward_dispatch(
            (*dispatcher.trusted, dispatcher.trusted[0], object()),
            models,
            get_world_info=GetWorldInfo,
            query_components=QueryComponents,
        )

    assert operations["IngestArtifacts"].world_id == "world-1"
    assert operations["IngestArtifacts"].sources == (artifact,)
    assert operations["IngestArtifacts"].storage_config is storage
    assert operations["QueryArtifacts"].world_id == "world-1"
    assert operations["QueryArtifacts"].storage_config is storage
    # RuntimeWorld.grade legitimately performs its query before RunGraders.
    run_graders_index = next(
        index
        for index, operation in enumerate(dispatcher.trusted)
        if operation is operations["RunGraders"]
    )
    assert type(dispatcher.trusted[run_graders_index - 1]) is QueryComponents
    grade_info = dispatcher.trusted[run_graders_index - 2]
    grade_query = dispatcher.trusted[run_graders_index - 1]
    assert type(grade_info) is GetWorldInfo
    assert grade_info.world_id == "world-1"
    assert grade_query.world_id == "world-1"
    assert grade_query.run_id == "run-1"
    assert grade_query.storage_config is storage
    assert grade_query.entity_ids is None
    assert len(grade_query.components) == 1
    assert grade_query.components[0].resolve() is DispatchMetric
    assert operations["RunGraders"].df is frame
    assert operations["RunGraders"].graders == (grader,)
    assert operations["Evaluate"].world_id == "world-1"
    assert operations["Evaluate"].components == (DispatchMetric,)
    assert operations["Evaluate"].contract is contract
    assert operations["Evaluate"].grader is grader
    assert operations["Evaluate"].evaluation_id == "evaluation-1"
    assert operations["Evaluate"].storage_config is storage
    assert operations["Evaluate"].ticks == (2,)
    assert operations["Evaluate"].entity_ids == (3,)
    assert operations["AutoResearch"].world_id == "world-1"
    assert operations["AutoResearch"].config is research_config
    assert operations["AutoResearch"].evaluator is evaluator
    assert operations["AutoResearch"].prepare_candidate is prepare_candidate
    assert operations["AutoResearch"].lab_world_id == "lab-world-1"
    assert operations["AutoResearch"].on_iteration is on_iteration


@pytest.mark.asyncio
async def test_runtime_has_no_actor_or_access_decision_evidence() -> None:
    dispatcher = _DispatchProbe()
    world, _state = _runtime_world(dispatcher)
    await world.step()
    assert dispatcher.actor_aware == []
    assert len(dispatcher.trusted) == 1
    assert not hasattr(dispatcher.trusted[0], "actor")


def test_runtime_world_has_no_duplicate_world_operation_lock_or_context_authority() -> None:
    """Runtime retains activation/close state, not world or cleanup authority."""

    runtime_world = import_module("archetype.runtime.world")
    source_path = Path(inspect.getsourcefile(runtime_world) or "")
    assert source_path.is_file()
    source = source_path.read_text()
    tree = ast.parse(source)

    def forbidden_names(parsed: ast.AST) -> set[str]:
        names = {
            node.id
            for node in ast.walk(parsed)
            if isinstance(node, ast.Name)
            and node.id
            in {
                "ContextVar",
                "_ADMITTED_STATE",
                "_RUNTIME_CLEANUP_STATE",
                "_runtime_cleanup_scope",
            }
        }
        names.update(
            node.attr
            for node in ast.walk(parsed)
            if isinstance(node, ast.Attribute)
            and node.attr
            in {
                "op_lock",
                "admission_lock",
                "active_operations",
            }
        )
        return names

    counterfactual = ast.parse(
        "from contextvars import ContextVar\n"
        "_ADMITTED_STATE = ContextVar('x')\n"
        "state.op_lock = object()\n"
    )
    assert forbidden_names(counterfactual) == {
        "ContextVar",
        "_ADMITTED_STATE",
        "op_lock",
    }

    assert forbidden_names(tree) == set()
    assert "_RUNTIME_CLEANUP_STATE" not in source
    assert "_runtime_cleanup_scope" not in source


class _ResourcesCloseProbe:
    def __init__(self) -> None:
        self.dispatcher = _DispatchProbe()
        self.close_calls = 0

    async def aclose(self) -> None:
        self.close_calls += 1

    def operation_admitted(self) -> bool:
        return False

    def ensure_close_allowed(self) -> None:
        return None


@pytest.mark.asyncio
async def test_runtime_shutdown_delegates_once_to_runtime_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The public host has one shutdown owner and no parallel cleanup loop."""

    wiring = import_module("archetype.wiring")
    runtime_module = import_module("archetype.runtime.runtime")
    resources = _ResourcesCloseProbe()
    build_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def build(*args: object, **kwargs: object) -> _ResourcesCloseProbe:
        build_calls.append((args, kwargs))
        return resources

    monkeypatch.setattr(wiring, "build_runtime_resources", build)
    monkeypatch.setattr(
        runtime_module,
        "build_runtime_resources",
        build,
        raising=False,
    )

    runtime = runtime_module.ArchetypeRuntime()
    assert runtime._resources is resources
    assert len(build_calls) == 1

    await runtime.shutdown()
    await runtime.shutdown()

    assert resources.close_calls == 1
    assert not hasattr(runtime, "_container")
    assert not hasattr(runtime, "_application")
    assert not hasattr(runtime, "_mission_handles")


@pytest.mark.asyncio
async def test_world_handle_close_is_local_and_fork_sibling_survives(
    tmp_path: Path,
) -> None:
    """Closing one source handle cannot close its independently owned fork."""

    runtime_type = import_module("archetype.runtime.runtime").ArchetypeRuntime
    runtime = runtime_type()
    source = runtime.world(
        "source",
        storage=StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="runtime-close-local",
        ),
    )
    try:
        await source.spawn(DispatchMetric(value=1))
        sibling = await source.fork("sibling")
        sibling_id = sibling.world_id

        await source.shutdown()

        with pytest.raises(RuntimeError, match="closed"):
            await source.spawn(DispatchMetric(value=2))
        assert sibling.world_id == sibling_id
        sibling_entity = await sibling.spawn(DispatchMetric(value=3))
        assert isinstance(sibling_entity, int)
        assert (await sibling.info()).world_id == sibling_id

        another = runtime.world("another")
        assert isinstance(await another.spawn(DispatchMetric(value=4)), int)
    finally:
        await runtime.shutdown()
