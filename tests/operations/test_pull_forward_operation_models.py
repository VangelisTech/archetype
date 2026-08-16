# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RED contracts for the PR-4 pull-forward operation boundary.

The canonical family modules do not exist on the PR-3 base.  Imports of those
modules therefore stay inside test bodies so every candidate node collects and
reports its own missing PR-4 seam.  Dispatcher counterfactuals run first for
the behavior nodes, distinguishing an absent family boundary from a broken
landed dispatcher.
"""

from __future__ import annotations

import ast
import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

import pytest
from pydantic import BaseModel, ConfigDict
from uuid_utils import uuid7

from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx, DurableOptions
from archetype.commands.registry import OperationRegistry, OperationSpec
from archetype.world.models import Spawn

pytestmark = pytest.mark.contract("commands.identity.idempotent")

_MODEL_BOUNDARIES = (
    ("archetype.artifacts.models", "IngestArtifacts", "ingest_artifacts"),
    ("archetype.artifacts.models", "QueryArtifacts", "query_artifacts"),
    ("archetype.evaluation.models", "RunGraders", "run_graders"),
    ("archetype.evaluation.models", "Evaluate", "evaluate"),
    ("archetype.research.models", "AutoResearch", "autoresearch"),
    (
        "archetype.physical_ai.models",
        "RunHostedEpisode",
        "run_hosted_episode",
    ),
    (
        "archetype.missions.trajectories.models",
        "IngestClaudeTranscript",
        "ingest_claude_transcript",
    ),
    (
        "archetype.missions.trajectories.models",
        "QueryTranscriptRows",
        "query_transcript_rows",
    ),
    ("archetype.missions.trajectories.models", "QueryTrajectory", "query_trajectory"),
    ("archetype.missions.trajectories.models", "GradeTrajectory", "grade_trajectory"),
    ("archetype.missions.models", "SubmitMission", "submit_mission"),
    ("archetype.missions.models", "RunMission", "run_mission"),
    (
        "archetype.missions.models",
        "RestoreMissionSandbox",
        "restore_mission_sandbox",
    ),
)
_EXPECTED_LITERALS = {
    model_name: literal for _module_name, model_name, literal in _MODEL_BOUNDARIES
}
_ACTOR_AWARE_MODELS = frozenset(
    {
        "AutoResearch",
        "Evaluate",
        "IngestArtifacts",
        "QueryArtifacts",
    }
)
_TRUSTED_ONLY_MODELS = frozenset(_EXPECTED_LITERALS) - _ACTOR_AWARE_MODELS
_MISSION_MODELS = frozenset(
    {
        "SubmitMission",
        "RunMission",
        "RestoreMissionSandbox",
    }
)
_SCHEDULER_FIELDS = frozenset(
    {
        "attempt",
        "attempts",
        "lease",
        "lease_owner",
        "max_attempts",
        "origin",
        "payload_digest",
        "principal",
        "principal_id",
        "priority",
        "review_evidence",
        "target_tick",
        "task_base_revision",
    }
)
_OUTWARD_IMPORT_PREFIXES = (
    "archetype.app",
    "archetype.api",
    "archetype.cli",
    "archetype.commands",
    "archetype.runtime",
    "archetype.wiring",
)


class _ProbeOperation(BaseModel):
    direct_only: ClassVar[bool] = True
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["probe"] = "probe"


class _MissionSummaryProbe(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["submit_mission"] = "submit_mission"
    owner_id: str
    task_base_revision: str
    candidate_diff: str
    validator_output: str
    critic_findings: str
    provider_configuration: str
    cleanup_state: str


class _AllowPolicy:
    def __init__(self) -> None:
        self.preauthorized: list[str] = []
        self.authorized: list[str] = []

    def preauthorize(self, _actor: ActorCtx, *, permission: str) -> None:
        self.preauthorized.append(permission)

    def authorize_application(
        self,
        _actor: ActorCtx,
        *,
        permission: str,
        token_cost: int = 0,
    ) -> None:
        assert token_cost == 0
        self.authorized.append(permission)


class _SchedulerTrap:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def admit(self, *_args: object, **_kwargs: object) -> None:
        self.calls.append("admit")
        raise AssertionError("direct-only pull-forward operation reached the scheduler")


class _AccessSink:
    def __init__(self) -> None:
        self.rows: list[object] = []

    async def __call__(self, row: object) -> None:
        self.rows.append(row)


class _LeakSentinel:
    def __init__(self, label: str) -> None:
        self.label = label

    def __str__(self) -> str:
        return f"LEAK_SENTINEL:{self.label}"


def _canonical_models(
    selected: frozenset[str] | None = None,
) -> dict[str, type[BaseModel]]:
    """Load all requested future models after pytest collection."""

    requested = selected or frozenset(_EXPECTED_LITERALS)
    loaded: dict[str, type[BaseModel]] = {}
    errors: list[str] = []
    for module_name, model_name, _literal in _MODEL_BOUNDARIES:
        if model_name not in requested:
            continue
        try:
            module = import_module(module_name)
            model = getattr(module, model_name)
        except (AttributeError, ImportError) as error:
            errors.append(f"{module_name}.{model_name}: {type(error).__name__}")
            continue
        if not isinstance(model, type) or not issubclass(model, BaseModel):
            errors.append(f"{module_name}.{model_name}: not a Pydantic model")
            continue
        loaded[model_name] = model
    if errors:
        pytest.fail(
            "PR-4 canonical operation boundary is incomplete:\n- " + "\n- ".join(errors),
            pytrace=False,
        )
    return loaded


def _operation_instance(model: type[BaseModel]) -> BaseModel:
    literal = _EXPECTED_LITERALS[model.__name__]
    return model.model_construct(operation=literal)


def _safe_summary(operation: BaseModel) -> Mapping[str, Any]:
    """The PR-4 registration allowlist: discriminator only for live payloads."""

    return {"operation": cast("Any", operation).operation}


def _registered_specs(
    models: Mapping[str, type[BaseModel]],
    effects: list[str],
) -> tuple[OperationRegistry, tuple[OperationSpec, ...]]:
    registry = OperationRegistry()
    specs: list[OperationSpec] = []

    for model_name, model in models.items():
        literal = _EXPECTED_LITERALS[model_name]

        def make_handler(operation_name: str):
            async def handler(_operation: BaseModel) -> str:
                effects.extend(
                    (
                        f"handler:{operation_name}",
                        f"provider:{operation_name}",
                        f"filesystem:{operation_name}",
                    )
                )
                return operation_name

            return handler

        spec = OperationSpec(
            name=literal,
            model=model,
            handler=make_handler(literal),
            permission=literal,
            summarize=_safe_summary,
            quota_scope="application",
            world_key=None,
            durable=None,
            trusted=True,
            untrusted=model_name in _ACTOR_AWARE_MODELS,
            token_cost=0,
        )
        registry.register(spec)
        specs.append(spec)
    return registry, tuple(specs)


def _dispatcher(
    registry: OperationRegistry,
    *,
    scheduler: _SchedulerTrap | None = None,
    access: _AccessSink | None = None,
) -> tuple[CommandDispatcher, _SchedulerTrap, _AccessSink]:
    scheduler = scheduler or _SchedulerTrap()
    access = access or _AccessSink()

    def unexpected_target_tick(_world_id: object) -> int:
        raise AssertionError("application-scoped pull-forward operation read a world tick")

    return (
        CommandDispatcher(
            registry=registry,
            policy=cast("Any", _AllowPolicy()),
            scheduler=scheduler,
            record_access=access,
            target_tick_for_world=unexpected_target_tick,
        ),
        scheduler,
        access,
    )


def _probe_spec(*, untrusted: bool, effects: list[str]) -> OperationSpec:
    async def handler(_operation: BaseModel) -> str:
        effects.append("probe-handler")
        return "probe"

    return OperationSpec(
        name="probe",
        model=_ProbeOperation,
        handler=handler,
        permission="probe",
        summarize=_safe_summary,
        quota_scope="application",
        world_key=None,
        durable=None,
        trusted=True,
        untrusted=untrusted,
    )


async def _assert_actor_aware_counterfactual() -> None:
    effects: list[str] = []
    registry = OperationRegistry()
    registry.register(_probe_spec(untrusted=True, effects=effects))
    dispatcher, scheduler, _access = _dispatcher(registry)
    result = await dispatcher.apply_as(
        ActorCtx(id=uuid7(), roles={"admin"}),
        _ProbeOperation(),
    )
    assert result == "probe"
    assert effects == ["probe-handler"]
    assert scheduler.calls == []


async def _assert_trusted_only_counterfactual() -> None:
    effects: list[str] = []
    registry = OperationRegistry()
    registry.register(_probe_spec(untrusted=False, effects=effects))
    dispatcher, scheduler, _access = _dispatcher(registry)
    with pytest.raises(PermissionError, match="not available to untrusted"):
        await dispatcher.apply_as(
            ActorCtx(id=uuid7(), roles={"admin"}),
            _ProbeOperation(),
        )
    assert effects == []
    assert scheduler.calls == []


async def _assert_direct_only_counterfactual() -> None:
    effects: list[str] = []
    registry = OperationRegistry()
    registry.register(_probe_spec(untrusted=True, effects=effects))
    dispatcher, scheduler, _access = _dispatcher(registry)
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    options = DurableOptions(target_tick=0)
    with pytest.raises(ValueError, match="direct-only"):
        await dispatcher.defer(_ProbeOperation(), options)
    with pytest.raises(ValueError, match="direct-only"):
        await dispatcher.defer_as(actor, _ProbeOperation(), options)
    assert effects == []
    assert scheduler.calls == []


def _forbidden_imports(source: str) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        names: tuple[str, ...]
        if isinstance(node, ast.Import):
            names = tuple(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            names = (node.module,)
        else:
            continue
        imported.update(
            name
            for name in names
            if any(
                name == prefix or name.startswith(f"{prefix}.")
                for prefix in _OUTWARD_IMPORT_PREFIXES
            )
        )
    return imported


def test_pull_forward_inventory_is_exactly_thirteen_models() -> None:
    models = _canonical_models()

    assert len(models) == 13
    assert set(models) == set(_EXPECTED_LITERALS)
    for module_name, model_name, literal in _MODEL_BOUNDARIES:
        model = models[model_name]
        assert model.__module__ == module_name
        assert model.model_fields["operation"].default == literal

    physical_models = import_module("archetype.physical_ai.models")
    assert not hasattr(physical_models, "SweepInstructions")


def test_models_are_frozen_extra_forbid_and_contain_no_scheduler_fields() -> None:
    # The landed world model is the equivalent lower-family boundary.
    assert Spawn.model_config.get("frozen") is True
    assert Spawn.model_config.get("extra") == "forbid"
    assert not (_SCHEDULER_FIELDS & set(Spawn.model_fields))

    models = _canonical_models()
    for model_name, model in models.items():
        assert model.model_config.get("frozen") is True, model_name
        assert model.model_config.get("extra") == "forbid", model_name
        assert not (_SCHEDULER_FIELDS & set(model.model_fields)), model_name


def test_family_models_import_no_app_commands_runtime_api_cli_or_wiring() -> None:
    # Prove the AST oracle sees the dependency violation it is meant to reject.
    assert _forbidden_imports("from archetype.commands import OperationSpec") == {
        "archetype.commands"
    }
    assert _forbidden_imports("from archetype.core.config import StorageConfig") == set()

    models = _canonical_models()
    checked_modules: set[str] = set()
    for model in models.values():
        if model.__module__ in checked_modules:
            continue
        checked_modules.add(model.__module__)
        module = import_module(model.__module__)
        module_path = Path(cast("str", module.__file__))
        forbidden = _forbidden_imports(module_path.read_text())
        assert forbidden == set(), f"{model.__module__}: {sorted(forbidden)}"


def test_supported_contract_aliases_preserve_object_identity() -> None:
    identity_pairs = (
        (
            "archetype.physical_ai.manipulation",
            "EnvClient",
            "archetype.physical_ai.interfaces",
            "EnvClient",
        ),
        (
            "archetype.physical_ai.policy",
            "PolicyClient",
            "archetype.physical_ai.interfaces",
            "PolicyClient",
        ),
    )

    # Compatibility paths remain live while canonical destinations are
    # verified independently.
    old_values: dict[tuple[str, str], object] = {}
    for old_module_name, old_name, _new_module_name, _new_name in identity_pairs:
        old_module = import_module(old_module_name)
        old_values[(old_module_name, old_name)] = getattr(old_module, old_name)
    assert len(old_values) == len(identity_pairs)

    errors: list[str] = []
    for old_module_name, old_name, new_module_name, new_name in identity_pairs:
        try:
            new_value = getattr(import_module(new_module_name), new_name)
        except (AttributeError, ImportError) as error:
            errors.append(f"{new_module_name}.{new_name}: {type(error).__name__}")
            continue
        assert old_values[(old_module_name, old_name)] is new_value
    assert errors == [], "supported identity moves are incomplete:\n- " + "\n- ".join(errors)


@pytest.mark.asyncio
async def test_pull_forward_specs_have_exact_immediate_availability_and_are_non_durable() -> None:
    probe_effects: list[str] = []
    probe_registry = OperationRegistry()
    probe_registry.register(_probe_spec(untrusted=True, effects=probe_effects))
    assert probe_registry.specs[0].durable is None

    models = _canonical_models()
    effects: list[str] = []
    registry, specs = _registered_specs(models, effects)
    specs_by_model = {spec.model.__name__: spec for spec in specs}

    assert len(_ACTOR_AWARE_MODELS) == 4
    assert len(_TRUSTED_ONLY_MODELS) == 9
    assert set(specs_by_model) == set(_EXPECTED_LITERALS)
    for model_name, spec in specs_by_model.items():
        assert spec.name == _EXPECTED_LITERALS[model_name]
        assert spec.trusted is True
        assert spec.untrusted is (model_name in _ACTOR_AWARE_MODELS)
        assert spec.durable is None

    dispatcher, scheduler, _access = _dispatcher(registry)
    results = [
        await dispatcher.apply(_operation_instance(models[model_name]))
        for model_name in sorted(models)
    ]
    expected_literals = [_EXPECTED_LITERALS[name] for name in sorted(models)]
    assert results == expected_literals
    assert effects == [
        effect
        for literal in expected_literals
        for effect in (
            f"handler:{literal}",
            f"provider:{literal}",
            f"filesystem:{literal}",
        )
    ]
    assert scheduler.calls == []


@pytest.mark.asyncio
async def test_apply_as_reaches_exact_four_actor_aware_handlers() -> None:
    await _assert_actor_aware_counterfactual()
    models = _canonical_models()
    effects: list[str] = []
    registry, _specs = _registered_specs(models, effects)
    dispatcher, scheduler, _access = _dispatcher(registry)
    actor = ActorCtx(id=uuid7(), roles={"admin"})

    results = []
    for model_name in sorted(_ACTOR_AWARE_MODELS):
        results.append(await dispatcher.apply_as(actor, _operation_instance(models[model_name])))

    expected_literals = [_EXPECTED_LITERALS[name] for name in sorted(_ACTOR_AWARE_MODELS)]
    assert results == expected_literals
    assert effects == [
        effect
        for literal in expected_literals
        for effect in (
            f"handler:{literal}",
            f"provider:{literal}",
            f"filesystem:{literal}",
        )
    ]
    assert scheduler.calls == []


@pytest.mark.asyncio
async def test_apply_as_rejects_other_nine_before_handler_provider_or_scheduler_effect() -> None:
    await _assert_trusted_only_counterfactual()
    models = _canonical_models()
    effects: list[str] = []
    registry, _specs = _registered_specs(models, effects)
    dispatcher, scheduler, _access = _dispatcher(registry)
    actor = ActorCtx(id=uuid7(), roles={"admin"})

    for model_name in sorted(_TRUSTED_ONLY_MODELS):
        with pytest.raises(PermissionError, match="not available to untrusted"):
            await dispatcher.apply_as(actor, _operation_instance(models[model_name]))

    assert len(_TRUSTED_ONLY_MODELS) == 9
    assert effects == []
    assert scheduler.calls == []


@pytest.mark.asyncio
async def test_defer_and_defer_as_reject_all_thirteen_before_handler_provider_or_scheduler_effect() -> (
    None
):
    await _assert_direct_only_counterfactual()
    models = _canonical_models()
    effects: list[str] = []
    registry, _specs = _registered_specs(models, effects)
    dispatcher, scheduler, _access = _dispatcher(registry)
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    options = DurableOptions(target_tick=0)

    for model_name, model in sorted(models.items()):
        operation = _operation_instance(model)
        with pytest.raises(ValueError, match="direct-only"):
            await dispatcher.defer(operation, options)
        expected_error = ValueError if model_name in _ACTOR_AWARE_MODELS else PermissionError
        with pytest.raises(expected_error):
            await dispatcher.defer_as(actor, operation, options)

    assert effects == []
    assert scheduler.calls == []


def test_mission_summary_excludes_task_base_candidate_critic_provider_and_cleanup_data() -> None:
    probe = _MissionSummaryProbe(
        owner_id="owner-1",
        task_base_revision="TASK_BASE_SENTINEL",
        candidate_diff="CANDIDATE_SENTINEL",
        validator_output="VALIDATOR_SENTINEL",
        critic_findings="CRITIC_SENTINEL",
        provider_configuration="PROVIDER_SENTINEL",
        cleanup_state="CLEANUP_SENTINEL",
    )
    assert _safe_summary(probe) == {"operation": "submit_mission"}

    models = _canonical_models(_MISSION_MODELS)
    _registry, specs = _registered_specs(models, [])
    forbidden_fragments = (
        "TASK_BASE",
        "CANDIDATE",
        "VALIDATOR",
        "CRITIC",
        "PROVIDER",
        "CLEANUP",
    )

    for spec in specs:
        values: dict[str, object] = {
            name: _LeakSentinel(name.upper())
            for name in spec.model.model_fields
            if name != "operation"
        }
        operation = cast("Any", spec.model).model_construct(
            operation=spec.name,
            **values,
        )
        summary = dict(spec.summarize(operation))
        assert summary == {"operation": spec.name}
        encoded = json.dumps(summary, sort_keys=True)
        assert all(fragment not in encoded for fragment in forbidden_fragments)
