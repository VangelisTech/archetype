# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Process-ownership contracts for physical-AI operation providers."""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
from contextlib import asynccontextmanager
from importlib import import_module
from pathlib import Path
from typing import Any

import httpx
import pytest

from archetype.core.config import StorageConfig
from archetype.errors import RuntimeShutdownError
from archetype.physical_ai.contracts import (
    InstructionSweepConfig,
    PhysicalTaskEvalConfig,
)
from archetype.physical_ai.interfaces import PhysicalClientLifetimeRegistrar
from archetype.physical_ai.manipulation import (
    ManipAction,
    ManipFrameRef,
    ManipProprio,
    ManipStatus,
    ManipTask,
)
from archetype.physical_ai.models import (
    EvaluatePhysicalTask,
    SweepPhysicalInstructions,
)
from archetype.storage.catalog.remote import RemoteControlCatalog
from archetype.storage.config import ControlCatalogConfig
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
from archetype.world.lifecycle import WorldLifecycle
from tests._runtime import build_test_runtime


class _StopWorkflow(Exception):
    """Bound a handler test immediately after the first lower-family effect."""


class _CloseableEnv:
    def __init__(
        self,
        *,
        events: list[tuple[str, object]] | None = None,
        fail_first_close: bool = False,
    ) -> None:
        self.events = events
        self.fail_first_close = fail_first_close
        self.close_calls = 0

    def task_language(self) -> str:
        if self.events is not None:
            self.events.append(("provider", "task_language"))
        return "reach"

    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        del env_id, seed
        if self.events is not None:
            self.events.append(("provider", "env.reset"))
        return {
            "eef_pos": [0.0, 0.0, 0.5],
            "eef_quat": [1.0, 0.0, 0.0, 0.0],
            "gripper": 0.0,
            "gripper_qpos": [0.0, 0.0],
        }

    def step(
        self,
        env_ids: list[int],
        actions: list[list[float]],
    ) -> list[dict[str, Any]]:
        del actions
        return [
            {
                "eef_pos": [0.0, 0.0, 0.5],
                "eef_quat": [1.0, 0.0, 0.0, 0.0],
                "gripper": 0.0,
                "gripper_qpos": [0.0, 0.0],
                "reward": 0.0,
                "done": True,
                "success": True,
            }
            for _ in env_ids
        ]

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        del instructions, observations
        return [[0.0] * 7 for _ in env_keys]

    async def aclose(self) -> None:
        self.close_calls += 1
        if self.fail_first_close and self.close_calls == 1:
            raise RuntimeError("environment close failed once")


class _CloseablePolicy:
    def __init__(self, *, events: list[tuple[str, object]] | None = None) -> None:
        self.events = events
        self.close_calls = 0

    def reset(self) -> None:
        if self.events is not None:
            self.events.append(("provider", "policy.reset"))

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        del instructions, observations
        return [[0.0] * 7 for _ in env_keys]

    async def aclose(self) -> None:
        self.close_calls += 1


class _EnvWithoutClose:
    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        del env_id, seed
        return {}

    def step(
        self,
        env_ids: list[int],
        actions: list[list[float]],
    ) -> list[dict[str, Any]]:
        del actions
        return [{} for _ in env_ids]


class _RegistrarSpy:
    def __init__(self, events: list[tuple[str, object]]) -> None:
        self.events = events

    def lease(
        self,
        env_client: object,
        policy_client: object | None,
    ) -> Any:
        clients = (env_client,) if policy_client is None else (env_client, policy_client)
        seen: set[int] = set()
        for client in clients:
            if id(client) not in seen:
                self.events.append(("register", client))
                seen.add(id(client))

        @asynccontextmanager
        async def held() -> Any:
            yield

        return held()


class _FirstEffectLifecycle:
    def __init__(self, events: list[tuple[str, object]]) -> None:
        self.events = events

    async def create_closing_world(self, *_args: object, **_kwargs: object) -> None:
        self.events.append(("workflow", "create_closing_world"))
        raise _StopWorkflow


def _physical_handlers() -> Any:
    return import_module("archetype.physical_ai.handlers")


def _physical_owner_clients(resources: Any) -> tuple[object, ...]:
    return tuple(
        reservation._resource
        for owner, reservation in resources._owners.items()
        if owner.startswith("physical-ai:client:")
    )


def _physical_lifetime_clients(lifetimes: Any) -> tuple[object, ...]:
    return tuple(entry.client for entry in lifetimes._entries.values())


def _world_cleanup_owner_names(resources: Any) -> tuple[str, ...]:
    return tuple(owner for owner in resources._owners if owner.startswith("world-cleanup:"))


def _exception_leaves(error: BaseException) -> list[BaseException]:
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in _exception_leaves(child)]
    return [error]


def test_family_lifetime_boundary_has_no_process_owner_import_or_durable_metadata() -> None:
    forbidden_prefixes = (
        "archetype.app",
        "archetype.runtime",
        "archetype.runtime_resources",
        "archetype.wiring",
    )
    imported: set[str] = set()
    for module_name in (
        "archetype.physical_ai.handlers",
        "archetype.physical_ai.interfaces",
        "archetype.physical_ai.models",
        "archetype.physical_ai.manipulation",
        "archetype.physical_ai.policy",
        "archetype.physical_ai.views",
    ):
        module = import_module(module_name)
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = (alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                names = (node.module,)
            else:
                continue
            imported.update(
                name
                for name in names
                if any(
                    name == prefix or name.startswith(f"{prefix}.") for prefix in forbidden_prefixes
                )
            )
    assert imported == set()

    manipulation = import_module("archetype.physical_ai.manipulation")
    policy = import_module("archetype.physical_ai.policy")
    assert not hasattr(manipulation, "EnvStepProcessor")
    assert not hasattr(manipulation, "FramedEnvStepProcessor")
    assert not hasattr(policy, "PolicyActionProcessor")

    forbidden_fields = {
        "cleanup",
        "cleanup_id",
        "close",
        "close_state",
        "owner",
        "owner_id",
        "reservation",
    }
    for model in (EvaluatePhysicalTask, SweepPhysicalInstructions):
        assert forbidden_fields.isdisjoint(model.model_fields)
    for component in (ManipAction, ManipFrameRef, ManipProprio, ManipStatus, ManipTask):
        assert forbidden_fields.isdisjoint(component.model_fields)

    assert not inspect.iscoroutinefunction(PhysicalClientLifetimeRegistrar.lease)


def test_physical_ai_class_udf_lifetime_inventory_is_exact() -> None:
    package = import_module("archetype.physical_ai")
    package_dir = Path(package.__file__).parent
    discovered: dict[str, ast.ClassDef] = {}

    for path in sorted(package_dir.rglob("*.py")):
        tree = ast.parse(path.read_text())
        daft_aliases = {
            alias.asname or alias.name
            for node in tree.body
            if isinstance(node, ast.Import)
            for alias in node.names
            if alias.name == "daft"
        }
        cls_aliases = {
            alias.asname or alias.name
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module == "daft"
            for alias in node.names
            if alias.name == "cls"
        }
        relative = path.relative_to(package_dir).with_suffix("")
        module_parts = relative.parts[:-1] if relative.name == "__init__" else relative.parts
        module_name = ".".join(("archetype", "physical_ai", *module_parts))

        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            decorated = False
            for decorator in node.decorator_list:
                target = decorator.func if isinstance(decorator, ast.Call) else decorator
                if (
                    isinstance(target, ast.Attribute)
                    and target.attr == "cls"
                    and isinstance(target.value, ast.Name)
                    and target.value.id in daft_aliases
                ) or (isinstance(target, ast.Name) and target.id in cls_aliases):
                    decorated = True
                    break
            if decorated:
                discovered[f"{module_name}.{node.name}"] = node

    passive = {
        "archetype.physical_ai.manipulation._EnvStepper": "EnvClient",
        "archetype.physical_ai.manipulation._FramedEnvStepper": "EnvClient",
        "archetype.physical_ai.policy._PolicyCaller": "PolicyClient",
        "archetype.physical_ai.policy._PolicyCallerNoRefs": "PolicyClient",
    }
    scratch = "archetype.physical_ai.mujoco_cartpole._CartpoleStepper"
    assert set(discovered) == {*passive, scratch}

    for qualified_name, annotation in passive.items():
        constructor = next(
            node
            for node in discovered[qualified_name].body
            if isinstance(node, ast.FunctionDef) and node.name == "__init__"
        )
        assert [arg.arg for arg in constructor.args.args] == ["self", "client"]
        assert ast.unparse(constructor.args.args[1].annotation) == annotation
        assert constructor.args.vararg is None
        assert constructor.args.kwarg is None
        assert len(constructor.body) == 1
        assignment = constructor.body[0]
        assert isinstance(assignment, ast.Assign)
        assert len(assignment.targets) == 1
        target = assignment.targets[0]
        assert isinstance(target, ast.Attribute)
        assert isinstance(target.value, ast.Name)
        assert (target.value.id, target.attr) == ("self", "_client")
        assert isinstance(assignment.value, ast.Name)
        assert assignment.value.id == "client"
        assert not any(
            isinstance(node, (ast.Call, ast.Import, ast.ImportFrom))
            for node in ast.walk(constructor)
        )

    stepper = discovered[scratch]
    constructor = next(
        node
        for node in stepper.body
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )
    assert [arg.arg for arg in constructor.args.args] == ["self", "xml", "substeps"]
    assert [ast.unparse(arg.annotation) for arg in constructor.args.args[1:]] == [
        "str",
        "int",
    ]
    imports = {
        alias.name
        for node in ast.walk(constructor)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert imports == {"mujoco"}

    def call_name(target: ast.expr) -> str:
        if isinstance(target, ast.Name):
            return target.id
        if isinstance(target, ast.Attribute):
            return f"{call_name(target.value)}.{target.attr}"
        return ast.dump(target, include_attributes=False)

    assert {
        call_name(node.func) for node in ast.walk(constructor) if isinstance(node, ast.Call)
    } == {"mj_model.from_xml_string", "mj_data"}
    assert {
        node.name
        for node in stepper.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }.isdisjoint({"aclose", "close", "shutdown"})

    boundary = import_module("archetype.physical_ai.boundary")
    assert not hasattr(boundary, "WORKER_LOCAL_SCRATCH_EXCEPTIONS")


@pytest.mark.asyncio
@pytest.mark.parametrize("operation_name", ["evaluate", "sweep"])
async def test_handlers_register_unique_providers_before_first_effect(
    operation_name: str,
) -> None:
    handlers = _physical_handlers()
    events: list[tuple[str, object]] = []
    registrar = _RegistrarSpy(events)
    lifecycle = _FirstEffectLifecycle(events)
    env = _CloseableEnv(events=events)
    policy = _CloseablePolicy(events=events)
    storage = StorageConfig()

    if operation_name == "evaluate":
        operation = EvaluatePhysicalTask(
            config=PhysicalTaskEvalConfig(
                suite="lifetime",
                task_id=1,
                trials=1,
                max_steps=1,
                storage=storage,
            ),
            env_client=env,
            policy_client=policy,
        )
        handler = handlers.evaluate_physical_task
    else:
        operation = SweepPhysicalInstructions(
            config=InstructionSweepConfig(
                suite="lifetime",
                task_id=1,
                variants=("reach",),
                seeds_per_variant=1,
                max_steps=1,
                storage=storage,
            ),
            env_client=env,
            policy_client=policy,
        )
        handler = handlers.sweep_physical_instructions

    with pytest.raises(_StopWorkflow):
        await handler(
            registrar,
            object(),
            lifecycle,
            object(),
            operation,
        )

    assert events == [
        ("register", env),
        ("register", policy),
        ("workflow", "create_closing_world"),
    ]


@pytest.mark.asyncio
async def test_missing_async_close_rejects_before_workflow_effect(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effects: list[str] = []

    async def forbidden_effect(
        _self: WorldLifecycle,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        effects.append("create_closing_world")

    monkeypatch.setattr(WorldLifecycle, "create_closing_world", forbidden_effect)
    resources = build_test_runtime(tmp_path)
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="lifetime",
            task_id=1,
            trials=1,
            max_steps=1,
        ),
        env_client=_EnvWithoutClose(),
    )
    try:
        with pytest.raises(TypeError, match=r"async aclose\(\)"):
            await resources.dispatcher.apply(operation)
        assert effects == []
        assert _physical_owner_clients(resources) == ()
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_cleanup_owner_reservation_failure_precedes_world_creation(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effects: list[str] = []

    async def forbidden_effect(
        _self: WorldLifecycle,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        effects.append("create_closing_world")
        raise _StopWorkflow

    def fail_reservation() -> None:
        raise RuntimeError("cleanup reservation unavailable")

    monkeypatch.setattr(WorldLifecycle, "create_closing_world", forbidden_effect)
    resources = build_test_runtime(tmp_path)
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes = handler.args[0]
    monkeypatch.setattr(
        lifetimes._cleanup_lifetimes,
        "reserve",
        fail_reservation,
        raising=False,
    )
    provider = _CloseableEnv()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="reservation",
            task_id=1,
            trials=1,
            max_steps=1,
        ),
        env_client=provider,
        policy_client=provider,
    )
    try:
        with pytest.raises(RuntimeError, match="cleanup reservation unavailable"):
            await resources.dispatcher.apply(operation)
        assert effects == []
        assert _world_cleanup_owner_names(resources) == ()
    finally:
        await resources.aclose()
    assert provider.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation_name", ["evaluate", "sweep"])
async def test_retain_failure_compensates_the_exact_new_evidence_world(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    operation_name: str,
) -> None:
    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace=f"retain-failure-{operation_name}",
    )
    resources = build_test_runtime(tmp_path)
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes, worlds, _lifecycle, storage_service = handler.args[:4]

    def fail_retain(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("retain failed after create")

    monkeypatch.setattr(lifetimes._cleanup_lifetimes, "retain", fail_retain)
    provider = _CloseableEnv()
    if operation_name == "evaluate":
        operation = EvaluatePhysicalTask(
            config=PhysicalTaskEvalConfig(
                suite="retain-failure",
                task_id=1,
                trials=1,
                max_steps=1,
                storage=storage,
            ),
            env_client=provider,
            policy_client=provider,
        )
    else:
        operation = SweepPhysicalInstructions(
            config=InstructionSweepConfig(
                suite="retain-failure",
                task_id=1,
                variants=("reach",),
                seeds_per_variant=1,
                max_steps=1,
                storage=storage,
            ),
            env_client=provider,
            policy_client=provider,
        )

    try:
        with pytest.raises(RuntimeError, match="retain failed after create"):
            await resources.dispatcher.apply(operation)

        assert await worlds.list_worlds() == []
        records = await storage_service.get_control_catalog(storage).list_worlds()
        assert len(records) == 1
        assert records[0].writer_mode == "cleanup_only"
        assert records[0].status == "destroyed"
        assert _world_cleanup_owner_names(resources) == ()
    finally:
        await resources.aclose()
    assert provider.close_calls == 1


@pytest.mark.asyncio
async def test_activation_owner_recovers_cleanup_promoted_before_metadata_failure(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="retain-partial-bind",
    )
    resources = build_test_runtime(tmp_path)
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes, worlds, _lifecycle, storage_service = handler.args[:4]
    original_associate = lifetimes._cleanup_lifetimes._associate_providers
    associate_calls = 0

    def fail_first_association(entry: Any, provider_ids: frozenset[int]) -> None:
        nonlocal associate_calls
        associate_calls += 1
        if entry.lease is not None:
            raise RuntimeError("provider association failed after exact promotion")
        original_associate(entry, provider_ids)

    monkeypatch.setattr(
        lifetimes._cleanup_lifetimes,
        "_associate_providers",
        fail_first_association,
    )
    provider = _CloseableEnv()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="retain-partial-bind",
            task_id=1,
            trials=1,
            max_steps=1,
            storage=storage,
        ),
        env_client=provider,
        policy_client=provider,
    )
    try:
        with pytest.raises(
            RuntimeError,
            match="provider association failed after exact promotion",
        ):
            await resources.dispatcher.apply(operation)

        assert associate_calls == 2
        assert await worlds.list_worlds() == []
        records = await storage_service.get_control_catalog(storage).list_worlds()
        assert len(records) == 1
        assert records[0].writer_mode == "cleanup_only"
        assert records[0].status == "destroyed"
        assert _world_cleanup_owner_names(resources) == ()
    finally:
        await resources.aclose()
    assert provider.close_calls == 1


@pytest.mark.asyncio
async def test_cancellation_waits_for_retain_failure_compensation(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="retain-cancel",
    )
    resources = build_test_runtime(tmp_path)
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes, worlds, lifecycle, storage_service = handler.args[:4]
    entered = asyncio.Event()
    release = asyncio.Event()
    original_destroy = lifecycle.destroy_world

    def fail_retain(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("retain failed before cancellation")

    async def blocked_destroy(
        world_id: object,
        *,
        lease: Any | None = None,
    ) -> None:
        entered.set()
        await release.wait()
        await original_destroy(world_id, lease=lease)

    monkeypatch.setattr(lifetimes._cleanup_lifetimes, "retain", fail_retain)
    monkeypatch.setattr(lifecycle, "destroy_world", blocked_destroy)
    provider = _CloseableEnv()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="retain-cancel",
            task_id=1,
            trials=1,
            max_steps=1,
            storage=storage,
        ),
        env_client=provider,
        policy_client=provider,
    )
    task = asyncio.create_task(resources.dispatcher.apply(operation))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        task.cancel()
        release.set()
        with pytest.raises(BaseExceptionGroup) as caught:
            await asyncio.wait_for(task, timeout=1.0)
        leaves = _exception_leaves(caught.value)
        assert [type(leaf) for leaf in leaves] == [RuntimeError, asyncio.CancelledError]
        assert str(leaves[0]) == "retain failed before cancellation"

        assert await worlds.list_worlds() == []
        records = await storage_service.get_control_catalog(storage).list_worlds()
        assert len(records) == 1
        assert records[0].writer_mode == "cleanup_only"
        assert records[0].status == "destroyed"
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await resources.aclose()
    assert provider.close_calls == 1


@pytest.mark.asyncio
async def test_prebind_validation_failure_remains_owned_for_shutdown_retry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="retain-prebind-retry",
    )
    resources = build_test_runtime(tmp_path)
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes, worlds, lifecycle, storage_service = handler.args[:4]
    original_validate = worlds.validate_cleanup_lease
    original_destroy = lifecycle.destroy_world
    validation_calls = 0
    events: list[str] = []

    def fail_first_two_validations(
        lease: Any,
        *,
        world_id: object,
    ) -> None:
        nonlocal validation_calls
        validation_calls += 1
        if validation_calls <= 2:
            raise RuntimeError(f"exact cleanup validation failed {validation_calls}")
        original_validate(lease, world_id=world_id)

    async def track_destroy(
        world_id: object,
        *,
        lease: Any | None = None,
    ) -> None:
        events.append("destroy")
        await original_destroy(world_id, lease=lease)

    class _OrderedProvider(_CloseableEnv):
        async def aclose(self) -> None:
            events.append("provider")
            await super().aclose()

    monkeypatch.setattr(worlds, "validate_cleanup_lease", fail_first_two_validations)
    monkeypatch.setattr(lifecycle, "destroy_world", track_destroy)
    provider = _OrderedProvider()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="retain-prebind-retry",
            task_id=1,
            trials=1,
            max_steps=1,
            storage=storage,
        ),
        env_client=provider,
        policy_client=provider,
    )

    try:
        with pytest.raises(BaseExceptionGroup) as caught:
            await resources.dispatcher.apply(operation)
        leaves = _exception_leaves(caught.value)
        assert [type(leaf) for leaf in leaves] == [RuntimeError, RuntimeError]
        assert [str(leaf) for leaf in leaves] == [
            "exact cleanup validation failed 1",
            "exact cleanup validation failed 2",
        ]

        records = await storage_service.get_control_catalog(storage).list_worlds()
        assert len(records) == 1
        world_id = records[0].world_id
        assert records[0].status == "active"
        assert records[0].writer_mode == "cleanup_only"
        assert await worlds.contains(world_id)
        assert len(_world_cleanup_owner_names(resources)) == 1

        await resources.aclose()

        assert validation_calls >= 3
        assert not await worlds.contains(world_id)
        final_record = await storage_service.get_control_catalog(storage).get_world(world_id)
        assert final_record is not None
        assert final_record.status == "destroyed"
        assert events == ["destroy", "provider"]
    finally:
        await resources.aclose()
    assert provider.close_calls == 1


@pytest.mark.asyncio
async def test_ambiguous_remote_registration_cleanup_is_owned_until_shutdown_retry(
    tmp_path,
) -> None:
    resources = build_runtime_resources(
        RuntimeBootstrapConfig(
            control_catalog_config=ControlCatalogConfig(
                remote_url="https://catalog.invalid",
                remote_token="test-token",
                catalog_dir=tmp_path / "catalogs",
            )
        )
    )
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    _lifetimes, worlds, _lifecycle, storage_service = handler.args[:4]
    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="remote-registration-owner",
    )
    catalog = storage_service.get_control_catalog(storage)
    assert isinstance(catalog, RemoteControlCatalog)
    await catalog._client.aclose()

    retained_row: dict[str, object] | None = None
    retirement_attempts = 0
    events: list[str] = []

    async def transport(request: httpx.Request) -> httpx.Response:
        nonlocal retained_row, retirement_attempts
        if request.url.path.endswith("/protocol"):
            return httpx.Response(200, json={"catalog_protocol_version": 8})
        if request.url.path.endswith("/protocol/v8/worlds"):
            retained_row = json.loads(request.content)
            return httpx.Response(503, json={"error": "status_mirror_failed"})
        if request.method == "GET":
            if retained_row is None:
                return httpx.Response(404, json={"error": "not_found"})
            return httpx.Response(200, json=retained_row)
        if request.url.path.endswith("/retire"):
            retirement_attempts += 1
            events.append(f"retire:{retirement_attempts}")
            if retirement_attempts <= 2:
                return httpx.Response(503, json={"error": "retirement_unavailable"})
            assert retained_row is not None
            retained_row = {**retained_row, "status": "destroyed"}
            return httpx.Response(
                200,
                json={
                    **retained_row,
                    "ok": True,
                    "disposition": "retired",
                    "catalog_protocol_version": 8,
                    "gateway_protocol_version": 8,
                    "catalog_status": "destroyed",
                    "world_status": "destroyed",
                },
            )
        raise AssertionError(f"unexpected remote catalog request: {request.method} {request.url}")

    catalog._client = httpx.AsyncClient(transport=httpx.MockTransport(transport))

    class _OrderedProvider(_CloseableEnv):
        async def aclose(self) -> None:
            events.append("provider")
            await super().aclose()

    provider = _OrderedProvider()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="remote-registration-owner",
            task_id=1,
            trials=1,
            max_steps=1,
            storage=storage,
        ),
        env_client=provider,
        policy_client=provider,
    )
    try:
        with pytest.raises(BaseExceptionGroup):
            await resources.dispatcher.apply(operation)

        assert retained_row is not None
        assert retained_row["status"] == "active"
        assert retained_row["writer_mode"] == "cleanup_only"
        assert not await worlds.contains(str(retained_row["world_id"]))
        assert len(_world_cleanup_owner_names(resources)) == 1
        assert provider.close_calls == 0

        await resources.aclose()

        assert retained_row["status"] == "destroyed"
        assert retirement_attempts == 3
        assert events == ["retire:1", "retire:2", "retire:3", "provider"]
        assert provider.close_calls == 1
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_failed_retain_compensation_remains_owned_for_shutdown_retry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="retain-compensation-retry",
    )
    resources = build_test_runtime(tmp_path)
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes, worlds, lifecycle, storage_service = handler.args[:4]
    original_destroy = lifecycle.destroy_world
    events: list[str] = []
    destroy_calls = 0

    class _OrderedProvider(_CloseableEnv):
        async def aclose(self) -> None:
            events.append("provider")
            await super().aclose()

    def fail_retain(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("retain failed after create")

    async def fail_destroy_once(
        world_id: object,
        *,
        lease: Any | None = None,
    ) -> None:
        nonlocal destroy_calls
        destroy_calls += 1
        events.append(f"destroy:{destroy_calls}")
        if destroy_calls == 1:
            raise RuntimeError("destroy failed once")
        await original_destroy(world_id, lease=lease)

    monkeypatch.setattr(lifetimes._cleanup_lifetimes, "retain", fail_retain)
    monkeypatch.setattr(lifecycle, "destroy_world", fail_destroy_once)
    provider = _OrderedProvider()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="retain-compensation-retry",
            task_id=1,
            trials=1,
            max_steps=1,
            storage=storage,
        ),
        env_client=provider,
        policy_client=provider,
    )

    with pytest.raises(BaseExceptionGroup) as caught:
        await resources.dispatcher.apply(operation)
    assert [str(exc) for exc in caught.value.exceptions] == [
        "retain failed after create",
        "destroy failed once",
    ]

    records = await storage_service.get_control_catalog(storage).list_worlds()
    assert len(records) == 1
    world_id = records[0].world_id
    assert records[0].writer_mode == "cleanup_only"
    assert records[0].status == "active"
    assert await worlds.contains(world_id)
    assert len(_world_cleanup_owner_names(resources)) == 1

    await resources.aclose()

    assert destroy_calls == 2
    assert not await worlds.contains(world_id)
    assert provider.close_calls == 1
    assert events == ["destroy:1", "destroy:2", "provider"]
    final_record = await storage_service.get_control_catalog(storage).get_world(world_id)
    assert final_record is not None
    assert final_record.status == "destroyed"


@pytest.mark.asyncio
async def test_cleanup_originated_cancellation_preserves_retain_failure_and_retry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = StorageConfig(
        uri=str(tmp_path / "store"),
        namespace="retain-cleanup-cancel",
    )
    resources = build_test_runtime(tmp_path)
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes, worlds, lifecycle, storage_service = handler.args[:4]
    original_destroy = lifecycle.destroy_world
    destroy_calls = 0

    def fail_retain(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("retain failed after create")

    async def cancel_destroy_once(
        world_id: object,
        *,
        lease: Any | None = None,
    ) -> None:
        nonlocal destroy_calls
        destroy_calls += 1
        if destroy_calls == 1:
            raise asyncio.CancelledError("cleanup cancelled internally")
        await original_destroy(world_id, lease=lease)

    monkeypatch.setattr(lifetimes._cleanup_lifetimes, "retain", fail_retain)
    monkeypatch.setattr(lifecycle, "destroy_world", cancel_destroy_once)
    provider = _CloseableEnv()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="retain-cleanup-cancel",
            task_id=1,
            trials=1,
            max_steps=1,
            storage=storage,
        ),
        env_client=provider,
        policy_client=provider,
    )

    with pytest.raises(BaseExceptionGroup) as caught:
        await resources.dispatcher.apply(operation)
    leaves = _exception_leaves(caught.value)
    assert [type(leaf) for leaf in leaves] == [RuntimeError, asyncio.CancelledError]
    assert [str(leaf) for leaf in leaves] == [
        "retain failed after create",
        "cleanup cancelled internally",
    ]

    record = (await storage_service.get_control_catalog(storage).list_worlds())[0]
    assert record.status == "active"
    assert record.writer_mode == "cleanup_only"
    assert await worlds.contains(record.world_id)
    assert len(_world_cleanup_owner_names(resources)) == 1

    await resources.aclose()

    assert destroy_calls == 2
    assert not await worlds.contains(record.world_id)
    assert provider.close_calls == 1


@pytest.mark.asyncio
async def test_reused_and_dual_role_clients_have_one_process_owner_and_close_once(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def stop_after_registration(
        _self: WorldLifecycle,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        raise _StopWorkflow

    monkeypatch.setattr(WorldLifecycle, "create_closing_world", stop_after_registration)
    resources = build_test_runtime(tmp_path)
    client = _CloseableEnv()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="lifetime",
            task_id=1,
            trials=1,
            max_steps=1,
        ),
        env_client=client,
        policy_client=client,
    )

    for _ in range(2):
        with pytest.raises(_StopWorkflow):
            await resources.dispatcher.apply(operation)

    assert _physical_owner_clients(resources) == (client,)
    await resources.aclose()
    assert client.close_calls == 1


@pytest.mark.asyncio
async def test_cancelled_evaluation_retains_provider_until_owned_cleanup(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()

    async def block_after_registration(
        _self: WorldLifecycle,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        entered.set()
        await asyncio.Future()

    monkeypatch.setattr(WorldLifecycle, "create_closing_world", block_after_registration)
    resources = build_test_runtime(tmp_path)
    env = _CloseableEnv()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="lifetime",
            task_id=1,
            trials=1,
            max_steps=1,
        ),
        env_client=env,
    )
    task = asyncio.create_task(resources.dispatcher.apply(operation))
    await asyncio.wait_for(entered.wait(), timeout=1.0)

    assert _physical_owner_clients(resources) == (env,)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)
    assert _physical_owner_clients(resources) == (env,)
    assert env.close_calls == 0

    await resources.aclose()
    assert env.close_calls == 1


@pytest.mark.asyncio
async def test_failed_provider_close_retains_only_failure_and_retries(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def stop_after_registration(
        _self: WorldLifecycle,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        raise _StopWorkflow

    monkeypatch.setattr(WorldLifecycle, "create_closing_world", stop_after_registration)
    resources = build_test_runtime(tmp_path)
    env = _CloseableEnv(fail_first_close=True)
    policy = _CloseablePolicy()
    operation = SweepPhysicalInstructions(
        config=InstructionSweepConfig(
            suite="lifetime",
            task_id=1,
            variants=("reach",),
            seeds_per_variant=1,
            max_steps=1,
        ),
        env_client=env,
        policy_client=policy,
    )
    with pytest.raises(_StopWorkflow):
        await resources.dispatcher.apply(operation)
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes = handler.args[0]

    assert set(_physical_owner_clients(resources)) == {env, policy}
    with pytest.raises(RuntimeShutdownError) as caught:
        await resources.aclose()
    assert caught.value.phase == "workflow-handles"
    assert _physical_owner_clients(resources) == (env,)
    assert _physical_lifetime_clients(lifetimes) == (env,)
    assert env.close_calls == 1
    assert policy.close_calls == 1
    assert resources._storage is not None

    await resources.aclose()
    assert _physical_owner_clients(resources) == ()
    assert _physical_lifetime_clients(lifetimes) == ()
    assert env.close_calls == 2
    assert policy.close_calls == 1
