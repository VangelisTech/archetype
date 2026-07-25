# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Process-ownership contracts for physical-AI operation providers."""

from __future__ import annotations

import ast
import asyncio
import inspect
from contextlib import asynccontextmanager
from importlib import import_module
from pathlib import Path
from typing import Any

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


def test_worker_local_scratch_exception_is_exact_and_non_io() -> None:
    boundary = import_module("archetype.physical_ai.boundary")
    assert boundary.WORKER_LOCAL_SCRATCH_EXCEPTIONS == (
        "archetype.physical_ai.mujoco_cartpole._CartpoleStepper",
    )

    module = import_module("archetype.physical_ai.mujoco_cartpole")
    tree = ast.parse(Path(module.__file__).read_text())
    stepper = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "_CartpoleStepper"
    )
    constructor = next(
        node
        for node in stepper.body
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )
    imports = {
        alias.name
        for node in ast.walk(constructor)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert imports == {"mujoco"}
    assert {
        node.name
        for node in stepper.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }.isdisjoint({"aclose", "close", "shutdown"})


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
