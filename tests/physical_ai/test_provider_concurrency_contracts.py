# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Concurrency contracts for process-owned physical-AI providers."""

from __future__ import annotations

import asyncio
import pickle
import threading
from typing import Any

import pytest

from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.physical_ai import handlers as physical_handlers
from archetype.physical_ai.manipulation import ManipStatus, ManipTask
from archetype.physical_ai.models import (
    EvaluatePhysicalTask,
    InstructionSweepConfig,
    PhysicalTaskEvalConfig,
    PhysicalTaskEvalReport,
    SweepPhysicalInstructions,
)
from archetype.runtime_resources import RuntimeCloseState
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.models import ComponentTypeRef, DestroyWorld, QueryComponents, Run
from tests._runtime import build_test_runtime


class _StopWorkflow(Exception):
    """Bound a handler immediately after its first workflow effect."""


class _Provider:
    def __init__(self, *, close_events: list[str] | None = None) -> None:
        self.close_calls = 0
        self.close_events = close_events

    def task_language(self) -> str:
        return "reach"

    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        del env_id, seed
        return {}

    def step(
        self,
        env_ids: list[int],
        actions: list[list[float]],
    ) -> list[dict[str, Any]]:
        del actions
        return [{"done": True, "success": True} for _ in env_ids]

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
        if self.close_events is not None:
            self.close_events.append("provider")


class _UnserializableProvider(_Provider):
    def __init__(self) -> None:
        super().__init__()
        self.live_handle = threading.Lock()


class _NonCallableEnvClose:
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

    aclose = object()


class _SyncCloseProvider(_Provider):
    def aclose(self) -> None:
        self.close_calls += 1


class _AsyncResetProvider(_Provider):
    async def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        del env_id, seed
        return {}


class _NonterminalProvider(_Provider):
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
                "done": False,
                "success": False,
            }
            for _ in env_ids
        ]


class _PropertyPolicyAct:
    @property
    def act(self) -> Any:
        raise AssertionError("static lease validation must not invoke provider properties")

    async def aclose(self) -> None:
        pass


def _physical_owner_clients(resources: Any) -> tuple[object, ...]:
    return tuple(
        reservation._resource
        for owner, reservation in resources._owners.items()
        if owner.startswith("physical-ai:client:")
    )


@pytest.mark.asyncio
async def test_lease_creation_validates_all_clients_before_atomic_registration(
    tmp_path,
) -> None:
    resources = build_test_runtime(tmp_path)
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes = handler.args[0]
    env = _Provider()
    policy = _Provider()

    invalid_envs = (
        (object(), r"environment.*synchronous reset\(\)"),
        (_NonCallableEnvClose(), r"async aclose\(\)"),
        (_SyncCloseProvider(), r"async aclose\(\)"),
        (_AsyncResetProvider(), r"synchronous reset\(\)"),
    )
    for invalid, message in invalid_envs:
        with pytest.raises(TypeError, match=message):
            lifetimes.lease(invalid)
        assert _physical_owner_clients(resources) == ()
    with pytest.raises(TypeError, match=r"policy.*synchronous act\(\)"):
        lifetimes.lease(env, _PropertyPolicyAct())
    assert _physical_owner_clients(resources) == ()

    with pytest.raises(TypeError, match=r"environment.*serializable by Daft"):
        lifetimes.lease(_UnserializableProvider())
    assert _physical_owner_clients(resources) == ()
    with pytest.raises(TypeError, match=r"policy.*serializable by Daft"):
        lifetimes.lease(env, _UnserializableProvider())
    assert _physical_owner_clients(resources) == ()

    lifetimes.lease(env, policy)
    assert set(_physical_owner_clients(resources)) == {env, policy}

    await resources.aclose()
    assert env.close_calls == 1
    assert policy.close_calls == 1


@pytest.mark.asyncio
async def test_lease_accepts_the_exact_broader_daft_serialization_contract(
    tmp_path,
) -> None:
    class _LocalProvider(_Provider):
        pass

    provider = _LocalProvider()
    with pytest.raises((AttributeError, pickle.PicklingError)):
        pickle.dumps(provider)

    resources = build_test_runtime(tmp_path)
    handler = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes = handler.args[0]
    lifetimes.lease(provider, provider)

    assert _physical_owner_clients(resources) == (provider,)
    await resources.aclose()
    assert provider.close_calls == 1


@pytest.mark.asyncio
async def test_returned_nonterminal_world_is_durable_evidence_not_live_provider_work(
    tmp_path,
) -> None:
    resources = build_test_runtime(tmp_path)
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="physical-inert")
    provider = _NonterminalProvider()
    try:
        report = await resources.dispatcher.apply(
            EvaluatePhysicalTask(
                config=PhysicalTaskEvalConfig(
                    suite="inert",
                    task_id=1,
                    trials=1,
                    max_steps=1,
                    storage=storage,
                ),
                env_client=provider,
                policy_client=provider,
            )
        )

        frame = await resources.dispatcher.apply(
            QueryComponents(
                components=tuple(
                    ComponentTypeRef.from_type(component) for component in (ManipStatus, ManipTask)
                ),
                world_id=report.world_id,
                run_id=report.run_id,
                storage_config=storage,
            )
        )
        assert frame.count_rows() == 1
        row = frame.to_pylist()[0]
        assert row["manipstatus__done"] is False
        assert row["maniptask__suite"] == "inert"
        with pytest.raises(KeyError, match=report.world_id):
            await resources.dispatcher.apply(
                Run(
                    world_id=report.world_id,
                    run_config=RunConfig(num_steps=1),
                )
            )
    finally:
        await resources.aclose()
    assert provider.close_calls == 1


@pytest.mark.asyncio
async def test_cancel_after_world_creation_retires_writer_before_releasing_lease(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    world_ids: list[str] = []

    async def block_episode(
        _registry: object,
        _storage: object,
        world_id: object,
        _world: object,
        _config: object,
        **_kwargs: object,
    ) -> None:
        world_ids.append(str(world_id))
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(physical_handlers.simulation, "_run_episode_locked", block_episode)
    resources = build_test_runtime(tmp_path)
    provider = _Provider()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="cancel",
            task_id=1,
            trials=1,
            max_steps=1,
        ),
        env_client=provider,
        policy_client=provider,
    )
    task = asyncio.create_task(resources.dispatcher.apply(operation))
    await entered.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(world_ids) == 1
    assert _physical_owner_clients(resources) == (provider,)
    with pytest.raises(KeyError, match=world_ids[0]):
        await resources.dispatcher.apply(
            Run(
                world_id=world_ids[0],
                run_config=RunConfig(num_steps=1),
            )
        )

    await resources.aclose()
    assert provider.close_calls == 1


@pytest.mark.asyncio
async def test_public_destroy_joins_physical_retirement_for_the_exact_world(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    world_ids: list[str] = []

    async def paused_evaluation(
        _registry: object,
        _storage: object,
        world: Any,
        operation: EvaluatePhysicalTask,
    ) -> PhysicalTaskEvalReport:
        world_id = str(world.world_id)
        world_ids.append(world_id)
        entered.set()
        await release.wait()
        return PhysicalTaskEvalReport(
            suite=operation.config.suite,
            task_id=operation.config.task_id,
            instruction=operation.config.instruction,
            world_id=world_id,
            run_id="00000000-0000-7000-8000-000000000091",
        )

    monkeypatch.setattr(
        physical_handlers,
        "_evaluate_physical_task_in_world",
        paused_evaluation,
    )
    resources = build_test_runtime(tmp_path)
    provider = _Provider()
    evaluation = asyncio.create_task(
        resources.dispatcher.apply(
            EvaluatePhysicalTask(
                config=PhysicalTaskEvalConfig(
                    suite="destroy-race",
                    task_id=1,
                    trials=1,
                    max_steps=1,
                ),
                env_client=provider,
                policy_client=provider,
            )
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        destroy = asyncio.create_task(
            resources.dispatcher.apply(DestroyWorld(world_id=world_ids[0]))
        )
        await asyncio.sleep(0)
        assert not destroy.done()

        release.set()
        report, destroyed = await asyncio.wait_for(
            asyncio.gather(evaluation, destroy),
            timeout=2.0,
        )

        assert report.world_id == world_ids[0]
        assert destroyed is None
    finally:
        release.set()
        await resources.aclose()
    assert provider.close_calls == 1


@pytest.mark.asyncio
async def test_failed_retirement_is_process_owned_and_retried_before_close(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resources = build_test_runtime(tmp_path)
    close_events: list[str] = []
    provider = _Provider(close_events=close_events)
    dispatcher = resources.dispatcher
    physical = dispatcher._registry.resolve_name("evaluate_physical_task").handler
    worlds = physical.args[1]
    scheduler = dispatcher._scheduler
    original_cancel_world = scheduler.cancel_world
    cancel_calls = 0

    async def fail_once(world_id: object) -> int:
        nonlocal cancel_calls
        cancel_calls += 1
        close_events.append(f"cancel:{cancel_calls}")
        if cancel_calls == 1:
            raise RuntimeError("cancel failed once")
        return await original_cancel_world(world_id)

    monkeypatch.setattr(scheduler, "cancel_world", fail_once)
    with pytest.raises(RuntimeError, match="cancel failed once"):
        await dispatcher.apply(
            EvaluatePhysicalTask(
                config=PhysicalTaskEvalConfig(
                    suite="retry",
                    task_id=1,
                    trials=1,
                    max_steps=1,
                ),
                env_client=provider,
                policy_client=provider,
            )
        )

    retained = await worlds.list_worlds()
    assert len(retained) == 1
    world_id = str(retained[0].world_id)
    assert await worlds.contains(world_id)

    await resources.aclose()

    assert cancel_calls == 2
    assert not await worlds.contains(world_id)
    assert provider.close_calls == 1
    assert close_events == ["cancel:1", "cancel:2", "provider"]


@pytest.mark.asyncio
async def test_completed_physical_retirement_handle_cannot_close_replacement(
    tmp_path,
) -> None:
    resources = build_test_runtime(tmp_path)
    physical = resources.dispatcher._registry.resolve_name("evaluate_physical_task").handler
    lifetimes, worlds, lifecycle = physical.args[:3]
    provider = _Provider()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="stale-retirement")

    async with lifetimes.lease(provider, provider) as workflow_lifetime:
        world, lease = await lifecycle.create_closing_world(
            WorldConfig(
                world_id="00000000-0000-7000-8000-0000000000b1",
                name="original",
            ),
            storage,
            activation_owner=workflow_lifetime,
        )
        stale_retirement = workflow_lifetime.retain_evidence_world(
            world.world_id,
            lease,
        )
        await resources.dispatcher.apply(DestroyWorld(world_id=world.world_id))
        assert not await worlds.contains(world.world_id)

        # A new registry entry is a replacement authority even when it holds
        # the same Python world object and durable ID.
        await worlds.insert(world, storage_config=storage)
        replacement_lease = await worlds.begin_close(world.world_id)

        await stale_retirement.aclose()

        worlds.validate_cleanup_lease(
            replacement_lease,
            world_id=world.world_id,
        )
        assert await worlds.live_world(world.world_id) is world
        await worlds.finish_close(replacement_lease)

    await resources.aclose()
    assert provider.close_calls == 1


@pytest.mark.asyncio
async def test_shared_eval_sweep_provider_serializes_while_disjoint_provider_overlaps(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = {label: asyncio.Event() for label in ("holder", "waiter", "disjoint")}
    release = {label: asyncio.Event() for label in entered}
    active: set[str] = set()
    overlaps: list[frozenset[str]] = []

    async def block_at_first_effect(
        _self: WorldLifecycle,
        config: Any,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        label = str(config.name).split(":", maxsplit=2)[1]
        active.add(label)
        overlaps.append(frozenset(active))
        entered[label].set()
        try:
            await release[label].wait()
        finally:
            active.remove(label)
        raise _StopWorkflow(label)

    monkeypatch.setattr(WorldLifecycle, "create_closing_world", block_at_first_effect)
    resources = build_test_runtime(tmp_path)
    shared = _Provider()
    disjoint = _Provider()
    holder = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="holder",
            task_id=1,
            trials=1,
            max_steps=1,
        ),
        env_client=shared,
        policy_client=shared,
    )
    waiter = SweepPhysicalInstructions(
        config=InstructionSweepConfig(
            suite="waiter",
            task_id=2,
            variants=("reach",),
            seeds_per_variant=1,
            max_steps=1,
        ),
        env_client=shared,
        policy_client=shared,
    )
    independent = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="disjoint",
            task_id=3,
            trials=1,
            max_steps=1,
        ),
        env_client=disjoint,
        policy_client=disjoint,
    )

    holder_task = asyncio.create_task(resources.dispatcher.apply(holder))
    await entered["holder"].wait()

    waiter_started = asyncio.Event()

    async def run_waiter() -> Any:
        waiter_started.set()
        return await resources.dispatcher.apply(waiter)

    waiter_task = asyncio.create_task(run_waiter())
    await waiter_started.wait()
    await asyncio.sleep(0)

    disjoint_task = asyncio.create_task(resources.dispatcher.apply(independent))
    await entered["disjoint"].wait()
    assert not entered["waiter"].is_set()
    assert frozenset({"holder", "disjoint"}) in overlaps

    release["holder"].set()
    with pytest.raises(_StopWorkflow, match="holder"):
        await holder_task
    await entered["waiter"].wait()

    release["waiter"].set()
    release["disjoint"].set()
    with pytest.raises(_StopWorkflow, match="waiter"):
        await waiter_task
    with pytest.raises(_StopWorkflow, match="disjoint"):
        await disjoint_task

    await resources.aclose()
    assert shared.close_calls == 1
    assert disjoint.close_calls == 1


@pytest.mark.asyncio
async def test_runtime_close_waits_for_active_dual_role_provider_lease(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def block_at_first_effect(
        _self: WorldLifecycle,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        entered.set()
        await release.wait()
        raise _StopWorkflow("active")

    monkeypatch.setattr(WorldLifecycle, "create_closing_world", block_at_first_effect)
    resources = build_test_runtime(tmp_path)
    provider = _Provider()
    operation = EvaluatePhysicalTask(
        config=PhysicalTaskEvalConfig(
            suite="close-race",
            task_id=1,
            trials=1,
            max_steps=1,
        ),
        env_client=provider,
        policy_client=provider,
    )

    operation_task = asyncio.create_task(resources.dispatcher.apply(operation))
    await entered.wait()
    close_task = asyncio.create_task(resources.aclose())

    async def wait_for_close_boundary() -> None:
        while resources.close_state is RuntimeCloseState.OPEN:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait_for_close_boundary(), timeout=1.0)
    assert not close_task.done()
    assert provider.close_calls == 0

    release.set()
    with pytest.raises(_StopWorkflow, match="active"):
        await operation_task
    await close_task
    assert provider.close_calls == 1

    await resources.aclose()
    assert provider.close_calls == 1
