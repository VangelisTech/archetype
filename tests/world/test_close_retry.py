# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Red contracts for sticky, retryable, exact-world registry cleanup."""

from __future__ import annotations

import asyncio
from importlib import import_module
from typing import Any

import pytest

from archetype.core.config import RunConfig, StorageConfig
from archetype.core.hooks import HookRegistry, OnDestroy

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("world.tick.atomic_visibility"),
]


class _ReceiptWorld:
    def __init__(
        self,
        *,
        world_id: str,
        name: str,
        receipt_type: type[Any],
    ) -> None:
        self.world_id = world_id
        self.run_id = world_id[:-1] + "9"
        self.name = name
        self.tick = 0
        self._receipt_type = receipt_type
        self.core_step_ticks: list[int] = []

    async def step(self, run_config: RunConfig, **input_kwargs: object) -> Any:
        del run_config, input_kwargs
        committed_tick = self.tick
        self.core_step_ticks.append(committed_tick)
        self.tick += 1
        return self._receipt_type(
            world_id=self.world_id,
            run_id=self.run_id,
            committed_tick=committed_tick,
            visibility_token=f"manifest-{self.name}-{committed_tick}",
            commands_applied=0,
        )


class _CleanupFailed(RuntimeError):
    pass


class _DestroyCatalog:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.statuses: list[tuple[str, str]] = []

    async def set_world_status(self, world_id: str, status: str) -> None:
        self.events.append(f"status:{status}")
        self.statuses.append((world_id, status))


class _DestroyStorage:
    def __init__(self, catalog: _DestroyCatalog) -> None:
        self.catalog = catalog

    def get_control_catalog(self, _storage_config: StorageConfig) -> _DestroyCatalog:
        return self.catalog


def _managed_api() -> tuple[Any, Any]:
    # Import the absent canonical family first: this is the approved red seam.
    return (
        import_module("archetype.world.registry"),
        import_module("archetype.world.simulation"),
    )


async def test_failed_cleanup_retains_exact_binding_and_retry_finishes_close() -> None:
    registry_module, simulation_module = _managed_api()
    from archetype.core.interfaces import CommittedTickReceipt

    WorldRegistry = registry_module.WorldRegistry
    PostCommitProjectionError = simulation_module.PostCommitProjectionError
    RequiredProjector = simulation_module.RequiredProjector
    retry_required_projection = simulation_module.retry_required_projection
    step = simulation_module.step

    projected: list[CommittedTickReceipt] = []

    async def fail_once(receipt: CommittedTickReceipt) -> None:
        projected.append(receipt)
        if len(projected) == 1:
            raise RuntimeError("projection unavailable")

    world = _ReceiptWorld(
        world_id="00000000-0000-7000-8000-000000000030",
        name="closing",
        receipt_type=CommittedTickReceipt,
    )
    sibling = _ReceiptWorld(
        world_id="00000000-0000-7000-8000-000000000040",
        name="sibling",
        receipt_type=CommittedTickReceipt,
    )
    projector = RequiredProjector(
        consumer_name="test.close-retry",
        project=fail_once,
    )
    registry = WorldRegistry()
    await registry.insert(world, required_projector=projector)
    await registry.insert(sibling)

    with pytest.raises(PostCommitProjectionError) as projection_failure:
        await step(registry, world.world_id, RunConfig())
    receipt = projection_failure.value.receipt
    assert registry.pending_receipt(world.world_id) is receipt
    assert world.core_step_ticks == [0]

    lease = await registry.begin_close(world.world_id)
    assert await registry.begin_close(world.world_id) is lease, (
        "closing retains one exact cleanup lease across retries"
    )

    with pytest.raises(RuntimeError):
        async with registry.operation(world.world_id):
            pytest.fail("sticky close must reject all new public work")
    async with registry.operation(sibling.world_id) as live_sibling:
        assert live_sibling is sibling

    with pytest.raises(_CleanupFailed, match="sandbox teardown interrupted"):
        async with registry.cleanup_operation(lease) as retained_world:
            assert retained_world is world
            raise _CleanupFailed("sandbox teardown interrupted")

    assert await registry.begin_close(world.world_id) is lease
    assert registry.pending_receipt(world.world_id) is receipt
    with pytest.raises(RuntimeError):
        async with registry.operation(world.world_id):
            pytest.fail("cleanup failure cannot reopen public admission")
    with pytest.raises(RuntimeError):
        await registry.finish_close(lease)

    async with registry.cleanup_operation(lease) as retained_world:
        assert retained_world is world, "cleanup retry must receive the same strong world"

    await retry_required_projection(registry, world.world_id, lease=lease)

    assert projected == [receipt, receipt], (
        "cleanup retry must use the retained projector and exact pending receipt"
    )
    assert registry.pending_receipt(world.world_id) is None
    assert world.core_step_ticks == [0], "projection retry cannot replay the committed tick"

    await registry.finish_close(lease)

    assert registry.pending_receipt(world.world_id) is None
    with pytest.raises(KeyError):
        async with registry.operation(world.world_id):
            pytest.fail("finished close must remove the world")
    async with registry.operation(sibling.world_id) as live_sibling:
        assert live_sibling is sibling, "one world's failed close cannot poison its sibling"


async def test_public_destroy_reconciles_prepared_commit_before_projection_and_status(
    monkeypatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module, simulation_module = _managed_api()
    from archetype.core.interfaces import CommittedTickReceipt

    events: list[str] = []

    class _PreparedWorld(_ReceiptWorld):
        def __init__(self) -> None:
            super().__init__(
                world_id="00000000-0000-7000-8000-000000000047",
                name="prepared-destroy",
                receipt_type=CommittedTickReceipt,
            )
            self.has_prepared_tick_commit = True
            self.last_committed_receipt = None
            self.hooks = HookRegistry()

        async def reconcile_prepared_tick(self) -> CommittedTickReceipt:
            events.append("publish:0")
            self.has_prepared_tick_commit = False
            self.tick = 1
            receipt = CommittedTickReceipt(
                world_id=self.world_id,
                run_id=self.run_id,
                committed_tick=0,
                visibility_token="manifest-prepared-0",
                commands_applied=1,
            )
            self.last_committed_receipt = receipt
            return receipt

    async def project(receipt: CommittedTickReceipt) -> None:
        events.append(f"project:{receipt.committed_tick}")

    async def on_destroy(_event: OnDestroy) -> None:
        events.append("destroy")

    world = _PreparedWorld()
    world.hooks.add(OnDestroy, on_destroy)
    storage_config = StorageConfig()
    registry = registry_module.WorldRegistry()
    await registry.insert(
        world,
        storage_config=storage_config,
        required_projector=simulation_module.RequiredProjector(
            consumer_name="test.prepared-destroy",
            project=project,
        ),
    )
    catalog = _DestroyCatalog(events)
    lifecycle = lifecycle_module.WorldLifecycle(_DestroyStorage(catalog), registry)
    monkeypatch.setattr(lifecycle_module, "AsyncWorld", _PreparedWorld)

    await lifecycle.destroy_world(world.world_id)

    assert events == ["publish:0", "project:0", "destroy", "status:destroyed"]
    assert catalog.statuses == [(world.world_id, "destroyed")]
    assert registry.pending_receipt(world.world_id) is None
    assert not await registry.contains(world.world_id)


async def test_public_destroy_retry_after_status_failure_does_not_refire_on_destroy(
    monkeypatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module, _simulation_module = _managed_api()
    from archetype.core.interfaces import CommittedTickReceipt

    events: list[str] = []

    class _FailOnceDestroyCatalog(_DestroyCatalog):
        def __init__(self) -> None:
            super().__init__(events)
            self.attempts = 0

        async def set_world_status(self, world_id: str, status: str) -> None:
            self.attempts += 1
            self.events.append(f"status:{status}")
            if self.attempts == 1:
                raise RuntimeError("control catalog unavailable")
            self.statuses.append((world_id, status))

    async def on_destroy(_event: OnDestroy) -> None:
        events.append("destroy")

    world = _ReceiptWorld(
        world_id="00000000-0000-7000-8000-000000000049",
        name="status-retry",
        receipt_type=CommittedTickReceipt,
    )
    world.has_prepared_tick_commit = False
    world.hooks = HookRegistry()
    world.hooks.add(OnDestroy, on_destroy)
    storage_config = StorageConfig()
    registry = registry_module.WorldRegistry()
    await registry.insert(world, storage_config=storage_config)
    catalog = _FailOnceDestroyCatalog()
    lifecycle = lifecycle_module.WorldLifecycle(_DestroyStorage(catalog), registry)
    monkeypatch.setattr(lifecycle_module, "AsyncWorld", _ReceiptWorld)

    with pytest.raises(RuntimeError, match="control catalog unavailable"):
        await lifecycle.destroy_world(world.world_id)

    assert await registry.contains(world.world_id)
    assert events == ["destroy", "status:destroyed"]

    await lifecycle.destroy_world(world.world_id)

    assert events == ["destroy", "status:destroyed", "status:destroyed"]
    assert catalog.statuses == [(world.world_id, "destroyed")]
    assert not await registry.contains(world.world_id)


async def test_public_destroy_retry_after_ambiguous_status_does_not_refire_on_destroy(
    monkeypatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module, _simulation_module = _managed_api()
    from archetype.core.interfaces import CommittedTickReceipt

    events: list[str] = []

    class _AmbiguousOnceDestroyCatalog(_DestroyCatalog):
        def __init__(self) -> None:
            super().__init__(events)
            self.attempts = 0

        async def set_world_status(self, world_id: str, status: str) -> None:
            self.attempts += 1
            self.events.append(f"status:{status}")
            self.statuses.append((world_id, status))
            if self.attempts == 1:
                raise RuntimeError("destroyed status response lost")

    async def on_destroy(_event: OnDestroy) -> None:
        events.append("destroy")

    world = _ReceiptWorld(
        world_id="00000000-0000-7000-8000-000000000059",
        name="ambiguous-status-retry",
        receipt_type=CommittedTickReceipt,
    )
    world.has_prepared_tick_commit = False
    world.hooks = HookRegistry()
    world.hooks.add(OnDestroy, on_destroy)
    storage_config = StorageConfig()
    registry = registry_module.WorldRegistry()
    await registry.insert(world, storage_config=storage_config)
    catalog = _AmbiguousOnceDestroyCatalog()
    lifecycle = lifecycle_module.WorldLifecycle(_DestroyStorage(catalog), registry)
    monkeypatch.setattr(lifecycle_module, "AsyncWorld", _ReceiptWorld)

    with pytest.raises(RuntimeError, match="destroyed status response lost"):
        await lifecycle.destroy_world(world.world_id)

    assert await registry.contains(world.world_id)
    assert events == ["destroy", "status:destroyed"]
    assert catalog.statuses == [(world.world_id, "destroyed")]

    await lifecycle.destroy_world(world.world_id)

    assert events == ["destroy", "status:destroyed", "status:destroyed"]
    assert catalog.statuses == [
        (world.world_id, "destroyed"),
        (world.world_id, "destroyed"),
    ]
    assert not await registry.contains(world.world_id)


async def test_public_destroy_retry_after_hook_cancellation_refires_unfinished_on_destroy(
    monkeypatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module, _simulation_module = _managed_api()
    from archetype.core.interfaces import CommittedTickReceipt

    events: list[str] = []
    hook_started = asyncio.Event()
    hook_attempts = 0

    async def on_destroy(_event: OnDestroy) -> None:
        nonlocal hook_attempts
        hook_attempts += 1
        events.append(f"destroy:{hook_attempts}")
        if hook_attempts == 1:
            hook_started.set()
            await asyncio.Future()

    world = _ReceiptWorld(
        world_id="00000000-0000-7000-8000-000000000064",
        name="cancelled-hook-retry",
        receipt_type=CommittedTickReceipt,
    )
    world.has_prepared_tick_commit = False
    world.hooks = HookRegistry()
    world.hooks.add(OnDestroy, on_destroy)
    storage_config = StorageConfig()
    registry = registry_module.WorldRegistry()
    await registry.insert(world, storage_config=storage_config)
    catalog = _DestroyCatalog(events)
    lifecycle = lifecycle_module.WorldLifecycle(_DestroyStorage(catalog), registry)
    monkeypatch.setattr(lifecycle_module, "AsyncWorld", _ReceiptWorld)

    destroy = asyncio.create_task(lifecycle.destroy_world(world.world_id))
    await hook_started.wait()
    destroy.cancel("destroy caller cancelled during hook")
    with pytest.raises(asyncio.CancelledError, match="destroy caller cancelled during hook"):
        await destroy

    assert await registry.contains(world.world_id)
    assert events == ["destroy:1"]

    await lifecycle.destroy_world(world.world_id)

    assert events == ["destroy:1", "destroy:2", "status:destroyed"]
    assert catalog.statuses == [(world.world_id, "destroyed")]
    assert not await registry.contains(world.world_id)


async def test_public_destroy_retry_after_post_hook_cancellation_does_not_refire_on_destroy(
    monkeypatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module, _simulation_module = _managed_api()
    from archetype.core.interfaces import CommittedTickReceipt

    events: list[str] = []

    class _BlockOnceDestroyCatalog(_DestroyCatalog):
        def __init__(self) -> None:
            super().__init__(events)
            self.attempts = 0
            self.first_attempt_started = asyncio.Event()

        async def set_world_status(self, world_id: str, status: str) -> None:
            self.attempts += 1
            self.events.append(f"status:{status}")
            if self.attempts == 1:
                self.first_attempt_started.set()
                await asyncio.Future()
            self.statuses.append((world_id, status))

    async def on_destroy(_event: OnDestroy) -> None:
        events.append("destroy")

    world = _ReceiptWorld(
        world_id="00000000-0000-7000-8000-000000000069",
        name="cancelled-status-retry",
        receipt_type=CommittedTickReceipt,
    )
    world.has_prepared_tick_commit = False
    world.hooks = HookRegistry()
    world.hooks.add(OnDestroy, on_destroy)
    storage_config = StorageConfig()
    registry = registry_module.WorldRegistry()
    await registry.insert(world, storage_config=storage_config)
    catalog = _BlockOnceDestroyCatalog()
    lifecycle = lifecycle_module.WorldLifecycle(_DestroyStorage(catalog), registry)
    monkeypatch.setattr(lifecycle_module, "AsyncWorld", _ReceiptWorld)

    destroy = asyncio.create_task(lifecycle.destroy_world(world.world_id))
    await catalog.first_attempt_started.wait()
    destroy.cancel("destroy caller cancelled after hook")
    with pytest.raises(asyncio.CancelledError, match="destroy caller cancelled after hook"):
        await destroy

    assert await registry.contains(world.world_id)
    assert events == ["destroy", "status:destroyed"]

    await lifecycle.destroy_world(world.world_id)

    assert events == ["destroy", "status:destroyed", "status:destroyed"]
    assert catalog.statuses == [(world.world_id, "destroyed")]
    assert not await registry.contains(world.world_id)


async def test_public_destroy_retries_pending_projection_before_destruction_effects(
    monkeypatch,
) -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module, simulation_module = _managed_api()
    from archetype.core.interfaces import CommittedTickReceipt

    events: list[str] = []
    attempts: list[CommittedTickReceipt] = []

    async def fail_twice(receipt: CommittedTickReceipt) -> None:
        attempts.append(receipt)
        events.append(f"project:{receipt.committed_tick}")
        if len(attempts) < 3:
            raise RuntimeError("projection unavailable")

    async def on_destroy(_event: OnDestroy) -> None:
        events.append("destroy")

    world = _ReceiptWorld(
        world_id="00000000-0000-7000-8000-000000000048",
        name="pending-destroy",
        receipt_type=CommittedTickReceipt,
    )
    world.hooks = HookRegistry()
    world.hooks.add(OnDestroy, on_destroy)
    world.has_prepared_tick_commit = False
    storage_config = StorageConfig()
    registry = registry_module.WorldRegistry()
    await registry.insert(
        world,
        storage_config=storage_config,
        required_projector=simulation_module.RequiredProjector(
            consumer_name="test.pending-destroy",
            project=fail_twice,
        ),
    )
    catalog = _DestroyCatalog(events)
    lifecycle = lifecycle_module.WorldLifecycle(_DestroyStorage(catalog), registry)
    monkeypatch.setattr(lifecycle_module, "AsyncWorld", _ReceiptWorld)

    with pytest.raises(simulation_module.PostCommitProjectionError):
        await simulation_module.step(registry, world.world_id, RunConfig())
    receipt = registry.pending_receipt(world.world_id)
    assert receipt is not None

    with pytest.raises(simulation_module.PostCommitProjectionError):
        await lifecycle.destroy_world(world.world_id)

    assert registry.pending_receipt(world.world_id) is receipt
    assert catalog.statuses == []
    assert events == ["project:0", "project:0"]
    assert await registry.contains(world.world_id)

    await lifecycle.destroy_world(world.world_id)

    assert events == ["project:0", "project:0", "project:0", "destroy", "status:destroyed"]
    assert catalog.statuses == [(world.world_id, "destroyed")]
    assert not await registry.contains(world.world_id)


async def test_cleanup_lease_cannot_authorize_a_sibling_world() -> None:
    registry_module, simulation_module = _managed_api()
    from archetype.core.interfaces import CommittedTickReceipt

    WorldRegistry = registry_module.WorldRegistry
    PostCommitProjectionError = simulation_module.PostCommitProjectionError
    RequiredProjector = simulation_module.RequiredProjector
    retry_required_projection = simulation_module.retry_required_projection
    step = simulation_module.step

    attempts: list[CommittedTickReceipt] = []

    async def fail_once(receipt: CommittedTickReceipt) -> None:
        attempts.append(receipt)
        if len(attempts) == 1:
            raise RuntimeError("projector fail once")

    first = _ReceiptWorld(
        world_id="00000000-0000-7000-8000-000000000050",
        name="first-closing",
        receipt_type=CommittedTickReceipt,
    )
    second = _ReceiptWorld(
        world_id="00000000-0000-7000-8000-000000000060",
        name="second-closing",
        receipt_type=CommittedTickReceipt,
    )
    registry = WorldRegistry()
    await registry.insert(first)
    await registry.insert(
        second,
        required_projector=RequiredProjector(
            consumer_name="test.second-projector",
            project=fail_once,
        ),
    )

    with pytest.raises(PostCommitProjectionError) as projection_failure:
        await step(registry, second.world_id, RunConfig())
    pending = projection_failure.value.receipt
    assert registry.pending_receipt(second.world_id) is pending

    first_lease = await registry.begin_close(first.world_id)
    second_lease = await registry.begin_close(second.world_id)

    async with registry.cleanup_operation(first_lease) as exact_first:
        assert exact_first is first
        with pytest.raises(RuntimeError):
            async with registry.operation(second.world_id):
                pytest.fail("cleanup authority must not ambiently bypass sibling close")

    with pytest.raises((RuntimeError, ValueError)) as mismatch:
        await retry_required_projection(
            registry,
            second.world_id,
            lease=first_lease,
        )
    assert "lease" in str(mismatch.value).lower() or "world" in str(mismatch.value).lower()
    assert registry.pending_receipt(second.world_id) is pending
    assert attempts == [pending], "a mismatched lease cannot invoke the sibling projector"

    await retry_required_projection(
        registry,
        second.world_id,
        lease=second_lease,
    )
    assert attempts == [pending, pending]
    assert registry.pending_receipt(second.world_id) is None

    await registry.finish_close(second_lease)
    await registry.finish_close(first_lease)
