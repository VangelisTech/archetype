# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Red contracts for sticky, retryable, exact-world registry cleanup."""

from __future__ import annotations

from importlib import import_module
from typing import Any

import pytest

from archetype.core.config import RunConfig

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
