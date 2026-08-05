# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Red contracts for required post-commit projection.

The managed imports intentionally remain local to each test while PR-2 source
does not exist.  Missing ``archetype.world`` symbols are therefore an explicit
red seam, not a collection or fixture failure.  Once the seam exists, the
tests exercise retention and ordering entirely through public behavior.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

import pytest

from archetype.core.config import RunConfig
from archetype.core.hooks import HookRegistry, PostTick

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("world.tick.atomic_visibility"),
]


class _ReceiptWorld:
    """Minimal core-world double: one invocation commits exactly one tick."""

    def __init__(self, receipt_type: type[Any], events: list[str]) -> None:
        self.world_id = "00000000-0000-7000-8000-000000000002"
        self.run_id = "00000000-0000-7000-8000-000000000003"
        self.name = "required-projection"
        self.tick = 0
        self.hooks = HookRegistry()
        self.core_step_ticks: list[int] = []
        self._receipt_type = receipt_type
        self._events = events

    async def step(self, run_config: RunConfig, **input_kwargs: object) -> Any:
        del run_config, input_kwargs
        committed_tick = self.tick
        self.core_step_ticks.append(committed_tick)
        self._events.append(f"core:{committed_tick}")
        self.tick += 1
        await self.hooks.fire(PostTick(world_id=self.world_id, tick=self.tick, results={}))
        return self._receipt_type(
            world_id=self.world_id,
            run_id=self.run_id,
            committed_tick=committed_tick,
            visibility_token=f"manifest-{committed_tick}",
            commands_applied=0,
        )


async def test_lazy_application_binds_projector_before_first_tick() -> None:
    from archetype.core.interfaces import CommittedTickReceipt
    from archetype.world.registry import WorldRegistry
    from archetype.world.simulation import RequiredProjector, step

    projected: list[CommittedTickReceipt] = []

    async def required(receipt: CommittedTickReceipt) -> None:
        projected.append(receipt)

    registry = WorldRegistry()
    world = _ReceiptWorld(CommittedTickReceipt, [])
    projector = RequiredProjector(
        consumer_name="test.lazy-application",
        project=required,
    )
    await registry.insert(world)
    await registry.bind_required_projector(world.world_id, projector)
    await registry.bind_required_projector(world.world_id, projector)

    await step(registry, world.world_id, RunConfig())

    assert registry.required_projector(world.world_id) is projector
    assert [receipt.committed_tick for receipt in projected] == [0]
    with pytest.raises(ValueError, match="different required projector"):
        await registry.bind_required_projector(
            world.world_id,
            RequiredProjector(
                consumer_name="test.other",
                project=required,
            ),
        )


async def test_fail_once_projector_retries_exact_receipt_before_next_tick() -> None:
    registry_module = import_module("archetype.world.registry")
    simulation_module = import_module("archetype.world.simulation")
    from archetype.core.interfaces import CommittedTickReceipt

    WorldRegistry = registry_module.WorldRegistry
    PostCommitProjectionError = simulation_module.PostCommitProjectionError
    RequiredProjector = simulation_module.RequiredProjector
    step = simulation_module.step

    events: list[str] = []
    projected: list[CommittedTickReceipt] = []

    async def fail_once(receipt: CommittedTickReceipt) -> None:
        projected.append(receipt)
        events.append(f"project:{receipt.committed_tick}")
        if len(projected) == 1:
            raise RuntimeError("required projection interrupted")

    projector = RequiredProjector(
        consumer_name="test.required-index",
        project=fail_once,
    )
    world = _ReceiptWorld(CommittedTickReceipt, events)
    registry = WorldRegistry()
    await registry.insert(world, required_projector=projector)

    with pytest.raises(PostCommitProjectionError) as raised:
        await step(registry, world.world_id, RunConfig())

    first_receipt = raised.value.receipt
    assert raised.value.consumer_name == "test.required-index"
    assert isinstance(raised.value.__cause__, RuntimeError)
    assert str(raised.value.__cause__) == "required projection interrupted"
    assert world.tick == 1, "post-commit failure cannot roll back the published tick"
    assert world.core_step_ticks == [0]
    assert projected == [first_receipt]
    assert events == ["core:0", "project:0"]

    await step(registry, world.world_id, RunConfig())

    assert world.tick == 2
    assert world.core_step_ticks == [0, 1], "tick 0 compute must never be replayed"
    assert len(projected) == 3
    assert projected[0] is projected[1], (
        "the retained exact receipt must be retried before another core step"
    )
    assert projected[0].identity == first_receipt.identity
    assert projected[2].committed_tick == 1
    assert events == [
        "core:0",
        "project:0",
        "project:0",
        "core:1",
        "project:1",
    ]


async def test_advisory_post_tick_failure_cannot_suppress_required_projection() -> None:
    registry_module = import_module("archetype.world.registry")
    simulation_module = import_module("archetype.world.simulation")
    from archetype.core.interfaces import CommittedTickReceipt

    WorldRegistry = registry_module.WorldRegistry
    RequiredProjector = simulation_module.RequiredProjector
    step = simulation_module.step

    events: list[str] = []
    projected: list[CommittedTickReceipt] = []
    world = _ReceiptWorld(CommittedTickReceipt, events)

    async def failing_advisory(event: PostTick) -> None:
        events.append(f"post_tick:{event.tick}")
        raise RuntimeError("advisory observer failed")

    async def required(receipt: CommittedTickReceipt) -> None:
        projected.append(receipt)
        events.append(f"required:{receipt.committed_tick}")

    world.hooks.add(PostTick, failing_advisory)
    projector = RequiredProjector(
        consumer_name="test.required-index",
        project=required,
    )
    registry = WorldRegistry()
    await registry.insert(world, required_projector=projector)

    await step(registry, world.world_id, RunConfig())

    assert world.tick == 1
    assert [receipt.committed_tick for receipt in projected] == [0]
    assert events == ["core:0", "post_tick:1", "required:0"]
    assert all(
        handler is not projector.project
        for _event_type, _handle, handler, _mode in world.hooks.items()
    ), "required projection must never be registered in HookRegistry"
