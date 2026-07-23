# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Red contracts for structural world-registry synchronization."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from importlib import import_module
from typing import Any

import pytest

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("world.tick.atomic_visibility"),
]


@dataclass(slots=True)
class _World:
    world_id: str
    name: str
    tick: int = 0


def _registry_type() -> type[Any]:
    # The absent canonical family is the intentional pre-PR-2 red seam.
    return import_module("archetype.world.registry").WorldRegistry


async def _insert_two_worlds() -> tuple[Any, _World, _World]:
    registry = _registry_type()()
    first = _World(
        world_id="00000000-0000-7000-8000-000000000010",
        name="first",
    )
    second = _World(
        world_id="00000000-0000-7000-8000-000000000020",
        name="second",
    )
    await registry.insert(first)
    await registry.insert(second)
    return registry, first, second


async def test_child_task_cannot_inherit_held_world_operation() -> None:
    registry, first, _second = await _insert_two_worlds()
    attempted = asyncio.Event()
    entered = asyncio.Event()

    async def child() -> None:
        attempted.set()
        async with registry.operation(first.world_id) as resolved:
            assert resolved is first
            entered.set()

    task: asyncio.Task[None] | None = None
    try:
        async with registry.operation(first.world_id) as resolved:
            assert resolved is first
            task = asyncio.create_task(child())
            await asyncio.wait_for(attempted.wait(), timeout=0.5)
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(entered.wait(), timeout=0.05)

        assert task is not None
        await asyncio.wait_for(task, timeout=0.5)
        assert entered.is_set(), "the child enters only after the parent releases"
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_same_world_serializes_while_sibling_overlaps() -> None:
    registry, first, second = await _insert_two_worlds()
    same_attempted = asyncio.Event()
    same_entered = asyncio.Event()
    sibling_attempted = asyncio.Event()
    sibling_entered = asyncio.Event()

    async def enter(
        world: _World,
        attempted: asyncio.Event,
        entered: asyncio.Event,
    ) -> None:
        attempted.set()
        async with registry.operation(world.world_id) as resolved:
            assert resolved is world
            entered.set()

    same_task: asyncio.Task[None] | None = None
    sibling_task: asyncio.Task[None] | None = None
    try:
        async with registry.operation(first.world_id):
            same_task = asyncio.create_task(enter(first, same_attempted, same_entered))
            sibling_task = asyncio.create_task(enter(second, sibling_attempted, sibling_entered))
            await asyncio.wait_for(
                asyncio.gather(same_attempted.wait(), sibling_attempted.wait()),
                timeout=0.5,
            )
            await asyncio.wait_for(sibling_entered.wait(), timeout=0.5)
            assert not same_entered.is_set(), (
                "a same-world operation must wait while a sibling progresses"
            )

        assert same_task is not None and sibling_task is not None
        await asyncio.wait_for(
            asyncio.gather(same_task, sibling_task),
            timeout=0.5,
        )
        assert same_entered.is_set()
    finally:
        tasks = [task for task in (same_task, sibling_task) if task is not None and not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_cancellation_releases_exact_world_lock() -> None:
    registry, first, _second = await _insert_two_worlds()
    entered = asyncio.Event()
    block_forever: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    async def holder() -> None:
        async with registry.operation(first.world_id):
            entered.set()
            await block_forever

    task = asyncio.create_task(holder())
    await asyncio.wait_for(entered.wait(), timeout=0.5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async with asyncio.timeout(0.5):
        async with registry.operation(first.world_id) as resolved:
            assert resolved is first


async def test_target_tick_snapshot_rejects_sticky_close() -> None:
    registry, first, _second = await _insert_two_worlds()
    first.tick = 4

    assert registry.target_tick(first.world_id) == 4
    lease = await registry.begin_close(first.world_id)
    with pytest.raises(RuntimeError, match="closing"):
        registry.target_tick(first.world_id)

    await registry.finish_close(lease)
    with pytest.raises(KeyError):
        registry.target_tick(first.world_id)


async def test_multiworld_operations_sort_before_acquiring_locks() -> None:
    """Opposite caller order cannot produce an A→B / B→A deadlock.

    Holding A lets the forward waiter queue on A first. An implementation that
    honors the reverse caller order lets the second waiter hold B while waiting
    on A; releasing the outer A lock then deterministically deadlocks them.
    Sorting both requests to A→B makes both queue on A without holding B.
    """

    registry, first, second = await _insert_two_worlds()
    forward_attempted = asyncio.Event()
    reverse_attempted = asyncio.Event()
    entered: list[str] = []

    async def acquire(
        label: str,
        world_ids: list[str],
        attempted: asyncio.Event,
    ) -> None:
        attempted.set()
        async with registry.operations(world_ids):
            entered.append(label)
            await asyncio.sleep(0)

    forward: asyncio.Task[None] | None = None
    reverse: asyncio.Task[None] | None = None
    try:
        async with registry.operation(first.world_id):
            forward = asyncio.create_task(
                acquire(
                    "forward",
                    [first.world_id, second.world_id],
                    forward_attempted,
                )
            )
            await asyncio.wait_for(forward_attempted.wait(), timeout=0.5)
            await asyncio.sleep(0)

            reverse = asyncio.create_task(
                acquire(
                    "reverse",
                    [second.world_id, first.world_id],
                    reverse_attempted,
                )
            )
            await asyncio.wait_for(reverse_attempted.wait(), timeout=0.5)
            await asyncio.sleep(0)

        assert forward is not None and reverse is not None
        await asyncio.wait_for(asyncio.gather(forward, reverse), timeout=0.5)
        assert entered == ["forward", "reverse"]
    finally:
        tasks = [task for task in (forward, reverse) if task is not None and not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
