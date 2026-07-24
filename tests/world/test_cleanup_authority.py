# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""RED contracts for explicit exact-world cleanup authority.

The absent PR-4 capability is imported inside each test so every node remains
independently collectible against the landed PR-3 candidate.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from archetype.core.component import Component
from archetype.core.config import RunConfig
from archetype.world.errors import WorldClosingError

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("world.tick.atomic_visibility"),
]


class _Evidence(Component):
    value: str = ""


class _CleanupWorld:
    def __init__(self, world_id: str, name: str, receipt_type: type[Any]) -> None:
        self.world_id = world_id
        self.run_id = world_id[:-1] + "9"
        self.name = name
        self.tick = 0
        self.has_prepared_tick_commit = False
        self.last_committed_receipt = None
        self._receipt_type = receipt_type
        self.entities: dict[int, list[Component]] = {}
        self.next_entity_id = 1
        self.core_step_ticks: list[int] = []
        self.events: list[str] = []

    async def create_entity(self, components: list[Component]) -> int:
        entity_id = self.next_entity_id
        self.next_entity_id += 1
        self.entities[entity_id] = list(components)
        self.events.append(f"stage:{entity_id}")
        return entity_id

    async def update_entity(
        self,
        entity_id: int,
        components: list[Component],
    ) -> None:
        if entity_id not in self.entities:
            raise KeyError(entity_id)
        self.entities[entity_id] = list(components)
        self.events.append(f"update:{entity_id}")

    async def step(self, run_config: RunConfig, **input_kwargs: object) -> Any:
        del run_config, input_kwargs
        committed_tick = self.tick
        self.core_step_ticks.append(committed_tick)
        self.events.append(f"core:{committed_tick}")
        self.tick += 1
        receipt = self._receipt_type(
            world_id=self.world_id,
            run_id=self.run_id,
            committed_tick=committed_tick,
            visibility_token=f"manifest-{self.name}-{committed_tick}",
            commands_applied=0,
        )
        self.last_committed_receipt = receipt
        return receipt


class _Lifecycle:
    def __init__(
        self,
        registry: Any,
        reconcile: Any,
        events: list[str],
    ) -> None:
        self.registry = registry
        self.reconcile = reconcile
        self.events = events
        self.calls: list[tuple[str, object]] = []

    async def destroy_world(self, world_id: str, *, lease: object) -> None:
        self.calls.append((str(world_id), lease))
        self.registry.validate_cleanup_lease(lease, world_id=world_id)
        async with self.registry.cleanup_operation(lease) as world:
            self.events.append("destroy:reconcile")
            await self.reconcile(self.registry, world_id, world)
            assert self.registry.pending_receipt(world_id) is None
        self.events.append("destroy:finish")
        await self.registry.finish_close(lease)


def _cleanup_type() -> type[Any]:
    return import_module("archetype.world.cleanup").WorldCleanup


async def test_cleanup_capability_is_exact_world_and_non_ambient() -> None:
    from archetype.core.interfaces import CommittedTickReceipt
    from archetype.world.registry import WorldRegistry
    from archetype.world.simulation import reconcile_committed_work_locked

    WorldCleanup = _cleanup_type()
    first = _CleanupWorld(
        "00000000-0000-7000-8000-000000000081",
        "first",
        CommittedTickReceipt,
    )
    sibling = _CleanupWorld(
        "00000000-0000-7000-8000-000000000082",
        "sibling",
        CommittedTickReceipt,
    )
    registry = WorldRegistry()
    await registry.insert(first)
    await registry.insert(sibling)
    lease = await registry.begin_close(first.world_id)
    lifecycle = _Lifecycle(registry, reconcile_committed_work_locked, [])
    validation_calls: list[str] = []
    dynamic_registry: Any = registry
    validate_cleanup_lease = dynamic_registry.validate_cleanup_lease

    def validate_exact_lease(candidate: Any, *, world_id: Any) -> None:
        validation_calls.append(str(world_id))
        validate_cleanup_lease(candidate, world_id=world_id)

    dynamic_registry.validate_cleanup_lease = validate_exact_lease
    cleanup = WorldCleanup(
        registry=registry,
        lifecycle=lifecycle,
        world_id=first.world_id,
        lease=lease,
    )

    entity_id = await cleanup.stage_teardown([_Evidence(value="teardown")])
    await cleanup.update_retained(entity_id, [_Evidence(value="retained")])

    assert cleanup.world_id == first.world_id
    assert first.events == [f"stage:{entity_id}", f"update:{entity_id}"]
    assert first.entities[entity_id] == [_Evidence(value="retained")]
    assert sibling.entities == {}
    assert validation_calls == [first.world_id, first.world_id, first.world_id]

    with pytest.raises(ValueError, match="bound|world"):
        WorldCleanup(
            registry=registry,
            lifecycle=lifecycle,
            world_id=sibling.world_id,
            lease=lease,
        )

    other_registry = WorldRegistry()
    await other_registry.insert(_CleanupWorld(first.world_id, "replacement", CommittedTickReceipt))
    with pytest.raises(ValueError, match="registry|lease"):
        WorldCleanup(
            registry=other_registry,
            lifecycle=lifecycle,
            world_id=first.world_id,
            lease=lease,
        )

    async def child_without_capability() -> None:
        async with registry.operation(first.world_id):
            pytest.fail("a child task must not inherit cleanup authority")

    with pytest.raises(WorldClosingError):
        await asyncio.create_task(child_without_capability())
    async with registry.operation(sibling.world_id) as live_sibling:
        assert live_sibling is sibling

    for method_name in ("stage_teardown", "update_retained", "commit", "finish"):
        assert "world_id" not in inspect.signature(getattr(WorldCleanup, method_name)).parameters

    source_path = Path(inspect.getsourcefile(WorldCleanup) or "")
    tree = ast.parse(source_path.read_text())
    assert not any(
        isinstance(node, ast.Name) and node.id == "ContextVar" for node in ast.walk(tree)
    )
    assert all(
        not (
            isinstance(node, ast.ImportFrom)
            and (node.module or "").startswith("archetype.commands")
        )
        for node in ast.walk(tree)
    )


async def test_cleanup_retry_reconciles_receipt_before_finish_close() -> None:
    from archetype.core.interfaces import CommittedTickReceipt
    from archetype.world.registry import WorldRegistry
    from archetype.world.simulation import (
        PostCommitProjectionError,
        RequiredProjector,
        reconcile_committed_work_locked,
    )

    WorldCleanup = _cleanup_type()
    events: list[str] = []
    projected: list[CommittedTickReceipt] = []

    async def fail_once(receipt: CommittedTickReceipt) -> None:
        projected.append(receipt)
        events.append(f"project:{receipt.committed_tick}")
        if len(projected) == 1:
            raise RuntimeError("projection unavailable")

    world = _CleanupWorld(
        "00000000-0000-7000-8000-000000000091",
        "cleanup-retry",
        CommittedTickReceipt,
    )
    sibling = _CleanupWorld(
        "00000000-0000-7000-8000-000000000092",
        "sibling",
        CommittedTickReceipt,
    )
    registry = WorldRegistry()
    await registry.insert(
        world,
        required_projector=RequiredProjector(
            consumer_name="test.cleanup-retry",
            project=fail_once,
        ),
    )
    await registry.insert(sibling)
    lease = await registry.begin_close(world.world_id)
    lifecycle = _Lifecycle(
        registry,
        reconcile_committed_work_locked,
        events,
    )
    cleanup = WorldCleanup(
        registry=registry,
        lifecycle=lifecycle,
        world_id=world.world_id,
        lease=lease,
    )

    await cleanup.stage_teardown([_Evidence(value="cleanup")])
    with pytest.raises(PostCommitProjectionError) as raised:
        await cleanup.commit(RunConfig())

    receipt = raised.value.receipt
    assert registry.pending_receipt(world.world_id) is receipt
    assert world.core_step_ticks == [0]

    await cleanup.finish()

    assert projected == [receipt, receipt]
    assert world.core_step_ticks == [0], "cleanup retry must not replay committed work"
    assert events == [
        "project:0",
        "destroy:reconcile",
        "project:0",
        "destroy:finish",
    ]
    assert lifecycle.calls == [(world.world_id, lease)]
    assert not await registry.contains(world.world_id)
    assert await registry.contains(sibling.world_id)
    async with registry.operation(sibling.world_id) as live_sibling:
        assert live_sibling is sibling
