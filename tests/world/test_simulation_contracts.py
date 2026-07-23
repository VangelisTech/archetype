# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Managed simulation contracts not covered by the core tick suite."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest

from archetype.core.config import RunConfig
from archetype.core.interfaces import CommittedTickReceipt
from archetype.world.simulation import (
    PostCommitProjectionError,
    RequiredProjector,
    retry_required_projection,
    run,
    step,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.contract("world.tick.atomic_visibility")]


class _World:
    def __init__(self, receipts: list[CommittedTickReceipt]) -> None:
        self.world_id = receipts[0].world_id
        self.run_id = receipts[0].run_id
        self.name = "managed"
        self.tick = 0
        self._receipts = iter(receipts)
        self.steps = 0

    async def step(self, run_config: RunConfig, **kwargs: object) -> CommittedTickReceipt:
        del run_config, kwargs
        receipt = next(self._receipts)
        self.steps += 1
        self.tick += 1
        return receipt


@dataclass
class _Registry:
    world: _World
    projector: RequiredProjector | None = None
    pending: CommittedTickReceipt | None = None

    @asynccontextmanager
    async def operation(self, world_id: object):
        assert str(world_id) == self.world.world_id
        yield self.world

    def required_projector(self, world_id: object) -> RequiredProjector | None:
        assert str(world_id) == self.world.world_id
        return self.projector

    def retain_receipt(self, world_id: object, receipt: CommittedTickReceipt) -> None:
        assert str(world_id) == self.world.world_id
        self.pending = receipt

    def pending_receipt(self, world_id: object) -> CommittedTickReceipt | None:
        assert str(world_id) == self.world.world_id
        return self.pending

    def acknowledge_receipt(
        self,
        world_id: object,
        *,
        consumer_name: str,
        receipt_identity: tuple[str, str, int, str | None],
    ) -> None:
        assert str(world_id) == self.world.world_id
        assert self.projector is not None
        assert consumer_name == self.projector.consumer_name
        assert self.pending is not None
        assert receipt_identity == self.pending.identity
        self.pending = None


@dataclass(frozen=True)
class _CleanupLease:
    world_id: str


class _CleanupRegistry(_Registry):
    def __init__(
        self,
        world: _World,
        projector: RequiredProjector,
        pending: CommittedTickReceipt,
    ) -> None:
        super().__init__(world, projector, pending)
        self.cleanup_entries = 0

    def validate_cleanup_lease(
        self,
        lease: _CleanupLease,
        *,
        world_id: object,
    ) -> None:
        if str(world_id) != lease.world_id:
            raise ValueError("cleanup lease belongs to a different world")

    @asynccontextmanager
    async def cleanup_operation(self, lease: _CleanupLease):
        self.cleanup_entries += 1
        yield self.world


_DEFAULT_TOKEN = object()


def _receipt(
    tick: int,
    *,
    token: str | None | object = _DEFAULT_TOKEN,
) -> CommittedTickReceipt:
    visibility_token: str | None
    if token is _DEFAULT_TOKEN:
        visibility_token = f"manifest-{tick}"
    else:
        assert token is None or isinstance(token, str)
        visibility_token = token
    return CommittedTickReceipt(
        world_id="00000000-0000-7000-8000-000000000002",
        run_id="00000000-0000-7000-8000-000000000003",
        committed_tick=tick,
        visibility_token=visibility_token,
        commands_applied=1,
    )


async def test_managed_projection_rejects_unbound_receipt_after_commit() -> None:
    receipt = _receipt(0, token=None)
    world = _World([receipt])

    async def project(_receipt: CommittedTickReceipt) -> None:
        pytest.fail("an unbound receipt must never reach the projector")

    registry = _Registry(
        world,
        projector=RequiredProjector(consumer_name="required", project=project),
    )

    with pytest.raises(PostCommitProjectionError) as raised:
        await step(registry, world.world_id, RunConfig())

    assert world.steps == 1
    assert world.tick == 1
    assert registry.pending is None
    assert raised.value.receipt is receipt


async def test_run_retries_pending_receipt_without_replaying_tick() -> None:
    receipts = [_receipt(0), _receipt(1)]
    world = _World(receipts)
    attempts: list[int] = []

    async def fail_once(receipt: CommittedTickReceipt) -> None:
        attempts.append(receipt.committed_tick)
        if len(attempts) == 1:
            raise RuntimeError("retry me")

    registry = _Registry(
        world,
        projector=RequiredProjector(consumer_name="required", project=fail_once),
    )

    with pytest.raises(PostCommitProjectionError):
        await step(registry, world.world_id, RunConfig())

    result = await run(registry, world.world_id, RunConfig(num_steps=1))

    assert world.steps == 2
    assert attempts == [0, 0, 1]
    assert result.ticks_completed == 1
    assert result.commands_applied == 1
    assert result.final_tick == 2
    assert registry.pending is None


async def test_cleanup_retry_rejects_a_lease_for_another_world_before_entry() -> None:
    receipt = _receipt(0)
    world = _World([receipt])
    projected: list[CommittedTickReceipt] = []

    async def project(value: CommittedTickReceipt) -> None:
        projected.append(value)

    registry = _CleanupRegistry(
        world,
        RequiredProjector(consumer_name="required", project=project),
        receipt,
    )
    lease = _CleanupLease(world_id="a-different-world")

    with pytest.raises(ValueError, match="different world"):
        await retry_required_projection(
            registry,
            world.world_id,
            lease=lease,  # type: ignore[arg-type]
        )

    assert registry.cleanup_entries == 0
    assert registry.pending is receipt
    assert projected == []
