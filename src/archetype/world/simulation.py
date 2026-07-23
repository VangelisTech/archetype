# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Managed tick, required projection, episode, and rollout behavior."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from daft import DataType, col
from uuid_utils import UUID

from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig
from archetype.core.interfaces import CommittedTickReceipt
from archetype.storage.service import StorageService
from archetype.world.models import (
    EpisodeConfig,
    EpisodeResult,
    RolloutConfig,
    RolloutResult,
    RunResult,
)

if TYPE_CHECKING:
    from archetype.world.registry import WorldCleanupLease, WorldRegistry

ProjectionCallable = Callable[[CommittedTickReceipt], Awaitable[None]]
ForkWorldCallable = Callable[..., Awaitable[Any]]
DestroyWorldCallable = Callable[[str | UUID], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class RequiredProjector:
    """One named generic post-commit consumer for a managed world."""

    consumer_name: str
    project: ProjectionCallable

    def __post_init__(self) -> None:
        if not self.consumer_name.strip():
            raise ValueError("required projector consumer_name cannot be empty")


class PostCommitProjectionError(RuntimeError):
    """A tick committed durably but its required projection did not finish."""

    def __init__(
        self,
        receipt: CommittedTickReceipt,
        consumer_name: str,
        message: str | None = None,
    ) -> None:
        self.receipt = receipt
        self.consumer_name = consumer_name
        super().__init__(
            message
            or (
                f"required projector {consumer_name!r} did not acknowledge "
                f"committed tick {receipt.committed_tick}"
            )
        )


def _validate_receipt(world: AsyncWorld, receipt: CommittedTickReceipt) -> None:
    if str(receipt.world_id) != str(world.world_id):
        raise ValueError(
            f"committed receipt world {receipt.world_id!r} does not match "
            f"managed world {world.world_id!r}"
        )
    if str(receipt.run_id) != str(world.run_id):
        raise ValueError(
            f"committed receipt run {receipt.run_id!r} does not match managed run {world.run_id!r}"
        )
    if receipt.committed_tick != world.tick - 1:
        raise ValueError(
            f"committed receipt tick {receipt.committed_tick} does not match "
            f"managed committed head {world.tick - 1}"
        )


async def _project_required_locked(
    registry: WorldRegistry,
    world_id: str | UUID,
    receipt: CommittedTickReceipt,
) -> None:
    """Project and acknowledge ``receipt`` while the world lock is held."""
    projector = registry.required_projector(world_id)
    if projector is None:
        raise PostCommitProjectionError(
            receipt,
            "<missing>",
            "a retained required-projection receipt has no projector binding",
        )
    if receipt.visibility_token is None:
        raise PostCommitProjectionError(
            receipt,
            projector.consumer_name,
            "managed required projection requires a manifest visibility token",
        )
    try:
        await projector.project(receipt)
        registry.acknowledge_receipt(
            world_id,
            consumer_name=projector.consumer_name,
            receipt_identity=receipt.identity,
        )
    except Exception as exc:
        raise PostCommitProjectionError(receipt, projector.consumer_name) from exc


async def _retry_required_projection_locked(
    registry: WorldRegistry,
    world_id: str | UUID,
) -> bool:
    """Retry the exact retained receipt; return whether one existed."""
    pending = registry.pending_receipt(world_id)
    if pending is None:
        return False
    await _project_required_locked(registry, world_id, pending)
    return True


async def retry_required_projection(
    registry: WorldRegistry,
    world_id: str | UUID,
    *,
    lease: WorldCleanupLease | None = None,
) -> bool:
    """Retry pending projection through public or exact-world cleanup authority."""
    if lease is None:
        async with registry.operation(world_id):
            return await _retry_required_projection_locked(registry, world_id)
    registry.validate_cleanup_lease(lease, world_id=world_id)
    async with registry.cleanup_operation(lease):
        return await _retry_required_projection_locked(registry, world_id)


async def _step_locked(
    registry: WorldRegistry,
    world_id: str | UUID,
    world: AsyncWorld,
    run_config: RunConfig,
    **input_kwargs: Any,
) -> int:
    """Advance exactly once when the caller already holds the world lock."""
    await _retry_required_projection_locked(registry, world_id)
    receipt = await world.step(run_config, **input_kwargs)
    if not isinstance(receipt, CommittedTickReceipt):
        raise TypeError(
            "managed AsyncWorld.step must return CommittedTickReceipt after publication"
        )
    projector = registry.required_projector(world_id)
    try:
        _validate_receipt(world, receipt)
    except ValueError as exc:
        raise PostCommitProjectionError(
            receipt,
            projector.consumer_name if projector is not None else "<managed>",
            str(exc),
        ) from exc
    if projector is not None:
        if receipt.visibility_token is None:
            raise PostCommitProjectionError(
                receipt,
                projector.consumer_name,
                "managed required projection requires a manifest visibility token",
            )
        registry.retain_receipt(world_id, receipt)
        await _project_required_locked(registry, world_id, receipt)
    return receipt.commands_applied


async def step(
    registry: WorldRegistry,
    world_id: str | UUID,
    run_config: RunConfig,
    **input_kwargs: Any,
) -> int:
    """Advance a managed world by one committed and projected tick."""
    async with registry.operation(world_id) as world:
        return await _step_locked(
            registry,
            world_id,
            world,
            run_config,
            **input_kwargs,
        )


async def run(
    registry: WorldRegistry,
    world_id: str | UUID,
    run_config: RunConfig,
    **input_kwargs: Any,
) -> RunResult:
    """Execute ``run_config.num_steps`` without reacquiring the world lock."""
    async with registry.operation(world_id) as world:
        commands_applied = 0
        for _ in range(run_config.num_steps):
            commands_applied += await _step_locked(
                registry,
                world_id,
                world,
                run_config,
                **input_kwargs,
            )
        return RunResult(
            run_id=world.run_id,
            world_id=world.world_id,
            ticks_completed=run_config.num_steps,
            commands_applied=commands_applied,
            final_tick=world.tick,
        )


async def _entities_terminal(
    storage: StorageService,
    world: AsyncWorld,
    component: type[Component],
    field: str,
    *,
    require_all: bool,
) -> bool:
    """Reduce a value-based terminal condition at the storage boundary."""
    frame = await world.get_components([component])
    flag = f"{component.get_prefix()}{field}"
    summary = frame.agg(
        col(flag).cast(DataType.int64()).sum().alias("n_done"),
        col(flag).count().alias("n_total"),
    )
    materialized = await storage.materialize(summary)
    rows = materialized.to_pylist()
    if not rows:
        return False
    total = rows[0]["n_total"] or 0
    done = rows[0]["n_done"] or 0
    if total == 0:
        return False
    return done == total if require_all else done >= 1


async def _run_episode_locked(
    registry: WorldRegistry,
    storage: StorageService,
    world_id: str | UUID,
    world: AsyncWorld,
    config: EpisodeConfig,
    **input_kwargs: Any,
) -> EpisodeResult:
    """Run one bounded episode while the exact world lock is held."""
    start_tick = world.tick
    terminated = False
    step_count = 0
    value_based = config.terminal_component is not None and config.terminal_field is not None

    while step_count < config.max_steps:
        if (
            not value_based
            and config.terminal_component is not None
            and any(
                config.terminal_component in signature for signature in world.entity2sig.values()
            )
        ):
            terminated = True
            break
        if config.termination is not None and config.termination(world):
            terminated = True
            break

        await _step_locked(
            registry,
            world_id,
            world,
            config.run_config,
            **input_kwargs,
        )
        step_count += 1

        if (
            value_based
            and config.terminal_component is not None
            and config.terminal_field is not None
            and await _entities_terminal(
                storage,
                world,
                config.terminal_component,
                config.terminal_field,
                require_all=config.terminal_all,
            )
        ):
            terminated = True
            break

    return EpisodeResult(
        episode_id=config.episode_id,
        world_id=world.world_id,
        run_id=world.run_id,
        start_tick=start_tick,
        final_tick=world.tick,
        terminated=terminated,
        duration_steps=world.tick - start_tick,
    )


async def run_episode(
    registry: WorldRegistry,
    storage: StorageService,
    world_id: str | UUID,
    config: EpisodeConfig,
    **input_kwargs: Any,
) -> EpisodeResult:
    """Run a bounded episode under one exact-world operation lease."""
    async with registry.operation(world_id) as world:
        return await _run_episode_locked(
            registry,
            storage,
            world_id,
            world,
            config,
            **input_kwargs,
        )


async def run_rollout(
    registry: WorldRegistry,
    storage: StorageService,
    fork_world: ForkWorldCallable,
    destroy_world: DestroyWorldCallable,
    world_id: str | UUID,
    config: RolloutConfig,
    **input_kwargs: Any,
) -> RolloutResult:
    """Run ordered episodes on distinct lifecycle-owned forks."""
    async with registry.operation(world_id) as base:
        base_name = base.name

    async def _run_one(index: int) -> EpisodeResult:
        fork = await fork_world(
            world_id,
            name=f"{base_name}:{config.name_prefix}:{index}",
        )
        fork_world_id = fork.world_id
        try:
            return await run_episode(
                registry,
                storage,
                fork_world_id,
                config.episode_config,
                **input_kwargs,
            )
        finally:
            if config.destroy_forks_on_complete:
                await destroy_world(fork_world_id)

    if config.parallel:
        results = tuple(
            await asyncio.gather(*(_run_one(index) for index in range(config.num_episodes)))
        )
    else:
        results = tuple([await _run_one(index) for index in range(config.num_episodes)])
    return RolloutResult(
        rollout_id=config.rollout_id,
        base_world_id=world_id,
        episodes=results,
        num_episodes=len(results),
        total_duration_steps=sum(result.duration_steps for result in results),
    )
