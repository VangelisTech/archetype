# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Managed tick, required projection, episode, and rollout behavior."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from daft import DataType, col
from uuid_utils import UUID

from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig
from archetype.core.interfaces import CommittedTickReceipt
from archetype.storage.interfaces import iStorageService
from archetype.world.models import (
    EpisodeConfig,
    EpisodeResult,
    RolloutConfig,
    RolloutResult,
    RunResult,
)

if TYPE_CHECKING:
    from archetype.world.interfaces import iWorldRegistry
    from archetype.world.registry import WorldCleanupLease

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
    registry: iWorldRegistry,
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
    registry: iWorldRegistry,
    world_id: str | UUID,
) -> bool:
    """Retry the exact retained receipt; return whether one existed."""
    pending = registry.pending_receipt(world_id)
    if pending is None:
        return False
    await _project_required_locked(registry, world_id, pending)
    return True


def _retain_interrupted_receipt(
    registry: iWorldRegistry,
    world_id: str | UUID,
    world: AsyncWorld,
    target_tick: int,
) -> None:
    """Retain a commit completed before raw task cancellation propagated."""
    receipt = world.last_committed_receipt
    projector = registry.required_projector(world_id)
    if (
        projector is not None
        and receipt is not None
        and receipt.committed_tick == target_tick
        and str(receipt.world_id) == str(world.world_id)
        and str(receipt.run_id) == str(world.run_id)
        and receipt.visibility_token is not None
    ):
        registry.retain_receipt(world_id, receipt)


async def _accept_committed_receipt_locked(
    registry: iWorldRegistry,
    world_id: str | UUID,
    world: AsyncWorld,
    receipt: CommittedTickReceipt,
) -> None:
    """Validate, retain, and project one receipt under exact-world authority."""
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


async def reconcile_prepared_tick_locked(
    registry: iWorldRegistry,
    world_id: str | UUID,
    world: AsyncWorld,
) -> bool:
    """Reconcile a prepared publication while the caller holds exact-world authority."""
    if not world.has_prepared_tick_commit:
        return False
    target_tick = world.tick
    try:
        receipt = await world.reconcile_prepared_tick()
    except BaseException:
        _retain_interrupted_receipt(registry, world_id, world, target_tick)
        raise
    if receipt is None:
        return False
    await _accept_committed_receipt_locked(registry, world_id, world, receipt)
    return True


async def reconcile_committed_work_locked(
    registry: iWorldRegistry,
    world_id: str | UUID,
    world: AsyncWorld,
) -> bool:
    """Finish retained projection and prepared publication before other work."""
    retried = await _retry_required_projection_locked(registry, world_id)
    reconciled = await reconcile_prepared_tick_locked(registry, world_id, world)
    return retried or reconciled


async def retry_required_projection(
    registry: iWorldRegistry,
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
    registry: iWorldRegistry,
    world_id: str | UUID,
    world: AsyncWorld,
    run_config: RunConfig,
    **input_kwargs: Any,
) -> int:
    """Advance exactly once when the caller already holds the world lock."""
    await _retry_required_projection_locked(registry, world_id)
    target_tick = world.tick
    try:
        receipt = await world.step(run_config, **input_kwargs)
    except BaseException:
        # Core records the manifest-bound receipt before awaiting advisory
        # PostTick hooks. If caller cancellation lands there, retain it
        # synchronously before preserving the raw cancellation. The next
        # managed entry projects this exact receipt before any new core work.
        _retain_interrupted_receipt(registry, world_id, world, target_tick)
        raise
    if not isinstance(receipt, CommittedTickReceipt):
        raise TypeError(
            "managed AsyncWorld.step must return CommittedTickReceipt after publication"
        )
    await _accept_committed_receipt_locked(registry, world_id, world, receipt)
    return receipt.commands_applied


async def step(
    registry: iWorldRegistry,
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
    registry: iWorldRegistry,
    world_id: str | UUID,
    run_config: RunConfig,
    **input_kwargs: Any,
) -> RunResult:
    """Execute ``run_config.num_steps`` without reacquiring the world lock."""
    async with registry.operation(world_id) as world:
        # A prior call may have committed before losing its response. Finish
        # that exact work before counting this call's requested new steps.
        # This also makes a zero-step run a coherent state observation.
        await reconcile_committed_work_locked(registry, world_id, world)
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
    storage: iStorageService,
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
    registry: iWorldRegistry,
    storage: iStorageService,
    world_id: str | UUID,
    world: AsyncWorld,
    config: EpisodeConfig,
    **input_kwargs: Any,
) -> EpisodeResult:
    """Run one bounded episode while the exact world lock is held."""
    # Recovery belongs before the episode boundary: a prior prepared commit
    # is neither one of this episode's steps nor input to a stale termination
    # decision. ``max_steps=0`` still returns a coherent committed head.
    await reconcile_committed_work_locked(registry, world_id, world)
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
    registry: iWorldRegistry,
    storage: iStorageService,
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
    registry: iWorldRegistry,
    storage: iStorageService,
    fork_world: ForkWorldCallable,
    destroy_world: DestroyWorldCallable,
    world_id: str | UUID,
    config: RolloutConfig,
    **input_kwargs: Any,
) -> RolloutResult:
    """Run ordered episodes on distinct lifecycle-owned forks."""
    async with registry.operation(world_id) as base:
        base_name = base.name

    async def _await_owned(
        owned: asyncio.Future[Any],
    ) -> tuple[Any, asyncio.CancelledError | None]:
        """Drain private acquisition/cleanup work across caller cancellation."""

        cancelled: asyncio.CancelledError | None = None
        while True:
            try:
                return await asyncio.shield(owned), cancelled
            except asyncio.CancelledError as exc:
                if owned.cancelled():
                    raise
                cancelled = cancelled or exc
                if owned.done():
                    return owned.result(), cancelled

    async def _run_one(index: int) -> EpisodeResult:
        fork_work = asyncio.ensure_future(
            fork_world(
                world_id,
                name=f"{base_name}:{config.name_prefix}:{index}",
            )
        )
        fork, cancelled = await _await_owned(fork_work)
        fork_world_id = fork.world_id
        try:
            if cancelled is not None:
                raise cancelled
            return await run_episode(
                registry,
                storage,
                fork_world_id,
                config.episode_config,
                **input_kwargs,
            )
        finally:
            if config.destroy_forks_on_complete:
                try:
                    teardown = asyncio.ensure_future(destroy_world(fork_world_id))
                    _, teardown_cancelled = await _await_owned(teardown)
                except BaseException as exc:
                    exc.add_note(f"rollout fork teardown failed for world_id={fork_world_id}")
                    raise
                if teardown_cancelled is not None:
                    raise teardown_cancelled

    async def _drain_started(indices: tuple[int, ...]) -> tuple[EpisodeResult, ...]:
        """Drain exactly the started children without exposing cancellation."""

        failures: list[BaseException] = []

        async def _capture(index: int) -> EpisodeResult | BaseException:
            try:
                return await _run_one(index)
            except BaseException as exc:
                # Capture in observation order so the exception asyncio.gather
                # would have propagated first remains the primary failure.
                failures.append(exc)
                return exc

        tasks = tuple(
            asyncio.create_task(
                _capture(index),
                name=f"archetype-rollout:{config.rollout_id}:{index}",
            )
            for index in indices
        )
        pending = asyncio.gather(*tasks, return_exceptions=True)
        cancelled: asyncio.CancelledError | None = None
        children_cancelled = False
        while True:
            try:
                outcomes = await asyncio.shield(pending)
                break
            except asyncio.CancelledError as exc:
                cancelled = cancelled or exc
                if not children_cancelled:
                    # Each child shields fork acquisition until it owns the
                    # returned ID, then skips/interrupts the episode and drains
                    # teardown. Repeated caller cancels must not re-cancel it.
                    for task in tasks:
                        task.cancel()
                    children_cancelled = True

        for outcome in outcomes:
            if isinstance(outcome, BaseException) and all(
                outcome is not failure for failure in failures
            ):
                failures.append(outcome)

        if cancelled is not None:
            substantive_failures = [
                failure for failure in failures if not isinstance(failure, asyncio.CancelledError)
            ]
            if substantive_failures:
                raise cancelled from BaseExceptionGroup(
                    "rollout failures observed while the caller was cancelled",
                    substantive_failures,
                )
            raise cancelled
        if failures:
            primary, *additional = failures
            if additional:
                raise primary from BaseExceptionGroup(
                    "additional rollout failures",
                    additional,
                )
            raise primary
        return tuple(cast(EpisodeResult, outcome) for outcome in outcomes)

    if config.parallel:
        results = await _drain_started(tuple(range(config.num_episodes)))
    else:
        ordered_results: list[EpisodeResult] = []
        for index in range(config.num_episodes):
            ordered_results.extend(await _drain_started((index,)))
        results = tuple(ordered_results)
    return RolloutResult(
        rollout_id=config.rollout_id,
        base_world_id=world_id,
        episodes=results,
        num_episodes=len(results),
        total_duration_steps=sum(result.duration_steps for result in results),
    )
