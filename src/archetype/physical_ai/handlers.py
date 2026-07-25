# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Free ledger-backed physical-AI operation handlers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Mapping
from typing import Any

from uuid_utils import UUID, uuid7

from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import WorldConfig
from archetype.physical_ai.interfaces import (
    EnvClient,
    PhysicalClientLifetimeRegistrar,
    PhysicalEvidenceWorldRetirement,
    PhysicalWorkflowLifetime,
    PolicyClient,
)
from archetype.physical_ai.manipulation import (
    ACTION_DIM,
    ManipAction,
    ManipFrameRef,
    ManipProprio,
    ManipStatus,
    ManipTask,
    _EnvStepProcessor,
    _FramedEnvStepProcessor,
)
from archetype.physical_ai.models import (
    EvaluatePhysicalTask,
    InstructionSweepReport,
    PhysicalTaskEvalReport,
    SweepPhysicalInstructions,
    TrialOutcome,
    VariantOutcome,
)
from archetype.physical_ai.policy import _PolicyActionProcessor
from archetype.physical_ai.views import latest_rows
from archetype.storage.interfaces import iStorageService
from archetype.world import mutation, query, simulation
from archetype.world.interfaces import iWorldLifecycle, iWorldRegistry
from archetype.world.models import EpisodeConfig
from archetype.world.registry import WorldCleanupLease


def _instruction_for(env_client: EnvClient, fallback: str) -> str:
    getter = getattr(env_client, "task_language", None)
    return str(getter()) if callable(getter) else fallback


def _trial_components(
    observation: Mapping[str, Any],
    *,
    suite: str,
    task_id: int,
    instruction: str,
    seed: int,
    env_key: int,
    with_frames: bool,
) -> list[Component]:
    components: list[Component] = [
        ManipProprio(
            eef_pos=list(observation.get("eef_pos", [0.0, 0.0, 0.0])),
            eef_quat=list(observation.get("eef_quat", [1.0, 0.0, 0.0, 0.0])),
            gripper=float(observation.get("gripper", 0.0)),
            gripper_qpos=list(observation.get("gripper_qpos", [0.0, 0.0])),
        ),
        ManipAction(values=[0.0] * ACTION_DIM),
        ManipStatus(),
        ManipTask(
            suite=suite,
            task_id=task_id,
            instruction=instruction,
            seed=seed,
            env_key=env_key,
        ),
    ]
    if with_frames:
        components.append(
            ManipFrameRef(
                agentview_ref=str(observation.get("agentview_ref", "")),
                wrist_ref=str(observation.get("wrist_ref", "")),
            )
        )
    return components


def _reset_policy(
    policy_client: PolicyClient | None,
    *,
    env_client: EnvClient,
) -> None:
    # A dual-role provider's required EnvClient.reset(env_id, seed) method is
    # not the policy's optional zero-argument reset hook. Environment state is
    # reset per trial below, so do not invoke the colliding method shape.
    if policy_client is None or policy_client is env_client:
        return
    reset = getattr(policy_client, "reset", None)
    if callable(reset):
        reset()


async def _finish_cleanup_uninterrupted(
    cleanup: Awaitable[None],
) -> None:
    """Join exact cleanup and preserve caller-vs-cleanup failure provenance."""

    retire = asyncio.ensure_future(cleanup)
    caller_cancellation: asyncio.CancelledError | None = None
    while not retire.done():
        try:
            await asyncio.shield(retire)
        except asyncio.CancelledError as interrupted:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                caller_cancellation = caller_cancellation or interrupted
        except BaseException:
            break

    try:
        retire.result()
    except asyncio.CancelledError as cleanup_cancellation:
        failures: list[BaseException] = [cleanup_cancellation]
        if caller_cancellation is not None:
            failures.append(caller_cancellation)
        raise BaseExceptionGroup(
            "exact cleanup was cancelled",
            failures,
        ) from None
    except BaseException as cleanup_failure:
        if caller_cancellation is not None:
            raise BaseExceptionGroup(
                "exact cleanup failed while its caller was cancelled",
                [cleanup_failure, caller_cancellation],
            ) from None
        raise
    if caller_cancellation is not None:
        raise caller_cancellation


async def _retire_evidence_world(
    retirement: PhysicalEvidenceWorldRetirement,
) -> None:
    """Cancel commands and retire one provider-backed writer despite cancellation."""

    await _finish_cleanup_uninterrupted(retirement.aclose())


async def _retain_or_compensate_evidence_world(
    workflow_lifetime: PhysicalWorkflowLifetime,
    world_id: str | UUID,
    cleanup_lease: WorldCleanupLease,
    *,
    label: str,
) -> PhysicalEvidenceWorldRetirement:
    """Retain exact cleanup through the pre-owned compensation authority."""

    try:
        return workflow_lifetime.retain_evidence_world(
            world_id,
            cleanup_lease,
        )
    except BaseException as retain_failure:
        try:
            compensation = workflow_lifetime.retain_evidence_world_for_compensation(
                world_id,
                cleanup_lease,
            )
        except BaseException as retry_failure:
            raise BaseExceptionGroup(
                f"{label} evidence-world retention and compensation binding failed",
                [retain_failure, retry_failure],
            ) from None

        try:
            await _finish_cleanup_uninterrupted(compensation.aclose())
        except BaseException as cleanup_failure:
            raise BaseExceptionGroup(
                f"{label} retention and exact-world compensation failed",
                [retain_failure, cleanup_failure],
            ) from None
        raise


async def evaluate_physical_task(
    client_lifetimes: PhysicalClientLifetimeRegistrar,
    world_registry: iWorldRegistry,
    world_lifecycle: iWorldLifecycle,
    storage: iStorageService,
    operation: EvaluatePhysicalTask,
) -> PhysicalTaskEvalReport:
    """Evaluate one instruction across a batch of trial entities."""

    async with client_lifetimes.lease(
        operation.env_client,
        operation.policy_client,
    ) as workflow_lifetime:
        return await _evaluate_physical_task(
            workflow_lifetime,
            world_registry,
            world_lifecycle,
            storage,
            operation,
        )


async def _evaluate_physical_task(
    workflow_lifetime: PhysicalWorkflowLifetime,
    world_registry: iWorldRegistry,
    world_lifecycle: iWorldLifecycle,
    storage: iStorageService,
    operation: EvaluatePhysicalTask,
) -> PhysicalTaskEvalReport:
    """Run one evaluation while its providers are exclusively leased."""

    config = operation.config
    world, cleanup_lease = await world_lifecycle.create_closing_world(
        WorldConfig(name=f"physical-eval:{config.suite}:t{config.task_id}:{uuid7()}"),
        config.storage,
        activation_owner=workflow_lifetime,
    )
    world_id = world.world_id
    retirement = await _retain_or_compensate_evidence_world(
        workflow_lifetime,
        world_id,
        cleanup_lease,
        label="physical evaluation",
    )
    try:
        async with world_registry.cleanup_operation(cleanup_lease) as exact_world:
            if exact_world is not world or not isinstance(exact_world, AsyncWorld):
                raise RuntimeError("physical evaluation lost exact-world cleanup authority")
            return await _evaluate_physical_task_in_world(
                world_registry,
                storage,
                exact_world,
                operation,
            )
    finally:
        await _retire_evidence_world(retirement)


async def _evaluate_physical_task_in_world(
    world_registry: iWorldRegistry,
    storage: iStorageService,
    world: AsyncWorld,
    operation: EvaluatePhysicalTask,
) -> PhysicalTaskEvalReport:
    """Evaluate while the caller holds exact non-public cleanup authority."""

    world_id = world.world_id
    env_client = operation.env_client
    policy_client = operation.policy_client
    config = operation.config
    if policy_client is not None:
        await mutation._add_processor_locked(
            world,
            _PolicyActionProcessor(policy_client),
        )
    env_processor = (
        _FramedEnvStepProcessor(env_client) if config.with_frames else _EnvStepProcessor(env_client)
    )
    await mutation._add_processor_locked(world, env_processor)
    _reset_policy(policy_client, env_client=env_client)

    instruction = _instruction_for(env_client, config.instruction)
    entities: list[list[Component]] = []
    trial_coordinates: list[tuple[int, int]] = []
    for trial_idx in range(config.trials):
        seed = config.task_id * 1000 + trial_idx
        observation = env_client.reset(trial_idx, seed)
        entities.append(
            _trial_components(
                observation,
                suite=config.suite,
                task_id=config.task_id,
                instruction=instruction,
                seed=seed,
                env_key=trial_idx,
                with_frames=config.with_frames,
            )
        )
        trial_coordinates.append((trial_idx, seed))

    entity_ids = await mutation._create_entities_locked(world, entities)
    episode = await simulation._run_episode_locked(
        world_registry,
        storage,
        world_id,
        world,
        EpisodeConfig(
            max_steps=config.max_steps,
            terminal_component=ManipStatus,
            terminal_field="done",
            terminal_all=True,
        ),
    )
    if episode.run_id is None:
        raise RuntimeError("physical evaluation completed without an active run identity")
    run_id = episode.run_id
    final = await latest_rows(
        await query.query_components(
            storage,
            [ManipStatus, ManipTask],
            str(world_id),
            str(run_id),
            config.storage,
        ),
        storage,
    )
    if len(final) != config.trials:
        raise ValueError(
            f"physical evaluation graded {len(final)} trials but spawned "
            f"{config.trials}; terminal ledger evidence is incomplete"
        )

    trials = tuple(
        TrialOutcome(
            trial_idx=trial_idx,
            env_key=trial_idx,
            seed=seed,
            success=bool(final[entity_id].get("manipstatus__success", False)),
            episode_length=int(final[entity_id].get("manipstatus__env_step", 0)),
        )
        for entity_id, (trial_idx, seed) in zip(entity_ids, trial_coordinates, strict=True)
    )
    return PhysicalTaskEvalReport(
        suite=config.suite,
        task_id=config.task_id,
        instruction=instruction,
        world_id=str(world_id),
        run_id=str(run_id),
        trials=trials,
    )


async def sweep_physical_instructions(
    client_lifetimes: PhysicalClientLifetimeRegistrar,
    world_registry: iWorldRegistry,
    world_lifecycle: iWorldLifecycle,
    storage: iStorageService,
    operation: SweepPhysicalInstructions,
) -> InstructionSweepReport:
    """Evaluate instruction variants on paired seeds in one persisted world."""

    async with client_lifetimes.lease(
        operation.env_client,
        operation.policy_client,
    ) as workflow_lifetime:
        return await _sweep_physical_instructions(
            workflow_lifetime,
            world_registry,
            world_lifecycle,
            storage,
            operation,
        )


async def _sweep_physical_instructions(
    workflow_lifetime: PhysicalWorkflowLifetime,
    world_registry: iWorldRegistry,
    world_lifecycle: iWorldLifecycle,
    storage: iStorageService,
    operation: SweepPhysicalInstructions,
) -> InstructionSweepReport:
    """Run one sweep while its providers are exclusively leased."""

    config = operation.config
    world, cleanup_lease = await world_lifecycle.create_closing_world(
        WorldConfig(name=f"physical-sweep:{config.suite}:t{config.task_id}:{uuid7()}"),
        config.storage,
        activation_owner=workflow_lifetime,
    )
    world_id = world.world_id
    retirement = await _retain_or_compensate_evidence_world(
        workflow_lifetime,
        world_id,
        cleanup_lease,
        label="physical sweep",
    )
    try:
        async with world_registry.cleanup_operation(cleanup_lease) as exact_world:
            if exact_world is not world or not isinstance(exact_world, AsyncWorld):
                raise RuntimeError("physical sweep lost exact-world cleanup authority")
            return await _sweep_physical_instructions_in_world(
                world_registry,
                storage,
                exact_world,
                operation,
            )
    finally:
        await _retire_evidence_world(retirement)


async def _sweep_physical_instructions_in_world(
    world_registry: iWorldRegistry,
    storage: iStorageService,
    world: AsyncWorld,
    operation: SweepPhysicalInstructions,
) -> InstructionSweepReport:
    """Sweep while the caller holds exact non-public cleanup authority."""

    world_id = world.world_id
    env_client = operation.env_client
    policy_client = operation.policy_client
    config = operation.config
    variants = list(dict.fromkeys(config.variants))
    await mutation._add_processor_locked(
        world,
        _PolicyActionProcessor(policy_client),
    )
    env_processor = (
        _FramedEnvStepProcessor(env_client) if config.with_frames else _EnvStepProcessor(env_client)
    )
    await mutation._add_processor_locked(world, env_processor)
    _reset_policy(policy_client, env_client=env_client)

    entities: list[list[Component]] = []
    env_key = 0
    for instruction in variants:
        for seed_slot in range(config.seeds_per_variant):
            seed = config.task_id * 1000 + seed_slot
            observation = env_client.reset(env_key, seed)
            entities.append(
                _trial_components(
                    observation,
                    suite=config.suite,
                    task_id=config.task_id,
                    instruction=instruction,
                    seed=seed,
                    env_key=env_key,
                    with_frames=config.with_frames,
                )
            )
            env_key += 1

    await mutation._create_entities_locked(world, entities)
    episode = await simulation._run_episode_locked(
        world_registry,
        storage,
        world_id,
        world,
        EpisodeConfig(
            max_steps=config.max_steps,
            terminal_component=ManipStatus,
            terminal_field="done",
            terminal_all=True,
        ),
    )
    if episode.run_id is None:
        raise RuntimeError("physical sweep completed without an active run identity")
    run_id = episode.run_id
    final = await latest_rows(
        await query.query_components(
            storage,
            [ManipStatus, ManipTask],
            str(world_id),
            str(run_id),
            config.storage,
        ),
        storage,
    )

    rows_by_instruction: dict[str, list[dict[str, Any]]] = {
        instruction: [] for instruction in variants
    }
    for row in final.values():
        instruction = str(row.get("maniptask__instruction", ""))
        if instruction not in rows_by_instruction:
            raise ValueError(f"physical sweep graded an unspawned instruction {instruction!r}")
        rows_by_instruction[instruction].append(row)

    outcomes: list[VariantOutcome] = []
    for instruction in variants:
        rows = rows_by_instruction[instruction]
        n_trials = len(rows)
        if n_trials != config.seeds_per_variant:
            raise ValueError(
                f"physical sweep variant {instruction!r} graded {n_trials} trials "
                f"but expected {config.seeds_per_variant}; terminal ledger "
                "evidence is incomplete"
            )
        n_success = sum(bool(row.get("manipstatus__success", False)) for row in rows)
        lengths = [int(row.get("manipstatus__env_step", 0)) for row in rows]
        outcomes.append(
            VariantOutcome(
                instruction=instruction,
                n_trials=n_trials,
                n_success=n_success,
                success_rate=n_success / n_trials if n_trials else 0.0,
                mean_length=sum(lengths) / n_trials if n_trials else 0.0,
            )
        )

    expected = len(variants) * config.seeds_per_variant
    graded = sum(outcome.n_trials for outcome in outcomes)
    if graded != expected:
        raise ValueError(
            f"physical sweep graded {graded} trials but spawned {expected}; "
            "terminal ledger evidence is incomplete"
        )
    return InstructionSweepReport(
        suite=config.suite,
        task_id=config.task_id,
        world_id=str(world_id),
        run_id=str(run_id),
        variants=tuple(outcomes),
    )


__all__ = [
    "evaluate_physical_task",
    "sweep_physical_instructions",
]
