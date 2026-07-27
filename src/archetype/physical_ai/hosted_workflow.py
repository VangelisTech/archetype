# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Family-owned hosted Physical-AI workflow over world and storage ports."""

from __future__ import annotations

from archetype.core.config import RunConfig
from archetype.physical_ai.hosted_activity_contracts import (
    HostedEpisodeObservation,
    hosted_episode_provider_operation_id,
)
from archetype.physical_ai.hosted_activity_world import PhysicalHostedActivityBinding
from archetype.physical_ai.hosted_episode import encode_hosted_episode_requests
from archetype.physical_ai.models import RunHostedEpisode
from archetype.world import mutation, simulation
from archetype.world.interfaces import iWorldRegistry


def _request_ipc(operation: RunHostedEpisode) -> bytes:
    if not operation.requests:
        raise ValueError("hosted episode request requires at least one episode")
    provider_operation_id = hosted_episode_provider_operation_id(
        operation.world_id,
        operation.activity_id,
    )
    return encode_hosted_episode_requests(
        [
            {
                "operation_id": provider_operation_id,
                "trial_id": request.trial_id,
                "suite": request.suite,
                "task_id": request.task_id,
                "seed": request.seed,
                "instruction": request.instruction,
                "max_transitions": request.max_transitions,
                "environment_id": request.environment_id,
                "policy_id": request.policy_id,
                "config_json": request.config_json,
            }
            for request in operation.requests
        ]
    )


async def run_hosted_episode(
    worlds: iWorldRegistry,
    binding: PhysicalHostedActivityBinding,
    operation: RunHostedEpisode,
) -> HostedEpisodeObservation:
    """Commit intent, run provider work outside the lock, then commit observation."""

    if binding.world_id != operation.world_id:
        raise ValueError("hosted Activity binding belongs to another world")
    request_ipc = _request_ipc(operation)
    intent = await binding.prepare_intent(
        activity_id=operation.activity_id,
        request_ipc=request_ipc,
    )
    async with worlds.operation(operation.world_id) as world:
        await mutation._create_entity_locked(world, [intent])
        await simulation._step_locked(
            worlds,
            operation.world_id,
            world,
            RunConfig(),
        )

    await binding.worker.run_once(activity_id=operation.activity_id)
    observation = await binding.observation(operation.activity_id)
    if observation is None:
        raise RuntimeError("hosted episode has no durable complete result")

    async with worlds.operation(operation.world_id) as world:
        await simulation._step_locked(
            worlds,
            operation.world_id,
            world,
            RunConfig(),
        )
    if not await binding.observation_settled(
        activity_id=operation.activity_id,
        result_digest=observation.result_digest,
    ):
        raise RuntimeError("hosted episode observation did not settle")
    return observation


__all__ = ["run_hosted_episode"]
