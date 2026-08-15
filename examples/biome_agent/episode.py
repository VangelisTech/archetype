# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable Archetype evidence for one live Biome control episode."""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass

from archetype import ArchetypeRuntime, StorageConfig

from .client import BiomeClient
from .components import (
    BiomeAgentDecision,
    BiomeEpisodeState,
    BiomeMission,
    BiomeMissionOutcome,
)
from .contracts import ExtractionGoal, MissionTrace
from .mission import monitor_mission, plan_mission
from .policy import GoalDirectedDrillPolicy


@dataclass(frozen=True, slots=True)
class DurableBiomeEpisodeResult:
    """Native mission result bound to its durable Archetype coordinates."""

    trace: MissionTrace
    biome_revision: str
    flecs_revision: str
    world_id: str
    run_id: str
    committed_tick: int
    episode_entity_id: int


def wait_until_ready(
    client: BiomeClient,
    process: subprocess.Popen[bytes] | None,
    *,
    timeout: float = 30.0,
) -> bool:
    """Wait until Flecs REST is ready or the owned Biome process exits."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if client.is_ready():
            return True
        if process is not None and process.poll() is not None:
            return False
        time.sleep(0.1)
    return False


def _state_from_trace(trace: MissionTrace) -> BiomeEpisodeState:
    sample = trace.final_sample
    drill = sample.drill if sample else None
    return BiomeEpisodeState(
        phase="succeeded" if trace.success else "failed",
        target_entity=trace.plan.action.target_path,
        deposit_amount=sample.deposit_amount if sample else trace.plan.target.amount,
        extracted=trace.extracted,
        drill_entity=trace.plan.action.drill_path,
        powered=drill.powered if drill else False,
        stored_amount=drill.stored_amount if drill else 0,
    )


def run_durable_episode(
    client: BiomeClient,
    goal: ExtractionGoal,
    *,
    storage: StorageConfig,
    biome_revision: str,
    flecs_revision: str,
    timeout: float = 15.0,
    poll_interval: float = 0.25,
    world_name: str = "live-biome-agent",
) -> DurableBiomeEpisodeResult:
    """Act in native Biome and persist the complete episode in Archetype."""

    if not biome_revision or not flecs_revision:
        raise ValueError("Biome and Flecs revisions must not be empty")

    policy = GoalDirectedDrillPolicy()
    with ArchetypeRuntime.sync() as runtime:
        world = runtime.world(world_name, storage=storage)
        episode = world.spawn(
            BiomeMission(
                environment_uri=client.base_url,
                resource=goal.resource,
                target_amount=goal.amount,
                biome_revision=biome_revision,
                flecs_revision=flecs_revision,
            ),
            BiomeEpisodeState(),
        )
        world.step()

        plan = plan_mission(client, policy, goal)
        world.update(
            episode,
            BiomeEpisodeState(
                phase="observed",
                target_entity=plan.action.target_path,
                deposit_amount=plan.target.amount,
            ),
        )
        world.step()

        client.deploy(plan.action)
        world.add_components(
            episode,
            BiomeAgentDecision(
                target_entity=plan.action.target_path,
                drill_x=plan.action.drill_cell.x,
                drill_y=plan.action.drill_cell.y,
                power_x=plan.action.power_cell.x,
                power_y=plan.action.power_cell.y,
            ),
        )
        world.update(
            episode,
            BiomeEpisodeState(
                phase="action_applied",
                target_entity=plan.action.target_path,
                deposit_amount=plan.target.amount,
                drill_entity=plan.action.drill_path,
            ),
        )
        world.step()

        trace = monitor_mission(
            client,
            plan,
            timeout=timeout,
            poll_interval=poll_interval,
        )
        final_sample = trace.final_sample
        elapsed = final_sample.elapsed_seconds if final_sample else 0.0
        world.update(episode, _state_from_trace(trace))
        world.add_components(
            episode,
            BiomeMissionOutcome(
                success=trace.success,
                extracted=trace.extracted,
                reason=trace.reason,
                elapsed_seconds=elapsed,
            ),
        )
        world.step()
        info = world.info()
        if info.tick < 1:  # pragma: no cover - guarded by the four successful steps above
            raise RuntimeError("Biome episode did not publish a durable tick")

        return DurableBiomeEpisodeResult(
            trace=trace,
            biome_revision=biome_revision,
            flecs_revision=flecs_revision,
            world_id=str(info.world_id),
            run_id=str(info.run_id),
            committed_tick=info.tick - 1,
            episode_entity_id=episode,
        )
