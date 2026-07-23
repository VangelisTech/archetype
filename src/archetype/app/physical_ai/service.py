# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Application workflow for batched physical-AI evaluation.

One application service owns the entire control-plane sequence: create an
evidence-preserving world, install family processors, reset external resources,
spawn trial entities, run a bounded episode, and derive a typed result from the
persisted ledger. Runtime callers provide only a typed request and provider
implementations; they never compose raw application services themselves.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from daft import DataFrame, col
from uuid_utils import uuid7

from archetype.app.evaluation.interfaces import iEvaluationService
from archetype.app.storage.interfaces import iStorageService
from archetype.app.world.interfaces import (
    iMutationService,
    iSimulationService,
    iWorldService,
)
from archetype.core.component import Component
from archetype.core.config import WorldConfig
from archetype.physical_ai.contracts import (
    InstructionSweepConfig,
    InstructionSweepReport,
    PhysicalTaskEvalConfig,
    PhysicalTaskEvalReport,
    TrialOutcome,
    VariantOutcome,
)
from archetype.physical_ai.manipulation import (
    ACTION_DIM,
    EnvClient,
    EnvStepProcessor,
    FramedEnvStepProcessor,
    ManipAction,
    ManipFrameRef,
    ManipProprio,
    ManipStatus,
    ManipTask,
)
from archetype.physical_ai.policy import PolicyActionProcessor, PolicyClient
from archetype.world.models import EpisodeConfig


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


async def _latest_rows(
    frame: DataFrame,
    storage_service: iStorageService,
) -> dict[int, dict[str, Any]]:
    """Materialize terminal rows at the report-producing analysis boundary."""

    heads = frame.groupby("entity_id").agg(col("tick").max().alias("latest_tick"))
    terminal = frame.join(
        heads,
        left_on=["entity_id", "tick"],
        right_on=["entity_id", "latest_tick"],
    ).select(*frame.column_names)
    materialized = await storage_service.materialize(terminal)
    return {int(row["entity_id"]): row for row in materialized.to_pylist()}


def _reset_policy(policy_client: PolicyClient | None) -> None:
    reset = getattr(policy_client, "reset", None)
    if callable(reset):
        reset()


class PhysicalAIService:
    """Compose physical state/processors with world, simulation, and eval ports."""

    def __init__(
        self,
        world_service: iWorldService,
        mutation_service: iMutationService,
        simulation_service: iSimulationService,
        evaluation_service: iEvaluationService,
        storage_service: iStorageService,
    ) -> None:
        self._worlds = world_service
        self._mutations = mutation_service
        self._simulation = simulation_service
        self._evaluations = evaluation_service
        self._storage = storage_service

    async def evaluate_task(
        self,
        config: PhysicalTaskEvalConfig,
        *,
        env_client: EnvClient,
        policy_client: PolicyClient | None = None,
    ) -> PhysicalTaskEvalReport:
        """Evaluate one instruction across a batch of trial entities."""

        world = await self._worlds.create_world(
            WorldConfig(name=f"physical-eval:{config.suite}:t{config.task_id}:{uuid7()}"),
            config.storage,
        )
        world_id = world.world_id
        if policy_client is not None:
            await self._mutations.add_processor(world_id, PolicyActionProcessor(policy_client))
        env_processor = (
            FramedEnvStepProcessor(env_client)
            if config.with_frames
            else EnvStepProcessor(env_client)
        )
        await self._mutations.add_processor(world_id, env_processor)
        _reset_policy(policy_client)

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

        entity_ids = await self._mutations.create_entities(world_id, entities)
        episode = await self._simulation.run_episode(
            world_id,
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
        final = await _latest_rows(
            await self._evaluations.query_components(
                [ManipStatus, ManipTask],
                world_id=world_id,
                run_id=run_id,
                storage_config=config.storage,
            ),
            self._storage,
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

    async def sweep_instructions(
        self,
        config: InstructionSweepConfig,
        *,
        env_client: EnvClient,
        policy_client: PolicyClient,
    ) -> InstructionSweepReport:
        """Evaluate variants on paired seeds in one persisted world."""

        variants = list(dict.fromkeys(config.variants))
        world = await self._worlds.create_world(
            WorldConfig(name=f"physical-sweep:{config.suite}:t{config.task_id}:{uuid7()}"),
            config.storage,
        )
        world_id = world.world_id
        await self._mutations.add_processor(world_id, PolicyActionProcessor(policy_client))
        env_processor = (
            FramedEnvStepProcessor(env_client)
            if config.with_frames
            else EnvStepProcessor(env_client)
        )
        await self._mutations.add_processor(world_id, env_processor)
        _reset_policy(policy_client)

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

        await self._mutations.create_entities(world_id, entities)
        episode = await self._simulation.run_episode(
            world_id,
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
        final = await _latest_rows(
            await self._evaluations.query_components(
                [ManipStatus, ManipTask],
                world_id=world_id,
                run_id=run_id,
                storage_config=config.storage,
            ),
            self._storage,
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
