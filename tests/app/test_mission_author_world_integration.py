# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real-world contracts for the Mission author Activity integration."""

from __future__ import annotations

from dataclasses import replace
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from archetype.app.missions.activity_world import (
    MissionAuthorActivityBinding,
    StorageMissionCommittedIntentReader,
    WorldMissionAuthorObservationStager,
)
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.hooks import OnSpawn
from archetype.missions.activities import (
    AuthorActivityResultRef,
    DurableAuthorExecutionObservation,
    complete_author_activity_fact_bundle,
    complete_author_activity_fact_count,
)
from archetype.missions.coding_agents.contracts import (
    AgentExecutionResult,
    CommitObservation,
    ValidationObservation,
)
from archetype.missions.components import (
    AgentExecution,
    Candidate,
    Commit,
    CompleteAuthorActivityObservation,
    FrictionLog,
    Mission,
    Sandbox,
    Task,
    TaskCriticPolicy,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    TaskWorkspace,
    ValidationResult,
)
from archetype.missions.contracts import CriticPolicy
from archetype.missions.projections import (
    project_complete_author_activity_fact_bundles,
    project_task_dispatch_requests,
)
from archetype.missions.relations import (
    AuthoredBy,
    CandidateFor,
    Executes,
    Guards,
    PartOfMission,
    ProducedBy,
    RunsIn,
    Supersedes,
)
from archetype.missions.sandboxes import SandboxIdentity, SandboxStatus
from archetype.missions.transitions import AgentExecutionStatus, TaskStatus
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.registry import WorldRegistry
from archetype.world.simulation import step


def _critic_component(policy: CriticPolicy) -> TaskCriticPolicy:
    return TaskCriticPolicy(
        policy_id=policy.policy_id,
        version=policy.version,
        digest=policy.digest,
        perspective=policy.perspective,
        information_view=policy.information_view,
        driver=policy.driver,
        model=policy.model,
        sampling=policy.sampling,
        max_reviews=policy.max_reviews,
        timeout_seconds=policy.timeout_seconds,
        output_schema_version=policy.output_schema_version,
        max_output_chars=policy.max_output_chars,
    )


@pytest.mark.asyncio
async def test_author_binding_composes_with_the_exact_injected_runtime_registry(
    tmp_path: Path,
) -> None:
    class _Catalog:
        async def has_unsettled_work(self, world_id: str) -> bool:
            return world_id == "bound-world"

    control = ControlCatalogConfig(catalog_dir=tmp_path / "binding-control")
    storage = StorageService(control_catalog_config=control)
    registry = WorldRegistry()
    binding = MissionAuthorActivityBinding(
        world_id="bound-world",
        owner="test-worker",
        reader=StorageMissionCommittedIntentReader(storage),
        catalog=cast(Any, _Catalog()),
        values=cast(Any, object()),
        executor=cast(Any, SimpleNamespace(provider="test-provider")),
        stager=WorldMissionAuthorObservationStager(
            storage=storage,
            registry=registry,
        ),
    )
    resources = build_runtime_resources(
        RuntimeBootstrapConfig(
            control_catalog_config=control,
            storage_service=storage,
            world_registry=registry,
            required_projector_factory=binding.required_projector_for,
            unsettled_world_oracle=binding.has_unsettled_work,
        )
    )
    try:
        create = resources.dispatcher._registry.resolve_name(  # noqa: SLF001
            "create_world"
        ).handler
        assert isinstance(create, partial)
        lifecycle = getattr(create.args[0], "__self__", None)
        assert isinstance(lifecycle, WorldLifecycle)
        assert lifecycle._registry is registry  # noqa: SLF001
        assert lifecycle._required_projector("bound-world") is binding.required_projector
        assert lifecycle._required_projector("other-world") is None
        assert await binding.has_unsettled_work("bound-world")
        assert not await binding.has_unsettled_work("other-world")
    finally:
        await resources.aclose()
        await storage.shutdown()


@pytest.mark.asyncio
async def test_complete_author_observation_survives_restart_without_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = ControlCatalogConfig(catalog_dir=tmp_path / "control")
    storage_config = StorageConfig(
        uri=str(tmp_path / "world-data"),
        namespace="mission-activity-world",
    )
    storage = StorageService(control_catalog_config=control)
    registry = WorldRegistry()
    lifecycle = WorldLifecycle(storage, registry)
    world = await lifecycle.create_world(
        WorldConfig(name="mission-activity"),
        storage_config,
    )
    try:
        policy = CriticPolicy()
        mission_id = await world.create_entity(
            [
                Mission(
                    name="activity-proof",
                    repository="owner/repo",
                    branch="proof/activity",
                    base_ref="main",
                )
            ]
        )
        task_id = await world.create_entity(
            [
                Task(name="prove-activity", prompt="commit proof.txt"),
                TaskWorkspace(
                    repository="owner/repo",
                    branch="proof/activity",
                    base_ref="main",
                ),
                TaskPolicy(),
                _critic_component(policy),
                TaskState(status=TaskStatus.DISPATCHED.value),
                TaskDispatch(dispatch_id="dispatch-1", sequence=1),
            ]
        )
        validator_id = await world.create_entity(
            [
                TaskValidator(
                    name="proof-exists",
                    command=["sh", "-c", "test -f proof.txt"],
                )
            ]
        )
        await world.create_entity([PartOfMission(source=task_id, target=mission_id)])
        await world.create_entity([Guards(source=validator_id, target=task_id)])

        await step(registry, world.world_id, RunConfig())
        intent_receipt = world.last_committed_receipt
        assert intent_receipt is not None
        reader = StorageMissionCommittedIntentReader(storage, storage_config)
        with monkeypatch.context() as patch:

            async def missing_registered_table(_frame):
                raise KeyError("registered Mission table is unavailable")

            patch.setattr(storage, "materialize", missing_registered_table)
            with pytest.raises(KeyError, match="registered Mission table"):
                await reader.read(intent_receipt)
        intent_snapshot = await reader.read(intent_receipt)
        requests = await project_task_dispatch_requests(intent_snapshot.as_post_tick())
        assert len(requests) == 1
        request = requests[0]
        assert (request.mission_id, request.task_id, request.dispatch_id) == (
            mission_id,
            task_id,
            "dispatch-1",
        )

        final_revision = "b" * 40
        observation = DurableAuthorExecutionObservation(
            result=AgentExecutionResult(
                mission_id=mission_id,
                task_id=task_id,
                dispatch_id=request.dispatch_id,
                dispatch_sequence=request.dispatch_sequence,
                status=AgentExecutionStatus.EXITED,
                sandbox=SandboxIdentity("local", "sandbox-1", "test-image"),
                worktree="/workspace/repo",
                agent_session_id="session-1",
                agent_returncode=0,
                starting_revision="a" * 40,
                final_revision=final_revision,
                diff_digest="c" * 64,
                validator_bundle_digest="d" * 64,
                validation=(
                    ValidationObservation(
                        validator_id=validator_id,
                        name="proof-exists",
                        command=("sh", "-c", "test -f proof.txt"),
                        expected_returncode=0,
                        actual_returncode=0,
                        revision=final_revision,
                    ),
                ),
                commits=(
                    CommitObservation(
                        sha=final_revision,
                        message="prove activity",
                        branch=request.branch,
                        pushed=True,
                        final_revision=True,
                    ),
                ),
            ),
            sandbox_status=SandboxStatus.READY,
            redaction_policy_id="test-redaction-v1",
            bind_mission=True,
        )
        result = AuthorActivityResultRef(
            ref="mission-author+json:sha256:" + ("e" * 64),
            digest="e" * 64,
            size_bytes=123,
        )
        second_revision = "1" * 40
        second_request = replace(
            request,
            dispatch_id="dispatch-2",
            dispatch_sequence=2,
        )
        second_observation = replace(
            observation,
            result=replace(
                observation.result,
                dispatch_id=second_request.dispatch_id,
                dispatch_sequence=second_request.dispatch_sequence,
                starting_revision=final_revision,
                final_revision=second_revision,
                diff_digest="2" * 64,
                validator_bundle_digest="3" * 64,
                validation=tuple(
                    replace(item, revision=second_revision)
                    for item in observation.result.validation
                ),
                commits=tuple(
                    replace(item, sha=second_revision) for item in observation.result.commits
                ),
            ),
        )
        second_result = AuthorActivityResultRef(
            ref="mission-author+json:sha256:" + ("4" * 64),
            digest="4" * 64,
            size_bytes=123,
        )
        next_entity_id = int(world.next_entity_id)
        second_fact_count = complete_author_activity_fact_count(
            second_request,
            second_observation,
            prior_candidate_id=1,
        )
        first_start = next_entity_id + second_fact_count + 1
        first_fact_count = complete_author_activity_fact_count(
            request,
            observation,
            prior_candidate_id=None,
        )
        future_first_bundle = complete_author_activity_fact_bundle(
            request,
            observation,
            entity_ids=tuple(range(first_start, first_start + first_fact_count)),
            prior_candidate_id=None,
            candidate_created_at_ms=0,
        )
        second_request = replace(
            second_request,
            prior_candidate_entity_id=future_first_bundle.candidate_entity_id,
        )
        spawn_order: list[type[object]] = []

        async def remember_spawn(event: OnSpawn) -> None:
            spawn_order.append(type(event.components[0]))

        world.add_hook(OnSpawn, remember_spawn)
        stager = WorldMissionAuthorObservationStager(
            storage=storage,
            registry=registry,
        )
        # Delivery order is not lineage authority. Dispatch 2 arrives first,
        # but its committed request already names Dispatch 1's candidate.
        await stager.stage_author_observation(
            world_id=str(world.world_id),
            activity_id=second_request.dispatch_id,
            request=second_request,
            result=second_result,
            observation=second_observation,
        )
        await stager.stage_author_observation(
            world_id=str(world.world_id),
            activity_id=request.dispatch_id,
            request=request,
            result=result,
            observation=observation,
        )
        first_spawn_count = len(spawn_order)

        # Resumed worlds may intern schema-identical component twins. Pending
        # idempotency therefore keys from canonical row columns, not Python
        # class identity in the signature tuple.
        signature_aliases = {}
        for index, (signature, rows) in enumerate(tuple(world.spawn_cache.items())):
            alias = tuple(
                type(f"PendingAlias{index}_{position}", (), {})
                for position, _component in enumerate(signature)
            )
            signature_aliases[alias] = signature
            world.spawn_cache[alias] = world.spawn_cache.pop(signature)
            for row in rows:
                world.entity2sig[int(row["entity_id"])] = alias
        await WorldMissionAuthorObservationStager(
            storage=storage,
            registry=registry,
        ).stage_author_observation(
            world_id=str(world.world_id),
            activity_id=request.dispatch_id,
            request=request,
            result=result,
            observation=observation,
        )
        assert len(spawn_order) == first_spawn_count
        for alias, signature in signature_aliases.items():
            rows = world.spawn_cache.pop(alias)
            world.spawn_cache[signature] = rows
            for row in rows:
                world.entity2sig[int(row["entity_id"])] = signature
        world._sig_intern = None
        assert spawn_order[-1] is CompleteAuthorActivityObservation

        await step(registry, world.world_id, RunConfig())
        observation_receipt = world.last_committed_receipt
        assert observation_receipt is not None
        observed_snapshot = await reader.read(observation_receipt)
        projected = project_complete_author_activity_fact_bundles(observed_snapshot.as_post_tick())
        assert len(projected) == 2
        projected_by_activity = {item.marker.activity_id: item for item in projected}
        bundle = projected_by_activity[request.dispatch_id].bundle
        assert len(bundle.components(Sandbox)) == 1
        assert len(bundle.components(AgentExecution)) == 1
        assert len(bundle.components(ValidationResult)) == 1
        assert len(bundle.components(Commit)) == 1
        assert len(bundle.components(FrictionLog)) == 0
        assert len(bundle.components(Candidate)) == 1
        assert len(bundle.components(PartOfMission)) == 1
        assert len(bundle.components(Executes)) == 1
        assert len(bundle.components(RunsIn)) == 1
        assert len(bundle.components(ProducedBy)) == 2
        assert len(bundle.components(CandidateFor)) == 1
        assert len(bundle.components(AuthoredBy)) == 1
        second_bundle = projected_by_activity[second_request.dispatch_id].bundle
        supersedes = second_bundle.components(Supersedes)
        assert len(supersedes) == 1
        edge = supersedes[0].component
        assert isinstance(edge, Supersedes)
        assert edge.target == bundle.candidate_entity_id
    finally:
        await storage.shutdown()

    # Reconstruct storage, registry, and world. The durable v2 marker makes a
    # redelivered result a no-op; no process-local staged set is required.
    recovered_storage = StorageService(control_catalog_config=control)
    recovered_registry = WorldRegistry()
    recovered_lifecycle = WorldLifecycle(recovered_storage, recovered_registry)
    try:
        recovered_world = await recovered_lifecycle.open_world_mutable(
            storage_config,
            str(world.world_id),
        )
        assert recovered_world.spawn_cache == {}
        recovered_stager = WorldMissionAuthorObservationStager(
            storage=recovered_storage,
            registry=recovered_registry,
        )
        await recovered_stager.stage_author_observation(
            world_id=str(world.world_id),
            activity_id=request.dispatch_id,
            request=request,
            result=result,
            observation=observation,
        )
        assert recovered_world.spawn_cache == {}
        recovered_reader = StorageMissionCommittedIntentReader(
            recovered_storage,
            storage_config,
        )
        recovered_snapshot = await recovered_reader.read(observation_receipt)
        assert (
            len(project_complete_author_activity_fact_bundles(recovered_snapshot.as_post_tick()))
            == 2
        )

        corrupt_request = replace(
            request,
            dispatch_id="dispatch-corrupt",
            dispatch_sequence=3,
        )
        corrupt_observation = replace(
            observation,
            result=replace(
                observation.result,
                dispatch_id=corrupt_request.dispatch_id,
                dispatch_sequence=corrupt_request.dispatch_sequence,
            ),
        )
        corrupt_result = AuthorActivityResultRef(
            ref="mission-author+json:sha256:" + ("f" * 64),
            digest="f" * 64,
            size_bytes=123,
        )
        await recovered_world.create_entity(
            [
                CompleteAuthorActivityObservation(
                    schema_version=2,
                    activity_id=corrupt_request.dispatch_id,
                    task_id=task_id,
                    dispatch_sequence=corrupt_request.dispatch_sequence,
                    result_ref=corrupt_result.ref,
                    result_digest=corrupt_result.digest,
                    fact_bundle_digest="0" * 64,
                    execution_id=recovered_world.next_entity_id + 100,
                    sandbox_entity_id=recovered_world.next_entity_id + 101,
                    relation_count=2,
                    redaction_policy_id=observation.redaction_policy_id,
                )
            ]
        )
        await step(recovered_registry, recovered_world.world_id, RunConfig())
        with pytest.raises(ValueError, match="incomplete committed fact bundle"):
            await recovered_stager.stage_author_observation(
                world_id=str(world.world_id),
                activity_id=corrupt_request.dispatch_id,
                request=corrupt_request,
                result=corrupt_result,
                observation=corrupt_observation,
            )
    finally:
        await recovered_storage.shutdown()
