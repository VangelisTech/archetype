# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real-world contracts for the Mission critic Activity integration."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from daft import lit

from archetype.activities import ActivityCoordinator
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.core.hooks import OnSpawn
from archetype.missions.activity_world import StorageMissionCommittedIntentReader
from archetype.missions.author_activity import CommittedMissionSnapshot
from archetype.missions.components import (
    AgentExecution,
    Candidate,
    CompleteCriticActivityObservation,
    CriticExecution,
    CriticFinding,
    CriticReceipt,
    Mission,
    Sandbox,
    Task,
    TaskCriticPolicy,
    TaskCriticSubjectPolicy,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    TaskWorkspace,
    ValidationResult,
)
from archetype.missions.contracts import CriticPolicy
from archetype.missions.critic_activity import MissionCriticActivityProjector
from archetype.missions.critic_activity_coordinator import (
    MissionCriticActivityCoordinator,
)
from archetype.missions.critic_activity_world import (
    MissionCriticActivityBinding,
    WorldMissionCriticObservationStager,
)
from archetype.missions.critics import (
    CandidateReviewRequest,
    CriticActivityCodec,
    CriticActivityRequest,
    CriticActivityResultRef,
    CriticExecutionResult,
    CriticFindingValue,
    CriticReceiptValue,
    CriticRecoveryUnknown,
    CriticSubjectPolicy,
    CriticSubjectTransport,
    CriticValidationEvidence,
    bind_critic_subject,
)
from archetype.missions.critics.contracts import (
    canonical_digest,
    validator_bundle_digest,
)
from archetype.missions.local_critic_activity_values import (
    LocalMissionCriticValueStore,
)
from archetype.missions.projections import (
    project_complete_critic_activity_fact_bundles,
    project_critic_activity_requests,
)
from archetype.missions.relations import ProducedBy, Reviews, RunsIn
from archetype.missions.sandboxes import SandboxIdentity, SandboxStatus
from archetype.missions.transitions import (
    AgentExecutionStatus,
    CriticConclusion,
    CriticExecutionStatus,
    TaskStatus,
)
from archetype.redaction import RedactionService
from archetype.storage.activity_catalog import SqliteActivityCatalog
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.registry import WorldRegistry
from archetype.world.simulation import step

_DIFF = b"diff --git a/value.txt b/value.txt\n-base\n+candidate\n"


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


def _raw_result(
    request: CriticActivityRequest,
    *,
    completed: bool = True,
) -> CriticExecutionResult:
    raw_request = request.as_review_request()
    sandbox = SandboxIdentity("local", f"critic-{request.review_id[:12]}", "critic")
    if not completed:
        return CriticExecutionResult(
            request=raw_request,
            status=CriticExecutionStatus.ERRORED,
            sandbox=sandbox,
            sandbox_status=SandboxStatus.ERRORED,
            sandbox_acquired=True,
            started_at_ms=110,
            ended_at_ms=120,
            error="critic failed",
        )
    subject = bind_critic_subject(
        CriticSubjectPolicy(
            digest=request.diff_digest,
            max_bytes=request.subject.max_bytes,
        ),
        metadata=f"review:{request.review_id}".encode(),
        content=_DIFF,
        transport=CriticSubjectTransport.SANDBOX_FILE,
        ref=f"/tmp/archetype-critic-subject.{request.review_id[:12]}/subject.diff",
    )
    finding = CriticFindingValue(
        finding_id="finding-advisory",
        severity="advisory",
        category="maintainability",
        confidence=0.8,
        title="Small cleanup",
        detail="The exact candidate is otherwise acceptable.",
    )
    receipt = CriticReceiptValue(
        review_id=request.review_id,
        conclusion=CriticConclusion.APPROVED,
        candidate_digest=request.candidate_digest,
        policy_digest=request.policy.digest,
        evidence_digest=canonical_digest({"conclusion": "approved"}),
        reviewed_base_revision=request.base_revision,
        reviewed_head_revision=request.head_revision,
        reviewed_diff_digest=request.diff_digest,
        validator_bundle_digest=request.validator_bundle_digest,
        subject_metadata_digest=subject.metadata_digest,
        subject_digest=subject.subject_digest,
        subject_content_size_bytes=subject.content_size_bytes,
        subject_metadata_size_bytes=subject.metadata_size_bytes,
        subject_size_bytes=subject.total_size_bytes,
        subject_media_type=subject.media_type,
        subject_transport=subject.transport.value,
        subject_ref=subject.ref,
        reviewed_scope="exact task diff",
        finding_count=1,
        blocking_count=0,
        output_schema_version=request.policy.output_schema_version,
        completed_at_ms=200,
    )
    return CriticExecutionResult(
        request=raw_request,
        status=CriticExecutionStatus.EXITED,
        sandbox=sandbox,
        sandbox_status=SandboxStatus.READY,
        sandbox_acquired=True,
        started_at_ms=110,
        ended_at_ms=200,
        raw_output='{"conclusion":"approved","schema_version":1}',
        findings=(finding,),
        receipt=receipt,
    )


class _Executor:
    provider = "test-critic"

    def __init__(self) -> None:
        self.execute_calls = 0

    async def execute(
        self,
        *,
        operation_id: str,
        request: CriticActivityRequest,
        attempt: int,
        fence: int,
        retry_guard,
    ) -> CriticExecutionResult:
        del operation_id, attempt, fence, retry_guard
        self.execute_calls += 1
        return _raw_result(request)

    async def reconcile(
        self,
        *,
        operation_id: str,
        request: CriticActivityRequest,
    ):
        del operation_id, request
        return CriticRecoveryUnknown("not used by this proof")


@pytest.mark.asyncio
async def test_complete_critic_observation_settles_exact_receipt_and_survives_restart(
    tmp_path: Path,
) -> None:
    control = ControlCatalogConfig(catalog_dir=tmp_path / "control")
    storage_config = StorageConfig(
        uri=str(tmp_path / "world-data"),
        namespace="mission-critic-activity-world",
    )
    storage = StorageService(control_catalog_config=control)
    registry = WorldRegistry()
    lifecycle = WorldLifecycle(storage, registry)
    world = await lifecycle.create_world(
        WorldConfig(name="mission-critic-activity"),
        storage_config,
    )
    physical = SqliteActivityCatalog(tmp_path / "activities.db")
    generic = ActivityCoordinator(physical)
    catalog = MissionCriticActivityCoordinator(generic, lease_seconds=30)
    values = LocalMissionCriticValueStore(
        tmp_path / "values",
        codec=CriticActivityCodec(RedactionService()),
    )
    executor = _Executor()
    reader = StorageMissionCommittedIntentReader(storage, storage_config)
    stager = WorldMissionCriticObservationStager(
        storage=storage,
        registry=registry,
    )
    binding = MissionCriticActivityBinding(
        world_id=str(world.world_id),
        owner="critic-worker",
        reader=reader,
        catalog=catalog,
        values=values,
        executor=executor,
        stager=stager,
    )
    try:
        policy = CriticPolicy(max_reviews=2, max_subject_bytes=1 << 20)
        mission_id = await world.create_entity(
            [
                Mission(
                    name="critic-proof",
                    repository="owner/repo",
                    branch="agent/review",
                    base_ref="main",
                )
            ]
        )
        task_id = await world.create_entity(
            [
                Task(name="review-candidate", prompt="Review the exact candidate."),
                TaskWorkspace(
                    repository="owner/repo",
                    branch="agent/review",
                    base_ref="main",
                ),
                TaskPolicy(),
                _critic_component(policy),
                TaskCriticSubjectPolicy(
                    max_subject_bytes=policy.max_subject_bytes,
                ),
                TaskState(status=TaskStatus.CANDIDATE.value),
                TaskDispatch(
                    dispatch_id=hashlib.sha256(b"dispatch").hexdigest(),
                    sequence=1,
                ),
            ]
        )
        author_execution_id = await world.create_entity(
            [
                AgentExecution(
                    task_id=task_id,
                    dispatch_id=hashlib.sha256(b"dispatch").hexdigest(),
                    dispatch_sequence=1,
                    status=AgentExecutionStatus.EXITED.value,
                    sandbox_id="author-sandbox",
                    agent_session_id="author-session",
                    agent_returncode=0,
                    redaction_policy_id=RedactionService().policy_id,
                    starting_revision="1" * 40,
                    final_revision="2" * 40,
                )
            ]
        )
        validator_spec = TaskValidator(name="focused", command=["pytest", "-q"])
        validator_id = await world.create_entity([validator_spec])
        exact_validator_bundle_digest = validator_bundle_digest(
            (
                (
                    validator_id,
                    validator_spec.name,
                    tuple(validator_spec.command),
                    validator_spec.expected_returncode,
                    validator_spec.timeout_seconds,
                ),
            )
        )
        await world.create_entity(
            [
                ValidationResult(
                    task_id=task_id,
                    validator_id=validator_id,
                    execution_id=author_execution_id,
                    dispatch_id=hashlib.sha256(b"dispatch").hexdigest(),
                    dispatch_sequence=1,
                    revision="2" * 40,
                    expected_returncode=0,
                    actual_returncode=0,
                    stdout="passed",
                )
            ]
        )
        candidate_entity_id = int(world.next_entity_id)
        expected_request = CandidateReviewRequest(
            candidate_entity_id=candidate_entity_id,
            candidate_id=hashlib.sha256(b"candidate").hexdigest(),
            mission_id=mission_id,
            task_id=task_id,
            task_name="review-candidate",
            task_prompt="Review the exact candidate.",
            dispatch_id=hashlib.sha256(b"dispatch").hexdigest(),
            dispatch_sequence=1,
            author_execution_id=author_execution_id,
            author_sandbox_id="author-sandbox",
            repository="owner/repo",
            branch="agent/review",
            base_ref="main",
            base_revision="1" * 40,
            head_revision="2" * 40,
            diff_digest=hashlib.sha256(_DIFF).hexdigest(),
            validator_bundle_digest=exact_validator_bundle_digest,
            policy=policy,
            validation=(
                CriticValidationEvidence(
                    validator_id=validator_id,
                    name="focused",
                    command=("pytest", "-q"),
                    expected_returncode=0,
                    actual_returncode=0,
                    revision="2" * 40,
                    stdout="passed",
                ),
            ),
            candidate_published_at_ms=100,
            attempt=1,
        )
        created_candidate = await world.create_entity(
            [
                Candidate(
                    candidate_id=expected_request.candidate_id,
                    mission_id=mission_id,
                    task_id=task_id,
                    dispatch_id=expected_request.dispatch_id,
                    dispatch_sequence=1,
                    author_execution_id=author_execution_id,
                    author_sandbox_id=expected_request.author_sandbox_id,
                    repository=expected_request.repository,
                    branch=expected_request.branch,
                    base_ref=expected_request.base_ref,
                    base_revision=expected_request.base_revision,
                    head_revision=expected_request.head_revision,
                    diff_digest=expected_request.diff_digest,
                    validator_bundle_digest=expected_request.validator_bundle_digest,
                    policy_digest=policy.digest,
                    candidate_digest=expected_request.candidate_digest,
                    created_at_ms=100,
                )
            ]
        )
        assert created_candidate == candidate_entity_id

        await step(registry, world.world_id, RunConfig())
        intent_receipt = world.last_committed_receipt
        assert intent_receipt is not None
        snapshot = await reader.read(intent_receipt)
        projected_requests = await project_critic_activity_requests(snapshot.as_post_tick())
        assert projected_requests == (expected_request,)

        await binding.projector.project(intent_receipt)
        await binding.projector.project(intent_receipt)
        admitted = await generic.pending(
            kind="missions.critic",
            world_id=str(world.world_id),
        )
        assert len(admitted) == 1

        spawn_order: list[type[object]] = []

        async def remember_spawn(event: OnSpawn) -> None:
            spawn_order.append(type(event.components[0]))

        world.add_hook(OnSpawn, remember_spawn)
        assert await binding.worker.run_once()
        assert executor.execute_calls == 1
        first_spawn_count = len(spawn_order)
        assert spawn_order[-1] is CompleteCriticActivityObservation
        pending = await catalog.pending_critic_results(world_id=str(world.world_id))
        assert len(pending) == 1
        durable_request = await values.get_request(pending[0].request)
        durable_result = await values.get_result(pending[0].result)

        # A fresh stager reconstructs the pending canonical rows rather than
        # relying on process-local deduplication state or Python class identity
        # in a resumed signature intern table.
        signature_aliases = {}
        for index, (signature, rows) in enumerate(tuple(world.spawn_cache.items())):
            alias = tuple(
                type(f"PendingCriticAlias{index}_{position}", (), {})
                for position, _component in enumerate(signature)
            )
            signature_aliases[alias] = signature
            world.spawn_cache[alias] = world.spawn_cache.pop(signature)
            for row in rows:
                world.entity2sig[int(row["entity_id"])] = alias
        await WorldMissionCriticObservationStager(
            storage=storage,
            registry=registry,
        ).stage_critic_observation(
            world_id=str(world.world_id),
            activity_id=durable_request.review_id,
            request=durable_request,
            result=pending[0].result,
            observation=durable_result,
        )
        assert len(spawn_order) == first_spawn_count
        for alias, signature in signature_aliases.items():
            rows = world.spawn_cache.pop(alias)
            world.spawn_cache[signature] = rows
            for row in rows:
                world.entity2sig[int(row["entity_id"])] = signature
        world._sig_intern = None  # noqa: SLF001

        await step(registry, world.world_id, RunConfig())
        observation_receipt = world.last_committed_receipt
        assert observation_receipt is not None
        observed = await reader.read(observation_receipt)
        complete = project_complete_critic_activity_fact_bundles(observed.as_post_tick())
        assert len(complete) == 1
        bundle = complete[0].bundle
        marker = complete[0].marker
        assert len(bundle.components(Sandbox)) == 1
        assert len(bundle.components(CriticExecution)) == 1
        assert len(bundle.components(CriticFinding)) == 1
        assert len(bundle.components(CriticReceipt)) == 1
        assert len(bundle.components(Reviews)) == 1
        assert len(bundle.components(RunsIn)) == 1
        assert len(bundle.components(ProducedBy)) == 2
        assert marker.author_sandbox_id == "author-sandbox"
        assert marker.critic_sandbox_id != marker.author_sandbox_id
        assert marker.subject_content_digest == expected_request.diff_digest
        assert marker.subject_content_size_bytes == len(_DIFF)
        assert marker.result_digest == pending[0].result.digest

        wrong_results = dict(observed.results)
        marker_prefix = CompleteCriticActivityObservation.get_prefix()
        wrong_results[(CompleteCriticActivityObservation,)] = wrong_results[
            (CompleteCriticActivityObservation,)
        ].with_column(
            f"{marker_prefix}result_digest",
            lit("0" * 64),
        )
        wrong_snapshot = replace(observed, results=wrong_results)

        class _Reader:
            async def read(self, _receipt) -> CommittedMissionSnapshot:
                return wrong_snapshot

        wrong_projector = MissionCriticActivityProjector(
            reader=_Reader(),
            catalog=catalog,
            values=values,
        )
        await wrong_projector.project(observation_receipt)
        not_settled = await generic.get(
            kind="missions.critic",
            world_id=str(world.world_id),
            activity_id=expected_request.review_id,
        )
        assert not_settled is not None
        assert not_settled.settlement is None

        await binding.projector.project(observation_receipt)
        settled = await generic.get(
            kind="missions.critic",
            world_id=str(world.world_id),
            activity_id=expected_request.review_id,
        )
        assert settled is not None
        assert settled.settlement is not None
        assert settled.settlement.receipt == observation_receipt
        assert settled.settlement.result_digest == pending[0].result.digest
    finally:
        await physical.close()
        await storage.shutdown()

    recovered_storage = StorageService(control_catalog_config=control)
    recovered_registry = WorldRegistry()
    recovered_lifecycle = WorldLifecycle(recovered_storage, recovered_registry)
    try:
        recovered_world = await recovered_lifecycle.open_world_mutable(
            storage_config,
            str(world.world_id),
        )
        assert recovered_world.spawn_cache == {}
        recovered_stager = WorldMissionCriticObservationStager(
            storage=recovered_storage,
            registry=recovered_registry,
        )
        await recovered_stager.stage_critic_observation(
            world_id=str(world.world_id),
            activity_id=durable_request.review_id,
            request=durable_request,
            result=pending[0].result,
            observation=durable_result,
        )
        assert recovered_world.spawn_cache == {}

        corrupt_request = replace(
            durable_request,
            candidate_id=hashlib.sha256(b"corrupt-candidate").hexdigest(),
            domain_review_attempt=2,
        )
        codec = CriticActivityCodec(RedactionService())
        corrupt_result = codec.prepare_result(
            _raw_result(corrupt_request, completed=False),
            corrupt_request,
        )
        encoded_corrupt = codec.encode_result(corrupt_result)
        corrupt_ref = CriticActivityResultRef(
            ref=encoded_corrupt.ref,
            digest=encoded_corrupt.digest,
            media_type=encoded_corrupt.media_type,
            size_bytes=encoded_corrupt.size_bytes,
        )
        await recovered_world.create_entity(
            [
                CompleteCriticActivityObservation(
                    activity_id=corrupt_request.review_id,
                    candidate_entity_id=corrupt_request.candidate_entity_id,
                    domain_review_attempt=corrupt_request.domain_review_attempt,
                    result_ref=corrupt_ref.ref,
                    result_digest=corrupt_ref.digest,
                    fact_bundle_digest="0" * 64,
                    execution_id=recovered_world.next_entity_id + 100,
                    sandbox_entity_id=recovered_world.next_entity_id + 101,
                    relation_count=2,
                    author_sandbox_id=corrupt_request.author_sandbox_id,
                    critic_sandbox_id=corrupt_result.sandbox.sandbox_id,
                    redaction_policy_id=corrupt_result.redaction_policy_id,
                )
            ]
        )
        await step(recovered_registry, recovered_world.world_id, RunConfig())
        with pytest.raises(ValueError, match="incomplete committed fact bundle"):
            await recovered_stager.stage_critic_observation(
                world_id=str(world.world_id),
                activity_id=corrupt_request.review_id,
                request=corrupt_request,
                result=corrupt_ref,
                observation=corrupt_result,
            )
    finally:
        await recovered_storage.shutdown()


@pytest.mark.asyncio
async def test_critic_binding_scopes_required_projector_and_unsettled_oracle(
    tmp_path: Path,
) -> None:
    class _Catalog:
        async def has_unsettled_work(self, world_id: str) -> bool:
            return world_id == "bound-world"

    storage = StorageService(
        control_catalog_config=ControlCatalogConfig(catalog_dir=tmp_path / "binding-control")
    )
    registry = WorldRegistry()
    binding = MissionCriticActivityBinding(
        world_id="bound-world",
        owner="worker",
        reader=StorageMissionCommittedIntentReader(storage),
        catalog=cast(Any, _Catalog()),
        values=cast(Any, object()),
        executor=cast(Any, _Executor()),
        stager=WorldMissionCriticObservationStager(
            storage=storage,
            registry=registry,
        ),
    )
    try:
        assert binding.required_projector_for("bound-world") is binding.required_projector
        assert binding.required_projector_for("other-world") is None
        assert await binding.has_unsettled_work("bound-world")
        assert not await binding.has_unsettled_work("other-world")
    finally:
        await storage.shutdown()
