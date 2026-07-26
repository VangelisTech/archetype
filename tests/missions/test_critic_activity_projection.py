# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Committed-intent projection contracts for Mission critic Activities."""

from __future__ import annotations

import hashlib
from typing import Any

import daft
import pytest

from archetype.core.component import Component
from archetype.core.hooks import PostTick
from archetype.missions.components import (
    AgentExecution,
    Candidate,
    CriticExecution,
    Task,
    TaskCriticPolicy,
    TaskCriticSubjectPolicy,
    TaskState,
    TaskValidator,
    ValidationResult,
)
from archetype.missions.contracts import CriticPolicy
from archetype.missions.critics import CriticActivityCodec
from archetype.missions.critics.contracts import (
    candidate_subject_digest,
    validator_bundle_digest,
)
from archetype.missions.projections import project_critic_activity_requests
from archetype.missions.transitions import (
    AgentExecutionStatus,
    CriticExecutionStatus,
    TaskStatus,
)
from archetype.redaction import RedactionService


def _frame(entity_id: int, tick: int, *components: Component):
    values: dict[str, list[Any]] = {"entity_id": [entity_id], "tick": [tick]}
    for component in components:
        prefix = type(component).get_prefix()
        for field, value in component.model_dump().items():
            values[f"{prefix}{field}"] = [value]
    return daft.from_pydict(values)


def _policy_component(policy: CriticPolicy) -> TaskCriticPolicy:
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


def _event(
    *,
    failed_attempts: int = 0,
    duplicate_current_candidate: bool = False,
    candidate_specs: tuple[tuple[int, int, str], ...] | None = None,
    validation_order: tuple[int, ...] = (4, 5),
    task_policy_digest: str | None = None,
    candidate_policy_digest: str | None = None,
    candidate_digest: str | None = None,
    author_sandbox_id: str = "author-sandbox",
    validator_name: str = "focused",
    validator_expected_returncode: int = 0,
    validation_expected_returncode: int = 0,
) -> PostTick:
    tick = 4
    policy = CriticPolicy(max_reviews=3, max_subject_bytes=12_345)
    task_id = 7
    author_execution_id = 9
    if candidate_specs is None:
        candidate_specs = (
            ((11, 1, "candidate"), (12, 1, "duplicate"))
            if duplicate_current_candidate
            else ((11, 1, "candidate"),)
        )
    current_sequence = max(sequence for _, sequence, _ in candidate_specs)
    current_spec = next(spec for spec in candidate_specs if spec[1] == current_sequence)
    candidate_entity_id, dispatch_sequence, current_label = current_spec
    dispatch_id = hashlib.sha256(f"dispatch-{dispatch_sequence}".encode()).hexdigest()
    current_candidate_id = hashlib.sha256(current_label.encode()).hexdigest()
    task_policy = _policy_component(policy)
    if task_policy_digest is not None:
        task_policy = task_policy.model_copy(update={"digest": task_policy_digest})
    task = (
        Task(name="review-candidate", prompt="Review the candidate."),
        task_policy,
        TaskState(status=TaskStatus.CANDIDATE.value),
    )
    canonical_validator_components = {
        4: TaskValidator(name="focused", command=["pytest", "-q"]),
        5: TaskValidator(name="architecture", command=["make", "architecture-audit"]),
    }
    bundle_digest = validator_bundle_digest(
        tuple(
            (
                validator_id,
                selected.name,
                tuple(selected.command),
                selected.expected_returncode,
                selected.timeout_seconds,
            )
            for validator_id, selected in sorted(canonical_validator_components.items())
        )
    )
    validator_components = dict(canonical_validator_components)
    validator_components[4] = validator_components[4].model_copy(
        update={
            "name": validator_name,
            "expected_returncode": validator_expected_returncode,
        }
    )
    candidates: list[tuple[int, Candidate]] = []
    for entity_id, sequence, label in candidate_specs:
        selected_candidate_id = hashlib.sha256(label.encode()).hexdigest()
        selected_dispatch_id = hashlib.sha256(f"dispatch-{sequence}".encode()).hexdigest()
        selected_policy_digest = candidate_policy_digest or policy.digest
        selected_candidate_digest = candidate_subject_digest(
            candidate_id=selected_candidate_id,
            mission_id=3,
            task_id=task_id,
            dispatch_id=selected_dispatch_id,
            author_execution_id=author_execution_id,
            repository="owner/repo",
            branch="agent/review",
            base_ref="main",
            base_revision="1" * 40,
            head_revision="2" * 40,
            diff_digest=hashlib.sha256(b"diff").hexdigest(),
            validator_bundle_digest=bundle_digest,
            policy_digest=selected_policy_digest,
        )
        if sequence == current_sequence and candidate_digest is not None:
            selected_candidate_digest = candidate_digest
        candidates.append(
            (
                entity_id,
                Candidate(
                    candidate_id=selected_candidate_id,
                    mission_id=3,
                    task_id=task_id,
                    dispatch_id=selected_dispatch_id,
                    dispatch_sequence=sequence,
                    author_execution_id=author_execution_id,
                    author_sandbox_id="author-sandbox",
                    repository="owner/repo",
                    branch="agent/review",
                    base_ref="main",
                    base_revision="1" * 40,
                    head_revision="2" * 40,
                    diff_digest=hashlib.sha256(b"diff").hexdigest(),
                    validator_bundle_digest=bundle_digest,
                    policy_digest=selected_policy_digest,
                    candidate_digest=selected_candidate_digest,
                    created_at_ms=100 + sequence,
                ),
            )
        )
    candidate_frame = _frame(candidates[0][0], tick, candidates[0][1])
    for entity_id, selected in candidates[1:]:
        candidate_frame = candidate_frame.concat(_frame(entity_id, tick, selected))

    validator_frame = _frame(4, tick, validator_components[4]).concat(
        _frame(5, tick, validator_components[5])
    )
    validation_components = {
        validator_id: ValidationResult(
            task_id=task_id,
            validator_id=validator_id,
            execution_id=author_execution_id,
            dispatch_id=dispatch_id,
            dispatch_sequence=dispatch_sequence,
            revision="2" * 40,
            expected_returncode=validation_expected_returncode,
            actual_returncode=0,
        )
        for validator_id in (4, 5)
    }
    validation_frame = _frame(
        30,
        tick,
        validation_components[validation_order[0]],
    )
    for position, validator_id in enumerate(validation_order[1:], start=1):
        validation_frame = validation_frame.concat(
            _frame(
                30 + position,
                tick,
                validation_components[validator_id],
            )
        )
    results = {
        tuple(type(item) for item in task): _frame(task_id, tick, *task),
        (TaskCriticSubjectPolicy,): _frame(
            task_id,
            tick,
            TaskCriticSubjectPolicy(max_subject_bytes=policy.max_subject_bytes),
        ),
        (Candidate,): candidate_frame,
        (AgentExecution,): _frame(
            author_execution_id,
            tick,
            AgentExecution(
                task_id=task_id,
                dispatch_id=dispatch_id,
                dispatch_sequence=dispatch_sequence,
                status=AgentExecutionStatus.EXITED.value,
                sandbox_id=author_sandbox_id,
                starting_revision="1" * 40,
                final_revision="2" * 40,
            ),
        ),
        (TaskValidator,): validator_frame,
        (ValidationResult,): validation_frame,
    }
    if failed_attempts:
        executions = _frame(
            20,
            tick,
            CriticExecution(
                candidate_entity_id=candidate_entity_id,
                candidate_id=current_candidate_id,
                review_id=hashlib.sha256(b"review-1").hexdigest(),
                attempt=1,
                status=CriticExecutionStatus.ERRORED.value,
                sandbox_id="critic-1",
            ),
        )
        for offset in range(1, failed_attempts):
            executions = executions.concat(
                _frame(
                    20 + offset,
                    tick,
                    CriticExecution(
                        candidate_entity_id=candidate_entity_id,
                        candidate_id=current_candidate_id,
                        review_id=hashlib.sha256(f"review-{offset + 1}".encode()).hexdigest(),
                        attempt=offset + 1,
                        status=CriticExecutionStatus.ERRORED.value,
                        sandbox_id=f"critic-{offset + 1}",
                    ),
                )
            )
        results[(CriticExecution,)] = executions
    return PostTick(world_id="world", tick=tick + 1, results=results)


@pytest.mark.asyncio
async def test_only_committed_critic_executions_consume_domain_review_attempts() -> None:
    first = await project_critic_activity_requests(_event())
    after_failure = await project_critic_activity_requests(_event(failed_attempts=1))
    exhausted = await project_critic_activity_requests(_event(failed_attempts=3))

    assert len(first) == len(after_failure) == 1
    assert first[0].attempt == 1
    assert after_failure[0].attempt == 2
    assert first[0].review_id != after_failure[0].review_id
    assert after_failure[0].policy.max_subject_bytes == 12_345
    assert exhausted == ()


@pytest.mark.asyncio
async def test_current_candidate_tie_fails_closed() -> None:
    with pytest.raises(
        ValueError,
        match="multiple current candidates at one dispatch sequence",
    ):
        await project_critic_activity_requests(_event(duplicate_current_candidate=True))


@pytest.mark.asyncio
async def test_validation_order_has_one_canonical_activity_value() -> None:
    forward = await project_critic_activity_requests(_event(validation_order=(4, 5)))
    reverse = await project_critic_activity_requests(_event(validation_order=(5, 4)))

    assert forward == reverse
    codec = CriticActivityCodec(RedactionService())
    forward_value = codec.encode_request(codec.prepare_request(forward[0]))
    reverse_value = codec.encode_request(codec.prepare_request(reverse[0]))
    assert forward_value.ref == reverse_value.ref


@pytest.mark.asyncio
async def test_duplicate_validator_observation_fails_closed() -> None:
    with pytest.raises(ValueError, match="duplicate validator observation"):
        await project_critic_activity_requests(_event(validation_order=(4, 4)))


@pytest.mark.asyncio
async def test_historical_candidate_tie_does_not_change_current_candidate() -> None:
    oldest_first = await project_critic_activity_requests(
        _event(
            candidate_specs=(
                (11, 1, "old-a"),
                (12, 1, "old-b"),
                (13, 2, "current"),
            )
        )
    )
    current_first = await project_critic_activity_requests(
        _event(
            candidate_specs=(
                (13, 2, "current"),
                (12, 1, "old-b"),
                (11, 1, "old-a"),
            )
        )
    )

    assert oldest_first == current_first
    assert oldest_first[0].candidate_entity_id == 13


@pytest.mark.asyncio
async def test_repeated_temporal_evidence_is_entity_idempotent() -> None:
    baseline = _event(failed_attempts=1)
    repeated_results = dict(baseline.results)
    for signature in ((ValidationResult,), (CriticExecution,)):
        current = repeated_results[signature]
        repeated_results[signature] = current.concat(current.with_column("tick", daft.lit(3)))
    repeated = PostTick(
        world_id=baseline.world_id,
        tick=baseline.tick,
        results=repeated_results,
    )

    baseline_requests = await project_critic_activity_requests(baseline)
    repeated_requests = await project_critic_activity_requests(repeated)
    assert repeated_requests == baseline_requests
    assert repeated_requests[0].attempt == 2
    assert len(repeated_requests[0].validation) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event_kwargs", "message"),
    (
        ({"task_policy_digest": "0" * 64}, "critic policy digest"),
        ({"candidate_policy_digest": "0" * 64}, "critic policy digest"),
        ({"candidate_digest": "0" * 64}, "candidate digest"),
        ({"author_sandbox_id": "other-sandbox"}, "author execution"),
        ({"validator_name": "drifted"}, "validator bundle"),
        ({"validation_expected_returncode": 1}, "validation expectation"),
    ),
)
async def test_exact_candidate_identity_drift_fails_closed(
    event_kwargs: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        await project_critic_activity_requests(_event(**event_kwargs))
