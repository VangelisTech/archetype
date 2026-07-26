# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Crash contracts for exact-candidate Mission critic Activities."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import TypeAdapter

from archetype.activities import ActivityCoordinator
from archetype.app.missions.critic_activities import (
    CriticActivityReconciliationRequired,
    MissionCriticActivityWorker,
)
from archetype.app.missions.critic_activity_coordinator import (
    MissionCriticActivityCoordinator,
)
from archetype.app.missions.local_critic_activity_values import (
    LocalMissionCriticValueStore,
)
from archetype.core.interfaces import CommittedTickReceipt
from archetype.missions import CriticPolicy
from archetype.missions.critics import (
    CandidateReviewRequest,
    CriticActivityCodec,
    CriticActivityRequest,
    CriticActivityResult,
    CriticActivityResultRef,
    CriticActivityRetryGuard,
    CriticConfirmedAbsent,
    CriticExecutionResult,
    CriticReceiptValue,
    CriticRecovered,
    CriticRecoveryUnknown,
    CriticSubjectPolicy,
    CriticSubjectTransport,
    bind_critic_subject,
    critic_provider_operation_id,
)
from archetype.missions.critics.contracts import canonical_digest
from archetype.missions.sandboxes import SandboxIdentity, SandboxStatus
from archetype.missions.transitions import CriticConclusion, CriticExecutionStatus
from archetype.redaction import RedactionService
from archetype.storage.activity_catalog import SqliteActivityCatalog

_RAW_RESULT = TypeAdapter(CriticExecutionResult)


def _git(*arguments: str, cwd: Path | None = None) -> str:
    return subprocess.run(
        ("git", *arguments),
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _candidate_repository(root: Path) -> tuple[Path, str, str, bytes]:
    repository = root / "candidate"
    _git("init", "--initial-branch=main", str(repository))
    _git("config", "user.name", "Archetype Test", cwd=repository)
    _git("config", "user.email", "test@archetype.local", cwd=repository)
    (repository / "value.txt").write_text("base\n")
    _git("add", "value.txt", cwd=repository)
    _git("commit", "-m", "base", cwd=repository)
    base = _git("rev-parse", "HEAD", cwd=repository)
    (repository / "value.txt").write_text("candidate\n")
    _git("add", "value.txt", cwd=repository)
    _git("commit", "-m", "candidate", cwd=repository)
    head = _git("rev-parse", "HEAD", cwd=repository)
    diff = subprocess.run(
        (
            "git",
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--binary",
            base,
            head,
        ),
        cwd=repository,
        check=True,
        capture_output=True,
    ).stdout
    return repository, base, head, diff


def _request(root: Path) -> tuple[CandidateReviewRequest, bytes]:
    repository, base, head, diff = _candidate_repository(root)
    policy = CriticPolicy(max_subject_bytes=1 << 20)
    return (
        CandidateReviewRequest(
            candidate_entity_id=11,
            candidate_id=hashlib.sha256(b"candidate").hexdigest(),
            mission_id=3,
            task_id=7,
            task_name="Review candidate",
            task_prompt="Prove the exact candidate is correct.",
            dispatch_id=hashlib.sha256(b"dispatch").hexdigest(),
            dispatch_sequence=1,
            author_execution_id=9,
            author_sandbox_id="author-sandbox",
            repository=str(repository),
            branch="main",
            base_ref="main",
            base_revision=base,
            head_revision=head,
            diff_digest=hashlib.sha256(diff).hexdigest(),
            validator_bundle_digest=hashlib.sha256(b"validators").hexdigest(),
            policy=policy,
            validation=(),
            candidate_published_at_ms=100,
            attempt=1,
        ),
        diff,
    )


def _open_catalog(
    path: Path,
    *,
    lease_seconds: float = 30,
    now_seconds: Callable[[], float] | None = None,
) -> tuple[
    SqliteActivityCatalog,
    ActivityCoordinator,
    MissionCriticActivityCoordinator,
]:
    physical = (
        SqliteActivityCatalog(path)
        if now_seconds is None
        else SqliteActivityCatalog(path, now_seconds=now_seconds)
    )
    generic = ActivityCoordinator(physical)
    return (
        physical,
        generic,
        MissionCriticActivityCoordinator(
            generic,
            lease_seconds=lease_seconds,
        ),
    )


class _DurableGitCritic:
    """Review a real Git diff and durably publish one provider result marker."""

    provider = "local-git-critic"

    def __init__(
        self,
        root: Path,
        *,
        crash_after_publish: bool = False,
        unknown: bool = False,
        confirmed_absent: bool = False,
    ) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.crash_after_publish = crash_after_publish
        self.unknown = unknown
        self.confirmed_absent = confirmed_absent
        self.state_path = root / "state.json"
        if not self.state_path.exists():
            self.state_path.write_text(
                json.dumps(
                    {
                        "execute_calls": 0,
                        "reconcile_calls": 0,
                        "subject_digest": "",
                        "subject_size_bytes": 0,
                        "subject_ref": "",
                        "retry_guard_ref": "",
                    },
                    sort_keys=True,
                )
            )

    def _state(self) -> dict[str, Any]:
        return json.loads(self.state_path.read_text())

    def _write_state(self, state: dict[str, Any]) -> None:
        self.state_path.write_text(json.dumps(state, sort_keys=True))

    @property
    def execute_calls(self) -> int:
        return int(self._state()["execute_calls"])

    @property
    def reconcile_calls(self) -> int:
        return int(self._state()["reconcile_calls"])

    async def execute(
        self,
        *,
        operation_id: str,
        request: CriticActivityRequest,
        attempt: int,
        fence: int,
        retry_guard: CriticActivityRetryGuard | None,
    ) -> CriticExecutionResult:
        del attempt, fence
        state = self._state()
        state["execute_calls"] = int(state["execute_calls"]) + 1
        state["retry_guard_ref"] = retry_guard.ref if retry_guard is not None else ""
        self._write_state(state)
        result = self._review(operation_id, request)
        self._publish(operation_id, result)
        if self.crash_after_publish:
            raise RuntimeError("worker died after external critic publication")
        return result

    async def reconcile(
        self,
        *,
        operation_id: str,
        request: CriticActivityRequest,
    ):
        state = self._state()
        state["reconcile_calls"] = int(state["reconcile_calls"]) + 1
        self._write_state(state)
        if self.unknown:
            return CriticRecoveryUnknown("provider lookup unavailable")
        if self.confirmed_absent:
            return CriticConfirmedAbsent(
                CriticActivityRetryGuard(
                    ref=f"critic-retry://{operation_id}",
                    digest=hashlib.sha256(operation_id.encode()).hexdigest(),
                )
            )
        result_path = self._result_path(operation_id)
        if not result_path.exists():
            return CriticRecoveryUnknown("provider has no atomic absence barrier")
        result = _RAW_RESULT.validate_python(json.loads(result_path.read_text()))
        if result.request.review_id != request.review_id:
            return CriticRecoveryUnknown("provider result belongs to another review")
        return CriticRecovered(result)

    def _review(
        self,
        operation_id: str,
        request: CriticActivityRequest,
    ) -> CriticExecutionResult:
        repository = Path(request.repository)
        diff = subprocess.run(
            (
                "git",
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--binary",
                request.base_revision,
                request.head_revision,
            ),
            cwd=repository,
            check=True,
            capture_output=True,
        ).stdout
        subject_directory = Path(
            tempfile.mkdtemp(prefix="archetype-critic-subject.", dir=self.root)
        )
        subject_path = subject_directory / "subject.diff"
        try:
            subject_path.write_bytes(diff)
            metadata = f"review:{request.review_id}".encode()
            subject = bind_critic_subject(
                CriticSubjectPolicy(
                    digest=request.diff_digest,
                    max_bytes=request.subject.max_bytes,
                ),
                metadata=metadata,
                content=subject_path.read_bytes(),
                transport=CriticSubjectTransport.SANDBOX_FILE,
                ref=str(subject_path),
            )
            state = self._state()
            state["subject_digest"] = subject.content_digest
            state["subject_size_bytes"] = subject.content_size_bytes
            state["subject_ref"] = subject.ref
            self._write_state(state)
            completed_at_ms = 200
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
                finding_count=0,
                blocking_count=0,
                output_schema_version=request.policy.output_schema_version,
                completed_at_ms=completed_at_ms,
            )
            operation_digest = hashlib.sha256(operation_id.encode()).hexdigest()
            return CriticExecutionResult(
                request=request.as_review_request(),
                status=CriticExecutionStatus.EXITED,
                sandbox=SandboxIdentity(
                    "local",
                    f"critic-{operation_digest[:16]}",
                    "local-git",
                ),
                sandbox_status=SandboxStatus.READY,
                sandbox_acquired=True,
                started_at_ms=150,
                ended_at_ms=completed_at_ms,
                raw_output='{"conclusion":"approved","schema_version":1}',
                trace_uri=f"local-git-critic://{operation_digest}",
                findings=(),
                receipt=receipt,
            )
        finally:
            if subject_path.exists():
                subject_path.unlink()
            subject_directory.rmdir()

    def _publish(self, operation_id: str, result: CriticExecutionResult) -> None:
        path = self._result_path(operation_id)
        payload = json.dumps(
            _RAW_RESULT.dump_python(result, mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        handle, temporary = tempfile.mkstemp(
            dir=self.root,
            prefix=f".{path.name}.",
            suffix=".tmp",
        )
        try:
            with os.fdopen(handle, "w") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def _result_path(self, operation_id: str) -> Path:
        digest = hashlib.sha256(operation_id.encode()).hexdigest()
        return self.root / f"{digest}.result.json"


class _Stager:
    def __init__(self, *, crash: bool = False) -> None:
        self.crash = crash
        self.staged: dict[tuple[str, str], CriticActivityResult] = {}
        self.refs: dict[tuple[str, str], CriticActivityResultRef] = {}

    async def stage_critic_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        request: CriticActivityRequest,
        result: CriticActivityResultRef,
        observation: CriticActivityResult,
    ) -> None:
        assert request.review_id == activity_id == observation.review_id
        if self.crash:
            self.crash = False
            raise RuntimeError("worker died before critic observation staging")
        self.staged.setdefault((world_id, activity_id), observation)
        self.refs.setdefault((world_id, activity_id), result)


@pytest.mark.asyncio
async def test_cold_restart_reconciles_exact_file_bound_critic_without_replay(
    tmp_path: Path,
) -> None:
    world_id = "world-a"
    receipt = CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0)
    raw_request, exact_diff = _request(tmp_path / "git")
    catalog_path = tmp_path / "activities.db"
    values_path = tmp_path / "values"
    provider_path = tmp_path / "provider"
    now = [100.0]

    def clock() -> float:
        return now[0]

    physical, generic, catalog = _open_catalog(
        catalog_path,
        now_seconds=clock,
    )
    values = LocalMissionCriticValueStore(
        values_path,
        codec=CriticActivityCodec(RedactionService()),
    )
    request_ref = await values.put_request(raw_request)
    durable_request = await values.get_request(request_ref)
    await catalog.admit_critic(
        world_id=world_id,
        receipt=receipt,
        activity_id=durable_request.review_id,
        request=request_ref,
    )
    first_provider = _DurableGitCritic(
        provider_path,
        crash_after_publish=True,
    )
    first_worker = MissionCriticActivityWorker(
        world_id=world_id,
        owner="before-provider-crash",
        catalog=catalog,
        values=values,
        executor=first_provider,
        stager=_Stager(),
    )
    with pytest.raises(RuntimeError, match="after external critic publication"):
        await first_worker.run_once()
    assert first_provider.execute_calls == 1
    assert first_provider._state()["subject_digest"] == hashlib.sha256(exact_diff).hexdigest()
    assert first_provider._state()["subject_size_bytes"] == len(exact_diff)
    assert not Path(str(first_provider._state()["subject_ref"])).exists()
    operation_id = critic_provider_operation_id(world_id, raw_request.review_id)
    assert first_provider._result_path(operation_id).exists()
    await physical.close()
    now[0] += 31

    recovered_physical, _, recovered_catalog = _open_catalog(
        catalog_path,
        lease_seconds=30,
        now_seconds=clock,
    )
    recovered_provider = _DurableGitCritic(provider_path)
    crash_before_stage = _Stager(crash=True)
    recovered_worker = MissionCriticActivityWorker(
        world_id=world_id,
        owner="after-provider-crash",
        catalog=recovered_catalog,
        values=LocalMissionCriticValueStore(
            values_path,
            codec=CriticActivityCodec(RedactionService()),
        ),
        executor=recovered_provider,
        stager=crash_before_stage,
    )
    with pytest.raises(RuntimeError, match="before critic observation staging"):
        await recovered_worker.run_once()
    assert recovered_provider.execute_calls == 1
    assert recovered_provider.reconcile_calls == 1
    await recovered_physical.close()

    final_physical, _, final_catalog = _open_catalog(catalog_path)
    final_stager = _Stager()
    final_worker = MissionCriticActivityWorker(
        world_id=world_id,
        owner="after-result-record",
        catalog=final_catalog,
        values=LocalMissionCriticValueStore(
            values_path,
            codec=CriticActivityCodec(RedactionService()),
        ),
        executor=_DurableGitCritic(provider_path),
        stager=final_stager,
    )
    assert await final_worker.run_once()
    observed = final_stager.staged[(world_id, raw_request.review_id)]
    assert observed.domain_review_attempt == raw_request.attempt == 1
    assert observed.receipt is not None
    assert observed.receipt.subject.content_digest == raw_request.diff_digest
    assert observed.receipt.subject.content_size_bytes == len(exact_diff)
    assert observed.sandbox.sandbox_id != raw_request.author_sandbox_id
    assert _DurableGitCritic(provider_path).execute_calls == 1
    assert len(await final_catalog.pending_critic_results(world_id=world_id)) == 1
    await final_physical.close()


@pytest.mark.asyncio
async def test_unknown_reconciliation_fails_closed_without_critic_replay(
    tmp_path: Path,
) -> None:
    world_id = "world-a"
    raw_request, _ = _request(tmp_path / "git")
    catalog_path = tmp_path / "activities.db"
    values_path = tmp_path / "values"
    provider_path = tmp_path / "provider"
    receipt = CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0)
    now = [100.0]

    def clock() -> float:
        return now[0]

    physical, _, catalog = _open_catalog(
        catalog_path,
        now_seconds=clock,
    )
    values = LocalMissionCriticValueStore(
        values_path,
        codec=CriticActivityCodec(RedactionService()),
    )
    request_ref = await values.put_request(raw_request)
    await catalog.admit_critic(
        world_id=world_id,
        receipt=receipt,
        activity_id=raw_request.review_id,
        request=request_ref,
    )
    claim = await catalog.claim_critic(world_id=world_id, owner="first")
    assert claim is not None
    await catalog.bind_provider_operation(
        claim,
        provider=_DurableGitCritic.provider,
        operation_id=critic_provider_operation_id(world_id, raw_request.review_id),
    )
    await physical.close()
    now[0] += 31

    recovered_physical, _, recovered_catalog = _open_catalog(
        catalog_path,
        now_seconds=clock,
    )
    provider = _DurableGitCritic(provider_path, unknown=True)
    worker = MissionCriticActivityWorker(
        world_id=world_id,
        owner="recovered",
        catalog=recovered_catalog,
        values=values,
        executor=provider,
        stager=_Stager(),
    )
    with pytest.raises(CriticActivityReconciliationRequired):
        await worker.run_once()
    assert provider.execute_calls == 0
    assert provider.reconcile_calls == 1
    await recovered_physical.close()


@pytest.mark.asyncio
async def test_confirmed_absence_rebinds_and_executes_with_exact_retry_guard(
    tmp_path: Path,
) -> None:
    world_id = "world-a"
    raw_request, _ = _request(tmp_path / "git")
    catalog_path = tmp_path / "activities.db"
    values_path = tmp_path / "values"
    provider_path = tmp_path / "provider"
    receipt = CommittedTickReceipt(world_id, "run-a", 1, "token-1", 0)
    now = [100.0]

    def clock() -> float:
        return now[0]

    physical, _, catalog = _open_catalog(
        catalog_path,
        now_seconds=clock,
    )
    values = LocalMissionCriticValueStore(
        values_path,
        codec=CriticActivityCodec(RedactionService()),
    )
    request_ref = await values.put_request(raw_request)
    await catalog.admit_critic(
        world_id=world_id,
        receipt=receipt,
        activity_id=raw_request.review_id,
        request=request_ref,
    )
    claim = await catalog.claim_critic(world_id=world_id, owner="first")
    assert claim is not None
    operation_id = critic_provider_operation_id(world_id, raw_request.review_id)
    await catalog.bind_provider_operation(
        claim,
        provider=_DurableGitCritic.provider,
        operation_id=operation_id,
    )
    await physical.close()
    now[0] += 31

    recovered_physical, _, recovered_catalog = _open_catalog(
        catalog_path,
        now_seconds=clock,
    )
    provider = _DurableGitCritic(provider_path, confirmed_absent=True)
    stager = _Stager()
    worker = MissionCriticActivityWorker(
        world_id=world_id,
        owner="replacement",
        catalog=recovered_catalog,
        values=values,
        executor=provider,
        stager=stager,
    )

    assert await worker.run_once()
    assert provider.reconcile_calls == 1
    assert provider.execute_calls == 1
    assert provider._state()["retry_guard_ref"] == f"critic-retry://{operation_id}"
    assert (world_id, raw_request.review_id) in stager.staged
    await recovered_physical.close()
