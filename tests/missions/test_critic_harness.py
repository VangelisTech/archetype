# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Independent exact-head critic harness and capability contracts."""

from __future__ import annotations

import asyncio
import hashlib
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import archetype.missions.critics.harness as critic_harness_module
from archetype.missions import CriticPolicy
from archetype.missions.coding_agents.contracts import AgentProcessObservation
from archetype.missions.critics import (
    CandidateReviewRequest,
    CodexAppServerCriticDriver,
    CodexCriticDriver,
    CriticHarness,
    CriticHarnessConfig,
    CriticPrewarmRequest,
    CriticProcessObservation,
    CriticValidationEvidence,
)
from archetype.missions.sandboxes import (
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxIdentity,
    SandboxStatus,
)
from archetype.missions.transitions import CriticConclusion, CriticExecutionStatus


def _git(*arguments: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        ("git", *arguments),
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _repository(tmp_path: Path) -> tuple[Path, str, str, str]:
    seed = tmp_path / "seed"
    seed.mkdir()
    _git("init", "-b", "main", cwd=seed)
    _git("config", "user.name", "Fixture", cwd=seed)
    _git("config", "user.email", "fixture@example.com", cwd=seed)
    (seed / "artifact.txt").write_text("base\n", encoding="utf-8")
    _git("add", "artifact.txt", cwd=seed)
    _git("commit", "-m", "base", cwd=seed)
    base = _git("rev-parse", "HEAD", cwd=seed).strip()
    _git("switch", "-c", "agent/review", cwd=seed)
    (seed / "artifact.txt").write_text("candidate\n", encoding="utf-8")
    _git("commit", "-am", "candidate", cwd=seed)
    head = _git("rev-parse", "HEAD", cwd=seed).strip()
    diff = _git("diff", "--binary", base, head, cwd=seed)
    remote = tmp_path / "remote.git"
    _git("clone", "--bare", str(seed), str(remote))
    return remote, base, head, diff


class _LocalSession:
    def __init__(self) -> None:
        self.requests: list[ProcessRequest] = []

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity("local", "critic-sandbox", "critic-env")

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(secret_names=("codex_oauth", "github"))

    async def status(self) -> SandboxStatus:
        return SandboxStatus.READY

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        self.requests.append(request)
        environment = os.environ.copy()
        environment.update(request.environment_dict())
        process = await asyncio.create_subprocess_exec(
            *request.argv,
            cwd=request.workdir,
            env=environment,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        return ProcessResult(
            request.argv,
            int(process.returncode),
            stdout.decode(),
            stderr.decode(),
        )

    async def checkpoint(self) -> CheckpointRef:
        raise AssertionError("critic sandboxes must never checkpoint")

    async def close(self) -> None:
        return None


class _MalformedMktempSession(_LocalSession):
    def __init__(self) -> None:
        super().__init__()
        self.subject_directory: Path | None = None

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        result = await super().exec(request)
        if request.argv[:2] == ("mktemp", "-d"):
            self.subject_directory = Path(result.stdout.strip())
            return ProcessResult(
                result.argv,
                result.returncode,
                result.stdout + "unexpected-provider-output\n",
                result.stderr,
            )
        return result


class _ReportedCleanupFailureSession(_LocalSession):
    async def exec(self, request: ProcessRequest) -> ProcessResult:
        result = await super().exec(request)
        if request.argv[:3] == ("rm", "-rf", "--"):
            return ProcessResult(
                result.argv,
                9,
                result.stdout,
                "provider reported cleanup failure",
            )
        return result


class _Driver:
    def __init__(self, output: str) -> None:
        self.output = output
        self.calls = 0
        self.prompts: list[str] = []

    async def run(self, session, request, prompt: str) -> CriticProcessObservation:
        del session
        self.calls += 1
        self.prompts.append(prompt)
        assert request.diff == ""
        assert request.subject_transport == "sandbox_file"
        assert request.subject_ref in prompt
        assert request.subject_size_bytes > 0
        assert request.subject_digest
        assert "diff --git" not in prompt
        return CriticProcessObservation(0, stdout=self.output)


def _request(
    remote: Path,
    base: str,
    head: str,
    diff: str,
) -> CandidateReviewRequest:
    return CandidateReviewRequest(
        candidate_entity_id=10,
        candidate_id="candidate-1",
        mission_id=1,
        task_id=2,
        task_name="review",
        task_prompt="Preserve the repository contract.",
        dispatch_id="dispatch-1",
        dispatch_sequence=1,
        author_execution_id=3,
        author_sandbox_id="author-sandbox",
        repository=str(remote),
        branch="agent/review",
        base_ref="main",
        base_revision=base,
        head_revision=head,
        diff_digest=hashlib.sha256(diff.encode()).hexdigest(),
        validator_bundle_digest="validator-digest",
        policy=CriticPolicy(),
        validation=(
            CriticValidationEvidence(
                validator_id=4,
                name="focused",
                command=("pytest", "-q"),
                expected_returncode=0,
                actual_returncode=0,
                revision=head,
            ),
        ),
        candidate_published_at_ms=100,
        attempt=1,
    )


@pytest.mark.parametrize(
    "repository",
    (
        "git@github.com:owner/private.git",
        "ssh://github.com/owner/private.git",
        "https://publication-token@github.com/owner/private.git",
        "https://github.com/owner/repository.git?token=publication-token",
    ),
)
def test_critic_public_repository_rejects_credential_bearing_locations(
    repository: str,
) -> None:
    with pytest.raises(ValueError, match="public repository|credentials|secret-bearing"):
        CriticHarness._public_repository(repository)  # noqa: SLF001 - capability boundary


@pytest.mark.asyncio
async def test_exact_head_review_is_independent_secret_negative_and_immutable(
    tmp_path: Path,
) -> None:
    remote, base, head, diff = _repository(tmp_path)
    session = _LocalSession()
    driver = _Driver(
        '{"schema_version":1,"conclusion":"approved",'
        '"reviewed_scope":"exact task diff","findings":[]}'
    )
    harness = CriticHarness(
        driver,
        CriticHarnessConfig(workspace=str(tmp_path / "review")),
    )
    prewarm = CriticPrewarmRequest(
        mission_id=1,
        task_id=2,
        dispatch_id="dispatch-1",
        repository=str(remote),
        branch="agent/review",
        base_ref="main",
    )
    hydrated = await harness.prewarm(session, prewarm)
    request = _request(remote, base, head, diff)
    advance = tmp_path / "advance"
    _git("clone", str(remote), str(advance))
    _git("config", "user.name", "Fixture", cwd=advance)
    _git("config", "user.email", "fixture@example.com", cwd=advance)
    (advance / "later.txt").write_text("later candidate\n", encoding="utf-8")
    _git("add", "later.txt", cwd=advance)
    _git("commit", "-m", "later candidate", cwd=advance)
    _git("push", "origin", "agent/review", cwd=advance)
    later_head = _git("rev-parse", "HEAD", cwd=advance).strip()
    result = await harness.execute(
        session,
        request,
        provision_started_at_ms=1,
        sandbox_ready_at_ms=2,
        base_hydrated_at_ms=3,
    )

    assert hydrated == base
    assert result.status is CriticExecutionStatus.EXITED
    assert result.receipt is not None
    assert result.receipt.conclusion is CriticConclusion.APPROVED
    assert result.receipt.candidate_digest == request.candidate_digest
    assert result.receipt.policy_digest == request.policy.digest
    assert result.receipt.reviewed_base_revision == base
    assert result.receipt.reviewed_head_revision == head
    assert result.receipt.reviewed_diff_digest == request.diff_digest
    assert result.receipt.validator_bundle_digest == request.validator_bundle_digest
    assert result.receipt.subject_content_size_bytes == len(diff.encode())
    assert result.receipt.subject_size_bytes <= request.policy.max_subject_bytes
    assert result.receipt.subject_transport == "sandbox_file"
    assert not Path(result.receipt.subject_ref).exists()
    assert result.head_ready_at_ms <= result.critic_started_at_ms <= result.ended_at_ms
    assert "Policy perspective: repository-correctness" in driver.prompts[0]
    assert "Policy information view: task-diff-validators" in driver.prompts[0]
    assert "Policy driver: codex" in driver.prompts[0]
    assert "Policy sampling: provider-default" in driver.prompts[0]
    assert "Exact diff file:" in driver.prompts[0]
    assert diff not in driver.prompts[0]
    assert all(not item.secret_names for item in session.requests)
    assert any(item.argv[:3] == ("rm", "-rf", "--") for item in session.requests)
    diff_request = next(item for item in session.requests if item.argv[:2] == ("git", "diff"))
    assert diff_request.argv[2:5] == (
        "--no-ext-diff",
        "--no-textconv",
        "--binary",
    )
    assert later_head != head
    assert _git("--git-dir", str(remote), "merge-base", "--is-ancestor", head, later_head) == ""


@pytest.mark.asyncio
async def test_candidate_symlink_cannot_redirect_critic_subject_output(
    tmp_path: Path,
) -> None:
    remote, base, head, diff = _repository(tmp_path)
    session = _LocalSession()
    driver = _Driver(
        '{"schema_version":1,"conclusion":"approved",'
        '"reviewed_scope":"exact task diff","findings":[]}'
    )
    workspace = tmp_path / "review"
    harness = CriticHarness(
        driver,
        CriticHarnessConfig(workspace=str(workspace)),
    )
    await harness.prewarm(
        session,
        CriticPrewarmRequest(1, 2, "dispatch-1", str(remote), "agent/review", "main"),
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    redirected = outside / "critic-subject.diff"
    redirected.write_text("sentinel", encoding="utf-8")
    (workspace / ".archetype").symlink_to(outside, target_is_directory=True)

    result = await harness.execute(session, _request(remote, base, head, diff))

    assert result.status is CriticExecutionStatus.EXITED
    assert result.receipt is not None
    assert redirected.read_text(encoding="utf-8") == "sentinel"
    subject = Path(result.receipt.subject_ref)
    assert subject.parent.parent == Path("/tmp")
    assert not subject.is_relative_to(workspace)
    assert not subject.exists()
    assert not subject.parent.exists()


@pytest.mark.asyncio
async def test_wrong_remote_head_and_malformed_output_fail_closed(
    tmp_path: Path,
) -> None:
    remote, base, head, diff = _repository(tmp_path)
    session = _LocalSession()
    driver = _Driver("not structured")
    harness = CriticHarness(
        driver,
        CriticHarnessConfig(workspace=str(tmp_path / "review")),
    )
    await harness.prewarm(
        session,
        CriticPrewarmRequest(1, 2, "dispatch-1", str(remote), "agent/review", "main"),
    )
    request = _request(remote, base, head, diff)

    wrong_head = await harness.execute(
        session,
        replace(request, head_revision=base),
    )
    assert wrong_head.status is CriticExecutionStatus.UNVERIFIABLE
    assert wrong_head.receipt is None
    assert driver.calls == 0

    malformed = await harness.execute(session, request)
    assert malformed.status is CriticExecutionStatus.MALFORMED
    assert malformed.receipt is None
    assert driver.calls == 1
    assert any(item.argv[:3] == ("rm", "-rf", "--") for item in session.requests)


@pytest.mark.asyncio
async def test_over_budget_exact_subject_fails_closed_before_critic(
    tmp_path: Path,
) -> None:
    remote, base, head, diff = _repository(tmp_path)
    session = _LocalSession()
    driver = _Driver(
        '{"schema_version":1,"conclusion":"approved",'
        '"reviewed_scope":"exact task diff","findings":[]}'
    )
    harness = CriticHarness(
        driver,
        CriticHarnessConfig(workspace=str(tmp_path / "review")),
    )
    await harness.prewarm(
        session,
        CriticPrewarmRequest(1, 2, "dispatch-1", str(remote), "agent/review", "main"),
    )
    request = replace(
        _request(remote, base, head, diff),
        policy=CriticPolicy(max_subject_bytes=1),
    )

    result = await harness.execute(session, request)

    assert result.status is CriticExecutionStatus.UNVERIFIABLE
    assert result.receipt is None
    assert driver.calls == 0
    assert request.diff_digest in result.error
    assert "observed_bytes=" in result.error
    assert diff not in result.error
    assert any(item.argv[:3] == ("rm", "-rf", "--") for item in session.requests)


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_mask_unverifiable_subject_failure(
    tmp_path: Path,
) -> None:
    remote, base, head, diff = _repository(tmp_path)
    session = _ReportedCleanupFailureSession()
    driver = _Driver("{}")
    harness = CriticHarness(
        driver,
        CriticHarnessConfig(workspace=str(tmp_path / "review")),
    )
    await harness.prewarm(
        session,
        CriticPrewarmRequest(1, 2, "dispatch-1", str(remote), "agent/review", "main"),
    )
    request = replace(
        _request(remote, base, head, diff),
        policy=CriticPolicy(max_subject_bytes=1),
    )

    result = await harness.execute(session, request)

    assert result.status is CriticExecutionStatus.UNVERIFIABLE
    assert request.diff_digest in result.error
    assert "observed_bytes=" in result.error
    assert "cleanup failure" not in result.error
    assert driver.calls == 0


@pytest.mark.asyncio
async def test_subject_directory_is_cleaned_when_provider_output_is_malformed(
    tmp_path: Path,
) -> None:
    remote, base, head, diff = _repository(tmp_path)
    session = _MalformedMktempSession()
    driver = _Driver("{}")
    harness = CriticHarness(
        driver,
        CriticHarnessConfig(workspace=str(tmp_path / "review")),
    )
    await harness.prewarm(
        session,
        CriticPrewarmRequest(1, 2, "dispatch-1", str(remote), "agent/review", "main"),
    )

    result = await harness.execute(session, _request(remote, base, head, diff))

    assert result.status is CriticExecutionStatus.UNVERIFIABLE
    assert "directory allocation is invalid" in result.error
    assert driver.calls == 0
    assert session.subject_directory is not None
    assert not session.subject_directory.exists()
    assert any(item.argv[:3] == ("rm", "-rf", "--") for item in session.requests)


class _CaptureSession:
    def __init__(self) -> None:
        self.requests: list[ProcessRequest] = []

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity("capture", "critic", "env")

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(
            secret_names=("codex_oauth", "github"),
            home_directory="/home/critic",
        )

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        self.requests.append(request)
        return ProcessResult(request.argv, 0, stdout="{}")


@pytest.mark.asyncio
async def test_codex_critic_receives_model_capability_but_never_git_publication_secret(
    tmp_path: Path,
) -> None:
    remote, base, head, diff = _repository(tmp_path)
    session = _CaptureSession()
    driver = CodexCriticDriver(secret_name="critic_oauth", workspace="/workspace/review")
    await driver.run(session, _request(remote, base, head, diff), "review")

    assert len(session.requests) == 1
    process = session.requests[0]
    assert process.secret_names == ("critic_oauth",)
    assert "github" not in process.secret_names


@pytest.mark.asyncio
async def test_app_server_critic_uses_the_shared_exact_turn_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote, base, head, diff = _repository(tmp_path)
    session = _CaptureSession()
    calls: list[dict[str, Any]] = []

    async def run_turn(
        selected_session: object,
        **kwargs: Any,
    ) -> AgentProcessObservation:
        assert selected_session is session
        calls.append(kwargs)
        return AgentProcessObservation(
            returncode=0,
            stdout='{"conclusion":"accept","summary":"clean","findings":[]}',
            trace_uri="modal-sandbox://sb-critic/trace",
        )

    class _Connector:
        def connect(self, selected_session: object) -> Any:
            raise AssertionError(f"unexpected direct connector call for {selected_session!r}")

    monkeypatch.setattr(critic_harness_module, "run_codex_app_server_turn", run_turn)
    request = _request(remote, base, head, diff)
    driver = CodexAppServerCriticDriver(
        connector=_Connector(),
        workspace="/workspace/review",
    )

    observed = await driver.run(session, request, "review exact head")

    assert observed.returncode == 0
    assert observed.trace_uri == "modal-sandbox://sb-critic/trace"
    assert len(calls) == 1
    assert calls[0]["workspace"] == "/workspace/review"
    assert calls[0]["model"] == request.policy.model
    assert calls[0]["timeout_seconds"] == request.policy.timeout_seconds
