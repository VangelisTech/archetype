# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact-head repository materialization and structured critic normalization."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, replace
from pathlib import PurePosixPath
from typing import Any, ClassVar
from urllib.parse import urlsplit

from archetype.missions.critics.activities import (
    CriticSubjectPolicy,
    CriticSubjectTooLarge,
    CriticSubjectTransport,
    bind_critic_subject_observation,
)
from archetype.missions.critics.contracts import (
    CandidateReviewRequest,
    CriticDriver,
    CriticExecutionResult,
    CriticFindingValue,
    CriticPrewarmRequest,
    CriticProcessObservation,
    CriticReceiptValue,
    canonical_digest,
)
from archetype.missions.sandboxes import (
    ProcessRequest,
    ProcessResult,
    SandboxSession,
    SandboxStatus,
)
from archetype.missions.transitions import CriticConclusion, CriticExecutionStatus

_SUBJECT_MEASUREMENT_SCRIPT = """\
import hashlib
import json
import sys

digest = hashlib.sha256()
size = 0
with open(sys.argv[1], "rb") as source:
    for chunk in iter(lambda: source.read(1 << 20), b""):
        digest.update(chunk)
        size += len(chunk)
print(json.dumps({"digest": digest.hexdigest(), "size_bytes": size}, sort_keys=True))
"""


@dataclass(frozen=True)
class CriticHarnessConfig:
    """Repository and process settings for an independent critic sandbox."""

    workspace: str = "/workspace/review"
    git_timeout_seconds: int = 900

    def __post_init__(self) -> None:
        workspace = PurePosixPath(self.workspace)
        if not workspace.is_absolute() or str(workspace) in {"/", "."}:
            raise ValueError("critic workspace must be a non-root absolute path")
        if self.git_timeout_seconds < 1:
            raise ValueError("critic git timeout must be positive")


@dataclass(frozen=True)
class CodexCriticDriver:
    """Fresh Codex invocation with model credentials and no Git capability."""

    driver_id: ClassVar[str] = "codex"
    secret_name: str = "codex_oauth"
    workspace: str = "/workspace/review"

    async def run(
        self,
        session: SandboxSession,
        request: CandidateReviewRequest,
        prompt: str,
    ) -> CriticProcessObservation:
        codex_home = f"{session.capabilities.home_directory.rstrip('/')}/.codex"
        argv = [
            "codex",
            "exec",
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            "--ignore-user-config",
            "-c",
            'shell_environment_policy.inherit="core"',
            "-c",
            'shell_environment_policy.exclude=["*KEY*","*SECRET*","*TOKEN*"]',
            "-c",
            'cli_auth_credentials_store="file"',
        ]
        if request.policy.model:
            argv.extend(["--model", request.policy.model])
        argv.append(prompt)
        result = await session.exec(
            ProcessRequest(
                tuple(argv),
                workdir=self.workspace,
                timeout_seconds=request.policy.timeout_seconds,
                env=(("NO_COLOR", "1"), ("CODEX_HOME", codex_home)),
                secret_names=(self.secret_name,),
                close_stdin=True,
            )
        )
        return CriticProcessObservation(
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            trace_uri=result.trace_uri,
        )


class CriticHarness:
    """Hydrate a public base, verify an immutable head, and invoke a critic."""

    def __init__(
        self,
        driver: CriticDriver,
        config: CriticHarnessConfig | None = None,
    ) -> None:
        self._driver = driver
        self.config = config or CriticHarnessConfig()

    async def prewarm(
        self,
        session: SandboxSession,
        request: CriticPrewarmRequest,
    ) -> str:
        """Clone and verify the base without requesting any Git secret."""

        workspace = self.config.workspace
        exists = await self._run(session, "git", "-C", workspace, "rev-parse", "--git-dir")
        if exists.returncode != 0:
            parent = str(PurePosixPath(workspace).parent)
            self._raise(await self._run(session, "mkdir", "-p", parent), "mkdir")
            repository = self._public_repository(request.repository)
            clone = await self._run(
                session,
                "git",
                "clone",
                "--branch",
                request.base_ref,
                "--single-branch",
                "--",
                repository,
                workspace,
                timeout=self.config.git_timeout_seconds,
            )
            self._raise(clone, "critic git clone")
        return (
            await self._git(session, "rev-parse", f"refs/remotes/origin/{request.base_ref}")
        ).stdout.strip()

    async def execute(
        self,
        session: SandboxSession,
        request: CandidateReviewRequest,
        *,
        provision_started_at_ms: int = 0,
        sandbox_ready_at_ms: int = 0,
        base_hydrated_at_ms: int = 0,
    ) -> CriticExecutionResult:
        """Verify exact remote identity before allowing critic inference."""

        started_at_ms = self._now_ms()
        head_ready_at_ms = 0
        critic_started_at_ms = 0
        raw_output = ""
        trace_uri = ""
        try:
            fetch = await self._run(
                session,
                "git",
                "fetch",
                "origin",
                f"refs/heads/{request.branch}",
                workdir=self.config.workspace,
                timeout=self.config.git_timeout_seconds,
            )
            self._raise(fetch, "critic exact-head fetch")
            remote_head = (await self._git(session, "rev-parse", "FETCH_HEAD")).stdout.strip()
            head_exists = await self._run(
                session,
                "git",
                "cat-file",
                "-e",
                f"{request.head_revision}^{{commit}}",
                workdir=self.config.workspace,
                timeout=self.config.git_timeout_seconds,
            )
            if head_exists.returncode != 0:
                raise _UnverifiableReview(
                    f"candidate head {request.head_revision} is not present after remote fetch"
                )
            reachable = await self._run(
                session,
                "git",
                "merge-base",
                "--is-ancestor",
                request.head_revision,
                remote_head,
                workdir=self.config.workspace,
                timeout=self.config.git_timeout_seconds,
            )
            if reachable.returncode != 0:
                raise _UnverifiableReview(
                    f"candidate head {request.head_revision} is not reachable from "
                    f"remote branch head {remote_head}"
                )
            base_exists = await self._run(
                session,
                "git",
                "cat-file",
                "-e",
                f"{request.base_revision}^{{commit}}",
                workdir=self.config.workspace,
                timeout=self.config.git_timeout_seconds,
            )
            if base_exists.returncode != 0:
                raise _UnverifiableReview(
                    f"candidate base {request.base_revision} is not present after remote fetch"
                )
            await self._git(session, "checkout", "--detach", request.head_revision)
            checked_out = (await self._git(session, "rev-parse", "HEAD")).stdout.strip()
            if checked_out != request.head_revision:
                raise _UnverifiableReview(
                    f"checked-out head is {checked_out}, expected {request.head_revision}"
                )
            subject_directory_result = await self._run(
                session,
                "mktemp",
                "-d",
                "/tmp/archetype-critic-subject.XXXXXXXXXX",
                workdir="/tmp",
            )
            self._raise(subject_directory_result, "critic subject directory")
            subject_directories = subject_directory_result.stdout.splitlines()
            if len(subject_directories) != 1:
                raise _UnverifiableReview("critic subject directory allocation is invalid")
            subject_directory = PurePosixPath(subject_directories[0])
            if (
                not subject_directory.is_absolute()
                or subject_directory.parent != PurePosixPath("/tmp")
                or not subject_directory.name.startswith("archetype-critic-subject.")
                or ".." in subject_directory.parts
            ):
                raise _UnverifiableReview("critic subject directory escaped provider ownership")
            subject_path = str(subject_directory / "subject.diff")
            try:
                await self._git(
                    session,
                    "diff",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--binary",
                    f"--output={subject_path}",
                    request.base_revision,
                    request.head_revision,
                )
                measured = await self._run(
                    session,
                    "python",
                    "-c",
                    _SUBJECT_MEASUREMENT_SCRIPT,
                    subject_path,
                    workdir=self.config.workspace,
                )
                self._raise(measured, "critic subject measurement")
                try:
                    measurement = json.loads(measured.stdout)
                    observed_diff_digest = str(measurement["digest"])
                    observed_diff_size = int(measurement["size_bytes"])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise _UnverifiableReview("critic subject measurement is invalid") from exc
                if observed_diff_digest != request.diff_digest:
                    raise _UnverifiableReview(
                        "exact base/head diff does not match the candidate digest"
                    )
                prompt = self._prompt(request, subject_path=subject_path)
                subject = bind_critic_subject_observation(
                    CriticSubjectPolicy(
                        digest=request.diff_digest,
                        max_bytes=request.policy.max_subject_bytes,
                    ),
                    metadata=prompt.encode(),
                    content_digest=observed_diff_digest,
                    content_size_bytes=observed_diff_size,
                    transport=CriticSubjectTransport.SANDBOX_FILE,
                    ref=subject_path,
                )
                head_ready_at_ms = self._now_ms()
                bound_request = replace(
                    request,
                    diff="",
                    subject_ref=subject.ref,
                    subject_transport=subject.transport.value,
                    subject_size_bytes=subject.total_size_bytes,
                    subject_digest=subject.subject_digest,
                )
                critic_started_at_ms = self._now_ms()
                process = await self._driver.run(
                    session,
                    bound_request,
                    prompt,
                )
            finally:
                cleanup = await self._run(
                    session,
                    "rm",
                    "-rf",
                    "--",
                    str(subject_directory),
                    workdir="/tmp",
                )
                self._raise(cleanup, "critic subject cleanup")
            raw_output = process.stdout
            trace_uri = process.trace_uri
            if process.returncode != 0:
                status = (
                    CriticExecutionStatus.TIMED_OUT
                    if process.returncode == 124
                    else CriticExecutionStatus.ERRORED
                )
                detail = process.stderr or process.stdout
                return self._failure(
                    request,
                    session,
                    status=status,
                    started_at_ms=started_at_ms,
                    provision_started_at_ms=provision_started_at_ms,
                    sandbox_ready_at_ms=sandbox_ready_at_ms,
                    base_hydrated_at_ms=base_hydrated_at_ms,
                    head_ready_at_ms=head_ready_at_ms,
                    critic_started_at_ms=critic_started_at_ms,
                    raw_output=raw_output,
                    trace_uri=trace_uri,
                    error=f"critic exited with {process.returncode}: {detail}",
                )
            payload = self._structured_payload(raw_output)
            normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            if len(normalized) > request.policy.max_output_chars:
                raise ValueError("critic structured output exceeds the policy bound")
            if int(payload.get("schema_version", 0)) != request.policy.output_schema_version:
                raise ValueError("critic output schema version does not match policy")
            findings = self._findings(payload)
            conclusion = CriticConclusion(str(payload.get("conclusion", "")))
            blocking_count = sum(finding.severity == "blocking" for finding in findings)
            if (conclusion is CriticConclusion.APPROVED) == (blocking_count > 0):
                raise ValueError("critic conclusion conflicts with its blocking findings")
            ended_at_ms = self._now_ms()
            receipt = CriticReceiptValue(
                review_id=request.review_id,
                conclusion=conclusion,
                candidate_digest=request.candidate_digest,
                policy_digest=request.policy.digest,
                evidence_digest=canonical_digest(payload),
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
                reviewed_scope=str(payload.get("reviewed_scope", "exact task diff")),
                finding_count=len(findings),
                blocking_count=blocking_count,
                output_schema_version=request.policy.output_schema_version,
                completed_at_ms=ended_at_ms,
            )
            return CriticExecutionResult(
                request=request,
                status=CriticExecutionStatus.EXITED,
                sandbox=session.identity,
                sandbox_status=SandboxStatus.READY,
                sandbox_acquired=True,
                started_at_ms=started_at_ms,
                ended_at_ms=ended_at_ms,
                provision_started_at_ms=provision_started_at_ms,
                sandbox_ready_at_ms=sandbox_ready_at_ms,
                base_hydrated_at_ms=base_hydrated_at_ms,
                head_ready_at_ms=head_ready_at_ms,
                critic_started_at_ms=critic_started_at_ms,
                raw_output=raw_output,
                trace_uri=trace_uri,
                findings=findings,
                receipt=receipt,
            )
        except (CriticSubjectTooLarge, _UnverifiableReview) as exc:
            return self._failure(
                request,
                session,
                status=CriticExecutionStatus.UNVERIFIABLE,
                started_at_ms=started_at_ms,
                provision_started_at_ms=provision_started_at_ms,
                sandbox_ready_at_ms=sandbox_ready_at_ms,
                base_hydrated_at_ms=base_hydrated_at_ms,
                head_ready_at_ms=head_ready_at_ms,
                critic_started_at_ms=critic_started_at_ms,
                error=str(exc),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            return self._failure(
                request,
                session,
                status=CriticExecutionStatus.MALFORMED,
                started_at_ms=started_at_ms,
                provision_started_at_ms=provision_started_at_ms,
                sandbox_ready_at_ms=sandbox_ready_at_ms,
                base_hydrated_at_ms=base_hydrated_at_ms,
                head_ready_at_ms=head_ready_at_ms,
                critic_started_at_ms=critic_started_at_ms,
                raw_output=raw_output,
                trace_uri=trace_uri,
                error=f"{type(exc).__name__}: {exc}",
            )
        except Exception as exc:
            return self._failure(
                request,
                session,
                status=CriticExecutionStatus.ERRORED,
                started_at_ms=started_at_ms,
                provision_started_at_ms=provision_started_at_ms,
                sandbox_ready_at_ms=sandbox_ready_at_ms,
                base_hydrated_at_ms=base_hydrated_at_ms,
                head_ready_at_ms=head_ready_at_ms,
                critic_started_at_ms=critic_started_at_ms,
                raw_output=raw_output,
                trace_uri=trace_uri,
                error=f"{type(exc).__name__}: {exc}",
            )

    @staticmethod
    def _findings(payload: dict[str, Any]) -> tuple[CriticFindingValue, ...]:
        raw_findings = payload.get("findings")
        if not isinstance(raw_findings, list):
            raise ValueError("critic findings must be a list")
        findings: list[CriticFindingValue] = []
        for raw in raw_findings:
            if not isinstance(raw, dict):
                raise ValueError("each critic finding must be an object")
            findings.append(
                CriticFindingValue(
                    finding_id=str(raw["finding_id"]),
                    severity=str(raw["severity"]),
                    category=str(raw["category"]),
                    confidence=float(raw["confidence"]),
                    title=str(raw["title"]),
                    detail=str(raw["detail"]),
                    evidence_location=str(raw.get("evidence_location", "")),
                    reproduction=str(raw.get("reproduction", "")),
                )
            )
        return tuple(findings)

    @classmethod
    def _structured_payload(cls, output: str) -> dict[str, Any]:
        candidates = [output]
        for line in reversed(output.splitlines()):
            candidates.append(line)
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            candidates.extend(cls._string_values(event))
        for candidate in candidates:
            try:
                payload = json.loads(candidate)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict) and {"schema_version", "conclusion"} <= payload.keys():
                return payload
        raise ValueError("critic output did not contain one structured result")

    @classmethod
    def _string_values(cls, value: object) -> list[str]:
        if isinstance(value, str):
            return [value]
        if isinstance(value, dict):
            values: list[str] = []
            for nested in value.values():
                values.extend(cls._string_values(nested))
            return values
        if isinstance(value, list):
            values = []
            for nested in value:
                values.extend(cls._string_values(nested))
            return values
        return []

    @staticmethod
    def _prompt(
        request: CandidateReviewRequest,
        *,
        subject_path: str,
    ) -> str:
        evidence = [asdict(item) for item in request.validation]
        schema = {
            "schema_version": request.policy.output_schema_version,
            "conclusion": "approved | blocking",
            "reviewed_scope": "short scope statement",
            "findings": [
                {
                    "finding_id": "stable-id",
                    "severity": "blocking | advisory",
                    "category": "correctness",
                    "confidence": 0.95,
                    "title": "short title",
                    "detail": "bounded explanation",
                    "evidence_location": "path:line",
                    "reproduction": "optional probe",
                }
            ],
        }
        return (
            "Act as an independent repository critic. Do not edit, commit, push, or inspect "
            "author trajectory. Review only the immutable exact-head subject below. Run useful "
            "read-only or disposable probes in this clone. Return exactly one JSON object "
            f"matching this schema:\n{json.dumps(schema, indent=2)}\n\n"
            f"Policy perspective: {request.policy.perspective}\n"
            f"Policy information view: {request.policy.information_view}\n"
            f"Policy driver: {request.policy.driver}\n"
            f"Policy sampling: {request.policy.sampling}\n\n"
            f"Task: {request.task_name}\nSpecification:\n{request.task_prompt}\n\n"
            f"Repository: {request.repository}\nBranch ref: {request.branch}\n"
            f"Base SHA: {request.base_revision}\nHead SHA: {request.head_revision}\n"
            f"Diff SHA-256: {request.diff_digest}\n"
            f"Validator bundle SHA-256: {request.validator_bundle_digest}\n"
            f"Policy SHA-256: {request.policy.digest}\n"
            f"Validator evidence:\n{json.dumps(evidence, indent=2)}\n\n"
            f"Exact diff file: {subject_path}\n"
            "Read that file for the exact binary diff. Do not substitute the current "
            "branch head or another working-tree view."
        )

    @staticmethod
    def _public_repository(repository: str) -> str:
        if repository.startswith("git@"):
            raise ValueError("initial critic support requires a public repository URL")
        parsed = urlsplit(repository)
        if parsed.scheme:
            if parsed.scheme not in {"file", "http", "https"}:
                raise ValueError("initial critic support requires a public repository URL")
            if parsed.username is not None or parsed.password is not None:
                raise ValueError("critic repository URLs must not contain credentials")
            if parsed.query or parsed.fragment:
                raise ValueError("critic repository URLs must not contain secret-bearing suffixes")
        if not parsed.scheme and not repository.startswith("/"):
            return f"https://github.com/{repository.removesuffix('.git')}.git"
        return repository

    async def _git(self, session: SandboxSession, *arguments: str) -> ProcessResult:
        result = await self._run(
            session,
            "git",
            *arguments,
            workdir=self.config.workspace,
            timeout=self.config.git_timeout_seconds,
        )
        self._raise(result, f"git {' '.join(arguments[:2])}")
        return result

    @staticmethod
    async def _run(
        session: SandboxSession,
        *arguments: str,
        workdir: str | None = None,
        timeout: int = 900,
    ) -> ProcessResult:
        return await session.exec(
            ProcessRequest(
                tuple(arguments),
                workdir=workdir,
                timeout_seconds=timeout,
            )
        )

    @staticmethod
    def _raise(result: ProcessResult, label: str) -> None:
        if result.returncode != 0:
            detail = result.stderr or result.stdout
            raise RuntimeError(f"{label} failed with exit code {result.returncode}: {detail}")

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @classmethod
    def _failure(
        cls,
        request: CandidateReviewRequest,
        session: SandboxSession,
        *,
        status: CriticExecutionStatus,
        started_at_ms: int,
        provision_started_at_ms: int,
        sandbox_ready_at_ms: int,
        base_hydrated_at_ms: int,
        head_ready_at_ms: int,
        critic_started_at_ms: int,
        raw_output: str = "",
        trace_uri: str = "",
        error: str,
    ) -> CriticExecutionResult:
        return CriticExecutionResult(
            request=request,
            status=status,
            sandbox=session.identity,
            sandbox_status=SandboxStatus.READY,
            sandbox_acquired=True,
            started_at_ms=started_at_ms,
            ended_at_ms=cls._now_ms(),
            provision_started_at_ms=provision_started_at_ms,
            sandbox_ready_at_ms=sandbox_ready_at_ms,
            base_hydrated_at_ms=base_hydrated_at_ms,
            head_ready_at_ms=head_ready_at_ms,
            critic_started_at_ms=critic_started_at_ms,
            raw_output=raw_output,
            trace_uri=trace_uri,
            error=error,
        )


class _UnverifiableReview(RuntimeError):
    """Exact candidate materialization failed before critic inference."""


__all__ = ["CodexCriticDriver", "CriticHarness", "CriticHarnessConfig"]
