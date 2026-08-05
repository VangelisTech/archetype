# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Repository harness layered over a provider-neutral sandbox session."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import asdict, dataclass, replace
from pathlib import PurePosixPath
from urllib.parse import urlsplit

from archetype.missions.coding_agents.contracts import (
    AgentExecutionResult,
    AgentProcessObservation,
    CodingAgentDriver,
    CommitObservation,
    DispatchedValidator,
    FrictionObservation,
    TaskDispatchRequest,
    ValidationObservation,
)
from archetype.missions.contracts import RepositoryPublicationPolicy
from archetype.missions.critics.contracts import validator_bundle_digest
from archetype.missions.diff_identity import (
    GIT_DIFF_IDENTITY_FLAGS,
    GIT_DIFF_MEASUREMENT_SCRIPT,
)
from archetype.missions.sandboxes import (
    ProcessRequest,
    ProcessResult,
    RepositoryPublicationRequest,
    RepositoryPublisher,
    SandboxSession,
)
from archetype.missions.transitions import AgentExecutionStatus

_TASK_BASE_REVISION_ENV = "ARCHETYPE_TASK_BASE_REVISION"
_CLEAN_GIT_ENV = (
    ("GIT_CONFIG_COUNT", "0"),
    ("GIT_CONFIG_GLOBAL", "/dev/null"),
    ("GIT_CONFIG_NOSYSTEM", "1"),
    ("GIT_CONFIG_SYSTEM", "/dev/null"),
    ("GIT_TERMINAL_PROMPT", "0"),
)
_CLEAN_GIT_ARGS = (
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "credential.helper=",
    "-c",
    "protocol.ext.allow=never",
)
_GITHUB_REPOSITORY_RE = re.compile(
    r"(?P<owner>[A-Za-z0-9][A-Za-z0-9_.-]*)/"
    r"(?P<repository>[A-Za-z0-9][A-Za-z0-9_.-]*?)(?:\.git)?"
)
_GIT_REVISION_RE = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
_CLEANUP_FRICTION_MAX_CHARS = 1024
_PUBLICATION_PREFIX = "archetype-mission-publication."
_PUBLICATION_REF = "refs/archetype/validated"
_VALIDATION_PREFIX = "archetype-mission-validation."
_VALIDATION_REF = "refs/archetype/candidate"


@dataclass(frozen=True)
class CodingAgentHarnessConfig:
    """Repository and agent settings shared by every task in one harness."""

    workspace: str = "/workspace/repo"
    agent_timeout_seconds: int = 45 * 60
    git_author_name: str = "Archetype Coding Agent"
    git_author_email: str = "coding-agent@archetype.local"
    github_secret_name: str = "github"

    def __post_init__(self) -> None:
        workspace = PurePosixPath(self.workspace)
        if not workspace.is_absolute() or str(workspace) in {"/", "."}:
            raise ValueError("workspace must be a non-root absolute path")
        if self.agent_timeout_seconds < 1:
            raise ValueError("agent_timeout_seconds must be positive")
        if not self.github_secret_name.strip():
            raise ValueError("commit-and-push requires a symbolic GitHub secret")


@dataclass(frozen=True)
class CodexDriver:
    """Codex CLI driver; authentication is a symbolic sandbox secret lease."""

    model: str = ""
    secret_name: str = "codex_oauth"
    timeout_seconds: int = 45 * 60
    workspace: str = "/workspace/repo"

    async def run(
        self,
        session: SandboxSession,
        request: TaskDispatchRequest,
        prompt: str,
    ) -> AgentProcessObservation:
        codex_home = f"{session.capabilities.home_directory.rstrip('/')}/.codex"
        common = [
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
        if self.model:
            common.extend(["--model", self.model])
        argv = (
            (
                "codex",
                "exec",
                "resume",
                *common,
                request.previous_agent_session_id,
                prompt,
            )
            if request.previous_agent_session_id
            else ("codex", "exec", *common, prompt)
        )
        result = await session.exec(
            ProcessRequest(
                tuple(argv),
                workdir=self.workspace,
                timeout_seconds=self.timeout_seconds,
                env=(("NO_COLOR", "1"), ("CODEX_HOME", codex_home)),
                secret_names=(self.secret_name,),
                close_stdin=True,
            )
        )
        return AgentProcessObservation(
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            session_id=self._session_id(result.stdout) or request.previous_agent_session_id,
            trace_uri=result.trace_uri,
        )

    @staticmethod
    def _session_id(output: str) -> str:
        for line in output.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") == "thread.started":
                return str(event.get("thread_id") or "")
        return ""


class CodingAgentHarness:
    """Prepare a repository, run an agent, validate, commit, and publish.

    The return value contains observations only. It deliberately has no
    accepted/rejected field; mission processors apply those facts to task
    state after they are persisted.
    """

    def __init__(
        self,
        driver: CodingAgentDriver,
        config: CodingAgentHarnessConfig | None = None,
    ) -> None:
        self._driver = driver
        self.config = config or CodingAgentHarnessConfig()

    async def execute(
        self,
        session: SandboxSession,
        request: TaskDispatchRequest,
    ) -> AgentExecutionResult:
        agent = AgentProcessObservation(returncode=-1)
        starting_revision = request.task_base_revision
        final_revision = ""
        validation: tuple[ValidationObservation, ...] = ()
        commits: tuple[CommitObservation, ...] = ()
        candidate_commits: tuple[CommitObservation, ...] = ()
        friction: list[FrictionObservation] = []
        bundle_digest = validator_bundle_digest(
            tuple(
                (
                    validator.validator_id,
                    validator.spec.name,
                    validator.spec.command,
                    validator.spec.expected_returncode,
                    validator.spec.timeout_seconds,
                )
                for validator in request.validators
            )
        )
        diff_digest = ""
        try:
            if request.publication_policy is not RepositoryPublicationPolicy.COMMIT_AND_PUSH:
                raise ValueError("unsupported repository publication policy")
            await self._prepare_repository(session, request)
            final_revision = (await self._git(session, "rev-parse", "HEAD")).stdout.strip()
            starting_revision = starting_revision or final_revision
            agent = await self._driver.run(session, request, self._prompt(request))
            if agent.returncode != 0:
                detail = agent.stderr or agent.stdout
                friction.append(
                    FrictionObservation(
                        kind="agent_process",
                        message=f"agent exited with {agent.returncode}: {detail}",
                    )
                )

            dirty = (await self._git(session, "status", "--porcelain")).stdout.strip()
            if dirty:
                staged = await self._run(
                    session,
                    "git",
                    *_CLEAN_GIT_ARGS,
                    "add",
                    "-A",
                    workdir=self.config.workspace,
                    env=_CLEAN_GIT_ENV,
                )
                self._raise(staged, "git add")
                message = f"{request.task_name}: {self._subject(request.prompt)}"
                committed = await self._run(
                    session,
                    "git",
                    *_CLEAN_GIT_ARGS,
                    "commit",
                    "-m",
                    message,
                    workdir=self.config.workspace,
                    env=_CLEAN_GIT_ENV,
                )
                self._raise(committed, "git commit")
            final_revision = (await self._git(session, "rev-parse", "HEAD")).stdout.strip()
            candidate_commits = await self._commits(
                session,
                starting_revision,
                request.branch,
                final_revision,
                pushed=False,
            )
            commits = candidate_commits
            if starting_revision and final_revision:
                diff_digest = await self._diff_digest(
                    session,
                    starting_revision,
                    final_revision,
                )

            raw_validation = await self._run_validators(
                session,
                request,
                task_base_revision=starting_revision,
                candidate_revision=final_revision,
                cleanup_friction=friction,
            )
            validators_passed = all(
                result.returncode == validator.spec.expected_returncode
                for validator, result in raw_validation
            )
            validation = tuple(
                ValidationObservation(
                    validator_id=validator.validator_id,
                    name=validator.spec.name,
                    command=validator.spec.command,
                    expected_returncode=validator.spec.expected_returncode,
                    actual_returncode=result.returncode,
                    revision=final_revision,
                    stdout=result.stdout,
                    stderr=result.stderr,
                )
                for validator, result in raw_validation
            )
            if validators_passed:
                if final_revision == starting_revision:
                    raise RuntimeError(
                        "validators passed but the task produced no repository change"
                    )
                publication_cleanup = await self._push(
                    session,
                    request,
                    final_revision,
                    cleanup_friction=friction,
                )
                if publication_cleanup is not None:
                    friction.append(publication_cleanup)
                commits = tuple(replace(item, pushed=True) for item in candidate_commits)
            else:
                for result in validation:
                    if not result.passed:
                        detail = result.stderr or result.stdout
                        friction.append(
                            FrictionObservation(
                                kind="validation",
                                message=(
                                    f"{result.name} exit={result.actual_returncode} "
                                    f"expected={result.expected_returncode}: {detail}"
                                ),
                            )
                        )
            return AgentExecutionResult(
                mission_id=request.mission_id,
                task_id=request.task_id,
                dispatch_id=request.dispatch_id,
                dispatch_sequence=request.dispatch_sequence,
                status=AgentExecutionStatus.EXITED,
                sandbox=session.identity,
                worktree=self.config.workspace,
                agent_session_id=agent.session_id,
                agent_returncode=agent.returncode,
                starting_revision=starting_revision,
                final_revision=final_revision,
                diff_digest=diff_digest,
                validator_bundle_digest=bundle_digest,
                agent_stdout=agent.stdout,
                agent_stderr=agent.stderr,
                trace_uri=agent.trace_uri,
                validation=validation,
                commits=commits,
                friction=tuple(friction),
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            friction.append(FrictionObservation(kind="execution", message=error))
            return AgentExecutionResult(
                mission_id=request.mission_id,
                task_id=request.task_id,
                dispatch_id=request.dispatch_id,
                dispatch_sequence=request.dispatch_sequence,
                status=AgentExecutionStatus.ERRORED,
                sandbox=session.identity,
                worktree=self.config.workspace,
                agent_session_id=agent.session_id,
                agent_returncode=agent.returncode,
                starting_revision=starting_revision,
                final_revision=final_revision,
                diff_digest=diff_digest,
                validator_bundle_digest=bundle_digest,
                agent_stdout=agent.stdout,
                agent_stderr=agent.stderr,
                trace_uri=agent.trace_uri,
                validation=validation,
                commits=commits,
                friction=tuple(friction),
                error=error,
            )

    async def _prepare_repository(
        self,
        session: SandboxSession,
        request: TaskDispatchRequest,
    ) -> None:
        if request.checkout_revision and not _GIT_REVISION_RE.fullmatch(request.checkout_revision):
            raise ValueError("checkout_revision must be a full Git object ID")
        workspace = self.config.workspace
        exists = await self._run(session, "git", "-C", workspace, "rev-parse", "--git-dir")
        if exists.returncode != 0:
            parent = str(PurePosixPath(workspace).parent)
            await self._checked(session, "mkdir", "-p", parent)
            repository, _authenticated_publication = self._publication_target(request.repository)
            clone_ref = request.branch if request.checkout_revision else request.base_ref
            clone = await self._run(
                session,
                "git",
                *_CLEAN_GIT_ARGS,
                "clone",
                "--branch",
                clone_ref,
                "--single-branch",
                "--",
                repository,
                workspace,
                timeout=self.config.agent_timeout_seconds,
                env=_CLEAN_GIT_ENV,
            )
            self._raise(clone, "git clone")
            await self._git(
                session,
                "switch",
                "-C",
                request.branch,
                request.checkout_revision or "HEAD",
            )
            await self._git(session, "config", "user.name", self.config.git_author_name)
            await self._git(session, "config", "user.email", self.config.git_author_email)
            await self._verify_checkout_revision(session, request.checkout_revision)
            return

        branch = (await self._git(session, "branch", "--show-current")).stdout.strip()
        if branch != request.branch:
            raise ValueError(
                f"sandbox repository is on branch {branch!r}, expected {request.branch!r}"
            )
        if request.checkout_revision:
            dirty = (await self._git(session, "status", "--porcelain")).stdout.strip()
            if dirty:
                raise RuntimeError("cannot hydrate an exact revision into a dirty repository")
            head = (await self._git(session, "rev-parse", "HEAD")).stdout.strip()
            if head != request.checkout_revision:
                raise RuntimeError(
                    "existing sandbox repository is not at the exact dispatch revision"
                )
            await self._verify_checkout_revision(session, request.checkout_revision)

    async def _run_validators(
        self,
        session: SandboxSession,
        request: TaskDispatchRequest,
        *,
        task_base_revision: str,
        candidate_revision: str,
        cleanup_friction: list[FrictionObservation],
    ) -> tuple[tuple[DispatchedValidator, ProcessResult], ...]:
        if not _GIT_REVISION_RE.fullmatch(candidate_revision):
            raise RuntimeError("candidate revision is not a full Git object ID")
        results: list[tuple[DispatchedValidator, ProcessResult]] = []
        for validator in request.validators:
            result = await self._run_validator(
                session,
                validator,
                task_base_revision=task_base_revision,
                candidate_revision=candidate_revision,
                cleanup_friction=cleanup_friction,
            )
            results.append((validator, result))
        return tuple(results)

    async def _run_validator(
        self,
        session: SandboxSession,
        validator: DispatchedValidator,
        *,
        task_base_revision: str,
        candidate_revision: str,
        cleanup_friction: list[FrictionObservation],
    ) -> ProcessResult:
        created = await self._checked(
            session,
            "mktemp",
            "-d",
            f"/tmp/{_VALIDATION_PREFIX}XXXXXXXXXX",
        )
        validation_directory = created.stdout.strip()
        if self._owned_temp_directory(validation_directory, _VALIDATION_PREFIX) is None:
            raise RuntimeError("mktemp returned an unsafe validation directory")
        checkout = f"{validation_directory}/checkout"
        operation_error: BaseException | None = None
        try:
            initialized = await self._run(
                session,
                "git",
                *_CLEAN_GIT_ARGS,
                "init",
                checkout,
                env=_CLEAN_GIT_ENV,
            )
            self._raise(initialized, "initialize clean validation repository")
            fetched = await self._run(
                session,
                "git",
                *_CLEAN_GIT_ARGS,
                "-C",
                checkout,
                "fetch",
                "--no-tags",
                "--no-recurse-submodules",
                "--",
                self.config.workspace,
                f"+{candidate_revision}:{_VALIDATION_REF}",
                timeout=self.config.agent_timeout_seconds,
                env=_CLEAN_GIT_ENV,
            )
            self._raise(fetched, "transfer candidate revision for validation")
            checked_out = await self._run(
                session,
                "git",
                *_CLEAN_GIT_ARGS,
                "-C",
                checkout,
                "checkout",
                "--detach",
                _VALIDATION_REF,
                env=_CLEAN_GIT_ENV,
            )
            self._raise(checked_out, "checkout exact candidate revision")
            resolved = await self._run(
                session,
                "git",
                *_CLEAN_GIT_ARGS,
                "-C",
                checkout,
                "rev-parse",
                "--verify",
                "HEAD",
                env=_CLEAN_GIT_ENV,
            )
            self._raise(resolved, "verify validation revision")
            if resolved.stdout.strip() != candidate_revision:
                raise RuntimeError("validation checkout resolved a different revision")
            return await self._run(
                session,
                *validator.spec.command,
                workdir=checkout,
                timeout=validator.spec.timeout_seconds,
                env=((_TASK_BASE_REVISION_ENV, task_base_revision),),
            )
        except BaseException as exc:
            operation_error = exc
            raise
        finally:
            observed_cleanup_friction = await self._cleanup_temp_directory(
                session,
                validation_directory,
                prefix=_VALIDATION_PREFIX,
                operation_error=operation_error,
                cleanup_friction=cleanup_friction,
            )
            if observed_cleanup_friction is not None:
                cleanup_friction.append(observed_cleanup_friction)

    async def _verify_checkout_revision(
        self,
        session: SandboxSession,
        checkout_revision: str,
    ) -> None:
        if not checkout_revision:
            return
        if not _GIT_REVISION_RE.fullmatch(checkout_revision):
            raise ValueError("checkout_revision must be a full Git object ID")
        resolved = (await self._git(session, "rev-parse", "--verify", "HEAD")).stdout.strip()
        if resolved != checkout_revision:
            raise RuntimeError("repository hydration resolved a different revision")

    async def _push(
        self,
        session: SandboxSession,
        request: TaskDispatchRequest,
        final_revision: str,
        *,
        cleanup_friction: list[FrictionObservation] | None = None,
    ) -> FrictionObservation | None:
        if not _GIT_REVISION_RE.fullmatch(final_revision):
            raise RuntimeError("validated revision is not a full Git object ID")
        target, authenticated = self._publication_target(request.repository)
        branch_ref = f"refs/heads/{request.branch}"
        branch_check = await self._run(
            session,
            "git",
            *_CLEAN_GIT_ARGS,
            "check-ref-format",
            branch_ref,
            env=_CLEAN_GIT_ENV,
        )
        self._raise(branch_check, "git publication branch validation")

        if authenticated:
            if not isinstance(session, RepositoryPublisher):
                raise RuntimeError("sandbox provider does not support isolated GitHub publication")
            published = await session.publish_repository(
                RepositoryPublicationRequest(
                    repository=target,
                    branch_ref=branch_ref,
                    revision=final_revision,
                    worktree=self.config.workspace,
                    timeout_seconds=self.config.agent_timeout_seconds,
                    secret_name=self.config.github_secret_name,
                )
            )
            self._raise(published, "git push")
            return None

        created = await self._checked(
            session,
            "mktemp",
            "-d",
            f"/tmp/{_PUBLICATION_PREFIX}XXXXXXXXXX",
        )
        publication_directory = created.stdout.strip()
        if self._owned_temp_directory(publication_directory, _PUBLICATION_PREFIX) is None:
            raise RuntimeError("mktemp returned an unsafe publication directory")
        publication_repository = f"{publication_directory}/repository.git"
        operation_error: BaseException | None = None
        try:
            initialized = await self._run(
                session,
                "git",
                *_CLEAN_GIT_ARGS,
                "init",
                "--bare",
                publication_repository,
                env=_CLEAN_GIT_ENV,
            )
            self._raise(initialized, "initialize clean publication repository")
            fetched = await self._run(
                session,
                "git",
                *_CLEAN_GIT_ARGS,
                f"--git-dir={publication_repository}",
                "fetch",
                "--no-tags",
                "--no-recurse-submodules",
                "--",
                self.config.workspace,
                f"+{final_revision}:{_PUBLICATION_REF}",
                timeout=self.config.agent_timeout_seconds,
                env=_CLEAN_GIT_ENV,
            )
            self._raise(fetched, "transfer validated revision for publication")
            resolved = await self._run(
                session,
                "git",
                *_CLEAN_GIT_ARGS,
                f"--git-dir={publication_repository}",
                "rev-parse",
                "--verify",
                _PUBLICATION_REF,
                env=_CLEAN_GIT_ENV,
            )
            self._raise(resolved, "verify publication revision")
            if resolved.stdout.strip() != final_revision:
                raise RuntimeError("clean publication repository resolved a different revision")

            pushed = await self._run(
                session,
                "git",
                *_CLEAN_GIT_ARGS,
                f"--git-dir={publication_repository}",
                "push",
                "--porcelain",
                "--",
                target,
                f"{_PUBLICATION_REF}:{branch_ref}",
                timeout=self.config.agent_timeout_seconds,
                env=_CLEAN_GIT_ENV,
            )
            self._raise(pushed, "git push")
        except BaseException as exc:
            operation_error = exc
            raise
        finally:
            cleanup_observation = await self._cleanup_temp_directory(
                session,
                publication_directory,
                prefix=_PUBLICATION_PREFIX,
                operation_error=operation_error,
                cleanup_friction=cleanup_friction,
            )
        return cleanup_observation

    async def _cleanup_temp_directory(
        self,
        session: SandboxSession,
        directory: str,
        *,
        prefix: str,
        operation_error: BaseException | None,
        cleanup_friction: list[FrictionObservation] | None = None,
    ) -> FrictionObservation | None:
        if self._owned_temp_directory(directory, prefix) is None:
            raise RuntimeError("refusing to remove an unowned temporary directory")
        cleanup_task = asyncio.create_task(
            self._run(
                session,
                "rm",
                "-rf",
                "--",
                directory,
            )
        )
        caller_cancellation = (
            operation_error if isinstance(operation_error, asyncio.CancelledError) else None
        )
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as interrupted:
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    caller_cancellation = caller_cancellation or interrupted
            except BaseException:
                break
        try:
            cleanup = cleanup_task.result()
        except BaseException as cleanup_error:
            if caller_cancellation is not None:
                raise caller_cancellation from cleanup_error
            observed_cleanup_friction = self._cleanup_friction(prefix, cleanup_error)
            if operation_error is not None:
                if cleanup_friction is not None:
                    cleanup_friction.append(observed_cleanup_friction)
                raise operation_error from cleanup_error
            return observed_cleanup_friction
        if cleanup.returncode != 0:
            cleanup_error = RuntimeError(
                self._cleanup_failure_message(
                    prefix,
                    f"exit code {cleanup.returncode}",
                    cleanup.stderr or cleanup.stdout,
                )
            )
            if caller_cancellation is not None:
                raise caller_cancellation from cleanup_error
            observed_cleanup_friction = FrictionObservation(
                kind="cleanup",
                message=str(cleanup_error),
            )
            if operation_error is not None:
                if cleanup_friction is not None:
                    cleanup_friction.append(observed_cleanup_friction)
                raise operation_error from cleanup_error
            return observed_cleanup_friction
        if caller_cancellation is not None and caller_cancellation is not operation_error:
            if operation_error is not None:
                raise caller_cancellation from operation_error
            raise caller_cancellation
        return None

    @classmethod
    def _cleanup_friction(
        cls,
        prefix: str,
        cleanup_error: BaseException,
    ) -> FrictionObservation:
        message = cls._cleanup_failure_message(
            prefix,
            f"raised {type(cleanup_error).__name__}",
            str(cleanup_error),
        )
        return FrictionObservation(kind="cleanup", message=message)

    @staticmethod
    def _cleanup_failure_message(prefix: str, outcome: str, detail: str) -> str:
        phase = {
            _VALIDATION_PREFIX: "validation checkout",
            _PUBLICATION_PREFIX: "publication repository",
        }.get(prefix, "owned temporary directory")
        message = f"{phase} cleanup failed with {outcome}"
        stripped = detail.strip()
        if stripped:
            available = _CLEANUP_FRICTION_MAX_CHARS - len(message) - 2
            message = f"{message}: {stripped[-max(available, 0) :]}"
        return message[:_CLEANUP_FRICTION_MAX_CHARS]

    async def _commits(
        self,
        session: SandboxSession,
        starting_revision: str,
        branch: str,
        final_revision: str,
        *,
        pushed: bool,
    ) -> tuple[CommitObservation, ...]:
        if not starting_revision or not final_revision or starting_revision == final_revision:
            return ()
        result = await self._git(
            session,
            "log",
            "--reverse",
            "--format=%H%x00%s",
            f"{starting_revision}..{final_revision}",
        )
        observations: list[CommitObservation] = []
        for line in result.stdout.splitlines():
            sha, separator, message = line.partition("\0")
            if separator:
                observations.append(
                    CommitObservation(
                        sha=sha,
                        message=message,
                        branch=branch,
                        pushed=pushed,
                        final_revision=sha == final_revision,
                    )
                )
        return tuple(observations)

    async def _diff_digest(
        self,
        session: SandboxSession,
        starting_revision: str,
        final_revision: str,
    ) -> str:
        measured = await self._run(
            session,
            "python",
            "-c",
            GIT_DIFF_MEASUREMENT_SCRIPT,
            starting_revision,
            final_revision,
            *GIT_DIFF_IDENTITY_FLAGS,
            workdir=self.config.workspace,
        )
        self._raise(measured, "git diff measurement")
        try:
            measurement = json.loads(measured.stdout)
            digest = str(measurement["digest"])
            size_bytes = int(measurement["size_bytes"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("git diff measurement is invalid") from exc
        if len(digest) != 64 or size_bytes < 0:
            raise RuntimeError("git diff measurement is invalid")
        return digest

    async def _git(
        self,
        session: SandboxSession,
        *arguments: str,
    ) -> ProcessResult:
        result = await self._run(
            session,
            "git",
            *arguments,
            workdir=self.config.workspace,
        )
        self._raise(result, f"git {' '.join(arguments[:2])}")
        return result

    async def _checked(
        self,
        session: SandboxSession,
        *arguments: str,
    ) -> ProcessResult:
        result = await self._run(session, *arguments)
        self._raise(result, arguments[0])
        return result

    @staticmethod
    async def _run(
        session: SandboxSession,
        *arguments: str,
        workdir: str | None = None,
        timeout: int = 900,
        env: tuple[tuple[str, str], ...] = (),
        secrets: tuple[str, ...] = (),
    ) -> ProcessResult:
        return await session.exec(
            ProcessRequest(
                tuple(arguments),
                workdir=workdir,
                timeout_seconds=timeout,
                env=env,
                secret_names=secrets,
            )
        )

    @staticmethod
    def _publication_target(repository: str) -> tuple[str, bool]:
        if "\x00" in repository:
            raise ValueError("repository contains an invalid null byte")
        candidate = PurePosixPath(repository)
        if candidate.is_absolute() and ".." not in candidate.parts:
            return str(candidate), False

        matched = _GITHUB_REPOSITORY_RE.fullmatch(repository)
        if matched is not None:
            owner = matched.group("owner")
            name = matched.group("repository")
            if owner in {".", ".."} or name in {".", ".."}:
                raise ValueError("invalid GitHub repository identity")
            return f"https://github.com/{owner}/{name}.git", True

        parsed = urlsplit(repository)
        if (
            parsed.scheme != "https"
            or parsed.hostname is None
            or parsed.hostname.lower() != "github.com"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "publication requires an owner/repository slug, canonical GitHub HTTPS URL, "
                "or absolute local test path"
            )
        path = parsed.path.removeprefix("/")
        if parsed.path != f"/{path}":
            raise ValueError("invalid GitHub repository path")
        matched = _GITHUB_REPOSITORY_RE.fullmatch(path)
        if matched is None:
            raise ValueError("invalid GitHub repository path")
        owner = matched.group("owner")
        name = matched.group("repository")
        if owner in {".", ".."} or name in {".", ".."}:
            raise ValueError("invalid GitHub repository identity")
        return f"https://github.com/{owner}/{name}.git", True

    @staticmethod
    def _owned_temp_directory(value: str, prefix: str) -> PurePosixPath | None:
        candidate = PurePosixPath(value)
        if (
            candidate.is_absolute()
            and candidate.parent == PurePosixPath("/tmp")
            and candidate.name.startswith(prefix)
            and ".." not in candidate.parts
        ):
            return candidate
        return None

    @staticmethod
    def _prompt(request: TaskDispatchRequest) -> str:
        if request.previous_validation or request.previous_critic_findings:
            evidence = json.dumps(
                {
                    "validator_results": [
                        {
                            **asdict(result),
                            "passed": result.passed,
                        }
                        for result in request.previous_validation
                    ],
                    "critic_findings": [
                        asdict(finding) for finding in request.previous_critic_findings
                    ],
                },
                indent=2,
            )
            return (
                "The repository gate withheld the previous candidate.\n\n"
                f"Original task:\n{request.prompt}\n\n"
                f"Durable validator and critic evidence:\n{evidence}\n\n"
                "Repair the existing worktree and finish when the validators and independent "
                "review concerns are addressed. You may create useful commits. Do not push or "
                "open a pull request."
            )
        return (
            f"Complete task {request.task_name!r}:\n\n{request.prompt}\n\n"
            "Work directly in the current repository. Read and follow its AGENTS.md. "
            "Make the smallest complete change and run useful checks. You may create useful "
            "commits. Do not push or open a pull request; the software factory publishes the "
            "validated final revision."
        )

    @staticmethod
    def _subject(value: str) -> str:
        return " ".join(value.strip().split())[:72] or "complete task"

    @classmethod
    def _raise(cls, result: ProcessResult, label: str) -> None:
        if result.returncode != 0:
            detail = result.stderr or result.stdout
            raise RuntimeError(f"{label} failed with exit code {result.returncode}: {detail}")


__all__ = ["CodexDriver", "CodingAgentHarness", "CodingAgentHarnessConfig"]
