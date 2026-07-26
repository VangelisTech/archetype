# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Repository harness layered over a provider-neutral sandbox session."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath

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
from archetype.missions.sandboxes import ProcessRequest, ProcessResult, SandboxSession
from archetype.missions.transitions import AgentExecutionStatus

_TASK_BASE_REVISION_ENV = "ARCHETYPE_TASK_BASE_REVISION"


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

            raw_validation = await self._run_validators(
                session,
                request,
                task_base_revision=starting_revision,
            )
            validators_passed = all(
                result.returncode == validator.spec.expected_returncode
                for validator, result in raw_validation
            )
            final_revision = (await self._git(session, "rev-parse", "HEAD")).stdout.strip()
            if validators_passed:
                dirty = (await self._git(session, "status", "--porcelain")).stdout.strip()
                if dirty:
                    await self._git(session, "add", "-A")
                    message = f"{request.task_name}: {self._subject(request.prompt)}"
                    await self._git(session, "commit", "-m", message)
                    final_revision = (await self._git(session, "rev-parse", "HEAD")).stdout.strip()
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
            commits = await self._commits(
                session,
                starting_revision,
                request.branch,
                final_revision,
                pushed=False,
            )
            if starting_revision and final_revision:
                diff_digest = await self._diff_digest(
                    session,
                    starting_revision,
                    final_revision,
                )
            if validators_passed:
                if final_revision == starting_revision:
                    raise RuntimeError(
                        "validators passed but the task produced no repository change"
                    )
                await self._push(session, request.branch)
                commits = await self._commits(
                    session,
                    starting_revision,
                    request.branch,
                    final_revision,
                    pushed=True,
                )
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
        workspace = self.config.workspace
        exists = await self._run(session, "git", "-C", workspace, "rev-parse", "--git-dir")
        if exists.returncode != 0:
            parent = str(PurePosixPath(workspace).parent)
            await self._checked(session, "mkdir", "-p", parent)
            repository = request.repository
            if "://" not in repository and not repository.startswith(("git@", "/")):
                repository = f"https://github.com/{repository.removesuffix('.git')}.git"
            clone = await self._run(
                session,
                "git",
                *self._git_auth_args(),
                "clone",
                "--branch",
                request.base_ref,
                "--single-branch",
                "--",
                repository,
                workspace,
                timeout=self.config.agent_timeout_seconds,
                secrets=(self.config.github_secret_name,),
            )
            self._raise(clone, "git clone")
            await self._git(session, "switch", "-C", request.branch)
            await self._git(session, "config", "user.name", self.config.git_author_name)
            await self._git(session, "config", "user.email", self.config.git_author_email)
            return

        branch = (await self._git(session, "branch", "--show-current")).stdout.strip()
        if branch != request.branch:
            raise ValueError(
                f"sandbox repository is on branch {branch!r}, expected {request.branch!r}"
            )

    async def _run_validators(
        self,
        session: SandboxSession,
        request: TaskDispatchRequest,
        *,
        task_base_revision: str,
    ) -> tuple[tuple[DispatchedValidator, ProcessResult], ...]:
        results: list[tuple[DispatchedValidator, ProcessResult]] = []
        for validator in request.validators:
            result = await self._run(
                session,
                *validator.spec.command,
                workdir=self.config.workspace,
                timeout=validator.spec.timeout_seconds,
                env=((_TASK_BASE_REVISION_ENV, task_base_revision),),
            )
            results.append((validator, result))
        return tuple(results)

    async def _push(self, session: SandboxSession, branch: str) -> None:
        result = await self._run(
            session,
            "git",
            *self._git_auth_args(),
            "push",
            "--set-upstream",
            "origin",
            branch,
            workdir=self.config.workspace,
            timeout=self.config.agent_timeout_seconds,
            secrets=(self.config.github_secret_name,),
        )
        self._raise(result, "git push")

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
    def _git_auth_args() -> tuple[str, ...]:
        helper = '!f() { echo "username=x-access-token"; echo "password=$GITHUB_TOKEN"; }; f'
        return ("-c", f"credential.helper={helper}")

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
