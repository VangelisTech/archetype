# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Minimal Modal resource for executing V1 coding-agent task attempts."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import PurePosixPath
from typing import Any

from archetype.missions.contracts import (
    ExecutionOutcome,
    Friction,
    RepositoryPublicationPolicy,
    TaskExecutionReceipt,
    TaskExecutionRequest,
    ValidatorResult,
)

_AUTH_MOUNT = "/auth"
_AUTH_VOLUME_PATH = f"{_AUTH_MOUNT}/auth.json"
_CODEX_HOME = "/root/.codex"
_MISSION_AUTH_PATH = f"{_CODEX_HOME}/auth.json"


@dataclass(frozen=True)
class ModalAgentSandboxConfig:
    """Process-level Modal and Codex configuration; repository data comes per mission."""

    app_name: str = "archetype-agent-missions"
    image_name: str = ""
    auth_volume_name: str = "archetype-codex-auth"
    github_secret_name: str = "archetype-github"
    model: str = ""
    workspace: str = "/workspace/repo"
    sandbox_timeout_seconds: int = 4 * 60 * 60
    idle_timeout_seconds: int = 20 * 60
    agent_timeout_seconds: int = 45 * 60
    git_author_name: str = "Archetype Coding Agent"
    git_author_email: str = "coding-agent@archetype.local"

    def __post_init__(self) -> None:
        workspace = PurePosixPath(self.workspace)
        if not workspace.is_absolute() or str(workspace) in {"/", "."}:
            raise ValueError("workspace must be a non-root absolute path")
        if self.sandbox_timeout_seconds < 1 or self.agent_timeout_seconds < 1:
            raise ValueError("Modal sandbox and agent timeouts must be positive")
        if not self.github_secret_name:
            raise ValueError("commit-and-push policy requires a GitHub secret")


@dataclass(frozen=True)
class _CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


@dataclass
class _ModalMissionSession:
    config: ModalAgentSandboxConfig
    mission_id: int
    repository: str
    branch: str
    base_ref: str
    sandbox: Any
    auth_sandbox: Any
    github_secret: Any | None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _closed: bool = False

    @property
    def sandbox_id(self) -> str:
        return str(self.sandbox.object_id)

    @classmethod
    async def create(
        cls,
        config: ModalAgentSandboxConfig,
        request: TaskExecutionRequest,
    ) -> _ModalMissionSession:
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc

        app = await modal.App.lookup.aio(config.app_name, create_if_missing=True)
        image = (
            modal.Image.from_name(config.image_name)
            if config.image_name
            else cls._default_image(modal)
        )
        auth_volume = modal.Volume.from_name(
            config.auth_volume_name,
            create_if_missing=False,
            version=2,
        )
        await auth_volume.hydrate.aio()
        github_secret = (
            modal.Secret.from_name(
                config.github_secret_name,
                required_keys=["GITHUB_TOKEN"],
            )
            if config.github_secret_name
            else None
        )
        auth_sandbox = await modal.Sandbox.create.aio(
            app=app,
            image=image,
            timeout=config.sandbox_timeout_seconds,
            idle_timeout=config.idle_timeout_seconds,
            workdir=_AUTH_MOUNT,
            volumes={_AUTH_MOUNT: auth_volume},
            tags={"kind": "archetype-agent-auth", "mission": str(request.mission_id)},
        )
        try:
            sandbox = await modal.Sandbox.create.aio(
                app=app,
                image=image,
                timeout=config.sandbox_timeout_seconds,
                idle_timeout=config.idle_timeout_seconds,
                workdir=str(PurePosixPath(config.workspace).parent),
                tags={
                    "kind": "archetype-agent-mission",
                    "mission": str(request.mission_id),
                    "branch": request.branch,
                },
            )
        except BaseException:
            await cls._terminate(auth_sandbox)
            raise

        session = cls(
            config=config,
            mission_id=request.mission_id,
            repository=request.repository,
            branch=request.branch,
            base_ref=request.base_ref,
            sandbox=sandbox,
            auth_sandbox=auth_sandbox,
            github_secret=github_secret,
        )
        try:
            await session._check_oauth()
            await session._prepare_repository()
        except BaseException:
            await session.close()
            raise
        return session

    @staticmethod
    def _default_image(modal: Any) -> Any:
        return (
            modal.Image.debian_slim(python_version="3.12")
            .apt_install("ca-certificates", "curl", "git", "openssh-client")
            .run_commands(
                "curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh",
                "curl -fsSL https://chatgpt.com/codex/install.sh "
                "| env CODEX_NON_INTERACTIVE=1 CODEX_INSTALL_DIR=/usr/local/bin sh",
            )
        )

    async def run(self, request: TaskExecutionRequest) -> TaskExecutionReceipt:
        if (
            request.mission_id != self.mission_id
            or request.repository != self.repository
            or request.branch != self.branch
            or request.base_ref != self.base_ref
        ):
            raise ValueError("task request changed its mission repository identity")
        if request.publication_policy is not RepositoryPublicationPolicy.COMMIT_AND_PUSH:
            raise ValueError("unsupported repository publication policy")
        async with self._lock:
            if self._closed:
                raise RuntimeError("Modal mission sandbox is closed")
            baseline = (await self._git("rev-parse", "HEAD")).stdout.strip()
            prompt = self._prompt(request)
            agent = await self._run_codex(prompt, request.previous_session_id)
            session_id = self._session_id(agent.stdout) or request.previous_session_id
            validator_results = await self._run_validators(request)
            accepted = all(result.passed for result in validator_results)
            commit_sha = ""
            commit_message = ""
            pushed = False
            error = ""
            friction: list[Friction] = []

            if accepted:
                status = (await self._git("status", "--porcelain")).stdout
                if not status.strip():
                    accepted = False
                    error = "validators passed but the task produced no repository change"
                else:
                    commit_message = f"{request.task_name}: {self._subject(request.prompt)}"
                    await self._git("add", "-A")
                    await self._git("commit", "-m", commit_message)
                    commit_sha = (await self._git("rev-parse", "HEAD")).stdout.strip()
                    if commit_sha == baseline:
                        accepted = False
                        error = "validators passed but the task produced no new commit"
                        commit_sha = ""
                    else:
                        await self._push()
                        pushed = True
            if not accepted:
                if not error:
                    failed = [result for result in validator_results if not result.passed]
                    error = (
                        "; ".join(
                            f"{result.name} exit={result.returncode}: "
                            f"{self._tail(result.stderr or result.stdout, 600)}"
                            for result in failed
                        )
                        or f"codex exited with {agent.returncode}"
                    )
                friction.append(Friction(kind="validation", message=error))

            return TaskExecutionReceipt(
                mission_id=request.mission_id,
                task_id=request.task_id,
                attempt_id=request.attempt_id,
                attempt_index=request.attempt_index,
                outcome=(ExecutionOutcome.ACCEPTED if accepted else ExecutionOutcome.REJECTED),
                validator_results=validator_results,
                sandbox_id=self.sandbox_id,
                worktree=self.config.workspace,
                agent_session_id=session_id,
                commit_sha=commit_sha,
                commit_message=commit_message,
                pushed=pushed,
                error=error,
                friction=tuple(friction),
            )

    async def _prepare_repository(self) -> None:
        parent = str(PurePosixPath(self.config.workspace).parent)
        await self._checked("mkdir", "-p", parent)
        repository = self.repository
        if "://" not in repository and not repository.startswith("git@"):
            repository = f"https://github.com/{repository.removesuffix('.git')}.git"
        clone = await self._exec(
            "git",
            *self._git_auth_args(),
            "clone",
            "--branch",
            self.base_ref,
            "--single-branch",
            "--",
            repository,
            self.config.workspace,
            timeout=self.config.agent_timeout_seconds,
            secrets=self._git_secrets(),
        )
        self._raise(clone, "git clone")
        await self._git("switch", "-C", self.branch)
        await self._git("config", "user.name", self.config.git_author_name)
        await self._git("config", "user.email", self.config.git_author_email)

    async def _run_codex(self, prompt: str, previous_session_id: str) -> _CommandResult:
        await self._stage_oauth()
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
        if self.config.model:
            common.extend(["--model", self.config.model])
        argv = (
            ["codex", "exec", "resume", *common, previous_session_id, prompt]
            if previous_session_id
            else ["codex", "exec", *common, prompt]
        )
        try:
            return await self._exec(
                *argv,
                workdir=self.config.workspace,
                timeout=self.config.agent_timeout_seconds,
                env={"NO_COLOR": "1", "CODEX_HOME": _CODEX_HOME},
                close_stdin=True,
            )
        finally:
            await self._persist_and_remove_oauth()

    async def _run_validators(self, request: TaskExecutionRequest) -> tuple[ValidatorResult, ...]:
        results: list[ValidatorResult] = []
        for validator in request.validators:
            result = await self._exec(
                *validator.command,
                workdir=self.config.workspace,
                timeout=validator.timeout_seconds,
            )
            results.append(
                ValidatorResult(
                    name=validator.name,
                    command=validator.command,
                    returncode=result.returncode,
                    passed=result.returncode == validator.expected_returncode,
                    stdout=self._tail(result.stdout),
                    stderr=self._tail(result.stderr),
                )
            )
        return tuple(results)

    async def _check_oauth(self) -> None:
        await self._stage_oauth()
        try:
            status = await self._exec(
                "codex",
                "login",
                "status",
                timeout=60,
                env={"CODEX_HOME": _CODEX_HOME, "NO_COLOR": "1"},
            )
        finally:
            await self._persist_and_remove_oauth()
        self._raise(status, "codex OAuth login")

    async def _stage_oauth(self) -> None:
        try:
            payload = await self.auth_sandbox.filesystem.read_text.aio(_AUTH_VOLUME_PATH)
        except Exception as exc:
            raise RuntimeError(
                f"Modal OAuth volume {self.config.auth_volume_name!r} has no Codex credential"
            ) from exc
        self._validate_oauth(payload)
        await self._checked("mkdir", "-p", _CODEX_HOME)
        await self.sandbox.filesystem.write_text.aio(payload, _MISSION_AUTH_PATH)
        await self._checked("chmod", "600", _MISSION_AUTH_PATH)

    async def _persist_and_remove_oauth(self) -> None:
        persistence_error: BaseException | None = None
        try:
            payload = await self.sandbox.filesystem.read_text.aio(_MISSION_AUTH_PATH)
            self._validate_oauth(payload)
            temporary = f"{_AUTH_VOLUME_PATH}.next"
            await self.auth_sandbox.filesystem.write_text.aio(payload, temporary)
            await self._auth_checked("chmod", "600", temporary)
            await self._auth_checked("mv", "-f", temporary, _AUTH_VOLUME_PATH)
            await self._auth_checked("sync", _AUTH_MOUNT)
        except BaseException as exc:
            persistence_error = exc
        finally:
            await self._checked("rm", "-f", _MISSION_AUTH_PATH)
        if persistence_error is not None:
            raise persistence_error

    async def _push(self) -> None:
        result = await self._exec(
            "git",
            *self._git_auth_args(),
            "push",
            "--set-upstream",
            "origin",
            self.branch,
            workdir=self.config.workspace,
            timeout=self.config.agent_timeout_seconds,
            secrets=self._git_secrets(),
        )
        self._raise(result, "git push")

    async def _git(self, *arguments: str) -> _CommandResult:
        result = await self._exec("git", *arguments, workdir=self.config.workspace)
        self._raise(result, f"git {' '.join(arguments[:2])}")
        return result

    async def _checked(self, *arguments: str, **kwargs: Any) -> _CommandResult:
        result = await self._exec(*arguments, **kwargs)
        self._raise(result, arguments[0])
        return result

    async def _auth_checked(self, *arguments: str) -> _CommandResult:
        result = await self._exec_on(
            self.auth_sandbox,
            *arguments,
            timeout=60,
        )
        self._raise(result, arguments[0])
        return result

    async def _exec(
        self,
        *arguments: str,
        workdir: str | None = None,
        timeout: int | None = None,
        secrets: tuple[Any, ...] | list[Any] = (),
        env: dict[str, str] | None = None,
        close_stdin: bool = False,
    ) -> _CommandResult:
        return await self._exec_on(
            self.sandbox,
            *arguments,
            workdir=workdir,
            timeout=timeout or self.config.agent_timeout_seconds,
            secrets=secrets,
            env=env,
            close_stdin=close_stdin,
        )

    @staticmethod
    async def _exec_on(
        sandbox: Any,
        *arguments: str,
        workdir: str | None = None,
        timeout: int,
        secrets: tuple[Any, ...] | list[Any] = (),
        env: dict[str, str] | None = None,
        close_stdin: bool = False,
    ) -> _CommandResult:
        process = await sandbox.exec.aio(
            *arguments,
            workdir=workdir,
            timeout=timeout,
            secrets=list(secrets),
            env=env,
        )
        if close_stdin:
            # Modal exposes stdin as an open pipe. Codex accepts optional prompt
            # input from that pipe and otherwise waits forever even when the
            # complete prompt was supplied as an argument.
            process.stdin.write_eof()
            await process.stdin.drain.aio()
        stdout_task = asyncio.create_task(process.stdout.read.aio())
        stderr_task = asyncio.create_task(process.stderr.read.aio())
        returncode, stdout, stderr = await asyncio.gather(
            process.wait.aio(), stdout_task, stderr_task
        )
        return _CommandResult(
            argv=tuple(arguments),
            returncode=int(returncode),
            stdout=str(stdout),
            stderr=str(stderr),
        )

    def _git_auth_args(self) -> tuple[str, ...]:
        if self.github_secret is None:
            return ()
        helper = '!f() { echo "username=x-access-token"; echo "password=$GITHUB_TOKEN"; }; f'
        return ("-c", f"credential.helper={helper}")

    def _git_secrets(self) -> list[Any]:
        return [self.github_secret] if self.github_secret is not None else []

    @staticmethod
    def _prompt(request: TaskExecutionRequest) -> str:
        if request.previous_validator_results:
            evidence = json.dumps(
                [asdict(result) for result in request.previous_validator_results],
                indent=2,
            )
            return (
                "The authoritative task gate failed after your previous turn.\n\n"
                f"Original task:\n{request.prompt}\n\nValidator evidence:\n{evidence}\n\n"
                "Repair the existing worktree and finish when it is ready for the same "
                "validators. Do not commit, push, or open a pull request."
            )
        return (
            f"Complete task {request.task_name!r}:\n\n{request.prompt}\n\n"
            "Work directly in the current repository. Read and follow its AGENTS.md. "
            "Make the smallest complete change and run useful checks. Do not commit, push, "
            "or open a pull request; the software factory owns validation and publication."
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

    @staticmethod
    def _subject(value: str) -> str:
        return " ".join(value.strip().split())[:72] or "complete task"

    @staticmethod
    def _tail(value: str, limit: int = 4000) -> str:
        return value[-limit:]

    @staticmethod
    def _validate_oauth(payload: str) -> None:
        try:
            value = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("Codex OAuth credential is not valid JSON") from exc
        if not isinstance(value, dict) or not value:
            raise RuntimeError("Codex OAuth credential must be a non-empty JSON object")

    @staticmethod
    def _raise(result: _CommandResult, label: str) -> None:
        if result.returncode != 0:
            detail = _ModalMissionSession._tail(result.stderr or result.stdout)
            raise RuntimeError(f"{label} failed with exit code {result.returncode}: {detail}")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        failures: list[BaseException] = []
        for sandbox in (self.sandbox, self.auth_sandbox):
            try:
                await self._terminate(sandbox)
            except BaseException as exc:
                failures.append(exc)
        if failures:
            raise failures[0]

    @staticmethod
    async def _terminate(sandbox: Any) -> None:
        try:
            await sandbox.terminate.aio(wait=True)
        finally:
            await sandbox.detach.aio()


class ModalAgentMissionSandbox:
    """Sandbox resource: persistent repository per mission, parallel across missions."""

    def __init__(self, config: ModalAgentSandboxConfig | None = None) -> None:
        self.config = config or ModalAgentSandboxConfig()
        self._sessions: dict[int, _ModalMissionSession] = {}
        self._session_lock = asyncio.Lock()

    async def run_many(
        self,
        requests: tuple[TaskExecutionRequest, ...],
    ) -> tuple[TaskExecutionReceipt, ...]:
        grouped: dict[int, list[TaskExecutionRequest]] = defaultdict(list)
        for request in requests:
            grouped[request.mission_id].append(request)

        async def run_mission(
            mission_requests: list[TaskExecutionRequest],
        ) -> list[TaskExecutionReceipt]:
            session = await self._session(mission_requests[0])
            receipts: list[TaskExecutionReceipt] = []
            for request in mission_requests:
                receipts.append(await session.run(request))
            return receipts

        batches = await asyncio.gather(*(run_mission(group) for group in grouped.values()))
        by_attempt = {receipt.attempt_id: receipt for batch in batches for receipt in batch}
        return tuple(by_attempt[request.attempt_id] for request in requests)

    async def _session(self, request: TaskExecutionRequest) -> _ModalMissionSession:
        async with self._session_lock:
            session = self._sessions.get(request.mission_id)
            if session is None:
                session = await _ModalMissionSession.create(self.config, request)
                self._sessions[request.mission_id] = session
            return session

    async def close_mission(self, mission_id: int) -> None:
        async with self._session_lock:
            session = self._sessions.pop(mission_id, None)
        if session is not None:
            await session.close()

    async def close(self) -> None:
        async with self._session_lock:
            sessions = tuple(self._sessions.values())
            self._sessions.clear()
        results = await asyncio.gather(
            *(session.close() for session in sessions),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise BaseExceptionGroup(
                f"failed to close {len(failures)} Modal mission sandbox(es)",
                failures,
            )


__all__ = ["ModalAgentMissionSandbox", "ModalAgentSandboxConfig"]
