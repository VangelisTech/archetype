# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Modal Backend and Session implementation for mission sandboxes."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from archetype.missions.sandboxes.contracts import (
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxIdentity,
    SandboxSession,
    SandboxSpec,
    SandboxStatus,
)

_AUTH_MOUNT = "/auth"
_AUTH_VOLUME_PATH = f"{_AUTH_MOUNT}/auth.json"
_CODEX_HOME = "/root/.codex"
_MISSION_AUTH_PATH = f"{_CODEX_HOME}/auth.json"
_CODEX_SECRET = "codex_oauth"
_GITHUB_SECRET = "github"


@dataclass(frozen=True)
class ModalSandboxConfig:
    """Provider configuration; repository coordinates arrive in ``SandboxSpec``."""

    app_name: str = "archetype-agent-missions"
    image_name: str = ""
    auth_volume_name: str = "archetype-codex-auth"
    github_secret_name: str = "archetype-github"

    def __post_init__(self) -> None:
        if not self.app_name.strip():
            raise ValueError("Modal app_name must not be empty")
        if not self.auth_volume_name.strip():
            raise ValueError("Modal Codex auth volume must not be empty")
        if not self.github_secret_name.strip():
            raise ValueError("commit-and-push requires a GitHub secret")


class ModalSandboxSession:
    """One Modal filesystem/process container plus an isolated auth broker."""

    def __init__(
        self,
        *,
        spec: SandboxSpec,
        sandbox: Any,
        auth_sandbox: Any,
        github_secret: Any,
        auth_volume_name: str,
    ) -> None:
        self._spec = spec
        self._sandbox = sandbox
        self._auth_sandbox = auth_sandbox
        self._github_secret = github_secret
        self._auth_volume_name = auth_volume_name
        self._lock = asyncio.Lock()
        self._status = SandboxStatus.READY

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity(
            provider="modal",
            sandbox_id=str(self._sandbox.object_id),
            environment=self._spec.environment,
        )

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(
            checkpoints=False,
            secret_names=(_CODEX_SECRET, _GITHUB_SECRET),
        )

    async def status(self) -> SandboxStatus:
        return self._status

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        unknown = set(request.secret_names) - set(self.capabilities.secret_names)
        if unknown:
            raise ValueError(f"unsupported Modal sandbox secret(s): {', '.join(sorted(unknown))}")
        async with self._lock:
            if self._status is SandboxStatus.CLOSED:
                raise RuntimeError("Modal sandbox session is closed")
            uses_oauth = _CODEX_SECRET in request.secret_names
            if uses_oauth:
                await self._stage_oauth()
            try:
                result = await self._exec_on(
                    self._sandbox,
                    request,
                    secrets=(
                        [self._github_secret] if _GITHUB_SECRET in request.secret_names else []
                    ),
                )
            except asyncio.CancelledError:
                self._status = SandboxStatus.INTERRUPTED
                raise
            except BaseException:
                self._status = SandboxStatus.ERRORED
                raise
            finally:
                if uses_oauth:
                    await self._persist_and_remove_oauth()
            return result

    async def checkpoint(self) -> CheckpointRef:
        raise NotImplementedError("Modal checkpoint capture is not configured for V1")

    async def close(self) -> None:
        if self._status is SandboxStatus.CLOSED:
            return
        self._status = SandboxStatus.CLOSED
        results = await asyncio.gather(
            self._terminate(self._sandbox),
            self._terminate(self._auth_sandbox),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise BaseExceptionGroup(f"failed to close {len(failures)} Modal resource(s)", failures)

    async def _stage_oauth(self) -> None:
        try:
            payload = await self._auth_sandbox.filesystem.read_text.aio(_AUTH_VOLUME_PATH)
        except Exception as exc:
            raise RuntimeError(
                f"Modal OAuth volume {self._auth_volume_name!r} has no Codex credential"
            ) from exc
        self._validate_oauth(payload)
        await self._checked(ProcessRequest(("mkdir", "-p", _CODEX_HOME), timeout_seconds=60))
        await self._sandbox.filesystem.write_text.aio(payload, _MISSION_AUTH_PATH)
        await self._checked(
            ProcessRequest(("chmod", "600", _MISSION_AUTH_PATH), timeout_seconds=60)
        )

    async def _persist_and_remove_oauth(self) -> None:
        persistence_error: BaseException | None = None
        try:
            payload = await self._sandbox.filesystem.read_text.aio(_MISSION_AUTH_PATH)
            self._validate_oauth(payload)
            temporary = f"{_AUTH_VOLUME_PATH}.next"
            await self._auth_sandbox.filesystem.write_text.aio(payload, temporary)
            await self._auth_checked("chmod", "600", temporary)
            await self._auth_checked("mv", "-f", temporary, _AUTH_VOLUME_PATH)
            await self._auth_checked("sync", _AUTH_MOUNT)
        except BaseException as exc:
            persistence_error = exc
        finally:
            await self._checked(
                ProcessRequest(("rm", "-f", _MISSION_AUTH_PATH), timeout_seconds=60)
            )
        if persistence_error is not None:
            raise persistence_error

    async def _checked(self, request: ProcessRequest) -> ProcessResult:
        result = await self._exec_on(self._sandbox, request)
        self._raise(result, request.argv[0])
        return result

    async def _auth_checked(self, *arguments: str) -> ProcessResult:
        request = ProcessRequest(tuple(arguments), timeout_seconds=60)
        result = await self._exec_on(self._auth_sandbox, request)
        self._raise(result, arguments[0])
        return result

    @staticmethod
    async def _exec_on(
        sandbox: Any,
        request: ProcessRequest,
        *,
        secrets: list[Any] | None = None,
    ) -> ProcessResult:
        process = await sandbox.exec.aio(
            *request.argv,
            workdir=request.workdir,
            timeout=request.timeout_seconds,
            secrets=secrets or [],
            env=request.environment_dict() or None,
        )
        if request.close_stdin:
            # Modal exposes stdin as an open pipe. Codex otherwise waits for
            # optional prompt input even when the prompt is in argv.
            process.stdin.write_eof()
            await process.stdin.drain.aio()
        stdout_task = asyncio.create_task(process.stdout.read.aio())
        stderr_task = asyncio.create_task(process.stderr.read.aio())
        returncode, stdout, stderr = await asyncio.gather(
            process.wait.aio(), stdout_task, stderr_task
        )
        return ProcessResult(
            argv=request.argv,
            returncode=int(returncode),
            stdout=str(stdout),
            stderr=str(stderr),
        )

    @staticmethod
    def _validate_oauth(payload: str) -> None:
        try:
            value = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("Codex OAuth credential is not valid JSON") from exc
        if not isinstance(value, dict) or not value:
            raise RuntimeError("Codex OAuth credential must be a non-empty JSON object")

    @staticmethod
    def _raise(result: ProcessResult, label: str) -> None:
        if result.returncode != 0:
            detail = (result.stderr or result.stdout)[-4000:]
            raise RuntimeError(f"{label} failed with exit code {result.returncode}: {detail}")

    @staticmethod
    async def _terminate(sandbox: Any) -> None:
        try:
            await sandbox.terminate.aio(wait=True)
        finally:
            await sandbox.detach.aio()


class ModalSandboxBackend:
    """Create Modal sessions; task and repository policy stay in the harness."""

    name = "modal"

    def __init__(self, config: ModalSandboxConfig | None = None) -> None:
        self.config = config or ModalSandboxConfig()

    async def create(self, spec: SandboxSpec) -> SandboxSession:
        if spec.provider != self.name:
            raise ValueError("Modal backend received a non-Modal sandbox spec")
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc

        app = await modal.App.lookup.aio(self.config.app_name, create_if_missing=True)
        image = (
            modal.Image.from_name(self.config.image_name)
            if self.config.image_name
            else self._default_image(modal)
        )
        auth_volume = modal.Volume.from_name(
            self.config.auth_volume_name,
            create_if_missing=False,
            version=2,
        )
        await auth_volume.hydrate.aio()
        github_secret = modal.Secret.from_name(
            self.config.github_secret_name,
            required_keys=["GITHUB_TOKEN"],
        )
        metadata = spec.metadata_dict()
        auth_sandbox = await modal.Sandbox.create.aio(
            app=app,
            image=image,
            timeout=spec.timeout_seconds,
            idle_timeout=spec.idle_timeout_seconds,
            workdir=_AUTH_MOUNT,
            volumes={_AUTH_MOUNT: auth_volume},
            tags={"kind": "archetype-agent-auth", **metadata},
        )
        try:
            sandbox = await modal.Sandbox.create.aio(
                app=app,
                image=image,
                timeout=spec.timeout_seconds,
                idle_timeout=spec.idle_timeout_seconds,
                workdir=str(PurePosixPath(spec.workdir).parent),
                tags={"kind": "archetype-agent-mission", **metadata},
            )
        except BaseException:
            await ModalSandboxSession._terminate(auth_sandbox)
            raise
        return ModalSandboxSession(
            spec=spec,
            sandbox=sandbox,
            auth_sandbox=auth_sandbox,
            github_secret=github_secret,
            auth_volume_name=self.config.auth_volume_name,
        )

    async def restore(
        self,
        spec: SandboxSpec,
        checkpoint: CheckpointRef,
    ) -> SandboxSession:
        raise NotImplementedError("Modal checkpoint restore is not configured for V1")

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


__all__ = ["ModalSandboxBackend", "ModalSandboxConfig", "ModalSandboxSession"]
