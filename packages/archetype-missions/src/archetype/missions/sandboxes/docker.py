# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Docker Backend used as the Linux and CI Sandbox protocol reference."""

from __future__ import annotations

import asyncio
import re
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar
from uuid import uuid4

from archetype.missions.sandboxes._image import (
    AGENT_HOME,
    AGENT_USER,
    CODEX_HOME,
    coding_agent_containerfile,
    coding_agent_environment,
    local_image_name,
    verify_coding_agent_environment,
)
from archetype.missions.sandboxes._subprocess import run_host
from archetype.missions.sandboxes.contracts import (
    CheckpointLocality,
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxIdentity,
    SandboxSession,
    SandboxSpec,
    SandboxStatus,
    validate_checkpoint_for_spec,
)

_PROVIDER = "docker"
_CHECKPOINT_PREFIX = "docker-image://"
_CODEX_AUTH_PATH = f"{CODEX_HOME}/auth.json"
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]*$")


@dataclass(frozen=True)
class DockerSandboxConfig:
    """Host-only configuration for Docker sandbox resources."""

    image_name: str = ""
    cpus: int = 4
    memory: str = "8g"
    checkpoint_repository: str = "archetype-agent-checkpoints"

    def __post_init__(self) -> None:
        if self.cpus < 1 or not self.memory.strip():
            raise ValueError("Docker resources require positive CPUs and memory")
        for label, value in (
            ("image_name", self.resolved_image_name),
            ("checkpoint_repository", self.checkpoint_repository),
        ):
            if not _NAME_RE.fullmatch(value):
                raise ValueError(f"invalid Docker {label}: {value!r}")

    @property
    def resolved_image_name(self) -> str:
        return self.image_name or local_image_name("archetype-agent")


class DockerSandboxSession:
    """One Docker mission container."""

    def __init__(
        self,
        *,
        spec: SandboxSpec,
        config: DockerSandboxConfig,
        sandbox_id: str,
    ) -> None:
        self._spec = spec
        self._config = config
        self._sandbox_id = sandbox_id
        self._status = SandboxStatus.READY
        self._lock = asyncio.Lock()
        self._close_resources = {"mission": sandbox_id}

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity(_PROVIDER, self._sandbox_id, self._spec.environment)

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(
            checkpoints=True,
            secret_names=(),
            home_directory=AGENT_HOME,
        )

    async def status(self) -> SandboxStatus:
        return self._status

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        unknown = set(request.secret_names) - set(self.capabilities.secret_names)
        if unknown:
            raise ValueError(f"unsupported Docker secret(s): {', '.join(sorted(unknown))}")
        async with self._lock:
            if self._status is not SandboxStatus.READY:
                raise RuntimeError(f"Docker sandbox session is {self._status.value}")
            try:
                result = await self._exec_request(request)
            except asyncio.CancelledError:
                self._status = SandboxStatus.INTERRUPTED
                raise
            except BaseException:
                self._status = SandboxStatus.ERRORED
                raise
            return result

    async def checkpoint(self) -> CheckpointRef:
        """Commit the session-owned writable layer to an immutable Docker image ID."""

        async with self._lock:
            if self._status is not SandboxStatus.READY:
                raise RuntimeError(f"Docker sandbox session is {self._status.value}")
            credentials_absent = await self._exec_request(
                ProcessRequest(("test", "!", "-e", _CODEX_AUTH_PATH), timeout_seconds=60)
            )
            self._raise(credentials_absent, "verify credential-free checkpoint")
            tag = f"{self._config.checkpoint_repository}:{uuid4().hex}"
            committed = await self._host(
                "commit",
                "--message",
                "Archetype Agent Mission checkpoint",
                self._sandbox_id,
                tag,
                timeout=5 * 60,
            )
            self._raise(committed, "docker commit")
            try:
                inspected = await self._host(
                    "image",
                    "inspect",
                    "--format",
                    "{{.Id}}",
                    tag,
                    timeout=60,
                )
                self._raise(inspected, "docker checkpoint inspect")
                image_id = inspected.stdout.strip()
                if not image_id.startswith("sha256:"):
                    raise RuntimeError(f"Docker checkpoint returned invalid image ID: {image_id!r}")
            except BaseException:
                try:
                    await self._host("rmi", "--force", tag, timeout=60)
                except BaseException:
                    pass
                raise
            return CheckpointRef(
                provider=_PROVIDER,
                checkpoint_id=image_id.removeprefix("sha256:"),
                uri=f"{_CHECKPOINT_PREFIX}{image_id}",
                created_at_ms=int(time.time() * 1000),
                environment=self._spec.environment,
                source_sandbox_id=self._sandbox_id,
                owner_id=self._spec.metadata_dict().get("mission", ""),
                locality=CheckpointLocality.HOST,
                integrity=image_id,
            )

    async def close(self) -> None:
        async with self._lock:
            if self._status is SandboxStatus.CLOSED:
                return
            resources = tuple(self._close_resources.items())
            results = await asyncio.gather(
                *(
                    self._host("rm", "--force", container_id, timeout=60)
                    for _label, container_id in resources
                ),
                return_exceptions=True,
            )
            failures: list[BaseException] = []
            for (label, container_id), result in zip(resources, results, strict=True):
                if isinstance(result, BaseException):
                    failures.append(result)
                elif result.returncode != 0:
                    failures.append(
                        RuntimeError(
                            f"failed to delete Docker container {container_id}: {result.stderr}"
                        )
                    )
                else:
                    self._close_resources.pop(label, None)
            if failures:
                self._status = SandboxStatus.ERRORED
                raise BaseExceptionGroup(
                    f"failed to close {len(failures)} Docker resource(s)", failures
                )
            self._status = SandboxStatus.CLOSED

    async def _exec_request(self, request: ProcessRequest) -> ProcessResult:
        argv = ["docker", "exec", "--user", AGENT_USER]
        if request.workdir:
            argv.extend(["--workdir", request.workdir])
        for key, value in request.env:
            argv.extend(["--env", f"{key}={value}"])
        argv.extend([self._sandbox_id, *request.argv])
        return await run_host(argv, timeout_seconds=request.timeout_seconds)

    @staticmethod
    async def _host(*arguments: str, timeout: int) -> ProcessResult:
        return await run_host(("docker", *arguments), timeout_seconds=timeout)

    @staticmethod
    def _raise(result: ProcessResult, label: str) -> None:
        if result.returncode != 0:
            detail = result.stderr or result.stdout
            raise RuntimeError(f"{label} failed with exit code {result.returncode}: {detail}")


class DockerSandboxBackend:
    """Create and restore Docker sessions through the Sandbox Backend contract."""

    name: ClassVar[str] = _PROVIDER

    def __init__(self, config: DockerSandboxConfig | None = None) -> None:
        self.config = config or DockerSandboxConfig()

    @property
    def environment(self) -> str:
        return coding_agent_environment()

    async def create(self, spec: SandboxSpec) -> SandboxSession:
        self._validate_spec(spec)
        await self._preflight()
        await self._ensure_image(self.config.resolved_image_name)
        return await self._launch(spec, self.config.resolved_image_name)

    async def restore(
        self,
        spec: SandboxSpec,
        checkpoint: CheckpointRef,
    ) -> SandboxSession:
        self._validate_spec(spec)
        validate_checkpoint_for_spec(checkpoint, spec)
        image_id = self._checkpoint_image(checkpoint)
        await self._preflight()
        inspected = await run_host(
            ("docker", "image", "inspect", image_id),
            timeout_seconds=60,
        )
        DockerSandboxSession._raise(inspected, "docker restored image inspect")
        session = await self._launch(spec, image_id)
        try:
            result = await session.exec(
                ProcessRequest(("test", "-d", spec.workdir), timeout_seconds=60)
            )
            DockerSandboxSession._raise(result, "verify restored workspace")
        except BaseException:
            await session.close()
            raise
        return session

    def _validate_spec(self, spec: SandboxSpec) -> None:
        if spec.provider != self.name:
            raise ValueError("Docker backend received a different provider")
        if spec.environment != self.environment:
            raise ValueError(
                f"Docker environment must be {self.environment!r}, got {spec.environment!r}"
            )

    async def _preflight(self) -> None:
        await self._require_runtime()

    @staticmethod
    async def _require_runtime() -> None:
        if shutil.which("docker") is None:
            raise RuntimeError("Docker is required for the Docker sandbox backend")
        status = await run_host(("docker", "info"), timeout_seconds=30)
        if status.returncode != 0:
            raise RuntimeError(f"Docker is not running: {status.stderr or status.stdout}")

    @staticmethod
    async def _ensure_image(image_name: str) -> None:
        inspected = await run_host(
            ("docker", "image", "inspect", image_name),
            timeout_seconds=30,
        )
        if inspected.returncode == 0:
            return
        with tempfile.TemporaryDirectory(prefix="archetype-docker-image-") as temp_dir:
            context = Path(temp_dir)
            dockerfile = context / "Dockerfile"
            dockerfile.write_text(coding_agent_containerfile(), encoding="utf-8")
            built = await run_host(
                (
                    "docker",
                    "build",
                    "--progress",
                    "plain",
                    "--tag",
                    image_name,
                    "--file",
                    str(dockerfile),
                    str(context),
                ),
                timeout_seconds=20 * 60,
            )
        DockerSandboxSession._raise(built, "docker image build")

    async def _launch(self, spec: SandboxSpec, image_name: str) -> DockerSandboxSession:
        sandbox_id = f"archetype-codex-{uuid4().hex[:12]}"
        launched = await run_host(
            (
                "docker",
                "run",
                "--detach",
                "--init",
                "--name",
                sandbox_id,
                "--cpus",
                str(self.config.cpus),
                "--memory",
                self.config.memory,
                "--cap-drop",
                "ALL",
                "--label",
                "archetype.kind=agent-mission",
                image_name,
                "sleep",
                "infinity",
            ),
            timeout_seconds=120,
        )
        DockerSandboxSession._raise(launched, "docker run")
        session = DockerSandboxSession(
            spec=spec,
            config=self.config,
            sandbox_id=sandbox_id,
        )
        try:
            await verify_coding_agent_environment(session, spec, expected_user=AGENT_USER)
        except BaseException:
            await session.close()
            raise
        return session

    @staticmethod
    def _checkpoint_image(checkpoint: CheckpointRef) -> str:
        if checkpoint.provider != _PROVIDER:
            raise ValueError("Docker checkpoint provider does not match")
        if checkpoint.locality is not CheckpointLocality.HOST:
            raise ValueError("Docker checkpoint locality does not match")
        if not checkpoint.uri.startswith(_CHECKPOINT_PREFIX) or "#" in checkpoint.uri:
            raise ValueError("invalid Docker image checkpoint")
        image_id = checkpoint.uri.removeprefix(_CHECKPOINT_PREFIX)
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
            raise ValueError("invalid Docker image checkpoint")
        if checkpoint.integrity != image_id:
            raise ValueError("Docker checkpoint integrity does not match")
        return image_id


__all__ = ["DockerSandboxBackend", "DockerSandboxConfig", "DockerSandboxSession"]
