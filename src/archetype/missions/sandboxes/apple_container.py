# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Apple Container Backend for local, VM-isolated Agent Missions."""

from __future__ import annotations

import asyncio
import hashlib
import os
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

_PROVIDER = "apple-container"
_CHECKPOINT_PREFIX = "apple-container-rootfs://"
_CODEX_AUTH_PATH = f"{CODEX_HOME}/auth.json"
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]*$")


@dataclass(frozen=True)
class AppleContainerSandboxConfig:
    """Host-only configuration for Apple Container resources."""

    image_name: str = ""
    state_dir: str = ".context/apple-container-checkpoints"
    cpus: int = 4
    memory: str = "8g"
    checkpoint_timeout_seconds: int = 5 * 60

    def __post_init__(self) -> None:
        if self.cpus < 1 or not self.memory.strip():
            raise ValueError("Apple Container resources require positive CPUs and memory")
        for label, value in (("image_name", self.resolved_image_name),):
            if not _NAME_RE.fullmatch(value):
                raise ValueError(f"invalid Apple Container {label}: {value!r}")
        if self.checkpoint_timeout_seconds < 1:
            raise ValueError("checkpoint timeout must be positive")

    @property
    def resolved_image_name(self) -> str:
        return self.image_name or local_image_name("archetype-agent")


class AppleContainerSandboxSession:
    """One Apple Container VM."""

    def __init__(
        self,
        *,
        spec: SandboxSpec,
        config: AppleContainerSandboxConfig,
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
            raise ValueError(f"unsupported Apple Container secret(s): {', '.join(sorted(unknown))}")
        async with self._lock:
            if self._status is not SandboxStatus.READY:
                raise RuntimeError(f"Apple Container sandbox session is {self._status.value}")
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
        """Export the session rootfs, excluding mounts, without leaving the VM stopped."""

        async with self._lock:
            if self._status is not SandboxStatus.READY:
                raise RuntimeError(f"Apple Container sandbox session is {self._status.value}")
            credentials_absent = await self._exec_request(
                ProcessRequest(("test", "!", "-e", _CODEX_AUTH_PATH), timeout_seconds=60)
            )
            self._raise(credentials_absent, "verify credential-free checkpoint")
            state_dir = Path(self._config.state_dir).expanduser().resolve()
            state_dir.mkdir(parents=True, exist_ok=True)
            temporary = state_dir / f".{self._sandbox_id}-{uuid4().hex}.tar.partial"
            stopped = await self._host("stop", "--time", "30", self._sandbox_id, timeout=60)
            self._raise(stopped, "container stop for checkpoint")
            export_error: BaseException | None = None
            try:
                exported = await self._host(
                    "export",
                    "--output",
                    str(temporary),
                    self._sandbox_id,
                    timeout=self._config.checkpoint_timeout_seconds,
                )
                self._raise(exported, "container filesystem export")
            except BaseException as exc:
                export_error = exc
            restart_error: BaseException | None = None
            try:
                restart = await self._host("start", self._sandbox_id, timeout=60)
                self._raise(restart, "container restart after checkpoint")
            except BaseException as exc:
                restart_error = exc
                self._status = SandboxStatus.ERRORED
            if export_error is not None:
                temporary.unlink(missing_ok=True)
                if restart_error is not None:
                    raise BaseExceptionGroup(
                        "Apple Container checkpoint export and restart both failed",
                        [export_error, restart_error],
                    )
                raise export_error
            try:
                digest = await asyncio.to_thread(self._digest, temporary)
                archive = state_dir / f"{digest}.rootfs.tar"
                os.replace(temporary, archive)
            except BaseException as exc:
                temporary.unlink(missing_ok=True)
                if restart_error is not None:
                    raise BaseExceptionGroup(
                        "Apple Container checkpoint publication and restart both failed",
                        [exc, restart_error],
                    ) from None
                raise
            if restart_error is not None:
                raise RuntimeError(
                    "container restart after checkpoint failed; "
                    f"completed checkpoint preserved at {archive}"
                ) from restart_error
            return CheckpointRef(
                provider=_PROVIDER,
                checkpoint_id=digest,
                uri=f"{_CHECKPOINT_PREFIX}{archive}",
                created_at_ms=int(time.time() * 1000),
                environment=self._spec.environment,
                source_sandbox_id=self._sandbox_id,
                owner_id=self._spec.metadata_dict().get("mission", ""),
                locality=CheckpointLocality.HOST,
                integrity=f"sha256:{digest}",
            )

    async def close(self) -> None:
        async with self._lock:
            if self._status is SandboxStatus.CLOSED:
                return
            resources = tuple(self._close_resources.items())
            results = await asyncio.gather(
                *(
                    self._host("delete", "--force", resource_id, timeout=60)
                    for _label, resource_id in resources
                ),
                return_exceptions=True,
            )
            failures: list[BaseException] = []
            for (label, _resource_id), result in zip(resources, results, strict=True):
                if isinstance(result, BaseException):
                    failures.append(result)
                elif result.returncode != 0:
                    failures.append(RuntimeError(f"failed to delete {label}: {result.stderr}"))
                else:
                    self._close_resources.pop(label, None)
            if failures:
                self._status = SandboxStatus.ERRORED
                raise BaseExceptionGroup(
                    f"failed to close {len(failures)} Apple Container resource(s)", failures
                )
            self._status = SandboxStatus.CLOSED

    async def _exec_request(self, request: ProcessRequest) -> ProcessResult:
        argv = ["container", "exec", "--user", AGENT_USER]
        if request.workdir:
            argv.extend(["--workdir", request.workdir])
        for key, value in request.env:
            argv.extend(["--env", f"{key}={value}"])
        argv.extend([self._sandbox_id, *request.argv])
        return await run_host(argv, timeout_seconds=request.timeout_seconds)

    @staticmethod
    async def _host(*arguments: str, timeout: int) -> ProcessResult:
        return await run_host(("container", *arguments), timeout_seconds=timeout)

    @staticmethod
    def _digest(path: Path) -> str:
        with path.open("rb") as file:
            return hashlib.file_digest(file, "sha256").hexdigest()

    @staticmethod
    def _raise(result: ProcessResult, label: str) -> None:
        if result.returncode != 0:
            detail = result.stderr or result.stdout
            raise RuntimeError(f"{label} failed with exit code {result.returncode}: {detail}")


class AppleContainerSandboxBackend:
    """Create and restore Apple Container sessions."""

    name: ClassVar[str] = _PROVIDER

    def __init__(self, config: AppleContainerSandboxConfig | None = None) -> None:
        self.config = config or AppleContainerSandboxConfig()

    @property
    def environment(self) -> str:
        """Executable image identity authors bind into ``AgentMissionConfig``."""

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
        archive = self._checkpoint_archive(checkpoint)
        digest = await asyncio.to_thread(AppleContainerSandboxSession._digest, archive)
        if (
            checkpoint.locality is not CheckpointLocality.HOST
            or checkpoint.checkpoint_id != digest
            or checkpoint.integrity != f"sha256:{digest}"
        ):
            raise ValueError("Apple Container checkpoint integrity or locality does not match")
        await self._preflight()
        await self._ensure_image(self.config.resolved_image_name)
        restored_image = await self._restore_image(archive, digest=digest)
        session = await self._launch(spec, restored_image)
        try:
            result = await session.exec(
                ProcessRequest(("test", "-d", spec.workdir), timeout_seconds=60)
            )
            AppleContainerSandboxSession._raise(result, "verify restored workspace")
        except BaseException:
            await session.close()
            raise
        return session

    def _validate_spec(self, spec: SandboxSpec) -> None:
        if spec.provider != self.name:
            raise ValueError("Apple Container backend received a different provider")
        if spec.environment != self.environment:
            raise ValueError(
                f"Apple Container environment must be {self.environment!r}, "
                f"got {spec.environment!r}"
            )

    async def _preflight(self) -> None:
        await self._require_runtime()

    @staticmethod
    async def _require_runtime() -> None:
        if shutil.which("container") is None:
            raise RuntimeError(
                "Apple Container is required; install it and run `container system start`"
            )
        status = await run_host(
            ("container", "system", "status"),
            timeout_seconds=30,
        )
        if status.returncode != 0:
            raise RuntimeError(
                "Apple Container is not running; run `container system start`: "
                f"{status.stderr or status.stdout}"
            )

    @staticmethod
    async def _ensure_image(image_name: str) -> None:
        inspected = await run_host(
            ("container", "image", "inspect", image_name),
            timeout_seconds=30,
        )
        if inspected.returncode == 0:
            return
        with tempfile.TemporaryDirectory(prefix="archetype-apple-image-") as temp_dir:
            context = Path(temp_dir)
            containerfile = context / "Containerfile"
            containerfile.write_text(coding_agent_containerfile(), encoding="utf-8")
            built = await run_host(
                (
                    "container",
                    "build",
                    "--progress",
                    "plain",
                    "--tag",
                    image_name,
                    "--file",
                    str(containerfile),
                    str(context),
                ),
                timeout_seconds=20 * 60,
            )
        AppleContainerSandboxSession._raise(built, "container image build")

    async def _launch(self, spec: SandboxSpec, image_name: str) -> AppleContainerSandboxSession:
        sandbox_id = f"archetype-codex-{uuid4().hex[:12]}"
        launched = await run_host(
            (
                "container",
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
        AppleContainerSandboxSession._raise(launched, "container run")
        session = AppleContainerSandboxSession(
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

    async def _restore_image(self, archive: Path, *, digest: str) -> str:
        image_name = f"archetype-agent:restore-{digest[:24]}"
        inspected = await run_host(
            ("container", "image", "inspect", image_name),
            timeout_seconds=30,
        )
        if inspected.returncode == 0:
            return image_name
        with tempfile.TemporaryDirectory(prefix="archetype-apple-restore-") as temp_dir:
            context = Path(temp_dir)
            target = context / "rootfs.tar"
            try:
                os.link(archive, target)
            except OSError:
                shutil.copy2(archive, target)
            containerfile = context / "Containerfile"
            containerfile.write_text(
                "FROM scratch\n"
                "ADD rootfs.tar /\n"
                'ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"\n'
                f"ENV HOME={AGENT_HOME}\n"
                f"ENV ARCHETYPE_SANDBOX_ENVIRONMENT={coding_agent_environment()}\n"
                f"USER {AGENT_USER}\n"
                "WORKDIR /workspace\n",
                encoding="utf-8",
            )
            built = await run_host(
                (
                    "container",
                    "build",
                    "--progress",
                    "plain",
                    "--tag",
                    image_name,
                    "--file",
                    str(containerfile),
                    str(context),
                ),
                timeout_seconds=20 * 60,
            )
        AppleContainerSandboxSession._raise(built, "container restore image build")
        return image_name

    @staticmethod
    def _checkpoint_archive(checkpoint: CheckpointRef) -> Path:
        if checkpoint.provider != _PROVIDER:
            raise ValueError("Apple Container checkpoint provider does not match")
        if not checkpoint.uri.startswith(_CHECKPOINT_PREFIX) or "#" in checkpoint.uri:
            raise ValueError("invalid Apple Container rootfs checkpoint")
        value = checkpoint.uri.removeprefix(_CHECKPOINT_PREFIX)
        if not value:
            raise ValueError("invalid Apple Container rootfs checkpoint")
        archive = Path(value).expanduser().resolve()
        if not archive.is_file():
            raise FileNotFoundError(f"Apple Container checkpoint does not exist: {archive}")
        return archive


__all__ = [
    "AppleContainerSandboxBackend",
    "AppleContainerSandboxConfig",
    "AppleContainerSandboxSession",
]
