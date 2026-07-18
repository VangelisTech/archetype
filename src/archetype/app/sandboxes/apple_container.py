# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run a coding agent locally inside an Apple Container lightweight VM."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shutil
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from archetype.app.sandboxes.modal import (
    AgentHarness,
    CodingAgentSandboxClient,
    CommandResult,
)

_CODEX_HOME = "/home/agent/.codex"


def _containerfile(harness: AgentHarness) -> str:
    install_agent = (
        "npm install --global @openai/codex"
        if harness == "codex"
        else "npm install --global @anthropic-ai/claude-code"
    )
    return f"""\
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
RUN apt-get update \\
    && apt-get install -y --no-install-recommends ca-certificates curl git make openssh-client nodejs npm \\
    && rm -rf /var/lib/apt/lists/*
RUN {install_agent}
RUN useradd --create-home --uid 1000 agent \\
    && mkdir -p /workspace \\
    && chown agent:agent /workspace
USER agent
WORKDIR /workspace
"""


@dataclass(frozen=True)
class AppleContainerSandboxSpec:
    """Configuration for one local coding mission in Apple Container."""

    repo_url: str
    branch: str
    base_ref: str = "main"
    harness: AgentHarness = "codex"
    model: str = ""
    workspace: str = "/workspace/repo"
    image_name: str = ""
    state_dir: str = ".context/apple-container-snapshots"
    cpus: int = 4
    memory: str = "8g"
    agent_timeout_seconds: int = 45 * 60
    snapshot_timeout_seconds: int = 120
    snapshot_ttl_seconds: int | None = None
    snapshot_after_attempt: bool = True
    capture_filesystem_manifests: bool = True
    push: bool = False
    git_author_name: str = "Archetype Coding Agent"
    git_author_email: str = "coding-agent@archetype.local"
    codex_auth_env: str = ""
    codex_auth_volume: str = "archetype-codex-auth"
    claude_api_key_env: str = "ANTHROPIC_API_KEY"
    github_token_env: str = "GITHUB_TOKEN"

    def __post_init__(self) -> None:
        workspace = PurePosixPath(self.workspace)
        if not workspace.is_absolute() or str(workspace) in {"/", "."}:
            raise ValueError("workspace must be a non-root absolute path")
        if not self.repo_url or self.repo_url.startswith("-"):
            raise ValueError("repo_url must be a non-empty git URL")
        if not self.branch or self.branch.startswith("-"):
            raise ValueError("branch must be a non-empty git branch name")
        if not self.base_ref or self.base_ref.startswith("-"):
            raise ValueError("base_ref must be a non-empty git ref")
        if self.harness not in {"codex", "claude-code"}:
            raise ValueError(f"unsupported coding-agent harness: {self.harness!r}")
        if self.cpus < 1 or not self.memory:
            raise ValueError("cpus and memory must define positive resources")
        for value in filter(
            None,
            (self.codex_auth_env, self.claude_api_key_env, self.github_token_env),
        ):
            if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", value):
                raise ValueError(f"invalid environment variable name: {value!r}")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", self.codex_auth_volume):
            raise ValueError(f"invalid Apple Container volume name: {self.codex_auth_volume!r}")

    @property
    def agent_secret_env(self) -> str:
        if self.harness == "codex":
            return self.codex_auth_env
        return self.claude_api_key_env

    @property
    def resolved_image_name(self) -> str:
        if self.image_name:
            return self.image_name
        digest = hashlib.sha256(_containerfile(self.harness).encode()).hexdigest()[:12]
        harness = self.harness.replace("-", "")
        return f"archetype-coding-agent-{harness}:local-{digest}"


@dataclass
class AppleContainerSandboxClient(CodingAgentSandboxClient[AppleContainerSandboxSpec]):
    """Coding-agent gate backed by Apple's ``container`` CLI.

    The repository exists only in the mission VM. No host directory or
    credential file is mounted there. OAuth state lives in a separate broker
    VM and is staged in memory only while Codex runs; API secrets are inherited
    by the selected agent process and authenticated Git commands only.
    """

    spec: AppleContainerSandboxSpec
    _sandbox: str
    _agent_secret: str | None
    _github_secret: str | None = None
    _auth_sandbox: str | None = None

    @property
    def sandbox_id(self) -> str:
        return self._sandbox

    @classmethod
    async def create(cls, spec: AppleContainerSandboxSpec) -> AppleContainerSandboxClient:
        """Build the cached image, launch a VM, and clone the repository."""

        await cls._preflight(spec)
        await cls._ensure_image(spec)
        client = await cls._launch(spec, mission_image=spec.resolved_image_name)
        try:
            if spec.harness == "codex" and not spec.codex_auth_env:
                await client._check_codex_oauth()
            await client._prepare_repository()
        except BaseException:
            await client.close()
            raise
        return client

    @classmethod
    async def restore(
        cls,
        spec: AppleContainerSandboxSpec,
        checkpoint_ref: str,
    ) -> AppleContainerSandboxClient:
        """Launch a new mission VM from a complete root-filesystem export."""

        prefix = "apple-container-rootfs://"
        archive_value = checkpoint_ref.removeprefix(prefix)
        if not checkpoint_ref.startswith(prefix) or not archive_value or "#" in archive_value:
            raise ValueError(
                "Apple Container checkpoint must be a non-empty apple-container-rootfs:// reference"
            )
        archive = Path(archive_value).expanduser().resolve()
        if not archive.is_file():
            raise FileNotFoundError(f"Apple Container checkpoint does not exist: {archive}")

        await cls._preflight(spec)
        # The base image remains useful for the credential broker. The mission
        # itself starts from a content-addressed image reconstructed from the
        # exported root filesystem.
        await cls._ensure_image(spec)
        restored_image = await cls._ensure_restore_image(spec, archive)
        client = await cls._launch(spec, mission_image=restored_image)
        try:
            if spec.harness == "codex" and not spec.codex_auth_env:
                await client._check_codex_oauth()
            await client._git("rev-parse", "--is-inside-work-tree")
        except BaseException:
            await client.close()
            raise
        return client

    @classmethod
    async def _preflight(cls, spec: AppleContainerSandboxSpec) -> None:
        await cls._require_container_runtime()
        if spec.agent_secret_env and not os.environ.get(spec.agent_secret_env):
            raise RuntimeError(
                f"local {spec.harness} backend requires {spec.agent_secret_env} in the host "
                "environment"
            )
        if spec.harness == "codex" and not spec.codex_auth_env:
            volume = await cls._run_host(
                "container", "volume", "inspect", spec.codex_auth_volume, timeout=30
            )
            if volume.returncode != 0:
                raise RuntimeError(
                    "Codex OAuth is not initialized. Run "
                    "`uv run python examples/11_coding_agent_mission.py --codex-login` "
                    "once, then retry the mission."
                )
        if spec.push and not os.environ.get(spec.github_token_env):
            raise RuntimeError(
                f"push=True requires {spec.github_token_env} in the host environment"
            )

    @classmethod
    async def _launch(
        cls,
        spec: AppleContainerSandboxSpec,
        *,
        mission_image: str,
    ) -> AppleContainerSandboxClient:
        container_id = f"archetype-{spec.harness.replace('-', '')}-{uuid4().hex[:12]}"
        launch_argv = [
            "container",
            "run",
            "--detach",
            "--init",
            "--name",
            container_id,
            "--cpus",
            str(spec.cpus),
            "--memory",
            spec.memory,
            "--cap-drop",
            "ALL",
            "--label",
            "archetype.kind=coding-agent",
            "--label",
            f"archetype.harness={spec.harness}",
        ]
        launch_argv.extend([mission_image, "sleep", "infinity"])
        launched = await cls._run_host(
            *launch_argv,
            timeout=120,
        )
        cls._raise_host_failure(launched, "container run")

        auth_sandbox = None
        if spec.harness == "codex" and not spec.codex_auth_env:
            auth_sandbox = f"{container_id}-auth"
            auth_launched = await cls._run_host(
                "container",
                "run",
                "--detach",
                "--init",
                "--name",
                auth_sandbox,
                "--cpus",
                "1",
                "--memory",
                "1g",
                "--cap-drop",
                "ALL",
                "--label",
                "archetype.kind=codex-auth-broker",
                "--volume",
                f"{spec.codex_auth_volume}:{_CODEX_HOME}",
                spec.resolved_image_name,
                "sleep",
                "infinity",
                timeout=120,
            )
            if auth_launched.returncode != 0:
                await cls._run_host("container", "delete", "--force", container_id, timeout=60)
                cls._raise_host_failure(auth_launched, "Codex auth broker container run")

        github_secret = spec.github_token_env if spec.push else None
        client = cls(
            spec=spec,
            _sandbox=container_id,
            _agent_secret=spec.agent_secret_env or None,
            _github_secret=github_secret,
            _auth_sandbox=auth_sandbox,
        )
        return client

    @classmethod
    async def login_codex(cls, spec: AppleContainerSandboxSpec) -> None:
        """Interactively persist a ChatGPT device login in a named VM volume."""

        if spec.harness != "codex":
            raise ValueError("Codex device login requires harness='codex'")
        await cls._require_container_runtime()
        await cls._ensure_image(spec)
        volume = await cls._run_host(
            "container", "volume", "inspect", spec.codex_auth_volume, timeout=30
        )
        if volume.returncode != 0:
            created = await cls._run_host(
                "container",
                "volume",
                "create",
                "--label",
                "archetype.kind=codex-auth",
                spec.codex_auth_volume,
                timeout=30,
            )
            cls._raise_host_failure(created, "Codex auth volume create")

        # Named volumes are initially root-owned. Make this one writable by the
        # unprivileged account used for the interactive Codex login.
        initialized = await cls._run_host(
            "container",
            "run",
            "--remove",
            "--user",
            "root",
            "--volume",
            f"{spec.codex_auth_volume}:{_CODEX_HOME}",
            spec.resolved_image_name,
            "chown",
            "-R",
            "agent:agent",
            _CODEX_HOME,
            timeout=60,
        )
        cls._raise_host_failure(initialized, "Codex auth volume initialization")

        login = await cls._run_host_passthrough(
            "container",
            "run",
            "--remove",
            "--interactive",
            "--tty",
            "--user",
            "agent",
            "--volume",
            f"{spec.codex_auth_volume}:{_CODEX_HOME}",
            spec.resolved_image_name,
            "codex",
            "login",
            "--device-auth",
        )
        if login != 0:
            raise RuntimeError(f"Codex device login failed with exit code {login}")

    @classmethod
    async def _require_container_runtime(cls) -> None:
        if shutil.which("container") is None:
            raise RuntimeError(
                "Apple Container is required for the local backend; install it from "
                "https://github.com/apple/container and run `container system start`"
            )
        status = await cls._run_host("container", "system", "status", timeout=30)
        if status.returncode != 0:
            raise RuntimeError(
                "Apple Container is not running; run `container system start`: "
                f"{status.stderr or status.stdout}"
            )

    @classmethod
    async def _ensure_image(cls, spec: AppleContainerSandboxSpec) -> None:
        inspect = await cls._run_host(
            "container", "image", "inspect", spec.resolved_image_name, timeout=30
        )
        if inspect.returncode == 0:
            return
        with tempfile.TemporaryDirectory(prefix="archetype-container-build-") as temp_dir:
            context = Path(temp_dir)
            containerfile = context / "Containerfile"
            containerfile.write_text(_containerfile(spec.harness))
            built = await cls._run_host(
                "container",
                "build",
                "--progress",
                "plain",
                "--tag",
                spec.resolved_image_name,
                "--file",
                str(containerfile),
                str(context),
                timeout=20 * 60,
            )
        cls._raise_host_failure(built, "container build")

    @classmethod
    async def _ensure_restore_image(
        cls,
        spec: AppleContainerSandboxSpec,
        archive: Path,
    ) -> str:
        """Build a cached image from a rootfs tar emitted by ``container export``."""

        image_name = await cls._restore_image_name(spec, archive)
        inspect = await cls._run_host("container", "image", "inspect", image_name, timeout=30)
        if inspect.returncode == 0:
            return image_name

        with tempfile.TemporaryDirectory(prefix="archetype-container-restore-") as temp_dir:
            context = Path(temp_dir)
            context_archive = context / "rootfs.tar"
            try:
                os.link(archive, context_archive)
            except OSError:
                shutil.copy2(archive, context_archive)
            containerfile = context / "Containerfile"
            containerfile.write_text(
                "FROM scratch\n"
                "ADD rootfs.tar /\n"
                'ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"\n'
                "ENV HOME=/home/agent\n"
                "USER agent\n"
                "WORKDIR /workspace\n"
            )
            built = await cls._run_host(
                "container",
                "build",
                "--progress",
                "plain",
                "--tag",
                image_name,
                "--file",
                str(containerfile),
                str(context),
                timeout=20 * 60,
            )
        cls._raise_host_failure(built, "container restore image build")
        return image_name

    @staticmethod
    async def _restore_image_name(spec: AppleContainerSandboxSpec, archive: Path) -> str:
        def digest_archive() -> str:
            with archive.open("rb") as file:
                return hashlib.file_digest(file, "sha256").hexdigest()

        digest = (await asyncio.to_thread(digest_archive))[:24]
        harness = spec.harness.replace("-", "")
        return f"archetype-coding-agent-{harness}:restore-{digest}"

    async def close(self) -> None:
        """Delete the mission VM and its optional credential-broker VM."""

        if self._closed:
            return
        self._closed = True
        failures: list[tuple[str, CommandResult]] = []
        for label, container_id in (
            ("container delete", self.sandbox_id),
            ("Codex auth broker delete", self._auth_sandbox),
        ):
            if container_id is None:
                continue
            deleted = await self._run_host(
                "container", "delete", "--force", container_id, timeout=60
            )
            if deleted.returncode != 0:
                failures.append((label, deleted))
        if failures:
            self._raise_host_failure(failures[0][1], failures[0][0])

    async def _exec(
        self,
        *args: str,
        workdir: str | None = None,
        timeout: int | None = None,
        secrets: Sequence[Any] = (),
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        argv = ["container", "exec", "--user", "agent"]
        if workdir:
            argv.extend(["--workdir", workdir])
        for key, value in (env or {}).items():
            argv.extend(["--env", f"{key}={value}"])
        for secret in secrets:
            if not secret:
                continue
            name = str(secret)
            if not os.environ.get(name):
                raise RuntimeError(f"required host environment variable is missing: {name}")
            argv.extend(["--env", name])
        argv.extend([self.sandbox_id, *args])
        return await self._run_host(*argv, timeout=timeout)

    async def _run_codex(self, prompt: str, *, session_id: str) -> CommandResult:
        if self._agent_secret:
            return await super()._run_codex(prompt, session_id=session_id)

        await self._stage_codex_oauth()
        try:
            return await super()._run_codex(prompt, session_id=session_id)
        finally:
            await self._persist_and_remove_codex_oauth()

    async def _check_codex_oauth(self) -> None:
        await self._stage_codex_oauth()
        try:
            status = await self._exec("codex", "login", "status", timeout=30)
        finally:
            await self._persist_and_remove_codex_oauth()
        if status.returncode != 0:
            raise RuntimeError(
                "The Codex OAuth volume does not contain a valid login. Run "
                "`uv run python examples/11_coding_agent_mission.py --codex-login` "
                "and complete the device flow, then retry."
            )

    async def _stage_codex_oauth(self) -> None:
        archive = await self._auth_exec(
            "sh",
            "-c",
            "tar -C /home/agent -czf - .codex | base64 -w 0",
            timeout=30,
        )
        self._raise_host_failure(archive, "read Codex OAuth credentials")
        script = (
            f"rm -rf {_CODEX_HOME} && mkdir -p /home/agent && base64 -d | tar -xz -C /home/agent"
        )
        result = await self._run_host(
            "container",
            "exec",
            "--interactive",
            "--user",
            "agent",
            self.sandbox_id,
            "sh",
            "-c",
            script,
            timeout=30,
            stdin=archive.stdout,
        )
        self._raise_host_failure(result, "stage Codex OAuth credentials")

    async def _persist_and_remove_codex_oauth(self) -> None:
        archive = await self._exec(
            "sh",
            "-c",
            "tar -C /home/agent -czf - .codex | base64 -w 0",
            timeout=30,
        )
        self._raise_host_failure(archive, "read refreshed Codex OAuth credentials")
        script = (
            f"find {_CODEX_HOME} -mindepth 1 -maxdepth 1 -exec rm -rf -- {{}} + "
            "&& base64 -d | tar -xz -C /home/agent"
        )
        result = await self._auth_exec("sh", "-c", script, timeout=30, stdin=archive.stdout)
        self._raise_host_failure(result, "persist Codex OAuth credentials")
        removed = await self._exec("rm", "-rf", _CODEX_HOME, timeout=30)
        self._raise_host_failure(removed, "remove staged Codex OAuth credentials")

    async def _auth_exec(
        self,
        *args: str,
        timeout: int | None = None,
        stdin: str | None = None,
    ) -> CommandResult:
        if self._auth_sandbox is None:
            raise RuntimeError("Codex OAuth credential broker is not running")
        argv = ["container", "exec", "--user", "agent"]
        if stdin is not None:
            argv.append("--interactive")
        argv.extend([self._auth_sandbox, *args])
        return await self._run_host(
            *argv,
            timeout=timeout,
            stdin=stdin,
        )

    async def _write_text(self, path: str, value: str) -> None:
        script = (
            "from pathlib import Path; import sys; "
            "p=Path(sys.argv[1]); p.parent.mkdir(parents=True, exist_ok=True); "
            "p.write_text(sys.stdin.read())"
        )
        result = await self._run_host(
            "container",
            "exec",
            "--interactive",
            "--user",
            "agent",
            self.sandbox_id,
            "python3",
            "-c",
            script,
            path,
            timeout=60,
            stdin=value,
        )
        self._raise_host_failure(result, "container write")

    async def _snapshot_if_configured(self, checkpoint_key: str = "") -> str:
        if not self.spec.snapshot_after_attempt:
            return ""
        state_dir = Path(self.spec.state_dir).expanduser().resolve()
        state_dir.mkdir(parents=True, exist_ok=True)
        sha = (await self._git("rev-parse", "HEAD")).stdout.strip()
        checkpoint = checkpoint_key[:12] or uuid4().hex[:12]
        archive_name = f"{self.sandbox_id}-{sha[:12]}-{checkpoint}-rootfs.tar"
        host_archive = state_dir / archive_name
        stopped = await self._run_host(
            "container", "stop", "--time", "30", self.sandbox_id, timeout=60
        )
        self._raise_host_failure(stopped, "container stop for filesystem export")
        exported: CommandResult | None = None
        try:
            exported = await self._run_host(
                "container",
                "export",
                "--output",
                str(host_archive),
                self.sandbox_id,
                timeout=self.spec.snapshot_timeout_seconds,
            )
        finally:
            restarted = await self._run_host("container", "start", self.sandbox_id, timeout=60)
        self._raise_host_failure(restarted, "container restart after filesystem export")
        assert exported is not None
        self._raise_host_failure(exported, "container filesystem export")
        return f"apple-container-rootfs://{host_archive}"

    def _checkpoint_provider(self) -> str:
        return "apple-container"

    def _sandbox_uri(self, path: str) -> str:
        return f"apple-container://{self.sandbox_id}{path}"

    @staticmethod
    async def _run_host(
        *args: str,
        timeout: int | None,
        stdin: str | None = None,
    ) -> CommandResult:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE if stdin is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(stdin.encode() if stdin is not None else None), timeout=timeout
            )
        except TimeoutError:
            process.kill()
            stdout, stderr = await process.communicate()
            stderr += f"\ncommand timed out after {timeout}s".encode()
            return CommandResult(tuple(args), 124, stdout.decode(), stderr.decode())
        return CommandResult(
            tuple(args),
            int(process.returncode or 0),
            stdout.decode(errors="replace"),
            stderr.decode(errors="replace"),
        )

    @staticmethod
    async def _run_host_passthrough(*args: str) -> int:
        process = await asyncio.create_subprocess_exec(*args)
        return int(await process.wait())

    @staticmethod
    def _raise_host_failure(result: CommandResult, label: str) -> None:
        if result.returncode != 0:
            detail = (result.stderr or result.stdout)[-4000:]
            raise RuntimeError(f"{label} failed with exit code {result.returncode}: {detail}")


class AppleContainerSandboxBackend:
    """Apple Container adapter consumed by :class:`SandboxService`."""

    name = "apple-container"

    async def create(self, spec: AppleContainerSandboxSpec) -> AppleContainerSandboxClient:
        return await AppleContainerSandboxClient.create(spec)

    async def restore(
        self, spec: AppleContainerSandboxSpec, checkpoint_ref: str
    ) -> AppleContainerSandboxClient:
        return await AppleContainerSandboxClient.restore(spec, checkpoint_ref)

    async def resume(
        self, spec: AppleContainerSandboxSpec, checkpoint_ref: str
    ) -> AppleContainerSandboxClient:
        # ``restore`` re-resolves host credentials and therefore supports both
        # credential-free recovery and authenticated continuation.
        return await AppleContainerSandboxClient.restore(spec, checkpoint_ref)

    async def authenticate(self, spec: AppleContainerSandboxSpec) -> None:
        if spec.harness == "codex":
            await AppleContainerSandboxClient.login_codex(spec)
        else:
            await AppleContainerSandboxClient._preflight(spec)


__all__ = [
    "AppleContainerSandboxBackend",
    "AppleContainerSandboxClient",
    "AppleContainerSandboxSpec",
]
