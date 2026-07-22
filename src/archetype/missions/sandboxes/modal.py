# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Modal Backend and Session implementation for mission sandboxes."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from archetype.missions.sandboxes._image import (
    BASE_IMAGE_REF,
    codex_install_command,
    codex_package,
    verify_coding_agent_environment,
)
from archetype.missions.sandboxes.contracts import (
    CheckpointLocality,
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxEvent,
    SandboxEventType,
    SandboxIdentity,
    SandboxSession,
    SandboxSpec,
    SandboxStatus,
    live_observation_paths,
    validate_checkpoint_for_spec,
)
from archetype.missions.sandboxes.versions import load_version_inventory

_AUTH_MOUNT = "/auth"
_AUTH_VOLUME_PATH = f"{_AUTH_MOUNT}/auth.json"
_CODEX_HOME = "/root/.codex"
_MISSION_AUTH_PATH = f"{_CODEX_HOME}/auth.json"
_CODEX_SECRET = "codex_oauth"
_GITHUB_SECRET = "github"


def _default_environment() -> str:
    """Return the content identity attested by the default Modal image."""

    pin = load_version_inventory().harness_pin("codex")
    material = "\n".join(
        (
            BASE_IMAGE_REF,
            "ca-certificates curl git nodejs npm openssh-client",
            pin.name,
            pin.version,
            pin.source,
            pin.immutable_ref,
            "user=root",
            "home=/root",
            "workdir=/workspace",
        )
    )
    return f"modal-agent://sha256:{hashlib.sha256(material.encode()).hexdigest()}"


@dataclass(frozen=True)
class ModalSandboxConfig:
    """Provider configuration; repository coordinates arrive in ``SandboxSpec``."""

    app_name: str = "archetype-agent-missions"
    image_id: str = ""
    auth_volume_name: str = "archetype-codex-auth"
    github_secret_name: str = "archetype-github"
    checkpoint_timeout_seconds: int = 5 * 60
    checkpoint_ttl_seconds: int | None = 30 * 24 * 60 * 60
    heartbeat_seconds: int = 15
    login_timeout_seconds: int = 15 * 60

    def __post_init__(self) -> None:
        if not self.app_name.strip():
            raise ValueError("Modal app_name must not be empty")
        if self.image_id and not self.image_id.startswith("im-"):
            raise ValueError("Modal image_id must be an immutable im-... identity")
        if not self.auth_volume_name.strip():
            raise ValueError("Modal Codex auth volume must not be empty")
        if not self.github_secret_name.strip():
            raise ValueError("commit-and-push requires a GitHub secret")
        if self.checkpoint_timeout_seconds < 1:
            raise ValueError("checkpoint timeout must be positive")
        if self.checkpoint_ttl_seconds is not None and self.checkpoint_ttl_seconds < 1:
            raise ValueError("checkpoint TTL must be positive when configured")
        if self.heartbeat_seconds < 1:
            raise ValueError("heartbeat interval must be positive")
        if self.login_timeout_seconds < 1:
            raise ValueError("login timeout must be positive")


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
        checkpoint_timeout_seconds: int,
        checkpoint_ttl_seconds: int | None,
        heartbeat_seconds: int,
    ) -> None:
        self._spec = spec
        self._sandbox = sandbox
        self._auth_sandbox = auth_sandbox
        self._github_secret = github_secret
        self._auth_volume_name = auth_volume_name
        self._checkpoint_timeout_seconds = checkpoint_timeout_seconds
        self._checkpoint_ttl_seconds = checkpoint_ttl_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._lock = asyncio.Lock()
        self._event_lock = asyncio.Lock()
        self._event_sequence = 0
        self._status = SandboxStatus.READY
        self._close_resources = {
            "mission": sandbox,
            "OAuth broker": auth_sandbox,
        }

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
            checkpoints=True,
            live_output=True,
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
            is_agent = request.close_stdin
            if uses_oauth:
                await self._stage_oauth()
            heartbeat: asyncio.Task[None] | None = None
            try:
                actual_request = request
                if is_agent:
                    await self._ensure_live_directory()
                    await self._emit_event(
                        SandboxEventType.PROCESS_STARTED,
                        operation=request.argv[0],
                    )
                    actual_request = self._trace_request(request)
                    heartbeat = asyncio.create_task(self._heartbeat())
                result = await self._exec_on(
                    self._sandbox,
                    actual_request,
                    secrets=(
                        [self._github_secret] if _GITHUB_SECRET in request.secret_names else []
                    ),
                )
                if is_agent:
                    await self._emit_event(
                        SandboxEventType.PROCESS_FINISHED,
                        operation=request.argv[0],
                        returncode=result.returncode,
                    )
            except asyncio.CancelledError:
                self._status = SandboxStatus.INTERRUPTED
                raise
            except BaseException:
                self._status = SandboxStatus.ERRORED
                raise
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
                if uses_oauth:
                    await self._persist_and_remove_oauth()
            return ProcessResult(
                argv=request.argv,
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )

    async def checkpoint(self) -> CheckpointRef:
        """Capture a provider-native filesystem image after credentials are absent."""

        async with self._lock:
            if self._status is SandboxStatus.CLOSED:
                raise RuntimeError("Modal sandbox session is closed")
            absent = await self._exec_on(
                self._sandbox,
                ProcessRequest(
                    ("test", "!", "-e", _MISSION_AUTH_PATH),
                    timeout_seconds=60,
                ),
            )
            self._raise(absent, "verify credential-free checkpoint")
            await self._ensure_live_directory()
            await self._emit_event(SandboxEventType.CHECKPOINT_STARTED)
            try:
                image = await self._sandbox.snapshot_filesystem.aio(
                    timeout=self._checkpoint_timeout_seconds,
                    ttl=self._checkpoint_ttl_seconds,
                )
            except Exception as exc:
                await self._emit_event(
                    SandboxEventType.CHECKPOINT_FAILED,
                    message=type(exc).__name__,
                )
                raise
            image_id = str(image.object_id)
            if not image_id.startswith("im-"):
                raise RuntimeError(f"Modal checkpoint returned invalid image ID: {image_id!r}")
            created_at_ms = int(time.time() * 1000)
            checkpoint = CheckpointRef(
                provider="modal",
                checkpoint_id=image_id,
                uri=f"modal-image://{image_id}",
                created_at_ms=created_at_ms,
                environment=self._spec.environment,
                source_sandbox_id=self.identity.sandbox_id,
                owner_id=self._spec.metadata_dict().get("mission", ""),
                locality=CheckpointLocality.PROVIDER,
                expires_at_ms=(
                    created_at_ms + self._checkpoint_ttl_seconds * 1000
                    if self._checkpoint_ttl_seconds is not None
                    else None
                ),
            )
            await self._emit_event(
                SandboxEventType.CHECKPOINT_FINISHED,
                checkpoint_uri=checkpoint.uri,
            )
            return checkpoint

    async def close(self) -> None:
        if self._status is SandboxStatus.CLOSED:
            return
        try:
            await self._emit_event(SandboxEventType.CLOSING)
        except Exception:
            pass
        resources = tuple(self._close_resources.items())
        results = await asyncio.gather(
            *(self._terminate(resource) for _label, resource in resources),
            return_exceptions=True,
        )
        failures: list[BaseException] = []
        for (label, _resource), result in zip(resources, results, strict=True):
            if isinstance(result, BaseException):
                failures.append(result)
            else:
                self._close_resources.pop(label, None)
        if failures:
            self._status = SandboxStatus.ERRORED
            raise BaseExceptionGroup(f"failed to close {len(failures)} Modal resource(s)", failures)
        self._status = SandboxStatus.CLOSED

    async def _stage_oauth(self) -> None:
        try:
            payload = await self._auth_sandbox.filesystem.read_text.aio(_AUTH_VOLUME_PATH)
        except Exception as exc:
            raise RuntimeError(
                f"Modal OAuth volume {self._auth_volume_name!r} has no Codex credential"
            ) from exc
        self._validate_oauth(payload)
        await self._checked(
            ProcessRequest(
                ("sh", "-c", f"rm -rf {_CODEX_HOME} && install -d -m 700 {_CODEX_HOME}"),
                timeout_seconds=60,
            )
        )
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

    @classmethod
    def live_observation_paths(cls) -> dict[str, str]:
        return live_observation_paths()

    def _live_observation_paths(self) -> dict[str, str]:
        return live_observation_paths(self.capabilities.observation_directory)

    async def _ensure_live_directory(self) -> None:
        paths = self._live_observation_paths()
        result = await self._exec_on(
            self._sandbox,
            ProcessRequest(("mkdir", "-p", paths["directory"]), timeout_seconds=60),
        )
        self._raise(result, "create live observation directory")

    def _trace_request(self, request: ProcessRequest) -> ProcessRequest:
        paths = self._live_observation_paths()
        script = (
            'stdout="$1"; stderr="$2"; shift 2; '
            '"$@" > >(tee -a "$stdout") 2> >(tee -a "$stderr" >&2)'
        )
        return ProcessRequest(
            (
                "bash",
                "-o",
                "pipefail",
                "-c",
                script,
                "archetype-agent-trace",
                paths["stdout"],
                paths["stderr"],
                *request.argv,
            ),
            workdir=request.workdir,
            timeout_seconds=request.timeout_seconds,
            env=request.env,
            close_stdin=True,
        )

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_seconds)
            await self._emit_event(SandboxEventType.HEARTBEAT)

    async def _emit_event(
        self,
        event_type: SandboxEventType,
        *,
        operation: str = "",
        returncode: int | None = None,
        checkpoint_uri: str = "",
        message: str = "",
    ) -> None:
        paths = self._live_observation_paths()
        async with self._event_lock:
            self._event_sequence += 1
            event = SandboxEvent(
                kind=event_type,
                sandbox=self.identity,
                timestamp_ms=int(time.time() * 1000),
                sequence=self._event_sequence,
                operation=operation,
                returncode=returncode,
                checkpoint_uri=checkpoint_uri,
                message=message,
            ).record()
            line = json.dumps(event, sort_keys=True) + "\n"
            try:
                existing = str(await self._sandbox.filesystem.read_text.aio(paths["events"]))
            except Exception as exc:
                if not self._is_missing_path(exc):
                    raise
                existing = ""
            await self._sandbox.filesystem.write_text.aio(existing + line, paths["events"])
            await self._sandbox.filesystem.write_text.aio(line, paths["status"])

    @classmethod
    async def monitor(
        cls,
        sandbox_id: str,
        *,
        follow: bool = True,
        poll_seconds: float = 1.0,
        disconnect_grace_seconds: float = 180.0,
        stdout_target: Any = None,
        stderr_target: Any = None,
        on_monitor_event: Callable[[dict[str, str]], None] | None = None,
    ) -> dict[str, Any]:
        """Attach to one Modal sandbox's durable live observation files."""

        if not sandbox_id.startswith("sb-"):
            raise ValueError("Modal sandbox IDs must start with 'sb-'")
        if poll_seconds <= 0 or disconnect_grace_seconds <= 0:
            raise ValueError("monitor timing values must be positive")
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc
        sandbox = await modal.Sandbox.from_id.aio(sandbox_id)
        paths = cls.live_observation_paths()
        offsets: dict[str, int] = {}
        status: dict[str, Any] = {}
        disconnected_at: float | None = None

        async def read(path: str) -> str:
            try:
                return str(await sandbox.filesystem.read_text.aio(path))
            except Exception as exc:
                if cls._is_missing_path(exc):
                    return ""
                raise

        while True:
            try:
                status_text, events, stdout, stderr = await asyncio.gather(
                    read(paths["status"]),
                    read(paths["events"]),
                    read(paths["stdout"]),
                    read(paths["stderr"]),
                )
                if status_text:
                    try:
                        parsed = json.loads(status_text)
                    except json.JSONDecodeError:
                        parsed = {}
                    if isinstance(parsed, dict):
                        status = parsed
                cls._write_delta(paths["events"], events, offsets, stdout_target)
                cls._write_delta(paths["stdout"], stdout, offsets, stdout_target)
                cls._write_delta(paths["stderr"], stderr, offsets, stderr_target)
                provider_returncode = await sandbox.poll.aio()
            except Exception as exc:
                if not follow:
                    raise
                now = time.monotonic()
                disconnected_at = disconnected_at or now
                if now - disconnected_at >= disconnect_grace_seconds:
                    if on_monitor_event is not None:
                        on_monitor_event(
                            {
                                "type": "monitor_disconnected",
                                "sandbox_id": sandbox_id,
                                "error": str(exc)[-1000:],
                            }
                        )
                    return status
                await asyncio.sleep(poll_seconds)
                continue
            if disconnected_at is not None:
                if on_monitor_event is not None:
                    on_monitor_event({"type": "monitor_reconnected", "sandbox_id": sandbox_id})
                disconnected_at = None
            if provider_returncode is not None:
                status = {**status, "provider_returncode": int(provider_returncode)}
                return status
            if not follow or status.get("type") == SandboxEventType.CLOSING.value:
                return status
            await asyncio.sleep(poll_seconds)

    @staticmethod
    def _write_delta(path: str, value: str, offsets: dict[str, int], target: Any) -> None:
        previous = offsets.get(path, 0)
        if previous > len(value):
            previous = 0
        delta = value[previous:]
        offsets[path] = len(value)
        if delta and target is not None:
            target.write(delta)
            target.flush()

    @staticmethod
    def _is_missing_path(exc: BaseException) -> bool:
        return isinstance(exc, FileNotFoundError) or type(exc).__name__ == (
            "SandboxFilesystemNotFoundError"
        )

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

    @property
    def environment(self) -> str:
        """Declared identity of the named image or complete default recipe."""

        if self.config.image_id:
            return f"modal-image://{self.config.image_id}"
        return _default_environment()

    async def create(self, spec: SandboxSpec) -> SandboxSession:
        if spec.provider != self.name:
            raise ValueError("Modal backend received a non-Modal sandbox spec")
        if spec.environment != self.environment:
            raise ValueError(
                f"Modal environment must be {self.environment!r}, got {spec.environment!r}"
            )
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc

        image = (
            modal.Image.from_id(self.config.image_id)
            if self.config.image_id
            else self._default_image(modal)
        )
        return await self._start(modal, spec, image)

    async def login_codex(self) -> None:
        """Persist an interactive Codex subscription login in the broker volume."""

        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc
        app = await modal.App.lookup.aio(self.config.app_name, create_if_missing=True)
        volume = modal.Volume.from_name(
            self.config.auth_volume_name,
            create_if_missing=True,
            version=2,
        )
        await volume.hydrate.aio()
        image = (
            modal.Image.from_id(self.config.image_id)
            if self.config.image_id
            else self._default_image(modal)
        )
        sandbox = await modal.Sandbox.create.aio(
            app=app,
            image=image,
            timeout=self.config.login_timeout_seconds,
            idle_timeout=self.config.login_timeout_seconds,
            workdir=_AUTH_MOUNT,
            volumes={_AUTH_MOUNT: volume},
            tags={"kind": "archetype-agent-oauth-login"},
        )
        try:
            process = await sandbox.exec.aio(
                "codex",
                "login",
                "--device-auth",
                "-c",
                'cli_auth_credentials_store="file"',
                workdir=_AUTH_MOUNT,
                timeout=self.config.login_timeout_seconds,
                env={"CODEX_HOME": _AUTH_MOUNT, "NO_COLOR": "1"},
                pty=True,
            )
            returncode = await self._passthrough_process(process)
            if returncode != 0:
                raise RuntimeError(f"Codex device login failed with exit code {returncode}")
            result = await ModalSandboxSession._exec_on(
                sandbox,
                ProcessRequest(
                    ("codex", "login", "status"),
                    workdir=_AUTH_MOUNT,
                    timeout_seconds=60,
                    env=(("CODEX_HOME", _AUTH_MOUNT),),
                ),
            )
            ModalSandboxSession._raise(result, "Codex device login verification")
        finally:
            cleanup = asyncio.create_task(self._cleanup_login(sandbox))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise

    @staticmethod
    async def _cleanup_login(sandbox: Any) -> None:
        try:
            result = await ModalSandboxSession._exec_on(
                sandbox,
                ProcessRequest(
                    (
                        "sh",
                        "-c",
                        f"find {_AUTH_MOUNT} -mindepth 1 -maxdepth 1 "
                        "! -name auth.json -exec rm -rf -- {} + "
                        f"&& chmod 600 {_AUTH_VOLUME_PATH} && sync {_AUTH_MOUNT}",
                    ),
                    timeout_seconds=60,
                ),
            )
            ModalSandboxSession._raise(result, "Codex OAuth volume cleanup")
        finally:
            await ModalSandboxSession._terminate(sandbox)

    @staticmethod
    async def _passthrough_process(process: Any) -> int:
        """Bridge a remote device-login PTY without retaining its output."""

        loop = asyncio.get_running_loop()
        writes: set[asyncio.Task[Any]] = set()
        stdin_fd: int | None = None

        async def write_remote(data: bytes) -> None:
            await process.stdin.write.aio(data)
            await process.stdin.drain.aio()

        def stdin_ready() -> None:
            assert stdin_fd is not None
            try:
                data = os.read(stdin_fd, 4096)
            except OSError:
                data = b""
            if not data:
                loop.remove_reader(stdin_fd)
                return
            task = asyncio.create_task(write_remote(data))
            writes.add(task)
            task.add_done_callback(writes.discard)

        async def pump(reader: Any, target: Any) -> None:
            async for chunk in reader:
                value = chunk.decode(errors="replace") if isinstance(chunk, bytes) else str(chunk)
                target.write(value)
                target.flush()

        try:
            try:
                stdin_fd = sys.stdin.fileno()
                loop.add_reader(stdin_fd, stdin_ready)
            except (AttributeError, OSError, ValueError, NotImplementedError):
                stdin_fd = None
            stdout = asyncio.create_task(pump(process.stdout, sys.stdout))
            stderr = asyncio.create_task(pump(process.stderr, sys.stderr))
            returncode = int(await process.wait.aio())
            await asyncio.gather(stdout, stderr)
            return returncode
        finally:
            if stdin_fd is not None:
                try:
                    loop.remove_reader(stdin_fd)
                except (OSError, ValueError):
                    pass
            if writes:
                await asyncio.gather(*writes, return_exceptions=True)

    async def _start(self, modal: Any, spec: SandboxSpec, image: Any) -> SandboxSession:
        app = await modal.App.lookup.aio(self.config.app_name, create_if_missing=True)
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
        session = ModalSandboxSession(
            spec=spec,
            sandbox=sandbox,
            auth_sandbox=auth_sandbox,
            github_secret=github_secret,
            auth_volume_name=self.config.auth_volume_name,
            checkpoint_timeout_seconds=self.config.checkpoint_timeout_seconds,
            checkpoint_ttl_seconds=self.config.checkpoint_ttl_seconds,
            heartbeat_seconds=self.config.heartbeat_seconds,
        )
        try:
            await verify_coding_agent_environment(
                session,
                spec,
                expected_user="root",
                verify_environment=not self.config.image_id,
            )
        except BaseException:
            await session.close()
            raise
        return session

    async def restore(
        self,
        spec: SandboxSpec,
        checkpoint: CheckpointRef,
    ) -> SandboxSession:
        if spec.provider != self.name:
            raise ValueError("Modal backend received a non-Modal sandbox spec")
        if spec.environment != self.environment:
            raise ValueError(
                f"Modal environment must be {self.environment!r}, got {spec.environment!r}"
            )
        validate_checkpoint_for_spec(checkpoint, spec)
        image_id = self._checkpoint_image(checkpoint)
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc
        image = modal.Image.from_id(image_id)
        session = await self._start(modal, spec, image)
        try:
            result = await session.exec(
                ProcessRequest(("test", "-d", spec.workdir), timeout_seconds=60)
            )
            ModalSandboxSession._raise(result, "verify restored workspace")
        except BaseException:
            await session.close()
            raise
        return session

    @staticmethod
    def _checkpoint_image(checkpoint: CheckpointRef) -> str:
        if checkpoint.provider != "modal":
            raise ValueError("Modal checkpoint provider does not match")
        if checkpoint.locality is not CheckpointLocality.PROVIDER:
            raise ValueError("Modal checkpoint locality does not match")
        prefix = "modal-image://"
        if not checkpoint.uri.startswith(prefix) or "#" in checkpoint.uri:
            raise ValueError("invalid Modal image checkpoint")
        image_id = checkpoint.uri.removeprefix(prefix)
        if image_id != checkpoint.checkpoint_id or not image_id.startswith("im-"):
            raise ValueError("invalid Modal image checkpoint")
        return image_id

    @staticmethod
    def _default_image(modal: Any) -> Any:
        return (
            modal.Image.from_registry(BASE_IMAGE_REF)
            .apt_install(
                "ca-certificates",
                "curl",
                "git",
                "nodejs",
                "npm",
                "openssh-client",
            )
            .run_commands(
                "mkdir -p /workspace",
                f"# {codex_package()}\n{codex_install_command()}",
            )
            .env({"ARCHETYPE_SANDBOX_ENVIRONMENT": _default_environment()})
        )


__all__ = ["ModalSandboxBackend", "ModalSandboxConfig", "ModalSandboxSession"]
