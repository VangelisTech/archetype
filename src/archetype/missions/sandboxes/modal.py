# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Modal Backend and Session implementation for mission sandboxes."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from uuid import uuid4

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
_MAX_PROVIDER_OPERATION_ID_LENGTH = 1024
_MAX_PROVIDER_OBJECT_ID_LENGTH = 256
_MAX_PROVIDER_COHORT_ID_LENGTH = 256
_MODAL_NAMESPACE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}")
_MODAL_OPERATION_COHORT = re.compile(r"cohort-v1:[0-9a-f]{32}")

# Epoch zero and operations without an epoch predate the persistent provider
# barrier. They must never infer retry permission from a missing marker.
MODAL_ACTIVITY_PROTOCOL_EPOCH = 1


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


def _require_modal_namespace_name(value: str, *, label: str) -> str:
    if not _MODAL_NAMESPACE_NAME.fullmatch(value):
        raise ValueError(f"Modal {label} is invalid")
    return value


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
    workspace_name: str | None = None
    environment_name: str | None = None
    operation_protocol_epoch: int | None = None

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
        if self.operation_protocol_epoch is not None and self.operation_protocol_epoch < 0:
            raise ValueError("Modal operation protocol epoch must not be negative")


@dataclass(frozen=True, slots=True)
class ModalSandboxOperationIdentity:
    """Stable full provider identity known before any Modal effect."""

    workspace_name: str
    environment_name: str
    app_name: str
    operation_id: str
    protocol_epoch: int

    def __post_init__(self) -> None:
        _require_modal_namespace_name(self.workspace_name, label="workspace_name")
        _require_modal_namespace_name(self.environment_name, label="environment_name")
        _require_modal_namespace_name(self.app_name, label="app_name")
        if not self.operation_id.strip():
            raise ValueError("Modal sandbox operation_id must not be empty")
        if self.operation_id != self.operation_id.strip():
            raise ValueError("Modal sandbox operation_id must not have surrounding whitespace")
        if "\x00" in self.operation_id:
            raise ValueError("Modal sandbox operation_id must not contain NUL")
        if len(self.operation_id) > _MAX_PROVIDER_OPERATION_ID_LENGTH:
            raise ValueError(
                "Modal sandbox operation_id must not exceed "
                f"{_MAX_PROVIDER_OPERATION_ID_LENGTH} characters"
            )
        if self.protocol_epoch < 0:
            raise ValueError("Modal operation protocol epoch must not be negative")

    @property
    def digest(self) -> str:
        """Return the complete namespaced content identity."""

        payload = json.dumps(
            {
                "schema_version": 2,
                "provider": "modal",
                "workspace_name": self.workspace_name,
                "environment_name": self.environment_name,
                "app_name": self.app_name,
                "protocol_epoch": self.protocol_epoch,
                "operation_id": self.operation_id,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        return f"sha256:{hashlib.sha256(payload).hexdigest()}"

    @property
    def mission_sandbox_name(self) -> str:
        """Return a deterministic provider-safe name for the mission sandbox."""

        return self._sandbox_name("m")

    @property
    def auth_sandbox_name(self) -> str:
        """Return a deterministic provider-safe name for the isolated auth broker."""

        return self._sandbox_name("a")

    def _sandbox_name(self, role: str) -> str:
        digest = bytes.fromhex(self.digest.removeprefix("sha256:"))
        encoded = base64.b32encode(digest).decode().rstrip("=").lower()
        return f"arc-{role}-{encoded}"


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
        operation_identity: ModalSandboxOperationIdentity | None = None,
        operation_cohort_id: str | None = None,
    ) -> None:
        self._spec = spec
        self._sandbox = sandbox
        self._auth_sandbox = auth_sandbox
        self._github_secret = github_secret
        self._auth_volume_name = auth_volume_name
        self._checkpoint_timeout_seconds = checkpoint_timeout_seconds
        self._checkpoint_ttl_seconds = checkpoint_ttl_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._operation_identity = operation_identity
        self._operation_cohort_id = operation_cohort_id
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
    def operation_identity(self) -> ModalSandboxOperationIdentity | None:
        """Return the stable named-operation identity, when this session has one."""

        return self._operation_identity

    @property
    def operation_cohort_id(self) -> str | None:
        """Return the exact two-resource cohort created for this session."""

        return self._operation_cohort_id

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
            if self._status is not SandboxStatus.READY:
                raise RuntimeError(f"Modal sandbox session is {self._status.value}")
            uses_oauth = _CODEX_SECRET in request.secret_names
            is_agent = request.close_stdin
            heartbeat: asyncio.Task[None] | None = None
            oauth_staged = False
            operation_error: BaseException | None = None
            trace_uri = ""
            try:
                if uses_oauth:
                    await self._stage_oauth()
                    oauth_staged = True
                actual_request = request
                if is_agent:
                    live_directory_ready = False
                    try:
                        await self._ensure_live_directory()
                        await self._clear_live_output()
                        live_directory_ready = True
                    except Exception:
                        pass
                    await self._emit_event_best_effort(
                        SandboxEventType.PROCESS_STARTED,
                        operation=request.argv[0],
                    )
                    if live_directory_ready:
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
                    await self._emit_event_best_effort(
                        SandboxEventType.PROCESS_FINISHED,
                        operation=request.argv[0],
                        returncode=result.returncode,
                    )
                    if live_directory_ready:
                        trace_uri = await self._capture_live_output_best_effort(uuid4().hex)
            except asyncio.CancelledError as exc:
                operation_error = exc
                self._status = SandboxStatus.INTERRUPTED
                raise
            except BaseException as exc:
                operation_error = exc
                self._status = SandboxStatus.ERRORED
                raise
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
                if uses_oauth:
                    try:
                        if oauth_staged:
                            await self._persist_and_remove_oauth()
                        else:
                            await self._remove_oauth()
                    except BaseException as exc:
                        self._status = SandboxStatus.ERRORED
                        if operation_error is not None:
                            raise exc from operation_error
                        raise
            return ProcessResult(
                argv=request.argv,
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                trace_uri=trace_uri,
            )

    async def checkpoint(self) -> CheckpointRef:
        """Capture a provider-native filesystem image after credentials are absent."""

        async with self._lock:
            if self._status is not SandboxStatus.READY:
                raise RuntimeError(f"Modal sandbox session is {self._status.value}")
            absent = await self._exec_on(
                self._sandbox,
                ProcessRequest(
                    ("test", "!", "-e", _MISSION_AUTH_PATH),
                    timeout_seconds=60,
                ),
            )
            self._raise(absent, "verify credential-free checkpoint")
            try:
                await self._ensure_live_directory()
            except Exception:
                pass
            await self._emit_event_best_effort(SandboxEventType.CHECKPOINT_STARTED)
            try:
                await self._scrub_live_output()
                image = await self._sandbox.snapshot_filesystem.aio(
                    timeout=self._checkpoint_timeout_seconds,
                    ttl=self._checkpoint_ttl_seconds,
                )
            except Exception as exc:
                await self._emit_event_best_effort(
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
            await self._emit_event_best_effort(
                SandboxEventType.CHECKPOINT_FINISHED,
                checkpoint_uri=checkpoint.uri,
            )
            return checkpoint

    async def close(self) -> None:
        async with self._lock:
            if self._status is SandboxStatus.CLOSED:
                return
            await self._emit_event_best_effort(SandboxEventType.CLOSING)
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
                raise BaseExceptionGroup(
                    f"failed to close {len(failures)} Modal resource(s)", failures
                )
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
        removal_error: BaseException | None = None
        try:
            await self._remove_oauth()
        except BaseException as exc:
            removal_error = exc
        if persistence_error is not None and removal_error is not None:
            raise BaseExceptionGroup(
                "failed to persist and remove Modal OAuth credential",
                [persistence_error, removal_error],
            )
        if persistence_error is not None:
            raise persistence_error
        if removal_error is not None:
            raise removal_error

    async def _remove_oauth(self) -> None:
        await self._checked(ProcessRequest(("rm", "-rf", _CODEX_HOME), timeout_seconds=60))

    @classmethod
    def live_observation_paths(cls, trace_id: str = "") -> dict[str, str]:
        return live_observation_paths(trace_id=trace_id)

    def _live_observation_paths(self, trace_id: str = "") -> dict[str, str]:
        return live_observation_paths(self.capabilities.observation_directory, trace_id)

    async def _ensure_live_directory(self) -> None:
        paths = self._live_observation_paths()
        result = await self._exec_on(
            self._sandbox,
            ProcessRequest(("mkdir", "-p", paths["directory"]), timeout_seconds=60),
        )
        self._raise(result, "create live observation directory")

    async def _scrub_live_output(self) -> None:
        paths = self._live_observation_paths()
        result = await self._exec_on(
            self._sandbox,
            ProcessRequest(
                (
                    "rm",
                    "-rf",
                    paths["stdout"],
                    paths["stderr"],
                    f"{paths['directory']}/executions",
                ),
                timeout_seconds=60,
            ),
        )
        self._raise(result, "remove raw live output before checkpoint")

    async def _clear_live_output(self) -> None:
        paths = self._live_observation_paths()
        result = await self._exec_on(
            self._sandbox,
            ProcessRequest(("rm", "-f", paths["stdout"], paths["stderr"]), timeout_seconds=60),
        )
        self._raise(result, "clear stale live output before process")

    async def _capture_live_output_best_effort(self, trace_id: str) -> str:
        try:
            current = self._live_observation_paths()
            captured = self._live_observation_paths(trace_id)
            mkdir = await self._exec_on(
                self._sandbox,
                ProcessRequest(("mkdir", "-p", captured["trace_directory"]), timeout_seconds=60),
            )
            self._raise(mkdir, "create execution trace directory")
            for stream in ("stdout", "stderr"):
                copied = await self._exec_on(
                    self._sandbox,
                    ProcessRequest(
                        ("cp", "--", current[stream], captured[stream]),
                        timeout_seconds=60,
                    ),
                )
                self._raise(copied, f"capture execution {stream}")
            return f"modal-sandbox://{self.identity.sandbox_id}{captured['stdout']}"
        except Exception:
            return ""

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
            await self._emit_event_best_effort(SandboxEventType.HEARTBEAT)

    async def _emit_event_best_effort(
        self,
        event_type: SandboxEventType,
        *,
        operation: str = "",
        returncode: int | None = None,
        checkpoint_uri: str = "",
        message: str = "",
    ) -> None:
        """Emit non-authoritative live observation without changing provider outcomes."""

        try:
            await self._emit_event(
                event_type,
                operation=operation,
                returncode=returncode,
                checkpoint_uri=checkpoint_uri,
                message=message,
            )
        except Exception:
            pass

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
            detail = result.stderr or result.stdout
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
        app = await modal.App.lookup.aio(
            self.config.app_name,
            create_if_missing=True,
            environment_name=self.config.environment_name,
        )
        volume = modal.Volume.from_name(
            self.config.auth_volume_name,
            create_if_missing=True,
            version=2,
            environment_name=self.config.environment_name,
        )
        await volume.hydrate.aio()
        image = (
            modal.Image.from_id(self.config.image_id)
            if self.config.image_id
            else self._default_image(modal)
        )
        sandbox = await modal.Sandbox.create.aio(
            "sleep",
            "infinity",
            app=app,
            image=image,
            timeout=self.config.login_timeout_seconds,
            idle_timeout=self.config.login_timeout_seconds,
            workdir=_AUTH_MOUNT,
            volumes={_AUTH_MOUNT: volume},
            tags={"kind": "archetype-agent-oauth-login"},
            environment_name=self.config.environment_name,
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

    async def _start(
        self,
        modal: Any,
        spec: SandboxSpec,
        image: Any,
        *,
        operation_identity: ModalSandboxOperationIdentity | None = None,
        client: Any | None = None,
    ) -> SandboxSession:
        if operation_identity is not None:
            self._validate_operation_identity(operation_identity)
        app = await modal.App.lookup.aio(
            self.config.app_name,
            create_if_missing=True,
            environment_name=self.config.environment_name,
            client=client,
        )
        auth_volume = modal.Volume.from_name(
            self.config.auth_volume_name,
            create_if_missing=False,
            version=2,
            environment_name=self.config.environment_name,
            client=client,
        )
        await auth_volume.hydrate.aio()
        github_secret = modal.Secret.from_name(
            self.config.github_secret_name,
            required_keys=["GITHUB_TOKEN"],
            environment_name=self.config.environment_name,
            client=client,
        )
        metadata = spec.metadata_dict()
        operation_cohort_id = f"cohort-v1:{uuid4().hex}" if operation_identity is not None else None
        operation_tags = (
            {
                "operation_digest": operation_identity.digest,
                "operation_cohort": str(operation_cohort_id),
                "operation_protocol_epoch": str(operation_identity.protocol_epoch),
            }
            if operation_identity is not None
            else {}
        )
        auth_name = (
            {"name": operation_identity.auth_sandbox_name} if operation_identity is not None else {}
        )
        auth_sandbox = await modal.Sandbox.create.aio(
            "sleep",
            "infinity",
            app=app,
            image=image,
            timeout=spec.timeout_seconds,
            idle_timeout=spec.idle_timeout_seconds,
            workdir=_AUTH_MOUNT,
            volumes={_AUTH_MOUNT: auth_volume},
            tags={**metadata, "kind": "archetype-agent-auth", **operation_tags},
            environment_name=self.config.environment_name,
            client=client,
            **auth_name,
        )
        try:
            mission_name = (
                {"name": operation_identity.mission_sandbox_name}
                if operation_identity is not None
                else {}
            )
            sandbox = await modal.Sandbox.create.aio(
                "sleep",
                "infinity",
                app=app,
                image=image,
                timeout=spec.timeout_seconds,
                idle_timeout=spec.idle_timeout_seconds,
                workdir=str(PurePosixPath(spec.workdir).parent),
                tags={**metadata, "kind": "archetype-agent-mission", **operation_tags},
                environment_name=self.config.environment_name,
                client=client,
                **mission_name,
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
            operation_identity=operation_identity,
            operation_cohort_id=operation_cohort_id,
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

    def _validate_operation_identity(
        self,
        identity: ModalSandboxOperationIdentity,
    ) -> None:
        expected = (
            self.config.workspace_name,
            self.config.environment_name,
            self.config.app_name,
            self.config.operation_protocol_epoch,
        )
        observed = (
            identity.workspace_name,
            identity.environment_name,
            identity.app_name,
            identity.protocol_epoch,
        )
        if observed != expected:
            raise ValueError("Modal operation identity does not match backend namespace")

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


@dataclass(frozen=True, slots=True)
class ModalSandboxOperationRunning:
    """Bounded evidence that one coherent named cohort was running.

    This value deliberately carries no executable session. Reconciliation
    cannot grant a second worker access to the inner ``exec`` boundary.
    """

    identity: ModalSandboxOperationIdentity
    mission_sandbox_id: str
    auth_sandbox_id: str
    cohort_id: str

    def __post_init__(self) -> None:
        if not self.mission_sandbox_id.strip() or not self.auth_sandbox_id.strip():
            raise ValueError("running Modal operation requires both sandbox identities")
        if (
            len(self.mission_sandbox_id) > _MAX_PROVIDER_OBJECT_ID_LENGTH
            or len(self.auth_sandbox_id) > _MAX_PROVIDER_OBJECT_ID_LENGTH
        ):
            raise ValueError("running Modal sandbox identity is too long")
        if self.mission_sandbox_id == self.auth_sandbox_id:
            raise ValueError("mission and auth sandbox identities must be distinct")
        if (
            not _MODAL_OPERATION_COHORT.fullmatch(self.cohort_id)
            or len(self.cohort_id) > _MAX_PROVIDER_COHORT_ID_LENGTH
        ):
            raise ValueError("running Modal operation cohort identity is invalid")


@dataclass(frozen=True, slots=True)
class ModalSandboxOperationUnknown:
    """Provider evidence cannot prove a complete live pair or safe absence."""

    identity: ModalSandboxOperationIdentity
    reason: str

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("unknown Modal sandbox reconciliation requires a reason")
        if len(self.reason) > 512:
            raise ValueError("unknown Modal sandbox reconciliation reason is too long")


type ModalSandboxOperationReconciliation = (
    ModalSandboxOperationRunning | ModalSandboxOperationUnknown
)


@dataclass(frozen=True, slots=True)
class _ModalSandboxLookupFailure:
    reason: str


_MODAL_SANDBOX_MISSING = object()


class ModalSandboxOperationCapability:
    """Start and observe one namespaced, cohort-tagged Modal sandbox pair.

    Identity includes the authenticated workspace, explicit Environment, App,
    protocol epoch, and logical operation. Epoch zero and any operation that
    existed before the persistent barrier are legacy: missing barrier evidence
    for them is always Unknown and must never authorize this endpoint.

    ``start`` is an initial execution endpoint, not an idempotent retry
    endpoint. A running provider name prevents a second ``start`` from
    acquiring the same pair, but Modal releases names when sandboxes stop.
    Consequently reconciliation never returns replay permission, a retry
    guard, or an executable session. Public name-based cleanup is deliberately
    absent because a late cleanup could terminate a newer name generation;
    successful sessions close the exact handles they created.

    This is provider-resource substrate, not full Activity parity. The current
    multi-step author harness still invokes inner processes after pair
    creation; those invocations are not one atomic named Modal operation. An
    Activity adapter must use ``ModalProviderStartBarrier.start_initial`` or
    ``start_retry``. The barrier owns both acknowledged marker acquisitions and
    the one allowed call into this capability; no structural value is accepted
    as start authority.
    """

    def __init__(self, backend: ModalSandboxBackend) -> None:
        self._backend = backend
        config = backend.config
        if config.workspace_name is None:
            raise ValueError("Modal named operations require an explicit workspace_name")
        if config.environment_name is None:
            raise ValueError("Modal named operations require an explicit environment_name")
        _require_modal_namespace_name(config.workspace_name, label="workspace_name")
        _require_modal_namespace_name(config.environment_name, label="environment_name")
        _require_modal_namespace_name(config.app_name, label="app_name")
        if config.operation_protocol_epoch != MODAL_ACTIVITY_PROTOCOL_EPOCH:
            raise ValueError(
                "Modal named operations require the barrier-aware protocol epoch "
                f"{MODAL_ACTIVITY_PROTOCOL_EPOCH}"
            )

    def identity(self, operation_id: str) -> ModalSandboxOperationIdentity:
        """Derive provider names without contacting Modal."""

        config = self._backend.config
        assert config.workspace_name is not None
        assert config.environment_name is not None
        assert config.operation_protocol_epoch is not None
        return ModalSandboxOperationIdentity(
            workspace_name=config.workspace_name,
            environment_name=config.environment_name,
            app_name=config.app_name,
            operation_id=operation_id,
            protocol_epoch=config.operation_protocol_epoch,
        )

    async def _start_after_provider_barrier(
        self,
        *,
        identity: ModalSandboxOperationIdentity,
        spec: SandboxSpec,
    ) -> ModalSandboxSession:
        """Create the pair after the barrier acknowledged its permanent marker.

        This is deliberately private. The ``ModalProviderStartBarrier`` start
        methods are the public family contract and never release transferable
        start authority. The first named sandbox create additionally
        serializes the live provider cohort, but it is not a retry guard
        because Modal releases sandbox names when they stop.
        """

        if self.identity(identity.operation_id) != identity:
            raise ValueError("Modal provider barrier belongs to another operation capability")
        self._validate_spec(spec)
        modal = self._load_modal()
        client = await self._verified_client(modal, phase="start")
        if isinstance(client, _ModalSandboxLookupFailure):
            raise RuntimeError(client.reason)
        image = (
            modal.Image.from_id(self._backend.config.image_id, client=client)
            if self._backend.config.image_id
            else self._backend._default_image(modal)
        )
        session = await self._backend._start(
            modal,
            spec,
            image,
            operation_identity=identity,
            client=client,
        )
        if not isinstance(session, ModalSandboxSession):
            raise TypeError("Modal named operation returned a non-Modal session")
        return session

    async def reconcile(
        self,
        *,
        operation_id: str,
        spec: SandboxSpec,
    ) -> ModalSandboxOperationReconciliation:
        """Observe a complete running pair without granting execution authority."""

        identity = self.identity(operation_id)
        self._validate_spec(spec)
        modal = self._load_modal()
        client = await self._verified_client(modal, phase="reconciliation")
        if isinstance(client, _ModalSandboxLookupFailure):
            return ModalSandboxOperationUnknown(identity, client.reason)
        mission, auth = await asyncio.gather(
            self._lookup(
                modal,
                client=client,
                role="mission",
                name=identity.mission_sandbox_name,
            ),
            self._lookup(
                modal,
                client=client,
                role="auth",
                name=identity.auth_sandbox_name,
            ),
        )
        lookups = {"mission": mission, "auth": auth}
        failures = [
            value.reason
            for value in lookups.values()
            if isinstance(value, _ModalSandboxLookupFailure)
        ]
        if failures:
            return ModalSandboxOperationUnknown(
                identity,
                "; ".join(sorted(failures)),
            )
        missing = [role for role, value in lookups.items() if value is _MODAL_SANDBOX_MISSING]
        if len(missing) == len(lookups):
            return ModalSandboxOperationUnknown(
                identity,
                "no running named Modal sandbox pair; absence is not retry authorization",
            )
        if missing:
            return ModalSandboxOperationUnknown(
                identity,
                "named Modal sandbox pair is partial; absent=" + ",".join(sorted(missing)),
            )

        mission_tags, auth_tags, mission_poll, auth_poll = await asyncio.gather(
            self._tags(mission, role="mission"),
            self._tags(auth, role="auth"),
            self._poll(mission, role="mission"),
            self._poll(auth, role="auth"),
        )
        tags = {"mission": mission_tags, "auth": auth_tags}
        polls = {"mission": mission_poll, "auth": auth_poll}
        observation_failures = [
            value.reason
            for value in (*tags.values(), *polls.values())
            if isinstance(value, _ModalSandboxLookupFailure)
        ]
        if observation_failures:
            return ModalSandboxOperationUnknown(
                identity,
                "; ".join(sorted(observation_failures)),
            )
        stopped = [role for role, value in polls.items() if value is not None]
        if stopped:
            return ModalSandboxOperationUnknown(
                identity,
                "named Modal sandbox is not running; stopped=" + ",".join(sorted(stopped)),
            )

        expected_tags = {
            "operation_digest": identity.digest,
            "operation_protocol_epoch": str(identity.protocol_epoch),
        }
        for role, values in tags.items():
            if not isinstance(values, dict):
                return ModalSandboxOperationUnknown(
                    identity,
                    f"{role} sandbox returned invalid cohort evidence",
                )
            if any(values.get(key) != value for key, value in expected_tags.items()):
                return ModalSandboxOperationUnknown(
                    identity,
                    f"{role} sandbox does not match the full provider operation identity",
                )
            if values.get("kind") != f"archetype-agent-{role}":
                return ModalSandboxOperationUnknown(
                    identity,
                    f"{role} sandbox role evidence does not match",
                )
        mission_cohort = mission_tags.get("operation_cohort")
        auth_cohort = auth_tags.get("operation_cohort")
        if (
            not isinstance(mission_cohort, str)
            or not _MODAL_OPERATION_COHORT.fullmatch(mission_cohort)
            or mission_cohort != auth_cohort
        ):
            return ModalSandboxOperationUnknown(
                identity,
                "named Modal sandbox pair has missing or mixed-generation cohort evidence",
            )

        return ModalSandboxOperationRunning(
            identity=identity,
            mission_sandbox_id=str(mission.object_id),
            auth_sandbox_id=str(auth.object_id),
            cohort_id=mission_cohort,
        )

    def _validate_spec(self, spec: SandboxSpec) -> None:
        if spec.provider != self._backend.name:
            raise ValueError("Modal operation capability received a non-Modal sandbox spec")
        if spec.environment != self._backend.environment:
            raise ValueError(
                f"Modal environment must be {self._backend.environment!r}, got {spec.environment!r}"
            )

    @staticmethod
    def _load_modal() -> Any:
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc
        return modal

    async def _verified_client(
        self,
        modal: Any,
        *,
        phase: str,
    ) -> Any | _ModalSandboxLookupFailure:
        expected = self._backend.config.workspace_name
        assert expected is not None
        try:
            workspace = modal.Workspace.from_context()
            await workspace.hydrate.aio()
            observed = str(workspace.name or "")
            client = workspace.client
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return _ModalSandboxLookupFailure(
                f"Modal {phase} workspace lookup failed ({type(exc).__name__[:128]})"
            )
        if observed != expected:
            return _ModalSandboxLookupFailure(
                f"Modal {phase} workspace identity does not match configured namespace"
            )
        return client

    async def _lookup(
        self,
        modal: Any,
        *,
        client: Any,
        role: str,
        name: str,
    ) -> Any:
        try:
            return await modal.Sandbox.from_name.aio(
                self._backend.config.app_name,
                name,
                environment_name=self._backend.config.environment_name,
                client=client,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            not_found = getattr(getattr(modal, "exception", None), "NotFoundError", None)
            if isinstance(not_found, type) and isinstance(exc, not_found):
                return _MODAL_SANDBOX_MISSING
            return _ModalSandboxLookupFailure(f"{role} lookup failed ({type(exc).__name__[:128]})")

    @staticmethod
    async def _tags(sandbox: Any, *, role: str) -> Any:
        try:
            return await sandbox.get_tags.aio()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return _ModalSandboxLookupFailure(
                f"{role} cohort lookup failed ({type(exc).__name__[:128]})"
            )

    @staticmethod
    async def _poll(sandbox: Any, *, role: str) -> Any:
        try:
            return await sandbox.poll.aio()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return _ModalSandboxLookupFailure(f"{role} poll failed ({type(exc).__name__[:128]})")


__all__ = [
    "MODAL_ACTIVITY_PROTOCOL_EPOCH",
    "ModalSandboxBackend",
    "ModalSandboxConfig",
    "ModalSandboxOperationCapability",
    "ModalSandboxOperationIdentity",
    "ModalSandboxOperationReconciliation",
    "ModalSandboxOperationRunning",
    "ModalSandboxOperationUnknown",
    "ModalSandboxSession",
]
