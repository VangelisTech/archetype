# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provider-neutral contracts for mission sandbox resources."""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Protocol, runtime_checkable


class SandboxStatus(StrEnum):
    """Lifecycle of an isolated filesystem and process container."""

    PROVISIONING = "provisioning"
    READY = "ready"
    ERRORED = "errored"
    INTERRUPTED = "interrupted"
    CLOSED = "closed"


class SandboxEventType(StrEnum):
    """Structured, provider-neutral operational events with no workflow authority."""

    READY = "sandbox_ready"
    PROCESS_STARTED = "process_started"
    HEARTBEAT = "heartbeat"
    PROCESS_FINISHED = "process_finished"
    CHECKPOINT_STARTED = "checkpoint_started"
    CHECKPOINT_FINISHED = "checkpoint_finished"
    CHECKPOINT_FAILED = "checkpoint_failed"
    CLOSING = "sandbox_closing"


class CheckpointLocality(StrEnum):
    """Where a provider-native checkpoint can be resolved."""

    HOST = "host"
    PROVIDER = "provider"


@dataclass(frozen=True)
class SandboxKey:
    """Application-selected identity used to reuse one live sandbox session."""

    value: str

    def __post_init__(self) -> None:
        if not self.value.strip():
            raise ValueError("sandbox key must not be empty")


@dataclass(frozen=True)
class SandboxSpec:
    """Portable request for one isolated execution environment."""

    provider: str
    environment: str
    workdir: str
    timeout_seconds: int = 4 * 60 * 60
    idle_timeout_seconds: int = 20 * 60
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("sandbox provider must not be empty")
        if not self.environment.strip():
            raise ValueError("sandbox environment identity must not be empty")
        workdir = PurePosixPath(self.workdir)
        if not workdir.is_absolute() or str(workdir) in {"/", "."}:
            raise ValueError("sandbox workdir must be a non-root absolute path")
        if self.timeout_seconds < 1 or self.idle_timeout_seconds < 1:
            raise ValueError("sandbox timeouts must be positive")
        keys = [key for key, _ in self.metadata]
        if any(not key.strip() for key in keys) or len(keys) != len(set(keys)):
            raise ValueError("sandbox metadata keys must be unique and non-empty")

    def metadata_dict(self) -> dict[str, str]:
        """Return metadata as a fresh provider-facing mapping."""

        return dict(self.metadata)


@dataclass(frozen=True)
class SandboxIdentity:
    """Provider identity for a live sandbox resource."""

    provider: str
    sandbox_id: str
    environment: str

    def __post_init__(self) -> None:
        if not self.provider.strip() or not self.sandbox_id.strip():
            raise ValueError("sandbox identity requires provider and sandbox_id")
        if not self.environment.strip():
            raise ValueError("sandbox identity requires an environment")


class SandboxTeardownError(RuntimeError):
    """A retained session could not be closed before explicit replacement."""

    def __init__(self, identity: SandboxIdentity, cause: BaseException) -> None:
        self.identity = identity
        super().__init__(f"failed to close sandbox {identity.sandbox_id!r}: {cause}")


@dataclass(frozen=True)
class SandboxEvent:
    """Bounded operational event safe for callbacks and provider spools."""

    kind: SandboxEventType
    sandbox: SandboxIdentity
    timestamp_ms: int
    sequence: int = 0
    operation: str = ""
    returncode: int | None = None
    checkpoint_uri: str = ""
    message: str = ""

    def __post_init__(self) -> None:
        if self.timestamp_ms < 0 or self.sequence < 0:
            raise ValueError("sandbox event time and sequence must not be negative")

    def record(self) -> dict[str, str | int | None]:
        """Return a stable JSON-compatible record without process output."""

        return {
            "schema_version": 1,
            "type": self.kind.value,
            "timestamp_ms": self.timestamp_ms,
            "sequence": self.sequence,
            "provider": self.sandbox.provider,
            "sandbox_id": self.sandbox.sandbox_id,
            "environment": self.sandbox.environment,
            "operation": self.operation,
            "returncode": self.returncode,
            "checkpoint_uri": self.checkpoint_uri,
            "message": self.message,
        }


SandboxEventObserver = Callable[[SandboxEvent], None]


@dataclass(frozen=True)
class SandboxCapabilities:
    """Optional capabilities exposed without changing workflow authority."""

    checkpoints: bool = False
    live_output: bool = False
    secret_names: tuple[str, ...] = ()
    home_directory: str = "/root"
    observation_directory: str = "/tmp/archetype-agent-missions/live"

    def __post_init__(self) -> None:
        if any(not name.strip() for name in self.secret_names):
            raise ValueError("sandbox secret capability names must not be empty")
        if len(set(self.secret_names)) != len(self.secret_names):
            raise ValueError("sandbox secret capability names must be unique")
        home = PurePosixPath(self.home_directory)
        if not home.is_absolute() or str(home) in {"/", "."}:
            raise ValueError("sandbox home_directory must be a non-root absolute path")
        observation = PurePosixPath(self.observation_directory)
        if not observation.is_absolute() or str(observation) in {"/", "."}:
            raise ValueError("sandbox observation_directory must be a non-root absolute path")


@dataclass(frozen=True)
class ProcessRequest:
    """One process invocation inside a sandbox session."""

    argv: tuple[str, ...]
    workdir: str | None = None
    timeout_seconds: int = 900
    env: tuple[tuple[str, str], ...] = ()
    secret_names: tuple[str, ...] = ()
    close_stdin: bool = False

    def __post_init__(self) -> None:
        if not self.argv or any(not argument for argument in self.argv):
            raise ValueError("process argv must contain non-empty arguments")
        if self.timeout_seconds < 1:
            raise ValueError("process timeout_seconds must be positive")
        if self.workdir is not None and not PurePosixPath(self.workdir).is_absolute():
            raise ValueError("process workdir must be absolute when provided")
        env_keys = [key for key, _ in self.env]
        if any(not key for key in env_keys) or len(env_keys) != len(set(env_keys)):
            raise ValueError("process environment keys must be unique and non-empty")
        if any(not name.strip() for name in self.secret_names):
            raise ValueError("process secret names must not be empty")
        if len(set(self.secret_names)) != len(self.secret_names):
            raise ValueError("process secret names must be unique")

    def environment_dict(self) -> dict[str, str]:
        """Return environment entries as a fresh provider-facing mapping."""

        return dict(self.env)


@dataclass(frozen=True)
class ProcessResult:
    """Factual output from one sandbox process."""

    argv: tuple[str, ...]
    returncode: int
    stdout: str = ""
    stderr: str = ""
    trace_uri: str = ""


@dataclass(frozen=True)
class CheckpointRef:
    """Lightweight reference to an optional provider recovery point."""

    provider: str
    checkpoint_id: str
    uri: str
    created_at_ms: int
    environment: str = ""
    source_sandbox_id: str = ""
    owner_id: str = ""
    locality: CheckpointLocality = CheckpointLocality.PROVIDER
    expires_at_ms: int | None = None
    integrity: str = ""
    restorable: bool = True

    def __post_init__(self) -> None:
        if not self.provider.strip() or not self.checkpoint_id.strip() or not self.uri.strip():
            raise ValueError("checkpoint requires provider, checkpoint_id, and uri")
        if self.created_at_ms < 0:
            raise ValueError("checkpoint created_at_ms must not be negative")
        object.__setattr__(self, "locality", CheckpointLocality(self.locality))
        if self.expires_at_ms is not None and self.expires_at_ms <= self.created_at_ms:
            raise ValueError("checkpoint expiry must be after creation")
        if self.integrity and not re.fullmatch(r"sha256:[0-9a-f]{64}", self.integrity):
            raise ValueError("checkpoint integrity must be a sha256 digest")

    def expired(self, *, now_ms: int | None = None) -> bool:
        """Return whether the provider-declared recovery window has elapsed."""

        observed = int(time.time() * 1000) if now_ms is None else now_ms
        return self.expires_at_ms is not None and observed >= self.expires_at_ms


def validate_checkpoint_for_spec(
    checkpoint: CheckpointRef,
    spec: SandboxSpec,
    *,
    now_ms: int | None = None,
) -> None:
    """Reject cross-provider, cross-environment, cross-owner, and expired restore."""

    if checkpoint.provider != spec.provider:
        raise ValueError("checkpoint provider does not match sandbox spec")
    if not checkpoint.restorable:
        raise ValueError("checkpoint is marked non-restorable")
    if not checkpoint.environment:
        raise ValueError("checkpoint has no environment lineage")
    if checkpoint.environment != spec.environment:
        raise ValueError("checkpoint environment does not match sandbox spec")
    if not checkpoint.source_sandbox_id:
        raise ValueError("checkpoint has no source sandbox identity")
    owner = spec.metadata_dict().get("mission", "")
    if checkpoint.owner_id != owner:
        raise ValueError("checkpoint owner does not match sandbox spec")
    if checkpoint.expired(now_ms=now_ms):
        raise ValueError("checkpoint has expired")


def live_observation_paths(
    root: str = "/tmp/archetype-agent-missions/live",
    trace_id: str = "",
) -> dict[str, str]:
    """Return session-owned observation paths outside the target repository."""

    root_path = PurePosixPath(root)
    if not root_path.is_absolute() or str(root_path) in {"/", "."}:
        raise ValueError("live artifact root must be a non-root absolute path")
    directory = str(root_path).rstrip("/")
    if trace_id and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", trace_id):
        raise ValueError("live trace identity contains unsupported characters")
    trace_directory = f"{directory}/executions/{trace_id}" if trace_id else directory
    return {
        "directory": directory,
        "trace_directory": trace_directory,
        "status": f"{directory}/session.json",
        "events": f"{directory}/events.jsonl",
        "stdout": f"{trace_directory}/agent.stdout.log",
        "stderr": f"{trace_directory}/agent.stderr.log",
    }


@runtime_checkable
class SandboxSession(Protocol):
    """Live handle for one provider-owned filesystem and process container."""

    @property
    def identity(self) -> SandboxIdentity: ...

    @property
    def capabilities(self) -> SandboxCapabilities: ...

    async def status(self) -> SandboxStatus: ...

    async def exec(self, request: ProcessRequest) -> ProcessResult: ...

    async def checkpoint(self) -> CheckpointRef: ...

    async def close(self) -> None: ...


@runtime_checkable
class SandboxBackend(Protocol):
    """Provider adapter that creates or restores sandbox sessions."""

    name: str

    async def create(self, spec: SandboxSpec) -> SandboxSession: ...

    async def restore(self, spec: SandboxSpec, checkpoint: CheckpointRef) -> SandboxSession: ...


@runtime_checkable
class SandboxServiceProtocol(Protocol):
    """Process-local Session lifetime required by mission composition."""

    async def acquire(self, key: SandboxKey, spec: SandboxSpec) -> SandboxSession: ...

    async def restore(
        self,
        key: SandboxKey,
        spec: SandboxSpec,
        checkpoint: CheckpointRef,
    ) -> SandboxSession: ...

    def session(self, key: SandboxKey) -> SandboxSession | None: ...

    async def close(self, key: SandboxKey) -> None: ...

    async def shutdown(self) -> None: ...


__all__ = [
    "CheckpointLocality",
    "CheckpointRef",
    "live_observation_paths",
    "ProcessRequest",
    "ProcessResult",
    "SandboxBackend",
    "SandboxCapabilities",
    "SandboxEvent",
    "SandboxEventObserver",
    "SandboxEventType",
    "SandboxIdentity",
    "SandboxKey",
    "SandboxSession",
    "SandboxServiceProtocol",
    "SandboxSpec",
    "SandboxStatus",
    "SandboxTeardownError",
    "validate_checkpoint_for_spec",
]
