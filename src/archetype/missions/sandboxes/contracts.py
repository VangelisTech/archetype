# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provider-neutral contracts for mission sandbox resources."""

from __future__ import annotations

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


@dataclass(frozen=True)
class SandboxCapabilities:
    """Optional capabilities exposed without changing workflow authority."""

    checkpoints: bool = False
    secret_names: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if any(not name.strip() for name in self.secret_names):
            raise ValueError("sandbox secret capability names must not be empty")
        if len(set(self.secret_names)) != len(self.secret_names):
            raise ValueError("sandbox secret capability names must be unique")


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


@dataclass(frozen=True)
class CheckpointRef:
    """Lightweight reference to an optional provider recovery point."""

    provider: str
    checkpoint_id: str
    uri: str
    created_at_ms: int
    restorable: bool = True

    def __post_init__(self) -> None:
        if not self.provider.strip() or not self.checkpoint_id.strip() or not self.uri.strip():
            raise ValueError("checkpoint requires provider, checkpoint_id, and uri")
        if self.created_at_ms < 0:
            raise ValueError("checkpoint created_at_ms must not be negative")


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

    async def close(self, key: SandboxKey) -> None: ...

    async def shutdown(self) -> None: ...


__all__ = [
    "CheckpointRef",
    "ProcessRequest",
    "ProcessResult",
    "SandboxBackend",
    "SandboxCapabilities",
    "SandboxIdentity",
    "SandboxKey",
    "SandboxSession",
    "SandboxServiceProtocol",
    "SandboxSpec",
    "SandboxStatus",
]
