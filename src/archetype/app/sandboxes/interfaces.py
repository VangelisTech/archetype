# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provider-neutral ports for isolated episode execution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from archetype.app.sandboxes.models import ValidatorSpec


@runtime_checkable
class iSandboxSession(Protocol):
    """One live, provider-owned sandbox episode."""

    @property
    def sandbox_id(self) -> str: ...

    async def run_attempt(
        self,
        *,
        prompt: str,
        validators: Sequence[ValidatorSpec | dict[str, Any]],
        step_name: str,
        attempt_index: int,
        idempotency_key: str,
        previous_session_id: str = "",
        previous_validator_details: Sequence[dict[str, Any]] = (),
        correlation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    async def close(self) -> None: ...


@runtime_checkable
class iSandboxBackend(Protocol):
    """Lifecycle adapter implemented by Modal, Apple Container, or another provider."""

    name: str

    async def create(self, spec: Any) -> iSandboxSession: ...
    async def restore(self, spec: Any, checkpoint_ref: str) -> iSandboxSession: ...
    async def resume(self, spec: Any, checkpoint_ref: str) -> iSandboxSession: ...
    async def authenticate(self, spec: Any) -> None: ...


@runtime_checkable
class iSandboxService(Protocol):
    """Own provider selection and live sandbox-handle lifetime."""

    def register_backend(self, backend: iSandboxBackend) -> None: ...
    async def create(self, provider: str, spec: Any) -> iSandboxSession: ...
    async def restore(self, provider: str, spec: Any, checkpoint_ref: str) -> iSandboxSession: ...
    async def resume(self, provider: str, spec: Any, checkpoint_ref: str) -> iSandboxSession: ...
    async def authenticate(self, provider: str, spec: Any) -> None: ...
    def session(self, sandbox_id: str) -> iSandboxSession | None: ...
    async def close(self, sandbox_id: str) -> None: ...
    async def shutdown(self) -> None: ...
