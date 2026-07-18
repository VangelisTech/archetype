# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Sandbox lifecycle coordinator."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Any

from archetype.app.sandboxes.interfaces import iSandboxBackend, iSandboxSession


class SandboxService:
    """Select providers and retain live handles until explicit finalization.

    Provider checkpoints are durable recovery objects; live handles are not.
    The service therefore owns only process lifetime and never treats its
    in-memory registry as mission state.
    """

    def __init__(self, backends: Iterable[iSandboxBackend] = ()) -> None:
        self._backends: dict[str, iSandboxBackend] = {}
        self._sessions: dict[str, iSandboxSession] = {}
        self._lock = asyncio.Lock()
        self._accepting = True
        for backend in backends:
            self.register_backend(backend)

    def register_backend(self, backend: iSandboxBackend) -> None:
        name = backend.name.strip()
        if not name:
            raise ValueError("sandbox backend name must not be empty")
        if name in self._backends:
            raise ValueError(f"sandbox backend {name!r} is already registered")
        self._backends[name] = backend

    def _backend(self, provider: str) -> iSandboxBackend:
        try:
            return self._backends[provider]
        except KeyError as exc:
            raise ValueError(f"unknown sandbox provider: {provider!r}") from exc

    async def _ensure_accepting(self) -> None:
        async with self._lock:
            if not self._accepting:
                raise RuntimeError("SandboxService is shutting down")

    async def _retain(self, session: iSandboxSession) -> iSandboxSession:
        async with self._lock:
            if not self._accepting:
                await session.close()
                raise RuntimeError("SandboxService is shutting down")
            existing = self._sessions.get(session.sandbox_id)
            if existing is not None and existing is not session:
                await session.close()
                raise RuntimeError(f"duplicate live sandbox id: {session.sandbox_id}")
            self._sessions[session.sandbox_id] = session
        return session

    async def create(self, provider: str, spec: Any) -> iSandboxSession:
        await self._ensure_accepting()
        return await self._retain(await self._backend(provider).create(spec))

    async def restore(self, provider: str, spec: Any, checkpoint_ref: str) -> iSandboxSession:
        await self._ensure_accepting()
        return await self._retain(await self._backend(provider).restore(spec, checkpoint_ref))

    async def resume(self, provider: str, spec: Any, checkpoint_ref: str) -> iSandboxSession:
        await self._ensure_accepting()
        return await self._retain(await self._backend(provider).resume(spec, checkpoint_ref))

    async def authenticate(self, provider: str, spec: Any) -> None:
        await self._backend(provider).authenticate(spec)

    def session(self, sandbox_id: str) -> iSandboxSession | None:
        """Return a process-local live handle, never durable mission state."""

        return self._sessions.get(sandbox_id)

    async def close(self, sandbox_id: str) -> None:
        async with self._lock:
            session = self._sessions.pop(sandbox_id, None)
        if session is not None:
            await session.close()

    async def shutdown(self) -> None:
        async with self._lock:
            self._accepting = False
            sessions = tuple(self._sessions.values())
            self._sessions.clear()
        results = await asyncio.gather(
            *(session.close() for session in sessions), return_exceptions=True
        )
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise BaseExceptionGroup(
                f"failed to close {len(failures)} sandbox session(s)", failures
            )
