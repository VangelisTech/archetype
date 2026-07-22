# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Process-local lifetime management for mission sandbox sessions."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

from archetype.missions.sandboxes.contracts import (
    CheckpointRef,
    SandboxBackend,
    SandboxKey,
    SandboxSession,
    SandboxSpec,
    SandboxStatus,
    SandboxTeardownError,
    validate_checkpoint_for_spec,
)


class SandboxService:
    """Select backends and single-flight live sessions by application key.

    The registry is process-local resource state. Durable mission state lives
    in Components; a retained session never authorizes a task transition.
    """

    def __init__(self, backends: Iterable[SandboxBackend] = ()) -> None:
        self._backends: dict[str, SandboxBackend] = {}
        self._sessions: dict[SandboxKey, tuple[SandboxSpec, SandboxSession]] = {}
        self._session_ids: dict[str, SandboxKey] = {}
        self._pending: dict[
            SandboxKey,
            tuple[SandboxSpec, CheckpointRef | None, asyncio.Task[SandboxSession]],
        ] = {}
        self._lock = asyncio.Lock()
        self._accepting = True
        for backend in backends:
            self.register_backend(backend)

    def register_backend(self, backend: SandboxBackend) -> None:
        """Register one provider adapter before the service begins shutdown."""

        name = backend.name.strip()
        if not name:
            raise ValueError("sandbox backend name must not be empty")
        if not self._accepting:
            raise RuntimeError("SandboxService is shutting down")
        if name in self._backends:
            raise ValueError(f"sandbox backend {name!r} is already registered")
        self._backends[name] = backend

    def _backend(self, provider: str) -> SandboxBackend:
        try:
            return self._backends[provider]
        except KeyError as exc:
            raise ValueError(f"unknown sandbox provider: {provider!r}") from exc

    async def acquire(self, key: SandboxKey, spec: SandboxSpec) -> SandboxSession:
        """Return one live session, creating it exactly once for ``key``."""

        return await self._acquire(key, spec, checkpoint=None)

    async def restore(
        self,
        key: SandboxKey,
        spec: SandboxSpec,
        checkpoint: CheckpointRef,
    ) -> SandboxSession:
        """Explicitly replace a retained session or restore an absent key."""

        validate_checkpoint_for_spec(checkpoint, spec)
        async with self._lock:
            if not self._accepting:
                raise RuntimeError("SandboxService is shutting down")
            pending_entry = self._pending.get(key)
            if pending_entry is not None:
                pending_spec, pending_checkpoint, pending = pending_entry
                if (pending_spec, pending_checkpoint) != (spec, checkpoint):
                    raise ValueError(
                        f"sandbox key {key.value!r} is already pending with another spec"
                    )
            else:
                retained = self._sessions.get(key)
                if retained is not None:
                    retained_spec, replaced = retained
                    if retained_spec != spec:
                        raise ValueError(
                            f"sandbox key {key.value!r} is already bound to another spec"
                        )
                else:
                    replaced = None
                backend = self._backend(spec.provider)
                pending = asyncio.create_task(
                    self._create(key, spec, backend, checkpoint, replaced=replaced),
                    name=f"sandbox-restore:{key.value}",
                )
                self._pending[key] = (spec, checkpoint, pending)
        return await asyncio.shield(pending)

    async def _acquire(
        self,
        key: SandboxKey,
        spec: SandboxSpec,
        *,
        checkpoint: CheckpointRef | None,
    ) -> SandboxSession:
        async with self._lock:
            if not self._accepting:
                raise RuntimeError("SandboxService is shutting down")
            pending_entry = self._pending.get(key)
            if pending_entry is not None:
                pending_spec, pending_checkpoint, pending = pending_entry
                if (pending_spec, pending_checkpoint) != (spec, checkpoint):
                    raise ValueError(
                        f"sandbox key {key.value!r} is already pending with another spec"
                    )
            else:
                retained = self._sessions.get(key)
                replaced: SandboxSession | None = None
                if retained is not None:
                    retained_spec, session = retained
                    if retained_spec != spec:
                        raise ValueError(
                            f"sandbox key {key.value!r} is already bound to another spec"
                        )
                    status = await session.status()
                    if status is SandboxStatus.READY:
                        return session
                    if status is SandboxStatus.CLOSED:
                        self._sessions.pop(key, None)
                        self._session_ids.pop(session.identity.sandbox_id, None)
                    else:
                        replaced = session
                backend = self._backend(spec.provider)
                pending = asyncio.create_task(
                    self._create(key, spec, backend, checkpoint, replaced=replaced),
                    name=f"sandbox:{key.value}",
                )
                self._pending[key] = (spec, checkpoint, pending)
        return await asyncio.shield(pending)

    async def _create(
        self,
        key: SandboxKey,
        spec: SandboxSpec,
        backend: SandboxBackend,
        checkpoint: CheckpointRef | None,
        *,
        replaced: SandboxSession | None = None,
    ) -> SandboxSession:
        session: SandboxSession | None = None
        try:
            if replaced is not None:
                try:
                    await replaced.close()
                except Exception as exc:
                    raise SandboxTeardownError(replaced.identity, exc) from exc
                async with self._lock:
                    retained = self._sessions.get(key)
                    if retained is not None and retained[1] is replaced:
                        self._sessions.pop(key, None)
                        self._session_ids.pop(replaced.identity.sandbox_id, None)
            session = (
                await backend.create(spec)
                if checkpoint is None
                else await backend.restore(spec, checkpoint)
            )
            async with self._lock:
                if not self._accepting:
                    raise RuntimeError("SandboxService is shutting down")
                sandbox_id = session.identity.sandbox_id
                existing_key = self._session_ids.get(sandbox_id)
                if existing_key is not None and existing_key != key:
                    raise RuntimeError(f"duplicate live sandbox id: {sandbox_id}")
                self._sessions[key] = (spec, session)
                self._session_ids[sandbox_id] = key
            return session
        except BaseException:
            if session is not None:
                await session.close()
            raise
        finally:
            async with self._lock:
                current = asyncio.current_task()
                entry = self._pending.get(key)
                if entry is not None and entry[2] is current:
                    self._pending.pop(key, None)

    def session(self, key: SandboxKey) -> SandboxSession | None:
        """Return a process-local handle without implying durable recovery."""

        retained = self._sessions.get(key)
        return retained[1] if retained is not None else None

    async def close(self, key: SandboxKey) -> None:
        """Close and forget one live session; a missing key is a no-op."""

        async with self._lock:
            retained = self._sessions.pop(key, None)
            if retained is not None:
                self._session_ids.pop(retained[1].identity.sandbox_id, None)
        if retained is not None:
            await retained[1].close()

    async def shutdown(self) -> None:
        """Stop acquisition, await creators, and attempt every session close."""

        async with self._lock:
            if not self._accepting and not self._sessions and not self._pending:
                return
            self._accepting = False
            pending = tuple(entry[2] for entry in self._pending.values())
        if pending:
            await asyncio.gather(
                *(asyncio.shield(task) for task in pending), return_exceptions=True
            )
        async with self._lock:
            sessions = tuple(session for _, session in self._sessions.values())
            self._sessions.clear()
            self._session_ids.clear()
        results = await asyncio.gather(
            *(session.close() for session in sessions),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise BaseExceptionGroup(
                f"failed to close {len(failures)} sandbox session(s)", failures
            )


__all__ = ["SandboxService"]
