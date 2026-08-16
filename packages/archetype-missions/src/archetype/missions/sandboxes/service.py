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
        self._closing: dict[SandboxKey, asyncio.Task[None]] = {}
        self._cleanup_sessions: dict[int, SandboxSession] = {}
        self._lifecycle_generations: dict[SandboxKey, int] = {}
        self._lock = asyncio.Lock()
        self._shutdown_lock = asyncio.Lock()
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
        while True:
            async with self._lock:
                if not self._accepting:
                    raise RuntimeError("SandboxService is shutting down")
                closing = self._closing.get(key)
                if closing is None:
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
                        pending = self._schedule_create(
                            key,
                            spec,
                            backend,
                            checkpoint,
                            replaced=replaced,
                        )
            if closing is None:
                session = await asyncio.shield(pending)
                ready, closing = await self._is_current_ready(key, session)
                if ready:
                    return session
            if closing is not None:
                await asyncio.gather(asyncio.shield(closing), return_exceptions=True)

    async def _acquire(
        self,
        key: SandboxKey,
        spec: SandboxSpec,
        *,
        checkpoint: CheckpointRef | None,
    ) -> SandboxSession:
        while True:
            candidate: SandboxSession | None = None
            candidate_generation: int | None = None
            pending: asyncio.Task[SandboxSession] | None = None
            async with self._lock:
                if not self._accepting:
                    raise RuntimeError("SandboxService is shutting down")
                closing = self._closing.get(key)
                if closing is None:
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
                            candidate = session
                            candidate_generation = self._lifecycle_generations.get(key, 0)
                        else:
                            backend = self._backend(spec.provider)
                            pending = self._schedule_create(
                                key,
                                spec,
                                backend,
                                checkpoint,
                                replaced=replaced,
                            )
            if closing is not None:
                await asyncio.gather(asyncio.shield(closing), return_exceptions=True)
                continue
            if candidate is not None:
                status = await candidate.status()
                async with self._lock:
                    if not self._accepting:
                        raise RuntimeError("SandboxService is shutting down")
                    closing = self._closing.get(key)
                    pending_entry = self._pending.get(key)
                    retained = self._sessions.get(key)
                    if (
                        closing is None
                        and pending_entry is None
                        and retained is not None
                        and retained[1] is candidate
                        and self._lifecycle_generations.get(key, 0) == candidate_generation
                    ):
                        if status is SandboxStatus.READY:
                            return candidate
                        if status is SandboxStatus.CLOSED:
                            self._sessions.pop(key, None)
                            self._session_ids.pop(candidate.identity.sandbox_id, None)
                            replaced = None
                        else:
                            replaced = candidate
                        backend = self._backend(spec.provider)
                        pending = self._schedule_create(
                            key,
                            spec,
                            backend,
                            checkpoint,
                            replaced=replaced,
                        )
                if closing is not None:
                    await asyncio.gather(asyncio.shield(closing), return_exceptions=True)
                    continue
                if pending is None:
                    continue
            if closing is None:
                assert pending is not None
                session = await asyncio.shield(pending)
                ready, closing = await self._is_current_ready(key, session)
                if ready:
                    return session
            if closing is not None:
                await asyncio.gather(asyncio.shield(closing), return_exceptions=True)

    def _schedule_create(
        self,
        key: SandboxKey,
        spec: SandboxSpec,
        backend: SandboxBackend,
        checkpoint: CheckpointRef | None,
        *,
        replaced: SandboxSession | None,
    ) -> asyncio.Task[SandboxSession]:
        """Register one creator while the service registry lock is held."""

        operation = "sandbox-restore" if checkpoint is not None else "sandbox"
        pending = asyncio.create_task(
            self._create(key, spec, backend, checkpoint, replaced=replaced),
            name=f"{operation}:{key.value}",
        )
        pending.add_done_callback(self._consume_creator_result)
        self._pending[key] = (spec, checkpoint, pending)
        self._advance_lifecycle(key)
        return pending

    async def _is_current_ready(
        self,
        key: SandboxKey,
        session: SandboxSession,
    ) -> tuple[bool, asyncio.Task[None] | None]:
        async with self._lock:
            if not self._accepting:
                raise RuntimeError("SandboxService is shutting down")
            closing = self._closing.get(key)
            retained = self._sessions.get(key)
            if (
                closing is not None
                or self._pending.get(key) is not None
                or retained is None
                or retained[1] is not session
            ):
                return False, closing
            generation = self._lifecycle_generations.get(key, 0)

        status = await session.status()

        async with self._lock:
            if not self._accepting:
                raise RuntimeError("SandboxService is shutting down")
            closing = self._closing.get(key)
            retained = self._sessions.get(key)
            current = (
                closing is None
                and self._pending.get(key) is None
                and retained is not None
                and retained[1] is session
                and self._lifecycle_generations.get(key, 0) == generation
            )
            if current and status is not SandboxStatus.READY:
                raise RuntimeError(
                    f"sandbox {session.identity.sandbox_id!r} became non-ready: {status.value}"
                )
            return current, closing

    def _advance_lifecycle(self, key: SandboxKey) -> None:
        self._lifecycle_generations[key] = self._lifecycle_generations.get(key, 0) + 1

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
        cleanup_id: int | None = None
        cleanup_deferred = False
        registered = False
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
            cleanup_id = id(session)
            # Claim cleanup ownership before another await can cancel this
            # creator or let shutdown observe an unowned provider handle.
            self._cleanup_sessions[cleanup_id] = session
            async with self._lock:
                if not self._accepting:
                    cleanup_deferred = True
                    raise RuntimeError("SandboxService is shutting down")
                sandbox_id = session.identity.sandbox_id
                existing_key = self._session_ids.get(sandbox_id)
                if existing_key is not None and existing_key != key:
                    raise RuntimeError(f"duplicate live sandbox id: {sandbox_id}")
                self._sessions[key] = (spec, session)
                self._session_ids[sandbox_id] = key
                self._cleanup_sessions.pop(cleanup_id, None)
                registered = True
            return session
        except BaseException:
            if session is not None and not registered and not cleanup_deferred:
                await session.close()
                async with self._lock:
                    if cleanup_id is not None:
                        current = self._cleanup_sessions.get(cleanup_id)
                        if current is session:
                            self._cleanup_sessions.pop(cleanup_id, None)
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
            closing = self._closing.get(key)
            if closing is None:
                self._advance_lifecycle(key)
                closing = asyncio.create_task(
                    self._close_registered(key),
                    name=f"sandbox-close:{key.value}",
                )
                closing.add_done_callback(self._consume_task_result)
                self._closing[key] = closing
        await asyncio.shield(closing)

    async def _close_registered(self, key: SandboxKey) -> None:
        try:
            async with self._lock:
                pending_entry = self._pending.get(key)
            if pending_entry is not None:
                await asyncio.gather(
                    asyncio.shield(pending_entry[2]),
                    return_exceptions=True,
                )
            async with self._lock:
                retained = self._sessions.get(key)
            if retained is not None:
                await retained[1].close()
                async with self._lock:
                    current = self._sessions.get(key)
                    if current is not None and current[1] is retained[1]:
                        self._sessions.pop(key, None)
                        self._session_ids.pop(retained[1].identity.sandbox_id, None)
        finally:
            async with self._lock:
                current = asyncio.current_task()
                if self._closing.get(key) is current:
                    self._closing.pop(key, None)

    async def shutdown(self) -> None:
        """Stop acquisition, await creators, and attempt every session close."""

        async with self._shutdown_lock:
            await self._shutdown()

    async def _shutdown(self) -> None:
        async with self._lock:
            if (
                not self._accepting
                and not self._sessions
                and not self._pending
                and not self._closing
                and not self._cleanup_sessions
            ):
                return
            self._accepting = False
            pending = tuple(entry[2] for entry in self._pending.values())
            closing = tuple(self._closing.values())
        if pending or closing:
            await asyncio.gather(
                *(asyncio.shield(task) for task in (*pending, *closing)),
                return_exceptions=True,
            )
        async with self._lock:
            keys = tuple(self._sessions)
            cleanup = tuple(self._cleanup_sessions.items())
        registered_results = await asyncio.gather(
            *(self.close(key) for key in keys),
            return_exceptions=True,
        )
        cleanup_results = await asyncio.gather(
            *(session.close() for _cleanup_id, session in cleanup),
            return_exceptions=True,
        )
        async with self._lock:
            for (cleanup_id, session), result in zip(
                cleanup,
                cleanup_results,
                strict=True,
            ):
                if isinstance(result, BaseException):
                    continue
                current = self._cleanup_sessions.get(cleanup_id)
                if current is session:
                    self._cleanup_sessions.pop(cleanup_id, None)
        failures = [
            result
            for result in (*registered_results, *cleanup_results)
            if isinstance(result, BaseException)
        ]
        if failures:
            raise BaseExceptionGroup(
                f"failed to close {len(failures)} sandbox session(s)", failures
            )

    @staticmethod
    def _consume_task_result(task: asyncio.Task[None]) -> None:
        """Retrieve background close failures when every waiter was cancelled."""

        if not task.cancelled():
            task.exception()

    @staticmethod
    def _consume_creator_result(task: asyncio.Task[SandboxSession]) -> None:
        """Retrieve creator failures even when every shielded waiter was cancelled."""

        if not task.cancelled():
            task.exception()


__all__ = ["SandboxService"]
