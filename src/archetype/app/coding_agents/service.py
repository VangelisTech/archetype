# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Repository-mission coordinator over mission and sandbox ports."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

from archetype.app.missions.interfaces import iMissionService
from archetype.app.sandboxes.interfaces import iSandboxService, iSandboxSession


class CodingAgentService:
    """Bind live sandbox sessions to durable mission state.

    The mapping is deliberately a cache. Checkpoints and mission components
    are the recovery authority; callers reattach with :meth:`restore_episode`
    after a process restart.
    """

    def __init__(self, missions: iMissionService, sandboxes: iSandboxService) -> None:
        self._missions = missions
        self._sandboxes = sandboxes
        self._episodes: dict[str, iSandboxSession] = {}
        self._lock = asyncio.Lock()

    async def _retain(self, mission_id: str, session: iSandboxSession) -> str:
        if not mission_id.strip():
            await session.close()
            raise ValueError("mission_id must not be empty")
        async with self._lock:
            if mission_id in self._episodes:
                await session.close()
                raise ValueError(f"coding-agent mission {mission_id!r} is already active")
            self._episodes[mission_id] = session
        return session.sandbox_id

    async def start_episode(self, mission_id: str, provider: str, spec: Any) -> str:
        return await self._retain(mission_id, await self._sandboxes.create(provider, spec))

    async def restore_episode(
        self,
        mission_id: str,
        provider: str,
        spec: Any,
        checkpoint_ref: str,
        *,
        resume_agent: bool = False,
    ) -> str:
        if not checkpoint_ref:
            raise ValueError("checkpoint_ref must not be empty")
        restore = self._sandboxes.resume if resume_agent else self._sandboxes.restore
        return await self._retain(mission_id, await restore(provider, spec, checkpoint_ref))

    async def run_tick(
        self, mission_id: str, row: Mapping[str, Any], *, tick: int
    ) -> dict[str, Any]:
        async with self._lock:
            session = self._episodes.get(mission_id)
        if session is None:
            raise KeyError(f"coding-agent mission {mission_id!r} has no live sandbox")
        request = self._missions.prepare_attempt(row, tick=tick)
        if request is None:
            return dict(row)
        outcome = await session.run_attempt(
            prompt=request.prompt,
            validators=request.validators,
            step_name=request.step_name,
            attempt_index=request.attempt_index,
            idempotency_key=request.idempotency_key,
            previous_session_id=request.previous_session_id,
            previous_validator_details=request.previous_validator_details,
            correlation=request.correlation,
        )
        return self._missions.apply_attempt(row, request, outcome)

    async def close_episode(self, mission_id: str) -> None:
        async with self._lock:
            session = self._episodes.pop(mission_id, None)
        if session is not None:
            await self._sandboxes.close(session.sandbox_id)
