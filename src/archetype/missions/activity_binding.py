# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""One Mission Activity binding for author and critic work."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from archetype.core.interfaces import CommittedTickReceipt
from archetype.world.simulation import RequiredProjector

from .activity_world import MissionAuthorActivityBinding
from .critic_activity_world import MissionCriticActivityBinding


class MissionActivityWorker:
    """Run author and critic Activity workers outside the tick lock."""

    def __init__(
        self,
        author: MissionAuthorActivityBinding,
        critic: MissionCriticActivityBinding,
    ) -> None:
        self._author = author.worker
        self._critic = critic.worker

    async def run_once(self) -> bool:
        author = await self._author.run_once()
        critic = await self._critic.run_once()
        return author or critic

    async def run_until_idle(self) -> bool:
        author = await self._author.run_until_idle()
        critic = await self._critic.run_until_idle()
        return author or critic


class MissionActivityBinding:
    """Bind one world to exact author and critic Activity settlement."""

    def __init__(
        self,
        *,
        world_id: str,
        author: MissionAuthorActivityBinding,
        critic: MissionCriticActivityBinding,
        close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if not world_id.strip():
            raise ValueError("mission Activity binding requires a world identity")
        if author.world_id != world_id or critic.world_id != world_id:
            raise ValueError("mission Activity workers must bind the exact same world")
        self.world_id = world_id
        self.author = author
        self.critic = critic
        self.worker = MissionActivityWorker(author, critic)
        self.required_projector = RequiredProjector(
            consumer_name="missions.activities",
            project=self._project,
        )
        self._close = close
        self._closed = False

    async def _project(self, receipt: CommittedTickReceipt) -> None:
        await self.author.projector.project(receipt)
        await self.critic.projector.project(receipt)

    def required_projector_for(self, world_id: str) -> RequiredProjector | None:
        return self.required_projector if str(world_id) == self.world_id else None

    async def has_unsettled_work(self, world_id: str) -> bool:
        if str(world_id) != self.world_id:
            return False
        return await self.author.has_unsettled_work(
            world_id
        ) or await self.critic.has_unsettled_work(world_id)

    async def aclose(self) -> None:
        if self._closed:
            return
        if self._close is not None:
            await self._close()
        self._closed = True


__all__ = ["MissionActivityBinding", "MissionActivityWorker"]
