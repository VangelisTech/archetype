# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Release contracts for Mission identity and disabled workflow restore."""

from __future__ import annotations

import pytest

from archetype.missions import (
    AgentMissionConfig,
    AgentTask,
    CommandValidator,
    Mission,
    mission_episode_id,
)
from archetype.missions.sandboxes import CheckpointRef
from archetype.missions.service import MissionService


class _World:
    active_world_id = "world-release"
    world_id = "world-release"

    def __init__(self) -> None:
        self.reserved: list[tuple[int, tuple[object, ...]]] = []
        self.next_id = 100

    async def reserve_ids(self, n: int) -> list[int]:
        return list(range(17, 17 + n))

    async def spawn_reserved(self, entity_id: int, *components: object) -> None:
        self.reserved.append((entity_id, components))

    async def spawn(self, *components: object) -> int:
        del components
        self.next_id += 1
        return self.next_id


class _Backend:
    name = "modal"


class _Sandboxes:
    def __init__(self) -> None:
        self.restore_calls = 0

    async def restore(self, *args: object) -> object:
        del args
        self.restore_calls += 1
        raise AssertionError("workflow restore must fail before provider I/O")


class _Redactor:
    policy_id = "test-redaction"


class _Worker:
    async def run_once(self) -> bool:
        return False

    async def run_until_idle(self) -> bool:
        return False


class _Activity:
    world_id = "world-release"
    worker = _Worker()

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_submit_persists_and_returns_real_episode_identity_while_restore_fails_closed() -> (
    None
):
    world = _World()
    sandboxes = _Sandboxes()

    async def activity_factory(world_id: str) -> _Activity:
        assert world_id == world.world_id
        return _Activity()

    service = MissionService(
        world_factory=lambda *args, **kwargs: world,
        name="release-contract",
        config=AgentMissionConfig(
            sandbox_backend=_Backend(),  # type: ignore[arg-type]
            sandbox_environment="modal-agent://sha256:test",
        ),
        sandbox_service=sandboxes,  # type: ignore[arg-type]
        redaction_service=_Redactor(),  # type: ignore[arg-type]
        cleanup_factory=None,  # type: ignore[arg-type]
        activity_factory=activity_factory,
    )
    submitted = await service.submit(
        repository="owner/repository",
        branch="agent/release",
        tasks=(
            AgentTask(
                name="implementation",
                prompt="Implement the release fix.",
                validators=(CommandValidator(name="tests", command=("pytest", "-q")),),
            ),
        ),
    )

    expected = mission_episode_id(world.world_id, submitted.mission_id)
    mission = next(
        component
        for _entity_id, components in world.reserved
        for component in components
        if isinstance(component, Mission)
    )
    assert submitted.episode_id == expected
    assert mission.episode_id == expected

    with pytest.raises(NotImplementedError, match="would otherwise ignore"):
        await service.restore_sandbox(
            submitted,
            CheckpointRef(
                provider="modal",
                checkpoint_id="im-test",
                uri="modal-image://im-test",
                created_at_ms=1,
            ),
        )
    assert sandboxes.restore_calls == 0
