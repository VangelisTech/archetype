# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Composed PR4 loopback and dependency-ordered cleanup contracts."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from uuid_utils import uuid7

from archetype.commands.models import ActorCtx
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.errors import RuntimeShutdownError
from archetype.runtime.runtime import _runtime_world_for_resources
from archetype.runtime_resources import RuntimeCloseState, RuntimeResources
from archetype.storage.config import ControlCatalogConfig
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
from archetype.world.models import GetWorldInfo


class LoopbackValue(Component):
    value: int = 0


@pytest.mark.asyncio
async def test_source_loopback_receipt_proves_shared_owner_dispatch_and_cleanup(
    tmp_path: Path,
) -> None:
    resources = build_runtime_resources(
        RuntimeBootstrapConfig(
            control_catalog_config=ControlCatalogConfig(
                catalog_dir=tmp_path / "catalogs",
            ),
        )
    )
    world = _runtime_world_for_resources(
        resources,
        "pr4-source-loopback",
        storage=StorageConfig(
            uri=str(tmp_path / "world"),
            namespace="pr4_source_loopback",
        ),
    )
    reservation = world._reservation  # noqa: SLF001 - exact shared-owner oracle
    dispatcher = resources.dispatcher

    try:
        entity_id = await world.spawn(LoopbackValue(value=7))
        await world.step()
        info = await dispatcher.apply_as(
            ActorCtx(id=uuid7(), roles={"admin"}),
            GetWorldInfo(world_id=world.world_id),
        )
        rows = (await world.query(LoopbackValue)).to_pylist()

        receipt = {
            "shared_dispatcher": world._dispatcher is dispatcher,  # noqa: SLF001
            "world_id": str(info.world_id),
            "entity_id": entity_id,
            "tick": info.tick,
            "values": [row[f"{LoopbackValue.get_prefix()}value"] for row in rows],
        }
        assert receipt == {
            "shared_dispatcher": True,
            "world_id": str(world.world_id),
            "entity_id": entity_id,
            "tick": 1,
            "values": [7],
        }

        await world.shutdown()
        assert reservation.released
    finally:
        await resources.aclose()

    assert resources.close_state is RuntimeCloseState.CLOSED
    with pytest.raises(RuntimeError, match="not accepting work"):
        await dispatcher.apply(GetWorldInfo(world_id=info.world_id))


class _Dispatcher:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    @asynccontextmanager
    async def _admit_runtime_operation(
        self,
        continuation: Callable[[], bool],
    ) -> AsyncIterator[None]:
        del continuation
        yield

    def request_stop(self) -> None:
        pass

    async def stop_admission(self) -> None:
        self._events.append("admission")

    async def wait_drained(self) -> None:
        self._events.append("drain")


class _Audit:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def shutdown(self) -> None:
        self._events.append("audit")


class _Storage:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def shutdown(self) -> None:
        self._events.append("storage")


@pytest.mark.asyncio
async def test_injected_mission_cleanup_failure_retries_before_storage_close() -> None:
    events: list[str] = []
    failure = RuntimeError("mission cleanup unavailable")
    attempts = 0

    async def close_mission() -> None:
        nonlocal attempts
        attempts += 1
        events.append(f"mission:{attempts}")
        if attempts == 1:
            raise failure

    resources = RuntimeResources(
        dispatcher=_Dispatcher(events),
        audit=_Audit(events),
        storage=_Storage(events),
        owns_storage=True,
    )
    mission = resources.reserve_owner("mission:loopback", phase="workflow-handles")
    mission.bind(object(), close=close_mission)

    with pytest.raises(RuntimeShutdownError) as raised:
        await resources.aclose()

    assert raised.value.phase == "workflow-handles"
    assert raised.value.failures[0].owner == "mission:loopback"
    assert raised.value.failures[0].cause is failure
    assert events == ["admission", "drain", "mission:1"]
    assert resources.close_state is RuntimeCloseState.CLOSING_RETRYABLE

    await resources.aclose()

    assert events == [
        "admission",
        "drain",
        "mission:1",
        "mission:2",
        "audit",
        "storage",
    ]
    assert mission.released
    assert resources.close_state is RuntimeCloseState.CLOSED
