# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic delegation evidence for direct gateway mutations."""

from unittest.mock import AsyncMock

import pytest
from uuid_utils import uuid7

from archetype.app.gateway.auth import guard
from archetype.app.gateway.auth.guard import reset_daily_tokens
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.gateway.service import CommandGateway

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _reset_gateway_quotas():
    guard._tick_counters.clear()
    reset_daily_tokens()
    yield
    guard._tick_counters.clear()
    reset_daily_tokens()


async def test_direct_mutations_authorize_then_delegate_without_owning_workflow():
    application = AsyncMock()
    gateway = CommandGateway(application, target_tick_for_world=lambda _world_id: 17)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    components = [object()]
    component_types = [type]
    processor = object()

    await gateway.update_entity(ctx, "world-1", 7, components)
    await gateway.add_components(ctx, "world-1", 7, components)
    await gateway.remove_components(ctx, "world-1", 7, component_types)
    await gateway.add_processor(ctx, "world-1", processor)
    await gateway.remove_processor(ctx, "world-1", type(processor))

    application.update_entity.assert_awaited_once_with("world-1", 7, components)
    application.add_components.assert_awaited_once_with("world-1", 7, components)
    application.remove_components.assert_awaited_once_with("world-1", 7, component_types)
    application.add_processor.assert_awaited_once_with("world-1", processor)
    application.remove_processor.assert_awaited_once_with("world-1", type(processor))


async def test_discovery_and_resume_delegate_to_the_application_boundary():
    application = AsyncMock()
    gateway = CommandGateway(application, target_tick_for_world=lambda _world_id: 17)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    storage = object()
    discovered = [object()]
    readonly = object()
    resumed = object()
    application.discover_worlds.return_value = discovered
    application.open_world_readonly.return_value = readonly
    application.resume_world.return_value = resumed

    assert await gateway.discover_worlds(ctx, storage) is discovered
    assert await gateway.open_world_readonly(ctx, storage, "world-1") is readonly
    assert await gateway.resume_world(ctx, storage, "world-1") is resumed

    application.discover_worlds.assert_awaited_once_with(storage)
    application.open_world_readonly.assert_awaited_once_with(storage, "world-1")
    application.resume_world.assert_awaited_once_with(storage, "world-1")
