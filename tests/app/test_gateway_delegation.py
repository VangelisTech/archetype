# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic delegation evidence for the untrusted gateway adapter."""

from unittest.mock import AsyncMock, call

import pytest
from uuid_utils import uuid7

from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.gateway.service import CommandGateway
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.world.models import (
    AddComponents,
    AddProcessor,
    ComponentTypeRef,
    ComponentValue,
    DiscoverWorlds,
    OpenWorldReadonly,
    RemoveComponents,
    RemoveProcessor,
    ResumeWorld,
    Update,
)

pytestmark = pytest.mark.asyncio


class Pos(Component):
    x: int = 0


def _gateway(application: AsyncMock, dispatcher: AsyncMock) -> CommandGateway:
    return CommandGateway(
        application,
        dispatcher,
        AsyncMock(),
        AsyncMock(),
        target_tick_for_world=lambda _world_id: 17,
    )


async def test_direct_mutations_construct_exact_operations_for_actor_aware_dispatch():
    application = AsyncMock()
    dispatcher = AsyncMock()
    gateway = _gateway(application, dispatcher)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    components = [Pos(x=3)]
    component_types = [Pos]
    processor = object()

    await gateway.update_entity(ctx, "world-1", 7, components)
    await gateway.add_components(ctx, "world-1", 7, components)
    await gateway.remove_components(ctx, "world-1", 7, component_types)
    await gateway.add_processor(ctx, "world-1", processor)
    await gateway.remove_processor(ctx, "world-1", type(processor))

    expected_value = (ComponentValue.from_component(components[0]),)
    dispatcher.apply_as.assert_has_awaits(
        [
            call(
                ctx,
                Update(world_id="world-1", entity_id=7, components=expected_value),
            ),
            call(
                ctx,
                AddComponents(world_id="world-1", entity_id=7, components=expected_value),
            ),
            call(
                ctx,
                RemoveComponents(
                    world_id="world-1",
                    entity_id=7,
                    component_types=(ComponentTypeRef.from_type(Pos),),
                ),
            ),
            call(ctx, AddProcessor(world_id="world-1", processor=processor)),
            call(
                ctx,
                RemoveProcessor(world_id="world-1", processor_type=type(processor)),
            ),
        ]
    )
    assert application.mock_calls == []


async def test_discovery_and_resume_construct_exact_operations_for_dispatch():
    application = AsyncMock()
    dispatcher = AsyncMock()
    gateway = _gateway(application, dispatcher)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    storage = StorageConfig()
    discovered = [object()]
    readonly = object()
    resumed = object()
    dispatcher.apply_as.side_effect = [discovered, readonly, resumed]

    assert await gateway.discover_worlds(ctx, storage) is discovered
    assert await gateway.open_world_readonly(ctx, storage, "world-1") is readonly
    assert await gateway.resume_world(ctx, storage, "world-1") is resumed

    dispatcher.apply_as.assert_has_awaits(
        [
            call(ctx, DiscoverWorlds(storage_config=storage)),
            call(
                ctx,
                OpenWorldReadonly(storage_config=storage, world_id="world-1"),
            ),
            call(ctx, ResumeWorld(storage_config=storage, world_id="world-1")),
        ]
    )
    assert application.mock_calls == []
