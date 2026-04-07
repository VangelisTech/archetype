# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""World routes.

Mutations (create, remove, fork) flow through the CommandBroker via
CommandService so they get RBAC validation, audit history, and the
broker's asyncio.Lock serialization for free.
"""

from fastapi import APIRouter, Depends, HTTPException
from uuid_utils import UUID

from archetype.api.deps import get_actor_ctx, get_command_service, get_world_service
from archetype.api.models import CreateWorldRequest, ForkWorldRequest, WorldResponse
from archetype.app.auth.models import ActorCtx
from archetype.app.command_service import CommandService
from archetype.app.models import Command, CommandType
from archetype.app.world_service import WorldService

router = APIRouter(prefix="/worlds", tags=["worlds"])


@router.post("", response_model=WorldResponse)
async def create_world(
    req: CreateWorldRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    cmd = Command(
        type=CommandType.CREATE_WORLD,
        tick=0,
        payload={
            "config": {"name": req.name},
            "storage_uri": req.storage_uri,
            "namespace": req.namespace,
        },
    )
    await cs.submit("__global__", cmd, ctx)
    # Apply immediately — world lifecycle commands are not tick-scheduled.
    world = await cs.apply_world_lifecycle(cmd)
    return WorldResponse(
        world_id=str(world.world_id),
        name=getattr(world, "name", None),
        tick=getattr(world, "tick", 0),
        entity_count=getattr(world, "entity_count", 0),
    )


@router.get("")
async def list_worlds(ws: WorldService = Depends(get_world_service)):
    worlds = ws.list_worlds()
    return [
        WorldResponse(
            world_id=str(w.world_id),
            name=w.name,
            tick=w.tick,
            entity_count=w.entity_count,
        )
        for w in worlds
    ]


@router.get("/{world_id}", response_model=WorldResponse)
async def get_world(world_id: str, ws: WorldService = Depends(get_world_service)):
    try:
        wid = UUID(world_id)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid UUID: {world_id}") from None
    try:
        world = ws.get_world(wid)
        return WorldResponse(
            world_id=str(world.world_id),
            name=getattr(world, "name", None),
            tick=getattr(world, "tick", 0),
            entity_count=getattr(world, "entity_count", 0),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None


@router.delete("/{world_id}")
async def remove_world(
    world_id: str,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    cmd = Command(
        type=CommandType.DESTROY_WORLD,
        tick=0,
        payload={"world_id": world_id},
    )
    await cs.submit("__global__", cmd, ctx)
    await cs.apply_world_lifecycle(cmd)
    return {"status": "removed", "world_id": world_id}


@router.post("/{world_id}/fork", response_model=WorldResponse)
async def fork_world(
    world_id: str,
    req: ForkWorldRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    cmd = Command(
        type=CommandType.FORK_WORLD,
        tick=0,
        payload={
            "source_world_id": world_id,
            "config": {"name": req.name},
        },
    )
    await cs.submit("__global__", cmd, ctx)
    try:
        new_world = await cs.apply_world_lifecycle(cmd)
    except (KeyError, TypeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from None

    return WorldResponse(
        world_id=str(new_world.world_id),
        name=getattr(new_world, "name", None),
        tick=getattr(new_world, "tick", 0),
        entity_count=getattr(new_world, "entity_count", 0),
    )
