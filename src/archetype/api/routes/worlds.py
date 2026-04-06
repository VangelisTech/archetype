# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""World routes."""

from fastapi import APIRouter, Depends, HTTPException
from uuid_utils import UUID

from archetype.api.deps import get_world_service
from archetype.api.models import CreateWorldRequest, ForkWorldRequest, WorldResponse
from archetype.app.world_service import WorldService
from archetype.core.config import StorageConfig, WorldConfig

router = APIRouter(prefix="/worlds", tags=["worlds"])


@router.post("", response_model=WorldResponse)
async def create_world(
    req: CreateWorldRequest,
    ws: WorldService = Depends(get_world_service),
):
    config = WorldConfig(name=req.name)
    storage_config = StorageConfig(uri=req.storage_uri, namespace=req.namespace)
    world = await ws.create_world(config, storage_config)
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
        world = ws.get_world(UUID(world_id))
        return WorldResponse(
            world_id=str(world.world_id),
            name=getattr(world, "name", None),
            tick=getattr(world, "tick", 0),
            entity_count=getattr(world, "entity_count", 0),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None


@router.delete("/{world_id}")
async def remove_world(world_id: str, ws: WorldService = Depends(get_world_service)):
    try:
        ws.remove_world(UUID(world_id))
        return {"status": "removed", "world_id": world_id}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None


@router.post("/{world_id}/fork", response_model=WorldResponse)
async def fork_world(
    world_id: str,
    req: ForkWorldRequest,
    ws: WorldService = Depends(get_world_service),
):
    try:
        new_world = await ws.fork_world(UUID(world_id), req.name, StorageConfig())
        return WorldResponse(
            world_id=str(new_world.world_id),
            name=getattr(new_world, "name", None),
            tick=getattr(new_world, "tick", 0),
            entity_count=getattr(new_world, "entity_count", 0),
        )
    except (KeyError, TypeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
