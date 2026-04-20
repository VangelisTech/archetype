# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Query routes."""

from fastapi import APIRouter, Depends, HTTPException
from uuid_utils import UUID

from archetype.api.deps import get_query_service
from archetype.app.query_service import QueryService

router = APIRouter(prefix="/worlds/{world_id}", tags=["query"])


@router.get("/state")
async def get_world_state(
    world_id: str,
    tick: int | None = None,
    qs: QueryService = Depends(get_query_service),
):
    try:
        wid = UUID(world_id)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid UUID: {world_id}") from None
    try:
        snapshot = await qs.get_world_state(wid, tick)
        data = snapshot.model_dump()
        data["world_id"] = str(data["world_id"])
        return data
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None


@router.get("/entities/{entity_id}")
async def get_entity(
    world_id: str,
    entity_id: int,
    tick: int | None = None,
    qs: QueryService = Depends(get_query_service),
):
    try:
        wid = UUID(world_id)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid UUID: {world_id}") from None
    try:
        entity = await qs.get_entity(wid, entity_id, tick)
        return entity
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None


@router.get("/components")
async def get_components(
    world_id: str,
    types: str = "",
    qs: QueryService = Depends(get_query_service),
):
    try:
        wid = UUID(world_id)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid UUID: {world_id}") from None
    try:
        component_types = [t.strip() for t in types.split(",") if t.strip()]
        result = await qs.get_components(wid, component_types)
        return result
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None


@router.get("/history")
async def get_command_history(
    world_id: str,
    limit: int = 100,
    qs: QueryService = Depends(get_query_service),
):
    try:
        wid = UUID(world_id)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid UUID: {world_id}") from None
    try:
        history = await qs.get_command_history(wid, limit)
        return [cmd.model_dump(mode="json") for cmd in history]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None
