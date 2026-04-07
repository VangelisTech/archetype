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
        snapshot = await qs.get_world_state(UUID(world_id), tick)
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
        entity = await qs.get_entity(UUID(world_id), entity_id, tick)
        return entity
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None


@router.get("/components")
async def get_components(
    world_id: str,
    types: str = "",
    tick: int | None = None,
    qs: QueryService = Depends(get_query_service),
):
    try:
        component_types = [t.strip() for t in types.split(",") if t.strip()]
        result = await qs.get_components(UUID(world_id), component_types, tick=tick)
        return result
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None


@router.get("/history")
async def get_command_history(
    world_id: str,
    limit: int = 100,
    qs: QueryService = Depends(get_query_service),
):
    try:
        history = await qs.get_command_history(UUID(world_id), limit)
        return [cmd.model_dump(mode="json") for cmd in history]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None
