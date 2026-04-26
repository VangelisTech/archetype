# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Entity mutation routes."""

from fastapi import APIRouter, Depends, Response, status

from archetype.api.deps import get_actor_ctx, get_command_service
from archetype.api.errors import raise_api_error
from archetype.api.models import (
    ComponentsRequest,
    ComponentTypesRequest,
    EntityResponse,
    hydrate_component_types,
    hydrate_components,
)
from archetype.app.auth.models import ActorCtx
from archetype.app.command_service import CommandService

router = APIRouter(prefix="/worlds/{world_id}/entities", tags=["entities"])


@router.post("", response_model=EntityResponse, status_code=status.HTTP_201_CREATED)
async def create_entity(
    world_id: str,
    req: ComponentsRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Create an entity. Requires player, operator, or admin."""
    try:
        entity_id = await cs.create_entity(ctx, world_id, hydrate_components(req.components))
        return EntityResponse(entity_id=entity_id)
    except Exception as exc:
        raise_api_error(exc)


@router.delete("/{entity_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_entity(
    world_id: str,
    entity_id: int,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Remove an entity. Requires player, operator, or admin."""
    try:
        await cs.remove_entity(ctx, world_id, entity_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as exc:
        raise_api_error(exc)


@router.patch("/{entity_id}", status_code=status.HTTP_204_NO_CONTENT)
async def update_entity(
    world_id: str,
    entity_id: int,
    req: ComponentsRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Overlay component values on an entity. Requires player, operator, or admin."""
    try:
        await cs.update_entity(ctx, world_id, entity_id, hydrate_components(req.components))
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as exc:
        raise_api_error(exc)


@router.post("/{entity_id}/components", status_code=status.HTTP_204_NO_CONTENT)
async def add_components(
    world_id: str,
    entity_id: int,
    req: ComponentsRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Extend an entity with components. Requires operator or admin."""
    try:
        await cs.add_components(ctx, world_id, entity_id, hydrate_components(req.components))
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as exc:
        raise_api_error(exc)


@router.delete("/{entity_id}/components", status_code=status.HTTP_204_NO_CONTENT)
async def remove_components(
    world_id: str,
    entity_id: int,
    req: ComponentTypesRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Remove component types from an entity. Requires operator or admin."""
    try:
        component_types = hydrate_component_types(req.component_types)
        await cs.remove_components(ctx, world_id, entity_id, component_types)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as exc:
        raise_api_error(exc)
