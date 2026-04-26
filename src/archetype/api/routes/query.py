# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Read/query routes."""

from fastapi import APIRouter, Depends, Query

from archetype.api.deps import get_actor_ctx, get_command_service
from archetype.api.errors import raise_api_error
from archetype.api.models import dataframe_to_rows, hydrate_component_types
from archetype.app.auth.models import ActorCtx
from archetype.app.command_service import CommandService

router = APIRouter(tags=["query"])


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _entity_ids(value: str | None) -> list[int] | None:
    items = _split_csv(value)
    if not items:
        return None
    return [int(item) for item in items]


async def _query_components(
    cs: CommandService,
    ctx: ActorCtx,
    world_id: str,
    *,
    component_names: list[str],
    tick: int | None = None,
    entity_ids: list[int] | None = None,
):
    if not component_names:
        return []
    info = await cs.get_world_info(ctx, world_id)
    component_types = hydrate_component_types(component_names)
    df = await cs.query_components(
        ctx,
        component_types,
        str(info.world_id),
        str(info.run_id or ""),
        ticks=[tick] if tick is not None else None,
        entity_ids=entity_ids,
    )
    return dataframe_to_rows(df)


@router.get("/worlds/{world_id}/state")
async def get_world_state(
    world_id: str,
    tick: int | None = None,
    entity_ids: str | None = None,
    components: str | None = None,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Read world state rows by component filter. Requires viewer, player, operator, or admin."""
    try:
        return await _query_components(
            cs,
            ctx,
            world_id,
            component_names=_split_csv(components),
            tick=tick,
            entity_ids=_entity_ids(entity_ids),
        )
    except Exception as exc:
        raise_api_error(exc)


@router.get("/worlds/{world_id}/entities/{entity_id}")
async def get_entity(
    world_id: str,
    entity_id: int,
    tick: int | None = None,
    components: str = Query("", description="Comma-separated component type names to project"),
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Read one entity by component filter. Requires viewer, player, operator, or admin."""
    try:
        return await _query_components(
            cs,
            ctx,
            world_id,
            component_names=_split_csv(components),
            tick=tick,
            entity_ids=[entity_id],
        )
    except Exception as exc:
        raise_api_error(exc)


@router.get("/worlds/{world_id}/components")
async def get_components(
    world_id: str,
    types: str = Query("", description="Comma-separated component type names"),
    tick: int | None = None,
    entity_ids: str | None = None,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Read entities containing component types. Requires viewer, player, operator, or admin."""
    try:
        return await _query_components(
            cs,
            ctx,
            world_id,
            component_names=_split_csv(types),
            tick=tick,
            entity_ids=_entity_ids(entity_ids),
        )
    except Exception as exc:
        raise_api_error(exc)


@router.get("/worlds/{world_id}/history")
async def get_audit_history(
    world_id: str,
    limit: int = 100,
    actor_id: str | None = None,
    signer_address: str | None = None,
    idempotency_key: str | None = None,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Read audit history. Requires viewer, player, operator, or admin."""
    try:
        df = await cs.get_audit_history(
            ctx,
            world_id,
            actor_id=actor_id,
            signer_address=signer_address,
            idempotency_key=idempotency_key,
            limit=limit,
        )
        rows = dataframe_to_rows(df)
        for row in rows:
            row.setdefault("type", row.get("command_type"))
        return rows
    except Exception as exc:
        raise_api_error(exc)


@router.get("/worlds/{world_id}/processors")
async def list_processors(
    world_id: str,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """List deployment-configured processors. Requires viewer, player, operator, or admin."""
    try:
        return await cs.list_processors(ctx, world_id)
    except Exception as exc:
        raise_api_error(exc)


@router.get("/worlds/{world_id}/hooks")
async def list_hooks(
    world_id: str,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """List deployment-configured hooks. Requires viewer, player, operator, or admin."""
    try:
        return await cs.list_hooks(ctx, world_id)
    except Exception as exc:
        raise_api_error(exc)


@router.get("/worlds/{world_id}/resources")
async def list_resources(
    world_id: str,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """List deployment-configured resources. Requires viewer, player, operator, or admin."""
    try:
        return await cs.list_resources(ctx, world_id)
    except Exception as exc:
        raise_api_error(exc)


@router.get("/signatures")
async def list_signatures(
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """List persisted archetype signatures. Requires viewer, player, operator, or admin."""
    try:
        return [str(sig) for sig in await cs.list_signatures(ctx)]
    except Exception as exc:
        raise_api_error(exc)
