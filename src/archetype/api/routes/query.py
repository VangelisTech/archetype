# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Read/query routes."""

from typing import Any

from fastapi import APIRouter, Depends, Query

from archetype.api.deps import get_actor_ctx, get_command_service
from archetype.api.errors import raise_api_error
from archetype.api.models import dataframe_to_rows, hydrate_component_types
from archetype.app.auth.models import ActorCtx
from archetype.app.gateway.gate import CommandService
from archetype.app.models import HookInfo, ProcessorInfo, ResourceInfo
from archetype.core.config import StorageConfig

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


def _tick_range(value: str | None) -> tuple[int, int] | None:
    items = _split_csv(value)
    if not items:
        return None
    if len(items) != 2:
        raise ValueError("tick_range must be two comma-separated integers: start,end")
    start, end = (int(item) for item in items)
    if start > end:
        raise ValueError("tick_range start must be less than or equal to end")
    return start, end


async def _query_all_state(
    cs: CommandService,
    ctx: ActorCtx,
    world_id: str,
    *,
    tick: int | None = None,
    entity_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    query_world_id, run_id = await _query_ids(cs, ctx, world_id)
    rows: list[dict[str, Any]] = []
    for sig in await cs.list_signatures(ctx):
        df = await cs.query_archetype(
            ctx,
            sig,
            query_world_id,
            run_id,
            ticks=[tick] if tick is not None else None,
            entity_ids=entity_ids,
        )
        rows.extend(dataframe_to_rows(df))
    return rows


async def _query_ids(cs: CommandService, ctx: ActorCtx, world_id: str) -> tuple[str, str]:
    try:
        info = await cs.get_world_info(ctx, world_id)
    except KeyError:
        return str(world_id), ""
    return str(info.world_id), str(info.run_id or "")


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
    query_world_id, run_id = await _query_ids(cs, ctx, world_id)
    component_types = hydrate_component_types(component_names)
    df = await cs.query_components(
        ctx,
        component_types,
        query_world_id,
        run_id,
        ticks=[tick] if tick is not None else None,
        entity_ids=entity_ids,
    )
    return dataframe_to_rows(df)


@router.get("/worlds/{world_id}/state", response_model=list[dict[str, Any]])
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
        component_names = _split_csv(components)
        if not component_names:
            return await _query_all_state(
                cs,
                ctx,
                world_id,
                tick=tick,
                entity_ids=_entity_ids(entity_ids),
            )
        return await _query_components(
            cs,
            ctx,
            world_id,
            component_names=component_names,
            tick=tick,
            entity_ids=_entity_ids(entity_ids),
        )
    except Exception as exc:
        raise_api_error(exc)


@router.get("/worlds/{world_id}/entities/{entity_id}", response_model=list[dict[str, Any]])
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


@router.get("/worlds/{world_id}/components", response_model=list[dict[str, Any]])
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


@router.get("/worlds/{world_id}/history", response_model=list[dict[str, Any]])
async def get_audit_history(
    world_id: str,
    limit: int = 100,
    tick_range: str | None = Query(None, description="Comma-separated inclusive start,end"),
    actor_id: str | None = None,
    idempotency_key: str | None = None,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Read audit history. Requires viewer, player, operator, or admin."""
    try:
        df = await cs.get_audit_history(
            ctx,
            world_id,
            tick_range=_tick_range(tick_range),
            actor_id=actor_id,
            idempotency_key=idempotency_key,
            limit=limit,
        )
        rows = dataframe_to_rows(df)
        for row in rows:
            row.setdefault("type", row.get("command_type"))
        return rows
    except Exception as exc:
        raise_api_error(exc)


@router.get("/worlds/{world_id}/processors", response_model=list[ProcessorInfo])
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


@router.get("/worlds/{world_id}/hooks", response_model=list[HookInfo])
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


@router.get("/worlds/{world_id}/resources", response_model=list[ResourceInfo])
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


@router.get("/signatures", response_model=list[str])
async def list_signatures(
    storage_uri: str | None = None,
    namespace: str | None = None,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """List persisted archetype signatures. Requires viewer, player, operator, or admin."""
    try:
        storage_config: StorageConfig | None = None
        if storage_uri is not None or namespace is not None:
            # Fill the unspecified field from StorageConfig so a single-arg
            # call still resolves to the same store the rest of the API uses.
            defaults = StorageConfig()
            storage_config = StorageConfig(
                uri=storage_uri if storage_uri is not None else defaults.uri,
                namespace=namespace if namespace is not None else defaults.namespace,
            )
        return [str(sig) for sig in await cs.list_signatures(ctx, storage_config)]
    except Exception as exc:
        raise_api_error(exc)
