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

"""World lifecycle routes."""

from fastapi import APIRouter, Depends, Response, status
from uuid_utils import UUID

from archetype.api.deps import get_actor_ctx, get_command_gateway
from archetype.api.errors import raise_api_error
from archetype.api.models import CreateWorldRequest, ForkWorldRequest
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.gateway.interfaces import iCommandGateway
from archetype.world.models import WorldInfo

router = APIRouter(prefix="/worlds", tags=["worlds"])


@router.post("", response_model=WorldInfo, status_code=status.HTTP_201_CREATED)
async def create_world(
    req: CreateWorldRequest,
    cs: iCommandGateway = Depends(get_command_gateway),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Create a world. Requires admin."""
    try:
        return await cs.create_world(ctx, req.world_config(), req.storage(), req.cache_config)
    except Exception as exc:
        raise_api_error(exc, conflict=True)


@router.get("", response_model=list[WorldInfo])
async def list_worlds(
    cs: iCommandGateway = Depends(get_command_gateway),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """List live worlds. Requires admin."""
    try:
        return await cs.list_worlds(ctx)
    except Exception as exc:
        raise_api_error(exc)


@router.get("/{world_id}", response_model=WorldInfo)
async def get_world(
    world_id: str,
    cs: iCommandGateway = Depends(get_command_gateway),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Get world metadata. Requires viewer, player, operator, or admin."""
    try:
        UUID(world_id)
        return await cs.get_world_info(ctx, world_id)
    except Exception as exc:
        raise_api_error(exc)


@router.delete("/{world_id}", status_code=status.HTTP_204_NO_CONTENT)
async def destroy_world(
    world_id: str,
    cs: iCommandGateway = Depends(get_command_gateway),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Drop the in-memory world instance. Persisted storage and audit rows are retained.

    Requires operator or admin. Destroying an unknown world is a no-op; an
    unparsable world id is rejected as a client error.
    """
    try:
        UUID(world_id)  # the no-op contract covers missing worlds, not bad ids
        await cs.destroy_world(ctx, world_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as exc:
        raise_api_error(exc)


@router.post("/{world_id}/fork", response_model=WorldInfo, status_code=status.HTTP_201_CREATED)
async def fork_world(
    world_id: str,
    req: ForkWorldRequest,
    cs: iCommandGateway = Depends(get_command_gateway),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Fork a world. Requires operator or admin."""
    try:
        return await cs.fork_world(
            ctx,
            world_id,
            req.name,
            storage_config=req.storage_config,
            cache_config=req.cache_config,
        )
    except Exception as exc:
        raise_api_error(exc, conflict=True)
