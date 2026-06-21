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

"""Command submission and audit-backed command history routes."""

from typing import Any

from fastapi import APIRouter, Depends

from archetype.api.deps import get_actor_ctx, get_command_service
from archetype.api.errors import raise_api_error
from archetype.api.models import (
    CommandBatchResponse,
    CommandResponse,
    SubmitBatchRequest,
    SubmitCommandRequest,
    dataframe_to_rows,
)
from archetype.app.auth.models import ActorCtx
from archetype.app.gateway.gate import CommandService
from archetype.app.models import Command, CommandType

router = APIRouter(prefix="/worlds/{world_id}/commands", tags=["commands"])


@router.post("", response_model=CommandResponse)
async def submit_command(
    world_id: str,
    req: SubmitCommandRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Queue a command. Required role depends on the command type."""
    try:
        cmd_type = CommandType(req.type)
        cmd = Command(type=cmd_type, tick=req.tick, payload=req.payload, priority=req.priority)
        cmd_id = await cs.submit(ctx, world_id, cmd)
        return CommandResponse(
            command_id=str(cmd_id),
            type=req.type,
            tick=req.tick,
            priority=req.priority,
        )
    except Exception as exc:
        raise_api_error(exc)


@router.post("/batch", response_model=CommandBatchResponse)
async def submit_batch(
    world_id: str,
    req: SubmitBatchRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Queue commands atomically. Required roles depend on the command types."""
    try:
        cmds = [
            Command(
                type=CommandType(item.type),
                tick=item.tick,
                payload=item.payload,
                priority=item.priority,
            )
            for item in req.commands
        ]
        ids = await cs.submit_batch(ctx, world_id, cmds)
        return CommandBatchResponse(command_ids=[str(command_id) for command_id in ids])
    except Exception as exc:
        raise_api_error(exc)


@router.get("", response_model=list[dict[str, Any]])
async def get_command_history(
    world_id: str,
    limit: int = 100,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Read audit history for a world. Requires viewer, player, operator, or admin."""
    try:
        df = await cs.get_audit_history(ctx, world_id, limit=limit)
        rows = dataframe_to_rows(df)
        for row in rows:
            row.setdefault("type", row.get("command_type"))
        return rows
    except Exception as exc:
        raise_api_error(exc)
