# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Command routes."""

from fastapi import APIRouter, Depends, HTTPException

from archetype.api.deps import get_actor_ctx, get_broker, get_command_service
from archetype.api.models import CommandResponse, SubmitBatchRequest, SubmitCommandRequest
from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.models import Command, CommandType

router = APIRouter(prefix="/worlds/{world_id}/commands", tags=["commands"])


@router.post("", response_model=CommandResponse)
async def submit_command(
    world_id: str,
    req: SubmitCommandRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    try:
        cmd_type = CommandType(req.type)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Unknown command type: {req.type}") from None

    cmd = Command(type=cmd_type, tick=req.tick, payload=req.payload, priority=req.priority)
    try:
        cmd_id = await cs.submit(world_id, cmd, ctx)
        return CommandResponse(id=str(cmd_id), type=req.type, tick=req.tick, priority=req.priority)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e)) from None


@router.post("/batch")
async def submit_batch(
    world_id: str,
    req: SubmitBatchRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    cmds = []
    for r in req.commands:
        try:
            cmd_type = CommandType(r.type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Unknown command type: {r.type}") from None
        cmds.append(Command(type=cmd_type, tick=r.tick, payload=r.payload, priority=r.priority))

    try:
        ids = await cs.submit_batch(world_id, cmds, ctx)
        return [str(i) for i in ids]
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e)) from None


@router.get("")
async def get_command_history(
    world_id: str,
    limit: int = 100,
    broker: CommandBroker = Depends(get_broker),
):
    history = await broker.get_history(world_id, limit)
    return [
        CommandResponse(
            id=str(cmd.id), type=cmd.type.value, tick=cmd.tick, priority=cmd.priority
        )
        for cmd in history
    ]


@router.get("/pending")
async def get_pending_count(
    world_id: str,
    broker: CommandBroker = Depends(get_broker),
):
    count = await broker.get_pending_count(world_id)
    return {"world_id": world_id, "pending_count": count}
