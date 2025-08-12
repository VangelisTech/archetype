from fastapi import APIRouter, Depends, status
import uuid_utils as uuid 
from archetype.app.models import Command
from archetype.app.broker import CommandBroker
from archetype.app.auth.models import ActorCtx
from ..deps import get_broker, get_actor_ctx

router = APIRouter(tags=["commands"])

@router.post("/commands", status_code=status.HTTP_202_ACCEPTED)
async def enqueue_cmd(
    cmd: Command, 
    broker: CommandBroker = Depends(get_broker),
    ctx: ActorCtx = Depends(get_actor_ctx)
):
    await broker.enqueue("default", cmd, ctx)
    return {"id": str(cmd.id), "queued": True}

