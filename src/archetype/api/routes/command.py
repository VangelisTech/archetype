from fastapi import APIRouter, Depends, status
import uuid_utils as uuid 
from archetype.core.aio.async_command import Command
from .deps import get_broker, get_actor_ctx

router = APIRouter(tags=["commands"])

@router.post("/commands", status_code=status.HTTP_202_ACCEPTED)
async def enqueue_cmd(cmd: Command, broker=Depends(get_broker), ctx=Depends(get_actor_ctx)):
    await broker.enqueue(cmd, ctx)
    return {"id": str(cmd.id), "queued": True}

