# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Chat graph routes — DAG-based conversation threading."""

from fastapi import APIRouter, Depends, HTTPException
from uuid_utils import UUID as UUID7

from archetype.api.deps import get_actor_ctx, get_broker, get_chat_graphs, get_command_service
from archetype.api.models import (
    ActivePathResponse,
    BranchRequest,
    ChatGraphResponse,
    CommandResponse,
    NavigateRequest,
)
from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.chat_graph import ChatGraphRegistry
from archetype.app.command_service import CommandService
from archetype.app.models import Command, CommandType

router = APIRouter(prefix="/worlds/{world_id}/chat", tags=["chat"])


@router.get("", response_model=ChatGraphResponse)
async def get_chat_graph(
    world_id: str,
    graphs: ChatGraphRegistry = Depends(get_chat_graphs),
):
    """Get the full conversation graph for a world."""
    graph = graphs.get(world_id)
    data = graph.to_dict()
    return ChatGraphResponse(**data)


@router.get("/path", response_model=ActivePathResponse)
async def get_active_path(
    world_id: str,
    graphs: ChatGraphRegistry = Depends(get_chat_graphs),
):
    """Get the linear message path from root to cursor (context window)."""
    graph = graphs.get(world_id)
    commands = graph.active_path()
    return ActivePathResponse(
        world_id=world_id,
        path=[
            CommandResponse(id=str(cmd.id), type=cmd.type.value, tick=cmd.tick, priority=cmd.priority)
            for cmd in commands
        ],
    )


@router.post("/branch", response_model=CommandResponse)
async def branch_conversation(
    world_id: str,
    req: BranchRequest,
    graphs: ChatGraphRegistry = Depends(get_chat_graphs),
    broker: CommandBroker = Depends(get_broker),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Create a branch (alternative reply) from an existing message."""
    try:
        cmd_type = CommandType(req.type)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Unknown command type: {req.type}") from None

    parent_uuid = UUID7(req.parent_id)
    # append_history=False: the branch route manages graph insertion directly
    cmd = Command(
        type=cmd_type,
        tick=req.tick,
        payload=req.payload,
        priority=req.priority,
        append_history=False,
        parent_id=parent_uuid,
    )

    graph = graphs.get(world_id)
    try:
        graph.branch(parent_uuid, cmd, label=req.label)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Parent node {req.parent_id} not found") from None

    # Enqueue for processing (append_history=False avoids double-insert into graph)
    await broker.enqueue(world_id, cmd, ctx)

    return CommandResponse(id=str(cmd.id), type=req.type, tick=req.tick, priority=req.priority)


@router.post("/navigate", response_model=ActivePathResponse)
async def navigate_to_node(
    world_id: str,
    req: NavigateRequest,
    graphs: ChatGraphRegistry = Depends(get_chat_graphs),
):
    """Move the cursor to a specific node and return the new active path."""
    graph = graphs.get(world_id)
    try:
        graph.navigate(UUID7(req.node_id))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Node {req.node_id} not found") from None

    commands = graph.active_path()
    return ActivePathResponse(
        world_id=world_id,
        path=[
            CommandResponse(id=str(cmd.id), type=cmd.type.value, tick=cmd.tick, priority=cmd.priority)
            for cmd in commands
        ],
    )


@router.post("/auto-nav", response_model=ActivePathResponse)
async def auto_navigate(
    world_id: str,
    graphs: ChatGraphRegistry = Depends(get_chat_graphs),
):
    """Auto-navigate to the deepest leaf along the most recent branch."""
    graph = graphs.get(world_id)
    graph.auto_nav_to_leaf()
    commands = graph.active_path()
    return ActivePathResponse(
        world_id=world_id,
        path=[
            CommandResponse(id=str(cmd.id), type=cmd.type.value, tick=cmd.tick, priority=cmd.priority)
            for cmd in commands
        ],
    )


@router.get("/branches/{node_id}")
async def get_branches(
    world_id: str,
    node_id: str,
    graphs: ChatGraphRegistry = Depends(get_chat_graphs),
):
    """List all branches (children) at a given node."""
    graph = graphs.get(world_id)
    branches = graph.branches_at(UUID7(node_id))
    return [
        {
            "cmd_id": str(b.cmd.id),
            "cmd_type": b.cmd.type.value,
            "branch_label": b.branch_label,
            "children_count": len(b.children),
        }
        for b in branches
    ]


@router.delete("/prune/{node_id}")
async def prune_subtree(
    world_id: str,
    node_id: str,
    graphs: ChatGraphRegistry = Depends(get_chat_graphs),
):
    """Remove a node and all its descendants."""
    graph = graphs.get(world_id)
    removed = graph.prune(UUID7(node_id))
    return {"removed": removed, "cursor": str(graph.cursor) if graph.cursor else None}
