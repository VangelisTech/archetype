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

from typing import Any, cast

from fastapi import APIRouter, Depends
from uuid_utils import uuid7

from archetype.api.deps import get_actor_ctx, get_dispatcher
from archetype.api.errors import raise_api_error
from archetype.api.models import (
    CommandBatchResponse,
    CommandResponse,
    SubmitBatchRequest,
    SubmitCommandRequest,
    dataframe_to_rows,
)
from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx, DeferredItem, DurableOptions, GetAuditHistory
from archetype.core.component import Component
from archetype.world.models import (
    AddComponents,
    ComponentTypeRef,
    ComponentValue,
    Despawn,
    RemoveComponents,
    Spawn,
    SpawnReserved,
    Update,
    WorldOperation,
)

router = APIRouter(prefix="/worlds/{world_id}/commands", tags=["commands"])

_PORTABLE_COMMAND_TYPES = frozenset(
    {
        "spawn",
        "despawn",
        "update",
        "add_component",
        "remove_component",
    }
)
_KNOWN_COMMAND_TYPES = _PORTABLE_COMMAND_TYPES | {
    "ingest_artifacts",
    "evaluate",
    "add_processor",
    "remove_processor",
    "create_world",
    "destroy_world",
    "fork_world",
    "step",
    "run",
    "run_rollout",
    "run_episode",
    "autoresearch",
    "query_world",
    "get_world_info",
    "get_audit_history",
    "list_signatures",
    "list_worlds",
    "list_processors",
    "list_hooks",
    "list_resources",
    "add_resource",
    "add_hook",
    "remove_hook",
    "message",
    "custom",
}


def _component(value: object) -> Component:
    if isinstance(value, Component):
        return value
    if isinstance(value, dict):
        return Component.from_dict(cast("dict[str, Any]", value))
    raise TypeError("deferred component values must be component payloads")


def _component_type(value: object) -> type[Component]:
    if isinstance(value, type) and issubclass(value, Component):
        return value
    if isinstance(value, str):
        return Component.get_type_by_name(value)
    if isinstance(value, dict):
        return type(_component(value))
    raise TypeError("deferred component types must be names or component payloads")


def _entity_id(value: object) -> int:
    if type(value) is int:
        return value
    if isinstance(value, str):
        digits = value[1:] if value[:1] in {"+", "-"} else value
        if digits and digits.isascii() and digits.isdecimal():
            return int(value)
    raise TypeError("entity_id must be an integer or decimal-integer string")


def _portable_request(
    world_id: str,
    request: SubmitCommandRequest,
) -> tuple[WorldOperation, DurableOptions]:
    """Translate the legacy HTTP shape directly into one exact operation."""
    if request.type not in _KNOWN_COMMAND_TYPES:
        raise ValueError(f"{request.type!r} is not a valid CommandType")
    if request.type not in _PORTABLE_COMMAND_TYPES:
        raise ValueError(
            f"{request.type} is direct-only or unsupported and cannot "
            "enter portable deferred admission"
        )

    payload = request.payload
    components = tuple(
        ComponentValue.from_component(_component(value)) for value in payload.get("components", ())
    )

    operation: WorldOperation
    if request.type == "spawn":
        if "entity_id" in payload:
            operation = SpawnReserved(
                world_id=world_id,
                entity_id=_entity_id(payload["entity_id"]),
                components=components,
            )
        else:
            operation = Spawn(world_id=world_id, components=components)
    elif request.type == "despawn":
        operation = Despawn(
            world_id=world_id,
            entity_id=_entity_id(payload.get("entity_id")),
        )
    elif request.type == "update":
        operation = Update(
            world_id=world_id,
            entity_id=_entity_id(payload.get("entity_id")),
            components=components,
        )
    elif request.type == "add_component":
        operation = AddComponents(
            world_id=world_id,
            entity_id=_entity_id(payload.get("entity_id")),
            components=components,
        )
    else:
        raw_types = payload.get("component_types", payload.get("components", ()))
        operation = RemoveComponents(
            world_id=world_id,
            entity_id=_entity_id(payload.get("entity_id")),
            component_types=tuple(
                ComponentTypeRef.from_type(_component_type(value)) for value in raw_types
            ),
        )

    return (
        operation,
        DurableOptions(
            target_tick=request.tick,
            priority=request.priority,
        ),
    )


@router.post("", response_model=CommandResponse)
async def submit_command(
    world_id: str,
    req: SubmitCommandRequest,
    dispatcher: CommandDispatcher = Depends(get_dispatcher),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Queue a command. Required role depends on the command type."""
    try:
        operation, options = _portable_request(world_id, req)
        command_id = uuid7()
        cmd_id = await dispatcher.defer_as(
            ctx,
            operation,
            options,
            command_id=command_id,
        )
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
    dispatcher: CommandDispatcher = Depends(get_dispatcher),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Queue commands atomically. Required roles depend on the command types."""
    try:
        items = tuple(
            DeferredItem(
                operation=operation,
                options=options,
                command_id=uuid7(),
            )
            for request in req.commands
            for operation, options in (_portable_request(world_id, request),)
        )
        ids = await dispatcher.defer_batch_as(ctx, items)
        return CommandBatchResponse(command_ids=[str(command_id) for command_id in ids])
    except Exception as exc:
        raise_api_error(exc)


@router.get("", response_model=list[dict[str, Any]])
async def get_command_history(
    world_id: str,
    limit: int = 100,
    dispatcher: CommandDispatcher = Depends(get_dispatcher),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Read audit history for a world. Requires viewer, player, operator, or admin."""
    try:
        df = await dispatcher.apply_as(
            ctx,
            GetAuditHistory(world_id=world_id, limit=limit),
        )
        rows = dataframe_to_rows(df)
        for row in rows:
            row.setdefault("type", row.get("command_type"))
        return rows
    except Exception as exc:
        raise_api_error(exc)
