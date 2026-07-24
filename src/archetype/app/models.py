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

import json
from enum import StrEnum
from typing import Any, cast

import pyarrow as pa
import uuid_utils as uuid
from pydantic import BaseModel, Field, FieldSerializationInfo, field_serializer
from uuid_utils import UUID

from archetype.commands.models import AuditRow, DurableOptions
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


class CommandType(StrEnum):
    """
    Command types for durable admission and tick-boundary dispatch.

    The command envelope supports:
    - Entity-level mutations (spawn, despawn, components)
    - Processor mutations (hot-swap behavior)
    - Simulation-level operations (recursive simulation, rollouts)
    """

    # Entity-level commands
    INGEST_ARTIFACTS = "ingest_artifacts"
    EVALUATE = "evaluate"
    SPAWN = "spawn"
    DESPAWN = "despawn"
    UPDATE = "update"
    ADD_COMPONENT = "add_component"
    REMOVE_COMPONENT = "remove_component"

    # Processor-level commands
    ADD_PROCESSOR = "add_processor"
    REMOVE_PROCESSOR = "remove_processor"

    # Simulation-level commands (for recursive/hierarchical simulation)
    CREATE_WORLD = "create_world"  # Spawn a child simulation
    DESTROY_WORLD = "destroy_world"  # Cleanup child simulation
    FORK_WORLD = "fork_world"  # Clone current state to explore alternatives

    # Simulation control
    STEP = "step"
    RUN = "run"
    RUN_ROLLOUT = "run_rollout"
    RUN_EPISODE = "run_episode"
    AUTORESEARCH = "autoresearch"  # Optimization loop over rollouts

    # Reads / introspection
    QUERY_WORLD = "query_world"
    GET_WORLD_INFO = "get_world_info"
    GET_AUDIT_HISTORY = "get_audit_history"
    LIST_SIGNATURES = "list_signatures"
    LIST_WORLDS = "list_worlds"
    LIST_PROCESSORS = "list_processors"
    LIST_HOOKS = "list_hooks"
    LIST_RESOURCES = "list_resources"

    # Resource management
    ADD_RESOURCE = "add_resource"
    ADD_HOOK = "add_hook"
    REMOVE_HOOK = "remove_hook"

    # Application-defined message envelope (the scheduler supplies ordering only)
    MESSAGE = "message"

    # Extensible
    CUSTOM = "custom"


class Command(BaseModel):
    id: UUID = Field(default_factory=uuid.uuid7)
    tick: int = 0
    actor_id: UUID | None = None
    type: CommandType = CommandType.CUSTOM
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0
    version: int = 1
    # Compatibility-only input order. Durable order is assigned atomically by
    # the control catalog; no process-global sequence authority remains here.
    seq: int = 0

    model_config = dict(frozen=True, arbitrary_types_allowed=True)

    @field_serializer("id", "actor_id")
    def serialize_uuids(self, v: UUID | None, info: FieldSerializationInfo):
        if v is None:
            return None
        if info.mode == "json":
            return str(v)
        return v.bytes

    @classmethod
    def arrow_schema(cls) -> pa.schema:
        """Return a canonical Arrow schema suitable for Parquet."""
        return pa.schema(
            [
                ("id", pa.binary(16)),
                ("tick", pa.int32()),
                ("actor_id", pa.binary(16)),
                ("type", pa.string()),
                ("payload", pa.string()),
                ("priority", pa.int16()),
                ("version", pa.int8()),
                ("seq", pa.int64()),
            ]
        )

    def to_arrow(self) -> pa.RecordBatch:
        return pa.record_batch(
            [
                [self.id.bytes],
                [self.tick],
                [self.actor_id.bytes if self.actor_id else None],
                [self.type.value],
                [json.dumps(self.payload)],
                [self.priority],
                [self.version],
                [self.seq],
            ],
            schema=self.arrow_schema(),
        )

    def __lt__(self, other: "Command") -> bool:
        return (self.tick, self.priority, self.seq) < (
            other.tick,
            other.priority,
            other.seq,
        )


def _legacy_component(value: object) -> Component:
    if isinstance(value, Component):
        return value
    if isinstance(value, dict):
        return Component.from_dict(cast("dict[str, Any]", value))
    raise TypeError("legacy deferred component values must be Component instances or payloads")


def _legacy_component_type(value: object) -> type[Component]:
    if isinstance(value, type) and issubclass(value, Component):
        return value
    if isinstance(value, str):
        return Component.get_type_by_name(value)
    if isinstance(value, dict):
        return type(_legacy_component(value))
    raise TypeError("legacy deferred component types must be Component types or names")


def _legacy_entity_id(value: object) -> int:
    if type(value) is int:
        return value
    if isinstance(value, str):
        digits = value[1:] if value[:1] in {"+", "-"} else value
        if digits and digits.isascii() and digits.isdecimal():
            return int(value)
    raise TypeError("entity_id must be an integer or decimal-integer string")


def deferred_operation(
    world_id: str | UUID,
    command: Command,
) -> tuple[WorldOperation, DurableOptions]:
    """Translate the finite legacy wire envelope into one exact family model."""
    payload = command.payload
    components = tuple(
        ComponentValue.from_component(_legacy_component(value))
        for value in payload.get("components", ())
    )

    operation: WorldOperation
    if command.type is CommandType.SPAWN:
        if "entity_id" in payload:
            operation = SpawnReserved(
                world_id=world_id,
                entity_id=_legacy_entity_id(payload["entity_id"]),
                components=components,
            )
        else:
            operation = Spawn(world_id=world_id, components=components)
    elif command.type is CommandType.DESPAWN:
        operation = Despawn(
            world_id=world_id,
            entity_id=_legacy_entity_id(payload.get("entity_id")),
        )
    elif command.type is CommandType.UPDATE:
        operation = Update(
            world_id=world_id,
            entity_id=_legacy_entity_id(payload.get("entity_id")),
            components=components,
        )
    elif command.type is CommandType.ADD_COMPONENT:
        operation = AddComponents(
            world_id=world_id,
            entity_id=_legacy_entity_id(payload.get("entity_id")),
            components=components,
        )
    elif command.type is CommandType.REMOVE_COMPONENT:
        raw_types = payload.get("component_types", payload.get("components", ()))
        operation = RemoveComponents(
            world_id=world_id,
            entity_id=_legacy_entity_id(payload.get("entity_id")),
            component_types=tuple(
                ComponentTypeRef.from_type(_legacy_component_type(value)) for value in raw_types
            ),
        )
    else:
        raise ValueError(
            f"{command.type.value} is direct-only or unsupported and cannot "
            "enter portable deferred admission"
        )

    return (
        operation,
        DurableOptions(
            target_tick=command.tick,
            priority=command.priority,
        ),
    )


__all__ = [
    "AuditRow",
    "Command",
    "CommandType",
    "deferred_operation",
]
