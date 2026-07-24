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
from itertools import count
from typing import Any

import pyarrow as pa
import uuid_utils as uuid
from pydantic import BaseModel, Field, FieldSerializationInfo, field_serializer
from uuid_utils import UUID

# Global sequence counter for command ordering
_SEQ = count()


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
    seq: int = Field(default_factory=lambda: next(_SEQ))

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


class AuditRow(BaseModel):
    """One row in the append-only audit log."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)

    # Identity
    audit_id: UUID = Field(default_factory=uuid.uuid7)
    command_id: UUID | None = None
    world_id: str | UUID | None = None
    actor_id: str | UUID | None = None

    # What happened
    command_type: str = ""
    status: str = "applied"  # "applied" | "rejected" | "queued"
    payload_json: str = "{}"

    # When
    accepted_at: str = Field(default_factory=lambda: "")
    applied_at: str = Field(default_factory=lambda: "")

    # Deduplication (nullable)
    idempotency_key: str | None = None
