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
    Command types for the command broker.

    The broker is the universal simulation interface supporting:
    - Entity-level mutations (spawn, despawn, components)
    - Processor mutations (hot-swap behavior)
    - Simulation-level operations (recursive simulation, rollouts)
    """

    # Entity-level commands
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

    # Rollout/Episode commands (for mental simulation / MCTS)
    RUN_ROLLOUT = "run_rollout"  # Run N steps in a world
    RUN_EPISODE = "run_episode"  # Full episode with sampled ICs
    QUERY_WORLD = "query_world"  # Get state/results from a world

    # Agent-to-agent messaging (realized at tick boundary)
    MESSAGE = "message"  # payload: {sender_id, receiver_id, channel?, content}

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


# ── Service layer models ──


class WorldInfo(BaseModel):
    model_config = dict(arbitrary_types_allowed=True)
    world_id: UUID
    name: str | None = None
    tick: int = 0
    entity_count: int = 0
    archetype_signatures: list[str] = Field(default_factory=list)


class RunResult(BaseModel):
    model_config = dict(arbitrary_types_allowed=True)
    run_id: UUID
    world_id: UUID
    ticks_completed: int = 0
    commands_applied: int = 0
    final_tick: int = 0


class ProcessorInfo(BaseModel):
    name: str
    priority: int
    components: list[str] = Field(default_factory=list)


class WorldSnapshot(BaseModel):
    model_config = dict(arbitrary_types_allowed=True)
    world_id: UUID
    tick: int = 0
    entities: dict[int, list[str]] = Field(default_factory=dict)
    archetype_counts: dict[str, int] = Field(default_factory=dict)
