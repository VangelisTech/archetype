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
from enum import Enum
from itertools import count
from typing import Any, Literal

import pyarrow as pa
import uuid_utils as uuid
from pydantic import BaseModel, Field, FieldSerializationInfo, field_serializer
from uuid_utils import UUID

# Global sequence counter for command ordering
_SEQ = count()


class CommandType(str, Enum):
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

    # Extensible
    CUSTOM = "custom"


class Command(BaseModel):
    id: UUID = Field(default_factory=uuid.uuid7)
    tick: int = 0
    actor_id: UUID | None = None  # Make optional for simpler examples
    type: CommandType = CommandType.CUSTOM
    op: (
        Literal[
            "create_entity",
            "delete_entity",
            "add_component",
            "remove_component",
            "add_processor",
            "remove_processor",
            "custom",
        ]
        | None
    ) = None  # Keep for backward compatibility
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0
    version: int = 1
    seq: int = Field(default_factory=lambda: next(_SEQ))

    model_config = dict(frozen=True, arbitrary_types_allowed=True)  # hashable & heap-friendly

    # ------------------------------------------------------------------ #
    # Provide Arrow-friendly serialisers
    # ------------------------------------------------------------------ #
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
                ("op", pa.string()),
                ("payload", pa.string()),  # JSON-encoded; keep simple
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
                [self.op or self.type.value],  # Use type if op is None
                [json.dumps(self.payload)],
                [self.priority],
                [self.version],
                [self.seq],
            ],
            schema=self.arrow_schema(),
        )

    # ------------------------------------------------------------------ #
    # Keep the heap ordering contract
    # ------------------------------------------------------------------ #
    def __lt__(self, other: "Command") -> bool:
        return (self.tick, self.priority, self.seq) < (
            other.tick,
            other.priority,
            other.seq,
        )
