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

from archetype.core.config import RunConfig

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
    """Immutable snapshot of a world's identity and position.

    This is the gate boundary type — iCommandService returns WorldInfo,
    never iWorld. Field access is sync; fetch is gated.
    """

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    world_id: str | UUID
    name: str | None = None
    tick: int = 0
    run_id: str | UUID | None = None


class RunResult(BaseModel):
    model_config = dict(arbitrary_types_allowed=True)
    run_id: UUID
    world_id: str | UUID
    ticks_completed: int = 0
    commands_applied: int = 0
    final_tick: int = 0


class EpisodeConfig(BaseModel):
    """Configuration for a single episode (bounded simulation run)."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    episode_id: UUID = Field(default_factory=uuid.uuid7)
    run_config: RunConfig = Field(default_factory=RunConfig)
    max_steps: int = 1000
    terminal_component: Any | None = None
    termination: Any | None = None  # Callable[[iWorld], bool] | None


class RolloutConfig(BaseModel):
    """Configuration for a rollout (N episodes forked from a base world)."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    rollout_id: UUID = Field(default_factory=uuid.uuid7)
    episode_config: EpisodeConfig = Field(default_factory=EpisodeConfig)
    num_episodes: int = 1
    parallel: bool = False
    name_prefix: str = "ep"
    destroy_forks_on_complete: bool = False


class EpisodeResult(BaseModel):
    """Result of a single episode."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    episode_id: UUID
    world_id: str | UUID
    final_tick: int = 0
    terminated: bool = False
    duration_steps: int = 0


class RolloutResult(BaseModel):
    """Result of a rollout (N episodes)."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    rollout_id: UUID
    base_world_id: str | UUID
    episodes: list[EpisodeResult] = Field(default_factory=list)
    num_episodes: int = 0


class ProcessorInfo(BaseModel):
    name: str
    priority: int
    components: list[str] = Field(default_factory=list)


class WorldSnapshot(BaseModel):
    model_config = dict(arbitrary_types_allowed=True)
    world_id: str | UUID
    tick: int = 0
    entities: dict[int, list[str]] = Field(default_factory=dict)
    archetype_counts: dict[str, int] = Field(default_factory=dict)
