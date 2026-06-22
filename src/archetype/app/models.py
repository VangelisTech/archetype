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

from archetype.core.config import JsonUUID, RunConfig

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

    # Simulation control
    STEP = "step"
    RUN = "run"
    RUN_ROLLOUT = "run_rollout"
    RUN_EPISODE = "run_episode"

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

    # Agent-to-agent messaging (realized at tick boundary)
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


# ── Service layer models ──


class WorldInfo(BaseModel):
    """Immutable snapshot of a world's identity and position.

    This is the gate boundary type — iCommandService returns WorldInfo,
    never iWorld. Field access is sync; fetch is gated.
    """

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    # str | UUID: worlds store str internally; gate accepts both
    world_id: str | JsonUUID
    name: str | None = None
    tick: int = 0
    run_id: str | JsonUUID | None = None


class RunResult(BaseModel):
    model_config = dict(arbitrary_types_allowed=True)
    run_id: str | JsonUUID
    world_id: str | JsonUUID
    ticks_completed: int = 0
    commands_applied: int = 0
    final_tick: int = 0


class EpisodeConfig(BaseModel):
    """Configuration for a single episode (bounded simulation run).

    Three termination strategies, checked in this order each tick (first to
    fire wins), plus the ``max_steps`` cap:

    1. **Structural** — ``terminal_component`` *without* ``terminal_field``:
       stop as soon as any entity *carries* that component type. Checked
       before each step (an already-terminal world runs zero steps).
    2. **Value-based** — ``terminal_component`` *with* ``terminal_field``:
       stop when entities carrying the component have the boolean field
       latched. ``terminal_all`` picks the reducer — True (default) waits
       for *every* such entity, False stops at the *first*. Checked after
       each step, against persisted rows. This is the "all entities done"
       contract the LIBERO driver hand-rolled per tick.
    3. **Callable** — ``termination(world) -> bool`` escape hatch, checked
       before each step.

    Setting ``terminal_field`` reinterprets ``terminal_component`` as the
    value-carrier, so the structural check is suppressed (otherwise the
    component's mere presence would terminate at tick 0).
    """

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    episode_id: str | JsonUUID = Field(default_factory=uuid.uuid7)
    run_config: RunConfig = Field(default_factory=RunConfig)
    max_steps: int = 1000
    terminal_component: Any | None = None
    terminal_field: str | None = None  # boolean field on terminal_component, e.g. "done"
    terminal_all: bool = True  # True: every entity must latch; False: any one
    termination: Any | None = None  # Callable[[iWorld], bool] | None


class RolloutConfig(BaseModel):
    """Configuration for a rollout (N episodes forked from a base world)."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    rollout_id: str | JsonUUID = Field(default_factory=uuid.uuid7)
    episode_config: EpisodeConfig = Field(default_factory=EpisodeConfig)
    num_episodes: int = 1
    parallel: bool = False
    name_prefix: str = "ep"
    destroy_forks_on_complete: bool = False


class EpisodeResult(BaseModel):
    """Result of a single episode."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    episode_id: str | JsonUUID
    world_id: str | JsonUUID
    run_id: str | JsonUUID | None = None
    start_tick: int = 0
    final_tick: int = 0
    terminated: bool = False
    duration_steps: int = 0


class RolloutResult(BaseModel):
    """Result of a rollout (N episodes)."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    rollout_id: str | JsonUUID
    base_world_id: str | JsonUUID
    episodes: list[EpisodeResult] = Field(default_factory=list)
    num_episodes: int = 0
    total_duration_steps: int = 0


class ProcessorInfo(BaseModel):
    """Read-only summary of a registered processor."""

    model_config = dict(frozen=True)
    qualname: str = Field(description="Processor class qualname")
    priority: int = 0
    components: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Component qualnames this processor operates on",
    )


class HookInfo(BaseModel):
    """Read-only summary of a registered hook."""

    model_config = dict(frozen=True)
    event_type: str = Field(description="HookEvent subclass qualname")
    handler_qualname: str = Field(description="Handler callable qualname")
    mode: str = Field(default="blocking", description="'blocking' or 'spawn'")
    handle_id: int = Field(description="HookHandle._id for removal")


class ResourceInfo(BaseModel):
    """Read-only summary of a resource attached to a world.

    Resources are keyed by type in the underlying Resources container;
    the qualname IS the unique identity of a resource within a world.
    """

    model_config = dict(frozen=True)
    qualname: str = Field(description="Resource class qualname")


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
