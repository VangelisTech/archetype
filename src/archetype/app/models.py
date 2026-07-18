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

from archetype.app.artifacts.models import ArtifactProcessor as ArtifactProcessor
from archetype.app.artifacts.models import ArtifactReceipt as ArtifactReceipt
from archetype.app.artifacts.models import ArtifactWriteReceipt as ArtifactWriteReceipt
from archetype.core.config import JsonUUID, RunConfig

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
    PUBLISH_ARTIFACT = "publish_artifact"  # Durable external artifact publication
    EVALUATE = "evaluate"  # Claim-before-grade: one visible receipt per evaluation_id
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


# ── Service layer models ──


class WorldInfo(BaseModel):
    """Immutable snapshot of a world's identity and current position."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    # str | UUID: worlds store str internally; gate accepts both
    world_id: str | JsonUUID = Field(description="Durable world identifier.")
    name: str | None = Field(default=None, description="Human-readable world name.")
    tick: int = Field(default=0, description="Next tick to execute.")
    run_id: str | JsonUUID | None = Field(
        default=None, description="Identifier of the active or most recent run."
    )


class RunResult(BaseModel):
    """Summary of a completed call to `RuntimeWorld.run()`."""

    model_config = dict(arbitrary_types_allowed=True)
    run_id: str | JsonUUID = Field(description="Identifier of the completed run.")
    world_id: str | JsonUUID = Field(description="World advanced by the run.")
    ticks_completed: int = Field(default=0, description="Number of ticks executed.")
    commands_applied: int = Field(
        default=0, description="Number of queued commands applied during the run."
    )
    final_tick: int = Field(default=0, description="World tick after the run completed.")


class EpisodeConfig(BaseModel):
    """Configure a bounded simulation episode.

    An episode stops at `max_steps`, when `termination` returns true, or when
    its terminal component condition is satisfied. Supplying only
    `terminal_component` stops on component presence. Adding `terminal_field`
    instead tests that boolean field; `terminal_all` chooses whether every or
    any matching entity must satisfy it.
    """

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    episode_id: str | JsonUUID = Field(
        default_factory=uuid.uuid7, description="Stable identifier for this episode."
    )
    run_config: RunConfig = Field(default_factory=RunConfig, description="Tick execution options.")
    max_steps: int = Field(default=1000, ge=0, description="Maximum ticks before stopping.")
    terminal_component: Any | None = Field(
        default=None, description="Component type used for structural or value termination."
    )
    terminal_field: str | None = Field(
        default=None, description="Boolean field tested on the terminal component."
    )
    terminal_all: bool = Field(
        default=True, description="Require every matching entity when testing a field."
    )
    termination: Any | None = Field(
        default=None, description="Optional callable termination predicate."
    )


class RolloutConfig(BaseModel):
    """Configure several episodes run on forks of one base world."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    rollout_id: str | JsonUUID = Field(
        default_factory=uuid.uuid7, description="Stable identifier for this rollout."
    )
    episode_config: EpisodeConfig = Field(
        default_factory=EpisodeConfig, description="Configuration shared by each episode."
    )
    num_episodes: int = Field(default=1, ge=0, description="Number of episode forks to run.")
    parallel: bool = Field(default=False, description="Run episode forks concurrently.")
    name_prefix: str = Field(default="ep", description="Name prefix for episode worlds.")
    destroy_forks_on_complete: bool = Field(
        default=False, description="Destroy live episode worlds after collecting results."
    )


class EpisodeResult(BaseModel):
    """Result of a single episode."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    episode_id: str | JsonUUID = Field(description="Episode identifier.")
    world_id: str | JsonUUID = Field(description="Forked world used by the episode.")
    run_id: str | JsonUUID | None = Field(default=None, description="Episode run identifier.")
    start_tick: int = Field(default=0, description="World tick at episode start.")
    final_tick: int = Field(default=0, description="World tick at episode completion.")
    terminated: bool = Field(
        default=False, description="Whether a termination condition stopped the episode."
    )
    duration_steps: int = Field(default=0, description="Number of ticks executed.")


class RolloutResult(BaseModel):
    """Aggregate result of a rollout."""

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
    rollout_id: str | JsonUUID = Field(description="Rollout identifier.")
    base_world_id: str | JsonUUID = Field(description="World forked for each episode.")
    episodes: list[EpisodeResult] = Field(
        default_factory=list, description="Results in episode order."
    )
    num_episodes: int = Field(default=0, description="Number of completed episodes.")
    total_duration_steps: int = Field(
        default=0, description="Total ticks executed across all episodes."
    )


class ProcessorInfo(BaseModel):
    """Read-only summary of a registered processor."""

    model_config = dict(frozen=True)
    qualname: str = Field(description="Qualified processor class name.")
    priority: int = Field(default=0, description="Execution priority; lower values run first.")
    components: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Qualified component names required by the processor.",
    )


class HookInfo(BaseModel):
    """Read-only summary of a registered hook."""

    model_config = dict(frozen=True)
    event_type: str = Field(description="Qualified lifecycle-event class name.")
    handler_qualname: str = Field(description="Qualified handler name.")
    mode: str = Field(default="blocking", description="Execution mode: blocking or spawn.")
    handle_id: int = Field(description="Handle identifier used to remove the hook.")


class ResourceInfo(BaseModel):
    """Read-only summary of a resource attached to a world.

    Resources are keyed by type in the underlying Resources container;
    the qualname IS the unique identity of a resource within a world.
    """

    model_config = dict(frozen=True)
    qualname: str = Field(description="Qualified resource class name.")


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
