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

"""Request/response schemas and wire helpers for the API."""

from __future__ import annotations

from typing import Any, cast

from daft import DataFrame
from pydantic import BaseModel, Field

from archetype.app.models import Command, EpisodeConfig, RolloutConfig
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig


def dataframe_to_rows(df: DataFrame | list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return a JSON-serializable row list from a Daft DataFrame-like result."""
    # Daft's to_pylist() is stubbed as list[Any], and isinstance(list) erases the
    # element type; rows are JSON objects in both branches.
    rows = df if isinstance(df, list) else df.collect().to_pylist()
    return cast("list[dict[str, Any]]", rows)


def hydrate_components(payloads: list[dict[str, Any]]) -> list[Component]:
    return [Component.from_dict(payload) for payload in payloads]


def hydrate_component_types(names: list[str]) -> list[type[Component]]:
    return [Component.get_type_by_name(name) for name in names]


_STORAGE_DEFAULTS = StorageConfig()


class CreateWorldRequest(BaseModel):
    config: WorldConfig | None = None
    storage_config: StorageConfig | None = None
    cache_config: CacheConfig | None = None

    # Backwards-compatible shorthand. Defaults MUST track StorageConfig so the
    # POST /worlds default landing path matches the GET /worlds/{id}/state and
    # GET /signatures default read path; otherwise data is silently sharded.
    name: str | None = None
    storage_uri: str = str(_STORAGE_DEFAULTS.uri)
    namespace: str = _STORAGE_DEFAULTS.namespace

    model_config = dict(arbitrary_types_allowed=True)

    def world_config(self) -> WorldConfig:
        return self.config or WorldConfig(name=self.name)

    def storage(self) -> StorageConfig:
        return self.storage_config or StorageConfig(uri=self.storage_uri, namespace=self.namespace)


class ForkWorldRequest(BaseModel):
    name: str | None = None
    storage_config: StorageConfig | None = None
    cache_config: CacheConfig | None = None

    model_config = dict(arbitrary_types_allowed=True)


class ComponentsRequest(BaseModel):
    components: list[dict[str, Any]] = Field(default_factory=list)


class ComponentTypesRequest(BaseModel):
    component_types: list[str] = Field(default_factory=list)


class EntityResponse(BaseModel):
    entity_id: int


class QueryCountResponse(BaseModel):
    count: int


class SubmitCommandRequest(BaseModel):
    type: str = "custom"
    tick: int = 0
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0

    def to_command(self) -> Command:
        from archetype.app.models import CommandType

        return Command(
            type=CommandType(self.type),
            tick=self.tick,
            payload=self.payload,
            priority=self.priority,
        )


class SubmitBatchRequest(BaseModel):
    commands: list[SubmitCommandRequest]


class CommandsRequest(BaseModel):
    commands: list[Command]


class StepRequest(BaseModel):
    run_config: RunConfig | None = None
    num_steps: int = 1
    debug: bool = False

    model_config = dict(arbitrary_types_allowed=True)

    def to_run_config(self) -> RunConfig:
        return self.run_config or RunConfig(num_steps=1, debug=self.debug)


class RunRequest(BaseModel):
    run_config: RunConfig | None = None
    num_steps: int = 1
    debug: bool = False

    model_config = dict(arbitrary_types_allowed=True)

    def to_run_config(self) -> RunConfig:
        return self.run_config or RunConfig(num_steps=self.num_steps, debug=self.debug)


class CommandResponse(BaseModel):
    command_id: str
    type: str
    tick: int
    priority: int


class CommandBatchResponse(BaseModel):
    command_ids: list[str]


class RunResultResponse(BaseModel):
    run_id: str
    world_id: str
    ticks_completed: int
    commands_applied: int
    final_tick: int


class StepResponse(BaseModel):
    commands_applied: int


class ErrorResponse(BaseModel):
    detail: str


__all__ = [
    "CacheConfig",
    "Command",
    "CommandBatchResponse",
    "CommandResponse",
    "CommandsRequest",
    "ComponentTypesRequest",
    "ComponentsRequest",
    "CreateWorldRequest",
    "EntityResponse",
    "EpisodeConfig",
    "ForkWorldRequest",
    "QueryCountResponse",
    "RolloutConfig",
    "RunConfig",
    "RunRequest",
    "RunResultResponse",
    "StepResponse",
    "StepRequest",
    "StorageConfig",
    "WorldConfig",
    "dataframe_to_rows",
    "hydrate_component_types",
    "hydrate_components",
]
