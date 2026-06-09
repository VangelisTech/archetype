# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Request/response schemas and wire helpers for the API."""

from __future__ import annotations

from typing import Any

from daft import DataFrame
from pydantic import BaseModel, Field

from archetype.app.models import Command, EpisodeConfig, RolloutConfig
from archetype.core.component import Component
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig


def dataframe_to_rows(df: DataFrame | list) -> list[dict[str, Any]]:
    """Return a JSON-serializable row list from a Daft DataFrame-like result."""
    if isinstance(df, list):
        return df
    return df.collect().to_pylist()


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
