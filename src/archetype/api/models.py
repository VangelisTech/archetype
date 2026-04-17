# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Request/response schemas for the API."""

from typing import Any

from pydantic import BaseModel, Field


class CreateWorldRequest(BaseModel):
    name: str | None = None
    storage_uri: str = "./archetype_data"
    namespace: str = "archetypes"


class ForkWorldRequest(BaseModel):
    name: str | None = None
    storage_uri: str = "./archetype_data"
    namespace: str = "archetypes"


class SubmitCommandRequest(BaseModel):
    type: str = "custom"
    tick: int = 0
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0


class SubmitBatchRequest(BaseModel):
    commands: list[SubmitCommandRequest]


class StepRequest(BaseModel):
    num_steps: int = 1
    debug: bool = False


class RunRequest(BaseModel):
    num_steps: int = 1
    debug: bool = False


class WorldResponse(BaseModel):
    world_id: str
    name: str | None = None
    tick: int = 0
    entity_count: int = 0


class CommandResponse(BaseModel):
    id: str
    type: str
    tick: int
    priority: int


class RunResultResponse(BaseModel):
    run_id: str
    world_id: str
    ticks_completed: int
    commands_applied: int
    final_tick: int


class ErrorResponse(BaseModel):
    detail: str
