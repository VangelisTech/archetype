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


class QueryRequest(BaseModel):
    """DataFrame query expressed as a self-contained JSON body."""

    components: list[str]
    where: str | None = None
    select: list[str] | None = None
    sort: str | None = None
    desc: bool = False
    limit: int = 50
    offset: int = 0
    tick: int | None = None
    count: bool = False


class QueryResponse(BaseModel):
    """Rows returned by a DataFrame query."""

    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    total: int = 0


class CountResponse(BaseModel):
    """Row count returned by a DataFrame count query."""

    count: int = 0


class ErrorResponse(BaseModel):
    detail: str
