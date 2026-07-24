# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Commands-owned boundary and durable-admission models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import uuid_utils as uuid
from pydantic import BaseModel, ConfigDict, Field
from uuid_utils import UUID


class _FrozenModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )


class ActorCtx(_FrozenModel):
    """Stable caller identity and its flat role grants."""

    id: UUID
    roles: set[str] = Field(default_factory=lambda: {"viewer"})


class DurableOptions(_FrozenModel):
    """Commands-owned scheduling controls for one durable operation."""

    target_tick: int = Field(ge=0)
    priority: int = 0
    max_attempts: int = Field(default=3, ge=1)


@dataclass(frozen=True, slots=True)
class DeferredItem:
    """One exact dispatcher/scheduler batch-admission item."""

    operation: BaseModel
    options: DurableOptions
    command_id: UUID | None = None
    version: int = 1


@dataclass(frozen=True, slots=True)
class PolicyRequest:
    """One bounded world/tick debit in an atomic policy batch."""

    permission: str
    world_id: object
    target_tick: int
    token_cost: int = 0


class GetAuditHistory(_FrozenModel):
    """Commands-owned, direct-only query over the durable access journal."""

    operation: Literal["get_audit_history"] = "get_audit_history"
    world_id: str | UUID
    tick_range: tuple[int, int] | None = None
    actor_id: str | UUID | None = None
    idempotency_key: str | None = None
    status: str | None = None
    limit: int | None = Field(default=None, ge=0)


class AccessSummary(_FrozenModel):
    """Bounded, redacted evidence for an actor-aware dispatch decision."""

    operation: str
    actor_id: str | UUID
    world_id: str | UUID | None = None
    decision: Literal["allowed", "denied"]
    outcome: Literal["succeeded", "failed", "denied", "rejected", "queued"]
    metadata: dict[str, Any] = Field(default_factory=dict)


class AuditRow(_FrozenModel):
    """One stable row in the durable command/access projection."""

    audit_id: UUID = Field(default_factory=uuid.uuid7)
    command_id: UUID | None = None
    world_id: str | UUID | None = None
    actor_id: str | UUID | None = None
    command_type: str = ""
    status: str = "applied"
    payload_json: str = "{}"
    accepted_at: str = ""
    applied_at: str = ""
    idempotency_key: str | None = None


__all__ = [
    "AccessSummary",
    "ActorCtx",
    "AuditRow",
    "DeferredItem",
    "DurableOptions",
    "GetAuditHistory",
    "PolicyRequest",
]
