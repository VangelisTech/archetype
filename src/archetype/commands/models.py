# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Commands-owned boundary and durable-admission models."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

import uuid_utils as uuid
from pydantic import BaseModel, ConfigDict, Field, model_validator
from uuid_utils import UUID

MAX_ACCESS_SUMMARY_BYTES = 4096


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

    def __post_init__(self) -> None:
        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise TypeError("version must be an integer")
        if self.version < 1:
            raise ValueError("version must be at least 1")


@dataclass(frozen=True, slots=True)
class PolicyRequest:
    """One bounded world/tick debit in an atomic policy batch."""

    permission: str
    world_id: object
    target_tick: int
    token_cost: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.permission, str) or not self.permission.strip():
            raise ValueError("permission must be a non-empty string")
        for name, value in (
            ("target_tick", self.target_tick),
            ("token_cost", self.token_cost),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
            if value < 0:
                raise ValueError(f"{name} must be non-negative")


class GetAuditHistory(_FrozenModel):
    """Commands-owned, direct-only query over the durable access journal."""

    direct_only: ClassVar[bool] = True
    operation: Literal["get_audit_history"] = "get_audit_history"
    world_id: str | UUID
    tick_range: tuple[int, int] | None = None
    actor_id: str | UUID | None = None
    idempotency_key: str | None = None
    status: str | None = None
    limit: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _validate_tick_range(self) -> GetAuditHistory:
        if self.tick_range is None:
            return self
        start, end = self.tick_range
        if start < 0 or end < 0:
            raise ValueError("tick_range values must be non-negative")
        if start > end:
            raise ValueError("tick_range start must not exceed its end")
        return self


class AccessSummary(_FrozenModel):
    """Bounded, redacted evidence for an actor-aware dispatch decision."""

    operation: str
    actor_id: str | UUID
    world_id: str | UUID | None = None
    decision: Literal["allowed", "denied"]
    outcome: Literal["succeeded", "failed", "denied", "rejected", "queued"]
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_canonical_size(self) -> AccessSummary:
        try:
            encoded = json.dumps(
                self.model_dump(mode="json"),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as error:
            raise ValueError("access summary must be canonical JSON") from error
        if len(encoded) > MAX_ACCESS_SUMMARY_BYTES:
            raise ValueError(f"access summary exceeds {MAX_ACCESS_SUMMARY_BYTES} encoded bytes")
        return self


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
    "MAX_ACCESS_SUMMARY_BYTES",
    "PolicyRequest",
]
