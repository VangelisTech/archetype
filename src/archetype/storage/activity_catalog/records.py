# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Physical control records for the local durable-activity catalog."""

from __future__ import annotations

from dataclasses import dataclass

from archetype.errors import ConflictError


class ActivityCatalogConflictError(ConflictError):
    """Stored immutable activity facts conflict with the requested operation."""


class ActivityCatalogNotFoundError(KeyError):
    """No physical activity record exists for one world-and-kind-scoped identity."""


class ActivityCatalogClaimError(ConflictError):
    """A physical claim is stale or insufficient for an operation."""


@dataclass(frozen=True, slots=True)
class ActivityAdmissionRecord:
    activity_id: str
    kind: str
    source_world_id: str
    source_run_id: str
    source_tick: int
    source_visibility_token: str | None
    input_ref: str
    input_digest: str


@dataclass(frozen=True, slots=True)
class ActivityRecord:
    sequence: int | None
    activity_id: str
    kind: str
    source_world_id: str
    source_run_id: str
    source_tick: int
    source_visibility_token: str | None
    input_ref: str
    input_digest: str
    result_ref: str | None
    result_digest: str | None
    result_media_type: str | None
    result_size_bytes: int | None
    result_attempt: int | None
    result_fence: int | None
    result_recorded_at: str | None
    observed_world_id: str | None
    observed_run_id: str | None
    observed_tick: int | None
    observed_visibility_token: str | None
    observed_result_digest: str | None
    observed_at: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class ActivityClaimRecord:
    activity: ActivityRecord
    acquired: bool
    attempt: int | None
    fence: int | None
    owner: str | None
    lease_expires_at: float | None
    provider: str | None
    provider_operation_id: str | None
    retry_guard_ref: str | None
    retry_guard_digest: str | None
    reconciles_attempt: int | None
    reconciles_provider: str | None
    reconciles_provider_operation_id: str | None


__all__ = [
    "ActivityAdmissionRecord",
    "ActivityCatalogClaimError",
    "ActivityCatalogConflictError",
    "ActivityCatalogNotFoundError",
    "ActivityClaimRecord",
    "ActivityRecord",
]
