# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Structural contract for the local durable-activity control plane."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from archetype.storage.activity_catalog.records import (
    ActivityAdmissionRecord,
    ActivityRecord,
)


@runtime_checkable
class ActivityCatalog(Protocol):
    """Physical activity authority; intentionally separate from ControlCatalog."""

    async def admit_activity(
        self,
        admission: ActivityAdmissionRecord,
        *,
        execution_provider: str | None = None,
        execution_operation_id: str | None = None,
    ) -> ActivityRecord: ...

    async def get_activity(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
    ) -> ActivityRecord | None: ...

    async def record_orchestrated_activity_result(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        *,
        provider: str,
        provider_operation_id: str,
        result_ref: str,
        result_digest: str,
        result_media_type: str,
        result_size_bytes: int,
    ) -> ActivityRecord: ...

    async def has_unsettled_activities(self, world_id: str) -> bool: ...

    async def list_unobserved_results(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        after_sequence: int = 0,
    ) -> list[ActivityRecord]: ...

    async def settle_activity_observation(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        *,
        observed_world_id: str,
        observed_run_id: str,
        observed_tick: int,
        observed_visibility_token: str | None,
        expected_result_digest: str,
    ) -> ActivityRecord: ...

    async def close(self) -> None: ...


__all__ = ["ActivityCatalog"]
