# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Structural contract shared by local and remote control catalogs."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from archetype.storage.catalog.records import (
    CommandAdmission,
    CommandRecord,
    EvaluationLease,
    ManifestRecord,
    OutboxRecord,
    SignatureRecord,
    WorldRecord,
)


@runtime_checkable
class ControlCatalog(Protocol):
    """Shared local/remote authority exposed by ``StorageService``."""

    async def register_world(self, record: WorldRecord) -> None: ...
    async def set_world_status(self, world_id: str, status: str) -> None: ...
    async def get_world(self, world_id: str) -> WorldRecord | None: ...
    async def list_worlds(self) -> list[WorldRecord]: ...
    async def register_signature(self, record: SignatureRecord) -> None: ...
    async def list_signatures(self) -> list[SignatureRecord]: ...
    async def acquire_fence(self, world_id: str, holder: str) -> int: ...
    async def current_fence_epoch(self, world_id: str) -> int | None: ...
    async def max_manifest_tick(self, world_id: str, run_id: str) -> int | None: ...
    async def publish_manifest(
        self,
        world_id: str,
        run_id: str,
        tick: int,
        commit_token: str,
        writer_epoch: int,
        table_ids: list[str],
        *,
        command_ids: list[str] | None = None,
        lease_owner: str | None = None,
    ) -> None: ...
    async def visible_tokens(
        self,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
    ) -> dict[int, list[str]] | None: ...
    async def list_manifests(
        self,
        world_id: str,
        run_id: str | None = None,
    ) -> list[ManifestRecord]: ...
    async def admit_commands(
        self,
        world_id: str,
        admissions: list[CommandAdmission],
    ) -> list[CommandRecord]: ...
    async def lease_commands(
        self,
        world_id: str,
        tick: int,
        owner: str,
        *,
        lease_seconds: float = 30.0,
        limit: int = 50_000,
    ) -> list[CommandRecord]: ...
    async def fail_command(
        self,
        world_id: str,
        command_id: str,
        owner: str,
        *,
        status: str,
        error_code: str,
        error_detail: str,
    ) -> CommandRecord: ...
    async def release_commands(
        self,
        world_id: str,
        command_ids: list[str],
        owner: str,
    ) -> None: ...
    async def list_commands(
        self,
        world_id: str,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[CommandRecord]: ...
    async def pending_command_count(self, world_id: str) -> int: ...
    async def max_reserved_entity_id(self, world_id: str) -> int | None: ...
    async def lease_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        subject_digest: str,
        contract_digest: str,
        owner: str,
        *,
        lease_seconds: float = 300.0,
    ) -> EvaluationLease: ...
    async def complete_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        owner: str,
    ) -> None: ...
    async def release_evaluation(
        self,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        owner: str,
    ) -> None: ...
    async def cancel_commands(self, world_id: str, *, reason: str) -> int: ...
    async def read_outbox(
        self,
        world_id: str,
        *,
        limit: int = 1000,
    ) -> list[OutboxRecord]: ...
    async def mark_outbox_projected(
        self,
        world_id: str,
        event_ids: list[str],
    ) -> None: ...
    async def outbox_progress(self, world_id: str) -> tuple[int, int]: ...
    async def close(self) -> None: ...
