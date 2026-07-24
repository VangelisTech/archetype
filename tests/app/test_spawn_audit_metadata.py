# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Audit metadata contracts for direct spawn operations."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from uuid_utils import uuid7

from archetype.commands.models import ActorCtx, GetAuditHistory
from archetype.core.component import Component
from archetype.core.config import StorageBackend, StorageConfig, WorldConfig
from archetype.world.models import CreateEntities, CreateWorld, Spawn
from tests._runtime import build_test_runtime


class AuditPosition(Component):
    x: float = 0.0


def _audit_storage(tmp_path: Path) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "audit-store"),
        namespace="spawn_audit",
        backend=StorageBackend.ICEBERG,
    )


@pytest.mark.asyncio
async def test_spawn_operations_emit_one_row_with_structured_metadata(
    tmp_path: Path,
) -> None:
    resources = build_test_runtime(
        tmp_path,
        audit_storage_config=_audit_storage(tmp_path),
    )
    dispatcher = resources.dispatcher
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="spawn-audit-contract"),
                storage_config=StorageConfig(uri=str(tmp_path / "world-store")),
            )
        )

        batch_ids = await dispatcher.apply_as(
            actor,
            CreateEntities.from_entities(
                world_id=world.world_id,
                entities=[[AuditPosition(x=1.0)], [AuditPosition(x=2.0)]],
            ),
        )
        entity_id = await dispatcher.apply_as(
            actor,
            Spawn.from_components(
                world_id=world.world_id,
                components=[AuditPosition(x=3.0)],
            ),
        )

        rows = (
            await dispatcher.apply(
                GetAuditHistory(
                    world_id=world.world_id,
                )
            )
        ).to_pylist()

        assert len(batch_ids) == 2
        assert entity_id not in batch_ids
        assert len(rows) == 2
        assert {row["command_type"]: json.loads(row["payload_json"]) for row in rows} == {
            "create_entities": {
                "operation": "create_entities",
                "world_id": str(world.world_id),
            },
            "spawn": {
                "operation": "spawn",
                "world_id": str(world.world_id),
            },
        }
        assert {row["status"] for row in rows} == {"succeeded"}
    finally:
        await resources.aclose()
