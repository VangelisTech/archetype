# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Audit metadata contracts for direct spawn operations."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
from archetype.core.component import Component
from archetype.core.config import StorageBackend, StorageConfig, WorldConfig


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
    caplog,
) -> None:
    container = ServiceContainer(audit_storage_config=_audit_storage(tmp_path))
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="spawn-audit-contract"),
            StorageConfig(uri=str(tmp_path / "world-store")),
        )

        with caplog.at_level(logging.WARNING, logger="archetype.app.gateway.service"):
            batch_ids = await container.command_gateway.create_entities(
                actor,
                world.world_id,
                [[AuditPosition(x=1.0)], [AuditPosition(x=2.0)]],
            )
            reserved_id = (
                await container.command_gateway.reserve_entity_ids(
                    actor,
                    world.world_id,
                    1,
                )
            )[0]
            await container.command_gateway.spawn_with_reserved_id(
                actor,
                world.world_id,
                reserved_id,
                [AuditPosition(x=3.0)],
            )

        rows = (await container.audit_log.query(world_id=world.world_id)).to_pylist()

        assert len(batch_ids) == 2
        assert len(rows) == 2
        assert {row["command_type"]: json.loads(row["payload_json"]) for row in rows} == {
            "spawn_batch": {"count": 2},
            "spawn_reserved": {"entity_id": reserved_id},
        }
        assert "audit emission failed" not in caplog.text
    finally:
        await container.shutdown()
