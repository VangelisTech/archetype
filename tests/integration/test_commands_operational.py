# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free source/wheel receipt for the commands execution boundary."""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi.testclient import TestClient
from uuid_utils import uuid7

from archetype.api.app import create_app
from archetype.api.deps import set_container
from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
from archetype.commands.models import DurableOptions
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.world import mutation as world_mutation
from archetype.world.models import ComponentValue, SpawnReserved

pytestmark = [
    pytest.mark.contract("gateway.authorization.rbac"),
    pytest.mark.contract("commands.identity.idempotent"),
    pytest.mark.contract("commands.settlement.atomic"),
    pytest.mark.contract("commands.failure.preserves_progress"),
    pytest.mark.integration,
]


class CommandsOperationalMarker(Component):
    value: int = 0


@pytest.mark.asyncio
async def test_trusted_direct_and_deferred_reserved_spawn_share_family_behavior(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct and deferred entry preserve one reserved ID through one family seam."""

    container = ServiceContainer()
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    calls: list[tuple[Any, int, list[Component]]] = []
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="commands-parity"),
            StorageConfig(uri=str(tmp_path / "store"), namespace="commands-parity"),
        )
        real_locked_spawn = world_mutation._spawn_with_reserved_id_locked

        async def observe_locked_spawn(
            actual_world: Any,
            entity_id: int,
            components: list[Component],
        ) -> None:
            calls.append((actual_world, entity_id, components))
            await real_locked_spawn(actual_world, entity_id, components)

        monkeypatch.setattr(
            world_mutation,
            "_spawn_with_reserved_id_locked",
            observe_locked_spawn,
        )

        (direct_id,) = await container.application.reserve_entity_ids(world.world_id, 1)
        await container.application.spawn_with_reserved_id(
            world.world_id,
            direct_id,
            [CommandsOperationalMarker(value=1)],
        )
        deferred_id = await container.command_gateway.submit_spawn(
            actor,
            world.world_id,
            [CommandsOperationalMarker(value=2)],
            tick=0,
        )

        assert [entity_id for _world, entity_id, _components in calls] == [direct_id]
        assert await container.application.step(world.world_id, RunConfig()) == 1
        assert [entity_id for _world, entity_id, _components in calls] == [
            direct_id,
            deferred_id,
        ]
        assert all(actual_world is world for actual_world, _entity_id, _components in calls)
        assert [
            component.value
            for _actual_world, _entity_id, components in calls
            for component in components
            if isinstance(component, CommandsOperationalMarker)
        ] == [1, 2]
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_due_spawn_creates_its_new_signature_in_the_due_tick(tmp_path) -> None:
    """A due spawn is visible to signature discovery and persistence in that tick."""

    container = ServiceContainer()
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="commands-due-tick"),
            StorageConfig(uri=str(tmp_path / "store"), namespace="commands-due-tick"),
        )
        entity_id = await container.command_gateway.submit_spawn(
            actor,
            world.world_id,
            [CommandsOperationalMarker(value=7)],
            tick=0,
        )

        assert await container.application.step(world.world_id, RunConfig()) == 1
        assert entity_id in world.entity2sig
        rows = (await world.get_components([CommandsOperationalMarker])).to_pylist()
        assert [(row["entity_id"], row["tick"]) for row in rows] == [(entity_id, 0)]
        (record,) = await container.command_scheduler.records(world.world_id)
        assert record.status == "APPLIED"
    finally:
        await container.shutdown()


def test_actor_aware_api_denial_emits_one_bounded_redacted_access_row(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A full-policy HTTP denial is a 403 with one payload-free access receipt."""

    audit_storage = StorageConfig(
        uri=str(tmp_path / "audit-store"),
        namespace="commands_operational",
        backend=StorageBackend.ICEBERG,
    )
    container = ServiceContainer(audit_storage_config=audit_storage)
    monkeypatch.setattr(container.policy, "_max_tokens_per_day", 0)
    set_container(container)
    try:
        with TestClient(create_app()) as client:
            response = client.post(
                "/worlds",
                headers={"Authorization": "Bearer admin"},
                json={
                    "name": "WORLD_NAME_SENTINEL",
                    "storage_uri": str(tmp_path / "STORAGE_URI_SENTINEL"),
                },
            )

            assert response.status_code == 403
            assert response.json() == {"detail": "actor exceeded daily token budget (0 tokens)"}
            (evidence,) = container.audit_log._pending  # noqa: SLF001 - exact receipt seam
            encoded = json.dumps(
                evidence.model_dump(mode="python"),
                sort_keys=True,
                default=str,
            )
            assert evidence.command_type == "create_world"
            assert evidence.status == "denied"
            assert evidence.world_id is None
            assert evidence.actor_id is not None
            assert evidence.payload_json == "{}"
            assert len(encoded.encode("utf-8")) <= 4096
            assert "WORLD_NAME_SENTINEL" not in encoded
            assert "STORAGE_URI_SENTINEL" not in encoded
    finally:
        set_container(None)


@pytest.mark.asyncio
async def test_materializer_failure_leaves_tick_and_command_unsettled_for_retry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Infrastructure failure advances neither tick nor durable settlement."""

    container = ServiceContainer()
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="commands-materializer-failure"),
            StorageConfig(
                uri=str(tmp_path / "store"),
                namespace="commands-materializer-failure",
            ),
        )
        await container.command_gateway.submit_spawn(
            actor,
            world.world_id,
            [CommandsOperationalMarker(value=11)],
            tick=0,
        )
        real_materialize = world._materialize_commands

        async def unavailable_materializer(_world, _tick):
            raise RuntimeError("command materializer unavailable")

        monkeypatch.setattr(world, "_materialize_commands", unavailable_materializer)
        with pytest.raises(RuntimeError, match="command materializer unavailable"):
            await container.application.step(world.world_id, RunConfig())

        assert world.tick == 0
        (pending,) = await container.command_scheduler.records(world.world_id)
        assert pending.status == "PENDING"
        assert world.entity2sig == {}

        monkeypatch.setattr(world, "_materialize_commands", real_materialize)
        assert await container.application.step(world.world_id, RunConfig()) == 1
        (applied,) = await container.command_scheduler.records(world.world_id)
        assert applied.status == "APPLIED"
        assert applied.applied_tick == 0
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_post_stage_retry_does_not_duplicate_the_reserved_spawn(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A manifest failure retries the staged identity without restaging mutation."""

    audit_storage = StorageConfig(
        uri=str(tmp_path / "audit-store"),
        namespace="commands-operational-audit",
        backend=StorageBackend.ICEBERG,
    )
    container = ServiceContainer(audit_storage_config=audit_storage)
    try:
        world = await container.world_lifecycle.create_world(
            WorldConfig(name="commands-post-stage-retry"),
            StorageConfig(
                uri=str(tmp_path / "world-store"),
                namespace="commands-post-stage-retry",
            ),
        )
        (entity_id,) = await container.application.reserve_entity_ids(world.world_id, 1)
        command_id = await container.command_dispatcher.defer(
            SpawnReserved(
                world_id=world.world_id,
                entity_id=entity_id,
                components=(ComponentValue.from_component(CommandsOperationalMarker(value=41)),),
            ),
            DurableOptions(target_tick=0),
        )
        storage_record = await container.world_registry.storage_record(str(world.world_id))
        assert storage_record is not None
        catalog = container.storage_service.get_control_catalog(storage_record[0])
        real_publish = catalog.publish_manifest
        crashed = False

        async def crash_once(*args, **kwargs):
            nonlocal crashed
            if not crashed:
                crashed = True
                raise RuntimeError("crash before manifest transaction")
            return await real_publish(*args, **kwargs)

        monkeypatch.setattr(catalog, "publish_manifest", crash_once)
        with pytest.raises(RuntimeError, match="crash before manifest"):
            await container.application.step(world.world_id, RunConfig())

        (leased,) = await container.command_scheduler.records(world.world_id)
        assert leased.status == "LEASED"
        assert leased.command_id == str(command_id)
        signature = world.entity2sig[entity_id]
        assert (
            len([row for row in world.spawn_cache[signature] if row["entity_id"] == entity_id]) == 1
        )

        assert await container.application.step(world.world_id, RunConfig()) == 1
        (applied,) = await container.command_scheduler.records(world.world_id)
        assert applied.status == "APPLIED"
        assert applied.applied_tick == 0
        assert not [
            row for row in world.spawn_cache.get(signature, []) if row["entity_id"] == entity_id
        ]
    finally:
        await container.shutdown()
