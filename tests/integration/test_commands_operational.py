# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free source/wheel receipt for the commands execution boundary."""

from __future__ import annotations

import json
from typing import Any

import pytest
from uuid_utils import uuid7

from archetype.commands.models import ActorCtx, DurableOptions
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageBackend, StorageConfig, WorldConfig
from archetype.storage.service import StorageService
from archetype.world import mutation as world_mutation
from archetype.world.models import (
    ComponentTypeRef,
    ComponentValue,
    CreateWorld,
    QueryComponents,
    ReserveEntityIds,
    Spawn,
    SpawnReserved,
    Step,
)
from tests._runtime import build_test_runtime

pytestmark = [
    pytest.mark.contract("gateway.authorization.rbac"),
    pytest.mark.contract("commands.identity.idempotent"),
    pytest.mark.contract("commands.settlement.atomic"),
    pytest.mark.contract("commands.failure.preserves_progress"),
    pytest.mark.integration,
]


class CommandsOperationalMarker(Component):
    value: int = 0


def _world_registry(dispatcher: Any) -> Any:
    step_spec = dispatcher._registry.resolve_name("step")
    return step_spec.handler.args[0]


@pytest.mark.asyncio
async def test_trusted_direct_and_deferred_reserved_spawn_share_family_behavior(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct and deferred entry preserve one reserved ID through one family seam."""

    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    calls: list[tuple[Any, int, list[Component]]] = []
    try:
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="commands-parity"),
                storage_config=StorageConfig(
                    uri=str(tmp_path / "store"),
                    namespace="commands-parity",
                ),
            )
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

        (direct_id,) = await dispatcher.apply(ReserveEntityIds(world_id=info.world_id, count=1))
        await dispatcher.apply(
            SpawnReserved(
                world_id=info.world_id,
                entity_id=direct_id,
                components=(ComponentValue.from_component(CommandsOperationalMarker(value=1)),),
            )
        )
        deferred_id, _command_id = await dispatcher.defer_spawn_as(
            actor,
            Spawn.from_components(
                world_id=info.world_id,
                components=[CommandsOperationalMarker(value=2)],
            ),
            DurableOptions(target_tick=0),
        )

        assert [entity_id for _world, entity_id, _components in calls] == [direct_id]
        assert await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig())) == 1
        assert [entity_id for _world, entity_id, _components in calls] == [
            direct_id,
            deferred_id,
        ]
        assert calls[0][0] is calls[1][0]
        assert [
            component.value
            for _actual_world, _entity_id, components in calls
            for component in components
            if isinstance(component, CommandsOperationalMarker)
        ] == [1, 2]
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_due_spawn_creates_its_new_signature_in_the_due_tick(tmp_path) -> None:
    """A due spawn is visible to signature discovery and persistence in that tick."""

    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        storage = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="commands-due-tick",
        )
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="commands-due-tick"),
                storage_config=storage,
            )
        )
        entity_id, _command_id = await dispatcher.defer_spawn_as(
            actor,
            Spawn.from_components(
                world_id=info.world_id,
                components=[CommandsOperationalMarker(value=7)],
            ),
            DurableOptions(target_tick=0),
        )

        assert await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig())) == 1
        rows = (
            await dispatcher.apply(
                QueryComponents(
                    components=(ComponentTypeRef.from_type(CommandsOperationalMarker),),
                    world_id=info.world_id,
                    run_id=info.run_id,
                    storage_config=storage,
                )
            )
        ).to_pylist()
        assert [(row["entity_id"], row["tick"]) for row in rows] == [(entity_id, 0)]
        (record,) = await dispatcher._scheduler.records(info.world_id)
        assert record.status == "APPLIED"
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_actor_aware_denial_emits_one_bounded_redacted_access_row(
    tmp_path,
) -> None:
    """A full-policy denial records one payload-free access receipt."""

    audit_storage = StorageConfig(
        uri=str(tmp_path / "audit-store"),
        namespace="commands_operational",
        backend=StorageBackend.ICEBERG,
    )
    resources = build_test_runtime(
        tmp_path,
        audit_storage_config=audit_storage,
    )
    dispatcher = resources.dispatcher
    dispatcher._policy._max_tokens_per_day = 0
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        with pytest.raises(
            PermissionError,
            match="actor exceeded daily token budget",
        ):
            await dispatcher.apply_as(
                actor,
                CreateWorld(
                    config=WorldConfig(name="WORLD_NAME_SENTINEL"),
                    storage_config=StorageConfig(
                        uri=str(tmp_path / "STORAGE_URI_SENTINEL"),
                    ),
                ),
            )

        audit = dispatcher._record_access.__self__
        (evidence,) = audit._pending
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
        await resources.aclose()


@pytest.mark.asyncio
async def test_materializer_failure_leaves_tick_and_command_unsettled_for_retry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Infrastructure failure advances neither tick nor durable settlement."""

    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    actor = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="commands-materializer-failure"),
                storage_config=StorageConfig(
                    uri=str(tmp_path / "store"),
                    namespace="commands-materializer-failure",
                ),
            )
        )
        world = await _world_registry(dispatcher).live_world(str(info.world_id))
        assert world is not None
        await dispatcher.defer_spawn_as(
            actor,
            Spawn.from_components(
                world_id=info.world_id,
                components=[CommandsOperationalMarker(value=11)],
            ),
            DurableOptions(target_tick=0),
        )
        real_materialize = world._materialize_commands

        async def unavailable_materializer(_world, _tick):
            raise RuntimeError("command materializer unavailable")

        monkeypatch.setattr(world, "_materialize_commands", unavailable_materializer)
        with pytest.raises(RuntimeError, match="command materializer unavailable"):
            await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))

        assert world.tick == 0
        (pending,) = await dispatcher._scheduler.records(info.world_id)
        assert pending.status == "PENDING"
        assert world.entity2sig == {}

        monkeypatch.setattr(world, "_materialize_commands", real_materialize)
        assert await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig())) == 1
        (applied,) = await dispatcher._scheduler.records(info.world_id)
        assert applied.status == "APPLIED"
        assert applied.applied_tick == 0
    finally:
        await resources.aclose()


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
    storage_service = StorageService()
    resources = build_test_runtime(
        tmp_path,
        audit_storage_config=audit_storage,
        storage_service=storage_service,
    )
    dispatcher = resources.dispatcher
    try:
        world_storage = StorageConfig(
            uri=str(tmp_path / "world-store"),
            namespace="commands-post-stage-retry",
        )
        info = await dispatcher.apply(
            CreateWorld(
                config=WorldConfig(name="commands-post-stage-retry"),
                storage_config=world_storage,
            )
        )
        world = await _world_registry(dispatcher).live_world(str(info.world_id))
        assert world is not None
        (entity_id,) = await dispatcher.apply(ReserveEntityIds(world_id=info.world_id, count=1))
        command_id = await dispatcher.defer(
            SpawnReserved(
                world_id=info.world_id,
                entity_id=entity_id,
                components=(ComponentValue.from_component(CommandsOperationalMarker(value=41)),),
            ),
            DurableOptions(target_tick=0),
        )
        catalog = storage_service.get_control_catalog(world_storage)
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
            await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig()))

        (leased,) = await dispatcher._scheduler.records(info.world_id)
        assert leased.status == "LEASED"
        assert leased.command_id == str(command_id)
        signature = world.entity2sig[entity_id]
        assert (
            len([row for row in world.spawn_cache[signature] if row["entity_id"] == entity_id]) == 1
        )

        assert await dispatcher.apply(Step(world_id=info.world_id, run_config=RunConfig())) == 1
        (applied,) = await dispatcher._scheduler.records(info.world_id)
        assert applied.status == "APPLIED"
        assert applied.applied_tick == 0
        assert not [
            row for row in world.spawn_cache.get(signature, []) if row["entity_id"] == entity_id
        ]
    finally:
        await resources.aclose()
        await storage_service.shutdown()
