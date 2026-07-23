# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Red contracts for durable scheduling, settlement staging, and outbox audit."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from importlib import import_module
from typing import Any, ClassVar, Literal, NamedTuple

import daft
import pytest
from pydantic import BaseModel, ConfigDict, Field
from uuid_utils import uuid7

from archetype.core.config import StorageBackend, StorageConfig
from archetype.storage.catalog import CommandConflictError
from archetype.world.models import Spawn, SpawnReserved

pytestmark = [
    pytest.mark.contract("commands.identity.idempotent"),
    pytest.mark.contract("commands.settlement.atomic"),
    pytest.mark.contract("commands.failure.preserves_progress"),
]


class _SchedulerApi(NamedTuple):
    CommandScheduler: type[Any]
    DeferredItem: type[Any]
    DurableOptions: type[Any]


def _scheduler_api() -> _SchedulerApi:
    """Load the intentionally absent pre-PR-3 family after collection."""
    scheduler_module = import_module("archetype.commands.scheduler")
    models_module = import_module("archetype.commands.models")
    return _SchedulerApi(
        CommandScheduler=scheduler_module.CommandScheduler,
        DeferredItem=models_module.DeferredItem,
        DurableOptions=models_module.DurableOptions,
    )


def _audit_type() -> type[Any]:
    return import_module("archetype.commands.audit").AuditLog


class _PortableOperation(BaseModel):
    direct_only: ClassVar[bool] = False
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["portable"] = "portable"
    world_id: str
    label: str
    value: dict[str, Any] = Field(default_factory=dict)


class _UnregisteredOperation(BaseModel):
    direct_only: ClassVar[bool] = False
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["unregistered"] = "unregistered"
    world_id: str


class _ExplosiveCapability:
    def __repr__(self) -> str:
        raise AssertionError("live capability was serialized before rejection")


class _LiveOperation(BaseModel):
    direct_only: ClassVar[bool] = True
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    operation: Literal["live"] = "live"
    world_id: str
    callback: Any


class _ReservedSpawn(BaseModel):
    direct_only: ClassVar[bool] = False
    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["spawn_reserved"] = "spawn_reserved"
    world_id: str
    entity_id: int = Field(ge=0)
    components: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _Durable:
    decode: Callable[[str], BaseModel]
    materialize: Callable[[Any, BaseModel], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class _Spec:
    name: str
    model: type[BaseModel]
    durable: _Durable | None


class _OperationRegistry:
    def __init__(self, specs: tuple[_Spec, ...]) -> None:
        self._by_model = {spec.model: spec for spec in specs}
        self._by_name = {spec.name: spec for spec in specs}
        self.resolved_operations: list[BaseModel] = []
        self.resolved_names: list[str] = []

    def resolve(self, operation: BaseModel) -> _Spec:
        self.resolved_operations.append(operation)
        spec = self._by_model.get(type(operation))
        if spec is None:
            raise KeyError(f"{type(operation).__name__} is not registered")
        return spec

    def resolve_name(self, name: str) -> _Spec:
        self.resolved_names.append(name)
        spec = self._by_name.get(name)
        if spec is None:
            raise KeyError(f"operation name {name!r} is not registered")
        return spec


def _field(value: object, *names: str) -> Any:
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    raise AssertionError(f"{type(value).__name__} is missing all of {names!r}")


@dataclass(slots=True)
class _Record:
    command_id: str
    world_id: str
    sequence: int
    scheduled_tick: int
    priority: int
    operation_name: str
    command_type: str
    payload_json: str
    payload_digest: str
    version: int = 1
    principal_id: str | None = None
    origin: str = "local"
    reserved_entity_id: int | None = None
    status: str = "PENDING"
    attempts: int = 0
    max_attempts: int = 3
    lease_owner: str | None = None


class _Catalog:
    def __init__(
        self,
        *,
        lease_batches: list[list[_Record]] | None = None,
        admit_failure: Exception | None = None,
    ) -> None:
        self.records: dict[str, _Record] = {}
        self.admit_calls: list[tuple[str, tuple[object, ...]]] = []
        self.lease_calls: list[tuple[str, int, str, float, int]] = []
        self.failures: list[dict[str, str]] = []
        self.releases: list[tuple[str, tuple[str, ...], str]] = []
        self.lease_batches = list(lease_batches or [])
        self.admit_failure = admit_failure
        self._next_sequence = 0

    @staticmethod
    def _immutable(admission: object) -> tuple[Any, ...]:
        return (
            _field(admission, "scheduled_tick"),
            _field(admission, "priority"),
            _field(admission, "operation_name", "command_type"),
            _field(admission, "payload_json"),
            _field(admission, "payload_digest"),
            _field(admission, "version"),
            _field(admission, "principal_id"),
            _field(admission, "origin"),
            _field(admission, "max_attempts"),
        )

    async def admit_commands(
        self,
        world_id: str,
        admissions: list[object],
    ) -> list[_Record]:
        self.admit_calls.append((world_id, tuple(admissions)))
        if self.admit_failure is not None:
            raise self.admit_failure
        result: list[_Record] = []
        for admission in admissions:
            command_id = str(_field(admission, "command_id"))
            existing = self.records.get(command_id)
            if existing is not None:
                if (
                    existing.world_id != world_id
                    or self._immutable(existing) != self._immutable(admission)
                ):
                    raise CommandConflictError(
                        f"command {command_id} content conflicts with its durable identity"
                    )
                result.append(existing)
                continue
            operation_name = str(_field(admission, "operation_name", "command_type"))
            record = _Record(
                command_id=command_id,
                world_id=world_id,
                sequence=self._next_sequence,
                scheduled_tick=int(_field(admission, "scheduled_tick")),
                priority=int(_field(admission, "priority")),
                operation_name=operation_name,
                command_type=operation_name,
                payload_json=str(_field(admission, "payload_json")),
                payload_digest=str(_field(admission, "payload_digest")),
                version=int(_field(admission, "version")),
                principal_id=_field(admission, "principal_id"),
                origin=str(_field(admission, "origin")),
                reserved_entity_id=getattr(admission, "reserved_entity_id", None),
                max_attempts=int(_field(admission, "max_attempts")),
            )
            self._next_sequence += 1
            self.records[command_id] = record
            result.append(record)
        return result

    async def lease_commands(
        self,
        world_id: str,
        tick: int,
        owner: str,
        *,
        lease_seconds: float,
        limit: int,
    ) -> list[_Record]:
        self.lease_calls.append((world_id, tick, owner, lease_seconds, limit))
        if self.lease_batches:
            records = self.lease_batches.pop(0)
        else:
            records = [
                record
                for record in self.records.values()
                if record.scheduled_tick <= tick
                and record.status in {"PENDING", "RETRYABLE", "LEASED"}
            ][:limit]
        for record in records:
            record.status = "LEASED"
            record.lease_owner = owner
            if record.attempts == 0:
                record.attempts = 1
        return list(records)

    async def fail_command(
        self,
        world_id: str,
        command_id: str,
        owner: str,
        *,
        status: str,
        error_code: str,
        error_detail: str,
    ) -> _Record:
        record = self.records[command_id]
        record.status = status
        self.failures.append(
            {
                "world_id": world_id,
                "command_id": command_id,
                "owner": owner,
                "status": status,
                "error_code": error_code,
                "error_detail": error_detail,
            }
        )
        return record

    async def release_commands(
        self,
        world_id: str,
        command_ids: list[str],
        owner: str,
    ) -> None:
        self.releases.append((world_id, tuple(command_ids), owner))
        for command_id in command_ids:
            record = self.records[command_id]
            record.status = "PENDING"
            record.lease_owner = None

    async def publish_manifest(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("scheduler must not settle before manifest publication")


@dataclass(slots=True)
class _Coordinator:
    staged: dict[int, list[str]] = field(default_factory=dict)
    owners: dict[int, str] = field(default_factory=dict)

    def stage_command(self, tick: int, owner: str, command_id: str) -> None:
        previous = self.owners.setdefault(tick, owner)
        if previous != owner:
            raise AssertionError("one tick cannot mix scheduler lease owners")
        values = self.staged.setdefault(tick, [])
        if command_id not in values:
            values.append(command_id)

    def is_command_staged(self, tick: int, command_id: str) -> bool:
        return command_id in self.staged.get(tick, ())

    async def settle_command(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("scheduler cannot settle outside manifest publication")


@dataclass(slots=True)
class _World:
    world_id: str
    commit_coordinator: _Coordinator

    def operation(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("materialization must not reacquire a world operation")


def _canonical(operation: BaseModel) -> str:
    return json.dumps(
        operation.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _record(
    operation: BaseModel,
    *,
    command_id: str,
    scheduled_tick: int,
    priority: int,
    sequence: int,
    attempts: int = 1,
    max_attempts: int = 3,
) -> _Record:
    payload_json = _canonical(operation)
    world_id = str(_field(operation, "world_id"))
    operation_name = str(_field(operation, "operation"))
    return _Record(
        command_id=command_id,
        world_id=world_id,
        sequence=sequence,
        scheduled_tick=scheduled_tick,
        priority=priority,
        operation_name=operation_name,
        command_type=operation_name,
        payload_json=payload_json,
        payload_digest=hashlib.sha256(payload_json.encode()).hexdigest(),
        status="LEASED",
        attempts=attempts,
        max_attempts=max_attempts,
    )


async def _catalog_resolver(
    catalog: _Catalog,
    resolutions: list[str],
    world_id: str,
) -> _Catalog:
    resolutions.append(str(world_id))
    return catalog


def _scheduler(
    api: _SchedulerApi,
    *,
    registry: _OperationRegistry,
    catalog: _Catalog,
    resolutions: list[str] | None = None,
    owner: str = "scheduler-test",
    reserve_entity_ids: Callable[[object, int], Awaitable[list[int]]] | None = None,
) -> Any:
    catalog_resolutions = resolutions if resolutions is not None else []

    async def catalog_for_world(world_id: str) -> _Catalog:
        return await _catalog_resolver(catalog, catalog_resolutions, world_id)

    return api.CommandScheduler(
        registry=registry,
        catalog_for_world=catalog_for_world,
        owner=owner,
        lease_seconds=30.0,
        max_dequeue=100,
        reserve_entity_ids=reserve_entity_ids,
    )


def _options(
    api: _SchedulerApi,
    *,
    target_tick: int,
    priority: int = 0,
    max_attempts: int = 3,
) -> Any:
    return api.DurableOptions(
        target_tick=target_tick,
        priority=priority,
        max_attempts=max_attempts,
    )


def _portable_spec(
    materialize: Callable[[Any, BaseModel], Awaitable[None]],
) -> _Spec:
    return _Spec(
        name="portable",
        model=_PortableOperation,
        durable=_Durable(
            decode=_PortableOperation.model_validate_json,
            materialize=materialize,
        ),
    )


@pytest.mark.asyncio
async def test_identical_admission_is_idempotent_and_changed_content_conflicts() -> None:
    api = _scheduler_api()

    async def materialize(_world: object, _operation: BaseModel) -> None:
        return None

    registry = _OperationRegistry((_portable_spec(materialize),))
    catalog = _Catalog()
    resolutions: list[str] = []
    scheduler = _scheduler(
        api,
        registry=registry,
        catalog=catalog,
        resolutions=resolutions,
    )
    operation = _PortableOperation(
        world_id="world-idempotent",
        label="same",
        value={"z": 2, "a": 1},
    )
    options = _options(api, target_tick=11, priority=4, max_attempts=5)
    command_id = uuid7()
    principal_id = uuid7()

    first = await scheduler.admit(
        operation,
        options,
        command_id=command_id,
        principal_id=principal_id,
        origin="gateway",
        version=1,
    )
    replay = await scheduler.admit(
        operation,
        options,
        command_id=command_id,
        principal_id=principal_id,
        origin="gateway",
        version=1,
    )

    assert str(first) == str(replay) == str(command_id)
    assert len(catalog.records) == 1
    assert len(catalog.admit_calls) == 2
    assert resolutions
    assert set(resolutions) == {"world-idempotent"}
    first_admission = catalog.admit_calls[0][1][0]
    replay_admission = catalog.admit_calls[1][1][0]
    assert _field(first_admission, "payload_json") == (
        '{"label":"same","operation":"portable","value":{"a":1,"z":2},'
        '"world_id":"world-idempotent"}'
    )
    assert _field(replay_admission, "payload_json") == _field(
        first_admission,
        "payload_json",
    )
    assert _field(replay_admission, "payload_digest") == _field(
        first_admission,
        "payload_digest",
    )
    assert len(_field(first_admission, "payload_digest")) == 64
    assert _field(first_admission, "scheduled_tick") == 11
    assert _field(first_admission, "priority") == 4
    assert _field(first_admission, "operation_name", "command_type") == "portable"
    assert _field(first_admission, "version") == 1
    assert _field(first_admission, "principal_id") == str(principal_id)
    assert _field(first_admission, "origin") == "gateway"
    assert _field(first_admission, "max_attempts") == 5
    assert "command_id" not in json.loads(_field(first_admission, "payload_json"))

    changed = operation.model_copy(update={"value": {"a": 999}})
    with pytest.raises(CommandConflictError):
        await scheduler.admit(
            changed,
            options,
            command_id=command_id,
            principal_id=principal_id,
            origin="gateway",
            version=1,
        )


@pytest.mark.asyncio
async def test_unregistered_and_direct_only_reject_before_catalog_persistence() -> None:
    api = _scheduler_api()

    async def materialize(_world: object, _operation: BaseModel) -> None:
        return None

    registry = _OperationRegistry(
        (
            _portable_spec(materialize),
            _Spec(name="live", model=_LiveOperation, durable=None),
        )
    )
    catalog = _Catalog()
    scheduler = _scheduler(api, registry=registry, catalog=catalog)
    options = _options(api, target_tick=3)

    with pytest.raises(KeyError, match=r"(?i)UnregisteredOperation.*not registered"):
        await scheduler.admit(
            _UnregisteredOperation(world_id="world-rejected"),
            options,
            command_id=uuid7(),
        )

    with pytest.raises(ValueError, match=r"(?i)direct-only"):
        await scheduler.admit(
            _LiveOperation(
                world_id="world-rejected",
                callback=_ExplosiveCapability(),
            ),
            options,
            command_id=uuid7(),
        )

    assert catalog.admit_calls == []
    assert catalog.records == {}


@pytest.mark.asyncio
async def test_lease_order_uses_actual_world_and_only_stages_before_manifest() -> None:
    api = _scheduler_api()
    seen: list[tuple[_World, _PortableOperation]] = []

    async def materialize(world: _World, operation: BaseModel) -> None:
        assert type(operation) is _PortableOperation
        seen.append((world, operation))

    registry = _OperationRegistry((_portable_spec(materialize),))
    operations = {
        "a": _PortableOperation(world_id="world-order", label="a"),
        "b": _PortableOperation(world_id="world-order", label="b"),
        "c": _PortableOperation(world_id="world-order", label="c"),
    }
    records = {
        "a": _record(
            operations["a"],
            command_id="command-a",
            scheduled_tick=1,
            priority=0,
            sequence=1,
        ),
        "b": _record(
            operations["b"],
            command_id="command-b",
            scheduled_tick=0,
            priority=9,
            sequence=2,
        ),
        "c": _record(
            operations["c"],
            command_id="command-c",
            scheduled_tick=1,
            priority=0,
            sequence=3,
        ),
    }
    catalog = _Catalog(lease_batches=[[records["c"], records["a"], records["b"]]])
    catalog.records.update({record.command_id: record for record in records.values()})
    scheduler = _scheduler(api, registry=registry, catalog=catalog)
    coordinator = _Coordinator()
    world = _World(world_id="world-order", commit_coordinator=coordinator)

    assert await scheduler.materialize(world, 1) == 3

    assert [operation.label for _world, operation in seen] == ["b", "a", "c"]
    assert all(actual_world is world for actual_world, _operation in seen)
    assert coordinator.staged == {
        1: ["command-b", "command-a", "command-c"],
    }
    assert [record.status for record in records.values()] == [
        "LEASED",
        "LEASED",
        "LEASED",
    ]
    assert catalog.failures == []
    assert registry.resolved_names == ["portable", "portable", "portable"]
    assert catalog.lease_calls == [("world-order", 1, "scheduler-test", 30.0, 100)]


@pytest.mark.asyncio
async def test_failure_classification_releases_transient_tail_then_dead_letters() -> None:
    api = _scheduler_api()
    calls: dict[str, int] = {}

    async def materialize(_world: _World, operation: BaseModel) -> None:
        assert type(operation) is _PortableOperation
        calls[operation.label] = calls.get(operation.label, 0) + 1
        if operation.label == "permanent":
            raise ValueError("invalid portable payload")
        if operation.label == "transient" and calls[operation.label] == 1:
            raise RuntimeError("temporary catalog outage")
        if operation.label == "exhausted":
            raise RuntimeError("still unavailable")

    registry = _OperationRegistry((_portable_spec(materialize),))
    permanent = _record(
        _PortableOperation(world_id="world-failures", label="permanent"),
        command_id="permanent",
        scheduled_tick=0,
        priority=0,
        sequence=0,
    )
    transient = _record(
        _PortableOperation(world_id="world-failures", label="transient"),
        command_id="transient",
        scheduled_tick=0,
        priority=1,
        sequence=1,
        attempts=1,
        max_attempts=3,
    )
    tail = _record(
        _PortableOperation(world_id="world-failures", label="tail"),
        command_id="tail",
        scheduled_tick=0,
        priority=2,
        sequence=2,
    )
    exhausted = _record(
        _PortableOperation(world_id="world-failures", label="exhausted"),
        command_id="exhausted",
        scheduled_tick=0,
        priority=3,
        sequence=3,
        attempts=3,
        max_attempts=3,
    )
    catalog = _Catalog(
        lease_batches=[
            [permanent, transient, tail, exhausted],
            [transient, tail, exhausted],
        ]
    )
    catalog.records.update(
        {record.command_id: record for record in (permanent, transient, tail, exhausted)}
    )
    scheduler = _scheduler(api, registry=registry, catalog=catalog)
    coordinator = _Coordinator()
    world = _World(world_id="world-failures", commit_coordinator=coordinator)

    assert await scheduler.materialize(world, 0) == 0
    assert [(item["command_id"], item["status"]) for item in catalog.failures] == [
        ("permanent", "REJECTED"),
        ("transient", "RETRYABLE"),
    ]
    assert catalog.releases == [
        (
            "world-failures",
            ("tail", "exhausted"),
            "scheduler-test",
        )
    ]
    assert calls == {"permanent": 1, "transient": 1}

    assert await scheduler.materialize(world, 0) == 2
    assert [(item["command_id"], item["status"]) for item in catalog.failures] == [
        ("permanent", "REJECTED"),
        ("transient", "RETRYABLE"),
        ("exhausted", "DEAD_LETTER"),
    ]
    assert calls == {
        "permanent": 1,
        "transient": 2,
        "tail": 1,
        "exhausted": 1,
    }
    assert coordinator.staged == {0: ["transient", "tail"]}


@pytest.mark.asyncio
async def test_already_staged_retry_skips_behavior_but_counts_applied() -> None:
    api = _scheduler_api()

    async def must_not_materialize(_world: object, _operation: BaseModel) -> None:
        raise AssertionError("already-staged command behavior was replayed")

    registry = _OperationRegistry((_portable_spec(must_not_materialize),))
    record = _record(
        _PortableOperation(world_id="world-staged", label="staged"),
        command_id="already-staged",
        scheduled_tick=7,
        priority=0,
        sequence=0,
    )
    catalog = _Catalog(lease_batches=[[record]])
    catalog.records[record.command_id] = record
    scheduler = _scheduler(api, registry=registry, catalog=catalog)
    coordinator = _Coordinator()
    coordinator.stage_command(7, "scheduler-test", record.command_id)
    world = _World(world_id="world-staged", commit_coordinator=coordinator)

    assert await scheduler.materialize(world, 7) == 1
    assert coordinator.staged == {7: ["already-staged"]}
    assert catalog.failures == []


@pytest.mark.asyncio
async def test_reserved_spawn_identity_survives_admission_decode_and_staging() -> None:
    api = _scheduler_api()
    seen: list[tuple[_World, int]] = []

    async def materialize(world: _World, operation: BaseModel) -> None:
        assert type(operation) is _ReservedSpawn
        seen.append((world, operation.entity_id))

    registry = _OperationRegistry(
        (
            _Spec(
                name="spawn_reserved",
                model=_ReservedSpawn,
                durable=_Durable(
                    decode=_ReservedSpawn.model_validate_json,
                    materialize=materialize,
                ),
            ),
        )
    )
    catalog = _Catalog()
    scheduler = _scheduler(api, registry=registry, catalog=catalog)
    operation = _ReservedSpawn(
        world_id="world-reserved",
        entity_id=41,
        components=("Marker",),
    )
    command_id = uuid7()
    options = _options(api, target_tick=4, priority=-10)

    admitted = await scheduler.admit(
        operation,
        options,
        command_id=command_id,
    )
    admission = catalog.admit_calls[0][1][0]
    assert str(admitted) == str(command_id)
    assert json.loads(_field(admission, "payload_json"))["entity_id"] == 41

    coordinator = _Coordinator()
    world = _World(world_id="world-reserved", commit_coordinator=coordinator)
    assert await scheduler.materialize(world, 4) == 1
    assert seen == [(world, 41)]
    assert coordinator.staged == {4: [str(command_id)]}


@dataclass(frozen=True, slots=True)
class _OutboxEvent:
    sequence: int
    event_id: str
    world_id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    command_type: str
    status: str
    actor_id: str | None
    payload_json: str
    occurred_at: str
    projected_at: str | None = None


class _OutboxSource:
    def __init__(self, events: tuple[_OutboxEvent, ...], order: list[str]) -> None:
        self.events = events
        self.order = order
        self.acknowledged: set[str] = set()
        self.ack_calls: list[tuple[str, ...]] = []
        self.fail_ack = False

    async def read(self, *, limit: int = 1000) -> list[_OutboxEvent]:
        return [event for event in self.events if event.event_id not in self.acknowledged][:limit]

    async def acknowledge(self, events: list[_OutboxEvent]) -> None:
        self.order.append("ack")
        event_ids = tuple(event.event_id for event in events)
        self.ack_calls.append(event_ids)
        if self.fail_ack:
            raise RuntimeError("ack response lost")
        self.acknowledged.update(event_ids)


class _AuditStorage:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.rows: list[dict[str, Any]] = []
        self.fail_append = False

    async def append_table(
        self,
        _config: StorageConfig,
        _table_name: str,
        frame: daft.DataFrame,
        **_kwargs: Any,
    ) -> None:
        self.order.append("append")
        if self.fail_append:
            raise RuntimeError("audit append failed")
        self.rows.extend(frame.to_pylist())

    async def read_table(
        self,
        _config: StorageConfig,
        _table_name: str,
    ) -> daft.DataFrame:
        if not self.rows:
            raise KeyError("audit table does not exist")
        return daft.from_pylist(self.rows)

    async def materialize(self, frame: daft.DataFrame) -> daft.DataFrame:
        return frame.collect()


def _audit_storage_config() -> StorageConfig:
    return StorageConfig(
        uri="/tmp/archetype-synthetic-command-audit",
        namespace="scheduler_audit_contract",
        backend=StorageBackend.ICEBERG,
    )


@pytest.mark.asyncio
async def test_outbox_append_precedes_ack_replay_dedupes_and_failure_never_acks() -> None:
    AuditLog = _audit_type()
    order: list[str] = []
    events = tuple(
        _OutboxEvent(
            sequence=index,
            event_id=str(uuid7()),
            world_id="world-outbox",
            aggregate_type="command",
            aggregate_id=str(uuid7()),
            event_type=f"command.{status}",
            command_type="portable",
            status=status,
            actor_id=None,
            payload_json="{}",
            occurred_at=f"2026-07-23T00:00:0{index}Z",
        )
        for index, status in enumerate(("queued", "applied"), start=1)
    )
    source = _OutboxSource(events, order)
    storage = _AuditStorage(order)
    audit = AuditLog(
        storage,
        _audit_storage_config(),
        read_outbox=source.read,
        acknowledge_outbox=source.acknowledge,
        flush_rows=100,
    )

    source.fail_ack = True
    with pytest.raises(RuntimeError, match="ack response lost"):
        await audit.project_outbox()
    assert order == ["append", "ack"]
    assert len(storage.rows) == 2
    assert source.acknowledged == set()

    source.fail_ack = False
    assert await audit.project_outbox() == 2
    assert order == ["append", "ack", "append", "ack"]
    assert len(storage.rows) == 4, "physical replay is allowed after ack ambiguity"
    assert source.acknowledged == {event.event_id for event in events}

    rows = (await audit.query(world_id="world-outbox")).to_pylist()
    assert len(rows) == 2
    assert {row["audit_id"] for row in rows} == {event.event_id for event in events}

    failed_order: list[str] = []
    failed_source = _OutboxSource(events, failed_order)
    failed_storage = _AuditStorage(failed_order)
    failed_storage.fail_append = True
    failed_audit = AuditLog(
        failed_storage,
        _audit_storage_config(),
        read_outbox=failed_source.read,
        acknowledge_outbox=failed_source.acknowledge,
        flush_rows=100,
    )

    with pytest.raises(RuntimeError, match="audit append failed"):
        await failed_audit.project_outbox()
    assert failed_order == ["append"]
    assert failed_source.ack_calls == []
    assert failed_source.acknowledged == set()
