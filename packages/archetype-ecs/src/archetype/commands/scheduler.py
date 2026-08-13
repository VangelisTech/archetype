# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Registry-driven durable command admission and tick materialization."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel
from uuid_utils import UUID, uuid7

from archetype.commands.models import DeferredItem, DurableOptions
from archetype.commands.registry import (
    OperationRegistry,
    OperationSpec,
    canonical_operation_json,
    decode_canonical_operation,
    encode_canonical_operation,
)
from archetype.storage.catalog import (
    CommandAdmission,
    CommandConflictError,
    CommandRecord,
    ControlCatalog,
    OutboxRecord,
)
from archetype.world.models import Spawn, SpawnReserved

if TYPE_CHECKING:
    from archetype.core.aio.async_world import AsyncWorld

_COMMAND_DIGEST_DOMAIN = "archetype.command.v2"

CatalogForWorld = Callable[[str], Awaitable[ControlCatalog]]
ReserveEntityIds = Callable[[object, int], Awaitable[list[int]]]


@dataclass(frozen=True, slots=True)
class _PreparedAdmission:
    """One fully validated admission and its extracted durable world."""

    world_id: str
    command_id: UUID
    operation: BaseModel
    spec: OperationSpec
    admission: CommandAdmission


@dataclass(frozen=True, slots=True)
class _ReservationIntent:
    """The one reserved ID retained for a caller-supplied command identity."""

    request_digest: str
    operation: SpawnReserved


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _world_key(spec: object, operation: BaseModel) -> str:
    extractor = getattr(spec, "world_key", None)
    value = extractor(operation) if callable(extractor) else getattr(operation, "world_id", None)
    if value is None:
        raise ValueError(f"durable operation {type(operation).__name__} has no world key")
    key = str(value)
    if not key:
        raise ValueError("durable operation world key must not be empty")
    return key


def _identity_document(
    *,
    world_id: str,
    scheduled_tick: int,
    priority: int,
    operation_name: str,
    payload_json: str,
    version: int,
    principal_id: str | None,
    origin: str,
    reserved_entity_id: int | None,
    max_attempts: int,
) -> str:
    """Serialize every immutable ledger field into one catalog conflict key."""
    return _canonical_json(
        {
            "domain": _COMMAND_DIGEST_DOMAIN,
            "world_id": world_id,
            "scheduled_tick": scheduled_tick,
            "priority": priority,
            "operation_name": operation_name,
            "payload_json": payload_json,
            "version": version,
            "principal_id": principal_id,
            "origin": origin,
            "reserved_entity_id": reserved_entity_id,
            "max_attempts": max_attempts,
        }
    )


def _identity_digest(
    *,
    world_id: str,
    scheduled_tick: int,
    priority: int,
    operation_name: str,
    payload_json: str,
    version: int,
    principal_id: str | None,
    origin: str,
    reserved_entity_id: int | None,
    max_attempts: int,
) -> str:
    return _sha256(
        _identity_document(
            world_id=world_id,
            scheduled_tick=scheduled_tick,
            priority=priority,
            operation_name=operation_name,
            payload_json=payload_json,
            version=version,
            principal_id=principal_id,
            origin=origin,
            reserved_entity_id=reserved_entity_id,
            max_attempts=max_attempts,
        )
    )


def _payload_digest(payload_json: str) -> str:
    """Return the pre-commands-family payload-only digest for read compatibility."""
    return _sha256(payload_json)


def _record_name(record: object) -> str:
    physical_name = getattr(record, "command_type", None)
    operation_name = getattr(record, "operation_name", physical_name)
    if not isinstance(operation_name, str) or not operation_name:
        raise ValueError("durable command record has no operation name")
    if not isinstance(physical_name, str) or physical_name != operation_name:
        raise ValueError("durable command record operation-name fields disagree")
    return operation_name


def _record_identity_digest(record: object, *, operation_name: str) -> str:
    value = cast("Any", record)
    return _identity_digest(
        world_id=str(value.world_id),
        scheduled_tick=int(value.scheduled_tick),
        priority=int(value.priority),
        operation_name=operation_name,
        payload_json=str(value.payload_json),
        version=int(value.version),
        principal_id=(
            None if getattr(value, "principal_id", None) is None else str(value.principal_id)
        ),
        origin=str(value.origin),
        reserved_entity_id=getattr(value, "reserved_entity_id", None),
        max_attempts=int(value.max_attempts),
    )


def _validate_scalar_metadata(
    options: DurableOptions,
    *,
    version: int,
    origin: str,
) -> None:
    if type(version) is not int or version < 1:
        raise ValueError("durable command version must be a positive integer")
    if not isinstance(origin, str) or not origin:
        raise ValueError("durable command origin must be a non-empty string")
    # DurableOptions normally enforces these constraints. Repeating them here
    # keeps the scheduler fail-closed for protocol-compatible test doubles.
    if type(options.target_tick) is not int or options.target_tick < 0:
        raise ValueError("target_tick must be a non-negative integer")
    if type(options.priority) is not int:
        raise TypeError("priority must be an integer")
    if type(options.max_attempts) is not int or options.max_attempts < 1:
        raise ValueError("max_attempts must be a positive integer")


class CommandScheduler:
    """Own durable admission, leases, failure policy, and staging.

    The scheduler never resolves or locks a world. ``materialize`` receives the
    already-locked world from the tick engine and only stages successful
    command IDs on that world's commit coordinator. Manifest publication is
    the sole settlement transaction.
    """

    def __init__(
        self,
        *,
        registry: OperationRegistry,
        catalog_for_world: CatalogForWorld,
        owner: str | None = None,
        lease_seconds: float = 30.0,
        max_dequeue: int = 100,
        reserve_entity_ids: ReserveEntityIds | None = None,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if max_dequeue < 1:
            raise ValueError("max_dequeue must be positive")
        self._registry = registry
        self._catalog_for_world = catalog_for_world
        self._reserve_entity_ids = reserve_entity_ids
        self._lease_seconds = float(lease_seconds)
        self._max_dequeue = max_dequeue
        self._owner = owner or f"{socket.gethostname()}:{os.getpid()}:{uuid7().hex[:12]}"

        self._catalogs: dict[str, ControlCatalog] = {}
        self._catalog_lock = asyncio.Lock()

        # The physical catalog uses payload_digest as its immutable conflict
        # key. New admissions hash every immutable field into that column.
        # This process-local index additionally closes cross-world catalog
        # races and pins a failed first admission to the same identity.
        self._admission_identities: dict[str, str] = {}
        self._identity_lock = asyncio.Lock()

        self._reservation_requests: dict[str, str] = {}
        self._reservation_intents: dict[str, _ReservationIntent] = {}
        self._reservation_tasks: dict[str, asyncio.Future[list[int]]] = {}
        self._reservation_locks: dict[str, asyncio.Lock] = {}

    @property
    def owner(self) -> str:
        """Return the stable lease owner used for staging and settlement."""
        return self._owner

    async def _catalog(self, world_id: object) -> ControlCatalog:
        key = str(world_id)
        cached = self._catalogs.get(key)
        if cached is not None:
            return cached
        async with self._catalog_lock:
            cached = self._catalogs.get(key)
            if cached is None:
                cached = await self._catalog_for_world(key)
                self._catalogs[key] = cached
            return cached

    def _prepare(
        self,
        operation: BaseModel,
        options: DurableOptions,
        *,
        command_id: UUID | None,
        principal_id: str | UUID | None,
        origin: str,
        version: int,
        reserved_entity_id: int | None = None,
    ) -> _PreparedAdmission:
        """Resolve, decode, and canonicalize before any catalog side effect."""
        _validate_scalar_metadata(options, version=version, origin=origin)
        spec = self._registry.resolve(operation)
        name = spec.name
        payload_json = encode_canonical_operation(spec, operation)
        world_id = _world_key(spec, operation)
        principal = str(principal_id) if principal_id is not None else None
        identity = command_id or uuid7()
        if not isinstance(identity, UUID):
            identity = UUID(str(identity))
        digest = _identity_digest(
            world_id=world_id,
            scheduled_tick=options.target_tick,
            priority=options.priority,
            operation_name=name,
            payload_json=payload_json,
            version=version,
            principal_id=principal,
            origin=origin,
            reserved_entity_id=reserved_entity_id,
            max_attempts=options.max_attempts,
        )
        return _PreparedAdmission(
            world_id=world_id,
            command_id=identity,
            operation=operation,
            spec=spec,
            admission=CommandAdmission(
                command_id=str(identity),
                scheduled_tick=options.target_tick,
                priority=options.priority,
                command_type=name,
                payload_json=payload_json,
                payload_digest=digest,
                version=version,
                principal_id=principal,
                origin=origin,
                reserved_entity_id=reserved_entity_id,
                max_attempts=options.max_attempts,
            ),
        )

    async def _claim_identities(
        self,
        prepared: Sequence[_PreparedAdmission],
        *,
        reservation_claims: Mapping[str, str] | None = None,
    ) -> None:
        """Atomically pin every command ID before attempting persistence."""
        incoming: dict[str, str] = {}
        for item in prepared:
            key = str(item.command_id)
            digest = item.admission.payload_digest
            prior = incoming.get(key)
            if prior is not None and prior != digest:
                raise CommandConflictError(
                    f"command {key} appears twice with different immutable content"
                )
            incoming[key] = digest

        allowed_reservations = reservation_claims or {}
        async with self._identity_lock:
            for key, digest in incoming.items():
                reserved_request = self._reservation_requests.get(key)
                if (
                    reserved_request is not None
                    and allowed_reservations.get(key) != reserved_request
                ):
                    raise CommandConflictError(
                        f"command {key} is already owned by a reserved-spawn intent"
                    )
                existing = self._admission_identities.get(key)
                if existing is not None and existing != digest:
                    raise CommandConflictError(
                        f"command {key} content conflicts with its durable identity"
                    )
            self._admission_identities.update(incoming)

    async def _admit_prepared(
        self,
        prepared: Sequence[_PreparedAdmission],
        *,
        reservation_claims: Mapping[str, str] | None = None,
    ) -> list[UUID]:
        if not prepared:
            raise ValueError("durable command batch must not be empty")
        world_id = prepared[0].world_id
        if any(item.world_id != world_id for item in prepared[1:]):
            raise ValueError("one durable command batch cannot span worlds")

        await self._claim_identities(prepared, reservation_claims=reservation_claims)
        catalog = await self._catalog(world_id)
        records = await catalog.admit_commands(
            world_id,
            [item.admission for item in prepared],
        )
        if len(records) != len(prepared):
            raise RuntimeError("control catalog returned an incomplete admission batch")
        result: list[UUID] = []
        for expected, record in zip(prepared, records, strict=True):
            if str(record.command_id) != str(expected.command_id):
                raise RuntimeError("control catalog changed an admitted command identity")
            result.append(UUID(str(record.command_id)))
        return result

    async def admit(
        self,
        operation: BaseModel,
        options: DurableOptions,
        *,
        command_id: UUID | None = None,
        principal_id: str | UUID | None = None,
        origin: str = "local",
        version: int = 1,
    ) -> UUID:
        """Persist one exact portable operation before returning its identity."""
        prepared = self._prepare(
            operation,
            options,
            command_id=command_id,
            principal_id=principal_id,
            origin=origin,
            version=version,
        )
        return (await self._admit_prepared((prepared,)))[0]

    async def admit_batch(
        self,
        items: tuple[DeferredItem, ...],
        *,
        principal_id: str | UUID | None = None,
        origin: str = "local",
    ) -> list[UUID]:
        """Canonicalize a same-world batch, then make one catalog call."""
        if not items:
            raise ValueError("durable command batch must not be empty")
        prepared = tuple(
            self._prepare(
                item.operation,
                item.options,
                command_id=item.command_id,
                principal_id=principal_id,
                origin=origin,
                version=item.version,
            )
            for item in items
        )
        return await self._admit_prepared(prepared)

    async def _reservation_lock(self, command_id: str) -> asyncio.Lock:
        async with self._identity_lock:
            return self._reservation_locks.setdefault(command_id, asyncio.Lock())

    @staticmethod
    def _spawn_request_digest(
        operation: Spawn,
        options: DurableOptions,
        *,
        principal_id: str | UUID | None,
        origin: str,
        version: int,
    ) -> str:
        return _sha256(
            _canonical_json(
                {
                    "domain": "archetype.command.spawn-reservation.v1",
                    "operation": json.loads(canonical_operation_json(operation)),
                    "target_tick": options.target_tick,
                    "priority": options.priority,
                    "max_attempts": options.max_attempts,
                    "principal_id": (None if principal_id is None else str(principal_id)),
                    "origin": origin,
                    "version": version,
                }
            )
        )

    async def _reserved_operation(
        self,
        operation: Spawn,
        options: DurableOptions,
        *,
        command_id: UUID,
        principal_id: str | UUID | None,
        origin: str,
        version: int,
    ) -> tuple[SpawnReserved, str]:
        if self._reserve_entity_ids is None:
            raise RuntimeError("reserved-spawn admission has no reservation capability")
        if type(operation) is not Spawn:
            raise TypeError("admit_spawn requires the exact world Spawn model")
        _validate_scalar_metadata(options, version=version, origin=origin)

        # Resolve exact durable eligibility before reserving any world state.
        spec = self._registry.resolve_name("spawn_reserved")
        if spec.model is not SpawnReserved or spec.durable is None:
            raise ValueError("spawn_reserved is not registered as an exact durable operation")

        key = str(command_id)
        request_digest = self._spawn_request_digest(
            operation,
            options,
            principal_id=principal_id,
            origin=origin,
            version=version,
        )
        lock = await self._reservation_lock(key)
        async with lock:
            async with self._identity_lock:
                existing_request = self._reservation_requests.get(key)
                if existing_request is not None and existing_request != request_digest:
                    raise CommandConflictError(
                        f"command {key} reservation conflicts with its immutable identity"
                    )
                existing_intent = self._reservation_intents.get(key)
                if existing_intent is not None:
                    return existing_intent.operation, request_digest
                if key in self._admission_identities and existing_request is None:
                    raise CommandConflictError(
                        f"command {key} is already used by another durable admission"
                    )
                self._reservation_requests[key] = request_digest
                task = self._reservation_tasks.get(key)
                if task is not None and task.cancelled():
                    self._reservation_tasks.pop(key, None)
                    task = None
                if task is None:
                    task = asyncio.ensure_future(self._reserve_entity_ids(operation.world_id, 1))
                    self._reservation_tasks[key] = task

            try:
                entity_ids = await asyncio.shield(task)
            except asyncio.CancelledError:
                # Caller cancellation leaves the shielded reservation live and
                # owned for an identical retry. A reservation task that
                # cancelled itself is terminal, so retaining it would pin every
                # later retry to the same CancelledError forever.
                if task.cancelled():
                    async with self._identity_lock:
                        if self._reservation_tasks.get(key) is task:
                            self._reservation_tasks.pop(key, None)
                raise
            except BaseException:
                async with self._identity_lock:
                    if self._reservation_tasks.get(key) is task:
                        self._reservation_tasks.pop(key, None)
                raise

            if len(entity_ids) != 1 or type(entity_ids[0]) is not int or entity_ids[0] < 0:
                async with self._identity_lock:
                    if self._reservation_tasks.get(key) is task:
                        self._reservation_tasks.pop(key, None)
                raise RuntimeError("reservation capability must return one non-negative integer")

            reserved = SpawnReserved(
                world_id=operation.world_id,
                entity_id=entity_ids[0],
                components=operation.components,
            )
            intent = _ReservationIntent(
                request_digest=request_digest,
                operation=reserved,
            )
            async with self._identity_lock:
                current = self._reservation_intents.get(key)
                if current is not None and current != intent:
                    raise CommandConflictError(
                        f"command {key} acquired conflicting reservation intents"
                    )
                self._reservation_intents[key] = intent
                self._reservation_tasks.pop(key, None)
            return reserved, request_digest

    async def admit_spawn(
        self,
        operation: Spawn,
        options: DurableOptions,
        *,
        command_id: UUID | None = None,
        principal_id: str | UUID | None = None,
        origin: str = "local",
        version: int = 1,
    ) -> tuple[int, UUID]:
        """Reserve once, then admit the exact family-owned SpawnReserved."""
        identity = command_id or uuid7()
        if not isinstance(identity, UUID):
            identity = UUID(str(identity))
        reserved, request_digest = await self._reserved_operation(
            operation,
            options,
            command_id=identity,
            principal_id=principal_id,
            origin=origin,
            version=version,
        )
        prepared = self._prepare(
            reserved,
            options,
            command_id=identity,
            principal_id=principal_id,
            origin=origin,
            version=version,
            reserved_entity_id=reserved.entity_id,
        )
        admitted = (
            await self._admit_prepared(
                (prepared,),
                reservation_claims={str(identity): request_digest},
            )
        )[0]
        return reserved.entity_id, admitted

    def _decode_record(
        self,
        record: object,
        *,
        actual_world_id: str,
    ) -> tuple[OperationSpec, BaseModel]:
        """Verify every stored identity field before family behavior."""
        value = cast("Any", record)
        if str(value.world_id) != actual_world_id:
            raise ValueError("leased command record targets a different world")
        name = _record_name(record)
        spec = self._registry.resolve_name(name)
        durable = getattr(spec, "durable", None)
        if durable is None:
            raise ValueError(f"stored operation {name!r} is direct-only")

        payload_json = str(value.payload_json)
        stored_digest = str(value.payload_digest)
        valid_digests = {_record_identity_digest(record, operation_name=name)}
        # A narrow logical-record adapter used before the physical substrate
        # adopted the full immutable digest exposes both operation_name and
        # command_type. Physical CommandRecord rows expose only command_type
        # and therefore always require the v2 all-field digest.
        if hasattr(record, "operation_name"):
            valid_digests.add(_payload_digest(payload_json))
        if stored_digest not in valid_digests:
            raise ValueError("durable command payload digest does not match its record")

        operation = decode_canonical_operation(spec, payload_json)
        if self._registry.resolve(operation) is not spec:
            raise TypeError("decoded operation does not resolve to its recorded registration")
        if _world_key(spec, operation) != actual_world_id:
            raise ValueError("decoded operation world differs from the actual locked world")

        reserved_entity_id = getattr(record, "reserved_entity_id", None)
        if reserved_entity_id is not None:
            operation_entity_id = getattr(operation, "entity_id", None)
            if type(operation_entity_id) is not int or operation_entity_id != reserved_entity_id:
                raise ValueError("reserved entity identity differs from the durable payload")
        return spec, operation

    @staticmethod
    def _failure_status(record: object, error: Exception) -> str:
        if isinstance(error, (LookupError, TypeError, ValueError)):
            return "REJECTED"
        value = cast("Any", record)
        if int(value.attempts) >= int(value.max_attempts):
            return "DEAD_LETTER"
        return "RETRYABLE"

    async def materialize(self, world: AsyncWorld, target_tick: int) -> int:
        """Materialize due work on the supplied already-locked world."""
        coordinator = getattr(world, "commit_coordinator", None)
        if (
            coordinator is None
            or not callable(getattr(coordinator, "stage_command", None))
            or not callable(getattr(coordinator, "is_command_staged", None))
        ):
            raise RuntimeError(
                "deferred commands require a coordinated world with atomic manifest settlement"
            )

        world_id = str(world.world_id)
        catalog = await self._catalog(world_id)
        records = await catalog.lease_commands(
            world_id,
            target_tick,
            self._owner,
            lease_seconds=self._lease_seconds,
            limit=self._max_dequeue,
        )
        records.sort(
            key=lambda record: (
                record.scheduled_tick,
                record.priority,
                record.sequence,
            )
        )

        settlement = cast("Any", coordinator)
        applied = 0
        for index, record in enumerate(records):
            try:
                spec, operation = self._decode_record(
                    record,
                    actual_world_id=world_id,
                )
                if settlement.is_command_staged(target_tick, record.command_id):
                    applied += 1
                    continue
                durable = spec.durable
                assert durable is not None
                await durable.materialize(world, operation)
            except BaseException as error:
                if not isinstance(error, Exception):
                    raise
                status = self._failure_status(record, error)
                await catalog.fail_command(
                    world_id,
                    record.command_id,
                    self._owner,
                    status=status,
                    error_code=type(error).__name__,
                    error_detail=str(error)[:2000],
                )
                if status == "RETRYABLE":
                    tail = [item.command_id for item in records[index + 1 :]]
                    if tail:
                        await catalog.release_commands(world_id, tail, self._owner)
                    break
                continue

            settlement.stage_command(target_tick, self._owner, record.command_id)
            applied += 1
        return applied

    async def pending_count(self, world_id: object) -> int:
        catalog = await self._catalog(world_id)
        return await catalog.pending_command_count(str(world_id))

    async def records(
        self,
        world_id: object,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[CommandRecord]:
        catalog = await self._catalog(world_id)
        return await catalog.list_commands(str(world_id), status=status, limit=limit)

    async def history(
        self,
        world_id: object,
        *,
        limit: int = 100,
    ) -> list[BaseModel]:
        """Decode durable history through exact registrations, never an enum."""
        records = await self.records(world_id, limit=limit)
        return [self._decode_record(record, actual_world_id=str(world_id))[1] for record in records]

    async def cancel_world(
        self,
        world_id: object,
        *,
        reason: str = "world destroyed",
    ) -> int:
        catalog = await self._catalog(world_id)
        return await catalog.cancel_commands(str(world_id), reason=reason)

    async def read_outbox(
        self,
        *,
        world_id: object | None = None,
        limit: int = 1000,
    ) -> list[OutboxRecord]:
        if limit < 1:
            raise ValueError("limit must be positive")
        if world_id is not None:
            key = str(world_id)
            catalog = await self._catalog(key)
            return await catalog.read_outbox(key, limit=limit)

        events: list[OutboxRecord] = []
        for key, catalog in sorted(self._catalogs.items()):
            if len(events) >= limit:
                break
            events.extend(
                await catalog.read_outbox(
                    key,
                    limit=max(1, limit - len(events)),
                )
            )
        return events

    async def acknowledge_outbox(self, events: list[OutboxRecord]) -> None:
        by_world: dict[str, list[str]] = {}
        for event in events:
            by_world.setdefault(str(event.world_id), []).append(str(event.event_id))
        for world_id, event_ids in by_world.items():
            catalog = await self._catalog(world_id)
            await catalog.mark_outbox_projected(world_id, event_ids)

    async def mark_outbox_projected(self, events: list[OutboxRecord]) -> None:
        """Compatibility spelling for the constructor-injected acknowledgement."""
        await self.acknowledge_outbox(events)

    async def outbox_progress(
        self,
        world_id: object | None = None,
    ) -> dict[str, tuple[int, int]]:
        if world_id is not None:
            key = str(world_id)
            catalog = await self._catalog(key)
            return {key: await catalog.outbox_progress(key)}
        return {
            key: await catalog.outbox_progress(key)
            for key, catalog in sorted(self._catalogs.items())
        }


__all__ = ["CommandScheduler"]
