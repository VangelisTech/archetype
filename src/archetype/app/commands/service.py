# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable command admission, scheduling, dispatch, and settlement."""

from __future__ import annotations

import hashlib
import json
import os
import socket
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from pydantic_core import to_jsonable_python
from uuid_utils import UUID, uuid7

from archetype.app.errors import WorldNotFoundError
from archetype.app.models import Command, CommandType
from archetype.app.storage.catalog import (
    CommandAdmission,
    CommandRecord,
    OutboxRecord,
    schema_fingerprint,
)
from archetype.core.component import Component

if TYPE_CHECKING:
    from archetype.app.world.interfaces import iMutationService, iWorldService

_COMMAND_DIGEST_DOMAIN = "archetype.command.v1"
_COMPONENT_WIRE_MARKER = "__archetype_component__"
_DEFERRED_COMMAND_TYPES = frozenset(
    {
        CommandType.SPAWN,
        CommandType.UPDATE,
        CommandType.DESPAWN,
        CommandType.ADD_COMPONENT,
        CommandType.REMOVE_COMPONENT,
        CommandType.MESSAGE,
        CommandType.CUSTOM,
        CommandType.QUERY_WORLD,
    }
)


def _parse_entity_id(value: object) -> int:
    """Decode an entity ID without lossy numeric coercion."""
    if type(value) is int:
        return value
    if isinstance(value, str):
        digits = value[1:] if value[:1] in {"+", "-"} else value
        if digits and digits.isascii() and digits.isdecimal():
            return int(value)
    raise TypeError("entity_id must be an integer or decimal-integer string")


def _component_type_payload(component_type: type[Component], *, fields: Any = None) -> dict:
    """Encode component identity by name and schema, never class-name alone.

    Application processes commonly load same-named component classes from
    tests, plugins, or versioned modules. Their prefixed schemas are the
    durable identity boundary used by the storage catalog, so command payloads
    use the same rule. Module paths are intentionally excluded: schema-
    identical definitions remain interchangeable across code moves.
    """
    payload = {
        _COMPONENT_WIRE_MARKER: 1,
        "type": component_type.__name__,
        "schema_fingerprint": schema_fingerprint(component_type.get_prefixed_schema()),
    }
    if fields is not None:
        payload["fields"] = _portable(fields)
    return payload


def _portable(value: Any) -> Any:
    """Convert a deferred payload to stable JSON or reject live capabilities."""
    if isinstance(value, Component):
        return _component_type_payload(type(value), fields=value.model_dump(mode="python"))
    if isinstance(value, type) and issubclass(value, Component):
        return _component_type_payload(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return _portable(value.value)
    if isinstance(value, BaseModel):
        return _portable(value.model_dump(mode="json"))
    if is_dataclass(value) and not isinstance(value, type):
        return _portable(asdict(value))
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("deferred command payload keys must be strings")
        return {key: _portable(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_portable(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        # Pydantic's converter handles datetime/decimal-like scalar values,
        # while allow_nan below still rejects non-portable floats.
        return to_jsonable_python(value)
    raise TypeError(
        "deferred command payloads must be portable JSON; "
        f"{type(value).__module__}.{type(value).__qualname__} is a live capability"
    )


def _canonical_admission(
    world_id: str,
    command: Command,
    *,
    principal_id: str | None,
    origin: str,
    max_attempts: int,
) -> tuple[CommandAdmission, Command]:
    payload = _portable(command.payload)
    assert isinstance(payload, dict)
    payload_json = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    immutable = json.dumps(
        {
            "domain": _COMMAND_DIGEST_DOMAIN,
            "world_id": world_id,
            "scheduled_tick": command.tick,
            "priority": command.priority,
            "command_type": command.type.value,
            "payload": payload,
            "version": command.version,
            "principal_id": principal_id,
            "origin": origin,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    digest = hashlib.sha256(immutable.encode("utf-8")).hexdigest()
    portable_command = command.model_copy(
        update={
            "actor_id": UUID(principal_id) if principal_id is not None else None,
            "payload": payload,
        }
    )
    reserved_entity_id = None
    if command.type == CommandType.SPAWN and "entity_id" in payload:
        reserved_entity_id = _parse_entity_id(payload["entity_id"])
    return (
        CommandAdmission(
            command_id=str(command.id),
            scheduled_tick=command.tick,
            priority=command.priority,
            command_type=command.type.value,
            payload_json=payload_json,
            payload_digest=digest,
            version=command.version,
            principal_id=principal_id,
            origin=origin,
            reserved_entity_id=reserved_entity_id,
            max_attempts=max_attempts,
        ),
        portable_command,
    )


def _record_to_command(record: CommandRecord) -> Command:
    return Command(
        id=UUID(record.command_id),
        tick=record.scheduled_tick,
        actor_id=UUID(record.principal_id) if record.principal_id else None,
        type=CommandType(record.command_type),
        payload=json.loads(record.payload_json),
        priority=record.priority,
        version=record.version,
        seq=record.sequence,
    )


class CommandScheduler:
    """Coordinate the durable per-world command state machine.

    The scheduler has no authorization dependency. The untrusted gateway
    supplies a principal snapshot after RBAC; trusted callers use origin
    ``local`` with no fabricated actor.
    """

    def __init__(
        self,
        worlds: iWorldService,
        mutations: iMutationService,
        *,
        lease_seconds: float = 30.0,
        max_attempts: int = 3,
        max_dequeue: int = 50_000,
        owner: str | None = None,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._worlds = worlds
        self._mutations = mutations
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts
        self._max_dequeue = max_dequeue
        self._owner = owner or f"{socket.gethostname()}:{os.getpid()}:{uuid7().hex[:12]}"
        self._catalogs: dict[str, Any] = {}

    @property
    def owner(self) -> str:
        return self._owner

    @staticmethod
    def validate_deferred(command: Command) -> None:
        if command.type not in _DEFERRED_COMMAND_TYPES:
            raise ValueError(
                f"{command.type.value} is a direct gated operation with no "
                "tick-deferred dispatcher; it cannot enter the deferred scheduler"
            )

    def require_world(self, world_id) -> None:
        if not self._worlds.has_world(world_id):
            raise WorldNotFoundError(world_id)

    def _catalog(self, world_id):
        key = str(world_id)
        catalog = self._worlds.control_catalog(key)
        self._catalogs[key] = catalog
        return catalog

    async def admit(
        self,
        world_id,
        command: Command,
        *,
        principal_id: str | UUID | None = None,
        origin: str = "local",
    ) -> UUID:
        records = await self.admit_batch(
            world_id,
            [command],
            principal_id=principal_id,
            origin=origin,
        )
        return records[0]

    async def admit_batch(
        self,
        world_id,
        commands: list[Command],
        *,
        principal_id: str | UUID | None = None,
        origin: str = "local",
    ) -> list[UUID]:
        self.require_world(world_id)
        principal = str(principal_id) if principal_id is not None else None
        admissions: list[CommandAdmission] = []
        for command in commands:
            self.validate_deferred(command)
            admission, _portable_command = _canonical_admission(
                str(world_id),
                command,
                principal_id=principal,
                origin=origin,
                max_attempts=self._max_attempts,
            )
            admissions.append(admission)
        records = await self._catalog(world_id).admit_commands(str(world_id), admissions)
        return [UUID(record.command_id) for record in records]

    async def admit_spawn(
        self,
        world_id,
        components,
        *,
        tick: int = 0,
        priority: int = 0,
        principal_id: str | UUID | None = None,
        origin: str = "local",
    ) -> tuple[int, Command]:
        self.require_world(world_id)
        entity_id = self._mutations.reserve_entity_ids(world_id, 1)[0]
        command = Command(
            type=CommandType.SPAWN,
            tick=tick,
            priority=priority,
            payload={"entity_id": entity_id, "components": list(components)},
        )
        try:
            await self.admit(
                world_id,
                command,
                principal_id=principal_id,
                origin=origin,
            )
        except BaseException:
            world = self._worlds.get_world(UUID(str(world_id)))
            if world.next_entity_id == entity_id + 1:
                world.next_entity_id = entity_id
            raise
        return entity_id, command

    async def drain_and_apply(self, world_id, tick: int) -> list[Command]:
        """Lease and stage due commands; the manifest transaction settles them."""
        world = self._worlds.get_world(UUID(str(world_id)))
        coordinator = getattr(world, "commit_coordinator", None)
        if coordinator is None or not hasattr(coordinator, "stage_command"):
            raise RuntimeError(
                "deferred commands require a coordinated world with atomic manifest settlement"
            )
        catalog = self._catalog(world_id)
        records = await catalog.lease_commands(
            str(world_id),
            tick,
            self._owner,
            lease_seconds=self._lease_seconds,
            limit=self._max_dequeue,
        )
        staged: list[Command] = []
        for index, record in enumerate(records):
            command = _record_to_command(record)
            if coordinator.is_command_staged(tick, record.command_id):
                staged.append(command)
                continue
            try:
                await self._apply(world_id, command)
            except BaseException as exc:
                if not isinstance(exc, Exception):
                    raise
                permanent = isinstance(exc, (KeyError, LookupError, TypeError, ValueError))
                if permanent:
                    status = "REJECTED"
                elif record.attempts >= record.max_attempts:
                    status = "DEAD_LETTER"
                else:
                    status = "RETRYABLE"
                await catalog.fail_command(
                    str(world_id),
                    record.command_id,
                    self._owner,
                    status=status,
                    error_code=type(exc).__name__,
                    error_detail=str(exc),
                )
                # A transient error preserves the unprocessed tail. Permanent
                # rejection and exhausted poison commands do not block it.
                if status == "RETRYABLE":
                    await catalog.release_commands(
                        str(world_id),
                        [tail.command_id for tail in records[index + 1 :]],
                        self._owner,
                    )
                    break
                continue
            coordinator.stage_command(tick, self._owner, record.command_id)
            staged.append(command)
        return staged

    async def _apply(self, world_id, command: Command) -> None:
        payload = command.payload
        match command.type:
            case CommandType.SPAWN:
                components = self._hydrate_components(payload.get("components", []))
                entity_id = payload.get("entity_id")
                if entity_id is None:
                    await self._mutations.create_entity(world_id, components)
                else:
                    await self._mutations.spawn_with_reserved_id(
                        world_id, _parse_entity_id(entity_id), components
                    )
            case CommandType.UPDATE:
                await self._mutations.update_entity(
                    world_id,
                    _parse_entity_id(payload["entity_id"]),
                    self._hydrate_components(payload.get("components", [])),
                )
            case CommandType.DESPAWN:
                await self._mutations.remove_entity(
                    world_id, _parse_entity_id(payload["entity_id"])
                )
            case CommandType.ADD_COMPONENT:
                await self._mutations.add_components(
                    world_id,
                    _parse_entity_id(payload["entity_id"]),
                    self._hydrate_components(payload.get("components", [])),
                )
            case CommandType.REMOVE_COMPONENT:
                await self._mutations.remove_components(
                    world_id,
                    _parse_entity_id(payload["entity_id"]),
                    self._hydrate_component_types(payload.get("component_types", [])),
                )
            case CommandType.MESSAGE | CommandType.CUSTOM | CommandType.QUERY_WORLD:
                return
            case _:
                raise ValueError(f"no deferred dispatcher for {command.type.value}")

    @staticmethod
    def _component_candidates(name: str) -> list[type[Component]]:
        candidates: list[type[Component]] = []
        stack: list[type[Component]] = list(Component.__subclasses__())
        seen: set[type[Component]] = set()
        while stack:
            component_type = stack.pop()
            if component_type in seen:
                continue
            seen.add(component_type)
            stack.extend(component_type.__subclasses__())
            if component_type.__name__ == name:
                candidates.append(component_type)
        return sorted(candidates, key=lambda cls: (cls.__module__, cls.__qualname__))

    @classmethod
    def _hydrate_component_type(cls, payload) -> type[Component]:
        if not isinstance(payload, dict) or payload.get(_COMPONENT_WIRE_MARKER) != 1:
            return (
                payload
                if isinstance(payload, type) and issubclass(payload, Component)
                else Component.get_type_by_name(str(payload))
            )

        name = payload.get("type")
        fingerprint = payload.get("schema_fingerprint")
        if not isinstance(name, str) or not isinstance(fingerprint, str):
            raise ValueError("component wire identity requires type and schema_fingerprint")
        matches = [
            component_type
            for component_type in cls._component_candidates(name)
            if schema_fingerprint(component_type.get_prefixed_schema()) == fingerprint
        ]
        if not matches:
            raise ValueError(
                f"Component type '{name}' with schema fingerprint {fingerprint} is not imported."
            )
        # Complete schema identity makes duplicate imported definitions
        # interchangeable; deterministic ordering chooses one representative.
        return matches[0]

    @classmethod
    def _hydrate_components(cls, payload_components):
        hydrated: list[Component] = []
        for payload in payload_components:
            if isinstance(payload, Component):
                hydrated.append(payload)
            elif isinstance(payload, dict) and payload.get(_COMPONENT_WIRE_MARKER) == 1:
                component_type = cls._hydrate_component_type(payload)
                fields = payload.get("fields")
                if not isinstance(fields, dict):
                    raise ValueError("component value wire payload requires object fields")
                hydrated.append(component_type(**fields))
            else:
                hydrated.append(Component.from_dict(payload))
        return hydrated

    @classmethod
    def _hydrate_component_types(cls, payload_types):
        return [cls._hydrate_component_type(component_type) for component_type in payload_types]

    async def pending_count(self, world_id) -> int:
        return await self._catalog(world_id).pending_command_count(str(world_id))

    async def records(
        self, world_id, *, status: str | None = None, limit: int = 100
    ) -> list[CommandRecord]:
        return await self._catalog(world_id).list_commands(
            str(world_id), status=status, limit=limit
        )

    async def history(self, world_id, *, limit: int = 100) -> list[Command]:
        return [_record_to_command(record) for record in await self.records(world_id, limit=limit)]

    async def cancel_world(self, world_id, *, reason: str = "world destroyed") -> int:
        return await self._catalog(world_id).cancel_commands(str(world_id), reason=reason)

    async def read_outbox(self, *, limit: int = 1000) -> list[OutboxRecord]:
        events: list[OutboxRecord] = []
        for world_id, catalog in sorted(self._catalogs.items()):
            if len(events) >= limit:
                break
            events.extend(await catalog.read_outbox(world_id, limit=max(1, limit - len(events))))
        return events

    async def mark_outbox_projected(self, events: list[OutboxRecord]) -> None:
        by_world: dict[str, list[str]] = {}
        for event in events:
            by_world.setdefault(event.world_id, []).append(event.event_id)
        for world_id, event_ids in by_world.items():
            catalog = self._catalogs.get(world_id)
            if catalog is not None:
                await catalog.mark_outbox_projected(world_id, event_ids)

    async def outbox_progress(self) -> dict[str, tuple[int, int]]:
        return {
            world_id: await catalog.outbox_progress(world_id)
            for world_id, catalog in sorted(self._catalogs.items())
        }
