# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Governed trusted, actor-aware, and durable command dispatch."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel
from uuid_utils import UUID

from archetype.commands.models import (
    AccessSummary,
    ActorCtx,
    DeferredItem,
    DurableOptions,
    PolicyRequest,
)
from archetype.errors import WorldNotFoundError
from archetype.world.errors import WorldClosingError
from archetype.world.models import Spawn

if TYPE_CHECKING:
    from archetype.commands.policy import Policy
    from archetype.commands.registry import OperationRegistry, OperationSpec

type Decision = Literal["allowed", "denied"]
type Outcome = Literal["succeeded", "failed", "denied", "rejected", "queued"]

_SAFE_METADATA_KEYS = frozenset(
    {
        "classification",
        "count",
        "entity_id",
        "label",
        "limit",
        "name",
        "operation",
        "priority",
        "safe_count",
        "source_world_id",
        "status",
        "world_id",
    }
)
_MAX_EVIDENCE_BYTES = 4096
_MAX_SCALAR_STRING = 256


def _bounded_string(value: object) -> str:
    normalized = str(value)
    if len(normalized) <= _MAX_SCALAR_STRING:
        return normalized
    return normalized[:_MAX_SCALAR_STRING]


def _safe_scalar(value: object) -> str | int | float | bool | None:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return _safe_scalar(value.value)
    if isinstance(value, str):
        return _bounded_string(value)
    if isinstance(value, int) and value.bit_length() <= 128:
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise TypeError("access metadata values must be bounded scalars")


def _bounded_metadata(summary: Mapping[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for key, value in summary.items():
        if key not in _SAFE_METADATA_KEYS:
            continue
        try:
            metadata[key] = _safe_scalar(value)
        except (TypeError, ValueError):
            continue
    return metadata


def _token_cost(spec: Any, operation: BaseModel) -> int:
    configured = spec.token_cost
    value = configured(operation) if callable(configured) else configured
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("operation token cost must be a non-negative integer")
    return value


def _world_key(spec: Any, operation: BaseModel) -> object:
    resolver = spec.world_key
    if resolver is None:
        raise RuntimeError(f"world-scoped operation {spec.name!r} has no world-key resolver")
    return resolver(operation)


def _target_tick(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("target_tick must be a non-negative integer")
    return value


class CommandDispatcher:
    """Coordinate governed entry without owning family behavior or world locks."""

    def __init__(
        self,
        *,
        registry: OperationRegistry,
        policy: Policy,
        scheduler: Any,
        record_access: Callable[[AccessSummary], Awaitable[None]],
        target_tick_for_world: Callable[[object], int],
    ) -> None:
        self._registry = registry
        self._policy = policy
        self._scheduler = scheduler
        self._record_access = record_access
        self._target_tick_for_world = target_tick_for_world
        self._admission_lock = asyncio.Lock()
        self._drained = asyncio.Event()
        self._drained.set()
        self._active_operations = 0
        self._admitted_task_depths: dict[asyncio.Task[Any], int] = {}
        self._accepting = True
        self._stop_requested = False

    async def apply(self, operation: BaseModel) -> Any:
        """Run one trusted exact operation without fabricating actor evidence."""
        async with self._admitted():
            spec = self._registry.resolve(operation)
            self._require_available(spec, actor_aware=False)
            return await spec.handler(operation)

    async def apply_as(self, actor: ActorCtx, operation: BaseModel) -> Any:
        """Authorize and run one exact operation with bounded access evidence."""
        async with self._admitted():
            spec = self._registry.resolve(operation)
            self._policy.preauthorize(actor, permission=spec.permission)
            if not spec.untrusted:
                await self._record_rejection(actor, spec)
                raise PermissionError(
                    f"operation {spec.name!r} is not available to untrusted callers"
                )

            world_id, target_tick, token_cost = self._direct_coordinates(
                spec,
                operation,
            )
            try:
                if spec.quota_scope == "application":
                    self._policy.authorize_application(
                        actor,
                        permission=spec.permission,
                        token_cost=token_cost,
                    )
                else:
                    assert world_id is not None
                    self._policy.authorize(
                        actor,
                        permission=spec.permission,
                        world_id=world_id,
                        target_tick=target_tick,
                        token_cost=token_cost,
                    )
            except Exception:
                await self._record_evidence(
                    actor,
                    spec,
                    world_id=world_id,
                    decision="denied",
                    outcome="denied",
                )
                raise

            try:
                result = await spec.handler(operation)
            except Exception:
                await self._record_evidence(
                    actor,
                    spec,
                    world_id=world_id,
                    decision="allowed",
                    outcome="failed",
                )
                raise

            await self._record_evidence(
                actor,
                spec,
                operation=operation,
                world_id=world_id,
                decision="allowed",
                outcome="succeeded",
            )
            return result

    async def defer(
        self,
        operation: BaseModel,
        options: DurableOptions,
        *,
        command_id: UUID | None = None,
        version: int = 1,
    ) -> Any:
        """Admit one trusted portable operation."""
        async with self._admitted():
            spec = self._registry.resolve(operation)
            self._require_deferred(spec, actor_aware=False)
            return await self._scheduler.admit(
                operation,
                options,
                command_id=command_id,
                principal_id=None,
                origin="local",
                version=version,
            )

    async def defer_as(
        self,
        actor: ActorCtx,
        operation: BaseModel,
        options: DurableOptions,
        *,
        command_id: UUID | None = None,
        version: int = 1,
    ) -> Any:
        """Authorize and admit one actor-aware portable operation."""
        async with self._admitted():
            spec = self._registry.resolve(operation)
            self._policy.preauthorize(actor, permission=spec.permission)
            if not spec.untrusted:
                await self._record_rejection(actor, spec)
                raise PermissionError(
                    f"operation {spec.name!r} is not available to untrusted callers"
                )
            if spec.durable is None:
                await self._record_rejection(actor, spec)
                raise ValueError(f"operation {spec.name!r} is direct-only")

            world_id, token_cost = self._deferred_coordinates(
                spec,
                operation,
                options,
            )
            try:
                self._policy.authorize(
                    actor,
                    permission=spec.permission,
                    world_id=world_id,
                    target_tick=_target_tick(options.target_tick),
                    token_cost=token_cost,
                )
            except Exception:
                await self._record_evidence(
                    actor,
                    spec,
                    world_id=world_id,
                    decision="denied",
                    outcome="denied",
                )
                raise

            try:
                result = await self._scheduler.admit(
                    operation,
                    options,
                    command_id=command_id,
                    principal_id=actor.id,
                    origin="gateway",
                    version=version,
                )
            except Exception:
                await self._record_evidence(
                    actor,
                    spec,
                    world_id=world_id,
                    decision="allowed",
                    outcome="failed",
                )
                raise

            await self._record_evidence(
                actor,
                spec,
                operation=operation,
                world_id=world_id,
                decision="allowed",
                outcome="queued",
            )
            return result

    async def defer_batch(self, items: tuple[DeferredItem, ...]) -> list[Any]:
        """Admit one non-empty trusted batch in one scheduler call."""
        async with self._admitted():
            self._require_nonempty_batch(items)
            specs = tuple(self._registry.resolve(item.operation) for item in items)
            for spec in specs:
                self._require_deferred(spec, actor_aware=False)
            return await self._scheduler.admit_batch(
                items,
                principal_id=None,
                origin="local",
            )

    async def defer_batch_as(
        self,
        actor: ActorCtx,
        items: tuple[DeferredItem, ...],
    ) -> list[Any]:
        """Authorize and admit one batch through ordered atomic phases."""
        async with self._admitted():
            self._require_nonempty_batch(items)
            specs = tuple(self._registry.resolve(item.operation) for item in items)

            for spec in specs:
                self._policy.preauthorize(actor, permission=spec.permission)

            for spec in specs:
                if not spec.untrusted:
                    await self._record_rejection(actor, spec)
                    raise PermissionError(
                        f"operation {spec.name!r} is not available to untrusted callers"
                    )
                if spec.durable is None:
                    await self._record_rejection(actor, spec)
                    raise ValueError(f"operation {spec.name!r} is direct-only")

            coordinates: list[tuple[object, int]] = []
            requests: list[PolicyRequest] = []
            for item, spec in zip(items, specs, strict=True):
                world_id, token_cost = self._deferred_coordinates(
                    spec,
                    item.operation,
                    item.options,
                )
                coordinates.append((world_id, token_cost))
                requests.append(
                    PolicyRequest(
                        permission=spec.permission,
                        world_id=world_id,
                        target_tick=_target_tick(item.options.target_tick),
                        token_cost=token_cost,
                    )
                )

            try:
                self._policy.authorize_batch(actor, requests=tuple(requests))
            except Exception:
                for spec, (world_id, _cost) in zip(specs, coordinates, strict=True):
                    await self._record_evidence(
                        actor,
                        spec,
                        world_id=world_id,
                        decision="denied",
                        outcome="denied",
                    )
                raise

            try:
                result = await self._scheduler.admit_batch(
                    items,
                    principal_id=actor.id,
                    origin="gateway",
                )
            except Exception:
                for spec, (world_id, _cost) in zip(specs, coordinates, strict=True):
                    await self._record_evidence(
                        actor,
                        spec,
                        world_id=world_id,
                        decision="allowed",
                        outcome="failed",
                    )
                raise

            for item, spec, (world_id, _cost) in zip(
                items,
                specs,
                coordinates,
                strict=True,
            ):
                await self._record_evidence(
                    actor,
                    spec,
                    operation=item.operation,
                    world_id=world_id,
                    decision="allowed",
                    outcome="queued",
                )
            return result

    async def defer_spawn(
        self,
        operation: Spawn,
        options: DurableOptions,
        *,
        command_id: UUID | None = None,
        version: int = 1,
    ) -> tuple[int, Any]:
        """Admit a trusted spawn whose reservation remains scheduler-owned."""
        async with self._admitted():
            spec = self._registry.resolve(operation)
            self._require_deferred(spec, actor_aware=False)
            return await self._scheduler.admit_spawn(
                operation,
                options,
                command_id=command_id,
                principal_id=None,
                origin="local",
                version=version,
            )

    async def defer_spawn_as(
        self,
        actor: ActorCtx,
        operation: Spawn,
        options: DurableOptions,
        *,
        command_id: UUID | None = None,
        version: int = 1,
    ) -> tuple[int, Any]:
        """Authorize before asking the scheduler to reserve and admit a spawn."""
        async with self._admitted():
            spec = self._registry.resolve(operation)
            self._policy.preauthorize(actor, permission=spec.permission)
            if not spec.untrusted:
                await self._record_rejection(actor, spec)
                raise PermissionError(
                    f"operation {spec.name!r} is not available to untrusted callers"
                )
            if spec.durable is None:
                await self._record_rejection(actor, spec)
                raise ValueError(f"operation {spec.name!r} is direct-only")

            world_id, token_cost = self._deferred_coordinates(
                spec,
                operation,
                options,
            )
            try:
                self._policy.authorize(
                    actor,
                    permission=spec.permission,
                    world_id=world_id,
                    target_tick=_target_tick(options.target_tick),
                    token_cost=token_cost,
                )
            except Exception:
                await self._record_evidence(
                    actor,
                    spec,
                    world_id=world_id,
                    decision="denied",
                    outcome="denied",
                )
                raise

            try:
                result = await self._scheduler.admit_spawn(
                    operation,
                    options,
                    command_id=command_id,
                    principal_id=actor.id,
                    origin="gateway",
                    version=version,
                )
            except Exception:
                await self._record_evidence(
                    actor,
                    spec,
                    world_id=world_id,
                    decision="allowed",
                    outcome="failed",
                )
                raise

            await self._record_evidence(
                actor,
                spec,
                operation=operation,
                world_id=world_id,
                decision="allowed",
                outcome="queued",
            )
            return result

    def request_stop(self) -> None:
        """Synchronously publish sticky stop intent before lock acquisition."""

        self._stop_requested = True

    async def stop_admission(self) -> None:
        """Atomically reject future top-level work."""
        self.request_stop()
        async with self._admission_lock:
            self._accepting = False

    async def wait_drained(self) -> None:
        """Wait until every operation admitted before shutdown has exited."""
        if self.operation_admitted():
            raise RuntimeError("command admission cannot drain its current task")
        await self._drained.wait()

    def _detach_closed_graph(self) -> None:
        """Release handler dependencies after terminal admission drain."""

        if self._accepting or self._active_operations or self._admitted_task_depths:
            raise RuntimeError("command dependencies require stopped, drained admission")
        for attribute in (
            "_registry",
            "_policy",
            "_scheduler",
            "_record_access",
            "_target_tick_for_world",
        ):
            self.__dict__.pop(attribute, None)

    def operation_admitted(self) -> bool:
        """Whether the current task may finish nested exact-operation work."""

        try:
            task = asyncio.current_task()
        except RuntimeError:
            return False
        return task is not None and task in self._admitted_task_depths

    @asynccontextmanager
    async def _admit_runtime_operation(
        self,
        continuation: Callable[[], bool],
    ) -> AsyncIterator[None]:
        """Count one trusted complete call in command admission."""

        async with self._admitted(continuation=continuation()):
            yield

    @asynccontextmanager
    async def _admitted(self, *, continuation: bool = False) -> AsyncIterator[None]:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("command admission requires an asyncio task")
        observed_depth = self._admitted_task_depths.get(task, 0)
        if observed_depth == 0 and self._stop_requested and not continuation:
            raise RuntimeError("command admission is not accepting work")
        async with self._admission_lock:
            depth = self._admitted_task_depths.get(task, 0)
            if depth == 0:
                if (self._stop_requested or not self._accepting) and not continuation:
                    raise RuntimeError("command admission is not accepting work")
                self._active_operations += 1
                self._drained.clear()
            self._admitted_task_depths[task] = depth + 1
        try:
            yield
        finally:
            release = asyncio.create_task(
                self._release_admission(task),
                name="archetype-command-admission-release",
            )
            interrupted = False
            while True:
                try:
                    await asyncio.shield(release)
                    break
                except asyncio.CancelledError:
                    interrupted = True
                    if release.done():
                        break
            release.result()
            if interrupted:
                raise asyncio.CancelledError

    async def _release_admission(self, task: asyncio.Task[Any]) -> None:
        """Finish exact-task deregistration despite repeated caller cancellation."""

        async with self._admission_lock:
            remaining = self._admitted_task_depths[task] - 1
            if remaining:
                self._admitted_task_depths[task] = remaining
            else:
                del self._admitted_task_depths[task]
                self._active_operations -= 1
                if self._active_operations == 0:
                    self._drained.set()

    @staticmethod
    def _require_nonempty_batch(items: tuple[DeferredItem, ...]) -> None:
        if not items:
            raise ValueError("deferred batch must contain at least one item")

    @staticmethod
    def _require_available(spec: OperationSpec, *, actor_aware: bool) -> None:
        attribute = "untrusted" if actor_aware else "trusted"
        if not getattr(spec, attribute):
            raise PermissionError(
                f"operation {spec.name!r} is not available to {attribute} callers"
            )

    @classmethod
    def _require_deferred(cls, spec: OperationSpec, *, actor_aware: bool) -> None:
        cls._require_available(spec, actor_aware=actor_aware)
        if spec.durable is None:
            raise ValueError(f"operation {spec.name!r} is direct-only")

    def _direct_coordinates(
        self,
        spec: OperationSpec,
        operation: BaseModel,
    ) -> tuple[object | None, int, int]:
        token_cost: int
        if spec.quota_scope == "application":
            token_cost = _token_cost(spec, operation)
            return None, 0, token_cost

        world_id = _world_key(spec, operation)
        if spec.quota_scope == "live_world":
            target_tick = _target_tick(self._target_tick_for_world(world_id))
        elif spec.quota_scope == "durable_world":
            try:
                target_tick = _target_tick(self._target_tick_for_world(world_id))
            except (KeyError, WorldNotFoundError, WorldClosingError):
                target_tick = 0
        else:
            raise RuntimeError(f"unsupported quota scope {spec.quota_scope!r}")
        token_cost = _token_cost(spec, operation)
        return world_id, target_tick, token_cost

    @staticmethod
    def _deferred_coordinates(
        spec: OperationSpec,
        operation: BaseModel,
        options: DurableOptions,
    ) -> tuple[object, int]:
        if spec.quota_scope == "application":
            raise ValueError("application-scoped operations cannot be deferred")
        _target_tick(options.target_tick)
        return _world_key(spec, operation), _token_cost(spec, operation)

    async def _record_rejection(self, actor: ActorCtx, spec: OperationSpec) -> None:
        await self._record_evidence(
            actor,
            spec,
            decision="denied",
            outcome="rejected",
        )

    async def _record_evidence(
        self,
        actor: ActorCtx,
        spec: OperationSpec,
        *,
        operation: BaseModel | None = None,
        world_id: object | None = None,
        decision: Decision,
        outcome: Outcome,
    ) -> None:
        metadata: dict[str, Any] = {}
        if operation is not None:
            try:
                metadata = _bounded_metadata(spec.summarize(operation))
            except Exception:
                metadata = {}

        try:
            evidence = AccessSummary(
                operation=_bounded_string(spec.name),
                # ``uuid_utils.UUID`` is accepted by the boundary model but is
                # not JSON-serializable through Pydantic's arbitrary-type
                # fallback. Access evidence is a wire projection, so normalize
                # it here.
                actor_id=str(actor.id),
                world_id=(None if world_id is None else _bounded_string(world_id)),
                decision=decision,
                outcome=outcome,
                metadata=metadata,
            )
            encoded = json.dumps(
                evidence.model_dump(mode="json"),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
            if len(encoded) > _MAX_EVIDENCE_BYTES:
                evidence = evidence.model_copy(update={"metadata": {}})
                encoded = json.dumps(
                    evidence.model_dump(mode="json"),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                ).encode("utf-8")
            if len(encoded) > _MAX_EVIDENCE_BYTES:
                return
        except Exception:
            # Evidence construction is part of the advisory projection and
            # cannot replace the primary result or failure.
            return

        try:
            await self._record_access(evidence)
        except Exception:
            return


__all__ = ["CommandDispatcher"]
