# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic required-projector composition for managed worlds."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from archetype.core.interfaces import CommittedTickReceipt
from archetype.world.simulation import RequiredProjector


class RequiredProjectorFanout:
    """Retain one stable world projector and fan out to bound family consumers."""

    def __init__(
        self,
        *,
        fallback_unsettled: Callable[[str], Awaitable[bool]] | None = None,
        static_projector_factory: Callable[[str], RequiredProjector | None] | None = None,
    ) -> None:
        self._fallback_unsettled = fallback_unsettled
        self._static_projector_factory = static_projector_factory
        self._bindings: dict[str, dict[str, object]] = {}
        self._projectors: dict[str, RequiredProjector] = {}
        self._lock = asyncio.Lock()

    async def bind(self, world_id: str, binding: object) -> None:
        required = getattr(binding, "required_projector", None)
        consumer_name = getattr(required, "consumer_name", "")
        if not isinstance(consumer_name, str) or not consumer_name.strip():
            raise TypeError("Activity binding must expose a named required projector")
        key = str(world_id)
        async with self._lock:
            consumers = self._bindings.setdefault(key, {})
            existing = consumers.get(consumer_name)
            if existing is not None and existing is not binding:
                raise ValueError(f"world {key!r} already has Activity consumer {consumer_name!r}")
            consumers[consumer_name] = binding

    async def unbind(self, world_id: str, binding: object) -> None:
        required = getattr(binding, "required_projector", None)
        consumer_name = getattr(required, "consumer_name", "")
        key = str(world_id)
        async with self._lock:
            consumers = self._bindings.get(key)
            if consumers is None or consumers.get(consumer_name) is not binding:
                return
            consumers.pop(consumer_name)
            if not consumers:
                self._bindings.pop(key)

    async def has_unsettled(self, world_id: str) -> bool:
        key = str(world_id)
        fallback = self._fallback_unsettled
        if fallback is not None and await fallback(key):
            return True
        async with self._lock:
            bindings = tuple(self._bindings.get(key, {}).values())
        for binding in bindings:
            oracle = getattr(binding, "has_unsettled_work", None)
            if not callable(oracle):
                raise TypeError("Activity binding must expose has_unsettled_work")
            if await oracle(key):
                return True
        return False

    def required_projector_for(self, world_id: str) -> RequiredProjector:
        key = str(world_id)
        retained = self._projectors.get(key)
        if retained is not None:
            return retained

        static = (
            self._static_projector_factory(key)
            if self._static_projector_factory is not None
            else None
        )

        async def project(receipt: CommittedTickReceipt) -> None:
            async with self._lock:
                bindings = dict(self._bindings.get(key, {}))
            callbacks: dict[str, Callable[[CommittedTickReceipt], Awaitable[None]]] = {}
            if static is not None:
                callbacks[static.consumer_name] = static.project
            for name, binding in bindings.items():
                required = getattr(binding, "required_projector", None)
                callback = getattr(required, "project", None)
                if not callable(callback):
                    raise TypeError("Activity binding must expose a required projector")
                callbacks[name] = callback
            if not callbacks:
                fallback = self._fallback_unsettled
                if fallback is not None and await fallback(key):
                    raise RuntimeError(
                        f"world {key!r} has durable Activity work but no executable binding"
                    )
                return
            for name in sorted(callbacks):
                await callbacks[name](receipt)

        projector = RequiredProjector(
            consumer_name="world.required-projectors",
            project=project,
        )
        self._projectors[key] = projector
        return projector


__all__ = ["RequiredProjectorFanout"]
