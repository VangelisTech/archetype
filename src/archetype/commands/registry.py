# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact operation registration for governed and durable command entry."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel

if TYPE_CHECKING:
    from archetype.core.aio.async_world import AsyncWorld


@dataclass(frozen=True, slots=True)
class DurableOperation:
    """Portable decoder and actual-world materializer for one operation."""

    decode: Callable[[str], BaseModel]
    materialize: Callable[[AsyncWorld, BaseModel], Awaitable[None]]


@dataclass(frozen=True, slots=True, kw_only=True)
class OperationSpec:
    """The complete commands-owned registration for one exact model type."""

    name: str
    model: type[BaseModel]
    handler: Callable[[BaseModel], Awaitable[Any]]
    permission: str
    summarize: Callable[[BaseModel], Mapping[str, Any]]
    quota_scope: Literal["application", "live_world", "durable_world"]
    world_key: Callable[[BaseModel], object] | None
    durable: DurableOperation | None = None
    trusted: bool = True
    untrusted: bool = True
    token_cost: int | Callable[[BaseModel], int] = 0


class OperationRegistry:
    """Insertion-ordered exact-name and exact-model operation registry."""

    def __init__(self) -> None:
        self._by_name: dict[str, OperationSpec] = {}
        self._by_model: dict[type[BaseModel], OperationSpec] = {}

    @property
    def specs(self) -> tuple[OperationSpec, ...]:
        """Return an immutable deterministic registration snapshot."""
        return tuple(self._by_name.values())

    def register(self, spec: OperationSpec) -> None:
        """Register one operation without MRO-based model fallback."""
        if spec.name in self._by_name:
            raise ValueError(f"operation name {spec.name!r} already registered")
        if spec.model in self._by_model:
            raise ValueError(f"operation model {spec.model.__name__} already registered")
        self._by_name[spec.name] = spec
        self._by_model[spec.model] = spec

    def resolve(self, operation: BaseModel) -> OperationSpec:
        """Resolve only the operation's exact concrete model type."""
        try:
            return self._by_model[type(operation)]
        except KeyError:
            raise KeyError(f"{type(operation).__name__} is not registered") from None

    def resolve_name(self, name: str) -> OperationSpec:
        """Resolve one exact registered discriminator."""
        try:
            return self._by_name[name]
        except KeyError:
            raise KeyError(f"operation name {name!r} is not registered") from None


__all__ = ["DurableOperation", "OperationRegistry", "OperationSpec"]
