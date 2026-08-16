# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact operation registration for governed and durable command entry."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, get_args, get_origin

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
        if not isinstance(spec, OperationSpec):
            raise TypeError("spec must be an OperationSpec")
        if spec.name in self._by_name:
            raise ValueError(f"operation name {spec.name!r} already registered")
        if spec.model in self._by_model:
            raise ValueError(f"operation model {spec.model.__name__} already registered")
        _validate_spec(spec)
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


def canonical_operation_json(operation: BaseModel) -> str:
    """Encode one operation as stable, strict canonical JSON."""
    return json.dumps(
        operation.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def decode_canonical_operation(spec: OperationSpec, payload_json: str) -> BaseModel:
    """Decode and prove one durable payload retains exact canonical identity."""
    if spec.durable is None:
        raise ValueError(f"operation {spec.name!r} is direct-only")
    if not isinstance(payload_json, str):
        raise TypeError("payload_json must be a string")
    try:
        decoded = spec.durable.decode(payload_json)
    except Exception as error:
        raise ValueError(f"operation {spec.name!r} payload could not be decoded") from error
    if type(decoded) is not spec.model:
        raise ValueError(
            f"durable decoder for {spec.name!r} returned "
            f"{type(decoded).__name__}, expected exact {spec.model.__name__}"
        )
    _validate_operation_discriminator(spec, decoded)
    try:
        reencoded = canonical_operation_json(decoded)
    except (TypeError, ValueError) as error:
        raise ValueError(f"operation {spec.name!r} did not encode as canonical JSON") from error
    if reencoded != payload_json:
        raise ValueError(f"durable decoder for {spec.name!r} did not round-trip canonical JSON")
    return decoded


def encode_canonical_operation(spec: OperationSpec, operation: BaseModel) -> str:
    """Encode and round-trip one exact durable operation before admission."""
    if type(operation) is not spec.model:
        raise TypeError(
            f"operation model {type(operation).__name__} does not match "
            f"registered exact model {spec.model.__name__}"
        )
    _validate_operation_discriminator(spec, operation)
    if spec.durable is None:
        raise ValueError(f"operation {spec.name!r} is direct-only")
    try:
        payload_json = canonical_operation_json(operation)
    except (TypeError, ValueError) as error:
        raise ValueError(f"operation {spec.name!r} is not portable canonical JSON") from error
    decode_canonical_operation(spec, payload_json)
    return payload_json


def operation_rejection_metadata(
    spec: OperationSpec,
    *,
    reason: str,
) -> dict[str, str]:
    """Describe a registration rejection without accepting or inspecting payload."""
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("rejection reason must be a non-empty string")
    return {
        "operation": spec.name,
        "model": f"{spec.model.__module__}.{spec.model.__qualname__}",
        "reason": reason,
    }


def _validate_spec(spec: OperationSpec) -> None:
    if not isinstance(spec, OperationSpec):
        raise TypeError("spec must be an OperationSpec")
    if not isinstance(spec.name, str) or not spec.name or spec.name.strip() != spec.name:
        raise ValueError("operation name must be a non-empty, unpadded string")
    if not isinstance(spec.model, type) or not issubclass(spec.model, BaseModel):
        raise TypeError("operation model must be a BaseModel subclass")
    operation_field = spec.model.model_fields.get("operation")
    if operation_field is None:
        raise ValueError(f"operation model {spec.model.__name__} has no discriminator")
    if operation_field.default != spec.name:
        raise ValueError(
            f"operation model {spec.model.__name__} discriminator "
            f"{operation_field.default!r} does not match registered name {spec.name!r}"
        )
    annotation = operation_field.annotation
    if get_origin(annotation) is not Literal or get_args(annotation) != (spec.name,):
        raise ValueError(
            f"operation model {spec.model.__name__} discriminator must be Literal[{spec.name!r}]"
        )
    if not callable(spec.handler):
        raise TypeError(f"operation {spec.name!r} handler must be callable")
    if not callable(spec.summarize):
        raise TypeError(f"operation {spec.name!r} summarizer must be callable")
    if not isinstance(spec.permission, str) or not spec.permission.strip():
        raise ValueError(f"operation {spec.name!r} permission must be non-empty")
    if spec.quota_scope not in {"application", "live_world", "durable_world"}:
        raise ValueError(f"operation {spec.name!r} has invalid quota scope")
    if spec.quota_scope == "application":
        if spec.world_key is not None:
            raise ValueError(
                f"application-scoped operation {spec.name!r} must not define world_key"
            )
    elif not callable(spec.world_key):
        raise ValueError(f"{spec.quota_scope}-scoped operation {spec.name!r} requires world_key")
    if type(spec.trusted) is not bool or type(spec.untrusted) is not bool:
        raise TypeError(f"operation {spec.name!r} availability flags must be booleans")
    if not spec.trusted and not spec.untrusted:
        raise ValueError(f"operation {spec.name!r} is unavailable to every caller")
    if not callable(spec.token_cost):
        if isinstance(spec.token_cost, bool) or not isinstance(spec.token_cost, int):
            raise TypeError(f"operation {spec.name!r} token cost must be an integer or callable")
        if spec.token_cost < 0:
            raise ValueError(f"operation {spec.name!r} token cost must be non-negative")
    if spec.durable is not None:
        if not isinstance(spec.durable, DurableOperation):
            raise TypeError(f"operation {spec.name!r} durable metadata is invalid")
        if getattr(spec.model, "direct_only", True):
            raise ValueError(f"direct-only operation model {spec.model.__name__} cannot be durable")
        if not callable(spec.durable.decode):
            raise TypeError(f"operation {spec.name!r} durable decoder must be callable")
        if not callable(spec.durable.materialize):
            raise TypeError(f"operation {spec.name!r} durable materializer must be callable")


def _validate_operation_discriminator(
    spec: OperationSpec,
    operation: BaseModel,
) -> None:
    if getattr(operation, "operation", None) != spec.name:
        raise ValueError(f"operation discriminator does not retain registered name {spec.name!r}")


__all__ = [
    "DurableOperation",
    "OperationRegistry",
    "OperationSpec",
    "canonical_operation_json",
    "decode_canonical_operation",
    "encode_canonical_operation",
    "operation_rejection_metadata",
]
