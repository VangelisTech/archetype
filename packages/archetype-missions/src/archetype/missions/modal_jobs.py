# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable outer Modal job identity for Temporal-owned Mission execution."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, runtime_checkable

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_OPERATION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:._/-]{0,1023}$")
_CALL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:._/-]{0,1023}$")
_SCHEMA_VERSION = 1

ModalMissionFamily = Literal["author", "critic"]


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _require_digest(value: str, field: str) -> None:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase sha256 digest")


def _require_identity(value: str, field: str, pattern: re.Pattern[str]) -> None:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"{field} is invalid")


@dataclass(frozen=True, slots=True)
class ModalMissionJobRef:
    """Exact deployed Modal call selected for one immutable Mission operation."""

    family: ModalMissionFamily
    operation_id: str
    request_digest: str
    namespace_digest: str
    call_id: str

    def __post_init__(self) -> None:
        if self.family not in {"author", "critic"}:
            raise ValueError("Modal Mission job family is invalid")
        _require_identity(self.operation_id, "Modal Mission operation_id", _OPERATION)
        _require_digest(self.request_digest, "Modal Mission request_digest")
        _require_digest(self.namespace_digest, "Modal Mission namespace_digest")
        _require_identity(self.call_id, "Modal Mission call_id", _CALL_ID)


@dataclass(frozen=True, slots=True)
class ModalMissionJobRunning:
    ref: ModalMissionJobRef


@dataclass(frozen=True, slots=True)
class ModalMissionJobReady:
    ref: ModalMissionJobRef


@dataclass(frozen=True, slots=True)
class ModalMissionJobUnknown:
    ref: ModalMissionJobRef | None
    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("Modal Mission Unknown reason must be non-empty")
        if len(self.reason) > 4096:
            raise ValueError("Modal Mission Unknown reason must be at most 4096 characters")


ModalMissionJobPoll = ModalMissionJobRunning | ModalMissionJobReady | ModalMissionJobUnknown


@runtime_checkable
class ModalMissionJobRuntime(Protocol):
    """Narrow durable operations required by the outer Mission job adapter."""

    async def get(self, key: str) -> object: ...

    async def put_if_absent(self, key: str, value: Mapping[str, Any]) -> bool: ...

    async def spawn(
        self,
        *,
        family: ModalMissionFamily,
        operation_id: str,
        request_bytes: bytes,
        namespace_digest: str,
    ) -> object: ...

    def call_id(self, call: object) -> str: ...

    async def reattach(self, call_id: str) -> object: ...

    async def call_result(self, call: object, *, timeout_seconds: float) -> object: ...

    async def result_ready(self, ref: ModalMissionJobRef) -> bool: ...


class ModalMissionJobStillRunning(Exception):
    """The exact durable provider call has not reached a terminal outcome."""


@dataclass(frozen=True, slots=True)
class ModalMissionJobNamespace:
    """Deployment and policy coordinates that make a call identity meaningful."""

    deployment_digest: str
    image_id: str
    result_dict_name: str
    redaction_policy_id: str
    protocol_epoch: int = 1

    def __post_init__(self) -> None:
        _require_digest(self.deployment_digest, "Modal Mission deployment_digest")
        for field, value in (
            ("image_id", self.image_id),
            ("result_dict_name", self.result_dict_name),
            ("redaction_policy_id", self.redaction_policy_id),
        ):
            _require_identity(value, f"Modal Mission {field}", _OPERATION)
        if isinstance(self.protocol_epoch, bool) or self.protocol_epoch != 1:
            raise ValueError("Modal Mission job protocol epoch is unsupported")

    @property
    def digest(self) -> str:
        return hashlib.sha256(
            _canonical_json(
                {
                    "deployment_digest": self.deployment_digest,
                    "image_id": self.image_id,
                    "protocol_epoch": self.protocol_epoch,
                    "redaction_policy_id": self.redaction_policy_id,
                    "result_dict_name": self.result_dict_name,
                    "schema_version": _SCHEMA_VERSION,
                }
            )
        ).hexdigest()

    def start_record(
        self,
        *,
        family: ModalMissionFamily,
        operation_id: str,
        request_digest: str,
    ) -> dict[str, str | int]:
        provisional = ModalMissionJobRef(
            family=family,
            operation_id=operation_id,
            request_digest=request_digest,
            namespace_digest=self.digest,
            call_id="pending",
        )
        return {
            "deployment_digest": self.deployment_digest,
            "family": provisional.family,
            "image_id": self.image_id,
            "namespace_digest": provisional.namespace_digest,
            "operation_id": provisional.operation_id,
            "protocol_epoch": self.protocol_epoch,
            "redaction_policy_id": self.redaction_policy_id,
            "request_digest": provisional.request_digest,
            "result_dict_name": self.result_dict_name,
            "schema_version": _SCHEMA_VERSION,
        }


def modal_mission_job_key(
    family: ModalMissionFamily,
    operation_id: str,
    phase: Literal["start", "call", "cancel"],
) -> str:
    ModalMissionJobRef(
        family=family,
        operation_id=operation_id,
        request_digest="0" * 64,
        namespace_digest="0" * 64,
        call_id="key",
    )
    digest = hashlib.sha256(operation_id.encode()).hexdigest()
    return f"{family}:{phase}:{digest}"


def modal_mission_call_record(ref: ModalMissionJobRef) -> dict[str, str | int]:
    return {
        "call_id": ref.call_id,
        "family": ref.family,
        "namespace_digest": ref.namespace_digest,
        "operation_id": ref.operation_id,
        "request_digest": ref.request_digest,
        "schema_version": _SCHEMA_VERSION,
    }


def parse_modal_mission_call_record(raw: object) -> ModalMissionJobRef:
    if not isinstance(raw, dict):
        raise ValueError("Modal Mission call record is not an object")
    record = dict(raw)
    if (
        set(record)
        != {
            "call_id",
            "family",
            "namespace_digest",
            "operation_id",
            "request_digest",
            "schema_version",
        }
        or record.get("schema_version") != _SCHEMA_VERSION
    ):
        raise ValueError("Modal Mission call record is incompatible")
    ref = ModalMissionJobRef(
        family=record["family"],
        operation_id=record["operation_id"],
        request_digest=record["request_digest"],
        namespace_digest=record["namespace_digest"],
        call_id=record["call_id"],
    )
    if modal_mission_call_record(ref) != record:
        raise ValueError("Modal Mission call record is not canonical")
    return ref


class ModalMissionJobClient:
    """Start once and observe one provider-native Mission controller call."""

    def __init__(
        self,
        namespace: ModalMissionJobNamespace,
        runtime: ModalMissionJobRuntime,
    ) -> None:
        self._namespace = namespace
        self._runtime = runtime

    async def start(
        self,
        *,
        family: ModalMissionFamily,
        operation_id: str,
        request_bytes: bytes,
        request_digest: str,
    ) -> ModalMissionJobRef | ModalMissionJobUnknown:
        _require_digest(request_digest, "Modal Mission request_digest")
        if hashlib.sha256(request_bytes).hexdigest() != request_digest:
            raise ValueError("Modal Mission request bytes do not match request_digest")
        start_key = modal_mission_job_key(family, operation_id, "start")
        call_key = modal_mission_job_key(family, operation_id, "call")
        marker = self._namespace.start_record(
            family=family,
            operation_id=operation_id,
            request_digest=request_digest,
        )
        acquired = await self._runtime.put_if_absent(start_key, marker)
        if not acquired:
            if await self._runtime.get(start_key) != marker:
                return ModalMissionJobUnknown(None, "durable start marker conflicts")
            raw_call = await self._runtime.get(call_key)
            if raw_call is None:
                return ModalMissionJobUnknown(
                    None,
                    "durable start exists without a self-registered provider call",
                )
            try:
                return self._require_ref(
                    parse_modal_mission_call_record(raw_call),
                    family=family,
                    operation_id=operation_id,
                    request_digest=request_digest,
                )
            except ValueError as exc:
                return ModalMissionJobUnknown(None, str(exc))

        call = await self._runtime.spawn(
            family=family,
            operation_id=operation_id,
            request_bytes=request_bytes,
            namespace_digest=self._namespace.digest,
        )
        ref = ModalMissionJobRef(
            family=family,
            operation_id=operation_id,
            request_digest=request_digest,
            namespace_digest=self._namespace.digest,
            call_id=self._runtime.call_id(call),
        )
        record = modal_mission_call_record(ref)
        inserted = await self._runtime.put_if_absent(call_key, record)
        if not inserted and await self._runtime.get(call_key) != record:
            return ModalMissionJobUnknown(ref, "durable provider call identity conflicts")
        return ref

    async def poll(self, ref: ModalMissionJobRef) -> ModalMissionJobPoll:
        try:
            self._require_ref(
                ref,
                family=ref.family,
                operation_id=ref.operation_id,
                request_digest=ref.request_digest,
            )
            await self._require_records(ref)
            if await self._runtime.result_ready(ref):
                return ModalMissionJobReady(ref)
            call = await self._runtime.reattach(ref.call_id)
            try:
                await self._runtime.call_result(call, timeout_seconds=0.0)
            except ModalMissionJobStillRunning:
                return ModalMissionJobRunning(ref)
            except Exception as exc:
                if await self._runtime.result_ready(ref):
                    return ModalMissionJobReady(ref)
                return ModalMissionJobUnknown(
                    ref,
                    f"provider call terminated without a durable result ({type(exc).__name__})",
                )
            if await self._runtime.result_ready(ref):
                return ModalMissionJobReady(ref)
            return ModalMissionJobUnknown(ref, "provider call completed without a durable result")
        except ValueError as exc:
            return ModalMissionJobUnknown(ref, str(exc))

    async def _require_records(self, ref: ModalMissionJobRef) -> None:
        marker = self._namespace.start_record(
            family=ref.family,
            operation_id=ref.operation_id,
            request_digest=ref.request_digest,
        )
        if (
            await self._runtime.get(modal_mission_job_key(ref.family, ref.operation_id, "start"))
            != marker
        ):
            raise ValueError("durable start marker conflicts")
        if await self._runtime.get(
            modal_mission_job_key(ref.family, ref.operation_id, "call")
        ) != modal_mission_call_record(ref):
            raise ValueError("durable provider call identity conflicts")

    def _require_ref(
        self,
        ref: ModalMissionJobRef,
        *,
        family: ModalMissionFamily,
        operation_id: str,
        request_digest: str,
    ) -> ModalMissionJobRef:
        if ref != ModalMissionJobRef(
            family=family,
            operation_id=operation_id,
            request_digest=request_digest,
            namespace_digest=self._namespace.digest,
            call_id=ref.call_id,
        ):
            raise ValueError("Modal Mission job reference conflicts with this namespace")
        return ref


__all__ = [
    "ModalMissionFamily",
    "ModalMissionJobNamespace",
    "ModalMissionJobClient",
    "ModalMissionJobPoll",
    "ModalMissionJobReady",
    "ModalMissionJobRef",
    "ModalMissionJobRunning",
    "ModalMissionJobRuntime",
    "ModalMissionJobStillRunning",
    "ModalMissionJobUnknown",
    "modal_mission_call_record",
    "modal_mission_job_key",
    "parse_modal_mission_call_record",
]
