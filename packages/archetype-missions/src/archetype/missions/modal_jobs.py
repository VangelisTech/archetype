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
_OPERATION_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_COHORT_ID = re.compile(r"^cohort-v1:[0-9a-f]{32}$")
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


@dataclass(frozen=True, slots=True)
class ModalMissionResourceIntent:
    """Durable cleanup authority written before either sandbox is created."""

    ref: ModalMissionJobRef
    operation_digest: str
    cohort_id: str

    def __post_init__(self) -> None:
        if _OPERATION_DIGEST.fullmatch(self.operation_digest) is None:
            raise ValueError("Modal Mission resource operation digest is invalid")
        if _COHORT_ID.fullmatch(self.cohort_id) is None:
            raise ValueError("Modal Mission resource cohort identity is invalid")


@dataclass(frozen=True, slots=True)
class ModalMissionResourceRef:
    """One exact provider resource bound to a durable resource intent."""

    intent: ModalMissionResourceIntent
    role: Literal["auth", "mission"]
    sandbox_id: str

    def __post_init__(self) -> None:
        if self.role not in {"auth", "mission"}:
            raise ValueError("Modal Mission resource role is invalid")
        _require_identity(self.sandbox_id, "Modal Mission sandbox_id", _CALL_ID)


@dataclass(frozen=True, slots=True)
class ModalMissionJobResources:
    """Current durable resource evidence for one exact controller call."""

    intent: ModalMissionResourceIntent
    auth: ModalMissionResourceRef | None = None
    mission: ModalMissionResourceRef | None = None

    def __post_init__(self) -> None:
        for role, value in (("auth", self.auth), ("mission", self.mission)):
            if value is not None and (value.role != role or value.intent != self.intent):
                raise ValueError("Modal Mission resource evidence conflicts with its intent")


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

    async def cancel(self, call: object) -> None: ...

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
    phase: Literal[
        "start",
        "call",
        "resource-intent",
        "resource-auth",
        "resource-mission",
        "cancel",
        "cleanup",
    ],
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


def modal_mission_resource_intent_record(
    intent: ModalMissionResourceIntent,
) -> dict[str, str | int]:
    ref = intent.ref
    return {
        "call_id": ref.call_id,
        "cohort_id": intent.cohort_id,
        "family": ref.family,
        "namespace_digest": ref.namespace_digest,
        "operation_digest": intent.operation_digest,
        "operation_id": ref.operation_id,
        "request_digest": ref.request_digest,
        "schema_version": _SCHEMA_VERSION,
    }


def parse_modal_mission_resource_intent_record(raw: object) -> ModalMissionResourceIntent:
    if not isinstance(raw, dict):
        raise ValueError("Modal Mission resource intent record is not an object")
    record = dict(raw)
    if (
        set(record)
        != {
            "call_id",
            "cohort_id",
            "family",
            "namespace_digest",
            "operation_digest",
            "operation_id",
            "request_digest",
            "schema_version",
        }
        or record.get("schema_version") != _SCHEMA_VERSION
    ):
        raise ValueError("Modal Mission resource intent record is incompatible")
    intent = ModalMissionResourceIntent(
        ref=ModalMissionJobRef(
            family=record["family"],
            operation_id=record["operation_id"],
            request_digest=record["request_digest"],
            namespace_digest=record["namespace_digest"],
            call_id=record["call_id"],
        ),
        operation_digest=record["operation_digest"],
        cohort_id=record["cohort_id"],
    )
    if modal_mission_resource_intent_record(intent) != record:
        raise ValueError("Modal Mission resource intent record is not canonical")
    return intent


def modal_mission_resource_record(resource: ModalMissionResourceRef) -> dict[str, str | int]:
    return {
        **modal_mission_resource_intent_record(resource.intent),
        "role": resource.role,
        "sandbox_id": resource.sandbox_id,
    }


def parse_modal_mission_resource_record(raw: object) -> ModalMissionResourceRef:
    if not isinstance(raw, dict):
        raise ValueError("Modal Mission resource record is not an object")
    record = dict(raw)
    role = record.pop("role", None)
    sandbox_id = record.pop("sandbox_id", None)
    resource = ModalMissionResourceRef(
        intent=parse_modal_mission_resource_intent_record(record),
        role=role,
        sandbox_id=sandbox_id,
    )
    if modal_mission_resource_record(resource) != raw:
        raise ValueError("Modal Mission resource record is not canonical")
    return resource


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

    async def cancel(self, ref: ModalMissionJobRef) -> ModalMissionJobRef:
        """Persist cancellation intent, then cancel only the exact durable call."""

        self._require_ref(
            ref,
            family=ref.family,
            operation_id=ref.operation_id,
            request_digest=ref.request_digest,
        )
        await self._require_records(ref)
        key = modal_mission_job_key(ref.family, ref.operation_id, "cancel")
        record = {**modal_mission_call_record(ref), "phase": "cancel"}
        inserted = await self._runtime.put_if_absent(key, record)
        stored = await self._runtime.get(key)
        if not ((inserted and stored == record) or (not inserted and stored == record)):
            raise ValueError("durable cancellation intent conflicts")
        call = await self._runtime.reattach(ref.call_id)
        if self._runtime.call_id(call) != ref.call_id:
            raise ValueError("Modal cancellation reattached a different provider call")
        await self._runtime.cancel(call)
        return ref

    async def register_remote_call(
        self,
        *,
        family: ModalMissionFamily,
        operation_id: str,
        request_digest: str,
        call_id: str,
    ) -> ModalMissionJobRef | ModalMissionJobUnknown:
        """Let the remote Function fence itself before any provider effect."""

        ref = ModalMissionJobRef(
            family=family,
            operation_id=operation_id,
            request_digest=request_digest,
            namespace_digest=self._namespace.digest,
            call_id=call_id,
        )
        marker = self._namespace.start_record(
            family=family,
            operation_id=operation_id,
            request_digest=request_digest,
        )
        start_key = modal_mission_job_key(family, operation_id, "start")
        if await self._runtime.get(start_key) != marker:
            return ModalMissionJobUnknown(ref, "remote call has no exact durable start")
        call_key = modal_mission_job_key(family, operation_id, "call")
        record = modal_mission_call_record(ref)
        inserted = await self._runtime.put_if_absent(call_key, record)
        stored = await self._runtime.get(call_key)
        if (inserted and stored == record) or (not inserted and stored == record):
            return ref
        return ModalMissionJobUnknown(
            ref,
            "another provider call already owns this Mission operation",
        )

    async def register_resource_intent(
        self,
        ref: ModalMissionJobRef,
        *,
        operation_digest: str,
        cohort_id: str,
    ) -> ModalMissionResourceIntent | ModalMissionJobUnknown:
        """Persist immutable cleanup intent before the first sandbox effect."""

        try:
            self._require_ref(
                ref,
                family=ref.family,
                operation_id=ref.operation_id,
                request_digest=ref.request_digest,
            )
            await self._require_records(ref)
            intent = ModalMissionResourceIntent(
                ref=ref,
                operation_digest=operation_digest,
                cohort_id=cohort_id,
            )
            key = modal_mission_job_key(ref.family, ref.operation_id, "resource-intent")
            record = modal_mission_resource_intent_record(intent)
            inserted = await self._runtime.put_if_absent(key, record)
            stored = await self._runtime.get(key)
            if (inserted and stored == record) or (not inserted and stored == record):
                return intent
            return ModalMissionJobUnknown(ref, "durable sandbox resource intent conflicts")
        except ValueError as exc:
            return ModalMissionJobUnknown(ref, str(exc))

    async def register_resource(
        self,
        intent: ModalMissionResourceIntent,
        *,
        role: Literal["auth", "mission"],
        sandbox_id: str,
    ) -> ModalMissionResourceRef | ModalMissionJobUnknown:
        """Persist one exact role ID before execution may cross the boundary."""

        try:
            await self._require_resource_intent(intent)
            resource = ModalMissionResourceRef(
                intent=intent,
                role=role,
                sandbox_id=sandbox_id,
            )
            key = modal_mission_job_key(
                intent.ref.family,
                intent.ref.operation_id,
                "resource-auth" if role == "auth" else "resource-mission",
            )
            record = modal_mission_resource_record(resource)
            inserted = await self._runtime.put_if_absent(key, record)
            stored = await self._runtime.get(key)
            if (inserted and stored == record) or (not inserted and stored == record):
                return resource
            return ModalMissionJobUnknown(intent.ref, f"durable {role} sandbox identity conflicts")
        except ValueError as exc:
            return ModalMissionJobUnknown(intent.ref, str(exc))

    async def resources(self, ref: ModalMissionJobRef) -> ModalMissionJobResources | None:
        """Read exact resource evidence without creating, cancelling, or cleaning up."""

        self._require_ref(
            ref,
            family=ref.family,
            operation_id=ref.operation_id,
            request_digest=ref.request_digest,
        )
        await self._require_records(ref)
        raw_intent = await self._runtime.get(
            modal_mission_job_key(ref.family, ref.operation_id, "resource-intent")
        )
        if raw_intent is None:
            return None
        intent = parse_modal_mission_resource_intent_record(raw_intent)
        if intent.ref != ref:
            raise ValueError("durable sandbox resource intent belongs to another call")
        roles: dict[str, ModalMissionResourceRef | None] = {}
        for role in ("auth", "mission"):
            raw = await self._runtime.get(
                modal_mission_job_key(
                    ref.family,
                    ref.operation_id,
                    "resource-auth" if role == "auth" else "resource-mission",
                )
            )
            resource = None if raw is None else parse_modal_mission_resource_record(raw)
            if resource is not None and (resource.intent != intent or resource.role != role):
                raise ValueError(f"durable {role} sandbox evidence conflicts")
            roles[role] = resource
        return ModalMissionJobResources(
            intent=intent,
            auth=roles["auth"],
            mission=roles["mission"],
        )

    async def _require_resource_intent(self, intent: ModalMissionResourceIntent) -> None:
        await self._require_records(intent.ref)
        stored = await self._runtime.get(
            modal_mission_job_key(intent.ref.family, intent.ref.operation_id, "resource-intent")
        )
        if stored != modal_mission_resource_intent_record(intent):
            raise ValueError("durable sandbox resource intent conflicts")

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
    "ModalMissionJobResources",
    "ModalMissionJobRunning",
    "ModalMissionJobRuntime",
    "ModalMissionJobStillRunning",
    "ModalMissionJobUnknown",
    "ModalMissionResourceIntent",
    "ModalMissionResourceRef",
    "modal_mission_call_record",
    "modal_mission_job_key",
    "modal_mission_resource_intent_record",
    "modal_mission_resource_record",
    "parse_modal_mission_call_record",
    "parse_modal_mission_resource_intent_record",
    "parse_modal_mission_resource_record",
]
