# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ref-only Temporal Activities for provider-native Mission jobs."""

from __future__ import annotations

import hashlib
from typing import Protocol

from temporalio import activity

from archetype.missions.modal_jobs import (
    ModalMissionJobPoll,
    ModalMissionJobReady,
    ModalMissionJobRef,
    ModalMissionJobResult,
    ModalMissionJobRunning,
    ModalMissionJobUnknown,
)

from .contracts import (
    MISSION_MODAL_JOB_CANCEL_ACTIVITY,
    MISSION_MODAL_JOB_CLEANUP_ACTIVITY,
    MISSION_MODAL_JOB_COLLECT_ACTIVITY,
    MISSION_MODAL_JOB_POLL_ACTIVITY,
    MISSION_MODAL_JOB_START_ACTIVITY,
    MissionJobValueRef,
    MissionModalJobCollection,
    MissionModalJobFamily,
    MissionModalJobPhaseInput,
    MissionModalJobPhaseResult,
    MissionModalJobRefPayload,
    MissionModalJobWorkflowInput,
)

_MAX_REASON_CHARS = 4096


class MissionModalJobService(Protocol):
    """Fixed built-in provider facade injected by host composition."""

    async def start(
        self,
        *,
        family: MissionModalJobFamily,
        operation_id: str,
        request_bytes: bytes,
    ) -> ModalMissionJobRef | ModalMissionJobUnknown: ...

    async def poll(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobPoll: ...

    async def collect(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobResult | ModalMissionJobUnknown: ...

    async def cancel(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobRef: ...

    async def cleanup(
        self,
        ref: ModalMissionJobRef,
        *,
        request_bytes: bytes,
    ) -> ModalMissionJobRef: ...


class MissionModalJobValueStore(Protocol):
    """Family-owned request/result values outside Temporal history."""

    async def get_request(self, ref: MissionJobValueRef) -> bytes: ...

    async def put_result(
        self,
        *,
        family: MissionModalJobFamily,
        operation_id: str,
        payload: bytes,
        payload_digest: str,
    ) -> MissionJobValueRef: ...


class MissionModalJobActivities:
    """Split durable start/poll/collect/cancel/cleanup adapter."""

    def __init__(
        self,
        jobs: MissionModalJobService,
        values: MissionModalJobValueStore,
    ) -> None:
        self._jobs = jobs
        self._values = values

    @activity.defn(name=MISSION_MODAL_JOB_START_ACTIVITY)
    async def start(self, command: MissionModalJobWorkflowInput) -> MissionModalJobPhaseResult:
        request_bytes = await self._request_bytes(command)
        outcome = await self._jobs.start(
            family=command.family,
            operation_id=command.operation_id,
            request_bytes=request_bytes,
        )
        if isinstance(outcome, ModalMissionJobUnknown):
            return _unknown(outcome)
        ref = _ref_payload(outcome)
        _require_job_ref(command, ref)
        return MissionModalJobPhaseResult(status="running", ref=ref)

    @activity.defn(name=MISSION_MODAL_JOB_POLL_ACTIVITY)
    async def poll(self, command: MissionModalJobPhaseInput) -> MissionModalJobPhaseResult:
        ref = _modal_ref(command.ref)
        _require_job_ref(command.job, command.ref)
        request_bytes = await self._request_bytes(command.job)
        outcome = await self._jobs.poll(ref, request_bytes=request_bytes)
        if isinstance(outcome, ModalMissionJobUnknown):
            return _unknown(outcome)
        if isinstance(outcome, ModalMissionJobReady):
            return MissionModalJobPhaseResult(status="ready", ref=_ref_payload(outcome.ref))
        if isinstance(outcome, ModalMissionJobRunning):
            return MissionModalJobPhaseResult(status="running", ref=_ref_payload(outcome.ref))
        raise TypeError("Modal Mission job poll returned an invalid outcome")

    @activity.defn(name=MISSION_MODAL_JOB_COLLECT_ACTIVITY)
    async def collect(self, command: MissionModalJobPhaseInput) -> MissionModalJobCollection:
        ref = _modal_ref(command.ref)
        _require_job_ref(command.job, command.ref)
        request_bytes = await self._request_bytes(command.job)
        outcome = await self._jobs.collect(ref, request_bytes=request_bytes)
        if isinstance(outcome, ModalMissionJobUnknown):
            return MissionModalJobCollection(
                status="unknown",
                ref=command.ref,
                reason=_reason(outcome.reason),
            )
        result = await self._values.put_result(
            family=command.job.family,
            operation_id=command.job.operation_id,
            payload=outcome.payload,
            payload_digest=outcome.payload_digest,
        )
        _validate_value_ref(result)
        if result.digest != outcome.payload_digest or result.size_bytes != len(outcome.payload):
            raise ValueError("Mission result store returned another provider value")
        return MissionModalJobCollection(
            status="ready",
            ref=_ref_payload(outcome.ref),
            result=result,
        )

    @activity.defn(name=MISSION_MODAL_JOB_CANCEL_ACTIVITY)
    async def cancel(self, command: MissionModalJobPhaseInput) -> MissionModalJobPhaseResult:
        ref = _modal_ref(command.ref)
        _require_job_ref(command.job, command.ref)
        request_bytes = await self._request_bytes(command.job)
        cancelled = await self._jobs.cancel(ref, request_bytes=request_bytes)
        if cancelled != ref:
            raise ValueError("Modal Mission cancellation returned another job")
        return MissionModalJobPhaseResult(status="ready", ref=command.ref)

    @activity.defn(name=MISSION_MODAL_JOB_CLEANUP_ACTIVITY)
    async def cleanup(self, command: MissionModalJobPhaseInput) -> MissionModalJobPhaseResult:
        ref = _modal_ref(command.ref)
        _require_job_ref(command.job, command.ref)
        request_bytes = await self._request_bytes(command.job)
        cleaned = await self._jobs.cleanup(ref, request_bytes=request_bytes)
        if cleaned != ref:
            raise ValueError("Modal Mission cleanup returned another job")
        return MissionModalJobPhaseResult(status="ready", ref=command.ref)

    async def _request_bytes(self, command: MissionModalJobWorkflowInput) -> bytes:
        _validate_value_ref(command.request)
        request_bytes = await self._values.get_request(command.request)
        if type(request_bytes) is not bytes:
            raise TypeError("Mission request store returned a non-bytes value")
        if len(request_bytes) != command.request.size_bytes:
            raise ValueError("Mission request size does not match its durable reference")
        if hashlib.sha256(request_bytes).hexdigest() != command.request.digest:
            raise ValueError("Mission request bytes do not match their durable reference")
        return request_bytes


def _ref_payload(ref: ModalMissionJobRef) -> MissionModalJobRefPayload:
    return MissionModalJobRefPayload(
        family=ref.family,
        operation_id=ref.operation_id,
        request_digest=ref.request_digest,
        namespace_digest=ref.namespace_digest,
        call_id=ref.call_id,
    )


def _modal_ref(ref: MissionModalJobRefPayload) -> ModalMissionJobRef:
    return ModalMissionJobRef(
        family=ref.family,
        operation_id=ref.operation_id,
        request_digest=ref.request_digest,
        namespace_digest=ref.namespace_digest,
        call_id=ref.call_id,
    )


def _require_job_ref(
    job: MissionModalJobWorkflowInput,
    ref: MissionModalJobRefPayload,
) -> None:
    if (
        ref.family != job.family
        or ref.operation_id != job.operation_id
        or ref.request_digest != job.request.digest
        or ref.namespace_digest != job.namespace_digest
    ):
        raise ValueError("Modal Mission job reference conflicts with Workflow input")


def _validate_value_ref(ref: MissionJobValueRef) -> None:
    if not ref.ref.strip() or len(ref.ref) > 4096:
        raise ValueError("Mission value reference is invalid")
    if len(ref.digest) != 64 or any(
        character not in "0123456789abcdef" for character in ref.digest
    ):
        raise ValueError("Mission value digest is invalid")
    if ref.size_bytes < 1 or ref.size_bytes > 1 << 20:
        raise ValueError("Mission value size is outside its durability bound")


def _unknown(outcome: ModalMissionJobUnknown) -> MissionModalJobPhaseResult:
    return MissionModalJobPhaseResult(
        status="unknown",
        ref=None if outcome.ref is None else _ref_payload(outcome.ref),
        reason=_reason(outcome.reason),
    )


def _reason(value: str) -> str:
    return value[:_MAX_REASON_CHARS]


__all__ = [
    "MissionModalJobActivities",
    "MissionModalJobService",
    "MissionModalJobValueStore",
]
