# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Behavior contracts for provider-native durable Mission jobs."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import Any

import pytest

from archetype.missions.modal_jobs import (
    ModalMissionJobClient,
    ModalMissionJobNamespace,
    ModalMissionJobReady,
    ModalMissionJobRef,
    ModalMissionJobResourceRecorder,
    ModalMissionJobResources,
    ModalMissionJobRunning,
    ModalMissionJobStillRunning,
    ModalMissionJobUnknown,
    ModalMissionResourceIntent,
    ModalMissionResourceRef,
    modal_mission_call_record,
    modal_mission_job_key,
    modal_mission_resource_intent_record,
    modal_mission_resource_record,
    parse_modal_mission_call_record,
    parse_modal_mission_resource_intent_record,
    parse_modal_mission_resource_record,
)
from archetype.missions.sandboxes import ModalSandboxOperationIdentity


class _Runtime:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.spawn_count = 0
        self.ready = False
        self.terminal: BaseException | None = None
        self.running = True
        self.cancelled: list[str] = []

    async def get(self, key: str) -> object:
        return self.values.get(key)

    async def put_if_absent(self, key: str, value: dict[str, Any]) -> bool:
        if key in self.values:
            return False
        self.values[key] = dict(value)
        return True

    async def spawn(self, **_kwargs: object) -> object:
        self.spawn_count += 1
        return "fc-123"

    def call_id(self, call: object) -> str:
        return str(call)

    async def reattach(self, call_id: str) -> object:
        return call_id

    async def cancel(self, call: object) -> None:
        self.cancelled.append(str(call))

    async def call_result(self, call: object, *, timeout_seconds: float) -> object:
        assert call == "fc-123"
        assert timeout_seconds == 0
        if self.terminal is not None:
            raise self.terminal
        if self.running:
            raise ModalMissionJobStillRunning
        return {"status": "complete"}

    async def result_ready(self, ref: ModalMissionJobRef) -> bool:
        assert ref.call_id == "fc-123"
        return self.ready


def _namespace() -> ModalMissionJobNamespace:
    return ModalMissionJobNamespace(
        deployment_digest="a" * 64,
        image_id="im-123",
        result_dict_name="mission-results",
        redaction_policy_id="redaction-v1",
    )


def _ref() -> ModalMissionJobRef:
    return ModalMissionJobRef(
        family="author",
        operation_id="mission:author:dispatch-1",
        request_digest="b" * 64,
        namespace_digest=_namespace().digest,
        call_id="fc-123",
    )


def test_call_record_round_trips_exact_job_identity() -> None:
    ref = _ref()
    encoded = modal_mission_call_record(ref)

    assert parse_modal_mission_call_record(encoded) == ref
    assert modal_mission_job_key("author", ref.operation_id, "call").startswith("author:call:")


def test_call_record_rejects_every_conflicting_identity_coordinate() -> None:
    record = modal_mission_call_record(_ref())
    changes = {
        "family": "critic",
        "operation_id": "mission:author:dispatch-2",
        "request_digest": "c" * 64,
        "namespace_digest": "d" * 64,
        "call_id": "fc-456",
    }
    for field, value in changes.items():
        conflicting = dict(record)
        conflicting[field] = value
        assert parse_modal_mission_call_record(conflicting) != _ref()

    with pytest.raises(ValueError, match="incompatible"):
        parse_modal_mission_call_record({**record, "extra": "field"})


def test_namespace_digest_binds_deployment_image_result_store_and_policy() -> None:
    namespace = _namespace()
    for field, value in {
        "deployment_digest": "e" * 64,
        "image_id": "im-456",
        "result_dict_name": "other-results",
        "redaction_policy_id": "redaction-v2",
    }.items():
        assert replace(namespace, **{field: value}).digest != namespace.digest

    marker = namespace.start_record(
        family="author",
        operation_id=_ref().operation_id,
        request_digest=_ref().request_digest,
    )
    assert marker["namespace_digest"] == namespace.digest
    assert marker["redaction_policy_id"] == namespace.redaction_policy_id


def test_unknown_is_bounded_and_requires_a_reason() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        ModalMissionJobUnknown(_ref(), "")
    with pytest.raises(ValueError, match="4096"):
        ModalMissionJobUnknown(_ref(), "x" * 4097)


@pytest.mark.asyncio
async def test_start_is_the_only_spawning_operation_and_replay_reuses_call() -> None:
    runtime = _Runtime()
    client = ModalMissionJobClient(_namespace(), runtime)
    request = b"canonical-request"
    digest = hashlib.sha256(request).hexdigest()

    first = await client.start(
        family="author",
        operation_id=_ref().operation_id,
        request_bytes=request,
        request_digest=digest,
    )
    assert isinstance(first, ModalMissionJobRef)
    replay = await client.start(
        family="author",
        operation_id=_ref().operation_id,
        request_bytes=request,
        request_digest=digest,
    )

    assert replay == first
    assert runtime.spawn_count == 1


@pytest.mark.asyncio
async def test_start_marker_without_call_is_unknown_and_never_respawns() -> None:
    runtime = _Runtime()
    client = ModalMissionJobClient(_namespace(), runtime)
    request = b"canonical-request"
    digest = hashlib.sha256(request).hexdigest()
    marker = _namespace().start_record(
        family="author", operation_id=_ref().operation_id, request_digest=digest
    )
    runtime.values[modal_mission_job_key("author", _ref().operation_id, "start")] = marker

    outcome = await client.start(
        family="author",
        operation_id=_ref().operation_id,
        request_bytes=request,
        request_digest=digest,
    )

    assert isinstance(outcome, ModalMissionJobUnknown)
    assert runtime.spawn_count == 0


@pytest.mark.asyncio
async def test_poll_is_result_first_and_terminal_without_result_is_unknown() -> None:
    runtime = _Runtime()
    client = ModalMissionJobClient(_namespace(), runtime)
    request = b"canonical-request"
    digest = hashlib.sha256(request).hexdigest()
    started = await client.start(
        family="author",
        operation_id=_ref().operation_id,
        request_bytes=request,
        request_digest=digest,
    )
    assert isinstance(started, ModalMissionJobRef)

    assert isinstance(await client.poll(started), ModalMissionJobRunning)
    runtime.terminal = RuntimeError("remote failure")
    outcome = await client.poll(started)
    assert isinstance(outcome, ModalMissionJobUnknown)
    assert "terminated without a durable result" in outcome.reason

    runtime.ready = True
    assert isinstance(await client.poll(started), ModalMissionJobReady)


@pytest.mark.asyncio
async def test_remote_self_registration_fences_duplicate_calls_before_effects() -> None:
    runtime = _Runtime()
    client = ModalMissionJobClient(_namespace(), runtime)
    request = b"canonical-request"
    digest = hashlib.sha256(request).hexdigest()
    marker = _namespace().start_record(
        family="author", operation_id=_ref().operation_id, request_digest=digest
    )
    runtime.values[modal_mission_job_key("author", _ref().operation_id, "start")] = marker

    winner = await client.register_remote_call(
        family="author",
        operation_id=_ref().operation_id,
        request_digest=digest,
        call_id="fc-winner",
    )
    duplicate = await client.register_remote_call(
        family="author",
        operation_id=_ref().operation_id,
        request_digest=digest,
        call_id="fc-duplicate",
    )

    assert isinstance(winner, ModalMissionJobRef)
    assert winner.call_id == "fc-winner"
    assert isinstance(duplicate, ModalMissionJobUnknown)
    assert "already owns" in duplicate.reason


@pytest.mark.asyncio
async def test_remote_self_registration_requires_exact_start_marker() -> None:
    runtime = _Runtime()
    client = ModalMissionJobClient(_namespace(), runtime)
    outcome = await client.register_remote_call(
        family="author",
        operation_id=_ref().operation_id,
        request_digest=_ref().request_digest,
        call_id="fc-unstarted",
    )

    assert isinstance(outcome, ModalMissionJobUnknown)
    assert "no exact durable start" in outcome.reason
    assert not runtime.values


@pytest.mark.asyncio
async def test_resource_intent_and_each_role_are_immutable_and_reconstructable() -> None:
    runtime = _Runtime()
    client = ModalMissionJobClient(_namespace(), runtime)
    request = b"canonical-request"
    digest = hashlib.sha256(request).hexdigest()
    started = await client.start(
        family="author",
        operation_id=_ref().operation_id,
        request_bytes=request,
        request_digest=digest,
    )
    assert isinstance(started, ModalMissionJobRef)

    intent = await client.register_resource_intent(
        started,
        operation_digest="sha256:" + "c" * 64,
        cohort_id="cohort-v1:" + "d" * 32,
    )
    assert isinstance(intent, ModalMissionResourceIntent)
    auth = await client.register_resource(intent, role="auth", sandbox_id="sb-auth-1")
    mission = await client.register_resource(
        intent,
        role="mission",
        sandbox_id="sb-mission-1",
    )
    assert isinstance(auth, ModalMissionResourceRef)
    assert isinstance(mission, ModalMissionResourceRef)

    observed = await client.resources(started)
    assert observed == ModalMissionJobResources(
        intent=intent,
        auth=auth,
        mission=mission,
    )
    assert (
        parse_modal_mission_resource_intent_record(modal_mission_resource_intent_record(intent))
        == intent
    )
    assert parse_modal_mission_resource_record(modal_mission_resource_record(auth)) == auth


@pytest.mark.asyncio
async def test_resource_identity_conflicts_fail_closed_without_replacing_evidence() -> None:
    runtime = _Runtime()
    client = ModalMissionJobClient(_namespace(), runtime)
    request = b"canonical-request"
    digest = hashlib.sha256(request).hexdigest()
    started = await client.start(
        family="author",
        operation_id=_ref().operation_id,
        request_bytes=request,
        request_digest=digest,
    )
    assert isinstance(started, ModalMissionJobRef)
    intent = await client.register_resource_intent(
        started,
        operation_digest="sha256:" + "c" * 64,
        cohort_id="cohort-v1:" + "d" * 32,
    )
    assert isinstance(intent, ModalMissionResourceIntent)
    assert isinstance(
        await client.register_resource(intent, role="auth", sandbox_id="sb-auth-1"),
        ModalMissionResourceRef,
    )

    conflicting_intent = await client.register_resource_intent(
        started,
        operation_digest="sha256:" + "e" * 64,
        cohort_id=intent.cohort_id,
    )
    conflicting_role = await client.register_resource(
        intent,
        role="auth",
        sandbox_id="sb-auth-2",
    )

    assert isinstance(conflicting_intent, ModalMissionJobUnknown)
    assert isinstance(conflicting_role, ModalMissionJobUnknown)
    observed = await client.resources(started)
    assert observed is not None
    assert observed.intent == intent
    assert observed.auth is not None
    assert observed.auth.sandbox_id == "sb-auth-1"


@pytest.mark.asyncio
async def test_cancellation_persists_intent_before_cancelling_only_the_exact_call() -> None:
    runtime = _Runtime()
    client = ModalMissionJobClient(_namespace(), runtime)
    request = b"canonical-request"
    digest = hashlib.sha256(request).hexdigest()
    started = await client.start(
        family="author",
        operation_id=_ref().operation_id,
        request_bytes=request,
        request_digest=digest,
    )
    assert isinstance(started, ModalMissionJobRef)

    assert await client.cancel(started) == started
    assert await client.cancel(started) == started

    assert runtime.cancelled == [started.call_id, started.call_id]
    assert runtime.values[
        modal_mission_job_key(started.family, started.operation_id, "cancel")
    ] == {**modal_mission_call_record(started), "phase": "cancel"}


@pytest.mark.asyncio
async def test_resource_observer_and_cleanup_replay_preserve_exact_partial_evidence() -> None:
    runtime = _Runtime()
    client = ModalMissionJobClient(_namespace(), runtime)
    request = b"canonical-request"
    digest = hashlib.sha256(request).hexdigest()
    started = await client.start(
        family="author",
        operation_id=_ref().operation_id,
        request_bytes=request,
        request_digest=digest,
    )
    assert isinstance(started, ModalMissionJobRef)
    identity = ModalSandboxOperationIdentity(
        workspace_name="workspace",
        environment_name="main",
        app_name="mission-app",
        operation_id=started.operation_id,
        protocol_epoch=1,
    )
    cohort_id = "cohort-v1:" + "d" * 32
    recorder = ModalMissionJobResourceRecorder(client, started)

    await recorder.observe(identity, cohort_id, "intent", "")
    await recorder.observe(identity, cohort_id, "auth", "sb-auth-partial")
    observed: list[ModalMissionJobResources] = []

    async def clean(resources: ModalMissionJobResources) -> None:
        observed.append(resources)

    assert await client.cleanup(started, cleaner=clean) == started
    assert await client.cleanup(started, cleaner=clean) == started

    resources = await client.resources(started)
    assert resources is not None
    assert resources.intent.operation_digest == identity.digest
    assert resources.intent.cohort_id == cohort_id
    assert resources.auth is not None
    assert resources.auth.sandbox_id == "sb-auth-partial"
    assert resources.mission is None
    assert observed == [resources, resources]
    assert runtime.values[
        modal_mission_job_key(started.family, started.operation_id, "cleanup")
    ] == {**modal_mission_resource_intent_record(resources.intent), "phase": "cleanup"}


@pytest.mark.asyncio
async def test_resource_observer_rejects_mismatched_operation_before_recording() -> None:
    runtime = _Runtime()
    client = ModalMissionJobClient(_namespace(), runtime)
    request = b"canonical-request"
    digest = hashlib.sha256(request).hexdigest()
    started = await client.start(
        family="author",
        operation_id=_ref().operation_id,
        request_bytes=request,
        request_digest=digest,
    )
    assert isinstance(started, ModalMissionJobRef)
    mismatched = ModalSandboxOperationIdentity(
        workspace_name="workspace",
        environment_name="main",
        app_name="mission-app",
        operation_id="mission:author:other-dispatch",
        protocol_epoch=1,
    )

    with pytest.raises(ValueError, match="another Mission job"):
        await ModalMissionJobResourceRecorder(client, started).observe(
            mismatched,
            "cohort-v1:" + "d" * 32,
            "intent",
            "",
        )

    assert (
        modal_mission_job_key(started.family, started.operation_id, "resource-intent")
        not in runtime.values
    )
