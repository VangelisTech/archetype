# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Behavior contracts for the Mission ECS-to-Temporal handoff."""

from __future__ import annotations

import hashlib
from typing import cast

import pytest
from temporalio.client import Client

from archetype.activities import ActivityCoordinator, ActivityExecutionIdentity
from archetype.core.interfaces import CommittedTickReceipt
from archetype.errors import ConflictError
from archetype.missions.activities import (
    AUTHOR_ACTIVITY_KIND,
    AuthorActivityRequestRef,
    AuthorActivityResultRef,
    author_provider_operation_id,
)
from archetype.missions.critics import CriticActivityRequestRef, CriticActivityResultRef
from archetype.missions.temporal.activity_runtime import (
    MissionTemporalAuthorActivityCatalog,
)
from archetype.missions.temporal.activity_values import MissionModalActivityValueStore
from archetype.missions.temporal.contracts import (
    MISSION_MODAL_JOB_TASK_QUEUE,
    MissionJobValueRef,
    MissionModalJobWorkflowInput,
    MissionModalJobWorkflowState,
    mission_modal_job_workflow_id,
)
from archetype.missions.temporal.modal_job_client import MissionModalJobTemporalClient
from archetype.storage.activity_catalog import (
    SqliteActivityCatalog,
    inspect_sqlite_activity_catalog,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("missions.temporal_activity_handoff"),
]

_WORLD_ID = "world-temporal-author"
_ACTIVITY_ID = "dispatch-temporal-author"
_NAMESPACE_DIGEST = "a" * 64
_REQUEST = b'{"kind":"author-request","schema_version":1}'
_REQUEST_DIGEST = hashlib.sha256(_REQUEST).hexdigest()
_REQUEST_REF = f"sqlite-value://author/{_REQUEST_DIGEST}"
_RESULT = b'{"kind":"author-result","schema_version":1}'
_RESULT_DIGEST = hashlib.sha256(_RESULT).hexdigest()
_RESULT_REF = MissionJobValueRef(
    ref=f"sqlite-value://author/{_RESULT_DIGEST}",
    digest=_RESULT_DIGEST,
    size_bytes=len(_RESULT),
)


def _receipt() -> CommittedTickReceipt:
    return CommittedTickReceipt(
        world_id=_WORLD_ID,
        run_id="run-temporal-author",
        committed_tick=4,
        visibility_token="manifest-temporal-author-4",
        commands_applied=0,
    )


class _AuthorValues:
    async def get_encoded_request(self, value: AuthorActivityRequestRef) -> bytes:
        assert value == AuthorActivityRequestRef(ref=_REQUEST_REF, digest=_REQUEST_DIGEST)
        return _REQUEST

    async def put_encoded_result(
        self,
        encoded: bytes,
        *,
        digest: str,
    ) -> AuthorActivityResultRef:
        assert encoded == _RESULT
        assert digest == _RESULT_DIGEST
        return AuthorActivityResultRef(
            ref=_RESULT_REF.ref,
            digest=digest,
            size_bytes=len(encoded),
        )


class _CriticValues:
    async def get_encoded_request(self, value: CriticActivityRequestRef) -> bytes:
        raise AssertionError(f"critic request was not expected: {value!r}")

    async def put_encoded_result(
        self,
        payload: bytes,
        *,
        digest: str,
    ) -> CriticActivityResultRef:
        raise AssertionError(f"critic result was not expected: {payload!r} {digest!r}")


class _WorkflowHandle:
    def __init__(self) -> None:
        self.state: MissionModalJobWorkflowState | None = None

    async def result(self) -> MissionModalJobWorkflowState:
        if self.state is None:
            raise AssertionError("test Workflow result was not configured")
        return self.state


class _AckLostOnceLauncher:
    """Accept the first start but lose its acknowledgement to the projector."""

    def __init__(self) -> None:
        self.commands: list[MissionModalJobWorkflowInput] = []
        self.handle = _WorkflowHandle()
        self.lose_first_ack = True

    async def start(self, command: MissionModalJobWorkflowInput) -> _WorkflowHandle:
        self.commands.append(command)
        if self.lose_first_ack:
            self.lose_first_ack = False
            raise ConnectionError("simulated response loss after Workflow acceptance")
        return self.handle


class _TemporalHandle(_WorkflowHandle):
    def __init__(self, request_digest: str) -> None:
        super().__init__()
        self.request_digest = request_digest

    async def query(self, _query: object) -> str:
        return self.request_digest


class _TemporalClient:
    def __init__(self, handle: _TemporalHandle) -> None:
        self.handle = handle
        self.calls: list[tuple[object, object, dict[str, object]]] = []

    async def start_workflow(
        self,
        workflow: object,
        command: object,
        **kwargs: object,
    ) -> _TemporalHandle:
        self.calls.append((workflow, command, kwargs))
        return self.handle

    def get_workflow_handle(self, *_args: object, **_kwargs: object) -> _TemporalHandle:
        return self.handle


async def test_temporal_client_uses_dedicated_queue_and_exact_workflow_id() -> None:
    command = MissionModalJobWorkflowInput(
        family="author",
        operation_id=author_provider_operation_id(_WORLD_ID, _ACTIVITY_ID),
        request=MissionJobValueRef(
            ref=_REQUEST_REF,
            digest=_REQUEST_DIGEST,
            size_bytes=len(_REQUEST),
        ),
        namespace_digest=_NAMESPACE_DIGEST,
    )
    handle = _TemporalHandle(_REQUEST_DIGEST)
    raw_client = _TemporalClient(handle)
    client = MissionModalJobTemporalClient(cast(Client, raw_client))

    assert await client.start(command) is handle
    assert len(raw_client.calls) == 1
    _, observed_command, options = raw_client.calls[0]
    assert observed_command == command
    assert options["id"] == mission_modal_job_workflow_id(
        command.family,
        command.operation_id,
        command.namespace_digest,
    )
    assert options["task_queue"] == MISSION_MODAL_JOB_TASK_QUEUE


async def test_temporal_client_rejects_existing_workflow_with_another_request() -> None:
    command = MissionModalJobWorkflowInput(
        family="author",
        operation_id=author_provider_operation_id(_WORLD_ID, _ACTIVITY_ID),
        request=MissionJobValueRef(
            ref=_REQUEST_REF,
            digest=_REQUEST_DIGEST,
            size_bytes=len(_REQUEST),
        ),
        namespace_digest=_NAMESPACE_DIGEST,
    )
    raw_client = _TemporalClient(_TemporalHandle("b" * 64))
    client = MissionModalJobTemporalClient(cast(Client, raw_client))

    with pytest.raises(ConflictError, match="another canonical request"):
        await client.start(command)


async def test_projector_retry_reuses_prebound_workflow_and_records_one_result(
    tmp_path,
) -> None:
    path = tmp_path / "activities.sqlite3"
    physical = SqliteActivityCatalog(path)
    index = ActivityCoordinator(physical)
    launcher = _AckLostOnceLauncher()
    values = MissionModalActivityValueStore(
        author=_AuthorValues(),
        critic=_CriticValues(),
    )
    request = AuthorActivityRequestRef(ref=_REQUEST_REF, digest=_REQUEST_DIGEST)
    operation_id = author_provider_operation_id(_WORLD_ID, _ACTIVITY_ID)
    workflow_id = mission_modal_job_workflow_id(
        "author",
        operation_id,
        _NAMESPACE_DIGEST,
    )

    first = MissionTemporalAuthorActivityCatalog(
        index=index,
        workflows=launcher,
        values=values,
        namespace_digest=_NAMESPACE_DIGEST,
    )
    try:
        with pytest.raises(ConnectionError, match="response loss"):
            await first.admit_author(
                world_id=_WORLD_ID,
                receipt=_receipt(),
                activity_id=_ACTIVITY_ID,
                request=request,
            )

        admitted = await index.get(_WORLD_ID, AUTHOR_ACTIVITY_KIND, _ACTIVITY_ID)
        assert admitted is not None
        assert admitted.execution == ActivityExecutionIdentity("temporal", workflow_id)
        inventory = inspect_sqlite_activity_catalog(path)
        assert inventory.attempt_count == 0
        assert inventory.provider_operation_count == 0

        replacement = MissionTemporalAuthorActivityCatalog(
            index=index,
            workflows=launcher,
            values=values,
            namespace_digest=_NAMESPACE_DIGEST,
        )
        await replacement.admit_author(
            world_id=_WORLD_ID,
            receipt=_receipt(),
            activity_id=_ACTIVITY_ID,
            request=request,
        )
        assert len(launcher.commands) == 2
        assert launcher.commands[0] == launcher.commands[1]

        command = launcher.commands[-1]
        launcher.handle.state = MissionModalJobWorkflowState(
            family="author",
            operation_id=command.operation_id,
            request_digest=command.request.digest,
            status="succeeded",
            result=_RESULT_REF,
        )
        assert await replacement.complete_started(world_id=_WORLD_ID)
        assert not await replacement.complete_started(world_id=_WORLD_ID)

        completed = await index.get(_WORLD_ID, AUTHOR_ACTIVITY_KIND, _ACTIVITY_ID)
        assert completed is not None
        assert completed.execution == ActivityExecutionIdentity("temporal", workflow_id)
        assert completed.result is not None
        assert completed.result.ref == _RESULT_REF.ref
        assert completed.result.digest == _RESULT_REF.digest
        assert completed.result_attempt is None
        assert completed.result_fence is None
        inventory = inspect_sqlite_activity_catalog(path)
        assert inventory.attempt_count == 0
        assert inventory.provider_operation_count == 0
    finally:
        await physical.close()
