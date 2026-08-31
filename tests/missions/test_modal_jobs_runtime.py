# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Offline contracts for the concrete named-Modal Mission job runtime."""

from __future__ import annotations

import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from archetype.missions.modal_jobs import (
    ModalMissionJobRef,
    ModalMissionJobRuntime,
    ModalMissionJobStillRunning,
)
from archetype.missions.modal_jobs_runtime import (
    ModalMissionJobRuntimeConfig,
    ModalNamedMissionJobRuntime,
)


class _AioMethod:
    def __init__(self, callback: Callable[..., Awaitable[object]]) -> None:
        self.aio = callback


@dataclass
class _FakeModalState:
    workspace_name: str = "mission-workspace"
    client: object = field(default_factory=object)
    values: dict[str, object] = field(default_factory=dict)
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = field(default_factory=list)
    result: object = field(default_factory=lambda: {"status": "complete"})
    result_error: BaseException | None = None
    next_call: int = 0


class _FakeCall:
    def __init__(self, state: _FakeModalState, object_id: str) -> None:
        self.object_id = object_id
        self.get = _AioMethod(self._get)
        self._state = state

    async def _get(self, **kwargs: object) -> object:
        self._state.calls.append(("call.get", (), kwargs))
        if self._state.result_error is not None:
            raise self._state.result_error
        return self._state.result


def _fake_modal(state: _FakeModalState) -> object:
    class Workspace:
        @staticmethod
        def from_context() -> object:
            state.calls.append(("Workspace.from_context", (), {}))
            workspace = SimpleNamespace(name=state.workspace_name, client=state.client)

            async def hydrate() -> None:
                state.calls.append(("workspace.hydrate", (), {}))

            workspace.hydrate = _AioMethod(hydrate)
            return workspace

    class Dict:
        @staticmethod
        def from_name(*args: object, **kwargs: object) -> object:
            state.calls.append(("Dict.from_name", args, kwargs))
            dictionary = SimpleNamespace()

            async def hydrate() -> None:
                state.calls.append(("dict.hydrate", (), {}))

            async def get(key: str, default: object = None) -> object:
                state.calls.append(("dict.get", (key, default), {}))
                return state.values.get(key, default)

            async def put(key: str, value: object, **put_kwargs: object) -> bool:
                state.calls.append(("dict.put", (key, value), put_kwargs))
                if key in state.values and put_kwargs.get("skip_if_exists") is True:
                    return False
                state.values[key] = value
                return True

            dictionary.hydrate = _AioMethod(hydrate)
            dictionary.get = _AioMethod(get)
            dictionary.put = _AioMethod(put)
            return dictionary

    class Function:
        @staticmethod
        def from_name(*args: object, **kwargs: object) -> object:
            state.calls.append(("Function.from_name", args, kwargs))
            function = SimpleNamespace()

            async def hydrate() -> None:
                state.calls.append(("function.hydrate", args, {}))

            async def spawn(*spawn_args: object) -> object:
                state.calls.append(("function.spawn", spawn_args, {}))
                state.next_call += 1
                return _FakeCall(state, f"fc-{state.next_call}")

            function.hydrate = _AioMethod(hydrate)
            function.spawn = _AioMethod(spawn)
            return function

    class FunctionCall:
        @classmethod
        def from_id(cls, call_id: str, **kwargs: object) -> object:
            state.calls.append(("FunctionCall.from_id", (call_id,), kwargs))
            return _FakeCall(state, call_id)

    return SimpleNamespace(
        Dict=Dict,
        Function=Function,
        FunctionCall=FunctionCall,
        Workspace=Workspace,
    )


def _config() -> ModalMissionJobRuntimeConfig:
    return ModalMissionJobRuntimeConfig(
        workspace_name="mission-workspace",
        environment_name="production",
        app_name="mission-jobs",
        job_dict_name="mission-job-state",
        author_function_name="mission-author-controller",
        critic_function_name="mission-critic-controller",
        create_if_missing=True,
    )


def _ref() -> ModalMissionJobRef:
    return ModalMissionJobRef(
        family="author",
        operation_id="mission:author:dispatch-1",
        request_digest="a" * 64,
        namespace_digest="b" * 64,
        call_id="fc-1",
    )


@pytest.mark.asyncio
async def test_named_runtime_uses_one_dict_and_deployed_family_functions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState()
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(state))
    seen_refs: list[ModalMissionJobRef] = []

    async def result_ready(ref: ModalMissionJobRef) -> bool:
        seen_refs.append(ref)
        return True

    runtime = ModalNamedMissionJobRuntime(_config(), result_ready=result_ready)

    assert isinstance(runtime, ModalMissionJobRuntime)
    assert await runtime.put_if_absent("start", {"schema_version": 1})
    assert not await runtime.put_if_absent("start", {"schema_version": 2})
    assert await runtime.get("start") == {"schema_version": 1}
    author = await runtime.spawn(
        family="author",
        operation_id="mission:author:dispatch-1",
        request_bytes=b"author-request",
        namespace_digest="a" * 64,
    )
    critic = await runtime.spawn(
        family="critic",
        operation_id="mission:critic:review-1",
        request_bytes=b"critic-request",
        namespace_digest="b" * 64,
    )

    assert runtime.call_id(author) == "fc-1"
    assert runtime.call_id(critic) == "fc-2"
    assert await runtime.result_ready(_ref())
    assert seen_refs == [_ref()]
    assert state.calls.count(("Workspace.from_context", (), {})) == 1
    assert (
        "Dict.from_name",
        ("mission-job-state",),
        {
            "environment_name": "production",
            "create_if_missing": True,
            "client": state.client,
        },
    ) in state.calls
    assert (
        "Function.from_name",
        ("mission-jobs", "mission-author-controller"),
        {"environment_name": "production", "client": state.client},
    ) in state.calls
    assert (
        "Function.from_name",
        ("mission-jobs", "mission-critic-controller"),
        {"environment_name": "production", "client": state.client},
    ) in state.calls
    assert (
        "function.spawn",
        ("author", "mission:author:dispatch-1", b"author-request", "a" * 64),
        {},
    ) in state.calls
    assert (
        "function.spawn",
        ("critic", "mission:critic:review-1", b"critic-request", "b" * 64),
        {},
    ) in state.calls


@pytest.mark.asyncio
async def test_runtime_reattaches_by_function_call_id_and_polls_without_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState(result={"finished": True})
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(state))

    async def result_ready(_ref: ModalMissionJobRef) -> bool:
        return False

    runtime = ModalNamedMissionJobRuntime(_config(), result_ready=result_ready)
    call = await runtime.reattach("fc-existing")

    assert runtime.call_id(call) == "fc-existing"
    assert await runtime.call_result(call, timeout_seconds=0.0) == {"finished": True}
    assert (
        "FunctionCall.from_id",
        ("fc-existing",),
        {"client": state.client},
    ) in state.calls
    assert ("call.get", (), {"timeout": 0}) in state.calls


@pytest.mark.asyncio
async def test_runtime_translates_only_nonblocking_timeout_to_still_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState(result_error=TimeoutError())
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(state))

    async def result_ready(_ref: ModalMissionJobRef) -> bool:
        return False

    runtime = ModalNamedMissionJobRuntime(_config(), result_ready=result_ready)
    call = await runtime.reattach("fc-running")

    with pytest.raises(ModalMissionJobStillRunning):
        await runtime.call_result(call, timeout_seconds=0.0)
    with pytest.raises(ValueError, match="zero-second"):
        await runtime.call_result(call, timeout_seconds=0.01)

    state.result_error = RuntimeError("remote failed")
    with pytest.raises(RuntimeError, match="remote failed"):
        await runtime.call_result(call, timeout_seconds=0.0)


@pytest.mark.asyncio
async def test_runtime_fails_before_named_lookups_in_the_wrong_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState(workspace_name="other-workspace")
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(state))

    async def result_ready(_ref: ModalMissionJobRef) -> bool:
        return False

    runtime = ModalNamedMissionJobRuntime(_config(), result_ready=result_ready)

    with pytest.raises(RuntimeError, match="workspace"):
        await runtime.get("start")
    assert not any(call[0] == "Dict.from_name" for call in state.calls)


def test_runtime_config_rejects_invalid_modal_names_and_non_boolean_creation() -> None:
    with pytest.raises(ValueError, match="job_dict_name"):
        ModalMissionJobRuntimeConfig(
            workspace_name="mission-workspace",
            environment_name="production",
            app_name="mission-jobs",
            job_dict_name="invalid/name",
            author_function_name="mission-author-controller",
            critic_function_name="mission-critic-controller",
        )
    with pytest.raises(ValueError, match="boolean"):
        ModalMissionJobRuntimeConfig(
            workspace_name="mission-workspace",
            environment_name="production",
            app_name="mission-jobs",
            job_dict_name="mission-job-state",
            author_function_name="mission-author-controller",
            critic_function_name="mission-critic-controller",
            create_if_missing=1,  # type: ignore[arg-type]
        )
