# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Offline contracts for the deployed Modal Mission controller app."""

from __future__ import annotations

import hashlib
import inspect
import json
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from types import SimpleNamespace
from typing import Any, cast

import pytest

import archetype.missions.modal_jobs_app as modal_jobs_app
from archetype.missions.activity_values import MissionAuthorValueCodec
from archetype.missions.coding_agents.contracts import (
    DispatchedValidator,
    TaskDispatchRequest,
)
from archetype.missions.contracts import (
    CommandValidator,
    CriticPolicy,
    RepositoryPublicationPolicy,
)
from archetype.missions.critics import CandidateReviewRequest, CriticActivityCodec
from archetype.missions.modal_jobs import (
    ModalMissionFamily,
    ModalMissionJobNamespace,
    ModalMissionJobReady,
    ModalMissionJobRef,
    ModalMissionJobResult,
    ModalMissionJobStillRunning,
    ModalMissionJobUnknown,
    modal_mission_call_record,
    modal_mission_job_key,
)
from archetype.missions.modal_jobs_app import (
    MODAL_MISSION_CONTROLLER_MAX_RECEIPT_BYTES,
    ModalMissionBuiltinJobService,
    ModalMissionControllerAppConfig,
    ModalMissionControllerExecutionFailed,
    ModalMissionControllerFailpoint,
    ModalMissionControllerFailpointReached,
    ModalMissionControllerRejected,
    build_modal_mission_controller_app,
)
from archetype.missions.modal_jobs_runtime import ModalMissionJobRuntimeConfig
from archetype.redaction import RedactionService

_Controller = Callable[
    [ModalMissionFamily, str, bytes, str],
    Awaitable[dict[str, str | int]],
]


class _AioMethod:
    def __init__(self, callback: Callable[..., Awaitable[object]]) -> None:
        self.aio = callback


@dataclass
class _FakeModalState:
    current_call_id: str | None = "fc-controller-1"
    workspace_name: str = "mission-workspace"
    client: object = field(default_factory=object)
    values: dict[str, object] = field(default_factory=dict)
    events: list[str] = field(default_factory=list)
    image_calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = field(
        default_factory=list
    )
    decorator_kwargs: dict[str, object] = field(default_factory=dict)
    app_name: str = ""


def _fake_modal(state: _FakeModalState) -> object:
    class Workspace:
        @staticmethod
        def from_context() -> object:
            state.events.append("Workspace.from_context")
            workspace = SimpleNamespace(name=state.workspace_name, client=state.client)

            async def hydrate() -> None:
                state.events.append("workspace.hydrate")

            workspace.hydrate = _AioMethod(hydrate)
            return workspace

    class Dict:
        @staticmethod
        def from_name(name: str, **kwargs: object) -> object:
            state.events.append(f"Dict.from_name:{name}")
            assert kwargs == {
                "environment_name": "proof",
                "create_if_missing": False,
                "client": state.client,
            }
            dictionary = SimpleNamespace()

            async def hydrate() -> None:
                state.events.append("dict.hydrate")

            async def get(key: str, default: object = None) -> object:
                state.events.append(f"dict.get:{key}")
                return state.values.get(key, default)

            async def put(key: str, value: object, **put_kwargs: object) -> bool:
                state.events.append(f"dict.put:{key}")
                assert put_kwargs == {"skip_if_exists": True}
                if key in state.values:
                    return False
                state.values[key] = value
                return True

            dictionary.hydrate = _AioMethod(hydrate)
            dictionary.get = _AioMethod(get)
            dictionary.put = _AioMethod(put)
            return dictionary

    class App:
        def __init__(self, name: str) -> None:
            state.app_name = name

        def function(self, **kwargs: object) -> Callable[[object], object]:
            state.decorator_kwargs = dict(kwargs)

            def decorate(function: object) -> object:
                return function

            return decorate

    class _Image:
        def uv_pip_install(self, *args: object, **kwargs: object) -> _Image:
            state.image_calls.append(("uv_pip_install", args, kwargs))
            return self

        def add_local_python_source(self, *args: object, **kwargs: object) -> _Image:
            state.image_calls.append(("add_local_python_source", args, kwargs))
            return self

    class Image:
        @staticmethod
        def debian_slim(*args: object, **kwargs: object) -> _Image:
            state.image_calls.append(("debian_slim", args, kwargs))
            return _Image()

    def current_function_call_id() -> str | None:
        state.events.append("current_function_call_id")
        return state.current_call_id

    return SimpleNamespace(
        App=App,
        Dict=Dict,
        Image=Image,
        Workspace=Workspace,
        current_function_call_id=current_function_call_id,
    )


def _namespace(**changes: object) -> ModalMissionJobNamespace:
    values: dict[str, Any] = {
        "deployment_digest": "a" * 64,
        "image_id": "im-controller-proof",
        "result_dict_name": "mission-results",
        "redaction_policy_id": RedactionService().policy_id,
    }
    values.update(changes)
    return ModalMissionJobNamespace(**values)


def _runtime_config(**changes: object) -> ModalMissionJobRuntimeConfig:
    values: dict[str, Any] = {
        "workspace_name": "mission-workspace",
        "environment_name": "proof",
        "app_name": "mission-controller-proof",
        "job_dict_name": "mission-job-state",
        "author_function_name": "mission-controller",
        "critic_function_name": "mission-controller",
        "create_if_missing": False,
    }
    values.update(changes)
    return ModalMissionJobRuntimeConfig(**values)


def _config(
    *,
    failpoint: ModalMissionControllerFailpoint | None = None,
) -> ModalMissionControllerAppConfig:
    return ModalMissionControllerAppConfig(
        namespace=_namespace(),
        runtime=_runtime_config(),
        timeout_seconds=90,
        failpoint=failpoint,
    )


def _author_request() -> bytes:
    request = TaskDispatchRequest(
        mission_id=3,
        task_id=7,
        task_name="prove-controller",
        dispatch_id="dispatch-controller-proof",
        dispatch_sequence=1,
        repository="owner/repo",
        branch="proof/controller",
        base_ref="main",
        prompt="write proof.txt",
        validators=(
            DispatchedValidator(
                validator_id=11,
                spec=CommandValidator(
                    name="proof-exists",
                    command=("sh", "-c", "test -f proof.txt"),
                ),
            ),
        ),
        publication_policy=RepositoryPublicationPolicy.COMMIT_AND_PUSH,
    )
    return MissionAuthorValueCodec(redactor=RedactionService()).encode_request(request)


def _critic_request() -> bytes:
    content = b"exact diff"
    request = CandidateReviewRequest(
        candidate_entity_id=11,
        candidate_id=hashlib.sha256(b"candidate").hexdigest(),
        mission_id=3,
        task_id=7,
        task_name="Review candidate",
        task_prompt="Prove the exact candidate is correct.",
        dispatch_id=hashlib.sha256(b"dispatch").hexdigest(),
        dispatch_sequence=1,
        author_execution_id=9,
        author_sandbox_id="sb-author-original",
        repository="https://github.com/example/repository.git",
        branch="agent/review",
        base_ref="main",
        base_revision="a" * 40,
        head_revision="b" * 40,
        diff_digest=hashlib.sha256(content).hexdigest(),
        validator_bundle_digest=hashlib.sha256(b"validators").hexdigest(),
        policy=CriticPolicy(max_subject_bytes=1 << 20),
        validation=(),
        candidate_published_at_ms=100,
        attempt=1,
    )
    codec = CriticActivityCodec(RedactionService())
    return codec.encode_request(codec.prepare_request(request)).payload


def _operation(family: ModalMissionFamily) -> str:
    return f"missions.{family}:" + ("b" if family == "author" else "c") * 64


def _admit_start(
    state: _FakeModalState,
    config: ModalMissionControllerAppConfig,
    *,
    family: ModalMissionFamily,
    operation_id: str,
    request_bytes: bytes,
) -> None:
    request_digest = hashlib.sha256(request_bytes).hexdigest()
    state.values[modal_mission_job_key(family, operation_id, "start")] = (
        config.namespace.start_record(
            family=family,
            operation_id=operation_id,
            request_digest=request_digest,
        )
    )


def _build(
    monkeypatch: pytest.MonkeyPatch,
    state: _FakeModalState,
    config: ModalMissionControllerAppConfig,
    *,
    stub_builtin: bool = True,
) -> _Controller:
    async def no_effect(**_kwargs: object) -> None:
        return None

    monkeypatch.setitem(sys.modules, "modal", _fake_modal(state))
    if stub_builtin:
        monkeypatch.setattr(modal_jobs_app, "_execute_builtin_job", no_effect)
    _app, controller = build_modal_mission_controller_app(config, image=object())
    return cast(_Controller, controller)


@pytest.mark.asyncio
async def test_builtin_controller_binds_resource_recorder_and_pinned_image_before_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState()
    config = _config()
    request_bytes = _author_request()
    operation_id = _operation("author")
    _admit_start(
        state,
        config,
        family="author",
        operation_id=operation_id,
        request_bytes=request_bytes,
    )
    captured: dict[str, object] = {}

    class FakeAuthorExecutor:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)
            self.capability = kwargs["capability"]

        def sandbox_spec(self, _request: object) -> object:
            return object()

        async def execute(self, *, operation_id: str, **_kwargs: object) -> None:
            capability = cast(Any, self.capability)
            observer = capability._resource_observer
            assert observer is not None
            identity = capability.identity(operation_id)
            cohort_id = "cohort-v1:" + "d" * 32
            await observer(identity, cohort_id, "intent", "")
            await observer(identity, cohort_id, "auth", "sb-controller-auth")
            await observer(identity, cohort_id, "mission", "sb-controller-mission")

    cleanup_calls: list[object] = []

    async def cleanup_resources(_self: object, *, cleanup: object, spec: object) -> None:
        cleanup_calls.append((cleanup, spec))

    monkeypatch.setattr(modal_jobs_app, "ModalMissionAuthorExecutor", FakeAuthorExecutor)
    monkeypatch.setattr(
        modal_jobs_app.ModalSandboxOperationCapability,
        "cleanup_resources",
        cleanup_resources,
    )
    controller = _build(monkeypatch, state, config, stub_builtin=False)

    await controller("author", operation_id, request_bytes, config.namespace.digest)

    executor_config = cast(Any, captured["config"])
    assert executor_config.sandbox_environment == (f"modal-image://{config.namespace.image_id}")
    assert modal_mission_job_key("author", operation_id, "resource-intent") in state.values
    assert modal_mission_job_key("author", operation_id, "resource-auth") in state.values
    assert modal_mission_job_key("author", operation_id, "resource-mission") in state.values
    assert modal_mission_job_key("author", operation_id, "cleanup") in state.values
    assert len(cleanup_calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("family", "request_factory"),
    [("author", _author_request), ("critic", _critic_request)],
)
async def test_controller_self_registers_before_returning_a_bounded_identity_receipt(
    monkeypatch: pytest.MonkeyPatch,
    family: ModalMissionFamily,
    request_factory: Callable[[], bytes],
) -> None:
    state = _FakeModalState()
    config = _config()
    request_bytes = request_factory()
    operation_id = _operation(family)
    _admit_start(
        state,
        config,
        family=family,
        operation_id=operation_id,
        request_bytes=request_bytes,
    )
    controller = _build(monkeypatch, state, config)

    receipt = await controller(
        family,
        operation_id,
        request_bytes,
        config.namespace.digest,
    )

    assert state.current_call_id is not None
    ref = ModalMissionJobRef(
        family=family,
        operation_id=operation_id,
        request_digest=hashlib.sha256(request_bytes).hexdigest(),
        namespace_digest=config.namespace.digest,
        call_id=state.current_call_id,
    )
    assert receipt == modal_mission_call_record(ref)
    assert state.values[modal_mission_job_key(family, operation_id, "call")] == receipt
    assert (
        len(json.dumps(receipt, separators=(",", ":"), sort_keys=True).encode())
        <= MODAL_MISSION_CONTROLLER_MAX_RECEIPT_BYTES
    )
    assert state.events.index("current_function_call_id") < next(
        index for index, event in enumerate(state.events) if event.startswith("dict.get:")
    )
    assert state.app_name == config.runtime.app_name
    assert state.decorator_kwargs == {
        "name": config.function_name,
        "image": state.decorator_kwargs["image"],
        "retries": 0,
        "serialized": True,
        "timeout": 90,
    }


def test_deployed_controller_has_exactly_four_remote_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState()
    controller = _build(monkeypatch, state, _config())

    assert tuple(inspect.signature(controller).parameters) == (
        "family",
        "operation_id",
        "request_bytes",
        "namespace_digest",
    )


def test_default_controller_image_is_release_pinned_and_copies_the_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState()
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(state))

    build_modal_mission_controller_app(_config())

    assert state.image_calls == [
        ("debian_slim", (), {"python_version": "3.12"}),
        ("uv_pip_install", ("archetype-missions[modal]==0.6.3",), {}),
        ("add_local_python_source", ("archetype",), {"copy": True}),
    ]


@pytest.mark.asyncio
async def test_duplicate_remote_call_is_rejected_before_the_post_registration_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState(current_call_id="fc-winner")
    config = _config(
        failpoint=ModalMissionControllerFailpoint.AFTER_SELF_REGISTRATION,
    )
    request_bytes = _author_request()
    operation_id = _operation("author")
    _admit_start(
        state,
        config,
        family="author",
        operation_id=operation_id,
        request_bytes=request_bytes,
    )
    controller = _build(monkeypatch, state, config)

    with pytest.raises(
        ModalMissionControllerFailpointReached,
        match="after-self-registration",
    ):
        await controller("author", operation_id, request_bytes, config.namespace.digest)
    winner = state.values[modal_mission_job_key("author", operation_id, "call")]

    state.current_call_id = "fc-duplicate"
    with pytest.raises(ModalMissionControllerRejected, match="already owns"):
        await controller("author", operation_id, request_bytes, config.namespace.digest)

    assert state.values[modal_mission_job_key("author", operation_id, "call")] == winner


@pytest.mark.asyncio
async def test_before_registration_failpoint_leaves_no_call_record_or_named_modal_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState()
    config = _config(
        failpoint=ModalMissionControllerFailpoint.BEFORE_SELF_REGISTRATION,
    )
    request_bytes = _author_request()
    operation_id = _operation("author")
    _admit_start(
        state,
        config,
        family="author",
        operation_id=operation_id,
        request_bytes=request_bytes,
    )
    controller = _build(monkeypatch, state, config)

    with pytest.raises(
        ModalMissionControllerFailpointReached,
        match="before-self-registration",
    ):
        await controller("author", operation_id, request_bytes, config.namespace.digest)

    assert modal_mission_job_key("author", operation_id, "call") not in state.values
    assert state.events == ["current_function_call_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["namespace", "request", "operation"])
async def test_identity_mismatch_never_opens_named_modal_state(
    monkeypatch: pytest.MonkeyPatch,
    mismatch: str,
) -> None:
    state = _FakeModalState()
    config = _config()
    request_bytes = _author_request()
    operation_id = _operation("author")
    namespace_digest = config.namespace.digest
    if mismatch == "namespace":
        namespace_digest = "0" * 64
    elif mismatch == "request":
        request_bytes += b"\n"
    else:
        operation_id = "invalid operation identity"
    controller = _build(monkeypatch, state, config)

    with pytest.raises(ModalMissionControllerExecutionFailed):
        await controller("author", operation_id, request_bytes, namespace_digest)

    assert state.events == ["current_function_call_id"]
    assert not state.values


@pytest.mark.asyncio
async def test_missing_current_function_call_identity_fails_before_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState(current_call_id=None)
    config = _config()
    controller = _build(monkeypatch, state, config)

    with pytest.raises(ModalMissionControllerExecutionFailed) as raised:
        await controller(
            "author",
            _operation("author"),
            _author_request(),
            config.namespace.digest,
        )

    assert raised.value.error_type == "RuntimeError"
    assert state.events == ["current_function_call_id"]
    assert not state.values


@pytest.mark.asyncio
async def test_controller_canonicalizes_remote_timeout_as_a_non_timeout_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState()
    config = _config()
    request_bytes = _author_request()
    operation_id = _operation("author")
    _admit_start(
        state,
        config,
        family="author",
        operation_id=operation_id,
        request_bytes=request_bytes,
    )
    controller = _build(monkeypatch, state, config)

    async def timeout(**_kwargs: object) -> None:
        raise TimeoutError("must not escape the deployed controller")

    monkeypatch.setattr(modal_jobs_app, "_execute_builtin_job", timeout)

    with pytest.raises(ModalMissionControllerExecutionFailed) as raised:
        await controller("author", operation_id, request_bytes, config.namespace.digest)

    assert not isinstance(raised.value, TimeoutError)
    assert raised.value.error_type == "TimeoutError"


def test_controller_config_is_fixed_to_one_function_and_builtin_redaction() -> None:
    with pytest.raises(ValueError, match="shared author/critic"):
        ModalMissionControllerAppConfig(
            namespace=_namespace(),
            runtime=_runtime_config(critic_function_name="other-controller"),
        )
    with pytest.raises(ValueError, match="redaction capability conflicts"):
        ModalMissionControllerAppConfig(
            namespace=_namespace(redaction_policy_id="custom-redaction-v1"),
            runtime=_runtime_config(),
        )
    with pytest.raises(ValueError, match="preprovisioned"):
        ModalMissionControllerAppConfig(
            namespace=_namespace(),
            runtime=_runtime_config(create_if_missing=True),
        )
    with pytest.raises(ValueError, match="pinned im-"):
        ModalMissionControllerAppConfig(
            namespace=_namespace(image_id="mutable-image-tag"),
            runtime=_runtime_config(),
        )
    with pytest.raises(ValueError, match="positive integer"):
        replace(_config(), timeout_seconds=0)
    with pytest.raises(ValueError, match="failpoint is invalid"):
        replace(_config(), failpoint="after-self-registration")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_builtin_host_service_collects_exact_result_without_reexecution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values: dict[str, object] = {}
    payload: bytes | None = None
    spawn_count = 0
    reattach_count = 0
    cancel_count = 0
    read_only_modes: list[bool] = []

    class HostRuntime:
        def __init__(
            self,
            config: ModalMissionJobRuntimeConfig,
            *,
            result_reader: Callable[[ModalMissionJobRef], Awaitable[bytes | None]],
        ) -> None:
            assert not config.create_if_missing
            self._result_reader = result_reader

        async def get(self, key: str) -> object:
            return values.get(key)

        async def put_if_absent(self, key: str, value: object) -> bool:
            if key in values:
                return False
            values[key] = value
            return True

        async def spawn(self, **_kwargs: object) -> object:
            nonlocal spawn_count
            spawn_count += 1
            return "fc-host-service"

        @staticmethod
        def call_id(call: object) -> str:
            return str(call)

        async def reattach(self, call_id: str) -> object:
            nonlocal reattach_count
            reattach_count += 1
            return call_id

        async def cancel(self, _call: object) -> None:
            nonlocal cancel_count
            cancel_count += 1

        async def call_result(self, _call: object, *, timeout_seconds: float) -> object:
            assert timeout_seconds == 0
            raise ModalMissionJobStillRunning

        async def result_payload(self, ref: ModalMissionJobRef) -> bytes | None:
            return await self._result_reader(ref)

        async def result_ready(self, ref: ModalMissionJobRef) -> bool:
            return await self.result_payload(ref) is not None

    def builtin_job(**kwargs: object) -> object:
        read_only_modes.append(bool(kwargs.get("read_only")))

        async def no_execute(_operation_id: str) -> None:
            raise AssertionError("host observation must not execute provider work")

        async def read_result(_operation_id: str) -> bytes | None:
            return payload

        return modal_jobs_app._BuiltinMissionJob(
            capability=cast(Any, object()),
            spec=cast(Any, object()),
            execute=no_execute,
            read_result=read_result,
        )

    monkeypatch.setattr(modal_jobs_app, "ModalNamedMissionJobRuntime", HostRuntime)
    monkeypatch.setattr(modal_jobs_app, "_builtin_mission_job", builtin_job)
    service = ModalMissionBuiltinJobService(_config())
    request_bytes = _author_request()
    started = await service.start(
        family="author",
        operation_id=_operation("author"),
        request_bytes=request_bytes,
    )
    assert isinstance(started, ModalMissionJobRef)

    assert not isinstance(
        await service.poll(started, request_bytes=request_bytes),
        ModalMissionJobReady,
    )
    payload = b'{"kind":"author-result","schema_version":1}'
    assert isinstance(
        await service.poll(started, request_bytes=request_bytes),
        ModalMissionJobReady,
    )
    first = await service.collect(started, request_bytes=request_bytes)
    replay = await service.collect(started, request_bytes=request_bytes)

    assert isinstance(first, ModalMissionJobResult)
    assert replay == first
    assert first.payload == payload
    assert spawn_count == 1
    assert reattach_count == 1
    assert cancel_count == 0
    assert read_only_modes == [True, True, True, True]

    mismatch = await service.collect(started, request_bytes=_critic_request())
    assert isinstance(mismatch, ModalMissionJobUnknown)
