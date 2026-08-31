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
    ModalMissionControllerDeploymentReceipt,
    ModalMissionControllerDeploymentSpec,
    ModalMissionControllerExecutionFailed,
    ModalMissionControllerFailpoint,
    ModalMissionControllerFailpointReached,
    ModalMissionControllerRejected,
    build_modal_mission_controller_app,
    build_modal_mission_controller_deployment,
    create_modal_mission_controller_deployment_receipt,
    prepare_modal_mission_job_worker,
    provision_modal_mission_controller_state,
    verify_modal_mission_controller_deployment,
)
from archetype.missions.modal_jobs_runtime import ModalMissionJobRuntimeConfig
from archetype.missions.temporal.contracts import MISSION_MODAL_JOB_TASK_QUEUE
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
    function_id: str = "fu-mission-controller-v17"
    workspace_name: str = "mission-workspace"
    client: object = field(default_factory=object)
    values: dict[str, object] = field(default_factory=dict)
    events: list[str] = field(default_factory=list)
    image_calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = field(
        default_factory=list
    )
    decorator_kwargs: dict[str, object] = field(default_factory=dict)
    app_name: str = ""
    function_lookups: list[tuple[tuple[object, ...], dict[str, object]]] = field(
        default_factory=list
    )


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
            assert kwargs["environment_name"] == "proof"
            assert isinstance(kwargs["create_if_missing"], bool)
            assert kwargs["client"] is state.client
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

    class Function:
        @staticmethod
        def from_name(*args: object, **kwargs: object) -> object:
            state.events.append(f"Function.from_name:{args[0]}:{args[1]}")
            state.function_lookups.append((args, dict(kwargs)))
            function = SimpleNamespace(object_id=state.function_id)

            async def hydrate() -> None:
                state.events.append("function.hydrate")

            function.hydrate = _AioMethod(hydrate)
            return function

    class _Image:
        def uv_pip_install(self, *args: object, **kwargs: object) -> _Image:
            state.image_calls.append(("uv_pip_install", args, kwargs))
            return self

        def add_local_python_source(self, *args: object, **kwargs: object) -> _Image:
            state.image_calls.append(("add_local_python_source", args, kwargs))
            return self

    class Image:
        @staticmethod
        def from_id(image_id: str) -> object:
            state.events.append(f"Image.from_id:{image_id}")
            return SimpleNamespace(object_id=image_id)

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
        Function=Function,
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


def _deployment_spec(**changes: object) -> ModalMissionControllerDeploymentSpec:
    values: dict[str, Any] = {
        "controller_image_id": "im-controller-production",
        "sandbox_image_id": "im-sandbox-checkpoint-production",
        "controller_artifact_digest": "f" * 64,
        "workspace_name": "mission-workspace",
        "environment_name": "production",
        "app_name": "mission-controller-production",
        "job_dict_name": "mission-job-state-production",
        "result_dict_name": "mission-results-production",
        "function_name": "mission-controller",
        "author_model": "gpt-5.4-author",
        "critic_model": "gpt-5.4-critic",
    }
    values.update(changes)
    return ModalMissionControllerDeploymentSpec(**values)


def _deployment_receipt(
    spec: ModalMissionControllerDeploymentSpec,
    **changes: object,
) -> ModalMissionControllerDeploymentReceipt:
    values: dict[str, Any] = {
        "deployment_digest": spec.deployment_digest,
        "controller_artifact_digest": spec.controller_artifact_digest,
        "controller_image_id": spec.controller_image_id,
        "app_name": spec.app_name,
        "function_name": spec.function_name,
        "function_version": 17,
        "function_id": "fu-mission-controller-v17",
    }
    values.update(changes)
    return ModalMissionControllerDeploymentReceipt(**values)


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


def _critic_request(policy: CriticPolicy | None = None) -> bytes:
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
        policy=policy or CriticPolicy(max_subject_bytes=1 << 20),
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
    assert not executor_config.create_result_dict_if_missing
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


def test_controller_accepts_only_deployment_pinned_critic_model_and_timeout() -> None:
    config = replace(
        _config(),
        critic_model="gpt-5.4-critic",
        critic_turn_timeout_seconds=777,
    )
    matching = _critic_request(
        CriticPolicy(
            model="gpt-5.4-critic",
            timeout_seconds=777,
            max_subject_bytes=1 << 20,
        )
    )

    assert (
        modal_jobs_app._canonical_request_digest(
            config=config,
            family="critic",
            request_bytes=matching,
        )
        == hashlib.sha256(matching).hexdigest()
    )
    with pytest.raises(ValueError, match="model conflicts"):
        modal_jobs_app._canonical_request_digest(
            config=config,
            family="critic",
            request_bytes=_critic_request(
                CriticPolicy(
                    model="gpt-5.5-critic",
                    timeout_seconds=777,
                    max_subject_bytes=1 << 20,
                )
            ),
        )
    with pytest.raises(ValueError, match="timeout conflicts"):
        modal_jobs_app._canonical_request_digest(
            config=config,
            family="critic",
            request_bytes=_critic_request(
                CriticPolicy(
                    model="gpt-5.4-critic",
                    timeout_seconds=778,
                    max_subject_bytes=1 << 20,
                )
            ),
        )


@pytest.mark.asyncio
async def test_builtin_host_service_collects_exact_result_without_reexecution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values: dict[str, object] = {}
    payload: bytes | None = None
    spawn_count = 0
    reattach_count = 0
    cancel_count = 0

    class HostRuntime:
        def __init__(
            self,
            config: ModalMissionJobRuntimeConfig,
            *,
            result_reader: Callable[[ModalMissionJobRef], Awaitable[bytes | None]],
            functions: object | None = None,
        ) -> None:
            assert not config.create_if_missing
            assert functions is None
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

    def builtin_job(**_kwargs: object) -> object:
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

    mismatch = await service.collect(started, request_bytes=_critic_request())
    assert isinstance(mismatch, ModalMissionJobUnknown)


@pytest.mark.asyncio
async def test_deployment_provisions_job_and_result_dicts_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState()
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(state))

    names = await provision_modal_mission_controller_state(_config())

    assert names == ("mission-job-state", "mission-results")
    assert "Dict.from_name:mission-job-state" in state.events
    assert "Dict.from_name:mission-results" in state.events
    assert state.events.count("dict.hydrate") == 2


@pytest.mark.asyncio
async def test_deployment_receipt_hydrates_exact_version_and_function_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState()
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(state))
    config = replace(
        _config(),
        runtime=replace(_runtime_config(), function_version=17),
    )
    receipt = ModalMissionControllerDeploymentReceipt(
        deployment_digest="a" * 64,
        controller_artifact_digest="b" * 64,
        controller_image_id="im-controller-proof",
        app_name=config.runtime.app_name,
        function_name=config.function_name,
        function_version=17,
        function_id=state.function_id,
    )

    observed = await verify_modal_mission_controller_deployment(config, receipt)

    assert observed == state.function_id
    assert state.function_lookups == [
        (
            ("mission-controller-proof", "mission-controller"),
            {
                "version": 17,
                "environment_name": "proof",
                "client": state.client,
            },
        )
    ]
    assert state.events == [
        "Workspace.from_context",
        "workspace.hydrate",
        "Function.from_name:mission-controller-proof:mission-controller",
        "function.hydrate",
    ]

    state.function_id = "fu-another-controller"
    with pytest.raises(RuntimeError, match="another Function"):
        await verify_modal_mission_controller_deployment(config, receipt)


@pytest.mark.asyncio
async def test_post_deploy_receipt_can_pin_active_function_id_without_version_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState()
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(state))
    spec = _deployment_spec(
        environment_name="proof",
        app_name="mission-controller-proof",
        job_dict_name="mission-job-state",
        result_dict_name="mission-results",
    )

    receipt = await create_modal_mission_controller_deployment_receipt(
        spec,
        expected_deployment_digest=spec.deployment_digest,
        function_version=None,
    )

    assert receipt == _deployment_receipt(
        spec,
        function_version=None,
        function_id=state.function_id,
    )
    assert state.function_lookups == [
        (
            ("mission-controller-proof", "mission-controller"),
            {"environment_name": "proof", "client": state.client},
        )
    ]


@pytest.mark.asyncio
async def test_post_deploy_receipt_is_created_after_dict_and_function_hydration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeModalState()
    monkeypatch.setitem(sys.modules, "modal", _fake_modal(state))
    spec = _deployment_spec(
        environment_name="proof",
        app_name="mission-controller-proof",
        job_dict_name="mission-job-state",
        result_dict_name="mission-results",
    )

    receipt = await create_modal_mission_controller_deployment_receipt(
        spec,
        expected_deployment_digest=spec.deployment_digest,
        function_version=17,
    )

    assert receipt == _deployment_receipt(spec, function_id=state.function_id)
    assert state.events == [
        "Workspace.from_context",
        "workspace.hydrate",
        "Dict.from_name:mission-job-state",
        "dict.hydrate",
        "Dict.from_name:mission-results",
        "dict.hydrate",
        "Workspace.from_context",
        "workspace.hydrate",
        "Function.from_name:mission-controller-proof:mission-controller",
        "function.hydrate",
    ]


def test_production_deployment_digest_binds_every_effectful_coordinate() -> None:
    spec = _deployment_spec()
    mutations: dict[str, object] = {
        "controller_image_id": "im-controller-rebuilt",
        "sandbox_image_id": "im-sandbox-checkpoint-rebuilt",
        "controller_artifact_digest": "e" * 64,
        "workspace_name": "other-workspace",
        "environment_name": "staging",
        "app_name": "other-controller-app",
        "job_dict_name": "other-job-state",
        "result_dict_name": "other-results",
        "function_name": "other-controller",
        "author_model": "gpt-5.5-author",
        "critic_model": "gpt-5.5-critic",
        "author_workspace": "/workspace/other-author",
        "critic_workspace": "/workspace/other-critic",
        "controller_timeout_seconds": 60,
        "sandbox_timeout_seconds": 61,
        "sandbox_idle_timeout_seconds": 62,
        "author_turn_timeout_seconds": 63,
        "critic_turn_timeout_seconds": 64,
        "auth_volume_name": "other-auth-volume",
        "github_secret_name": "other-github-secret",
        "task_queue": "archetype-missions-modal-jobs-v2",
        "checkpoint_after_dispatch": False,
    }

    assert len(spec.deployment_digest) == 64
    for field_name, value in mutations.items():
        changed = replace(spec, **{field_name: value})
        assert changed.deployment_digest != spec.deployment_digest, field_name


def test_production_config_rejects_server_digest_drift_and_pins_runtime() -> None:
    spec = _deployment_spec()

    with pytest.raises(ValueError, match="server-pinned manifest"):
        spec.app_config(expected_deployment_digest="0" * 64)

    config = spec.app_config(
        expected_deployment_digest=spec.deployment_digest,
        function_version=17,
        function_id="fu-mission-controller-v17",
    )

    assert config.namespace.deployment_digest == spec.deployment_digest
    assert config.namespace.image_id == spec.sandbox_image_id
    assert config.runtime.function_version == 17
    assert config.runtime.function_id == "fu-mission-controller-v17"
    assert config.runtime.author_function_name == spec.function_name
    assert config.runtime.critic_function_name == spec.function_name
    assert not config.runtime.create_if_missing
    assert config.failpoint is None
    assert config.author_model == spec.author_model
    assert config.critic_model == spec.critic_model
    assert config.author_turn_timeout_seconds == spec.author_turn_timeout_seconds
    assert config.critic_turn_timeout_seconds == spec.critic_turn_timeout_seconds


def test_production_spec_requires_content_addressed_artifact_and_models() -> None:
    with pytest.raises(ValueError, match="controller image"):
        _deployment_spec(controller_image_id="mutable-controller")
    with pytest.raises(ValueError, match="must be distinct"):
        _deployment_spec(controller_image_id="im-sandbox-checkpoint-production")
    with pytest.raises(ValueError, match="artifact"):
        _deployment_spec(controller_artifact_digest="mutable-wheel")
    with pytest.raises(ValueError, match="author_model"):
        _deployment_spec(author_model="")
    with pytest.raises(ValueError, match="critic_model"):
        _deployment_spec(critic_model=" provider-default ")
    with pytest.raises(ValueError, match="dedicated task queue"):
        _deployment_spec(task_queue="archetype-missions")
    with pytest.raises(ValueError, match="function_version"):
        _deployment_receipt(_deployment_spec(), function_version=0)


def test_production_app_uses_controller_image_not_sandbox_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    spec = _deployment_spec()
    image = object()
    app = object()
    controller = object()

    def pinned_image(image_id: str) -> object:
        assert image_id == spec.controller_image_id
        events.append("resolve-image")
        return image

    def build_app(
        config: ModalMissionControllerAppConfig,
        *,
        image: object | None = None,
    ) -> tuple[object, object]:
        assert events == ["resolve-image"]
        assert config.namespace.image_id == spec.sandbox_image_id
        assert image is not None
        events.append("build-app")
        return app, controller

    monkeypatch.setattr(modal_jobs_app, "_pinned_controller_image", pinned_image)
    monkeypatch.setattr(modal_jobs_app, "build_modal_mission_controller_app", build_app)

    deployment = build_modal_mission_controller_deployment(
        spec,
        expected_deployment_digest=spec.deployment_digest,
    )

    assert events == ["resolve-image", "build-app"]
    assert deployment.app is app
    assert deployment.controller is controller
    assert deployment.config.runtime.function_version is None


@pytest.mark.asyncio
async def test_prepared_worker_provisions_and_verifies_before_composition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    spec = _deployment_spec()
    receipt = _deployment_receipt(spec)
    worker = object()
    controller = SimpleNamespace(object_id=receipt.function_id)
    values = cast(Any, object())
    temporal_client = cast(Any, object())

    async def provision(config: ModalMissionControllerAppConfig) -> tuple[str, ...]:
        assert config.namespace.deployment_digest == spec.deployment_digest
        events.append("provision-dicts")
        return (spec.job_dict_name, spec.result_dict_name)

    async def verify(
        config: ModalMissionControllerAppConfig,
        received: ModalMissionControllerDeploymentReceipt,
        *,
        function: object | None = None,
    ) -> str:
        assert events == ["provision-dicts", "resolve-function"]
        assert config.runtime.function_version == receipt.function_version
        assert config.runtime.function_id == receipt.function_id
        assert received is receipt
        assert function is controller
        events.append("verify-function")
        return receipt.function_id

    class Service:
        def __init__(
            self,
            config: ModalMissionControllerAppConfig,
            *,
            controller: object | None = None,
        ) -> None:
            assert config.runtime.function_version == receipt.function_version
            assert config.runtime.function_id == receipt.function_id
            assert controller is not None
            events.append("build-service")

    async def resolve(
        config: ModalMissionControllerAppConfig,
        *,
        function_version: int | None,
    ) -> object:
        assert events == ["provision-dicts"]
        assert config.runtime.function_id == receipt.function_id
        assert function_version == receipt.function_version
        events.append("resolve-function")
        return controller

    def build_worker(
        client: object,
        service: object,
        received_values: object,
        *,
        task_queue: str,
    ) -> object:
        assert client is temporal_client
        assert isinstance(service, Service)
        assert received_values is values
        assert task_queue == MISSION_MODAL_JOB_TASK_QUEUE
        events.append("build-worker")
        return worker

    monkeypatch.setattr(
        modal_jobs_app,
        "provision_modal_mission_controller_state",
        provision,
    )
    monkeypatch.setattr(
        modal_jobs_app,
        "verify_modal_mission_controller_deployment",
        verify,
    )
    monkeypatch.setattr(
        modal_jobs_app,
        "_hydrated_modal_mission_controller_function",
        resolve,
    )
    monkeypatch.setattr(modal_jobs_app, "ModalMissionBuiltinJobService", Service)
    monkeypatch.setattr(modal_jobs_app, "create_mission_modal_job_worker", build_worker)

    prepared = await prepare_modal_mission_job_worker(
        temporal_client,
        values,
        spec=spec,
        receipt=receipt,
        expected_deployment_digest=spec.deployment_digest,
    )

    assert events == [
        "provision-dicts",
        "resolve-function",
        "verify-function",
        "build-service",
        "build-worker",
    ]
    assert prepared.worker is worker
    assert prepared.receipt is receipt
    assert prepared.provisioned_dict_names == (
        spec.job_dict_name,
        spec.result_dict_name,
    )


@pytest.mark.asyncio
async def test_production_factory_rejects_digest_before_touching_modal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _deployment_spec()

    async def touched_modal(_config: ModalMissionControllerAppConfig) -> tuple[str, ...]:
        raise AssertionError("digest drift must fail before a Modal lookup")

    monkeypatch.setattr(
        modal_jobs_app,
        "provision_modal_mission_controller_state",
        touched_modal,
    )

    with pytest.raises(ValueError, match="server deployment digest"):
        await prepare_modal_mission_job_worker(
            cast(Any, object()),
            cast(Any, object()),
            spec=spec,
            receipt=_deployment_receipt(spec),
            expected_deployment_digest="0" * 64,
        )


@pytest.mark.asyncio
async def test_prepared_worker_rejects_artifact_receipt_before_provisioning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _deployment_spec()

    async def touched_state(_config: ModalMissionControllerAppConfig) -> tuple[str, ...]:
        raise AssertionError("receipt drift must fail before Modal state is opened")

    monkeypatch.setattr(
        modal_jobs_app,
        "provision_modal_mission_controller_state",
        touched_state,
    )

    with pytest.raises(ValueError, match="deployment manifest"):
        await prepare_modal_mission_job_worker(
            cast(Any, object()),
            cast(Any, object()),
            spec=spec,
            receipt=_deployment_receipt(spec, controller_artifact_digest="e" * 64),
            expected_deployment_digest=spec.deployment_digest,
        )
