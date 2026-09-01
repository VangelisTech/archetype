# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Paid Modal evidence for exact Mission controller call recovery.

The compatibility controller deliberately performs no sandbox or Git work. It
isolates the provider contract that Temporal relies on: a named Function spawns
once, its caller dies by ``SIGKILL``, and a clean process reconstructs the same
``FunctionCall`` by ID before collecting one durable result and recording exact
cleanup intent.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import modal
import pytest

from archetype.missions.modal_jobs import (
    ModalMissionJobClient,
    ModalMissionJobNamespace,
    ModalMissionJobReady,
    ModalMissionJobRef,
    ModalMissionJobResources,
    ModalMissionJobResult,
    modal_mission_job_key,
)
from archetype.missions.modal_jobs_runtime import (
    ModalMissionJobRuntimeConfig,
    ModalNamedMissionJobRuntime,
)

_LIVE = os.environ.get("ARCHETYPE_MODAL_TEMPORAL_MISSION_LIVE") == "1"
_APP_NAME = "archetype-mission-temporal-compat-gate-v1"
_FUNCTION_NAME = "compat-controller"
_JOB_DICT_NAME = "archetype-mission-temporal-compat-jobs-v1"
_RESULT_DICT_NAME = "archetype-mission-temporal-compat-results-v1"
_DEPLOYMENT_DIGEST = hashlib.sha256(b"modal-temporal-compat-gate-v1").hexdigest()
_REQUEST_BYTES = b'{"compatibility":"live-hard-kill","schema_version":1}'
_REQUEST_DIGEST = hashlib.sha256(_REQUEST_BYTES).hexdigest()

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("missions.temporal.recovery"),
    pytest.mark.integration,
    pytest.mark.external,
    pytest.mark.slow,
    pytest.mark.skipif(
        not _LIVE,
        reason="set ARCHETYPE_MODAL_TEMPORAL_MISSION_LIVE=1 for paid Modal evidence",
    ),
]


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise AssertionError(f"live Modal Temporal evidence requires {name}")
    return value


def _result_key(operation_id: str) -> str:
    return f"compat-result:{hashlib.sha256(operation_id.encode()).hexdigest()}"


def _winner_key(operation_id: str) -> str:
    return f"compat-winner:{hashlib.sha256(operation_id.encode()).hexdigest()}"


def _spawn_prefix(operation_id: str) -> str:
    return f"compat-spawn:{hashlib.sha256(operation_id.encode()).hexdigest()}:"


def _namespace() -> ModalMissionJobNamespace:
    return ModalMissionJobNamespace(
        deployment_digest=_DEPLOYMENT_DIGEST,
        image_id="im-compat-gate-no-sandbox",
        result_dict_name=_RESULT_DICT_NAME,
        redaction_policy_id="compat-redaction-v1",
    )


def _runtime_config(
    *,
    workspace_name: str,
    environment_name: str,
    function_id: str,
) -> ModalMissionJobRuntimeConfig:
    return ModalMissionJobRuntimeConfig(
        workspace_name=workspace_name,
        environment_name=environment_name,
        app_name=_APP_NAME,
        job_dict_name=_JOB_DICT_NAME,
        author_function_name=_FUNCTION_NAME,
        critic_function_name=_FUNCTION_NAME,
        function_id=function_id,
        create_if_missing=False,
    )


async def _runtime(
    *,
    workspace_name: str,
    environment_name: str,
    function_id: str,
) -> ModalNamedMissionJobRuntime:
    results = modal.Dict.from_name(
        _RESULT_DICT_NAME,
        environment_name=environment_name,
        create_if_missing=False,
    )
    await results.hydrate.aio()

    async def read_result(ref: ModalMissionJobRef) -> bytes | None:
        return await results.get.aio(_result_key(ref.operation_id), None)

    return ModalNamedMissionJobRuntime(
        _runtime_config(
            workspace_name=workspace_name,
            environment_name=environment_name,
            function_id=function_id,
        ),
        result_reader=read_result,
    )


def _compatibility_app(environment_name: str) -> modal.App:
    app = modal.App(_APP_NAME)
    job_dict_name = _JOB_DICT_NAME
    result_dict_name = _RESULT_DICT_NAME

    @app.function(name=_FUNCTION_NAME, serialized=True, timeout=60)
    async def compatibility_controller(
        family: str,
        operation_id: str,
        request_bytes: bytes,
        namespace_digest: str,
        _environment_name: str = environment_name,
        _job_dict_name: str = job_dict_name,
        _result_dict_name: str = result_dict_name,
    ) -> dict[str, str]:
        import asyncio as remote_asyncio
        import hashlib as remote_hashlib
        import json as remote_json

        import modal as remote_modal

        jobs = remote_modal.Dict.from_name(
            _job_dict_name,
            environment_name=_environment_name,
            create_if_missing=False,
        )
        results = remote_modal.Dict.from_name(
            _result_dict_name,
            environment_name=_environment_name,
            create_if_missing=False,
        )
        call_id = remote_modal.current_function_call_id()
        if not call_id:
            raise RuntimeError("compatibility controller has no call identity")
        operation_digest = remote_hashlib.sha256(operation_id.encode()).hexdigest()
        request_digest = remote_hashlib.sha256(request_bytes).hexdigest()
        spawn_record = {
            "call_id": call_id,
            "family": family,
            "namespace_digest": namespace_digest,
            "operation_id": operation_id,
            "request_digest": request_digest,
        }
        await jobs.put.aio(
            f"compat-spawn:{operation_digest}:{call_id}",
            spawn_record,
            skip_if_exists=True,
        )
        won = await jobs.put.aio(
            f"compat-winner:{operation_digest}",
            spawn_record,
            skip_if_exists=True,
        )
        winner = await jobs.get.aio(f"compat-winner:{operation_digest}", None)
        if not won and winner != spawn_record:
            raise RuntimeError("duplicate compatibility controller lost exact-call fence")

        await remote_asyncio.sleep(8)
        payload = remote_json.dumps(
            {
                "call_id": call_id,
                "family": family,
                "namespace_digest": namespace_digest,
                "operation_id": operation_id,
                "request_digest": request_digest,
                "schema_version": 1,
            },
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        await results.put.aio(
            f"compat-result:{operation_digest}",
            payload,
            skip_if_exists=True,
        )
        if await results.get.aio(f"compat-result:{operation_digest}", None) != payload:
            raise RuntimeError("compatibility result conflicts")
        return {"call_id": call_id}

    return app


async def _starter_process(
    *,
    ref_path: Path,
    operation_id: str,
    workspace_name: str,
    environment_name: str,
    function_id: str,
) -> None:
    runtime = await _runtime(
        workspace_name=workspace_name,
        environment_name=environment_name,
        function_id=function_id,
    )
    outcome = await ModalMissionJobClient(_namespace(), runtime).start(
        family="author",
        operation_id=operation_id,
        request_bytes=_REQUEST_BYTES,
        request_digest=_REQUEST_DIGEST,
    )
    if not isinstance(outcome, ModalMissionJobRef):
        raise RuntimeError(f"starter failed to acquire exact call: {outcome!r}")
    ref_path.write_text(
        json.dumps(
            {
                "call_id": outcome.call_id,
                "namespace_digest": outcome.namespace_digest,
                "operation_id": outcome.operation_id,
                "request_digest": outcome.request_digest,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    while True:
        await asyncio.sleep(60)


async def _wait_for_path(path: Path, process: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 30
    while not path.exists():
        if process.poll() is not None:
            stderr = (process.stderr.read() if process.stderr else b"").decode()
            raise RuntimeError(f"starter exited {process.returncode}: {stderr[-4000:]}")
        if time.monotonic() >= deadline:
            raise TimeoutError("starter did not persist exact call reference")
        await asyncio.sleep(0.1)


async def test_live_modal_call_survives_hard_killed_starter_without_duplicate_effects(
    tmp_path: Path,
) -> None:
    workspace_name = _required_environment("CODING_AGENT_MODAL_WORKSPACE")
    environment_name = _required_environment("CODING_AGENT_MODAL_ENVIRONMENT")
    for name in (_JOB_DICT_NAME, _RESULT_DICT_NAME):
        dictionary = modal.Dict.from_name(
            name,
            environment_name=environment_name,
            create_if_missing=True,
        )
        await dictionary.hydrate.aio()
    with modal.enable_output():
        await _compatibility_app(environment_name).deploy.aio(environment_name=environment_name)
    function = modal.Function.from_name(
        _APP_NAME,
        _FUNCTION_NAME,
        environment_name=environment_name,
    )
    await function.hydrate.aio()
    function_id = function.object_id

    operation_id = f"mission:author:live-compat:{time.time_ns()}"
    ref_path = tmp_path / "exact-call.json"
    process = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--starter",
            str(ref_path),
            operation_id,
            workspace_name,
            environment_name,
            function_id,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        await _wait_for_path(ref_path, process)
        first = json.loads(ref_path.read_text(encoding="utf-8"))
        process.kill()
        await asyncio.to_thread(process.wait, 10)
        assert process.returncode is not None and process.returncode < 0

        replacement_runtime = await _runtime(
            workspace_name=workspace_name,
            environment_name=environment_name,
            function_id=function_id,
        )
        client = ModalMissionJobClient(_namespace(), replacement_runtime)
        replay = await client.start(
            family="author",
            operation_id=operation_id,
            request_bytes=_REQUEST_BYTES,
            request_digest=_REQUEST_DIGEST,
        )
        assert isinstance(replay, ModalMissionJobRef)
        assert replay.call_id == first["call_id"]

        attached = await replacement_runtime.reattach(replay.call_id)
        assert replacement_runtime.call_id(attached) == replay.call_id
        deadline = time.monotonic() + 60
        while not isinstance(await client.poll(replay), ModalMissionJobReady):
            assert time.monotonic() < deadline
            await asyncio.sleep(0.25)
        result = await client.collect(replay)
        assert isinstance(result, ModalMissionJobResult)

        cleaned: list[ModalMissionJobResources] = []

        async def cleaner(resources: ModalMissionJobResources) -> None:
            cleaned.append(resources)

        await client.cleanup(replay, cleaner=cleaner)
        await client.cleanup(replay, cleaner=cleaner)

        jobs = modal.Dict.from_name(
            _JOB_DICT_NAME,
            environment_name=environment_name,
            create_if_missing=False,
        )
        results = modal.Dict.from_name(
            _RESULT_DICT_NAME,
            environment_name=environment_name,
            create_if_missing=False,
        )
        await jobs.hydrate.aio()
        await results.hydrate.aio()
        spawn_keys = [
            key
            async for key in jobs.keys.aio()
            if isinstance(key, str) and key.startswith(_spawn_prefix(operation_id))
        ]
        start_record = await jobs.get.aio(
            modal_mission_job_key("author", operation_id, "start"), None
        )
        call_record = await jobs.get.aio(
            modal_mission_job_key("author", operation_id, "call"), None
        )
        cleanup_record = await jobs.get.aio(
            modal_mission_job_key("author", operation_id, "cleanup"), None
        )
        result_payload = await results.get.aio(_result_key(operation_id), None)

        assert len(spawn_keys) == 1
        assert isinstance(start_record, dict)
        assert call_record["call_id"] == replay.call_id
        assert cleanup_record["call_id"] == replay.call_id
        assert cleanup_record["phase"] == "cleanup"
        assert json.loads(result_payload)["call_id"] == replay.call_id
        assert cleaned == []
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)


def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--starter", action="store_true")
    parser.add_argument("args", nargs="*")
    parsed = parser.parse_args()
    if not parsed.starter:
        raise SystemExit("this module's command interface is only for the hard-kill starter")
    ref_path, operation_id, workspace_name, environment_name, function_id = parsed.args
    asyncio.run(
        _starter_process(
            ref_path=Path(ref_path),
            operation_id=operation_id,
            workspace_name=workspace_name,
            environment_name=environment_name,
            function_id=function_id,
        )
    )


if __name__ == "__main__":  # pragma: no cover - exercised by paid subprocess gate
    _main()
