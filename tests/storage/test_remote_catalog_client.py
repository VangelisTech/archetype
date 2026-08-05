# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Transport behavior for the remote control-catalog client."""

import asyncio
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from archetype.storage.catalog import (
    CommandAdmission,
    RemoteControlCatalog,
    WorldRecord,
)
from archetype.storage.catalog import remote as _remote_catalog
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService

pytestmark = pytest.mark.asyncio


async def _catalog_with(
    responses: list[httpx.Response], requests: list[httpx.Request] | None = None
) -> RemoteControlCatalog:
    catalog = RemoteControlCatalog("https://catalog.invalid", "test")
    await catalog._client.aclose()

    def handler(_request: httpx.Request) -> httpx.Response:
        if requests is not None:
            requests.append(_request)
        return responses.pop(0)

    catalog._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return catalog


def _cleanup_only_world_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "world_id": "private",
        "name": "private",
        "run_id": "run",
        "parent_world_id": None,
        "status": "active",
        "tick_head": 0,
        "writer_mode": "cleanup_only",
    }
    row.update(overrides)
    return row


def _retirement_receipt(**overrides: object) -> dict[str, object]:
    return {
        **_cleanup_only_world_row(status="destroyed"),
        "ok": True,
        "disposition": "retired",
        "catalog_protocol_version": 8,
        "gateway_protocol_version": 8,
        "catalog_status": "destroyed",
        "world_status": "destroyed",
        **overrides,
    }


def _exception_leaves(error: BaseException) -> list[BaseException]:
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in _exception_leaves(child)]
    return [error]


async def test_remote_catalog_configuration_requires_token(monkeypatch):
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", "https://catalog.invalid")
    monkeypatch.delenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", raising=False)

    with pytest.raises(RuntimeError, match="ARCHETYPE_CONTROL_CATALOG_TOKEN is required"):
        StorageService(control_catalog_config=ControlCatalogConfig.from_env())


async def test_get_world_retries_transient_server_errors(monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(_remote_catalog.asyncio, "sleep", sleep)
    catalog = await _catalog_with(
        [
            httpx.Response(503),
            httpx.Response(
                200,
                json={
                    "world_id": "w1",
                    "name": "alpha",
                    "run_id": "r1",
                    "parent_world_id": None,
                    "status": "active",
                    "tick_head": 3,
                },
            ),
        ]
    )
    try:
        world = await catalog.get_world("w1")
        assert world is not None and world.tick_head == 3
        assert world.writer_mode == "resumable"
        sleep.assert_awaited_once_with(0.5)
    finally:
        await catalog.close()


async def test_cleanup_only_registration_requires_v8_before_any_write():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with([httpx.Response(404)], requests)
    record = WorldRecord(
        world_id="private",
        name="private",
        run_id="run",
        parent_world_id=None,
        status="active",
        tick_head=0,
        writer_mode="cleanup_only",
    )
    try:
        with pytest.raises(RuntimeError, match="protocol v8"):
            await catalog.register_world(record)
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/ns/test/protocol")
        ]
    finally:
        await catalog.close()


async def test_cleanup_only_registration_sends_marker_after_v8_preflight():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json={"catalog_protocol_version": 8}),
            httpx.Response(
                200,
                json={
                    "ok": True,
                    "status": "active",
                    "catalog_status": "active",
                    "world_status": "active",
                    "writer_mode": "cleanup_only",
                    "catalog_protocol_version": 8,
                    "gateway_protocol_version": 8,
                },
            ),
        ],
        requests,
    )
    record = WorldRecord(
        world_id="private",
        name="private",
        run_id="run",
        parent_world_id=None,
        status="active",
        tick_head=0,
        writer_mode="cleanup_only",
    )
    try:
        await catalog.register_world(record)
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/ns/test/protocol"),
            ("POST", "/ns/test/protocol/v8/worlds"),
        ]
        assert json.loads(requests[1].content)["writer_mode"] == "cleanup_only"
    finally:
        await catalog.close()


@pytest.mark.parametrize("missing_status", ["catalog_status", "world_status"])
async def test_cleanup_only_registration_requires_both_active_authorities(
    missing_status: str,
):
    requests: list[httpx.Request] = []
    registration = {
        "ok": True,
        "status": "active",
        "catalog_status": "active",
        "world_status": "active",
        "writer_mode": "cleanup_only",
        "catalog_protocol_version": 8,
        "gateway_protocol_version": 8,
    }
    registration.pop(missing_status)
    catalog = await _catalog_with(
        [
            httpx.Response(200, json={"catalog_protocol_version": 8}),
            httpx.Response(200, json=registration),
            httpx.Response(200, json=_cleanup_only_world_row()),
            httpx.Response(200, json=_retirement_receipt()),
        ],
        requests,
    )
    try:
        with pytest.raises(RuntimeError, match="authorities.*active"):
            await catalog.register_world(WorldRecord(**_cleanup_only_world_row()))
        assert requests[-1].url.path.endswith("/retire")
    finally:
        await catalog.close()


async def test_cleanup_only_registration_requires_v8_gateway_confirmation():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json={"catalog_protocol_version": 8}),
            httpx.Response(
                200,
                json={
                    "ok": True,
                    "status": "active",
                    "writer_mode": "cleanup_only",
                    "catalog_protocol_version": 8,
                },
            ),
            httpx.Response(200, json=_cleanup_only_world_row()),
            httpx.Response(200, json=_retirement_receipt()),
        ],
        requests,
    )
    record = WorldRecord(
        world_id="private",
        name="private",
        run_id="run",
        parent_world_id=None,
        status="active",
        tick_head=0,
        writer_mode="cleanup_only",
    )
    try:
        with pytest.raises(RuntimeError, match="gateway protocol v8"):
            await catalog.register_world(record)
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/ns/test/protocol"),
            ("POST", "/ns/test/protocol/v8/worlds"),
            ("GET", "/ns/test/worlds/private"),
            ("POST", "/ns/test/protocol/v8/worlds/private/retire"),
        ]
    finally:
        await catalog.close()


async def test_cleanup_only_registration_never_falls_back_after_versioned_rejection():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json={"catalog_protocol_version": 8}),
            httpx.Response(404, json={"error": "bad_route"}),
            httpx.Response(404, json={"error": "not_found"}),
            httpx.Response(200, json=_retirement_receipt(disposition="tombstoned")),
        ],
        requests,
    )
    record = WorldRecord(
        world_id="private",
        name="private",
        run_id="run",
        parent_world_id=None,
        status="active",
        tick_head=0,
        writer_mode="cleanup_only",
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await catalog.register_world(record)
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/ns/test/protocol"),
            ("POST", "/ns/test/protocol/v8/worlds"),
            ("GET", "/ns/test/worlds/private"),
            ("POST", "/ns/test/protocol/v8/worlds/private/retire"),
        ]
    finally:
        await catalog.close()


async def test_cleanup_only_registration_requires_exact_v8_response():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json={"catalog_protocol_version": 8}),
            httpx.Response(
                200,
                json={
                    "ok": True,
                    "status": "active",
                    "writer_mode": "cleanup_only",
                    "catalog_protocol_version": 7,
                    "gateway_protocol_version": 8,
                },
            ),
            httpx.Response(200, json=_cleanup_only_world_row()),
            httpx.Response(200, json=_retirement_receipt()),
        ],
        requests,
    )
    record = WorldRecord(
        world_id="private",
        name="private",
        run_id="run",
        parent_world_id=None,
        status="active",
        tick_head=0,
        writer_mode="cleanup_only",
    )
    try:
        with pytest.raises(RuntimeError, match="protocol v8"):
            await catalog.register_world(record)
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/ns/test/protocol"),
            ("POST", "/ns/test/protocol/v8/worlds"),
            ("GET", "/ns/test/worlds/private"),
            ("POST", "/ns/test/protocol/v8/worlds/private/retire"),
        ]
    finally:
        await catalog.close()


async def test_ambiguous_cleanup_only_registration_retires_exact_committed_identity():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json={"catalog_protocol_version": 8}),
            httpx.Response(503, json={"error": "status_mirror_failed"}),
            httpx.Response(200, json=_cleanup_only_world_row()),
            httpx.Response(200, json=_retirement_receipt()),
        ],
        requests,
    )
    record = WorldRecord(**_cleanup_only_world_row())
    try:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await catalog.register_world(record)
        assert caught.value.response.status_code == 503
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/ns/test/protocol"),
            ("POST", "/ns/test/protocol/v8/worlds"),
            ("GET", "/ns/test/worlds/private"),
            ("POST", "/ns/test/protocol/v8/worlds/private/retire"),
        ]
        assert json.loads(requests[-1].content) == {
            **_cleanup_only_world_row(),
            "status": "destroyed",
        }
    finally:
        await catalog.close()


async def test_ambiguous_cleanup_only_registration_never_retires_conflicting_identity():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json={"catalog_protocol_version": 8}),
            httpx.Response(503, json={"error": "status_mirror_failed"}),
            httpx.Response(
                200,
                json=_cleanup_only_world_row(run_id="other-run"),
            ),
        ],
        requests,
    )
    record = WorldRecord(**_cleanup_only_world_row())
    try:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await catalog.register_world(record)
        assert caught.value.response.status_code == 503
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/ns/test/protocol"),
            ("POST", "/ns/test/protocol/v8/worlds"),
            ("GET", "/ns/test/worlds/private"),
        ]
    finally:
        await catalog.close()


async def test_ambiguous_cleanup_only_transport_error_tombstones_absent_identity():
    requests: list[httpx.Request] = []
    catalog = RemoteControlCatalog("https://catalog.invalid", "test")
    await catalog._client.aclose()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/protocol"):
            return httpx.Response(200, json={"catalog_protocol_version": 8})
        if request.url.path.endswith("/protocol/v8/worlds"):
            raise httpx.ReadError("registration response lost", request=request)
        if request.method == "GET":
            return httpx.Response(404, json={"error": "not_found"})
        return httpx.Response(
            200,
            json={
                **_cleanup_only_world_row(status="destroyed"),
                "ok": True,
                "disposition": "tombstoned",
                "catalog_protocol_version": 8,
                "gateway_protocol_version": 8,
                "catalog_status": "destroyed",
                "world_status": "destroyed",
            },
        )

    catalog._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    record = WorldRecord(**_cleanup_only_world_row())
    try:
        with pytest.raises(httpx.ReadError, match="response lost"):
            await catalog.register_world(record)
        assert [(request.method, request.url.path) for request in requests[:3]] == [
            ("GET", "/ns/test/protocol"),
            ("POST", "/ns/test/protocol/v8/worlds"),
            ("GET", "/ns/test/worlds/private"),
        ]
        retirement = requests[3]
        assert retirement.method == "POST"
        assert retirement.url.path.endswith("/retire")
        assert json.loads(retirement.content) == {
            **_cleanup_only_world_row(),
            "status": "destroyed",
        }
    finally:
        await catalog.close()


async def test_ambiguous_cleanup_only_registration_cancellation_waits_for_exact_retirement():
    requests: list[httpx.Request] = []
    registration_committed = asyncio.Event()
    retirement_entered = asyncio.Event()
    release_retirement = asyncio.Event()
    retirement_finished = asyncio.Event()
    catalog = RemoteControlCatalog("https://catalog.invalid", "test")
    await catalog._client.aclose()

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/protocol"):
            return httpx.Response(200, json={"catalog_protocol_version": 8})
        if request.url.path.endswith("/protocol/v8/worlds"):
            registration_committed.set()
            await asyncio.Event().wait()
            raise AssertionError("cancelled registration unexpectedly resumed")
        if request.method == "GET":
            return httpx.Response(200, json=_cleanup_only_world_row())
        if request.url.path.endswith("/retire"):
            retirement_entered.set()
            await release_retirement.wait()
            retirement_finished.set()
            return httpx.Response(200, json=_retirement_receipt())
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    catalog._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    record = WorldRecord(**_cleanup_only_world_row())
    task = asyncio.create_task(catalog.register_world(record))
    try:
        await asyncio.wait_for(registration_committed.wait(), timeout=1.0)
        task.cancel()
        await asyncio.wait_for(retirement_entered.wait(), timeout=1.0)
        await asyncio.sleep(0)
        assert not task.done()
        release_retirement.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1.0)
        assert retirement_finished.is_set()
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/ns/test/protocol"),
            ("POST", "/ns/test/protocol/v8/worlds"),
            ("GET", "/ns/test/worlds/private"),
            ("POST", "/ns/test/protocol/v8/worlds/private/retire"),
        ]
    finally:
        release_retirement.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await catalog.close()


async def test_confirmation_mismatch_never_retires_conflicting_identity():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json={"catalog_protocol_version": 8}),
            httpx.Response(
                200,
                json={
                    "ok": True,
                    "status": "active",
                    "writer_mode": "resumable",
                    "catalog_protocol_version": 8,
                    "gateway_protocol_version": 8,
                },
            ),
            httpx.Response(
                200,
                json=_cleanup_only_world_row(run_id="other-run"),
            ),
        ],
        requests,
    )
    record = WorldRecord(**_cleanup_only_world_row())
    try:
        with pytest.raises(RuntimeError, match="writer mode"):
            await catalog.register_world(record)
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/ns/test/protocol"),
            ("POST", "/ns/test/protocol/v8/worlds"),
            ("GET", "/ns/test/worlds/private"),
        ]
    finally:
        await catalog.close()


async def test_registration_mismatch_retirement_finishes_before_cancellation():
    requests: list[httpx.Request] = []
    retirement_entered = asyncio.Event()
    release_retirement = asyncio.Event()
    retirement_finished = asyncio.Event()
    catalog = RemoteControlCatalog("https://catalog.invalid", "test")
    await catalog._client.aclose()

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/protocol"):
            return httpx.Response(200, json={"catalog_protocol_version": 8})
        if request.url.path.endswith("/protocol/v8/worlds"):
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "status": "active",
                    "writer_mode": "resumable",
                    "catalog_protocol_version": 8,
                    "gateway_protocol_version": 8,
                },
            )
        if request.method == "GET":
            return httpx.Response(200, json=_cleanup_only_world_row())
        if request.url.path.endswith("/retire"):
            retirement_entered.set()
            await release_retirement.wait()
            retirement_finished.set()
            return httpx.Response(200, json=_retirement_receipt())
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    catalog._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    record = WorldRecord(
        world_id="private",
        name="private",
        run_id="run",
        parent_world_id=None,
        status="active",
        tick_head=0,
        writer_mode="cleanup_only",
    )
    task = asyncio.create_task(catalog.register_world(record))
    try:
        await asyncio.wait_for(retirement_entered.wait(), timeout=1.0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release_retirement.set()
        with pytest.raises(BaseExceptionGroup) as caught:
            await asyncio.wait_for(task, timeout=1.0)
        failures = _exception_leaves(caught.value)
        assert [type(failure) for failure in failures] == [
            RuntimeError,
            asyncio.CancelledError,
        ]
        assert (
            str(failures[0])
            == "remote control catalog protocol v8 did not preserve immutable world writer mode"
        )
        assert retirement_finished.is_set()
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/ns/test/protocol"),
            ("POST", "/ns/test/protocol/v8/worlds"),
            ("GET", "/ns/test/worlds/private"),
            ("POST", "/ns/test/protocol/v8/worlds/private/retire"),
        ]
    finally:
        release_retirement.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await catalog.close()


async def test_registration_mismatch_preserves_cancellation_and_retirement_failure():
    retirement_entered = asyncio.Event()
    release_retirement = asyncio.Event()
    catalog = RemoteControlCatalog("https://catalog.invalid", "test")
    await catalog._client.aclose()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/protocol"):
            return httpx.Response(200, json={"catalog_protocol_version": 8})
        if request.url.path.endswith("/protocol/v8/worlds"):
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "status": "active",
                    "writer_mode": "resumable",
                    "catalog_protocol_version": 8,
                    "gateway_protocol_version": 8,
                },
            )
        if request.method == "GET":
            return httpx.Response(200, json=_cleanup_only_world_row())
        if request.url.path.endswith("/retire"):
            retirement_entered.set()
            await release_retirement.wait()
            raise RuntimeError("fail-closed retirement failed")
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    catalog._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    record = WorldRecord(
        world_id="private",
        name="private",
        run_id="run",
        parent_world_id=None,
        status="active",
        tick_head=0,
        writer_mode="cleanup_only",
    )
    task = asyncio.create_task(catalog.register_world(record))
    try:
        await asyncio.wait_for(retirement_entered.wait(), timeout=1.0)
        task.cancel()
        release_retirement.set()
        with pytest.raises(BaseExceptionGroup) as caught:
            await asyncio.wait_for(task, timeout=1.0)

        failures = _exception_leaves(caught.value)
        assert [type(failure) for failure in failures] == [
            RuntimeError,
            RuntimeError,
            asyncio.CancelledError,
        ]
        assert [str(failure) for failure in failures[:2]] == [
            "remote control catalog protocol v8 did not preserve immutable world writer mode",
            "fail-closed retirement failed",
        ]
    finally:
        release_retirement.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await catalog.close()


def _command_row(**overrides):
    row = {
        "command_id": "c1",
        "sequence": 1,
        "scheduled_tick": 2,
        "priority": 3,
        "command_type": "custom",
        "payload_json": "{}",
        "payload_digest": "digest",
        "version": 1,
        "principal_id": "actor-1",
        "origin": "gateway",
        "reserved_entity_id": 7,
        "status": "PENDING",
        "attempts": 0,
        "max_attempts": 3,
        "lease_owner": None,
        "lease_expires_at": None,
        "last_error_code": None,
        "last_error_detail": None,
        "accepted_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "applied_tick": None,
        "commit_token": None,
    }
    row.update(overrides)
    return row


def _outbox_row():
    return {
        "sequence": 4,
        "event_id": "event-1",
        "aggregate_type": "command",
        "aggregate_id": "c1",
        "event_type": "command_queued",
        "command_type": "custom",
        "status": "queued",
        "actor_id": "actor-1",
        "payload_json": "{}",
        "occurred_at": "2026-01-01T00:00:00Z",
        "projected_at": None,
    }


def _evaluation_lease_row(**overrides):
    row = {
        "run_id": "run-1",
        "evaluation_id": "evaluation-1",
        "subject_digest": "subject",
        "contract_digest": "contract",
        "status": "RUNNING",
        "owner": "worker-1",
        "lease_expires_at": 30.0,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "acquired": True,
    }
    row.update(overrides)
    return row


async def test_command_ledger_transport_round_trip_is_typed_and_scoped():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json=[_command_row()]),
            httpx.Response(
                200,
                json=[
                    _command_row(
                        status="LEASED",
                        attempts=1,
                        lease_owner="worker-1",
                        lease_expires_at=30.0,
                    )
                ],
            ),
            httpx.Response(
                200,
                json=_command_row(
                    status="REJECTED",
                    attempts=1,
                    last_error_code="poison",
                    last_error_detail="invalid payload",
                ),
            ),
            httpx.Response(204),
            httpx.Response(200, json=[_command_row()]),
            httpx.Response(200, json={"count": 1}),
            httpx.Response(200, json={"entity_id": 7}),
            httpx.Response(200, json={"count": 1}),
        ],
        requests,
    )
    admission = CommandAdmission(
        command_id="c1",
        scheduled_tick=2,
        priority=3,
        command_type="custom",
        payload_json="{}",
        payload_digest="digest",
        version=1,
        principal_id="actor-1",
        origin="gateway",
        reserved_entity_id=7,
    )
    try:
        (admitted,) = await catalog.admit_commands("world-1", [admission])
        (leased,) = await catalog.lease_commands(
            "world-1", 2, "worker-1", lease_seconds=30.0, limit=4
        )
        failed = await catalog.fail_command(
            "world-1",
            "c1",
            "worker-1",
            status="REJECTED",
            error_code="poison",
            error_detail="invalid payload",
        )
        await catalog.release_commands("world-1", ["c1"], "worker-1")
        listed = await catalog.list_commands("world-1", status="PENDING", limit=5)
        pending = await catalog.pending_command_count("world-1")
        reserved = await catalog.max_reserved_entity_id("world-1")
        cancelled = await catalog.cancel_commands("world-1", reason="destroyed")

        assert admitted.world_id == "world-1" and admitted.reserved_entity_id == 7
        assert leased.status == "LEASED" and leased.lease_owner == "worker-1"
        assert failed.status == "REJECTED" and failed.last_error_code == "poison"
        assert [record.command_id for record in listed] == ["c1"]
        assert (pending, reserved, cancelled) == (1, 7, 1)
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/commands/admit",
            "/ns/test/w/world-1/commands/lease",
            "/ns/test/w/world-1/commands/c1/fail",
            "/ns/test/w/world-1/commands/release",
            "/ns/test/w/world-1/commands",
            "/ns/test/w/world-1/commands/pending-count",
            "/ns/test/w/world-1/commands/max-reserved",
            "/ns/test/w/world-1/commands/cancel",
        ]
        assert dict(requests[4].url.params) == {"limit": "5", "status": "PENDING"}
    finally:
        await catalog.close()


async def test_outbox_transport_preserves_order_and_projection_progress():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json=[_outbox_row()]),
            httpx.Response(204),
            httpx.Response(200, json={"watermark": 4, "pending": 0}),
        ],
        requests,
    )
    try:
        (event,) = await catalog.read_outbox("world-1", limit=8)
        await catalog.mark_outbox_projected("world-1", [event.event_id])
        progress = await catalog.outbox_progress("world-1")

        assert event.world_id == "world-1"
        assert event.sequence == 4 and event.status == "queued"
        assert progress == (4, 0)
        assert dict(requests[0].url.params) == {"limit": "8"}
        assert requests[1].method == "POST"
        assert requests[1].content == b'{"event_ids":["event-1"]}'
    finally:
        await catalog.close()


async def test_evaluation_lease_transport_is_typed_and_scoped():
    requests: list[httpx.Request] = []
    catalog = await _catalog_with(
        [
            httpx.Response(200, json=_evaluation_lease_row()),
            httpx.Response(200, json={"ok": True}),
            httpx.Response(200, json={"ok": True}),
        ],
        requests,
    )
    try:
        lease = await catalog.lease_evaluation(
            "world-1",
            "run-1",
            "evaluation-1",
            "subject",
            "contract",
            "worker-1",
            lease_seconds=45,
        )
        await catalog.complete_evaluation("world-1", "run-1", "evaluation-1", "worker-1")
        await catalog.release_evaluation("world-1", "run-1", "evaluation-1", "worker-1")

        assert lease.world_id == "world-1"
        assert lease.acquired and lease.owner == "worker-1"
        assert [request.url.path for request in requests] == [
            "/ns/test/w/world-1/evaluations/lease",
            "/ns/test/w/world-1/evaluations/complete",
            "/ns/test/w/world-1/evaluations/release",
        ]
        assert json.loads(requests[0].content)["lease_seconds"] == 45
    finally:
        await catalog.close()
