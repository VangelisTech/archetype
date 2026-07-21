# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Transport behavior for the remote control-catalog client."""

import json
from unittest.mock import AsyncMock

import httpx
import pytest

from archetype.app.storage import remote_catalog as _remote_catalog
from archetype.app.storage.catalog import CommandAdmission
from archetype.app.storage.remote_catalog import RemoteControlCatalog
from archetype.app.storage.service import StorageService
from archetype.core.config import StorageConfig

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


async def test_remote_catalog_configuration_requires_token(monkeypatch):
    monkeypatch.setenv("ARCHETYPE_CONTROL_CATALOG_URL", "https://catalog.invalid")
    monkeypatch.delenv("ARCHETYPE_CONTROL_CATALOG_TOKEN", raising=False)

    service = StorageService()
    with pytest.raises(RuntimeError, match="ARCHETYPE_CONTROL_CATALOG_TOKEN is required"):
        service.get_control_catalog(StorageConfig())


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
        sleep.assert_awaited_once_with(0.5)
    finally:
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
