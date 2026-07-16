# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Transport behavior for the remote control-catalog client."""

from unittest.mock import AsyncMock

import httpx
import pytest

from archetype.app import _remote_catalog
from archetype.app._remote_catalog import RemoteControlCatalog
from archetype.app.storage_service import StorageService
from archetype.core.config import StorageConfig

pytestmark = pytest.mark.asyncio


async def _catalog_with(responses: list[httpx.Response]) -> RemoteControlCatalog:
    catalog = RemoteControlCatalog("https://catalog.invalid", "test")
    await catalog._client.aclose()

    def handler(_request: httpx.Request) -> httpx.Response:
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


async def test_get_claim_retries_then_treats_not_found_as_terminal(monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(_remote_catalog.asyncio, "sleep", sleep)
    catalog = await _catalog_with([httpx.Response(500), httpx.Response(404)])
    try:
        assert await catalog.get_claim("w1", "missing") is None
        sleep.assert_awaited_once_with(0.5)
    finally:
        await catalog.close()
