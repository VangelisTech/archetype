# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""API route tests using FastAPI TestClient."""

import pytest

from archetype.api.app import create_app
from archetype.api.deps import set_container
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.container import ServiceContainer

pytest.importorskip("httpx", reason="httpx required for API tests")

from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest.fixture
def client():
    container = ServiceContainer()
    set_container(container)
    app = create_app()
    with TestClient(app) as c:
        yield c
    set_container(None)


class TestRootRoute:
    def test_root(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "archetype-ecs"


class TestWorldRoutes:
    def test_list_worlds_empty(self, client):
        resp = client.get("/worlds")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_create_world(self, client, tmp_path):
        resp = client.post(
            "/worlds",
            json={"name": "test_world", "storage_uri": str(tmp_path / "store")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test_world"
        assert "world_id" in data

    def test_create_and_list(self, client, tmp_path):
        client.post(
            "/worlds",
            json={"name": "w1", "storage_uri": str(tmp_path / "store")},
        )
        resp = client.get("/worlds")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_get_world_not_found(self, client):
        resp = client.get("/worlds/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404


class TestCommandRoutes:
    def test_submit_command(self, client, tmp_path):
        # Create a world first
        create_resp = client.post(
            "/worlds",
            json={"name": "cmd_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.post(
            f"/worlds/{world_id}/commands",
            json={"type": "spawn", "payload": {"components": []}},
        )
        assert resp.status_code == 200
        assert resp.json()["type"] == "spawn"

    def test_get_pending(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "pending_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.get(f"/worlds/{world_id}/commands/pending")
        assert resp.status_code == 200
        assert resp.json()["pending_count"] == 0


class TestSimulationRoutes:
    def test_step(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "step_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.post(f"/worlds/{world_id}/step", json={"num_steps": 1})
        assert resp.status_code == 200
        assert "commands_applied" in resp.json()


class TestQueryRoutes:
    def test_get_state(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "query_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.get(f"/worlds/{world_id}/state")
        assert resp.status_code == 200
        assert "world_id" in resp.json()

    def test_get_history(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "history_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.get(f"/worlds/{world_id}/history")
        assert resp.status_code == 200
        assert resp.json() == []
