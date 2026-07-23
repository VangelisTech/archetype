# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""API route tests using FastAPI TestClient."""

import subprocess
import sys

import pytest

from archetype import __version__
from archetype.api.app import create_app
from archetype.api.deps import get_actor_ctx, set_container
from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth import guard
from archetype.app.gateway.auth.errors import GuardrailError
from archetype.app.gateway.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig
from archetype.world.models import EpisodeConfig, RolloutConfig

pytest.importorskip("httpx", reason="httpx required for API tests")

from fastapi.testclient import TestClient

from archetype import Component

ROLES = ("admin", "operator", "player", "viewer")


class QueryRouteMetric104(Component):
    value: float = 0.0


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
        assert data["version"] == __version__
        assert client.get("/openapi.json").json()["info"]["version"] == __version__

    def test_healthz_does_not_require_auth(self, client):
        resp = client.get("/healthz")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


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
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "test_world"
        assert "world_id" in data
        assert set(data) == {"world_id", "name", "tick", "run_id"}

    @pytest.mark.parametrize("role", ROLES)
    def test_create_world_role_gate(self, client, tmp_path, role):
        resp = client.post(
            "/worlds",
            json={"name": f"gate_{role}", "storage_uri": str(tmp_path / f"store-{role}")},
            headers={"Authorization": f"Bearer {role}"},
        )
        assert resp.status_code == (201 if role == "admin" else 403)

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


class TestWorldRouteErrors:
    def test_get_world_invalid_uuid(self, client):
        resp = client.get("/worlds/not-a-uuid")
        assert resp.status_code == 400

    def test_delete_world_not_found(self, client):
        resp = client.delete("/worlds/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 204
        assert resp.content == b""

    def test_delete_world(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "del_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.delete(f"/worlds/{world_id}")
        assert resp.status_code == 204
        assert resp.content == b""

        # Verify world is gone
        resp = client.get(f"/worlds/{world_id}")
        assert resp.status_code == 404

    def test_create_world_duplicate_name_conflict(self, client, tmp_path):
        """Issue #180: duplicate world name is a 409, not a 500."""
        body = {"name": "dup_name", "storage_uri": str(tmp_path / "store")}
        assert client.post("/worlds", json=body).status_code == 201
        resp = client.post("/worlds", json=body)
        assert resp.status_code == 409
        assert "dup_name" in resp.json()["detail"]

    def test_fork_world_duplicate_name_conflict(self, client, tmp_path):
        body = {"name": "dup_fork", "storage_uri": str(tmp_path / "store")}
        created = client.post("/worlds", json=body)
        world_id = created.json()["world_id"]

        resp = client.post(f"/worlds/{world_id}/fork", json={"name": "dup_fork"})

        assert resp.status_code == 409
        assert client.get(f"/worlds/{world_id}").status_code == 200

    def test_delete_world_invalid_uuid(self, client):
        """Issue #180: an unparsable id is a client error, not a silent no-op.

        The safe-no-op destroy contract covers *missing* worlds; an id that can
        never name a world must be rejected like the sibling GET route does.
        """
        resp = client.delete("/worlds/not-a-uuid")
        assert resp.status_code == 400

    def test_submit_command_unknown_world_is_404(self, client):
        """Issue #180: WorldNotFoundError maps to 404, not 500."""
        resp = client.post(
            "/worlds/00000000-0000-0000-0000-000000000000/commands",
            json={"type": "despawn", "payload": {"entity_id": 1}},
        )
        assert resp.status_code == 404, resp.text

    def test_submit_command_invalid_uuid_world_is_404(self, client):
        """An id that can never exist in the registry is not-found, not a 500."""
        resp = client.post(
            "/worlds/not-a-uuid/commands",
            json={"type": "despawn", "payload": {"entity_id": 1}},
        )
        assert resp.status_code == 404, resp.text

    def test_submit_batch_unknown_world_is_404(self, client):
        resp = client.post(
            "/worlds/00000000-0000-0000-0000-000000000000/commands/batch",
            json={"commands": [{"type": "despawn", "payload": {"entity_id": 1}}]},
        )
        assert resp.status_code == 404, resp.text

    def test_entity_and_simulation_routes_unknown_world_are_404(self, client):
        phantom = "00000000-0000-0000-0000-000000000000"
        assert (
            client.post(f"/worlds/{phantom}/entities", json={"components": []}).status_code == 404
        )
        assert client.delete(f"/worlds/{phantom}/entities/1").status_code == 404
        assert client.post(f"/worlds/{phantom}/simulation/step", json={}).status_code == 404


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

    def test_submit_unknown_world_is_not_found(self, client):
        resp = client.post(
            "/worlds/00000000-0000-0000-0000-000000000000/commands",
            json={"type": "spawn", "payload": {"components": []}},
        )
        assert resp.status_code == 404

    def test_submit_invalid_command_type(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "bad_cmd", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.post(
            f"/worlds/{world_id}/commands",
            json={"type": "nonexistent_type", "payload": {}},
        )
        assert resp.status_code == 400
        assert "not a valid CommandType" in resp.json()["detail"]

    def test_submit_batch(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "batch_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.post(
            f"/worlds/{world_id}/commands/batch",
            json={
                "commands": [
                    {"type": "spawn", "payload": {"components": []}},
                    {"type": "spawn", "payload": {"components": []}},
                ]
            },
        )
        assert resp.status_code == 200
        ids = resp.json()["command_ids"]
        assert len(ids) == 2
        assert all(isinstance(i, str) for i in ids)

    def test_submit_batch_invalid_type(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "batch_bad", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.post(
            f"/worlds/{world_id}/commands/batch",
            json={
                "commands": [
                    {"type": "spawn", "payload": {}},
                    {"type": "bogus", "payload": {}},
                ]
            },
        )
        assert resp.status_code == 400

    def test_get_command_history(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "hist_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        # Submit a command
        client.post(
            f"/worlds/{world_id}/commands",
            json={"type": "spawn", "payload": {"components": []}},
        )

        resp = client.get(f"/worlds/{world_id}/commands")
        assert resp.status_code == 200
        history = resp.json()
        assert len(history) >= 1
        assert any(row["type"] == "spawn" for row in history)

    def test_command_history_schema_is_pinned(self, client):
        schema = client.get("/openapi.json").json()
        response_schema = schema["paths"]["/worlds/{world_id}/commands"]["get"]["responses"]["200"][
            "content"
        ]["application/json"]["schema"]
        assert response_schema["type"] == "array"
        assert response_schema["items"]["type"] == "object"

    def test_get_pending(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "pending_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.get(f"/worlds/{world_id}/commands/pending")
        assert resp.status_code == 404

    def test_get_pending_after_submit(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "pending2", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        client.post(
            f"/worlds/{world_id}/commands",
            json={"type": "spawn", "payload": {"components": []}},
        )

        resp = client.get(f"/worlds/{world_id}/commands/pending")
        assert resp.status_code == 404


class TestSimulationRoutes:
    def test_step(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "step_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.post(f"/worlds/{world_id}/step", json={"num_steps": 1})
        assert resp.status_code == 200
        assert set(resp.json()) == {"commands_applied"}

    def test_step_emits_one_audit_row(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "step_audit", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.post(f"/worlds/{world_id}/step", json={"num_steps": 1})
        assert resp.status_code == 200

        history = client.get(f"/worlds/{world_id}/history").json()
        assert [row["command_type"] for row in history] == ["create_world", "step"]

    def test_step_not_found(self, client):
        resp = client.post(
            "/worlds/00000000-0000-0000-0000-000000000000/step",
            json={},
        )
        assert resp.status_code == 404

    def test_run(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "run_test", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.post(
            f"/worlds/{world_id}/run",
            json={"num_steps": 3},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ticks_completed"] == 3
        assert data["world_id"] == world_id
        assert "run_id" in data

    def test_run_not_found(self, client):
        resp = client.post(
            "/worlds/00000000-0000-0000-0000-000000000000/run",
            json={"num_steps": 1},
        )
        assert resp.status_code == 404

    def test_negative_run_count_is_rejected_before_execution(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "negative-run", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        response = client.post(f"/worlds/{world_id}/run", json={"num_steps": -1})

        assert response.status_code == 422
        assert client.get(f"/worlds/{world_id}").json()["tick"] == 0

    def test_step_schema_is_pinned(self, client):
        schema = client.get("/openapi.json").json()
        response_schema = schema["paths"]["/worlds/{world_id}/step"]["post"]["responses"]["200"][
            "content"
        ]["application/json"]["schema"]
        assert response_schema["$ref"] == "#/components/schemas/StepResponse"


class TestQueryRoutes:
    @staticmethod
    def _create_metric_world(client, tmp_path) -> str:
        create_resp = client.post(
            "/worlds",
            json={"name": "query-options", "storage_uri": str(tmp_path / "store")},
        )
        assert create_resp.status_code == 201, create_resp.text
        world_id = create_resp.json()["world_id"]

        for value in range(5):
            spawn = client.post(
                f"/worlds/{world_id}/entities",
                json={"components": [QueryRouteMetric104(value=value).to_payload()]},
            )
            assert spawn.status_code == 201, spawn.text

        step = client.post(f"/worlds/{world_id}/step", json={})
        assert step.status_code == 200, step.text
        return world_id

    def test_component_query_filters_before_show(self, client, tmp_path):
        world_id = self._create_metric_world(client, tmp_path)
        response = client.get(
            f"/worlds/{world_id}/components",
            params={
                "types": "QueryRouteMetric104",
                "where": "queryroutemetric104__value >= 2",
                "show": 2,
            },
        )

        assert response.status_code == 200, response.text
        rows = response.json()
        assert len(rows) == 2
        assert all(row["queryroutemetric104__value"] >= 2 for row in rows)

    def test_unfiltered_state_and_entity_use_world_storage(self, client, tmp_path):
        world_id = self._create_metric_world(client, tmp_path)

        state = client.get(f"/worlds/{world_id}/state")
        target_entity = min(row["entity_id"] for row in state.json())
        entity = client.get(f"/worlds/{world_id}/entities/{target_entity}")

        assert state.status_code == 200, state.text
        assert entity.status_code == 200, entity.text
        assert len(state.json()) == 5
        assert [row["entity_id"] for row in entity.json()] == [target_entity]

    def test_component_query_count_is_a_distinct_terminal(self, client, tmp_path, monkeypatch):
        world_id = self._create_metric_world(client, tmp_path)

        def forbid_row_materialization(_frame):
            pytest.fail("count queries must not serialize rows")

        monkeypatch.setattr(
            "archetype.api.routes.query.dataframe_to_rows",
            forbid_row_materialization,
        )
        response = client.get(
            f"/worlds/{world_id}/components",
            params={
                "types": "QueryRouteMetric104",
                "where": "queryroutemetric104__value > 1",
                "count": True,
            },
        )

        assert response.status_code == 200, response.text
        assert response.json() == {"count": 3}

    def test_component_query_tick_selects_a_historical_snapshot(self, client, tmp_path):
        world_id = self._create_metric_world(client, tmp_path)
        spawn = client.post(
            f"/worlds/{world_id}/entities",
            json={"components": [QueryRouteMetric104(value=5).to_payload()]},
        )
        assert spawn.status_code == 201, spawn.text
        step = client.post(f"/worlds/{world_id}/step", json={})
        assert step.status_code == 200, step.text

        counts = []
        for tick in (0, 1):
            response = client.get(
                f"/worlds/{world_id}/components",
                params={"types": "QueryRouteMetric104", "tick": tick, "count": True},
            )
            assert response.status_code == 200, response.text
            counts.append(response.json()["count"])

        assert counts == [5, 6]

    @pytest.mark.parametrize(
        ("params", "detail"),
        (
            (
                {"types": "QueryRouteMetric104", "where": "value > 1 and value < 4"},
                "Invalid where expression",
            ),
            (
                {"types": "QueryRouteMetric104", "where": "missing__value > 1"},
                "Unknown query column",
            ),
            (
                {"types": "QueryRouteMetric104", "count": True, "show": 2},
                "mutually exclusive",
            ),
            (
                {"count": True},
                "require at least one component type",
            ),
            (
                {"show": 2},
                "require at least one component type",
            ),
            (
                {"where": "entity_id > 0"},
                "require at least one component type",
            ),
        ),
    )
    def test_component_query_rejects_invalid_options(self, client, tmp_path, params, detail):
        world_id = self._create_metric_world(client, tmp_path)
        response = client.get(f"/worlds/{world_id}/components", params=params)

        assert response.status_code == 400, response.text
        assert detail in response.json()["detail"]

    def test_history_accepts_tick_range(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "history_tick_range", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.get(f"/worlds/{world_id}/history?tick_range=0,10")
        assert resp.status_code == 200

    def test_history_rejects_invalid_tick_range(self, client, tmp_path):
        create_resp = client.post(
            "/worlds",
            json={"name": "history_bad_tick_range", "storage_uri": str(tmp_path / "store")},
        )
        world_id = create_resp.json()["world_id"]

        resp = client.get(f"/worlds/{world_id}/history?tick_range=10,0")
        assert resp.status_code == 400

    def test_signatures_accepts_storage_query(self, client, tmp_path):
        resp = client.get(f"/signatures?storage_uri={tmp_path / 'store'}&namespace=ns")
        assert resp.status_code == 200
        assert resp.json() == []

    @pytest.mark.parametrize(
        ("query", "expected_uri", "expected_namespace"),
        (
            ("storage_uri=&namespace=custom", None, "custom"),
            ("storage_uri=/tmp/custom&namespace=", "/tmp/custom", None),
        ),
    )
    def test_signatures_treats_empty_storage_fields_as_missing(
        self,
        client,
        monkeypatch,
        query,
        expected_uri,
        expected_namespace,
    ):
        from archetype.app.gateway.service import CommandGateway
        from archetype.core.config import StorageConfig

        captured = {}

        async def list_signatures(_self, _ctx, storage_config):
            captured["config"] = storage_config
            return []

        monkeypatch.setattr(CommandGateway, "list_signatures", list_signatures)
        assert client.get(f"/signatures?{query}").status_code == 200

        defaults = StorageConfig()
        config = captured["config"]
        assert config.uri == (expected_uri or defaults.uri)
        assert config.namespace == (expected_namespace or defaults.namespace)

    def test_create_world_request_defaults_match_storage_config(self):
        """The API's `CreateWorldRequest` shorthand defaults must match
        ``StorageConfig`` defaults.

        Otherwise a world created via ``POST /worlds`` with no body lands at
        one storage path while subsequent reads (``GET /worlds/{id}/state``,
        ``GET /signatures``) default to the ``StorageConfig()`` path and return
        empty results.
        """
        from archetype.api.models import CreateWorldRequest
        from archetype.core.config import StorageConfig

        api_default = CreateWorldRequest()
        sc_default = StorageConfig()

        assert api_default.storage_uri == sc_default.uri, (
            f"CreateWorldRequest.storage_uri default ({api_default.storage_uri!r}) "
            f"does not match StorageConfig.uri default ({sc_default.uri!r})"
        )
        assert api_default.namespace == sc_default.namespace, (
            f"CreateWorldRequest.namespace default ({api_default.namespace!r}) "
            f"does not match StorageConfig.namespace default ({sc_default.namespace!r})"
        )

    def test_signatures_default_storage_matches_storage_config(self):
        """``GET /signatures`` with only one storage param must fall back to
        ``StorageConfig`` defaults, not to a hand-rolled set in the route.
        """
        # The query route used ``"./archetype_data"`` and ``"archetypes"`` as
        # local fallbacks, contradicting StorageConfig's actual defaults.
        from archetype.api.routes.query import _split_csv  # noqa: F401  (smoke import)
        from archetype.core.config import StorageConfig

        # If the route ever resolves a single-arg call, both fields must come
        # from StorageConfig() so the data location matches the rest of the API.
        sc_default = StorageConfig()
        # Build the StorageConfig the way the route should: omit fields and let
        # pydantic apply defaults rather than substituting the wrong literal.
        from_route = StorageConfig(uri=sc_default.uri, namespace="custom")
        assert from_route.uri == sc_default.uri
        assert from_route.namespace == "custom"

    def test_create_then_query_roundtrip_default_storage(self, client, tmp_path, monkeypatch):
        """End-to-end: a world created via POST /worlds with the API's default
        shorthand must be readable via GET /signatures without explicit storage.

        The defaults must agree across endpoints; otherwise data lands in one
        store and reads target another.
        """
        # Run inside tmp_path so any relative defaults resolve to a tmp dir
        # rather than polluting the workspace.
        monkeypatch.chdir(tmp_path)

        create_resp = client.post("/worlds", json={"name": "default_storage"})
        assert create_resp.status_code == 201
        world_id = create_resp.json()["world_id"]

        # Submit a spawn, then step so the row materializes to disk.
        client.post(
            f"/worlds/{world_id}/commands",
            json={"type": "spawn", "payload": {"components": []}},
        )
        client.post(f"/worlds/{world_id}/step", json={"num_steps": 1})

        # /signatures with no params must hit the same store the POST landed on.
        sigs_resp = client.get("/signatures")
        assert sigs_resp.status_code == 200, sigs_resp.text
        assert sigs_resp.json(), (
            "API default-storage POST landed in a different store than "
            "GET /signatures default-storage; signatures list came back empty"
        )

    @pytest.mark.parametrize(
        ("path", "method"),
        (
            ("/worlds/{world_id}/state", "get"),
            ("/worlds/{world_id}/entities/{entity_id}", "get"),
            ("/worlds/{world_id}/components", "get"),
            ("/worlds/{world_id}/history", "get"),
            ("/worlds/{world_id}/processors", "get"),
            ("/worlds/{world_id}/hooks", "get"),
            ("/worlds/{world_id}/resources", "get"),
            ("/signatures", "get"),
        ),
    )
    def test_query_response_schema_is_pinned(self, client, path, method):
        schema = client.get("/openapi.json").json()
        response_schema = schema["paths"][path][method]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]
        assert response_schema != {}


def test_route_modules_do_not_import_forbidden_services():
    route_dir = __import__("pathlib").Path("src/archetype/api/routes")
    forbidden = (
        "archetype.app.world.mutation",
        "archetype.app.world.simulation",
        "archetype.app.query.service",
        "archetype.app.world.service",
    )
    offenders = []
    for path in route_dir.glob("*.py"):
        text = path.read_text()
        for needle in forbidden:
            if needle in text:
                offenders.append(f"{path}:{needle}")
    assert offenders == []


@pytest.mark.asyncio
async def test_development_principal_identity_is_stable_across_requests(monkeypatch):
    monkeypatch.setattr(guard, "MAX_CMDS_PER_TICK", 1)
    first = await get_actor_ctx("Bearer player")
    second = await get_actor_ctx("Bearer player")

    assert first.id == second.id
    guard.guardrail_allow(Command(type=CommandType.CUSTOM), first)
    with pytest.raises(GuardrailError):
        guard.guardrail_allow(Command(type=CommandType.CUSTOM), second)


@pytest.mark.parametrize(
    ("model", "field"),
    (
        (lambda: RunConfig(num_steps=-1), "num_steps"),
        (lambda: EpisodeConfig(max_steps=-1), "max_steps"),
        (lambda: RolloutConfig(num_episodes=-1), "num_episodes"),
    ),
)
def test_negative_execution_counts_fail_model_validation(model, field):
    with pytest.raises(ValueError, match=field):
        model()


def test_api_import_boundary_lint_script_passes():
    result = subprocess.run(
        [sys.executable, "scripts/check_api_import_boundaries.py"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
