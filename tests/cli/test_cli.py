# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""CLI tests using Typer CliRunner.

Tests for ``serve`` and ``--help`` run without a server. Integration tests
patch ``_request`` to route through a FastAPI TestClient against a real
ServiceContainer so the full stack is exercised without a running server.
"""

from __future__ import annotations

import ast
from unittest.mock import patch

import httpx
import pytest
from click import unstyle
from click.exceptions import Exit as ClickExit
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from archetype.api.app import create_app
from archetype.api.deps import set_container
from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.cli import main as cli_mod
from archetype.cli.main import ENV_BASE_URL, _base_url, _check_server, _handle_response, app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest.fixture
def api_client():
    """Yield a TestClient wired to a fresh ServiceContainer."""
    container = ServiceContainer()
    set_container(container)
    fastapi_app = create_app()
    with TestClient(fastapi_app) as tc:
        yield tc
    set_container(None)


@pytest.fixture
def _patch_request(api_client):
    """Patch ``archetype.cli.main._request`` so CLI commands route through
    the in-process TestClient without creating real HTTP connections."""

    def _fake_request(method: str, path: str, **kwargs):
        role = kwargs.pop("role", None)
        token = kwargs.pop("token", None)
        kwargs.pop("url", None)
        headers = kwargs.pop("headers", {})
        headers = {**headers, **cli_mod._headers(role, token)}
        if headers:
            kwargs["headers"] = headers
        resp = getattr(api_client, method)(path, **kwargs)
        # Let success and failure propagate the same way the real _request does,
        # including 204 No Content responses.
        return cli_mod._handle_response(resp, role=role)

    with patch("archetype.cli.main._request", _fake_request):
        yield


class TestCLI:
    def test_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "archetype" in result.output.lower() or "simulation" in result.output.lower()

    def test_serve_help(self):
        result = runner.invoke(app, ["serve", "--help"])
        assert result.exit_code == 0
        assert "host" in result.output.lower()
        assert "port" in result.output.lower()

    def test_world_help(self):
        result = runner.invoke(app, ["world", "--help"])
        assert result.exit_code == 0
        assert "create" in result.output.lower()
        assert "list" in result.output.lower()
        assert "destroy" in result.output.lower()

    def test_world_create_help(self):
        result = runner.invoke(app, ["world", "create", "--help"])
        assert result.exit_code == 0

    def test_query_help_exposes_lazy_terminals(self):
        result = runner.invoke(app, ["query", "--help"], color=True)
        assert result.exit_code == 0, result.output
        output = unstyle(result.output)
        assert "COMPONENT_TYPES" in output
        assert "--show" in output
        assert "--count" in output
        assert "--where" in output

    def test_query_forwards_positional_components_and_options(self, monkeypatch):
        captured = {}

        def fake_request(method, path, **kwargs):
            captured.update(method=method, path=path, kwargs=kwargs)
            return [{"score__value": 0.75}]

        monkeypatch.setattr(cli_mod, "_request", fake_request)
        result = runner.invoke(
            app,
            [
                "query",
                "world-1",
                "Agent,Score",
                "--tick",
                "3",
                "--entity-ids",
                "7,8",
                "--where",
                "score__value > 0.5",
                "--show",
                "5",
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        assert captured["method"] == "get"
        assert captured["path"] == "/worlds/world-1/components"
        assert captured["kwargs"]["params"] == {
            "entity_ids": "7,8",
            "tick": 3,
            "types": "Agent,Score",
            "show": 5,
            "where": "score__value > 0.5",
        }

    def test_query_count_uses_object_terminal(self, monkeypatch):
        captured = {}

        def fake_request(method, path, **kwargs):
            captured.update(method=method, path=path, kwargs=kwargs)
            return {"count": 3}

        monkeypatch.setattr(cli_mod, "_request", fake_request)
        result = runner.invoke(app, ["query", "world-1", "Score", "--count"])

        assert result.exit_code == 0, result.output
        assert "Count: 3" in result.output
        assert captured["path"] == "/worlds/world-1/components"
        assert captured["kwargs"]["params"] == {"types": "Score", "count": True}

    @pytest.mark.parametrize(
        "arguments",
        (
            ["world-1", "Score", "--types", "Score"],
            ["world-1", "Score", "--count", "--show", "2"],
            ["world-1", "--where", "score__value > 0.5"],
        ),
    )
    def test_query_rejects_ambiguous_option_combinations(self, arguments):
        result = runner.invoke(app, ["query", *arguments])
        assert result.exit_code == 1
        assert "validation error" in result.output

    def test_base_url_uses_env_and_strips_trailing_slash(self, monkeypatch):
        monkeypatch.setenv(ENV_BASE_URL, "http://example.com/api/")
        assert _base_url() == "http://example.com/api"

    def test_check_server_request_error_exits_cleanly(self, capsys):
        class _Client:
            def get(self, path):
                raise httpx.RequestError("boom", request=httpx.Request("GET", "http://localhost"))

        with pytest.raises(ClickExit):
            _check_server(_Client())

        captured = capsys.readouterr()
        assert "Cannot reach Archetype server" in captured.err

    def test_handle_response_falls_back_to_text_when_json_is_invalid(self):
        request = httpx.Request("GET", "http://localhost/worlds")
        response = httpx.Response(500, request=request, text="server exploded")

        with pytest.raises(ClickExit):
            _handle_response(response)

    def test_handle_response_uses_error_detail_from_json(self):
        request = httpx.Request("GET", "http://localhost/worlds")
        response = httpx.Response(404, request=request, json={"detail": "missing world"})

        with pytest.raises(ClickExit):
            _handle_response(response)

    def test_request_closes_client_after_success(self, monkeypatch):
        response = httpx.Response(
            200,
            request=httpx.Request("GET", "http://localhost/worlds"),
            json={"ok": True},
        )

        class _Client:
            def __init__(self):
                self.closed = False

            def get(self, path, **kwargs):
                assert path == "/worlds"
                assert kwargs == {}
                return response

            def close(self):
                self.closed = True

        client = _Client()
        monkeypatch.setattr(cli_mod, "_client", lambda _url=None: client)
        monkeypatch.setattr(cli_mod, "_check_server", lambda _client, _url=None: None)

        assert cli_mod._request("get", "/worlds") == {"ok": True}
        assert client.closed is True

    def test_request_closes_client_after_error(self, monkeypatch):
        response = httpx.Response(
            500,
            request=httpx.Request("GET", "http://localhost/worlds"),
            json={"detail": "boom"},
        )

        class _Client:
            def __init__(self):
                self.closed = False

            def get(self, path, **kwargs):
                assert path == "/worlds"
                return response

            def close(self):
                self.closed = True

        client = _Client()
        monkeypatch.setattr(cli_mod, "_client", lambda _url=None: client)
        monkeypatch.setattr(cli_mod, "_check_server", lambda _client, _url=None: None)

        with pytest.raises(ClickExit):
            cli_mod._request("get", "/worlds")

        assert client.closed is True


class TestCLIIntegration:
    """Integration tests that hit the real API routes via TestClient."""

    @pytest.mark.usefixtures("_patch_request")
    def test_status_empty(self):
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "No worlds found" in result.output

    @pytest.mark.usefixtures("_patch_request")
    def test_status_lists_worlds(self, tmp_path):
        uri = str(tmp_path / "store")

        create = runner.invoke(app, ["world", "create", "status-test", "--uri", uri])
        world_id = create.output.split("Created world: ")[1].split()[0]

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert world_id in result.output
        assert "status-test" in result.output

    @pytest.mark.usefixtures("_patch_request")
    def test_world_create_then_list(self, tmp_path):
        """Regression for #60: create + list must share state via the server."""
        uri = str(tmp_path / "store")

        create = runner.invoke(
            app,
            ["world", "create", "my-sim", "--uri", uri, "--namespace", "ns"],
        )
        assert create.exit_code == 0, create.output
        assert "Created world" in create.output

        listing = runner.invoke(app, ["world", "list"])
        assert listing.exit_code == 0, listing.output
        assert "my-sim" in listing.output

    @pytest.mark.usefixtures("_patch_request")
    def test_world_inspect(self, tmp_path):
        uri = str(tmp_path / "store")

        create = runner.invoke(app, ["world", "create", "inspect-src", "--uri", uri])
        world_id = create.output.split("Created world: ")[1].split()[0]

        inspect = runner.invoke(app, ["world", "inspect", world_id])
        assert inspect.exit_code == 0, inspect.output
        assert f"World ID: {world_id}" in inspect.output
        assert "Name: inspect-src" in inspect.output

    @pytest.mark.usefixtures("_patch_request")
    def test_run_and_step(self, tmp_path):
        uri = str(tmp_path / "store")

        create = runner.invoke(
            app,
            ["world", "create", "run-test", "--uri", uri],
        )
        world_id = create.output.split("Created world: ")[1].split()[0]

        result = runner.invoke(app, ["run", world_id, "--steps", "2"])
        assert result.exit_code == 0, result.output
        assert "2 ticks" in result.output

        result = runner.invoke(app, ["step", world_id])
        assert result.exit_code == 0, result.output
        assert "Step complete" in result.output

    @pytest.mark.usefixtures("_patch_request")
    def test_world_destroy(self, tmp_path):
        uri = str(tmp_path / "store")

        create = runner.invoke(app, ["world", "create", "rm-test", "--uri", uri])
        world_id = create.output.split("Created world: ")[1].split()[0]

        result = runner.invoke(app, ["world", "destroy", world_id])
        assert result.exit_code == 0, result.output
        assert "Destroyed world" in result.output

        listing = runner.invoke(app, ["world", "list"])
        assert listing.exit_code == 0
        assert "No worlds found" in listing.output

    @pytest.mark.usefixtures("_patch_request")
    def test_role_flag_maps_api_permission(self, tmp_path):
        ok = runner.invoke(
            app,
            [
                "world",
                "create",
                "role-admin",
                "--uri",
                str(tmp_path / "admin-store"),
                "--role",
                "admin",
            ],
        )
        assert ok.exit_code == 0, ok.output

        shorthand = runner.invoke(
            app,
            [
                "world",
                "create",
                "role-short",
                "--uri",
                str(tmp_path / "short-store"),
                "--role",
                "a",
            ],
        )
        assert shorthand.exit_code == 0, shorthand.output

        denied = runner.invoke(
            app,
            [
                "world",
                "create",
                "role-viewer",
                "--uri",
                str(tmp_path / "viewer-store"),
                "--role",
                "viewer",
            ],
        )
        assert denied.exit_code == 1
        assert "permission denied for role viewer" in denied.output

    @pytest.mark.usefixtures("_patch_request")
    def test_entity_spawn(self, tmp_path):
        create = runner.invoke(app, ["world", "create", "entity-test", "--uri", str(tmp_path)])
        world_id = create.output.split("Created world: ")[1].split()[0]

        result = runner.invoke(
            app,
            ["entity", "spawn", world_id, "--components", "[]", "--role", "player"],
        )
        assert result.exit_code == 0, result.output
        assert "Spawned entity:" in result.output

    @pytest.mark.usefixtures("_patch_request")
    def test_destroy_then_query_without_live_world_succeeds(self, tmp_path):
        create = runner.invoke(app, ["world", "create", "destroy-query", "--uri", str(tmp_path)])
        world_id = create.output.split("Created world: ")[1].split()[0]
        destroy = runner.invoke(app, ["world", "destroy", world_id])
        assert destroy.exit_code == 0, destroy.output

        query = runner.invoke(app, ["query", world_id, "--json"])
        assert query.exit_code == 0, query.output
        assert "[]" in query.output

    @pytest.mark.usefixtures("_patch_request")
    def test_rollout_summary(self, tmp_path):
        create = runner.invoke(app, ["world", "create", "rollout-test", "--uri", str(tmp_path)])
        world_id = create.output.split("Created world: ")[1].split()[0]

        result = runner.invoke(
            app,
            ["rollout", world_id, "--num-episodes", "1", "--max-steps", "1"],
        )
        assert result.exit_code == 0, result.output
        assert "Rollout ID:" in result.output
        assert "Episodes: 1" in result.output

    @pytest.mark.usefixtures("_patch_request")
    def test_introspection_lists(self, tmp_path):
        create = runner.invoke(app, ["world", "create", "introspection", "--uri", str(tmp_path)])
        world_id = create.output.split("Created world: ")[1].split()[0]

        for group, empty in (
            ("processors", "No processors."),
            ("hooks", "No hooks."),
            ("resources", "No resources."),
        ):
            result = runner.invoke(app, [group, "list", world_id])
            assert result.exit_code == 0, result.output
            assert empty in result.output


def _is_app_module(module: str) -> bool:
    return module == "archetype.app" or module.startswith("archetype.app.")


def test_cli_app_import_boundary_uses_dotted_segments():
    assert _is_app_module("archetype.app.models")
    assert not _is_app_module("archetype.application")


def test_cli_does_not_import_forbidden_app_modules():
    source = cli_mod.__file__
    tree = ast.parse(open(source).read())
    forbidden = []
    allowed = {"archetype.app.models", "archetype.app.gateway.auth.models"}
    for node in ast.walk(tree):
        module = None
        if isinstance(node, ast.ImportFrom):
            module = node.module
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if _is_app_module(alias.name) and alias.name not in allowed:
                    forbidden.append(alias.name)
        if module and _is_app_module(module) and module not in allowed:
            forbidden.append(module)
    assert forbidden == []

    def test_no_server_gives_clear_error(self):
        """When the server is down, the CLI should exit with a useful message."""
        result = runner.invoke(app, ["status"])
        assert result.exit_code != 0
