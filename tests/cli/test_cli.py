# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""CLI tests using Typer CliRunner.

Tests for ``serve`` and ``--help`` run without a server. Integration tests
patch ``_request`` to route through a FastAPI TestClient against a real
ServiceContainer so the full stack is exercised without a running server.
"""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pytest
from click.exceptions import Exit as ClickExit
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from archetype.api.app import create_app
from archetype.api.deps import set_container
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.container import ServiceContainer
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
        resp = getattr(api_client, method)(path, **kwargs)
        if resp.is_success:
            return resp.json()
        # Let failures propagate the same way the real _request does
        from archetype.cli import main as cli_mod

        return cli_mod._handle_response(resp)

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

    def test_world_create_help(self):
        result = runner.invoke(app, ["world", "create", "--help"])
        assert result.exit_code == 0

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
        monkeypatch.setattr(cli_mod, "_client", lambda: client)
        monkeypatch.setattr(cli_mod, "_check_server", lambda _client: None)

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
        monkeypatch.setattr(cli_mod, "_client", lambda: client)
        monkeypatch.setattr(cli_mod, "_check_server", lambda _client: None)

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
    def test_world_remove(self, tmp_path):
        uri = str(tmp_path / "store")

        create = runner.invoke(app, ["world", "create", "rm-test", "--uri", uri])
        world_id = create.output.split("Created world: ")[1].split()[0]

        result = runner.invoke(app, ["world", "remove", world_id])
        assert result.exit_code == 0, result.output
        assert "Removed world" in result.output

        listing = runner.invoke(app, ["world", "list"])
        assert listing.exit_code == 0
        assert "No worlds found" in listing.output

    def test_no_server_gives_clear_error(self):
        """When the server is down, the CLI should exit with a useful message."""
        result = runner.invoke(app, ["status"])
        assert result.exit_code != 0
