# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""CLI tests using Typer CliRunner.

Tests for ``serve`` and ``--help`` run without a server. Integration tests
patch ``_request`` to route through a FastAPI TestClient against a real
ServiceContainer so the full stack is exercised without a running server.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from archetype.api.app import create_app
from archetype.api.deps import set_container
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.container import ServiceContainer
from archetype.cli.main import app

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


class TestCLIIntegration:
    """Integration tests that hit the real API routes via TestClient."""

    @pytest.mark.usefixtures("_patch_request")
    def test_status_empty(self):
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "No worlds found" in result.output

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
    def test_query_and_history(self, tmp_path):
        uri = str(tmp_path / "store")

        create = runner.invoke(app, ["world", "create", "q-test", "--uri", uri])
        world_id = create.output.split("Created world: ")[1].split()[0]

        result = runner.invoke(app, ["query", world_id])
        assert result.exit_code == 0, result.output
        assert "world_id" in result.output

        result = runner.invoke(app, ["history", world_id])
        assert result.exit_code == 0, result.output

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

    @pytest.mark.usefixtures("_patch_request")
    def test_query_no_components_returns_state(self, tmp_path):
        """query without component types returns world state overview."""
        uri = str(tmp_path / "store")
        create = runner.invoke(app, ["world", "create", "q2-test", "--uri", uri])
        world_id = create.output.split("Created world: ")[1].split()[0]

        result = runner.invoke(app, ["query", world_id])
        assert result.exit_code == 0, result.output
        assert "world_id" in result.output

    def test_query_help_shows_options(self):
        """query --help shows --show, --count, --where options."""
        result = runner.invoke(app, ["query", "--help"])
        assert result.exit_code == 0
        assert "--show" in result.output
        assert "--count" in result.output
        assert "--where" in result.output

    def test_no_server_gives_clear_error(self):
        """When the server is down, the CLI should exit with a useful message."""
        result = runner.invoke(app, ["status"])
        assert result.exit_code != 0
