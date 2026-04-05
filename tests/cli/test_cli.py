# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""CLI tests using Typer CliRunner."""

from typer.testing import CliRunner

from archetype.app.registry import REGISTRY_ENV_VAR
from archetype.cli.main import app

runner = CliRunner()


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

    def test_status_runs(self):
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0

    def test_world_create_then_list_shares_state(self, tmp_path, monkeypatch):
        """Regression for #60: two CLI invocations must share world state."""
        monkeypatch.setenv(REGISTRY_ENV_VAR, str(tmp_path / "registry.json"))
        store_uri = str(tmp_path / "store")

        create = runner.invoke(
            app,
            ["world", "create", "my-sim", "--uri", store_uri, "--namespace", "ns"],
        )
        assert create.exit_code == 0, create.output
        assert "Created world" in create.output

        listing = runner.invoke(app, ["world", "list"])
        assert listing.exit_code == 0, listing.output
        assert "my-sim" in listing.output
