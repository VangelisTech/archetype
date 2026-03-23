# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""CLI tests using Typer CliRunner."""

from typer.testing import CliRunner

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
