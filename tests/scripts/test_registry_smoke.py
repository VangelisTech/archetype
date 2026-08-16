# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for post-publication package-index verification."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import pytest

from scripts.registry_smoke import (
    _clean_environment,
    _install_commands,
    _manifest_identity,
    _manifest_version,
    _probe_source,
    _release_version,
    _requirements,
    _run_checked,
    _run_matrix,
)
from scripts.release_artifact import DISTRIBUTIONS, SCHEMA, manifest_sha256


def _manifest() -> dict[str, object]:
    prefixes = {
        "archetype-ecs": "archetype_ecs",
        "archetype-missions": "archetype_missions",
        "archetype-physical-ai": "archetype_physical_ai",
        "archetype-research": "archetype_research",
    }
    return {
        "schema": SCHEMA,
        "version": "0.6.0",
        "commit": "a" * 40,
        "clean_checkout": True,
        "artifacts": [
            {
                "distribution": distribution,
                "kind": kind,
                "name": (
                    f"{prefixes[distribution]}-0.6.0-py3-none-any.whl"
                    if kind == "wheel"
                    else f"{prefixes[distribution]}-0.6.0.tar.gz"
                ),
                "sha256": "a" * 64,
                "size_bytes": 1,
            }
            for distribution in DISTRIBUTIONS
            for kind in ("wheel", "sdist")
        ],
    }


def test_registry_matrix_pins_every_selected_distribution() -> None:
    assert _requirements("base", "0.6.0") == ("archetype-ecs==0.6.0",)
    assert _requirements("missions", "0.6.0") == (
        "archetype-ecs==0.6.0",
        "archetype-missions==0.6.0",
    )
    assert _requirements("physical-ai", "0.6.0") == (
        "archetype-ecs==0.6.0",
        "archetype-physical-ai==0.6.0",
    )
    assert _requirements("research", "0.6.0") == (
        "archetype-ecs==0.6.0",
        "archetype-research==0.6.0",
    )
    assert _requirements("all", "0.6.0") == (
        "archetype-ecs==0.6.0",
        "archetype-missions==0.6.0",
        "archetype-physical-ai==0.6.0",
        "archetype-research==0.6.0",
    )


@pytest.mark.parametrize("value", ["v0.6.0", "0.6", "0.6.0+local", "0.6.0.dev1"])
def test_registry_smoke_rejects_noncanonical_release_versions(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        _release_version(value)


def test_production_index_install_uses_no_cache_or_fallback() -> None:
    (command,) = _install_commands(
        uv="uv",
        python=Path("/venv/bin/python"),
        requirements=("archetype-ecs==0.6.0",),
        index_url="https://pypi.org/simple",
        extra_index_url=None,
    )

    assert command == [
        "uv",
        "--no-config",
        "pip",
        "install",
        "--python",
        "/venv/bin/python",
        "--no-cache",
        "--only-binary=:all:",
        "--index-url",
        "https://pypi.org/simple",
        "archetype-ecs==0.6.0",
    ]
    assert "--extra-index-url" not in command
    assert "--no-config" in command
    assert "--only-binary=:all:" in command


def test_test_index_mode_sources_internal_artifacts_before_dependencies() -> None:
    requirements = _requirements("all", "0.6.0")
    target, dependencies = _install_commands(
        uv="uv",
        python=Path("/venv/bin/python"),
        requirements=requirements,
        index_url="https://test.pypi.org/simple",
        extra_index_url="https://pypi.org/simple",
    )

    assert target[-len(requirements) :] == list(requirements)
    assert target[target.index("--index-url") + 1] == "https://test.pypi.org/simple"
    assert "--no-deps" in target
    assert dependencies[-len(requirements) :] == list(requirements)
    assert dependencies[dependencies.index("--index-url") + 1] == "https://pypi.org/simple"
    assert "--no-deps" not in dependencies
    assert "--extra-index-url" not in target + dependencies
    assert "--no-config" in target and "--no-config" in dependencies
    assert "--only-binary=:all:" in target and "--only-binary=:all:" in dependencies


def test_registry_probe_rejects_split_compatibility_facades() -> None:
    probe = _probe_source("all", "0.6.0")

    assert "assert not any(hasattr(archetype, name)" in probe
    assert '"__getattr__" not in ArchetypeRuntime.__dict__' in probe
    assert 'not hasattr(importlib.import_module("archetype.missions"), "RuntimeMissions")' in probe
    assert 'not hasattr(research, "CandidateContext")' in probe
    assert 'find_spec("archetype.artifacts.contracts") is None' in probe
    assert '"library" not in SyncRuntimeWorld.__dict__' in probe
    assert '"library" not in SyncArchetypeRuntime.__dict__' in probe


def test_registry_smoke_derives_version_from_clean_manifest(tmp_path: Path) -> None:
    path = tmp_path / "release-artifact.json"
    path.write_text(json.dumps(_manifest()), encoding="utf-8")

    assert _manifest_version(path) == "0.6.0"
    assert _manifest_identity(path) == {
        "version": "0.6.0",
        "commit": "a" * 40,
        "sha256": manifest_sha256(_manifest()),
    }

    value = _manifest()
    value["clean_checkout"] = False
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="clean-checkout"):
        _manifest_version(path)


def test_registry_commands_discard_ambient_resolver_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PIP_INDEX_URL", "https://wrong.invalid/simple")
    monkeypatch.setenv("UV_EXTRA_INDEX_URL", "https://wrong.invalid/simple")
    monkeypatch.setenv("PYTHONPATH", "/workspace/packages")
    monkeypatch.setenv("PATH", "/usr/bin")

    clean = _clean_environment()

    assert clean["PATH"] == "/usr/bin"
    assert "PIP_INDEX_URL" not in clean
    assert "UV_EXTRA_INDEX_URL" not in clean
    assert "PYTHONPATH" not in clean


def test_registry_command_failure_preserves_resolver_diagnostics(tmp_path: Path) -> None:
    def fail(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 2, stdout="resolver output", stderr="missing wheel")

    with pytest.raises(RuntimeError, match=r"(?s)resolver output.*missing wheel"):
        _run_checked(
            ["uv", "pip", "install"],
            cwd=tmp_path,
            env={},
            label="test installation",
            run=fail,
        )


def test_registry_matrix_receipt_records_requirements_and_resolved_inventory(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[2:4] == ["pip", "freeze"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="zeta==2\narchetype-ecs==0.6.0\n",
                stderr="",
            )
        if command[0].endswith("/bin/python"):
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=(
                    '{"matrix":"base","libraries":[],"operations":37,'
                    '"module":"site-packages/archetype","version":"0.6.0"}\n'
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    result = _run_matrix(
        matrix="base",
        version="0.6.0",
        index_url="https://pypi.org/simple",
        extra_index_url=None,
        uv="uv",
        root=tmp_path,
        run=run,
    )

    assert result["requirements"] == ["archetype-ecs==0.6.0"]
    assert result["installed_distributions"] == ["archetype-ecs==0.6.0", "zeta==2"]
    assert any(command[2:4] == ["pip", "freeze"] for command in calls)
