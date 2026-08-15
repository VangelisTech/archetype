#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Install and probe one complete Archetype release from a package index."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from packaging.version import InvalidVersion, Version

if __package__:
    from .package_smoke import (
        _LIBRARY_IMPORTS,
        _OPERATION_COUNTS,
        _WORLD_STACK_DISTRIBUTIONS,
        _smol_probe_source,
    )
    from .release_artifact import SCHEMA, artifact_records, manifest_sha256
else:  # pragma: no cover - exercised by the command-line entry point
    from package_smoke import (  # type: ignore[no-redef]
        _LIBRARY_IMPORTS,
        _OPERATION_COUNTS,
        _WORLD_STACK_DISTRIBUTIONS,
        _smol_probe_source,
    )
    from release_artifact import SCHEMA, artifact_records, manifest_sha256

Run = Callable[..., subprocess.CompletedProcess[str]]
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")


def _release_version(value: str) -> str:
    """Return one canonical public version or reject ambiguous requirements."""

    try:
        parsed = Version(value)
    except InvalidVersion as error:
        raise argparse.ArgumentTypeError(f"invalid release version {value!r}") from error
    if (
        str(parsed) != value
        or len(parsed.release) != 3
        or parsed.is_prerelease
        or parsed.is_postrelease
        or parsed.is_devrelease
        or parsed.local is not None
    ):
        raise argparse.ArgumentTypeError(
            "release version must be canonical and cannot be a development or local version"
        )
    return value


def _manifest_identity(path: Path) -> dict[str, str]:
    """Return the exact version, commit, and digest of one clean manifest."""

    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError("release artifact manifest must be an object")
    if value.get("schema") != SCHEMA or value.get("clean_checkout") is not True:
        raise ValueError("registry smoke requires a current clean-checkout release manifest")
    artifact_records(value)
    version = value.get("version")
    if not isinstance(version, str):  # pragma: no cover - artifact_records owns this check
        raise ValueError("release artifact manifest has no version")
    try:
        release_version = _release_version(version)
    except argparse.ArgumentTypeError as error:
        raise ValueError(str(error)) from error
    commit = value.get("commit")
    if not isinstance(commit, str) or _COMMIT.fullmatch(commit) is None:
        raise ValueError("registry smoke requires a full manifest commit")
    return {
        "version": release_version,
        "commit": commit,
        "sha256": manifest_sha256(value),
    }


def _manifest_version(path: Path) -> str:
    """Return the exact public version bound to one clean release manifest."""

    return _manifest_identity(path)["version"]


def _index_url(value: str) -> str:
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise argparse.ArgumentTypeError("package index must be an HTTPS base URL")
    return value.rstrip("/")


def _requirements(matrix: str, version: str) -> tuple[str, ...]:
    exact = {
        distribution: f"{distribution}=={version}"
        for distribution in (*_WORLD_STACK_DISTRIBUTIONS, "archetype-smol")
    }
    selected = {
        "base": (exact["archetype-ecs"],),
        "missions": (exact["archetype-ecs"], exact["archetype-missions"]),
        "physical-ai": (exact["archetype-ecs"], exact["archetype-physical-ai"]),
        "research": (exact["archetype-ecs"], exact["archetype-research"]),
        "all": tuple(exact[distribution] for distribution in _WORLD_STACK_DISTRIBUTIONS),
        "smol": (exact["archetype-smol"],),
    }
    try:
        return selected[matrix]
    except KeyError as error:  # pragma: no cover - internal caller invariant
        raise ValueError(f"unknown registry smoke matrix {matrix!r}") from error


def _install_commands(
    *,
    uv: str,
    python: Path,
    requirements: Sequence[str],
    index_url: str,
    extra_index_url: str | None,
) -> tuple[list[str], ...]:
    command = [
        uv,
        "--no-config",
        "pip",
        "install",
        "--python",
        str(python),
        "--no-cache",
        "--only-binary=:all:",
        "--index-url",
        index_url,
    ]
    if extra_index_url is None:
        command.extend(requirements)
        return (command,)

    # Test indexes generally do not mirror third-party dependencies. Install
    # the exact Archetype artifacts from the target index without dependencies,
    # then ask the dependency index to satisfy the already-installed packages.
    # This avoids an extra-index strategy that could silently source an
    # Archetype wheel from the wrong registry.
    target = [*command, "--no-deps", *requirements]
    dependencies = [
        uv,
        "--no-config",
        "pip",
        "install",
        "--python",
        str(python),
        "--no-cache",
        "--only-binary=:all:",
        "--index-url",
        extra_index_url,
        *requirements,
    ]
    return target, dependencies


def _clean_environment() -> dict[str, str]:
    """Discard ambient Python and resolver configuration for registry evidence."""

    return {
        name: value
        for name, value in os.environ.items()
        if name not in {"PYTHONHOME", "PYTHONPATH"}
        and not name.startswith("PIP_")
        and not name.startswith("UV_")
    }


def _run_checked(
    command: Sequence[str],
    *,
    cwd: Path,
    env: dict[str, str],
    label: str,
    run: Run,
) -> None:
    process = run(
        list(command),
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if process.returncode:
        raise RuntimeError(
            f"registry {label} failed with exit code {process.returncode}\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )


def _probe_source(matrix: str, version: str) -> str:
    if matrix == "smol":
        return _smol_probe_source(version, matrix)

    expected_libraries = {
        "base": [],
        "missions": ["missions"],
        "physical-ai": ["physical-ai"],
        "research": ["research"],
        "all": ["missions", "physical-ai", "research"],
    }[matrix]
    return f"""
import asyncio
import importlib
import importlib.util
import json
import sys
from importlib.metadata import version
from pathlib import Path

import archetype
from archetype.api.app import create_app
from archetype.runtime.runtime import ArchetypeRuntime, SyncArchetypeRuntime
from archetype.runtime.world import RuntimeWorld, SyncRuntimeWorld
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources

expected = {expected_libraries!r}
imports = {_LIBRARY_IMPORTS!r}
release_version = {version!r}
assert archetype.__version__ == release_version
assert version("archetype-ecs") == release_version
package_root = Path(archetype.__file__).resolve()
assert "site-packages" in package_root.parts, package_root
assert not any("/packages/" in value and "/src" in value for value in sys.path), sys.path

for name, module in imports.items():
    assert (importlib.util.find_spec(module) is not None) is (name in expected), (name, expected)
    if name in expected:
        assert version("archetype-" + name) == release_version
assert importlib.util.find_spec("archetype.episodes") is None
assert importlib.util.find_spec("archetype.artifacts.contracts") is None
assert importlib.util.find_spec("archetype.smol") is None

removed_root_names = (
    "AutoResearchConfig",
    "AutoResearchResult",
    "CandidateContext",
    "EvaluationResult",
    "HostedEpisodeObservation",
    "HostedEpisodeRequest",
    "ModalHostedEpisodeConfig",
    "ResearchCandidateContext",
)
assert not any(hasattr(archetype, name) for name in removed_root_names)
assert "__getattr__" not in ArchetypeRuntime.__dict__
assert "__getattr__" not in RuntimeWorld.__dict__
assert "__getattr__" not in SyncRuntimeWorld.__dict__
assert "library" not in SyncRuntimeWorld.__dict__
assert "library" not in SyncArchetypeRuntime.__dict__
if "missions" in expected:
    assert not hasattr(importlib.import_module("archetype.missions"), "RuntimeMissions")
if "research" in expected:
    research = importlib.import_module("archetype.research")
    assert not hasattr(research, "CandidateContext")
    assert not hasattr(importlib.import_module("archetype.research.models"), "CandidateContext")

resources = build_runtime_resources(RuntimeBootstrapConfig.from_env())
try:
    names = [manifest.name for manifest in resources.world_library_manifests]
    assert names == expected, names
    operation_count = len(resources.dispatcher._registry.specs)
    assert operation_count == {_OPERATION_COUNTS[matrix]}, operation_count
finally:
    asyncio.run(resources.aclose())

app = create_app()
mission_paths = sorted(
    path
    for path in app.openapi()["paths"]
    if "/missions" in path or "/tasks/" in path
)
assert len(mission_paths) == (3 if "missions" in expected else 0), mission_paths
print(json.dumps({{
    "matrix": {matrix!r},
    "libraries": expected,
    "operations": operation_count,
    "module": str(package_root),
    "version": release_version,
}}))
"""


def _run_matrix(
    *,
    matrix: str,
    version: str,
    index_url: str,
    extra_index_url: str | None,
    uv: str,
    root: Path,
    run: Run = subprocess.run,
) -> dict[str, Any]:
    environment = root / f"venv-{matrix}"
    clean_env = _clean_environment()
    _run_checked(
        [uv, "--no-config", "venv", "--python", sys.executable, str(environment)],
        cwd=root,
        env=clean_env,
        label=f"{matrix} environment creation",
        run=run,
    )
    python = environment / "bin" / "python"
    for command in _install_commands(
        uv=uv,
        python=python,
        requirements=_requirements(matrix, version),
        index_url=index_url,
        extra_index_url=extra_index_url,
    ):
        _run_checked(
            command,
            cwd=root,
            env=clean_env,
            label=f"{matrix} installation",
            run=run,
        )
    _run_checked(
        [uv, "--no-config", "pip", "check", "--python", str(python)],
        cwd=root,
        env=clean_env,
        label=f"{matrix} dependency check",
        run=run,
    )
    freeze = run(
        [uv, "--no-config", "pip", "freeze", "--python", str(python)],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        env=clean_env,
    )
    if freeze.returncode:
        raise RuntimeError(
            f"registry {matrix} dependency inventory failed\n"
            f"stdout:\n{freeze.stdout}\nstderr:\n{freeze.stderr}"
        )
    process = run(
        [str(python), "-c", _probe_source(matrix, version)],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        env=clean_env,
    )
    if process.returncode:
        raise RuntimeError(
            f"registry {matrix} package probe failed\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
    result = json.loads(process.stdout.strip().splitlines()[-1])
    result["requirements"] = list(_requirements(matrix, version))
    result["installed_distributions"] = sorted(
        line for line in freeze.stdout.splitlines() if line.strip()
    )
    return result


def smoke_registry(
    *,
    version: str,
    index_url: str,
    extra_index_url: str | None = None,
) -> list[dict[str, Any]]:
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("registry smoke requires uv")
    with tempfile.TemporaryDirectory(prefix="archetype-registry-smoke-") as temporary:
        root = Path(temporary)
        return [
            _run_matrix(
                matrix=matrix,
                version=version,
                index_url=index_url,
                extra_index_url=extra_index_url,
                uv=uv,
                root=root,
            )
            for matrix in (*_OPERATION_COUNTS, "smol")
        ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--index-url", default="https://pypi.org/simple", type=_index_url)
    parser.add_argument("--extra-index-url", type=_index_url)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    identity = _manifest_identity(args.manifest)
    version = identity["version"]
    results = smoke_registry(
        version=version,
        index_url=args.index_url,
        extra_index_url=args.extra_index_url,
    )
    receipt = {
        "schema": "archetype.registry-install-evidence/v2",
        "version": version,
        "manifest_commit": identity["commit"],
        "manifest_sha256": identity["sha256"],
        "index_url": args.index_url,
        "dependency_index_url": args.extra_index_url,
        "matrices": results,
    }
    if args.out is not None:
        args.out.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        "Registry distribution matrix passed: "
        + ", ".join(
            f"{row['matrix']}={row['operations']}" if "operations" in row else f"{row['matrix']}=ok"
            for row in results
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
