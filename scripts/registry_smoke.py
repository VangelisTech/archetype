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
    from .release_artifact import INDEPENDENT_VERSIONS, SCHEMA, artifact_records, manifest_sha256
else:  # pragma: no cover - exercised by the command-line entry point
    from release_artifact import INDEPENDENT_VERSIONS, SCHEMA, artifact_records, manifest_sha256

MATRICES = ("base", "analysis", "transports", "smol")

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
    native = f"archetype-native=={version}"
    selected = {
        "base": (f"archetype-ecs=={version}", native),
        "analysis": (f"archetype-ecs[analysis]=={version}", native),
        "transports": (
            f"archetype-ecs[transports]=={version}",
            native,
            f"archetype-transports=={version}",
        ),
        "smol": (f"archetype-smol=={INDEPENDENT_VERSIONS['archetype-smol']}",),
    }
    try:
        return selected[matrix]
    except KeyError as error:
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
    if matrix not in MATRICES:
        raise ValueError("Unknown installation matrix")
    common = """
import asyncio, importlib, importlib.util, json, pathlib, sys
from importlib.metadata import version
if sys.flags.optimize: raise RuntimeError("Optimized registry proof is unsupported")
def require(value, label):
    if not value: raise RuntimeError(label)
def origin(module):
    path = pathlib.Path(importlib.import_module(module).__file__).resolve()
    require(path.is_relative_to(pathlib.Path(sys.prefix).resolve()), "Noninstalled module: " + module)
    return str(path)
"""
    if matrix == "smol":
        return (
            common
            + f"""
require(version("archetype-smol") == {INDEPENDENT_VERSIONS["archetype-smol"]!r}, "Smol version")
require(importlib.util.find_spec("archetype.core") is None, "Smol loaded framework")
from archetype.smol import Component, Processor, World
from daft import col
class Counter(Component):
    count: int = 0
class Increment(Processor):
    components = (Counter,)
    def process(self, df, *, tick):
        return df.with_column("counter__count", col("counter__count") + 1)
world = World(processors=[Increment()])
world.spawn(Counter())
world.run(steps=2)
require(world.query(Counter).to_pylist()[0]["counter__count"] == 2, "Smol execution")
print(json.dumps({{"matrix":"smol", "module":origin("archetype.smol"), "version":version("archetype-smol")}}))
"""
        )
    modules = ["archetype", "archetype.runtime", "archetype_native"]
    if matrix == "transports":
        modules.append("archetype_transports")
    source = (
        common
        + f"""
require(version("archetype-ecs") == {version!r}, "ECS version")
require(version("archetype-native") == {version!r}, "Native version")
origins = {{name:origin(name) for name in {modules!r}}}
require(importlib.util.find_spec("archetype.research") is None, "Removed Research installed")
from archetype import ArchetypeRuntime
require(not any(name == "daft" or name.startswith(("daft.", "archetype.core.")) for name in sys.modules), "Live facade eagerly imported analysis")
async def inert():
    async with ArchetypeRuntime(storage_only=True) as runtime:
        world = runtime.world("inert")
        require(not any(hasattr(world, name) for name in ("spawn", "step", "request")), "Removed runtime operation")
        await world.shutdown()
asyncio.run(inert())
"""
    )
    if matrix == "analysis":
        source += """
from archetype import ArtifactSource
value = ArtifactSource(source_uri="synthetic.txt")
require(value is not None, "Artifact declaration")
require(importlib.util.find_spec("daft") is not None, "Analysis extra missing")
"""
    if matrix == "transports":
        source += f"""
require(version("archetype-transports") == {version!r}, "Transports version")
from archetype.api.config import ServerConfig
from archetype.api.principals import PrincipalDirectory
require(importlib.util.find_spec("mcp") is not None, "Official MCP SDK missing")
"""
    return (
        source
        + f'\nprint(json.dumps({{"matrix":{matrix!r}, "origins":origins, "version":version("archetype-ecs")}}))\n'
    )


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
            for matrix in MATRICES
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
        "schema": "archetype.registry-install-evidence/v3",
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
