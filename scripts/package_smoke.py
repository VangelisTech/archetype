#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Validate and install every built Archetype distribution outside the checkout."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from email.parser import Parser
from pathlib import Path
from typing import Any

_DISTRIBUTIONS = (
    "archetype-ecs",
    "archetype-missions",
    "archetype-physical-ai",
    "archetype-research",
)
_PACKAGE_PREFIXES = {
    "archetype-ecs": "archetype_ecs",
    "archetype-missions": "archetype_missions",
    "archetype-physical-ai": "archetype_physical_ai",
    "archetype-research": "archetype_research",
}
_LIBRARY_IMPORTS = {
    "missions": "archetype.missions",
    "physical-ai": "archetype.physical_ai",
    "research": "archetype.research",
}
_OPERATION_COUNTS = {
    "base": 37,
    "missions": 44,
    "physical-ai": 38,
    "research": 38,
    "all": 46,
}


def _one(paths: list[Path], label: str) -> Path:
    if len(paths) != 1:
        raise RuntimeError(f"expected exactly one {label}, found {[path.name for path in paths]}")
    return paths[0]


def _artifacts(dist_dir: Path) -> tuple[dict[str, Path], dict[str, Path]]:
    wheels: dict[str, Path] = {}
    sdists: dict[str, Path] = {}
    for distribution in _DISTRIBUTIONS:
        prefix = _PACKAGE_PREFIXES[distribution]
        wheels[distribution] = _one(
            sorted(dist_dir.glob(f"{prefix}-*.whl")),
            f"{distribution} wheel",
        )
        sdists[distribution] = _one(
            sorted(dist_dir.glob(f"{prefix}-*.tar.gz")),
            f"{distribution} source distribution",
        )
    unexpected_wheels = set(dist_dir.glob("*.whl")) - set(wheels.values())
    unexpected_sdists = set(dist_dir.glob("*.tar.gz")) - set(sdists.values())
    if unexpected_wheels or unexpected_sdists:
        raise RuntimeError(
            "unexpected distribution artifacts: "
            + ", ".join(sorted(path.name for path in unexpected_wheels | unexpected_sdists))
        )
    return wheels, sdists


def _validate_wheel_contents(
    distribution: str,
    wheel: Path,
    *,
    expected_version: str | None = None,
) -> str:
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        metadata_files = sorted(name for name in names if name.endswith(".dist-info/METADATA"))
        if len(metadata_files) != 1:
            raise RuntimeError(
                f"{distribution} wheel has invalid metadata inventory: {metadata_files}"
            )
        metadata = archive.read(metadata_files[0]).decode("utf-8")
    parsed_metadata = Parser().parsestr(metadata)
    metadata_name = parsed_metadata.get("Name")
    metadata_version = parsed_metadata.get("Version")
    if metadata_name != distribution:
        raise RuntimeError(
            f"{distribution} wheel declares unexpected project name {metadata_name!r}"
        )
    if not metadata_version:
        raise RuntimeError(f"{distribution} wheel does not declare a version")
    if expected_version is not None and metadata_version != expected_version:
        raise RuntimeError(
            f"{distribution} rebuilt wheel version {metadata_version!r} does not match "
            f"the attested wheel version {expected_version!r}"
        )
    license_files = sorted(name for name in names if ".dist-info/licenses/LICENSE" in name)
    if len(license_files) != 1:
        raise RuntimeError(
            f"{distribution} wheel must ship exactly one Apache license file: {license_files}"
        )
    forbidden_harnesses = ("tests/", "evals/", "bench/", "quality/")
    leaked = sorted(name for name in names if name.startswith(forbidden_harnesses))
    if leaked:
        raise RuntimeError(f"{distribution} wheel leaked repository files: {leaked[:5]}")

    root_init = "archetype/__init__.py"
    family_prefixes = {
        "missions": "archetype/missions/",
        "physical-ai": "archetype/physical_ai/",
        "research": "archetype/research/",
    }
    if distribution == "archetype-ecs":
        if root_init not in names:
            raise RuntimeError("framework wheel does not own archetype/__init__.py")
        leaked_families = sorted(
            name
            for name in names
            if any(name.startswith(prefix) for prefix in family_prefixes.values())
        )
        if leaked_families:
            raise RuntimeError(
                f"framework wheel contains world-library code: {leaked_families[:5]}"
            )
        for library in ("missions", "physical-ai", "research"):
            requirement = f'Requires-Dist: archetype-{library}<0.7,>=0.6; extra == "all"'
            if requirement not in metadata:
                raise RuntimeError(
                    "framework all extra does not converge on every first-party "
                    f"world library: missing {requirement}"
                )
        return metadata_version

    if root_init in names:
        raise RuntimeError(f"{distribution} must not replace the framework root facade")
    library = distribution.removeprefix("archetype-")
    owned_prefix = family_prefixes[library]
    code = sorted(name for name in names if name.startswith("archetype/") and name.endswith(".py"))
    foreign = [name for name in code if not name.startswith(owned_prefix)]
    if not code or foreign:
        raise RuntimeError(
            f"{distribution} wheel has invalid namespace contents; foreign={foreign[:5]}"
        )
    if distribution == "archetype-missions" and (
        "archetype/missions/sandboxes/versions.toml" not in names
    ):
        raise RuntimeError("Missions wheel is missing its pinned version inventory")
    return metadata_version


def _clean_subprocess_environment() -> dict[str, str]:
    return {
        name: value
        for name, value in os.environ.items()
        if name not in {"PYTHONHOME", "PYTHONPATH"}
        and not name.startswith("PIP_")
        and not name.startswith("UV_")
    }


def _rebuild_sdists(
    *,
    sdists: dict[str, Path],
    uv: str,
    root: Path,
) -> dict[str, Path]:
    """Build every exact sdist through isolated PEP 517 in a clean wheelhouse."""

    wheelhouse = root / "rebuilt-sdist-wheels"
    wheelhouse.mkdir()
    environment = _clean_subprocess_environment()
    rebuilt: dict[str, Path] = {}
    for distribution in _DISTRIBUTIONS:
        sdist = sdists[distribution].resolve()
        command = [
            uv,
            "build",
            "--wheel",
            "--force-pep517",
            "--no-config",
            "--no-cache",
            "--no-create-gitignore",
            "--out-dir",
            str(wheelhouse),
            str(sdist),
        ]
        process = subprocess.run(
            command,
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        if process.returncode:
            raise RuntimeError(
                f"isolated PEP 517 rebuild failed for {distribution} from {sdist.name}\n"
                f"command: {' '.join(command)}\n"
                f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
            )
        prefix = _PACKAGE_PREFIXES[distribution]
        rebuilt[distribution] = _one(
            sorted(wheelhouse.glob(f"{prefix}-*.whl")),
            f"rebuilt {distribution} wheel",
        )
    return rebuilt


def _run_matrix(
    *,
    matrix: str,
    version: str,
    dist_dir: Path,
    wheels: dict[str, Path],
    uv: str,
    root: Path,
) -> dict[str, Any]:
    framework = str(wheels["archetype-ecs"].resolve())
    selected = {
        "base": [framework],
        "missions": [framework, str(wheels["archetype-missions"].resolve())],
        "physical-ai": [framework, str(wheels["archetype-physical-ai"].resolve())],
        "research": [framework, str(wheels["archetype-research"].resolve())],
        # Every lane names the exact local artifacts under test. The framework
        # METADATA assertion above separately proves that its ``all`` extra
        # declares the same dependency set without letting an index satisfy
        # this installed-bytes oracle with an older published distribution.
        "all": [str(wheels[distribution].resolve()) for distribution in _DISTRIBUTIONS],
    }[matrix]
    expected_libraries = {
        "base": [],
        "missions": ["missions"],
        "physical-ai": ["physical-ai"],
        "research": ["research"],
        "all": ["missions", "physical-ai", "research"],
    }[matrix]

    clean_env = _clean_subprocess_environment()
    environment = root / f"venv-{matrix}"
    subprocess.run(
        [uv, "--no-config", "venv", "--python", sys.executable, str(environment)],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        env=clean_env,
    )
    python = environment / "bin" / "python"
    subprocess.run(
        [
            uv,
            "--no-config",
            "pip",
            "install",
            "--python",
            str(python),
            "--find-links",
            str(dist_dir.resolve()),
            "--no-cache",
            "--only-binary=:all:",
            *selected,
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        env=clean_env,
    )
    probe = f"""
import asyncio
import importlib.util
import json
import sys
from importlib.metadata import version
from pathlib import Path

import archetype
from archetype.api.app import create_app
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
print(json.dumps({{"matrix": {matrix!r}, "libraries": expected, "operations": operation_count, "module": str(package_root)}}))
"""
    process = subprocess.run(
        [str(python), "-c", probe],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        env=clean_env,
    )
    if process.returncode:
        raise RuntimeError(
            f"installed {matrix} package probe failed\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
    return json.loads(process.stdout.strip().splitlines()[-1])


def smoke(dist_dir: Path) -> list[dict[str, Any]]:
    wheels, sdists = _artifacts(dist_dir)
    versions = {
        distribution: _validate_wheel_contents(distribution, wheel)
        for distribution, wheel in wheels.items()
    }
    if len(set(versions.values())) != 1:
        raise RuntimeError(f"distribution wheel versions do not converge: {versions}")
    version = next(iter(versions.values()))
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("package smoke requires uv")
    with tempfile.TemporaryDirectory(prefix="archetype-package-smoke-") as temporary:
        root = Path(temporary)
        results = [
            _run_matrix(
                matrix=matrix,
                version=version,
                dist_dir=dist_dir,
                wheels=wheels,
                uv=uv,
                root=root,
            )
            for matrix in _OPERATION_COUNTS
        ]
        rebuilt_wheels = _rebuild_sdists(sdists=sdists, uv=uv, root=root)
        for distribution, rebuilt_wheel in rebuilt_wheels.items():
            _validate_wheel_contents(
                distribution,
                rebuilt_wheel,
                expected_version=versions[distribution],
            )
        sdist_probe_root = root / "sdist-probe"
        sdist_probe_root.mkdir()
        rebuilt_result = _run_matrix(
            matrix="all",
            version=version,
            dist_dir=next(iter(rebuilt_wheels.values())).parent,
            wheels=rebuilt_wheels,
            uv=uv,
            root=sdist_probe_root,
        )
        rebuilt_result["matrix"] = "sdist-all"
        results.append(rebuilt_result)
        return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dist_dir", type=Path, nargs="?", default=Path("dist"))
    args = parser.parse_args(argv)
    results = smoke(args.dist_dir)
    print(
        "Installed distribution matrix passed: "
        + ", ".join(f"{row['matrix']}={row['operations']}" for row in results)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
