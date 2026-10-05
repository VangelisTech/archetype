#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Validate and install the four current wheels outside the checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tarfile
import tempfile
import zipfile
from email.parser import Parser
from pathlib import Path

DISTRIBUTIONS = {
    "archetype-ecs": "0.7.0",
    "archetype-native": "0.7.0",
    "archetype-transports": "0.7.0",
    "archetype-smol": "0.6.3",
}


def _one(paths, label):
    if len(paths) != 1:
        raise RuntimeError(f"Expected one {label}, found {paths}")
    return paths[0]


def _artifacts(directory):
    wheels, sdists = {}, {}
    for name in DISTRIBUTIONS:
        prefix = name.replace("-", "_")
        wheels[name] = _one(sorted(directory.glob(prefix + "-*.whl")), name + " wheel")
        sdists[name] = _one(sorted(directory.glob(prefix + "-*.tar.gz")), name + " sdist")
    if set(directory.glob("*.whl")) != set(wheels.values()) or set(
        directory.glob("*.tar.gz")
    ) != set(sdists.values()):
        raise RuntimeError("Unexpected product distribution")
    return wheels, sdists


def _validate_wheel_contents(distribution, wheel, *, expected_version=None):
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        metadata = Parser().parsestr(
            archive.read(
                _one([name for name in names if name.endswith(".dist-info/METADATA")], "metadata")
            ).decode()
        )
        if metadata["Name"] != distribution or metadata["Version"] != (
            expected_version or DISTRIBUTIONS[distribution]
        ):
            raise RuntimeError("Distribution identity mismatch")
        if not any(name.endswith("/licenses/LICENSE") for name in names):
            raise RuntimeError("Missing Apache license")
        if any(name.startswith(("tests/", "evals/", "bench/", "quality/")) for name in names):
            raise RuntimeError("Repository harness leaked into wheel")
        if distribution == "archetype-ecs":
            if "archetype/__init__.py" not in names or any(
                name.startswith(
                    (
                        "archetype/missions/",
                        "archetype/physical_ai/",
                        "archetype/research/",
                        "archetype/smol/",
                    )
                )
                for name in names
            ):
                raise RuntimeError("Framework package ownership mismatch")
            requirements = metadata.get_all("Requires-Dist", [])
            ordinary = [value for value in requirements if "extra ==" not in value]
            if not any(value.startswith("archetype-native==0.7.0") for value in ordinary) or any(
                any(
                    token in value.lower()
                    for token in ("daft", "lance", "iceberg", "research", "missions", "physical-ai")
                )
                for value in ordinary
            ):
                raise RuntimeError("Default live dependency split drift")
        elif distribution == "archetype-native":
            if metadata.get_all("Requires-Dist", []):
                raise RuntimeError("Private native loader must remain stdlib-only")
        elif distribution == "archetype-smol":
            if any(
                value.startswith("archetype-") for value in metadata.get_all("Requires-Dist", [])
            ):
                raise RuntimeError("Smol must remain independent")
        elif any(name.startswith("archetype/") for name in names):
            raise RuntimeError("Transport package must not replace the root facade")
        for name in names:
            if name.endswith(".dist-info/entry_points.txt"):
                points = archive.read(name).decode()
                if "archetype.world_libraries" in points or any(
                    word in points for word in ("missions", "physical_ai", "research")
                ):
                    raise RuntimeError(
                        "Removed or compatibility-only domain entry point in current wheel"
                    )
    return metadata["Version"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dist_dir", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    wheels, sdists = _artifacts(args.dist_dir.resolve())
    for name, wheel in wheels.items():
        _validate_wheel_contents(name, wheel)
    stage = Path(tempfile.mkdtemp(prefix="archetype-package-smoke-")).resolve()
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "PYTHONHOME", "PYTHONOPTIMIZE"}
    }
    env.update(DO_NOT_TRACK="1", PYTHONDONTWRITEBYTECODE="1", PYTHONOPTIMIZE="0")
    subprocess.run(["uv", "venv", "--python", "3.12", str(stage / "env")], env=env, check=True)
    python = stage / "env/bin/python"
    rebuilt_identity = {}
    for name, sdist in sdists.items():
        unpack = stage / "sdists" / name
        unpack.mkdir(parents=True)
        with tarfile.open(sdist) as archive:
            archive.extractall(unpack, filter="data")
        source = _one([path for path in unpack.iterdir() if path.is_dir()], name + " source root")
        output = stage / "rebuilt" / name
        subprocess.run(
            ["uv", "build", "--wheel", str(source), "--out-dir", str(output)],
            env=env,
            check=True,
            capture_output=True,
        )
        rebuilt = _one(list(output.glob("*.whl")), name + " rebuilt wheel")
        _validate_wheel_contents(name, rebuilt)

        def content(path):
            with zipfile.ZipFile(path) as archive:
                return {
                    member: hashlib.sha256(archive.read(member)).hexdigest()
                    for member in archive.namelist()
                    if not member.endswith(".dist-info/RECORD")
                }

        if content(rebuilt) != content(wheels[name]):
            raise RuntimeError("Sdist/wheel content parity failed for " + name)
        rebuilt_identity[name] = hashlib.sha256(rebuilt.read_bytes()).hexdigest()
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            *map(str, (wheel for name, wheel in wheels.items() if name != "archetype-smol")),
        ],
        env=env,
        check=True,
    )
    probe = r"""import asyncio, importlib, importlib.metadata, json, pathlib, sys
from archetype import ArchetypeRuntime
if sys.flags.optimize: raise RuntimeError("Optimized probe cannot verify package identity")
root=pathlib.Path(sys.prefix).resolve()
for name in ("archetype","archetype.runtime","archetype_native","archetype_transports"):
    module=importlib.import_module(name)
    assert pathlib.Path(module.__file__).resolve().is_relative_to(root),(name,module.__file__)
assert not any(n=="daft" or n.startswith(("daft.","archetype.core.")) for n in sys.modules)
async def inert():
    async with ArchetypeRuntime(storage_only=True) as runtime:
        world=runtime.world("inert")
        assert not hasattr(world,"spawn") and not hasattr(world,"step") and not hasattr(world,"request")
        await world.shutdown()
asyncio.run(inert())
print(json.dumps({name:importlib.metadata.version(name) for name in ("archetype-ecs","archetype-native","archetype-transports")}))
"""
    completed = subprocess.run(
        [str(python), "-c", probe], cwd=stage, env=env, check=False, capture_output=True, text=True
    )
    (stage / "minimal-probe.log").write_text(completed.stdout + completed.stderr)
    completed.check_returncode()
    # Smol is an independent educational engine and deliberately loads Daft.
    smol_env = stage / "smol-env"
    subprocess.run(["uv", "venv", "--python", "3.12", str(smol_env)], env=env, check=True)
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(smol_env / "bin/python"),
            str(wheels["archetype-smol"]),
        ],
        env=env,
        check=True,
    )
    subprocess.run(
        [
            str(smol_env / "bin/python"),
            "-c",
            "import archetype.smol, pathlib, sys; assert pathlib.Path(archetype.smol.__file__).resolve().is_relative_to(pathlib.Path(sys.prefix))",
        ],
        env=env,
        cwd=stage,
        check=True,
    )
    receipt = {
        "schema": "archetype.package-smoke/v1",
        "stage": str(stage),
        "mode": "minimal exact-wheel import/inert lifecycle; no native execution claim",
        "versions": {
            **json.loads(completed.stdout),
            "archetype-smol": DISTRIBUTIONS["archetype-smol"],
        },
        "sdist_wheel_parity": rebuilt_identity,
        "independent_smol_environment": str(smol_env),
        "artifacts": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (*wheels.values(), *sdists.values())
        },
    }
    (stage / "receipt.json").write_text(json.dumps(receipt, indent=2))
    if args.out:
        args.out.write_text(json.dumps(receipt, indent=2))
    print("Current package smoke passed:", stage)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
