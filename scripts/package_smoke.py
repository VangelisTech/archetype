#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Install the built wheel outside the checkout and smoke supported surfaces."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import site
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


def _single(paths: list[Path], kind: str) -> Path:
    if len(paths) != 1:
        raise RuntimeError(f"expected exactly one {kind}, found {[path.name for path in paths]}")
    return paths[0]


def validate_wheel_contents(wheel: Path) -> None:
    """Reject a wheel missing runtime code or containing repository harnesses."""
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    if "archetype/__init__.py" not in names:
        raise RuntimeError("wheel does not contain archetype/__init__.py")
    forbidden = ("tests/", "evals/", "bench/", "quality/")
    leaked = sorted(name for name in names if name.startswith(forbidden))
    if leaked:
        raise RuntimeError(f"repository-only files leaked into wheel: {leaked[:5]}")


def smoke(dist_dir: Path) -> dict[str, str]:
    wheel = _single(sorted(dist_dir.glob("*.whl")), "wheel")
    _single(sorted(dist_dir.glob("*.tar.gz")), "source distribution")
    validate_wheel_contents(wheel)

    with tempfile.TemporaryDirectory(prefix="archetype-package-smoke-") as tmp:
        root = Path(tmp)
        environment = root / "venv"
        uv = shutil.which("uv")
        if uv is None:
            raise RuntimeError("package smoke requires the repository's uv build tool")
        subprocess.run(
            [
                uv,
                "venv",
                "--python",
                sys.executable,
                str(environment),
            ],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        python = environment / "bin" / "python"
        subprocess.run(
            [
                uv,
                "pip",
                "install",
                "--python",
                str(python),
                "--no-deps",
                str(wheel.resolve()),
            ],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        # Reuse the already-locked development environment's third-party
        # packages without processing its editable-project .pth file. A .pth
        # path is appended after this venv's own site-packages, so the wheel
        # under test remains authoritative while the probe stays offline.
        child_site = subprocess.run(
            [
                str(python),
                "-c",
                "import site; print(site.getsitepackages()[0])",
            ],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dependency_sites = [path for path in site.getsitepackages() if Path(path).is_dir()]
        Path(child_site, "_archetype_smoke_dependencies.pth").write_text(
            "".join(f"{path}\n" for path in dependency_sites),
            encoding="utf-8",
        )
        probe = """
import json
from importlib.metadata import version
from pathlib import Path

import archetype
from archetype import ArchetypeRuntime
from archetype.api.app import create_app
from archetype.app.sandboxes.versions import load_version_inventory
from archetype.cli.main import app
from typer.testing import CliRunner

package_root = Path(archetype.__file__).resolve()
assert 'site-packages' in package_root.parts, package_root
assert '/src/archetype/' not in str(package_root), package_root
assert version('archetype-ecs') == archetype.__version__
assert ArchetypeRuntime.__name__ == 'ArchetypeRuntime'
inventory = load_version_inventory()
assert inventory.digest.startswith('sha256:'), inventory.digest
assert inventory.harness_pin('codex').immutable_ref
api = create_app()
assert api.version == archetype.__version__
result = CliRunner().invoke(app, ['--help'])
assert result.exit_code == 0, result.output
print(json.dumps({'version': archetype.__version__, 'module': str(package_root)}))
"""
        clean_env = os.environ.copy()
        clean_env.pop("PYTHONPATH", None)
        clean_env.pop("PYTHONHOME", None)
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
                "installed package probe failed\n"
                f"stdout:\n{process.stdout}\n"
                f"stderr:\n{process.stderr}"
            )
        return json.loads(process.stdout.strip().splitlines()[-1])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dist_dir", type=Path, nargs="?", default=Path("dist"))
    args = parser.parse_args(argv)
    result = smoke(args.dist_dir)
    print(f"Installed package smoke passed: {result['version']} ({result['module']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
