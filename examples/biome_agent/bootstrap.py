# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Reproducibly prepare and launch upstream Biome without vendoring it."""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

BIOME_REPOSITORY = "https://github.com/SanderMertens/biome.git"
BIOME_REVISION = "d3372c2b3d7491b9260727292c27e554d12c0478"
BIOME_REF = "main"

FLECS_REPOSITORY = "https://github.com/SanderMertens/flecs.git"
FLECS_REVISION = "fd137d63deccded67aba4a0dd8a8a4231d24e897"
FLECS_REF = "script_await"

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CHECKOUT_ROOT = _REPOSITORY_ROOT / ".context" / "upstream"
MISSION_SCENE = Path(__file__).with_name("archetype_agent.flecs")
NATIVE_MODULE = Path(__file__).with_name("native") / "archetype_biome.c"

_MAIN_DECLARATION_ANCHOR = '#include "biome.h"\n'
_MAIN_DECLARATION = "void archetypeBiomeImport(ecs_world_t *world);\n"
_MAIN_IMPORT_ANCHOR = "    ECS_IMPORT(world, biomeUi);\n"
_MAIN_IMPORT = "    ECS_IMPORT(world, archetypeBiome);\n"


@dataclass(frozen=True)
class BiomeCheckout:
    root: Path
    biome: Path
    flecs: Path
    build: Path
    executable: Path
    scene: Path


def _run(command: list[str], *, cwd: Path | None = None) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def _output(command: list[str], *, cwd: Path | None = None) -> str:
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def _ensure_checkout(
    path: Path,
    repository: str,
    remote_ref: str,
    revision: str,
) -> None:
    if path.exists() and not (path / ".git").is_dir():
        raise RuntimeError(f"refusing to replace non-git path: {path}")
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        _run(["git", "clone", repository, str(path)])

    remote = _output(["git", "config", "--get", "remote.origin.url"], cwd=path)
    accepted = {repository, repository.removesuffix(".git")}
    if remote not in accepted:
        raise RuntimeError(f"{path} has unexpected origin {remote!r}")

    _run(["git", "fetch", "origin", remote_ref], cwd=path)
    _run(["git", "checkout", "--detach", revision], cwd=path)
    actual = _output(["git", "rev-parse", "HEAD"], cwd=path)
    if actual != revision:
        raise RuntimeError(f"expected {revision} in {path}, found {actual}")


def _patch_main(source: str) -> str:
    """Register the example-local native module in upstream Biome's main."""

    if _MAIN_DECLARATION in source and _MAIN_IMPORT in source:
        return source
    if source.count(_MAIN_DECLARATION_ANCHOR) != 1:
        raise RuntimeError("pinned Biome main.c declaration anchor changed")
    if source.count(_MAIN_IMPORT_ANCHOR) != 1:
        raise RuntimeError("pinned Biome main.c import anchor changed")
    source = source.replace(
        _MAIN_DECLARATION_ANCHOR,
        f"{_MAIN_DECLARATION_ANCHOR}\n{_MAIN_DECLARATION}",
        1,
    )
    return source.replace(
        _MAIN_IMPORT_ANCHOR,
        f"{_MAIN_IMPORT_ANCHOR}{_MAIN_IMPORT}",
        1,
    )


def _stage_native_bridge(biome: Path) -> None:
    """Patch only the managed checkout with the Archetype-owned C bridge."""

    main = biome / "src" / "main.c"
    pristine = subprocess.check_output(
        ["git", "show", f"{BIOME_REVISION}:src/main.c"],
        cwd=biome,
        text=True,
    )
    main.write_text(_patch_main(pristine))
    shutil.copyfile(NATIVE_MODULE, biome / "src" / "modules" / NATIVE_MODULE.name)


def prepare(
    checkout_root: Path = DEFAULT_CHECKOUT_ROOT, *, jobs: int | None = None
) -> BiomeCheckout:
    """Clone pinned sources, stage our scene, and build the real game."""

    if shutil.which("git") is None or shutil.which("cmake") is None:
        raise RuntimeError("the live Biome example requires git and cmake")

    root = checkout_root.resolve()
    biome = root / "biome"
    flecs = root / "flecs"
    build = biome / "build-agent"
    scene = biome / "etc" / "scenes" / "archetype_agent.flecs"

    _ensure_checkout(biome, BIOME_REPOSITORY, BIOME_REF, BIOME_REVISION)
    _ensure_checkout(flecs, FLECS_REPOSITORY, FLECS_REF, FLECS_REVISION)
    _stage_native_bridge(biome)
    shutil.copyfile(MISSION_SCENE, scene)

    _run(
        [
            "cmake",
            "-S",
            str(biome),
            "-B",
            str(build),
            f"-DFETCHCONTENT_SOURCE_DIR_FLECS={flecs}",
        ]
    )
    build_command = ["cmake", "--build", str(build), "--parallel"]
    if jobs is not None:
        if jobs < 1:
            raise ValueError("jobs must be at least 1")
        build_command.append(str(jobs))
    _run(build_command)

    executable = build / "biome"
    if not executable.is_file():
        raise RuntimeError(f"Biome build did not produce {executable}")
    return BiomeCheckout(root, biome, flecs, build, executable, scene)


def launch(checkout: BiomeCheckout) -> subprocess.Popen[bytes]:
    """Launch the pinned game and its Flecs REST server."""

    return subprocess.Popen(
        [str(checkout.executable), "--scene", "etc/scenes/archetype_agent.flecs"],
        cwd=checkout.biome,
        start_new_session=True,
    )
