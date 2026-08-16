# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Reproducibly prepare and launch upstream Biome without vendoring it."""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

BIOME_REPOSITORY = "https://github.com/SanderMertens/biome.git"
BIOME_REVISION = "d3372c2b3d7491b9260727292c27e554d12c0478"
BIOME_REF = "main"

FLECS_REPOSITORY = "https://github.com/SanderMertens/flecs.git"
FLECS_REVISION = "fd137d63deccded67aba4a0dd8a8a4231d24e897"
FLECS_REF = "script_await"

BIOME_HOST = "127.0.0.1"
BIOME_PORT = 27750
BIOME_URL = f"http://{BIOME_HOST}:{BIOME_PORT}"

_PROCESS_TERM_GRACE_SECONDS = 5.0
_PROCESS_KILL_GRACE_SECONDS = 5.0
_PORT_CLOSE_GRACE_SECONDS = 5.0

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
    # Preserve leading columns from machine-readable output such as
    # ``git status --porcelain`` while removing its final line ending.
    return subprocess.check_output(command, cwd=cwd, text=True).rstrip()


def _revision_file(checkout: Path, revision: str, path: str) -> str:
    return subprocess.check_output(
        ["git", "show", f"{revision}:{path}"],
        cwd=checkout,
        text=True,
    )


def is_process_group_alive(process_group: int) -> bool:
    """Return whether any process remains in an owned process group."""

    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def is_port_open(host: str = BIOME_HOST, port: int = BIOME_PORT) -> bool:
    """Return whether a TCP listener is reachable at the Biome endpoint."""

    try:
        with socket.create_connection((host, port), timeout=0.2):
            return True
    except OSError:
        return False


def _signal_process_group(process_group: int, requested_signal: signal.Signals) -> None:
    try:
        os.killpg(process_group, requested_signal)
    except (PermissionError, ProcessLookupError):
        pass


def _wait_for_process_group_exit(process_group: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not is_process_group_alive(process_group):
            return True
        time.sleep(0.05)
    return not is_process_group_alive(process_group)


def _wait_for_port_close(host: str, port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not is_port_open(host, port):
            return True
        time.sleep(0.05)
    return not is_port_open(host, port)


def _ensure_checkout(
    path: Path,
    repository: str,
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

    # Named upstream branches are provenance labels only. Fetch the immutable
    # object directly so branch deletion or rewriting cannot block release.
    _run(["git", "fetch", "--no-tags", "--depth=1", "origin", revision], cwd=path)
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
    pristine = _revision_file(biome, BIOME_REVISION, "src/main.c")
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

    _ensure_checkout(biome, BIOME_REPOSITORY, BIOME_REVISION)
    _ensure_checkout(flecs, FLECS_REPOSITORY, FLECS_REVISION)
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


def _verify_pinned_checkout(checkout: BiomeCheckout) -> None:
    revisions = (
        ("Biome", checkout.biome, BIOME_REVISION),
        ("Flecs", checkout.flecs, FLECS_REVISION),
    )
    for label, path, expected in revisions:
        actual = _output(["git", "rev-parse", "HEAD"], cwd=path)
        if actual != expected:
            raise RuntimeError(
                f"refusing to launch {label} revision {actual!r}; expected exact pin {expected}"
            )

    biome_status = _output(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=checkout.biome
    )
    allowed_biome_paths = {
        "etc/scenes/archetype_agent.flecs",
        "src/main.c",
        "src/modules/archetype_biome.c",
    }
    unexpected_biome_paths = sorted(
        line[3:]
        for line in biome_status.splitlines()
        if line[3:] not in allowed_biome_paths and not line[3:].startswith("build-agent/")
    )
    if unexpected_biome_paths:
        raise RuntimeError(
            "refusing to launch a Biome checkout with unrelated source changes: "
            f"{unexpected_biome_paths}"
        )
    flecs_status = _output(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=checkout.flecs
    )
    if flecs_status:
        raise RuntimeError("refusing to launch a Flecs checkout with local source changes")

    expected_build = (checkout.biome / "build-agent").resolve()
    expected_executable = (expected_build / "biome").resolve()
    expected_scene = (checkout.biome / "etc" / "scenes" / MISSION_SCENE.name).resolve()
    if checkout.build.resolve() != expected_build:
        raise RuntimeError("refusing to launch a Biome build outside the managed pinned checkout")
    if checkout.executable.resolve() != expected_executable or not os.access(
        checkout.executable, os.X_OK
    ):
        raise RuntimeError("refusing to launch an unexpected or non-executable Biome binary")
    if (
        checkout.scene.resolve() != expected_scene
        or checkout.scene.read_bytes() != MISSION_SCENE.read_bytes()
    ):
        raise RuntimeError("refusing to launch without the exact Archetype Biome mission scene")
    expected_main = _patch_main(_revision_file(checkout.biome, BIOME_REVISION, "src/main.c"))
    if (checkout.biome / "src" / "main.c").read_text() != expected_main:
        raise RuntimeError("refusing to launch with an unexpected Biome main.c bridge")
    if (
        checkout.biome / "src" / "modules" / NATIVE_MODULE.name
    ).read_bytes() != NATIVE_MODULE.read_bytes():
        raise RuntimeError("refusing to launch with an unexpected native Biome bridge")


def launch(checkout: BiomeCheckout) -> subprocess.Popen[bytes]:
    """Launch the pinned game and its Flecs REST server."""

    _verify_pinned_checkout(checkout)
    if is_port_open():
        raise RuntimeError(f"refusing to launch Biome while {BIOME_HOST}:{BIOME_PORT} is in use")
    return subprocess.Popen(
        [str(checkout.executable), "--scene", "etc/scenes/archetype_agent.flecs"],
        cwd=checkout.biome,
        start_new_session=True,
    )


def terminate(
    process: subprocess.Popen[bytes],
    *,
    host: str = BIOME_HOST,
    port: int = BIOME_PORT,
    term_timeout: float = _PROCESS_TERM_GRACE_SECONDS,
    kill_timeout: float = _PROCESS_KILL_GRACE_SECONDS,
    port_timeout: float = _PORT_CLOSE_GRACE_SECONDS,
) -> None:
    """Terminate the complete owned Biome process group and prove port closure."""

    process_group = process.pid
    _signal_process_group(process_group, signal.SIGTERM)
    try:
        process.wait(timeout=term_timeout)
    except subprocess.TimeoutExpired:
        _signal_process_group(process_group, signal.SIGKILL)
        try:
            process.wait(timeout=kill_timeout)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"Biome process leader {process.pid} could not be reaped") from exc

    if not _wait_for_process_group_exit(process_group, term_timeout):
        _signal_process_group(process_group, signal.SIGKILL)
    if not _wait_for_process_group_exit(process_group, kill_timeout):
        raise RuntimeError(f"Biome process group {process_group} survived cleanup")
    if not _wait_for_port_close(host, port, port_timeout):
        raise RuntimeError(f"Biome REST port {host}:{port} survived cleanup")
