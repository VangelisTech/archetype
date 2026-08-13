#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Record and verify the immutable distribution matrix used by releases."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any

SCHEMA = "archetype.release-artifact/v2"
DISTRIBUTIONS = (
    "archetype-ecs",
    "archetype-missions",
    "archetype-physical-ai",
    "archetype-research",
)
FRAMEWORK_DISTRIBUTION = "archetype-ecs"
_PACKAGE_PREFIXES = {
    "archetype-ecs": "archetype_ecs",
    "archetype-missions": "archetype_missions",
    "archetype-physical-ai": "archetype_physical_ai",
    "archetype-research": "archetype_research",
}
_KINDS = ("wheel", "sdist")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _one(paths: list[Path], label: str) -> Path:
    if len(paths) != 1:
        raise RuntimeError(f"release requires exactly one {label}; found {[p.name for p in paths]}")
    return paths[0]


def _distribution_files(dist: Path) -> dict[tuple[str, str], Path]:
    """Return the exact four-wheel/four-sdist release matrix."""

    artifacts: dict[tuple[str, str], Path] = {}
    for distribution in DISTRIBUTIONS:
        prefix = _PACKAGE_PREFIXES[distribution]
        artifacts[(distribution, "wheel")] = _one(
            sorted(dist.glob(f"{prefix}-*.whl")),
            f"{distribution} wheel",
        )
        artifacts[(distribution, "sdist")] = _one(
            sorted(dist.glob(f"{prefix}-*.tar.gz")),
            f"{distribution} sdist",
        )

    discovered = set(dist.glob("*.whl")) | set(dist.glob("*.tar.gz"))
    unexpected = discovered - set(artifacts.values())
    if unexpected:
        raise RuntimeError(
            "release contains unexpected distribution artifacts: "
            + ", ".join(sorted(path.name for path in unexpected))
        )
    if len(artifacts) != len(DISTRIBUTIONS) * len(_KINDS):  # pragma: no cover - invariant
        raise RuntimeError("release distribution matrix is incomplete")
    return artifacts


def _artifact_record(distribution: str, kind: str, path: Path) -> dict[str, Any]:
    return {
        "distribution": distribution,
        "kind": kind,
        "name": path.name,
        "sha256": _sha256(path),
        "size_bytes": path.stat().st_size,
    }


def record(root: Path, dist: Path) -> dict[str, Any]:
    artifacts = _distribution_files(dist)
    dirty = bool(_git(root, "status", "--porcelain", "--untracked-files=all"))
    records = [
        _artifact_record(distribution, kind, artifacts[(distribution, kind)])
        for distribution in DISTRIBUTIONS
        for kind in _KINDS
    ]
    return {
        "schema": SCHEMA,
        "commit": _git(root, "rev-parse", "HEAD"),
        "clean_checkout": not dirty,
        "artifacts": records,
    }


def artifact_records(manifest: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    """Validate and index the exact manifest record inventory."""

    raw_records = manifest.get("artifacts")
    expected = {(distribution, kind) for distribution in DISTRIBUTIONS for kind in _KINDS}
    if not isinstance(raw_records, list) or len(raw_records) != len(expected):
        raise ValueError("release artifact manifest must contain four wheel and four sdist records")

    records: dict[tuple[str, str], dict[str, Any]] = {}
    for value in raw_records:
        if not isinstance(value, dict):
            raise TypeError("release artifact record must be an object")
        distribution = value.get("distribution")
        kind = value.get("kind")
        if not isinstance(distribution, str) or not isinstance(kind, str):
            raise ValueError("release artifact distribution and kind must be strings")
        key = (distribution, kind)
        if key not in expected or key in records:
            raise ValueError(f"invalid or duplicate release artifact coordinate: {key!r}")
        name = value.get("name")
        sha256 = value.get("sha256")
        size_bytes = value.get("size_bytes")
        if not isinstance(name, str) or not name or Path(name).name != name:
            raise ValueError(f"release artifact {key!r} has an invalid filename")
        if not isinstance(sha256, str) or _SHA256.fullmatch(sha256) is None:
            raise ValueError(f"release artifact {key!r} has an invalid sha256")
        if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
            raise ValueError(f"release artifact {key!r} has an invalid size_bytes")
        records[key] = value
    if records.keys() != expected:
        missing = sorted(expected - records.keys())
        raise ValueError(f"release artifact manifest is missing coordinates: {missing!r}")
    return records


def verify(
    manifest: dict[str, Any],
    dist: Path,
    *,
    expected_commit: str,
) -> dict[str, Any]:
    if manifest.get("schema") != SCHEMA:
        raise ValueError("release artifact manifest has an unsupported schema")
    if manifest.get("clean_checkout") is not True:
        raise ValueError("release artifact was not built from a clean checkout")
    if manifest.get("commit") != expected_commit:
        raise ValueError(
            "release artifact commit mismatch: "
            f"manifest={manifest.get('commit')!r}, checkout={expected_commit!r}"
        )

    actual = _distribution_files(dist)
    records = artifact_records(manifest)
    for coordinate, path in actual.items():
        distribution, kind = coordinate
        value = records[coordinate]
        expected = {
            "name": path.name,
            "sha256": _sha256(path),
            "size_bytes": path.stat().st_size,
        }
        for field, observed in expected.items():
            if value.get(field) != observed:
                raise ValueError(
                    f"release {distribution} {kind} {field} mismatch: "
                    f"manifest={value.get(field)!r}, actual={observed!r}"
                )
    return manifest


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError("release artifact manifest must be an object")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("record", "verify"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--dist", type=Path, default=Path("dist"))
    parser.add_argument("--manifest", type=Path, default=Path("release-artifact.json"))
    args = parser.parse_args(argv)
    root = args.root.resolve()
    dist = args.dist.resolve()
    manifest_path = args.manifest.resolve()
    if args.command == "record":
        value = record(root, dist)
        manifest_path.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    else:
        value = verify(
            _load(manifest_path),
            dist,
            expected_commit=_git(root, "rev-parse", "HEAD"),
        )
    framework_wheel = artifact_records(value)[(FRAMEWORK_DISTRIBUTION, "wheel")]
    print(f"Release framework wheel sha256:{framework_wheel['sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
