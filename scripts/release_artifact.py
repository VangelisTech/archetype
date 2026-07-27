#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Record and verify the immutable distribution used by release evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

SCHEMA = "archetype.release-artifact/v1"


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


def _distribution_files(dist: Path) -> tuple[Path, Path]:
    wheels = sorted(dist.glob("*.whl"))
    sdists = sorted(dist.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise RuntimeError(
            "release artifact requires exactly one wheel and one sdist; "
            f"found wheels={[path.name for path in wheels]}, "
            f"sdists={[path.name for path in sdists]}"
        )
    return wheels[0], sdists[0]


def record(root: Path, dist: Path) -> dict[str, Any]:
    wheel, sdist = _distribution_files(dist)
    dirty = bool(_git(root, "status", "--porcelain", "--untracked-files=all"))
    artifacts = [
        {
            "kind": kind,
            "name": path.name,
            "sha256": _sha256(path),
            "size_bytes": path.stat().st_size,
        }
        for kind, path in (("wheel", wheel), ("sdist", sdist))
    ]
    return {
        "schema": SCHEMA,
        "commit": _git(root, "rev-parse", "HEAD"),
        "clean_checkout": not dirty,
        "artifacts": artifacts,
    }


def verify(manifest: dict[str, Any], dist: Path) -> dict[str, Any]:
    if manifest.get("schema") != SCHEMA:
        raise ValueError("release artifact manifest has an unsupported schema")
    wheel, sdist = _distribution_files(dist)
    actual = {"wheel": wheel, "sdist": sdist}
    records = manifest.get("artifacts")
    if not isinstance(records, list) or len(records) != 2:
        raise ValueError("release artifact manifest must contain wheel and sdist records")
    seen: set[str] = set()
    for value in records:
        if not isinstance(value, dict):
            raise TypeError("release artifact record must be an object")
        kind = str(value.get("kind", ""))
        if kind not in actual or kind in seen:
            raise ValueError(f"invalid or duplicate release artifact kind: {kind!r}")
        seen.add(kind)
        path = actual[kind]
        expected = {
            "name": path.name,
            "sha256": _sha256(path),
            "size_bytes": path.stat().st_size,
        }
        for field, observed in expected.items():
            if value.get(field) != observed:
                raise ValueError(
                    f"release {kind} {field} mismatch: "
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
        value = verify(_load(manifest_path), dist)
    wheel = next(item for item in value["artifacts"] if item["kind"] == "wheel")
    print(f"Release wheel sha256:{wheel['sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
