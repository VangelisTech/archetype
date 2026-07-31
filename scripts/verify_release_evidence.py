#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verify complete release-scenario evidence against one immutable wheel."""

from __future__ import annotations

import argparse
import json
import re
import tomllib
from pathlib import Path
from typing import Any

if __package__:
    from .release_artifact import SCHEMA as ARTIFACT_SCHEMA
else:  # pragma: no cover - exercised by the command-line entry point
    from release_artifact import SCHEMA as ARTIFACT_SCHEMA

RESULT_SCHEMA = "archetype.operational-results/v1"


def _object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain one JSON object")
    return value


def verify(
    *,
    registry: Path,
    manifest_path: Path,
    receipt_paths: list[Path],
) -> dict[str, Any]:
    manifest = _object(manifest_path)
    if manifest.get("schema") != ARTIFACT_SCHEMA:
        raise ValueError("release evidence references an unsupported artifact manifest")
    if manifest.get("clean_checkout") is not True:
        raise ValueError("release artifact was not built from a clean checkout")
    commit = str(manifest.get("commit", ""))
    wheel_records = [
        item
        for item in manifest.get("artifacts", [])
        if isinstance(item, dict) and item.get("kind") == "wheel"
    ]
    if len(wheel_records) != 1:
        raise ValueError("release artifact manifest has no unique wheel")
    wheel_sha256 = str(wheel_records[0].get("sha256", ""))
    if not re.fullmatch(r"[0-9a-f]{64}", wheel_sha256):
        raise ValueError("release artifact manifest has an invalid wheel sha256")
    wheel_digest = f"sha256:{wheel_sha256}"

    with registry.open("rb") as stream:
        rows = tomllib.load(stream).get("scenario", [])
    required = {
        str(row["id"])
        for row in rows
        if isinstance(row, dict) and "release" in row.get("required_cadence", [])
    }
    passed: set[str] = set()
    for path in receipt_paths:
        receipt = _object(path)
        if receipt.get("schema") != RESULT_SCHEMA:
            raise ValueError(f"{path} has an unsupported result schema")
        if receipt.get("mode") != "wheel" or receipt.get("outcome") != "passed":
            raise ValueError(f"{path} is not passing installed-wheel evidence")
        profile = receipt.get("profile")
        if not isinstance(profile, str) or not profile.startswith("release:wheel:tier-"):
            raise ValueError(f"{path} is not release-cadence evidence")
        if receipt.get("revision") != commit or receipt.get("clean_checkout") is not True:
            raise ValueError(f"{path} is not bound to the clean release commit")
        wheel = receipt.get("wheel")
        if not isinstance(wheel, dict) or wheel.get("digest") != wheel_digest:
            raise ValueError(f"{path} is not bound to the release wheel digest")
        if receipt.get("cleanup", {}).get("status") != "closed":
            raise ValueError(f"{path} did not close its isolated filesystem")
        results = receipt.get("results")
        if not isinstance(results, list):
            raise TypeError(f"{path} has no scenario result list")
        for result in results:
            if not isinstance(result, dict) or result.get("status") != "passed":
                raise ValueError(f"{path} contains failed or not-run release evidence")
            scenario = str(result.get("scenario", ""))
            if scenario not in required:
                raise ValueError(f"{path} contains undeclared release scenario {scenario!r}")
            if scenario in passed:
                raise ValueError(f"{path} duplicates release scenario evidence {scenario!r}")
            passed.add(scenario)

    missing = sorted(required - passed)
    if missing:
        raise ValueError("release evidence is missing required scenario(s): " + ", ".join(missing))
    return {
        "schema": "archetype.release-evidence-summary/v1",
        "commit": commit,
        "wheel_sha256": wheel_sha256,
        "required_scenarios": len(required),
        "passed_scenarios": len(passed),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=Path("quality/operational_scenarios.toml"))
    parser.add_argument("--manifest", type=Path, default=Path("release-artifact.json"))
    parser.add_argument("--out", type=Path, default=Path("release-evidence-summary.json"))
    parser.add_argument("receipts", type=Path, nargs="+")
    args = parser.parse_args(argv)
    summary = verify(
        registry=args.registry,
        manifest_path=args.manifest,
        receipt_paths=args.receipts,
    )
    args.out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        f"Release evidence passed: {summary['passed_scenarios']} scenarios "
        f"for sha256:{summary['wheel_sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
