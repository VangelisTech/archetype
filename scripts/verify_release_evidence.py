#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verify complete release-scenario evidence against one artifact matrix."""

from __future__ import annotations

import argparse
import json
import re
import tomllib
from pathlib import Path
from typing import Any

if __package__:
    from .release_artifact import (
        DISTRIBUTIONS,
        FRAMEWORK_DISTRIBUTION,
        artifact_records,
    )
    from .release_artifact import (
        SCHEMA as ARTIFACT_SCHEMA,
    )
else:  # pragma: no cover - exercised by the command-line entry point
    from release_artifact import (  # type: ignore[no-redef]
        DISTRIBUTIONS,
        FRAMEWORK_DISTRIBUTION,
        artifact_records,
    )
    from release_artifact import (
        SCHEMA as ARTIFACT_SCHEMA,
    )

RESULT_SCHEMA = "archetype.operational-results/v1"
SUMMARY_SCHEMA = "archetype.release-evidence-summary/v2"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain one JSON object")
    return value


def _wheel_manifest(manifest: dict[str, Any]) -> dict[str, dict[str, str]]:
    records = artifact_records(manifest)
    result: dict[str, dict[str, str]] = {}
    for distribution in DISTRIBUTIONS:
        record = records[(distribution, "wheel")]
        name = record.get("name")
        sha256 = record.get("sha256")
        if not isinstance(name, str) or not name:
            raise ValueError(f"release artifact manifest has invalid {distribution} wheel name")
        if not isinstance(sha256, str) or _SHA256.fullmatch(sha256) is None:
            raise ValueError(f"release artifact manifest has invalid {distribution} wheel sha256")
        result[distribution] = {
            "filename": name,
            "digest": f"sha256:{sha256}",
        }
    return result


def _verify_receipt_artifact_set(
    *,
    path: Path,
    wheel: dict[str, Any],
    release_wheels: dict[str, dict[str, str]],
) -> None:
    """Verify the optional exact matrix emitted by the operational runner."""

    raw = wheel.get("artifacts")
    if raw is None:
        return
    if not isinstance(raw, list) or len(raw) != len(DISTRIBUTIONS):
        raise ValueError(f"{path} wheel artifact set must contain all four distributions")

    observed: dict[str, dict[str, str]] = {}
    for value in raw:
        if not isinstance(value, dict):
            raise TypeError(f"{path} wheel artifact record must be an object")
        distribution = value.get("distribution")
        filename = value.get("filename")
        digest = value.get("digest")
        if not all(isinstance(item, str) and item for item in (distribution, filename, digest)):
            raise ValueError(f"{path} wheel artifact record has invalid fields")
        assert isinstance(distribution, str)
        if distribution not in release_wheels or distribution in observed:
            raise ValueError(
                f"{path} wheel artifact set has invalid or duplicate distribution {distribution!r}"
            )
        observed[distribution] = {"filename": str(filename), "digest": str(digest)}
    if observed != release_wheels:
        raise ValueError(f"{path} wheel artifact set does not match release manifest digests")


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
    release_wheels = _wheel_manifest(manifest)
    framework_wheel = release_wheels[FRAMEWORK_DISTRIBUTION]

    with registry.open("rb") as stream:
        rows = tomllib.load(stream).get("scenario", [])
    required = {
        str(row["id"])
        for row in rows
        if isinstance(row, dict) and "release" in row.get("required_cadence", [])
    }
    passed: set[str] = set()
    receipts_with_matrix = 0
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
        if not isinstance(wheel, dict) or wheel.get("digest") != framework_wheel["digest"]:
            raise ValueError(f"{path} is not bound to the archetype-ecs release wheel digest")
        filename = wheel.get("filename")
        if filename is not None and filename != framework_wheel["filename"]:
            raise ValueError(f"{path} names a different archetype-ecs release wheel")
        if wheel.get("artifacts") is not None:
            receipts_with_matrix += 1
        _verify_receipt_artifact_set(
            path=path,
            wheel=wheel,
            release_wheels=release_wheels,
        )
        cleanup = receipt.get("cleanup")
        if not isinstance(cleanup, dict) or cleanup.get("status") != "closed":
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
        "schema": SUMMARY_SCHEMA,
        "commit": commit,
        "framework_wheel_sha256": framework_wheel["digest"].removeprefix("sha256:"),
        "wheel_artifacts": [
            {"distribution": distribution, **release_wheels[distribution]}
            for distribution in DISTRIBUTIONS
        ],
        "receipts_with_artifact_matrix": receipts_with_matrix,
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
        f"for archetype-ecs sha256:{summary['framework_wheel_sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
