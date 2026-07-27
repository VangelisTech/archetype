# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for immutable release artifacts and aggregate evidence."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.release_artifact import SCHEMA as ARTIFACT_SCHEMA
from scripts.release_artifact import verify as verify_artifact
from scripts.verify_release_evidence import RESULT_SCHEMA, verify


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact(tmp_path: Path) -> tuple[Path, Path, str]:
    dist = tmp_path / "dist"
    dist.mkdir()
    wheel = dist / "archetype.whl"
    sdist = dist / "archetype.tar.gz"
    wheel.write_bytes(b"wheel")
    sdist.write_bytes(b"sdist")
    manifest = {
        "schema": ARTIFACT_SCHEMA,
        "commit": "a" * 40,
        "clean_checkout": True,
        "artifacts": [
            {
                "kind": "wheel",
                "name": wheel.name,
                "sha256": _sha256(wheel),
                "size_bytes": wheel.stat().st_size,
            },
            {
                "kind": "sdist",
                "name": sdist.name,
                "sha256": _sha256(sdist),
                "size_bytes": sdist.stat().st_size,
            },
        ],
    }
    path = tmp_path / "release-artifact.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return dist, path, f"sha256:{_sha256(wheel)}"


def test_release_artifact_verification_rejects_changed_bytes(tmp_path: Path) -> None:
    dist, manifest_path, _digest_value = _artifact(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_artifact(manifest, dist)

    (dist / "archetype.whl").write_bytes(b"other")
    with pytest.raises(ValueError, match="mismatch"):
        verify_artifact(manifest, dist)


def test_release_evidence_requires_every_scenario_on_exact_wheel(tmp_path: Path) -> None:
    _dist, manifest_path, wheel_digest = _artifact(tmp_path)
    registry = tmp_path / "scenarios.toml"
    registry.write_text(
        """
[[scenario]]
id = "one"
required_cadence = ["release"]

[[scenario]]
id = "two"
required_cadence = ["release"]
""",
        encoding="utf-8",
    )

    def receipt(path: Path, scenario: str) -> None:
        path.write_text(
            json.dumps(
                {
                    "schema": RESULT_SCHEMA,
                    "profile": "release:wheel:tier-0-6",
                    "mode": "wheel",
                    "outcome": "passed",
                    "revision": "a" * 40,
                    "clean_checkout": True,
                    "wheel": {"digest": wheel_digest},
                    "cleanup": {"status": "closed"},
                    "results": [{"scenario": scenario, "status": "passed"}],
                }
            ),
            encoding="utf-8",
        )

    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    receipt(first, "one")
    with pytest.raises(ValueError, match="missing required scenario.*two"):
        verify(registry=registry, manifest_path=manifest_path, receipt_paths=[first])

    receipt(second, "two")
    summary = verify(
        registry=registry,
        manifest_path=manifest_path,
        receipt_paths=[first, second],
    )
    assert summary["passed_scenarios"] == 2


def test_release_evidence_rejects_another_wheel_digest(tmp_path: Path) -> None:
    _dist, manifest_path, _wheel_digest = _artifact(tmp_path)
    registry = tmp_path / "scenarios.toml"
    registry.write_text(
        '[[scenario]]\nid = "one"\nrequired_cadence = ["release"]\n',
        encoding="utf-8",
    )
    receipt = tmp_path / "receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "schema": RESULT_SCHEMA,
                "profile": "release:wheel:tier-0-6",
                "mode": "wheel",
                "outcome": "passed",
                "revision": "a" * 40,
                "clean_checkout": True,
                "wheel": {"digest": "b" * 64},
                "cleanup": {"status": "closed"},
                "results": [{"scenario": "one", "status": "passed"}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="release wheel digest"):
        verify(registry=registry, manifest_path=manifest_path, receipt_paths=[receipt])


def test_release_evidence_rejects_non_release_or_duplicate_receipts(tmp_path: Path) -> None:
    _dist, manifest_path, wheel_digest = _artifact(tmp_path)
    registry = tmp_path / "scenarios.toml"
    registry.write_text(
        '[[scenario]]\nid = "one"\nrequired_cadence = ["release"]\n',
        encoding="utf-8",
    )

    def receipt(path: Path, profile: str) -> None:
        path.write_text(
            json.dumps(
                {
                    "schema": RESULT_SCHEMA,
                    "profile": profile,
                    "mode": "wheel",
                    "outcome": "passed",
                    "revision": "a" * 40,
                    "clean_checkout": True,
                    "wheel": {"digest": wheel_digest},
                    "cleanup": {"status": "closed"},
                    "results": [{"scenario": "one", "status": "passed"}],
                }
            ),
            encoding="utf-8",
        )

    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    receipt(first, "pr:wheel:tier-0-6")
    with pytest.raises(ValueError, match="not release-cadence"):
        verify(registry=registry, manifest_path=manifest_path, receipt_paths=[first])

    receipt(first, "release:wheel:tier-0-6")
    receipt(second, "release:wheel:tier-0-6")
    with pytest.raises(ValueError, match="duplicates release scenario"):
        verify(
            registry=registry,
            manifest_path=manifest_path,
            receipt_paths=[first, second],
        )
