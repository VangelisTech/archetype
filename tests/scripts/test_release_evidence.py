# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for immutable release artifacts and aggregate evidence."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.release_artifact import (
    DISTRIBUTIONS,
    WORLD_STACK_DISTRIBUTIONS,
    _distribution_files,
    record,
)
from scripts.release_artifact import SCHEMA as ARTIFACT_SCHEMA
from scripts.release_artifact import verify as verify_artifact
from scripts.verify_release_evidence import RESULT_SCHEMA, SUMMARY_SCHEMA, verify

_PREFIXES = {
    "archetype-ecs": "archetype_ecs",
    "archetype-missions": "archetype_missions",
    "archetype-physical-ai": "archetype_physical_ai",
    "archetype-research": "archetype_research",
    "archetype-smol": "archetype_smol",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact(tmp_path: Path) -> tuple[Path, Path, dict[str, dict[str, str]]]:
    dist = tmp_path / "dist"
    dist.mkdir()
    wheel_records: dict[str, dict[str, str]] = {}
    records: list[dict[str, Any]] = []
    for distribution in DISTRIBUTIONS:
        prefix = _PREFIXES[distribution]
        wheel = dist / f"{prefix}-0.6.0-py3-none-any.whl"
        sdist = dist / f"{prefix}-0.6.0.tar.gz"
        wheel.write_bytes(f"wheel:{distribution}".encode())
        sdist.write_bytes(f"sdist:{distribution}".encode())
        for kind, path in (("wheel", wheel), ("sdist", sdist)):
            records.append(
                {
                    "distribution": distribution,
                    "kind": kind,
                    "name": path.name,
                    "sha256": _sha256(path),
                    "size_bytes": path.stat().st_size,
                }
            )
        wheel_records[distribution] = {
            "filename": wheel.name,
            "digest": f"sha256:{_sha256(wheel)}",
        }
    manifest = {
        "schema": ARTIFACT_SCHEMA,
        "version": "0.6.0",
        "commit": "a" * 40,
        "clean_checkout": True,
        "artifacts": records,
    }
    path = tmp_path / "release-artifact.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return dist, path, wheel_records


def _registry(tmp_path: Path, *scenarios: str) -> Path:
    registry = tmp_path / "scenarios.toml"
    registry.write_text(
        "\n".join(
            f'[[scenario]]\nid = "{scenario}"\nrequired_cadence = ["release"]\n'
            for scenario in scenarios
        ),
        encoding="utf-8",
    )
    return registry


def _receipt(
    path: Path,
    scenario: str,
    wheels: dict[str, dict[str, str]],
    *,
    include_matrix: bool = True,
) -> None:
    framework = wheels["archetype-ecs"]
    wheel: dict[str, Any] = {
        "filename": framework["filename"],
        "digest": framework["digest"],
    }
    if include_matrix:
        wheel["artifacts"] = [
            {"distribution": distribution, **wheels[distribution]}
            for distribution in WORLD_STACK_DISTRIBUTIONS
        ]
    path.write_text(
        json.dumps(
            {
                "schema": RESULT_SCHEMA,
                "profile": "release:wheel:tier-0-6",
                "mode": "wheel",
                "outcome": "passed",
                "revision": "a" * 40,
                "clean_checkout": True,
                "wheel": wheel,
                "cleanup": {"status": "closed"},
                "results": [{"scenario": scenario, "status": "passed"}],
            }
        ),
        encoding="utf-8",
    )


def test_release_artifact_verification_accepts_exact_five_distribution_matrix(
    tmp_path: Path,
) -> None:
    dist, manifest_path, _wheels = _artifact(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert len(_distribution_files(dist)) == 10
    assert verify_artifact(manifest, dist, expected_commit="a" * 40) is manifest


@pytest.mark.parametrize("extra_kind", ["wheel", "sdist"])
def test_release_artifact_verification_rejects_extra_artifact(
    tmp_path: Path,
    extra_kind: str,
) -> None:
    dist, manifest_path, _wheels = _artifact(tmp_path)
    suffix = ".whl" if extra_kind == "wheel" else ".tar.gz"
    (dist / f"unrelated-1.0{suffix}").write_bytes(b"extra")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    with pytest.raises(RuntimeError, match="unexpected distribution artifacts"):
        verify_artifact(manifest, dist, expected_commit="a" * 40)


def test_release_artifact_verification_rejects_missing_distribution_kind(
    tmp_path: Path,
) -> None:
    dist, manifest_path, _wheels = _artifact(tmp_path)
    (dist / "archetype_research-0.6.0.tar.gz").unlink()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    with pytest.raises(RuntimeError, match="exactly one archetype-research sdist"):
        verify_artifact(manifest, dist, expected_commit="a" * 40)


def test_release_artifact_verification_rejects_changed_bytes(tmp_path: Path) -> None:
    dist, manifest_path, _wheels = _artifact(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verify_artifact(manifest, dist, expected_commit="a" * 40)

    (dist / "archetype_missions-0.6.0-py3-none-any.whl").write_bytes(b"other")
    with pytest.raises(ValueError, match="archetype-missions wheel sha256 mismatch"):
        verify_artifact(manifest, dist, expected_commit="a" * 40)


def test_release_artifact_verification_rejects_duplicate_manifest_coordinate(
    tmp_path: Path,
) -> None:
    dist, manifest_path, _wheels = _artifact(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"][-1] = dict(manifest["artifacts"][0])

    with pytest.raises(ValueError, match="invalid or duplicate.*coordinate"):
        verify_artifact(manifest, dist, expected_commit="a" * 40)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("name", "../wheel.whl", id="filename"),
        ("sha256", "not-a-digest"),
        ("size_bytes", -1),
    ],
)
def test_release_artifact_verification_rejects_malformed_record_fields(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    dist, manifest_path, _wheels = _artifact(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"][0][field] = value

    expected_label = "filename" if field == "name" else field
    with pytest.raises(ValueError, match=f"invalid {expected_label}"):
        verify_artifact(manifest, dist, expected_commit="a" * 40)


def test_release_artifact_record_emits_distribution_coordinates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dist, _manifest_path, _wheels = _artifact(tmp_path)
    values = iter(("", "a" * 40))
    monkeypatch.setattr("scripts.release_artifact._git", lambda *args: next(values))

    manifest = record(tmp_path, dist)

    assert manifest["schema"] == ARTIFACT_SCHEMA
    assert manifest["version"] == "0.6.0"
    assert [(artifact["distribution"], artifact["kind"]) for artifact in manifest["artifacts"]] == [
        (distribution, kind) for distribution in DISTRIBUTIONS for kind in ("wheel", "sdist")
    ]


def test_release_artifact_verification_requires_clean_build(tmp_path: Path) -> None:
    dist, manifest_path, _wheels = _artifact(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    manifest["clean_checkout"] = False
    with pytest.raises(ValueError, match="not built from a clean checkout"):
        verify_artifact(manifest, dist, expected_commit="a" * 40)

    manifest.pop("clean_checkout")
    with pytest.raises(ValueError, match="not built from a clean checkout"):
        verify_artifact(manifest, dist, expected_commit="a" * 40)


@pytest.mark.parametrize(
    "version",
    [None, "", "not a version", "0.6.0.0"],
)
def test_release_artifact_manifest_requires_one_canonical_version(
    tmp_path: Path,
    version: object,
) -> None:
    dist, manifest_path, _wheels = _artifact(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if version is None:
        manifest.pop("version")
    else:
        manifest["version"] = version

    with pytest.raises(ValueError, match="version"):
        verify_artifact(manifest, dist, expected_commit="a" * 40)


def test_release_artifact_manifest_binds_every_filename_to_its_version(
    tmp_path: Path,
) -> None:
    dist, manifest_path, _wheels = _artifact(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["artifacts"][-1]["name"] = "archetype_smol-0.6.3.tar.gz"

    with pytest.raises(ValueError, match="not bound to manifest version 0.6.0"):
        verify_artifact(manifest, dist, expected_commit="a" * 40)


@pytest.mark.parametrize(
    "commit",
    [
        pytest.param(None, id="missing"),
        pytest.param(7, id="non-string"),
        pytest.param("not-a-commit", id="malformed"),
        pytest.param("b" * 40, id="another-valid-commit"),
    ],
)
def test_release_artifact_verification_requires_exact_checkout_commit(
    tmp_path: Path,
    commit: object,
) -> None:
    dist, manifest_path, _wheels = _artifact(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if commit is None:
        manifest.pop("commit")
    else:
        manifest["commit"] = commit

    with pytest.raises(ValueError, match="release artifact commit mismatch"):
        verify_artifact(manifest, dist, expected_commit="a" * 40)


def test_release_evidence_requires_every_scenario_on_framework_wheel(tmp_path: Path) -> None:
    _dist, manifest_path, wheels = _artifact(tmp_path)
    registry = _registry(tmp_path, "one", "two")
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    _receipt(first, "one", wheels)

    with pytest.raises(ValueError, match="missing required scenario.*two"):
        verify(registry=registry, manifest_path=manifest_path, receipt_paths=[first])

    _receipt(second, "two", wheels)
    summary = verify(
        registry=registry,
        manifest_path=manifest_path,
        receipt_paths=[first, second],
    )
    assert summary["schema"] == SUMMARY_SCHEMA
    assert summary["version"] == "0.6.0"
    assert summary["passed_scenarios"] == 2
    assert summary["framework_wheel_sha256"] == wheels["archetype-ecs"]["digest"].split(":")[1]
    assert len(summary["wheel_artifacts"]) == 5


def test_release_evidence_requires_exact_wheel_artifact_set(tmp_path: Path) -> None:
    _dist, manifest_path, wheels = _artifact(tmp_path)
    registry = _registry(tmp_path, "one")
    receipt = tmp_path / "receipt.json"
    _receipt(receipt, "one", wheels, include_matrix=True)

    summary = verify(registry=registry, manifest_path=manifest_path, receipt_paths=[receipt])

    assert summary["receipts_with_artifact_matrix"] == 1


def test_release_evidence_rejects_legacy_single_wheel_anchor(tmp_path: Path) -> None:
    _dist, manifest_path, wheels = _artifact(tmp_path)
    registry = _registry(tmp_path, "one")
    receipt = tmp_path / "receipt.json"
    _receipt(receipt, "one", wheels, include_matrix=False)

    with pytest.raises(
        ValueError,
        match="artifact set must contain all four world-stack distributions",
    ):
        verify(registry=registry, manifest_path=manifest_path, receipt_paths=[receipt])


@pytest.mark.parametrize("field", ["filename", "digest"])
def test_release_evidence_rejects_mismatched_wheel_artifact_set(
    tmp_path: Path,
    field: str,
) -> None:
    _dist, manifest_path, wheels = _artifact(tmp_path)
    registry = _registry(tmp_path, "one")
    receipt = tmp_path / "receipt.json"
    _receipt(receipt, "one", wheels, include_matrix=True)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["wheel"]["artifacts"][1][field] = "wrong"
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="artifact set does not match release manifest digests"):
        verify(registry=registry, manifest_path=manifest_path, receipt_paths=[receipt])


def test_release_evidence_rejects_another_framework_wheel_digest(tmp_path: Path) -> None:
    _dist, manifest_path, wheels = _artifact(tmp_path)
    registry = _registry(tmp_path, "one")
    receipt = tmp_path / "receipt.json"
    _receipt(receipt, "one", wheels)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["wheel"]["digest"] = "sha256:" + "b" * 64
    receipt.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="archetype-ecs release wheel digest"):
        verify(registry=registry, manifest_path=manifest_path, receipt_paths=[receipt])


def test_release_evidence_rejects_non_release_or_duplicate_receipts(tmp_path: Path) -> None:
    _dist, manifest_path, wheels = _artifact(tmp_path)
    registry = _registry(tmp_path, "one")
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    _receipt(first, "one", wheels)
    payload = json.loads(first.read_text(encoding="utf-8"))
    payload["profile"] = "pr:wheel:tier-0-6"
    first.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="not release-cadence"):
        verify(registry=registry, manifest_path=manifest_path, receipt_paths=[first])

    _receipt(first, "one", wheels)
    _receipt(second, "one", wheels)
    with pytest.raises(ValueError, match="duplicates release scenario"):
        verify(
            registry=registry,
            manifest_path=manifest_path,
            receipt_paths=[first, second],
        )
