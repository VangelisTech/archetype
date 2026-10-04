# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import scripts.scan_pinned_artifacts as scan_module
from scripts.scan_pinned_artifacts import build_queries, load_pinned_artifacts, main, scan


@pytest.fixture
def inventory(tmp_path: Path) -> Path:
    path = tmp_path / "inventory.toml"
    rows = [
        ("codex-cli", "npm-package", "@openai/codex", "1.0"),
        ("modal-sdk", "python-package", "modal", "1.0"),
        ("ttyd-x86-64", "binary", "ttyd", "1.0"),
        ("ttyd-aarch64", "binary", "ttyd", "1.0"),
        ("coding-agent-base-image", "image", "example", "1.0"),
    ]
    path.write_text(
        "\n".join(
            f'[[artifact]]\nid = "{key}"\nkind = "{kind}"\nname = "{name}"\nversion = "{version}"\nstatus = "pinned"\n'
            for key, kind, name, version in rows
        )
    )
    return path


def test_inventory_is_explicit() -> None:
    with pytest.raises(SystemExit):
        main(["--out", "unused.json"])


def _fake_fetch(vulnerable_names: set[str]) -> Any:
    def fetch(url: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
        results = []
        for query in payload["queries"]:
            vulns = (
                [{"id": "OSV-TEST-1"}, {"id": "OSV-TEST-2"}]
                if query["package"]["name"] in vulnerable_names
                else []
            )
            results.append({"vulns": vulns})
        return {"results": results}

    return fetch


def test_build_queries_covers_registry_pins_and_names_unscannable_kinds(inventory: Path) -> None:
    queries, unscannable = build_queries(load_pinned_artifacts(inventory))
    packages = {
        (item["query"]["package"]["ecosystem"], item["query"]["package"]["name"])
        for item in queries
    }
    assert ("npm", "@openai/codex") in packages
    assert ("PyPI", "modal") in packages
    assert unscannable == [
        "ttyd-x86-64",
        "ttyd-aarch64",
        "coding-agent-base-image",
    ]


def test_scan_reports_advisories_per_pinned_artifact(inventory: Path) -> None:
    report = scan(
        inventory=inventory,
        endpoint="https://osv.invalid/querybatch",
        timeout=1.0,
        fetch=_fake_fetch({"modal"}),
    )
    by_id = {result["artifact_id"]: result for result in report["results"]}
    assert by_id["modal-sdk"]["vulnerabilities"] == ["OSV-TEST-1", "OSV-TEST-2"]
    assert by_id["codex-cli"]["vulnerabilities"] == []
    assert report["unscannable"] == [
        "coding-agent-base-image",
        "ttyd-aarch64",
        "ttyd-x86-64",
    ]


def test_scan_rejects_mismatched_osv_response(inventory: Path) -> None:
    with pytest.raises(ValueError, match="does not match the query count"):
        scan(
            inventory=inventory,
            endpoint="https://osv.invalid/querybatch",
            timeout=1.0,
            fetch=lambda url, payload, timeout: {"results": []},
        )


def test_main_writes_report_and_gates_on_findings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, inventory: Path
) -> None:
    out = tmp_path / "pinned-artifact-osv.json"

    monkeypatch.setattr(scan_module, "_post_json", _fake_fetch(set()))
    assert main(["--inventory", str(inventory), "--out", str(out), "--fail-on-findings"]) == 0
    clean = json.loads(out.read_text(encoding="utf-8"))
    assert clean["schema_version"] == 1
    assert all(not result["vulnerabilities"] for result in clean["results"])

    monkeypatch.setattr(scan_module, "_post_json", _fake_fetch({"modal"}))
    assert main(["--inventory", str(inventory), "--out", str(out)]) == 0
    assert main(["--inventory", str(inventory), "--out", str(out), "--fail-on-findings"]) == 1
