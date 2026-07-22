# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import scripts.scan_pinned_artifacts as scan_module
from scripts.scan_pinned_artifacts import build_queries, load_pinned_artifacts, main, scan

pytestmark = pytest.mark.contract("missions.environment.pinned")


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


def test_build_queries_covers_registry_pins_and_names_unscannable_kinds() -> None:
    queries, unscannable = build_queries(load_pinned_artifacts())
    packages = {
        (item["query"]["package"]["ecosystem"], item["query"]["package"]["name"])
        for item in queries
    }
    assert ("npm", "@openai/codex") in packages
    assert ("PyPI", "modal") in packages
    assert unscannable == ["coding-agent-base-image"]


def test_scan_reports_advisories_per_pinned_artifact() -> None:
    report = scan(
        inventory=scan_module.INVENTORY,
        endpoint="https://osv.invalid/querybatch",
        timeout=1.0,
        fetch=_fake_fetch({"modal"}),
    )
    by_id = {result["artifact_id"]: result for result in report["results"]}
    assert by_id["modal-sdk"]["vulnerabilities"] == ["OSV-TEST-1", "OSV-TEST-2"]
    assert by_id["codex-cli"]["vulnerabilities"] == []
    assert report["unscannable"] == ["coding-agent-base-image"]


def test_scan_rejects_mismatched_osv_response() -> None:
    with pytest.raises(ValueError, match="does not match the query count"):
        scan(
            inventory=scan_module.INVENTORY,
            endpoint="https://osv.invalid/querybatch",
            timeout=1.0,
            fetch=lambda url, payload, timeout: {"results": []},
        )


def test_main_writes_report_and_gates_on_findings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "pinned-artifact-osv.json"

    monkeypatch.setattr(scan_module, "_post_json", _fake_fetch(set()))
    assert main(["--out", str(out), "--fail-on-findings"]) == 0
    clean = json.loads(out.read_text(encoding="utf-8"))
    assert clean["schema_version"] == 1
    assert all(not result["vulnerabilities"] for result in clean["results"])

    monkeypatch.setattr(scan_module, "_post_json", _fake_fetch({"modal"}))
    assert main(["--out", str(out)]) == 0
    assert main(["--out", str(out), "--fail-on-findings"]) == 1
