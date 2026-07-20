#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Query OSV for known vulnerabilities in the pinned execution inventory.

The daily security audit runs this against
``src/archetype/missions/sandboxes/versions.toml`` so advisory coverage includes
the pinned coding-agent CLIs and SDKs, not only the Python dependency graph.
The script is stdlib-only: it parses the inventory directly and reports one
machine-readable result per scannable artifact. Artifacts without an OSV
ecosystem (installers, container images) are listed as unscannable evidence
rather than silently skipped.
"""

from __future__ import annotations

import argparse
import json
import tomllib
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "src" / "archetype" / "missions" / "sandboxes" / "versions.toml"
OSV_ENDPOINT = "https://api.osv.dev/v1/querybatch"
_ECOSYSTEMS = {"npm-package": "npm", "python-package": "PyPI"}


def load_pinned_artifacts(path: Path = INVENTORY) -> list[dict[str, Any]]:
    with path.open("rb") as stream:
        payload = tomllib.load(stream)
    rows = payload.get("artifact")
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{path}: [[artifact]] rows are required")
    return [row for row in rows if row.get("status") == "pinned"]


def build_queries(artifacts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    """Return (OSV queries, unscannable artifact ids) for pinned artifacts."""

    queries: list[dict[str, Any]] = []
    unscannable: list[str] = []
    for artifact in artifacts:
        ecosystem = _ECOSYSTEMS.get(str(artifact.get("kind")))
        if ecosystem is None:
            unscannable.append(str(artifact.get("id")))
            continue
        queries.append(
            {
                "artifact_id": str(artifact["id"]),
                "query": {
                    "package": {"ecosystem": ecosystem, "name": str(artifact["name"])},
                    "version": str(artifact["version"]),
                },
            }
        )
    return queries, unscannable


def _post_json(url: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def scan(
    *,
    inventory: Path,
    endpoint: str,
    timeout: float,
    fetch: Any = None,
) -> dict[str, Any]:
    fetch = fetch if fetch is not None else _post_json
    artifacts = load_pinned_artifacts(inventory)
    queries, unscannable = build_queries(artifacts)
    results: list[dict[str, Any]] = []
    if queries:
        response = fetch(endpoint, {"queries": [item["query"] for item in queries]}, timeout)
        rows = response.get("results")
        if not isinstance(rows, list) or len(rows) != len(queries):
            raise ValueError("OSV querybatch response does not match the query count")
        for item, row in zip(queries, rows, strict=True):
            vulnerabilities = sorted(
                str(vulnerability.get("id"))
                for vulnerability in (row or {}).get("vulns", [])
                if vulnerability.get("id")
            )
            results.append(
                {
                    "artifact_id": item["artifact_id"],
                    "ecosystem": item["query"]["package"]["ecosystem"],
                    "name": item["query"]["package"]["name"],
                    "version": item["query"]["version"],
                    "vulnerabilities": vulnerabilities,
                }
            )
    return {
        "schema_version": 1,
        "endpoint": endpoint,
        "results": results,
        "unscannable": sorted(unscannable),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=INVENTORY)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--endpoint", default=OSV_ENDPOINT)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--fail-on-findings", action="store_true")
    args = parser.parse_args(argv)

    report = scan(inventory=args.inventory, endpoint=args.endpoint, timeout=args.timeout)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    findings = 0
    for result in report["results"]:
        count = len(result["vulnerabilities"])
        findings += count
        status = f"{count} advisories" if count else "clean"
        print(f"{result['artifact_id']} {result['name']}=={result['version']}: {status}")
    for artifact_id in report["unscannable"]:
        print(f"{artifact_id}: no OSV ecosystem; verify through its pinned digest")
    if findings and args.fail_on_findings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
